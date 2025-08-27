#!/usr/bin/env python3
"""
FIXED ADVANCED BACKTESTER WITH PROPER PROGRESS TRACKING
======================================================
FIXES:
- Proper progress persistence and tracking
- State cleanup on restart
- Better cross-communication with dashboard
- Async task management improvements
- Database state management
"""

import pandas as pd
import numpy as np
import asyncio
import logging
import sqlite3
import time
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import threading
from dataclasses import dataclass, asdict
from contextlib import asynccontextmanager
import traceback

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@dataclass
class BacktestProgress:
    """Backtesting progress tracking"""
    status: str = 'not_started'
    current_symbol: Optional[str] = None
    current_strategy: Optional[str] = None
    completed: int = 0
    total: int = 0
    error_count: int = 0
    start_time: Optional[str] = None
    completion_time: Optional[str] = None
    progress_percent: float = 0.0
    eta_minutes: Optional[float] = None
    results_count: int = 0
    last_update: Optional[str] = None

class FixedAdvancedBacktester:
    """Fixed Advanced Backtester with proper progress tracking and state management"""
    
    def __init__(self, controller=None):
        self.controller = controller
        self.logger = logging.getLogger(f"{__name__}.FixedBacktester")
        
        # Database setup
        self.db_path = Path('data/comprehensive_backtest.db')
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.progress_db_path = Path('data/backtest_progress.db')
        
        # Initialize databases
        self._initialize_database()
        self._initialize_progress_database()
        
        # Configuration
        self.all_pairs = [
            'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'SOLUSDT', 'XRPUSDT',
            'DOGEUSDT', 'DOTUSDT', 'AVAXUSDT', 'SHIBUSDT', 'LINKUSDT', 'LTCUSDT',
            'UNIUSDT', 'ATOMUSDT', 'ALGOUSDT', 'VETUSDT', 'ICPUSDT', 'FILUSDT',
            'TRXUSDT', 'XLMUSDT', 'ETCUSDT', 'AAVEUSDT', 'EOSUSDT', 'FTMUSDT'
        ]
        
        self.timeframes = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']
        
        self.mtf_combinations = [
            (['1m', '5m', '15m'], 'MTF_Scalping_Ultra'),
            (['3m', '15m', '30m'], 'MTF_Scalping_Standard'),
            (['5m', '15m', '1h'], 'MTF_Short_Term_Momentum'),
            (['15m', '30m', '2h'], 'MTF_Short_Term_Trend'),
            (['30m', '1h', '4h'], 'MTF_Intraday_Swing'),
            (['1h', '4h', '8h'], 'MTF_Intraday_Position'),
            (['2h', '8h', '1d'], 'MTF_Daily_Swing'),
            (['4h', '1d', '3d'], 'MTF_Multi_Day_Swing'),
            (['6h', '1d', '1w'], 'MTF_Weekly_Position'),
            (['8h', '1d', '1w'], 'MTF_Weekly_Swing'),
            (['1d', '3d', '1w'], 'MTF_Weekly_Trend'),
            (['1d', '1w', '1M'], 'MTF_Long_Term_Trend')
        ]
        
        # Progress tracking
        self._progress_lock = threading.Lock()
        self.total_combinations = len(self.all_pairs) * len(self.mtf_combinations)
        self.progress = BacktestProgress(total=self.total_combinations)
        
        # State management
        self._running = False
        self._should_stop = False
        self._current_task = None
        
        self.logger.info(f"Fixed Backtester initialized: {len(self.all_pairs)} pairs, {self.total_combinations} combinations")
    
    def _initialize_database(self):
        """Initialize main results database"""
        schema = '''
        CREATE TABLE IF NOT EXISTS historical_backtests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timeframes TEXT NOT NULL,
            strategy_type TEXT NOT NULL,
            start_date TEXT,
            end_date TEXT,
            total_candles INTEGER DEFAULT 0,
            total_trades INTEGER DEFAULT 0,
            winning_trades INTEGER DEFAULT 0,
            win_rate REAL DEFAULT 0.0,
            total_return_pct REAL DEFAULT 0.0,
            max_drawdown REAL DEFAULT 0.0,
            sharpe_ratio REAL DEFAULT 0.0,
            avg_trade_duration_hours REAL DEFAULT 0.0,
            volatility REAL DEFAULT 0.0,
            best_trade_pct REAL DEFAULT 0.0,
            worst_trade_pct REAL DEFAULT 0.0,
            confluence_strength REAL DEFAULT 0.0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_backtests_symbol ON historical_backtests(symbol);
        CREATE INDEX IF NOT EXISTS idx_backtests_strategy ON historical_backtests(strategy_type);
        CREATE INDEX IF NOT EXISTS idx_backtests_sharpe ON historical_backtests(sharpe_ratio);
        CREATE INDEX IF NOT EXISTS idx_backtests_return ON historical_backtests(total_return_pct);
        '''
        
        try:
            conn = sqlite3.connect(self.db_path)
            conn.executescript(schema)
            conn.close()
            self.logger.info("Main database initialized")
        except Exception as e:
            self.logger.error(f"Database initialization failed: {e}")
    
    def _initialize_progress_database(self):
        """Initialize progress tracking database"""
        schema = '''
        CREATE TABLE IF NOT EXISTS backtest_progress (
            id INTEGER PRIMARY KEY,
            status TEXT DEFAULT 'not_started',
            current_symbol TEXT,
            current_strategy TEXT,
            completed INTEGER DEFAULT 0,
            total INTEGER DEFAULT 0,
            error_count INTEGER DEFAULT 0,
            start_time TEXT,
            completion_time TEXT,
            progress_percent REAL DEFAULT 0.0,
            eta_minutes REAL,
            results_count INTEGER DEFAULT 0,
            last_update TEXT DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS backtest_state (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        '''
        
        try:
            conn = sqlite3.connect(self.progress_db_path)
            conn.executescript(schema)
            conn.close()
            self.logger.info("Progress database initialized")
        except Exception as e:
            self.logger.error(f"Progress database initialization failed: {e}")
    
    def clear_previous_state(self):
        """Clear previous backtest state - CRITICAL FOR RESTART ISSUES"""
        try:
            with self._progress_lock:
                # Clear progress database
                conn = sqlite3.connect(self.progress_db_path)
                conn.execute("DELETE FROM backtest_progress")
                conn.execute("DELETE FROM backtest_state")
                conn.commit()
                conn.close()
                
                # Reset progress object
                self.progress = BacktestProgress(total=self.total_combinations)
                self._running = False
                self._should_stop = False
                
                self.logger.info("Previous backtest state cleared")
                
        except Exception as e:
            self.logger.error(f"Failed to clear previous state: {e}")
    
    def save_progress(self):
        """Save current progress to database"""
        try:
            with self._progress_lock:
                self.progress.last_update = datetime.now().isoformat()
                self.progress.progress_percent = (self.progress.completed / self.progress.total * 100) if self.progress.total > 0 else 0
                
                # Calculate ETA
                if self.progress.start_time and self.progress.completed > 0:
                    elapsed_time = (datetime.now() - datetime.fromisoformat(self.progress.start_time)).total_seconds()
                    remaining = self.progress.total - self.progress.completed
                    if remaining > 0:
                        time_per_item = elapsed_time / self.progress.completed
                        self.progress.eta_minutes = (remaining * time_per_item) / 60
                
                conn = sqlite3.connect(self.progress_db_path)
                conn.execute("""
                    INSERT OR REPLACE INTO backtest_progress 
                    (id, status, current_symbol, current_strategy, completed, total, error_count,
                     start_time, completion_time, progress_percent, eta_minutes, results_count, last_update)
                    VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    self.progress.status,
                    self.progress.current_symbol,
                    self.progress.current_strategy,
                    self.progress.completed,
                    self.progress.total,
                    self.progress.error_count,
                    self.progress.start_time,
                    self.progress.completion_time,
                    self.progress.progress_percent,
                    self.progress.eta_minutes,
                    self.progress.results_count,
                    self.progress.last_update
                ))
                conn.commit()
                conn.close()
                
        except Exception as e:
            self.logger.error(f"Failed to save progress: {e}")
    
    def get_progress(self) -> Dict:
        """Get current progress - used by dashboard"""
        try:
            conn = sqlite3.connect(self.progress_db_path)
            cursor = conn.execute("SELECT * FROM backtest_progress WHERE id = 1")
            result = cursor.fetchone()
            conn.close()
            
            if result:
                # Map database result to progress dict
                columns = ['id', 'status', 'current_symbol', 'current_strategy', 'completed', 
                          'total', 'error_count', 'start_time', 'completion_time', 
                          'progress_percent', 'eta_minutes', 'results_count', 'last_update']
                
                progress_data = dict(zip(columns[1:], result[1:]))  # Skip id column
                return progress_data
            
            # Return current in-memory progress if no database entry
            return asdict(self.progress)
            
        except Exception as e:
            self.logger.error(f"Failed to get progress: {e}")
            return asdict(self.progress)
    
    def is_running(self) -> bool:
        """Check if backtesting is currently running"""
        return self._running
    
    def stop_backtest(self):
        """Stop the current backtest"""
        self._should_stop = True
        if self._current_task:
            self._current_task.cancel()
        self.logger.info("Backtest stop requested")
    
    async def generate_mock_market_data(self, symbol: str, timeframe: str, days: int = 30) -> pd.DataFrame:
        """Generate realistic market data for backtesting"""
        try:
            # Calculate number of candles needed
            timeframe_minutes = {
                '1m': 1, '3m': 3, '5m': 5, '15m': 15, '30m': 30,
                '1h': 60, '2h': 120, '4h': 240, '6h': 360, '8h': 480, '12h': 720,
                '1d': 1440, '3d': 4320, '1w': 10080, '1M': 43200
            }
            
            minutes_per_candle = timeframe_minutes.get(timeframe, 15)
            total_candles = int((days * 24 * 60) / minutes_per_candle)
            
            # Base price for different symbols
            base_prices = {
                'BTCUSDT': 45000, 'ETHUSDT': 2800, 'BNBUSDT': 320, 'ADAUSDT': 0.48,
                'SOLUSDT': 95, 'XRPUSDT': 0.52, 'DOGEUSDT': 0.078, 'DOTUSDT': 6.5,
                'AVAXUSDT': 22, 'SHIBUSDT': 0.000012, 'LINKUSDT': 14.5, 'LTCUSDT': 95,
                'UNIUSDT': 6.2, 'ATOMUSDT': 8.5, 'ALGOUSDT': 0.25, 'VETUSDT': 0.025
            }
            
            base_price = base_prices.get(symbol, 100)
            
            # Generate price series with realistic volatility
            np.random.seed(hash(symbol + timeframe) % 2**32)
            
            # Create trends and volatility patterns
            trend_changes = np.random.randint(5, 15)  # Number of trend changes
            trend_points = np.sort(np.random.choice(total_candles, trend_changes, replace=False))
            
            prices = []
            current_price = base_price
            
            for i in range(total_candles):
                # Volatility based on timeframe
                volatility = 0.001 if minutes_per_candle <= 5 else 0.002 if minutes_per_candle <= 60 else 0.005
                
                # Add trend component
                if i in trend_points:
                    trend_strength = np.random.uniform(-0.05, 0.05)  # -5% to +5% trend
                else:
                    trend_strength = 0
                
                # Random price movement
                change = np.random.normal(trend_strength, volatility)
                current_price *= (1 + change)
                
                # Ensure price doesn't go negative or too extreme
                current_price = max(current_price, base_price * 0.1)
                current_price = min(current_price, base_price * 10)
                
                prices.append(current_price)
            
            # Generate OHLCV data
            data = []
            start_time = datetime.now() - timedelta(days=days)
            
            for i, close_price in enumerate(prices):
                timestamp = start_time + timedelta(minutes=i * minutes_per_candle)
                
                # Generate realistic OHLC from close price
                volatility_range = close_price * 0.02  # 2% intraday range
                
                high = close_price * np.random.uniform(1.0, 1.02)
                low = close_price * np.random.uniform(0.98, 1.0)
                open_price = close_price * np.random.uniform(0.995, 1.005)
                
                # Generate volume (higher volume during trends)
                base_volume = np.random.uniform(1000, 10000)
                if i > 0 and abs(prices[i] - prices[i-1]) / prices[i-1] > 0.01:  # High price movement
                    base_volume *= np.random.uniform(2, 5)
                
                data.append({
                    'timestamp': timestamp.isoformat(),
                    'open': open_price,
                    'high': high,
                    'low': low,
                    'close': close_price,
                    'volume': base_volume
                })
            
            df = pd.DataFrame(data)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to generate data for {symbol} {timeframe}: {e}")
            return pd.DataFrame()
    
    def calculate_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate comprehensive technical indicators"""
        try:
            df = df.copy()
            
            # Moving averages
            df['sma_10'] = df['close'].rolling(10).mean()
            df['sma_20'] = df['close'].rolling(20).mean()
            df['sma_50'] = df['close'].rolling(50).mean()
            df['ema_12'] = df['close'].ewm(span=12).mean()
            df['ema_26'] = df['close'].ewm(span=26).mean()
            
            # RSI
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            df['rsi'] = 100 - (100 / (1 + rs))
            
            # MACD
            df['macd'] = df['ema_12'] - df['ema_26']
            df['macd_signal'] = df['macd'].ewm(span=9).mean()
            df['macd_histogram'] = df['macd'] - df['macd_signal']
            
            # Bollinger Bands
            df['bb_middle'] = df['close'].rolling(20).mean()
            bb_std = df['close'].rolling(20).std()
            df['bb_upper'] = df['bb_middle'] + (bb_std * 2)
            df['bb_lower'] = df['bb_middle'] - (bb_std * 2)
            
            # ATR
            high_low = df['high'] - df['low']
            high_close = np.abs(df['high'] - df['close'].shift())
            low_close = np.abs(df['low'] - df['close'].shift())
            ranges = np.maximum(high_low, np.maximum(high_close, low_close))
            df['atr'] = ranges.rolling(14).mean()
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to calculate indicators: {e}")
            return df
    
    def analyze_multi_timeframe_strategy(self, symbol: str, timeframes: List[str], strategy_type: str) -> Optional[Dict]:
        """Analyze a multi-timeframe strategy combination"""
        try:
            # Get data for all timeframes
            timeframe_data = {}
            for tf in timeframes:
                data = asyncio.run(self.generate_mock_market_data(symbol, tf, days=60))
                if not data.empty:
                    data = self.calculate_technical_indicators(data)
                    timeframe_data[tf] = data
            
            if len(timeframe_data) < len(timeframes):
                return None
            
            # Primary timeframe (shortest) for trade signals
            primary_tf = timeframes[0]
            primary_data = timeframe_data[primary_tf]
            
            if len(primary_data) < 100:
                return None
            
            # Run backtest
            trades = []
            balance = 10000
            position = None
            
            for i in range(50, len(primary_data) - 10):  # Leave buffer for indicators
                
                # Check confluence signals across timeframes
                signals = self.get_confluence_signals(timeframe_data, timeframes, i, primary_tf)
                
                current_price = primary_data.iloc[i]['close']
                
                # Entry logic
                if position is None and signals['strength'] >= 2:
                    position = {
                        'entry_price': current_price,
                        'entry_time': primary_data.iloc[i]['timestamp'],
                        'side': signals['direction'],
                        'confidence': signals['confidence']
                    }
                
                # Exit logic
                elif position is not None:
                    exit_triggered = False
                    
                    # Stop loss / take profit
                    if position['side'] == 'BUY':
                        pnl_pct = (current_price - position['entry_price']) / position['entry_price']
                        if pnl_pct <= -0.02 or pnl_pct >= 0.04:  # 2% stop loss, 4% take profit
                            exit_triggered = True
                    else:  # SELL
                        pnl_pct = (position['entry_price'] - current_price) / position['entry_price']
                        if pnl_pct <= -0.02 or pnl_pct >= 0.04:
                            exit_triggered = True
                    
                    # Signal reversal exit
                    if signals['direction'] != position['side'] and signals['strength'] >= 2:
                        exit_triggered = True
                    
                    # Time-based exit (prevent holding too long)
                    if i - primary_data.index[primary_data['timestamp'] == position['entry_time']].tolist()[0] > 50:
                        exit_triggered = True
                    
                    if exit_triggered:
                        # Calculate trade result
                        if position['side'] == 'BUY':
                            return_pct = (current_price - position['entry_price']) / position['entry_price']
                        else:
                            return_pct = (position['entry_price'] - current_price) / position['entry_price']
                        
                        return_pct -= 0.002  # Trading fees
                        balance *= (1 + return_pct)
                        
                        trades.append({
                            **position,
                            'exit_price': current_price,
                            'exit_time': primary_data.iloc[i]['timestamp'],
                            'return_pct': return_pct,
                            'profit_loss': balance * return_pct
                        })
                        
                        position = None
            
            # Calculate performance metrics
            if len(trades) < 5:
                return None
            
            returns = [t['return_pct'] for t in trades]
            winning_trades = sum(1 for r in returns if r > 0)
            win_rate = (winning_trades / len(trades)) * 100
            total_return = ((balance - 10000) / 10000) * 100
            
            avg_return = np.mean(returns) * 100
            volatility = np.std(returns) * 100
            sharpe_ratio = (avg_return / volatility) if volatility > 0 else 0
            
            # Max drawdown calculation
            cumulative_returns = np.cumprod([1 + r for r in returns])
            running_max = np.maximum.accumulate(cumulative_returns)
            drawdowns = (cumulative_returns - running_max) / running_max
            max_drawdown = np.min(drawdowns) * 100
            
            return {
                'symbol': symbol,
                'timeframes': json.dumps(timeframes),
                'strategy_type': strategy_type,
                'start_date': primary_data.iloc[0]['timestamp'].isoformat(),
                'end_date': primary_data.iloc[-1]['timestamp'].isoformat(),
                'total_candles': len(primary_data),
                'total_trades': len(trades),
                'winning_trades': winning_trades,
                'win_rate': win_rate,
                'total_return_pct': total_return,
                'max_drawdown': abs(max_drawdown),
                'sharpe_ratio': sharpe_ratio,
                'avg_trade_duration_hours': 2.5,  # Approximation
                'volatility': volatility,
                'best_trade_pct': max(returns) * 100 if returns else 0,
                'worst_trade_pct': min(returns) * 100 if returns else 0,
                'confluence_strength': np.mean([signals['strength'] for signals in [signals]]),
                'created_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Strategy analysis failed for {symbol} {strategy_type}: {e}")
            return None
    
    def get_confluence_signals(self, timeframe_data: Dict, timeframes: List[str], index: int, primary_tf: str) -> Dict:
        """Get confluence signals from multiple timeframes"""
        
        signals = {'BUY': 0, 'SELL': 0}
        total_strength = 0
        
        for tf in timeframes:
            data = timeframe_data[tf]
            
            # Adjust index for different timeframe lengths
            tf_index = min(index, len(data) - 1)
            
            if tf_index < 20:  # Need enough data
                continue
            
            # RSI signal
            rsi = data.iloc[tf_index]['rsi']
            if rsi < 30:
                signals['BUY'] += 1
            elif rsi > 70:
                signals['SELL'] += 1
            
            # MACD signal
            macd = data.iloc[tf_index]['macd']
            macd_signal = data.iloc[tf_index]['macd_signal']
            if macd > macd_signal:
                signals['BUY'] += 1
            else:
                signals['SELL'] += 1
            
            # Moving average signal
            if tf_index > 0:
                price = data.iloc[tf_index]['close']
                sma20 = data.iloc[tf_index]['sma_20']
                if price > sma20:
                    signals['BUY'] += 1
                else:
                    signals['SELL'] += 1
            
            total_strength += 1
        
        # Determine overall direction
        max_direction = 'BUY' if signals['BUY'] > signals['SELL'] else 'SELL'
        strength = max(signals['BUY'], signals['SELL'])
        confidence = (strength / (total_strength * 3)) * 100 if total_strength > 0 else 0  # 3 signals per timeframe
        
        return {
            'direction': max_direction,
            'strength': strength,
            'confidence': min(confidence, 100),
            'signals': signals
        }
    
    def save_backtest_result(self, result: Dict):
        """Save backtest result to database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO historical_backtests 
                (symbol, timeframes, strategy_type, start_date, end_date, total_candles,
                 total_trades, winning_trades, win_rate, total_return_pct, max_drawdown,
                 sharpe_ratio, avg_trade_duration_hours, volatility, best_trade_pct,
                 worst_trade_pct, confluence_strength, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                result['symbol'], result['timeframes'], result['strategy_type'],
                result['start_date'], result['end_date'], result['total_candles'],
                result['total_trades'], result['winning_trades'], result['win_rate'],
                result['total_return_pct'], result['max_drawdown'], result['sharpe_ratio'],
                result['avg_trade_duration_hours'], result['volatility'], result['best_trade_pct'],
                result['worst_trade_pct'], result['confluence_strength'], result['created_at']
            ))
            
            conn.commit()
            conn.close()
            
            # Update results count
            with self._progress_lock:
                self.progress.results_count += 1
            
        except Exception as e:
            self.logger.error(f"Failed to save result: {e}")
    
    async def run_comprehensive_backtest(self) -> bool:
        """Run comprehensive backtest with proper progress tracking"""
        try:
            # Clear previous state first
            self.clear_previous_state()
            
            with self._progress_lock:
                self._running = True
                self._should_stop = False
                self.progress.status = 'in_progress'
                self.progress.start_time = datetime.now().isoformat()
                self.progress.completed = 0
                self.progress.error_count = 0
                self.progress.results_count = 0
            
            self.save_progress()
            self.logger.info("Starting comprehensive backtest...")
            
            combinations_processed = 0
            successful_results = 0
            
            for i, symbol in enumerate(self.all_pairs):
                if self._should_stop:
                    break
                
                for j, (timeframes, strategy_type) in enumerate(self.mtf_combinations):
                    if self._should_stop:
                        break
                    
                    combinations_processed += 1
                    
                    # Update progress
                    with self._progress_lock:
                        self.progress.current_symbol = symbol
                        self.progress.current_strategy = strategy_type
                        self.progress.completed = combinations_processed
                    
                    self.save_progress()
                    
                    # Notify dashboard of progress
                    if self.controller:
                        try:
                            await self.controller.update_backtest_progress(self.get_progress())
                        except Exception as e:
                            self.logger.warning(f"Failed to notify controller: {e}")
                    
                    self.logger.info(f"Processing {combinations_processed}/{self.total_combinations}: {symbol} - {strategy_type}")
                    
                    try:
                        # Analyze strategy
                        result = self.analyze_multi_timeframe_strategy(symbol, timeframes, strategy_type)
                        
                        if result and result.get('total_trades', 0) >= 5:
                            self.save_backtest_result(result)
                            successful_results += 1
                            self.logger.info(f"Saved result: {result['total_trades']} trades, {result['win_rate']:.1f}% win rate")
                        
                        # Small delay to prevent system overload
                        await asyncio.sleep(0.1)
                        
                    except Exception as e:
                        with self._progress_lock:
                            self.progress.error_count += 1
                        self.logger.error(f"Error processing {symbol} {strategy_type}: {e}")
                        continue
            
            # Complete the backtest
            with self._progress_lock:
                self.progress.status = 'completed' if not self._should_stop else 'stopped'
                self.progress.completion_time = datetime.now().isoformat()
                self.progress.completed = combinations_processed
                self._running = False
            
            self.save_progress()
            
            self.logger.info(f"Backtest {'completed' if not self._should_stop else 'stopped'}: {successful_results} successful results from {combinations_processed} combinations")
            
            return not self._should_stop
            
        except Exception as e:
            self.logger.error(f"Comprehensive backtest failed: {e}")
            
            with self._progress_lock:
                self.progress.status = 'error'
                self.progress.completion_time = datetime.now().isoformat()
                self._running = False
            
            self.save_progress()
            return False
    
    def get_best_strategies(self, limit: int = 20) -> List[Dict]:
        """Get best performing strategies"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT symbol, timeframes, strategy_type, total_trades, winning_trades, 
                       win_rate, total_return_pct, sharpe_ratio, max_drawdown
                FROM historical_backtests 
                WHERE total_trades >= 10 AND sharpe_ratio > 0.5
                ORDER BY sharpe_ratio DESC, total_return_pct DESC
                LIMIT ?
            """, (limit,))
            
            results = cursor.fetchall()
            conn.close()
            
            strategies = []
            for row in results:
                strategies.append({
                    'symbol': row[0],
                    'timeframes': row[1],
                    'strategy_type': row[2],
                    'total_trades': row[3],
                    'winning_trades': row[4],
                    'win_rate': row[5],
                    'total_return_pct': row[6],
                    'sharpe_ratio': row[7],
                    'max_drawdown': row[8]
                })
            
            return strategies
            
        except Exception as e:
            self.logger.error(f"Failed to get best strategies: {e}")
            return []
    
    def get_backtest_summary(self) -> Dict:
        """Get summary of backtest results"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT COUNT(*) as total_results,
                       AVG(win_rate) as avg_win_rate,
                       AVG(total_return_pct) as avg_return,
                       AVG(sharpe_ratio) as avg_sharpe,
                       MAX(total_return_pct) as best_return,
                       MIN(max_drawdown) as min_drawdown,
                       COUNT(CASE WHEN sharpe_ratio > 1.0 THEN 1 END) as good_strategies
                FROM historical_backtests
                WHERE total_trades >= 5
            """)
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                return {
                    'total_results': result[0] or 0,
                    'avg_win_rate': result[1] or 0,
                    'avg_return': result[2] or 0,
                    'avg_sharpe': result[3] or 0,
                    'best_return': result[4] or 0,
                    'min_drawdown': result[5] or 0,
                    'good_strategies': result[6] or 0
                }
            
            return {
                'total_results': 0,
                'avg_win_rate': 0,
                'avg_return': 0,
                'avg_sharpe': 0,
                'best_return': 0,
                'min_drawdown': 0,
                'good_strategies': 0
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get backtest summary: {e}")
            return {}
    
    def cleanup(self):
        """Cleanup resources"""
        self._should_stop = True
        self._running = False