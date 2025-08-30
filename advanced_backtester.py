#!/usr/bin/env python3
"""
V3 ADVANCED BACKTESTER - REAL DATA ONLY VERSION
===============================================
CRITICAL FIX: Removed all mock data generation functions
Now uses ONLY real Binance historical data for backtesting

Changes Made:
- Removed generate_mock_market_data() function
- Added fetch_real_historical_data() function
- All data comes from actual Binance API
- Enhanced error handling for real data fetching
- Maintained all existing backtesting logic
"""

import pandas as pd
import numpy as np
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import os
from pathlib import Path
import sqlite3
import json

# Import real Binance client
try:
    from binance.client import Client
    from binance import BinanceAPIException
    BINANCE_AVAILABLE = True
except ImportError:
    print("WARNING: python-binance not installed. Install with: pip install python-binance")
    Client = None
    BinanceAPIException = Exception
    BINANCE_AVAILABLE = False

# Import API rotation for real API key management
try:
    from api_rotation_manager import get_api_key, report_api_result
    API_ROTATION_AVAILABLE = True
except ImportError:
    print("WARNING: API rotation not available")
    API_ROTATION_AVAILABLE = False

class V3RealDataBacktester:
    """V3 Backtester using ONLY real market data - NO MOCK DATA"""
    
    def __init__(self, controller=None):
        self.logger = logging.getLogger(__name__)
        self.controller = controller
        
        # Initialize real Binance client
        self.client = self._initialize_real_binance_client()
        
        # Database for storing real backtest results
        self.db_path = Path('data/v3_real_backtests.db')
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize_database()
        
        # Real trading pairs (no synthetic data)
        self.real_pairs = [
            'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'SOLUSDT',
            'XRPUSDT', 'DOGEUSDT', 'AVAXUSDT', 'DOTUSDT', 'LINKUSDT',
            'LTCUSDT', 'UNIUSDT', 'ATOMUSDT', 'ALGOUSDT', 'VETUSDT',
            'SHIBUSDT', 'MATICUSDT', 'FILUSDT', 'TRXUSDT', 'ETCUSDT'
        ]
        
        # Real Binance timeframes only
        self.real_timeframes = [
            '1m', '3m', '5m', '15m', '30m',
            '1h', '2h', '4h', '6h', '8h', '12h', 
            '1d', '3d', '1w', '1M'
        ]
        
        # Multi-timeframe combinations for confluence analysis
        self.mtf_combinations = [
            (['1m', '5m', '15m'], 'scalping'),
            (['5m', '15m', '30m'], 'short_term'),
            (['15m', '1h', '4h'], 'intraday'),
            (['1h', '4h', '1d'], 'swing'),
            (['4h', '1d', '1w'], 'position'),
            (['1d', '1w', '1M'], 'long_term')
        ]
        
        # Progress tracking
        self.total_combinations = len(self.real_pairs) * len(self.mtf_combinations)
        self.completed = 0
        self.current_symbol = None
        self.current_strategy = None
        self.start_time = None
        self.status = 'not_started'
        
        self.logger.info("V3 Real Data Backtester initialized - NO MOCK DATA")
        self.logger.info(f"Real pairs: {len(self.real_pairs)}, Combinations: {self.total_combinations}")
    
    def _initialize_real_binance_client(self) -> Optional[Client]:
        """Initialize real Binance client using API rotation or environment variables"""
        if not BINANCE_AVAILABLE:
            self.logger.warning("Binance client not available - install python-binance")
            return None
        
        try:
            # Try API rotation first
            if API_ROTATION_AVAILABLE:
                binance_creds = get_api_key('binance')
                if binance_creds:
                    client = Client(
                        binance_creds['api_key'], 
                        binance_creds['api_secret'], 
                        testnet=True  # Use testnet for safety
                    )
                    self.logger.info("Real Binance client initialized via API rotation (TESTNET)")
                    return client
            
            # Fallback to environment variables
            api_key = os.getenv('BINANCE_API_KEY_1')
            api_secret = os.getenv('BINANCE_API_SECRET_1')
            
            if api_key and api_secret:
                client = Client(api_key, api_secret, testnet=True)
                self.logger.info("Real Binance client initialized via environment (TESTNET)")
                return client
            
            self.logger.error("No valid Binance API credentials found")
            return None
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Binance client: {e}")
            return None
    
    def _initialize_database(self):
        """Initialize database for storing real backtest results"""
        schema = '''
        CREATE TABLE IF NOT EXISTS v3_real_backtests (
            id INTEGER PRIMARY KEY,
            symbol TEXT NOT NULL,
            timeframes TEXT NOT NULL,
            strategy_type TEXT NOT NULL,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            total_candles INTEGER NOT NULL,
            total_trades INTEGER NOT NULL,
            winning_trades INTEGER NOT NULL,
            win_rate REAL NOT NULL,
            total_return_pct REAL NOT NULL,
            max_drawdown REAL NOT NULL,
            sharpe_ratio REAL NOT NULL,
            avg_trade_duration_hours REAL NOT NULL,
            volatility REAL NOT NULL,
            best_trade_pct REAL NOT NULL,
            worst_trade_pct REAL NOT NULL,
            confluence_strength REAL NOT NULL,
            data_source TEXT DEFAULT 'REAL_BINANCE',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS backtest_progress (
            id INTEGER PRIMARY KEY,
            status TEXT NOT NULL,
            current_symbol TEXT,
            current_strategy TEXT,
            completed INTEGER DEFAULT 0,
            total INTEGER DEFAULT 0,
            start_time TEXT,
            completion_time TEXT,
            data_source TEXT DEFAULT 'REAL_BINANCE',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_real_backtests_symbol ON v3_real_backtests(symbol);
        CREATE INDEX IF NOT EXISTS idx_real_backtests_strategy ON v3_real_backtests(strategy_type);
        CREATE INDEX IF NOT EXISTS idx_real_backtests_sharpe ON v3_real_backtests(sharpe_ratio);
        '''
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.executescript(schema)
            self.logger.info("Real backtest database initialized")
        except Exception as e:
            self.logger.error(f"Database initialization failed: {e}")
    
    async def fetch_real_historical_data(self, symbol: str, timeframe: str, 
                                       days_back: int = 365) -> Optional[pd.DataFrame]:
        """
        Fetch REAL historical data from Binance API
        CRITICAL: This function replaces all mock data generation
        """
        if not self.client:
            self.logger.error("No Binance client available for real data fetching")
            return None
        
        try:
            # Calculate start time
            end_time = datetime.now()
            start_time = end_time - timedelta(days=days_back)
            
            self.logger.info(f"Fetching REAL data for {symbol} {timeframe} from {start_time.date()} to {end_time.date()}")
            
            # Fetch real klines from Binance
            klines = self.client.get_historical_klines(
                symbol=symbol,
                interval=timeframe,
                start_str=start_time.strftime('%Y-%m-%d'),
                end_str=end_time.strftime('%Y-%m-%d')
            )
            
            if not klines:
                self.logger.warning(f"No real data returned for {symbol} {timeframe}")
                return None
            
            # Convert to DataFrame
            df = pd.DataFrame(klines, columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_asset_volume', 'number_of_trades',
                'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
            ])
            
            # Process real data
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Remove any invalid data
            df = df.dropna()
            
            if len(df) < 100:
                self.logger.warning(f"Insufficient real data for {symbol} {timeframe}: {len(df)} candles")
                return None
            
            self.logger.info(f"Fetched {len(df)} real candles for {symbol} {timeframe}")
            
            # Report successful API usage
            if API_ROTATION_AVAILABLE:
                report_api_result('binance', True, f"Fetched {len(df)} candles")
            
            return df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
            
        except BinanceAPIException as e:
            self.logger.error(f"Binance API error for {symbol} {timeframe}: {e}")
            if API_ROTATION_AVAILABLE:
                report_api_result('binance', False, str(e))
            return None
        except Exception as e:
            self.logger.error(f"Error fetching real data for {symbol} {timeframe}: {e}")
            return None
    
    async def run_comprehensive_real_backtest(self) -> Dict[str, Any]:
        """
        Run comprehensive backtest using ONLY real market data
        CRITICAL: No mock data is used anywhere in this process
        """
        self.logger.info("Starting V3 comprehensive real data backtest")
        self.start_time = datetime.now()
        self.status = 'in_progress'
        self.completed = 0
        
        # Save progress to database
        self._save_progress()
        
        all_results = {}
        successful_backtests = 0
        failed_backtests = 0
        
        try:
            for symbol in self.real_pairs:
                self.current_symbol = symbol
                self.logger.info(f"Testing {symbol} with real data...")
                
                # Fetch real data for all timeframes
                symbol_data = {}
                for timeframe in self.real_timeframes:
                    real_data = await self.fetch_real_historical_data(symbol, timeframe, days_back=365)
                    if real_data is not None and len(real_data) >= 200:
                        symbol_data[timeframe] = real_data
                    await asyncio.sleep(0.1)  # Rate limiting
                
                if len(symbol_data) < 3:
                    self.logger.warning(f"Insufficient real data for {symbol}")
                    failed_backtests += 1
                    continue
                
                # Test each multi-timeframe strategy
                symbol_results = {}
                for timeframes, strategy_type in self.mtf_combinations:
                    self.current_strategy = strategy_type
                    
                    # Check if we have real data for these timeframes
                    available_tfs = [tf for tf in timeframes if tf in symbol_data]
                    
                    if len(available_tfs) >= 2:  # Need at least 2 timeframes
                        result = await self._backtest_strategy_real_data(
                            symbol, available_tfs, symbol_data, strategy_type
                        )
                        
                        if result:
                            symbol_results[strategy_type] = result
                            successful_backtests += 1
                            
                            # Save to database
                            self._save_backtest_result(result)
                        else:
                            failed_backtests += 1
                    
                    self.completed += 1
                    self._save_progress()
                    
                    # Progress update
                    progress_pct = (self.completed / self.total_combinations) * 100
                    self.logger.info(f"Progress: {self.completed}/{self.total_combinations} ({progress_pct:.1f}%)")
                
                if symbol_results:
                    all_results[symbol] = symbol_results
                
                # Break if we have enough results for testing
                if len(all_results) >= 5 and successful_backtests >= 10:
                    self.logger.info("Sufficient real data backtests completed for validation")
                    break
        
        except Exception as e:
            self.logger.error(f"Comprehensive backtest error: {e}")
            self.status = 'error'
        else:
            self.status = 'completed'
        finally:
            self._save_progress()
        
        # Summary
        total_time = (datetime.now() - self.start_time).total_seconds()
        self.logger.info(f"Real data backtest completed in {total_time:.1f}s")
        self.logger.info(f"Successful: {successful_backtests}, Failed: {failed_backtests}")
        
        return {
            'results': all_results,
            'summary': {
                'total_symbols_tested': len(all_results),
                'successful_backtests': successful_backtests,
                'failed_backtests': failed_backtests,
                'execution_time_seconds': total_time,
                'data_source': 'REAL_BINANCE_API',
                'status': self.status
            }
        }
    
    async def _backtest_strategy_real_data(self, symbol: str, timeframes: List[str], 
                                         real_data: Dict[str, pd.DataFrame], 
                                         strategy_type: str) -> Optional[Dict[str, Any]]:
        """
        Backtest a strategy using ONLY real market data
        CRITICAL: All data comes from real Binance API calls
        """
        try:
            primary_tf = timeframes[0]  # Shortest timeframe for execution
            primary_data = real_data[primary_tf].copy()
            
            if len(primary_data) < 200:
                return None
            
            # Calculate technical indicators on real data
            indicators = self._calculate_real_indicators(primary_data)
            
            # Get confluence signals from multiple real timeframes
            confluence_data = {}
            for tf in timeframes:
                if tf in real_data:
                    tf_data = real_data[tf].copy()
                    tf_indicators = self._calculate_real_indicators(tf_data)
                    confluence_data[tf] = tf_indicators
            
            # Run backtest simulation on real data
            trades = []
            balance = 10000.0
            position = None
            max_balance = balance
            min_balance = balance
            
            # Use real price data for backtesting
            for i in range(50, len(indicators) - 1):  # Allow indicator warmup
                current_price = float(indicators.iloc[i]['close'])
                
                # Get real confluence signals
                signals = self._get_real_confluence_signals(confluence_data, timeframes, i)
                
                # Entry logic using real market signals
                if not position and signals['strength'] >= 2:
                    position = {
                        'symbol': symbol,
                        'entry_price': current_price,
                        'entry_time': indicators.iloc[i]['timestamp'],
                        'side': signals['direction'],
                        'confidence': signals['confidence'],
                        'strategy_type': strategy_type,
                        'timeframes': timeframes,
                        'entry_index': i
                    }
                
                # Exit logic using real market data
                elif position:
                    exit_signal = self._check_real_exit_conditions(indicators, position, i)
                    
                    if exit_signal:
                        # Calculate real P&L
                        if position['side'] == 'BUY':
                            return_pct = (current_price - position['entry_price']) / position['entry_price']
                        else:
                            return_pct = (position['entry_price'] - current_price) / position['entry_price']
                        
                        # Apply real trading costs
                        return_pct -= 0.002  # 0.2% total trading fees
                        
                        # Update balance
                        balance *= (1 + return_pct)
                        max_balance = max(max_balance, balance)
                        min_balance = min(min_balance, balance)
                        
                        # Record trade with real data
                        trade = {
                            **position,
                            'exit_price': current_price,
                            'exit_time': indicators.iloc[i]['timestamp'],
                            'return_pct': return_pct * 100,
                            'balance_after': balance,
                            'exit_reason': exit_signal['reason'],
                            'hold_duration': i - position['entry_index']
                        }
                        
                        trades.append(trade)
                        position = None
            
            # Calculate performance metrics from real data
            if not trades:
                return None
            
            returns = [t['return_pct'] / 100 for t in trades]
            winning_trades = sum(1 for r in returns if r > 0)
            
            # Real performance metrics
            total_return_pct = ((balance - 10000) / 10000) * 100
            win_rate = (winning_trades / len(trades)) * 100
            avg_return = np.mean(returns) * 100
            volatility = np.std(returns) * 100
            max_drawdown = ((min_balance - 10000) / 10000) * 100
            
            # Sharpe ratio calculation
            if volatility > 0:
                sharpe_ratio = (avg_return - 0.02) / volatility  # Assuming 2% risk-free rate
            else:
                sharpe_ratio = 0
            
            # Trade duration analysis
            durations = [t['hold_duration'] for t in trades]
            avg_duration_hours = np.mean(durations) * self._get_timeframe_minutes(primary_tf) / 60
            
            result = {
                'symbol': symbol,
                'strategy_type': strategy_type,
                'timeframes': timeframes,
                'data_source': 'REAL_BINANCE',
                'start_date': indicators.iloc[50]['timestamp'].isoformat(),
                'end_date': indicators.iloc[-1]['timestamp'].isoformat(),
                'total_candles': len(indicators),
                'total_trades': len(trades),
                'winning_trades': winning_trades,
                'win_rate': win_rate,
                'total_return_pct': total_return_pct,
                'avg_return_per_trade': avg_return,
                'volatility': volatility,
                'sharpe_ratio': sharpe_ratio,
                'max_drawdown': max_drawdown,
                'avg_trade_duration_hours': avg_duration_hours,
                'best_trade_pct': max(returns) * 100 if returns else 0,
                'worst_trade_pct': min(returns) * 100 if returns else 0,
                'confluence_strength': np.mean([t['confidence'] for t in trades if 'confidence' in t]),
                'trades': trades[-10:]  # Last 10 trades for analysis
            }
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error in strategy backtest for {symbol}: {e}")
            return None
    
    def _calculate_real_indicators(self, real_data: pd.DataFrame) -> pd.DataFrame:
        """Calculate technical indicators on REAL market data"""
        df = real_data.copy()
        
        # Ensure we have real price data
        if df.empty or len(df) < 50:
            return df
        
        # Moving averages on real prices
        df['sma_20'] = df['close'].rolling(window=20, min_periods=20).mean()
        df['sma_50'] = df['close'].rolling(window=50, min_periods=50).mean()
        df['ema_12'] = df['close'].ewm(span=12).mean()
        df['ema_26'] = df['close'].ewm(span=26).mean()
        
        # RSI on real price changes
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14, min_periods=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14, min_periods=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # MACD on real prices
        df['macd'] = df['ema_12'] - df['ema_26']
        df['macd_signal'] = df['macd'].ewm(span=9).mean()
        df['macd_histogram'] = df['macd'] - df['macd_signal']
        
        # Bollinger Bands on real prices
        bb_period = 20
        df['bb_middle'] = df['close'].rolling(window=bb_period, min_periods=bb_period).mean()
        bb_std = df['close'].rolling(window=bb_period, min_periods=bb_period).std()
        df['bb_upper'] = df['bb_middle'] + (bb_std * 2)
        df['bb_lower'] = df['bb_middle'] - (bb_std * 2)
        
        # Volume indicators on real volume
        df['volume_sma'] = df['volume'].rolling(window=20, min_periods=20).mean()
        
        return df
    
    def _get_real_confluence_signals(self, confluence_data: Dict[str, pd.DataFrame], 
                                   timeframes: List[str], index: int) -> Dict[str, Any]:
        """Get confluence signals from multiple real timeframes"""
        signals = {'BUY': 0, 'SELL': 0}
        total_confidence = 0
        signal_count = 0
        
        for tf in timeframes:
            if tf not in confluence_data:
                continue
                
            tf_data = confluence_data[tf]
            tf_index = min(index, len(tf_data) - 1)
            
            if tf_index < 50:  # Need enough data
                continue
            
            # Real price and indicator values
            current_row = tf_data.iloc[tf_index]
            
            tf_signals = []
            
            # RSI signals from real data
            if 'rsi' in current_row and not pd.isna(current_row['rsi']):
                rsi = float(current_row['rsi'])
                if rsi < 30:
                    tf_signals.append('BUY')
                elif rsi > 70:
                    tf_signals.append('SELL')
            
            # MACD signals from real data
            if all(col in current_row for col in ['macd', 'macd_signal']):
                macd = float(current_row['macd'])
                macd_signal = float(current_row['macd_signal'])
                
                if tf_index > 0:
                    prev_row = tf_data.iloc[tf_index - 1]
                    if all(col in prev_row for col in ['macd', 'macd_signal']):
                        prev_macd = float(prev_row['macd'])
                        prev_signal = float(prev_row['macd_signal'])
                        
                        if macd > macd_signal and prev_macd <= prev_signal:
                            tf_signals.append('BUY')
                        elif macd < macd_signal and prev_macd >= prev_signal:
                            tf_signals.append('SELL')
            
            # Count signals for this timeframe
            buy_count = tf_signals.count('BUY')
            sell_count = tf_signals.count('SELL')
            
            if buy_count > sell_count:
                signals['BUY'] += 1
                total_confidence += 70
            elif sell_count > buy_count:
                signals['SELL'] += 1
                total_confidence += 70
            
            signal_count += 1
        
        # Determine overall direction
        if signals['BUY'] > signals['SELL']:
            direction = 'BUY'
            strength = signals['BUY']
        elif signals['SELL'] > signals['BUY']:
            direction = 'SELL'
            strength = signals['SELL']
        else:
            direction = 'NEUTRAL'
            strength = 0
        
        confidence = (total_confidence / max(signal_count, 1))
        
        return {
            'direction': direction,
            'strength': strength,
            'confidence': confidence
        }
    
    def _check_real_exit_conditions(self, real_data: pd.DataFrame, position: Dict, 
                                  index: int) -> Optional[Dict[str, str]]:
        """Check exit conditions using real market data"""
        if index >= len(real_data) - 1:
            return {'reason': 'end_of_data'}
        
        current_price = float(real_data.iloc[index]['close'])
        entry_price = position['entry_price']
        hold_duration = index - position['entry_index']
        
        # Calculate current P&L
        if position['side'] == 'BUY':
            pnl_pct = (current_price - entry_price) / entry_price
        else:
            pnl_pct = (entry_price - current_price) / entry_price
        
        # Stop loss (real risk management)
        if pnl_pct <= -0.02:  # 2% stop loss
            return {'reason': 'stop_loss'}
        
        # Take profit (real profit target)
        if pnl_pct >= 0.04:  # 4% take profit
            return {'reason': 'take_profit'}
        
        # Maximum hold duration (real time management)
        max_hold = 100  # Maximum bars to hold
        if hold_duration >= max_hold:
            return {'reason': 'max_hold_duration'}
        
        # RSI reversal exit using real RSI
        if 'rsi' in real_data.columns and not pd.isna(real_data.iloc[index]['rsi']):
            rsi = float(real_data.iloc[index]['rsi'])
            if position['side'] == 'BUY' and rsi > 75:
                return {'reason': 'rsi_overbought'}
            elif position['side'] == 'SELL' and rsi < 25:
                return {'reason': 'rsi_oversold'}
        
        return None
    
    def _get_timeframe_minutes(self, timeframe: str) -> int:
        """Get minutes for a timeframe"""
        timeframe_minutes = {
            '1m': 1, '3m': 3, '5m': 5, '15m': 15, '30m': 30,
            '1h': 60, '2h': 120, '4h': 240, '6h': 360, '8h': 480, '12h': 720,
            '1d': 1440, '3d': 4320, '1w': 10080, '1M': 43200
        }
        return timeframe_minutes.get(timeframe, 60)
    
    def _save_progress(self):
        """Save backtest progress to database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO backtest_progress 
                    (id, status, current_symbol, current_strategy, completed, total, start_time)
                    VALUES (1, ?, ?, ?, ?, ?, ?)
                ''', (
                    self.status,
                    self.current_symbol,
                    self.current_strategy,
                    self.completed,
                    self.total_combinations,
                    self.start_time.isoformat() if self.start_time else None
                ))
        except Exception as e:
            self.logger.error(f"Error saving progress: {e}")
    
    def _save_backtest_result(self, result: Dict[str, Any]):
        """Save backtest result to database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT INTO v3_real_backtests (
                        symbol, timeframes, strategy_type, start_date, end_date,
                        total_candles, total_trades, winning_trades, win_rate,
                        total_return_pct, max_drawdown, sharpe_ratio, avg_trade_duration_hours,
                        volatility, best_trade_pct, worst_trade_pct, confluence_strength
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    result['symbol'], json.dumps(result['timeframes']), result['strategy_type'],
                    result['start_date'], result['end_date'], result['total_candles'],
                    result['total_trades'], result['winning_trades'], result['win_rate'],
                    result['total_return_pct'], result['max_drawdown'], result['sharpe_ratio'],
                    result['avg_trade_duration_hours'], result['volatility'],
                    result['best_trade_pct'], result['worst_trade_pct'], result['confluence_strength']
                ))
        except Exception as e:
            self.logger.error(f"Error saving backtest result: {e}")
    
    def get_progress(self) -> Dict[str, Any]:
        """Get current backtest progress"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT * FROM backtest_progress WHERE id = 1')
                row = cursor.fetchone()
                
                if row:
                    return {
                        'status': row[1],
                        'current_symbol': row[2],
                        'current_strategy': row[3],
                        'completed': row[4],
                        'total': row[5],
                        'progress_percent': (row[4] / max(row[5], 1)) * 100,
                        'start_time': row[6]
                    }
        except Exception as e:
            self.logger.error(f"Error getting progress: {e}")
        
        return {
            'status': self.status,
            'current_symbol': self.current_symbol,
            'current_strategy': self.current_strategy,
            'completed': self.completed,
            'total': self.total_combinations,
            'progress_percent': (self.completed / max(self.total_combinations, 1)) * 100,
            'start_time': self.start_time.isoformat() if self.start_time else None
        }

# Compatibility alias for existing code
AdvancedMultiTimeframeBacktester = V3RealDataBacktester

if __name__ == "__main__":
    # Test the real data backtester
    async def test_real_backtester():
        backtester = V3RealDataBacktester()
        
        # Test fetching real data
        real_data = await backtester.fetch_real_historical_data('BTCUSDT', '1h', 30)
        if real_data is not None:
            print(f"Successfully fetched {len(real_data)} real candles for BTCUSDT 1h")
            print("Sample data:")
            print(real_data.head())
        else:
            print("Failed to fetch real data")
    
    asyncio.run(test_real_backtester())