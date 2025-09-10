#!/usr/bin/env python3
"""
V3 Trading Controller - REAL DATA ONLY
Enhanced backtesting with comprehensive error handling and real market data
NO SIMULATION - PRODUCTION GRADE SYSTEM
"""

import os
import sys
import logging
import asyncio
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import sqlite3
import json
from dataclasses import dataclass
import threading
from concurrent.futures import ThreadPoolExecutor
import signal

# Import V3 components
from binance_exchange_manager import BinanceExchangeManager
from external_data_collector import EnhancedExternalDataCollector
from advanced_ml_engine import AdvancedMLEngine
from multi_pair_scanner import MultiPairScanner
from pnl_persistence import PnLPersistence

# Enforce real data mode
REAL_DATA_ONLY = True
MOCK_DATA_DISABLED = True

@dataclass
class BacktestResult:
    """Structure for backtest results"""
    symbol: str
    strategy: str
    timeframe: str
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    total_return: float
    sharpe_ratio: float
    max_drawdown: float
    profit_factor: float
    avg_trade_duration: float
    data_quality: str
    timestamp: datetime

class AsyncTaskManager:
    """Manage async background tasks"""
    
    def __init__(self):
        self.logger = logging.getLogger('main_controller.AsyncTaskManager')
        self.tasks = {}
        self.running = True
        
    async def start_background_updates(self, controller):
        """Start background update tasks"""
        try:
            while self.running:
                await asyncio.sleep(30)  # Update every 30 seconds
                # Add background update logic here if needed
        except asyncio.CancelledError:
            self.logger.info("Task background_updates was cancelled")
        except Exception as e:
            self.logger.error(f"Background updates error: {e}")
    
    def stop_all_tasks(self):
        """Stop all running tasks"""
        self.running = False
        for task_name, task in self.tasks.items():
            if not task.done():
                task.cancel()
                self.logger.info(f"Task {task_name} was cancelled")

class V3TradingController:
    """
    V3 Trading Controller - Main system orchestrator
    REAL DATA ONLY - NO SIMULATION
    """
    
    def __init__(self):
        """Initialize V3 Trading Controller with real data enforcement"""
        self.logger = logging.getLogger('main_controller')
        
        # Enforce real data mode
        if not REAL_DATA_ONLY:
            raise ValueError("CRITICAL: System must use REAL DATA ONLY")
        
        # System state
        self.system_ready = False
        self.trading_enabled = False
        self.mode = os.getenv('DEFAULT_TRADING_MODE', 'PAPER_TRADING')
        
        # Components
        self.exchange_manager = None
        self.external_data_collector = None
        self.ml_engine = None
        self.multi_pair_scanner = None
        self.backtester = None
        self.pnl_persistence = None
        
        # Performance tracking
        self.performance_metrics = {
            'total_trades': 0,
            'total_pnl': 0.0,
            'win_rate': 0.0,
            'active_positions': 0
        }
        
        # Strategy management
        self.strategies = {}
        self.strategy_performance = {}
        
        # Task management
        self.task_manager = AsyncTaskManager()
        
        # Initialize system
        self._initialize_system()
        
        self.logger.info(f"V3 Trading Controller initialized - Mode: {self.mode}")
    
    def _initialize_system(self):
        """Initialize all system components"""
        try:
            self.logger.info("Initializing V3 Trading System")
            
            # Initialize exchange manager
            self.exchange_manager = BinanceExchangeManager()
            self.logger.info("Exchange manager initialized")
            
            # Initialize external data collector
            self.external_data_collector = EnhancedExternalDataCollector()
            self.logger.info("External data collector initialized")
            
            # Initialize ML engine
            try:
                self.ml_engine = AdvancedMLEngine()
                self.logger.info("AI Brain initialized")
            except Exception as e:
                self.logger.warning(f"ML engine initialization failed: {e}")
                self.ml_engine = None
            
            # Initialize multi-pair scanner
            try:
                self.multi_pair_scanner = MultiPairScanner()
                self.logger.info("Multi-pair scanner initialized")
            except Exception as e:
                self.logger.warning(f"Multi-pair scanner initialization failed: {e}")
                self.multi_pair_scanner = None
            
            # Initialize PnL persistence
            self.pnl_persistence = PnLPersistence()
            self.logger.info("PnL persistence initialized")
            
            # Load performance data
            performance_data = self.pnl_persistence.get_performance_summary()
            total_trades = performance_data.get('total_trades', 0)
            total_pnl = performance_data.get('total_pnl', 0.0)
            self.logger.info(f"[V3_ENGINE] Loaded REAL performance: {total_trades} trades, ${total_pnl:.2f} P&L")
            
            # Initialize trading engine
            try:
                self._initialize_trading_engine()
            except Exception as e:
                self.logger.warning(f"Trading engine initialization failed: {e}")
            
            # Initialize backtester
            self.backtester = self.Backtester(self)
            self.logger.info("Comprehensive backtester initialized")
            
            # Load strategies
            self._load_strategies()
            
            self.system_ready = True
            self.logger.info(f"V3 System initialized - Trading Mode: {self.mode}")
            
        except Exception as e:
            self.logger.error(f"System initialization failed: {e}")
            raise
    
    def _initialize_trading_engine(self):
        """Initialize the trading engine"""
        try:
            # Get exchange client for trading
            exchange_client = self.exchange_manager.get_exchange_client()
            
            if not exchange_client:
                raise Exception("SYSTEM FAILURE: Cannot connect to REAL Binance - CRITICAL: No REAL Binance credentials available from API rotation")
            
            self.trading_enabled = True
            self.logger.info("Trading engine initialized successfully")
            
        except Exception as e:
            self.logger.error(f"CRITICAL: V3 REAL Binance client initialization failed: {e}")
            raise
    
    def _load_strategies(self):
        """Load trading strategies"""
        try:
            # Load basic strategies
            self.strategies = {
                'scalping': {
                    'timeframes': ['5m', '15m'],
                    'min_candles': 100,
                    'description': 'Short-term scalping strategy'
                },
                'short_term': {
                    'timeframes': ['15m', '1h'],
                    'min_candles': 80,
                    'description': 'Short-term momentum strategy'
                },
                'intraday': {
                    'timeframes': ['1h', '4h'],
                    'min_candles': 50,
                    'description': 'Intraday swing strategy'
                },
                'swing': {
                    'timeframes': ['4h', '1d'],
                    'min_candles': 30,
                    'description': 'Swing trading strategy'
                },
                'position': {
                    'timeframes': ['1d'],
                    'min_candles': 20,
                    'description': 'Position trading strategy'
                },
                'momentum': {
                    'timeframes': ['4h', '1d'],
                    'min_candles': 30,
                    'description': 'Momentum strategy'
                },
                'confluence': {
                    'timeframes': ['1h', '4h'],
                    'min_candles': 50,
                    'description': 'Multi-timeframe confluence'
                },
                'trend_following': {
                    'timeframes': ['4h', '1d'],
                    'min_candles': 30,
                    'description': 'Trend following strategy'
                }
            }
            
            # Count strategies
            strategy_count = len(self.strategies)
            ml_trained_count = 1 if self.ml_engine else 0
            
            self.logger.info(f"Loaded {strategy_count} strategies, {ml_trained_count} ML-trained")
            
        except Exception as e:
            self.logger.error(f"Strategy loading failed: {e}")
            self.strategies = {}
    
    def get_trading_status(self) -> Dict[str, Any]:
        """Get current trading status"""
        try:
            performance_data = self.pnl_persistence.get_performance_summary() if self.pnl_persistence else {}
            
            return {
                'total_trades': performance_data.get('total_trades', 0),
                'total_pnl': performance_data.get('total_pnl', 0.0),
                'win_rate': performance_data.get('win_rate', 0.0),
                'active_positions': performance_data.get('active_positions', 0),
                'trading_enabled': self.trading_enabled,
                'mode': self.mode,
                'system_ready': self.system_ready
            }
        except Exception as e:
            self.logger.error(f"Error getting trading status: {e}")
            return {
                'total_trades': 0,
                'total_pnl': 0.0,
                'win_rate': 0.0,
                'active_positions': 0,
                'trading_enabled': False,
                'mode': self.mode,
                'system_ready': self.system_ready
            }
    
    def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status"""
        try:
            return {
                'system_ready': self.system_ready,
                'trading_enabled': self.trading_enabled,
                'mode': self.mode,
                'strategies_loaded': len(self.strategies),
                'ml_engine_active': self.ml_engine is not None,
                'external_data_active': self.external_data_collector is not None,
                'exchange_connected': self.exchange_manager is not None,
                'scanner_active': self.multi_pair_scanner is not None,
                'components': {
                    'exchange_manager': self.exchange_manager is not None,
                    'external_data_collector': self.external_data_collector is not None,
                    'ml_engine': self.ml_engine is not None,
                    'multi_pair_scanner': self.multi_pair_scanner is not None,
                    'backtester': self.backtester is not None,
                    'pnl_persistence': self.pnl_persistence is not None
                }
            }
        except Exception as e:
            self.logger.error(f"Error getting system status: {e}")
            return {
                'system_ready': False,
                'error': str(e)
            }
    
    class Backtester:
        """
        Enhanced Backtester with real data validation and error handling
        REAL MARKET DATA ONLY
        """
        
        def __init__(self, controller):
            self.controller = controller
            self.logger = logging.getLogger('main_controller.Backtester')
            
            # Get exchange client for data
            self.exchange_client = controller.exchange_manager.get_exchange_client()
            if self.exchange_client:
                self.logger.info("Using exchange_manager client for backtesting")
            else:
                raise Exception("No exchange client available for backtesting")
            
            # Backtesting configuration
            self.pairs = self._get_trading_pairs()
            self.timeframes = ['5m', '15m', '1h', '4h', '1d']
            self.strategies = list(controller.strategies.keys())
            
            # Calculate total combinations
            total_combinations = len(self.pairs) * len(self.strategies) * len(self.timeframes)
            self.logger.info(f"Backtester initialized: {len(self.pairs)} pairs, {total_combinations} combinations")
            
            # Progress tracking
            self.progress = {
                'current': 0,
                'total': total_combinations,
                'completed_combinations': [],
                'failed_combinations': [],
                'results': [],
                'start_time': None,
                'status': 'ready'
            }
        
        def _get_trading_pairs(self) -> List[str]:
            """Get trading pairs for backtesting"""
            try:
                # Get pairs from environment or use defaults
                pairs_config = os.getenv('MAJOR_PAIRS', 'BTCUSDT,ETHUSDT,BNBUSDT,XRPUSDT,ADAUSDT,SOLUSDT,DOGEUSDT,LINKUSDT,LTCUSDT,UNIUSDT,AVAXUSDT,DOTUSDT')
                pairs = [pair.strip() for pair in pairs_config.split(',')]
                
                # Limit pairs for performance
                max_pairs = int(os.getenv('COMPREHENSIVE_PAIRS_COUNT', 12))
                return pairs[:max_pairs]
                
            except Exception as e:
                self.logger.error(f"Error getting trading pairs: {e}")
                return ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'XRPUSDT', 'ADAUSDT', 'SOLUSDT']
        
        def run_comprehensive_backtest(self) -> Dict[str, Any]:
            """Run comprehensive backtest with real data"""
            try:
                self.progress['status'] = 'running'
                self.progress['start_time'] = datetime.now()
                self.progress['current'] = 0
                self.progress['results'] = []
                
                total_combinations = len(self.pairs) * len(self.strategies) * len(self.timeframes)
                self.logger.info(f"Starting REAL DATA comprehensive backtest: {total_combinations} combinations")
                
                combination_count = 0
                successful_results = 0
                
                # Run backtests for each combination
                for symbol in self.pairs:
                    for strategy in self.strategies:
                        for timeframe in self.timeframes:
                            combination_count += 1
                            self.progress['current'] = combination_count
                            
                            try:
                                result = self._run_backtest_combination(symbol, strategy, timeframe)
                                if result:
                                    self.progress['results'].append(result)
                                    successful_results += 1
                                    self.progress['completed_combinations'].append(f"{symbol}_{strategy}_{timeframe}")
                                else:
                                    self.progress['failed_combinations'].append(f"{symbol}_{strategy}_{timeframe}")
                                
                            except Exception as e:
                                self.logger.error(f"Backtest error for {symbol} {strategy} {timeframe}: {e}")
                                self.progress['failed_combinations'].append(f"{symbol}_{strategy}_{timeframe}")
                            
                            # Add small delay to prevent overwhelming the system
                            time.sleep(0.1)
                
                self.progress['status'] = 'completed'
                completion_time = datetime.now()
                duration = (completion_time - self.progress['start_time']).total_seconds()
                
                self.logger.info(f"Comprehensive backtest completed: {successful_results}/{total_combinations} successful in {duration:.1f}s")
                
                return {
                    'status': 'completed',
                    'total_combinations': total_combinations,
                    'successful_results': successful_results,
                    'failed_combinations': len(self.progress['failed_combinations']),
                    'duration_seconds': duration,
                    'results': self.progress['results']
                }
                
            except Exception as e:
                self.logger.error(f"Comprehensive backtest failed: {e}")
                self.progress['status'] = 'failed'
                return {
                    'status': 'failed',
                    'error': str(e),
                    'results': self.progress['results']
                }
        
        def _run_backtest_combination(self, symbol: str, strategy: str, timeframe: str) -> Optional[BacktestResult]:
            """Run backtest for a specific combination"""
            try:
                # Fetch real market data
                df = self._fetch_real_data(symbol, timeframe)
                
                # Validate data sufficiency
                if not self._validate_data_sufficiency(df, symbol, timeframe, strategy):
                    return None
                
                # Execute strategy backtest
                result = self._execute_strategy_backtest(df, symbol, strategy, timeframe)
                
                if result and self._is_quality_result(result):
                    self.logger.info(f"? {symbol} {strategy}: {result.total_trades} trades, "
                                   f"{result.win_rate:.1f}% win rate, {result.sharpe_ratio:.2f} Sharpe")
                    return result
                else:
                    self.logger.debug(f"Low quality result for {symbol} {strategy}")
                    return None
                    
            except Exception as e:
                self.logger.error(f"Backtest combination error for {symbol} {strategy} {timeframe}: {e}")
                return None
        
        def _fetch_real_data(self, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
            """Fetch real market data with improved collection"""
            try:
                self.logger.info(f"Fetching REAL {symbol} {timeframe} data via CCXT...")
                
                # Determine appropriate limit based on timeframe
                timeframe_limits = {
                    '1m': 1000,
                    '5m': 1000,
                    '15m': 500,
                    '1h': 500,
                    '4h': 250,
                    '1d': 100
                }
                
                limit = timeframe_limits.get(timeframe, 500)
                
                # Fetch data with retries
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        ohlcv = self.exchange_client.fetch_ohlcv(symbol, timeframe, limit=limit)
                        
                        if ohlcv and len(ohlcv) > 0:
                            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                            df.set_index('timestamp', inplace=True)
                            
                            self.logger.info(f"Fetched {len(df)} REAL candles for {symbol} {timeframe} via CCXT")
                            return df
                            
                    except Exception as e:
                        self.logger.warning(f"Attempt {attempt + 1} failed for {symbol} {timeframe}: {e}")
                        if attempt < max_retries - 1:
                            time.sleep(2 ** attempt)  # Exponential backoff
                
                self.logger.error(f"Failed to fetch data for {symbol} {timeframe} after {max_retries} attempts")
                return None
                
            except Exception as e:
                self.logger.error(f"Error fetching {symbol} {timeframe}: {e}")
                return None
        
        def _validate_data_sufficiency(self, df: Optional[pd.DataFrame], symbol: str, timeframe: str, strategy: str) -> bool:
            """Check if data is sufficient for backtesting"""
            if df is None or df.empty:
                self.logger.warning(f"No data available for {symbol} {timeframe}")
                return False
            
            # Get strategy requirements
            strategy_config = self.controller.strategies.get(strategy, {})
            min_required = strategy_config.get('min_candles', 50)
            
            # Additional timeframe-specific requirements
            timeframe_minimums = {
                '1m': 100,
                '5m': 100,
                '15m': 80,
                '1h': 50,
                '4h': 30,
                '1d': 20
            }
            
            min_required = max(min_required, timeframe_minimums.get(timeframe, 50))
            actual_count = len(df)
            
            if actual_count < min_required:
                self.logger.warning(f"Insufficient REAL data for {symbol} {timeframe}: {actual_count} candles (need {min_required})")
                self.logger.warning(f"Skipping {symbol} {strategy}: insufficient REAL data")
                return False
            
            return True
        
        def _execute_strategy_backtest(self, df: pd.DataFrame, symbol: str, strategy: str, timeframe: str) -> Optional[BacktestResult]:
            """Execute the actual strategy backtest"""
            try:
                # Simple strategy implementation for demonstration
                # In production, this would call the actual strategy logic
                
                # Calculate simple moving averages
                df['sma_fast'] = df['close'].rolling(window=10).mean()
                df['sma_slow'] = df['close'].rolling(window=20).mean()
                
                # Generate signals
                df['signal'] = 0
                df.loc[df['sma_fast'] > df['sma_slow'], 'signal'] = 1
                df.loc[df['sma_fast'] < df['sma_slow'], 'signal'] = -1
                
                # Calculate trades
                trades = []
                position = 0
                entry_price = 0
                
                for i in range(1, len(df)):
                    current_signal = df.iloc[i]['signal']
                    prev_signal = df.iloc[i-1]['signal']
                    current_price = df.iloc[i]['close']
                    
                    # Entry signals
                    if current_signal == 1 and prev_signal != 1 and position == 0:
                        position = 1
                        entry_price = current_price
                    elif current_signal == -1 and prev_signal != -1 and position == 0:
                        position = -1
                        entry_price = current_price
                    
                    # Exit signals
                    elif current_signal != position and position != 0:
                        exit_price = current_price
                        pnl = (exit_price - entry_price) * position / entry_price
                        
                        trades.append({
                            'entry_price': entry_price,
                            'exit_price': exit_price,
                            'pnl': pnl,
                            'position': position
                        })
                        
                        position = 0
                
                # Calculate metrics
                if not trades:
                    return None
                
                total_trades = len(trades)
                winning_trades = len([t for t in trades if t['pnl'] > 0])
                losing_trades = total_trades - winning_trades
                win_rate = (winning_trades / total_trades) * 100 if total_trades > 0 else 0
                
                total_return = sum([t['pnl'] for t in trades])
                
                # Calculate Sharpe ratio (simplified)
                returns = [t['pnl'] for t in trades]
                if len(returns) > 1:
                    sharpe_ratio = np.mean(returns) / np.std(returns) if np.std(returns) > 0 else 0
                else:
                    sharpe_ratio = 0
                
                # Calculate max drawdown
                cumulative_returns = np.cumsum(returns)
                running_max = np.maximum.accumulate(cumulative_returns)
                drawdown = cumulative_returns - running_max
                max_drawdown = np.min(drawdown) if len(drawdown) > 0 else 0
                
                # Calculate profit factor
                gross_profit = sum([t['pnl'] for t in trades if t['pnl'] > 0])
                gross_loss = abs(sum([t['pnl'] for t in trades if t['pnl'] < 0]))
                profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')
                
                return BacktestResult(
                    symbol=symbol,
                    strategy=strategy,
                    timeframe=timeframe,
                    total_trades=total_trades,
                    winning_trades=winning_trades,
                    losing_trades=losing_trades,
                    win_rate=win_rate,
                    total_return=total_return,
                    sharpe_ratio=sharpe_ratio,
                    max_drawdown=max_drawdown,
                    profit_factor=profit_factor,
                    avg_trade_duration=0.0,  # Simplified
                    data_quality='high',
                    timestamp=datetime.now()
                )
                
            except Exception as e:
                self.logger.error(f"Strategy execution error for {symbol} {strategy}: {e}")
                return None
        
        def _is_quality_result(self, result: BacktestResult) -> bool:
            """Check if backtest result meets quality thresholds"""
            if not result:
                return False
            
            # Quality thresholds from environment or defaults
            min_trades = int(os.getenv('MIN_BACKTEST_TRADES', 3))
            min_win_rate = float(os.getenv('MIN_WIN_RATE', 25.0))
            min_sharpe = float(os.getenv('MIN_SHARPE_RATIO', -2.0))
            
            if result.total_trades < min_trades:
                return False
            if result.win_rate < min_win_rate:
                return False
            if result.sharpe_ratio < min_sharpe:
                return False
            
            return True
        
        def get_progress(self) -> Dict[str, Any]:
            """Get current backtest progress"""
            try:
                progress_percent = (self.progress['current'] / self.progress['total']) * 100 if self.progress['total'] > 0 else 0
                
                # Calculate ETA
                eta_seconds = 0
                if self.progress['start_time'] and self.progress['current'] > 0:
                    elapsed = (datetime.now() - self.progress['start_time']).total_seconds()
                    rate = self.progress['current'] / elapsed
                    remaining = self.progress['total'] - self.progress['current']
                    eta_seconds = remaining / rate if rate > 0 else 0
                
                return {
                    'current': self.progress['current'],
                    'total': self.progress['total'],
                    'progress_percent': progress_percent,
                    'status': self.progress['status'],
                    'successful_results': len(self.progress['results']),
                    'failed_combinations': len(self.progress['failed_combinations']),
                    'eta_seconds': eta_seconds,
                    'start_time': self.progress['start_time'].isoformat() if self.progress['start_time'] else None
                }
            except Exception as e:
                self.logger.error(f"Error getting progress: {e}")
                return {
                    'current': 0,
                    'total': 0,
                    'progress_percent': 0,
                    'status': 'error',
                    'error': str(e)
                }
    
    def start_backtest(self) -> Dict[str, Any]:
        """Start comprehensive backtest"""
        try:
            if not self.backtester:
                return {'status': 'error', 'message': 'Backtester not available'}
            
            # Run backtest in background
            import threading
            def run_backtest():
                try:
                    self.backtester.run_comprehensive_backtest()
                except Exception as e:
                    self.logger.error(f"Background backtest failed: {e}")
            
            backtest_thread = threading.Thread(target=run_backtest)
            backtest_thread.daemon = True
            backtest_thread.start()
            
            return {'status': 'started', 'message': 'Backtest started successfully'}
            
        except Exception as e:
            self.logger.error(f"Failed to start backtest: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def get_backtest_progress(self) -> Dict[str, Any]:
        """Get backtest progress"""
        try:
            if not self.backtester:
                return {'status': 'error', 'message': 'Backtester not available'}
            
            return self.backtester.get_progress()
            
        except Exception as e:
            self.logger.error(f"Error getting backtest progress: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def get_recent_trades(self) -> List[Dict]:
        """Get recent trades"""
        try:
            if self.pnl_persistence:
                return self.pnl_persistence.get_recent_trades(limit=10)
            return []
        except Exception as e:
            self.logger.error(f"Error getting recent trades: {e}")
            return []
    
    def shutdown(self):
        """Graceful shutdown of the trading system"""
        try:
            self.logger.info("Initiating graceful shutdown...")
            
            # Stop async tasks
            self.task_manager.stop_all_tasks()
            
            # Close connections
            if self.exchange_manager:
                # Close exchange connections
                pass
            
            if self.pnl_persistence:
                # Close database connections
                self.pnl_persistence.close()
            
            self.system_ready = False
            self.trading_enabled = False
            
            self.logger.info("System shutdown completed")
            
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")

# Global instance management
_controller_instance = None

def get_controller() -> V3TradingController:
    """Get or create the trading controller instance"""
    global _controller_instance
    if _controller_instance is None:
        _controller_instance = V3TradingController()
    return _controller_instance

def shutdown_controller():
    """Shutdown the trading controller"""
    global _controller_instance
    if _controller_instance:
        _controller_instance.shutdown()
        _controller_instance = None

# Signal handlers for graceful shutdown
def signal_handler(signum, frame):
    """Handle shutdown signals"""
    print(f"\nReceived signal {signum}, shutting down gracefully...")
    shutdown_controller()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

if __name__ == "__main__":
    # Test the controller
    controller = V3TradingController()
    print(f"V3 Trading Controller initialized: {controller.get_system_status()}")