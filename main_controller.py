#!/usr/bin/env python3
"""
V3 MAIN CONTROLLER - FIXED WITH DATABASE AND ASYNC IMPROVEMENTS
===============================================================
FIXES APPLIED:
- Database connection pooling and proper lifecycle management
- Asyncio/Threading synchronization improvements
- Memory leak prevention
- Enhanced error handling and recovery
- Keep existing API rotation system (api_rotation_manager.py)
- REMOVED: Fake trade simulation (_simulate_trade function)
"""
import numpy as np
from binance.client import Client
import asyncio
import logging
import json
import os
import psutil
import random
from typing import List, Dict, Tuple, Optional
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time
import uuid
from collections import defaultdict, deque  # Changed to deque for memory management
import pandas as pd
import sqlite3
from pathlib import Path
from threading import Thread, Lock, Event
import traceback
import contextlib
from concurrent.futures import ThreadPoolExecutor
import weakref
import gc
import signal
import sys
import queue
import threading

load_dotenv()

# Keep your existing API rotation system - it's already excellent
from api_rotation_manager import get_api_key, report_api_result
from pnl_persistence import PnLPersistence

class DatabaseManager:
    """Enhanced database manager with connection pooling"""
    
    def __init__(self, db_path: str, max_connections: int = 5):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._pool = queue.Queue(maxsize=max_connections)
        self._lock = threading.Lock()
        self._max_connections = max_connections
        self._active_connections = 0
        
    def _create_connection(self) -> sqlite3.Connection:
        """Create a new database connection with proper settings"""
        conn = sqlite3.connect(
            self.db_path,
            timeout=30.0,
            check_same_thread=False,
            isolation_level='DEFERRED'
        )
        # Enable WAL mode for better concurrency
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA synchronous=NORMAL') 
        conn.execute('PRAGMA cache_size=10000')
        conn.execute('PRAGMA temp_store=MEMORY')
        return conn
        
    @contextlib.contextmanager
    def get_connection(self):
        """Get a database connection from the pool"""
        conn = None
        try:
            # Try to get connection from pool
            try:
                conn = self._pool.get_nowait()
            except queue.Empty:
                # Create new connection if pool is empty and we haven't hit max
                with self._lock:
                    if self._active_connections < self._max_connections:
                        conn = self._create_connection()
                        self._active_connections += 1
                    else:
                        # Wait for connection from pool
                        conn = self._pool.get(timeout=10)
            
            yield conn
            
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            raise e
        finally:
            if conn:
                try:
                    conn.commit()
                    # Return connection to pool
                    self._pool.put_nowait(conn)
                except queue.Full:
                    # Pool is full, close connection
                    conn.close()
                    with self._lock:
                        self._active_connections -= 1
                except:
                    conn.close()
                    with self._lock:
                        self._active_connections -= 1
    
    def initialize_schema(self, schema_sql: str):
        """Initialize database schema"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.executescript(schema_sql)
            conn.commit()
    
    def close_all(self):
        """Close all connections in the pool"""
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
            except:
                break
        self._active_connections = 0

class AsyncTaskManager:
    """Enhanced async task manager with proper lifecycle and error handling"""
    
    def __init__(self):
        self._tasks: Dict[str, asyncio.Task] = {}
        self._cleanup_lock = asyncio.Lock()
        self._shutdown_event = asyncio.Event()
        self._logger = logging.getLogger(f"{__name__}.AsyncTaskManager")
        
    async def create_task(self, coro, name: str, error_callback=None):
        """Create and track an async task with error handling"""
        async def wrapped_coro():
            try:
                return await coro
            except asyncio.CancelledError:
                self._logger.info(f"Task {name} was cancelled")
                raise
            except Exception as e:
                self._logger.error(f"Task {name} failed: {e}", exc_info=True)
                if error_callback:
                    try:
                        await error_callback(e)
                    except Exception as cb_error:
                        self._logger.error(f"Error callback for {name} failed: {cb_error}")
                raise
        
        async with self._cleanup_lock:
            # Cancel existing task with same name
            if name in self._tasks and not self._tasks[name].done():
                self._tasks[name].cancel()
                try:
                    await self._tasks[name]
                except asyncio.CancelledError:
                    pass
            
            # Create new task
            task = asyncio.create_task(wrapped_coro(), name=name)
            self._tasks[name] = task
            
            # Clean up completed tasks
            completed_tasks = [k for k, v in self._tasks.items() if v.done()]
            for k in completed_tasks:
                del self._tasks[k]
                
        return task
    
    async def cancel_task(self, name: str):
        """Cancel a specific task"""
        async with self._cleanup_lock:
            if name in self._tasks and not self._tasks[name].done():
                self._tasks[name].cancel()
                try:
                    await self._tasks[name]
                except asyncio.CancelledError:
                    pass
                del self._tasks[name]
    
    async def shutdown_all(self, timeout: float = 5.0):
        """Shutdown all tasks gracefully"""
        self._shutdown_event.set()
        
        async with self._cleanup_lock:
            if not self._tasks:
                return
            
            # Cancel all tasks
            for task in self._tasks.values():
                if not task.done():
                    task.cancel()
            
            # Wait for tasks to complete
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._tasks.values(), return_exceptions=True),
                    timeout=timeout
                )
            except asyncio.TimeoutError:
                self._logger.warning(f"Some tasks didn't complete within {timeout}s timeout")
            
            self._tasks.clear()

class EnhancedComprehensiveMultiTimeframeBacktester:
    """Enhanced backtester with proper resource management"""
    
    def __init__(self, controller=None):
        self.controller = weakref.ref(controller) if controller else None
        self.logger = logging.getLogger(f"{__name__}.Backtester")
        
        # Database manager
        self.db_manager = DatabaseManager('data/comprehensive_backtest.db')
        self._initialize_database()
        
        # Configuration
        self.all_pairs = [
            'BTCUSD', 'BTCUSDT', 'BTCUSDC', 'ETHUSDT', 'ETHUSD', 'ETHUSDC', 'ETHBTC',
            'BNBUSD', 'BNBUSDT', 'BNBBTC', 'ADAUSD', 'ADAUSDC', 'ADAUSDT', 'ADABTC',
            'SOLUSD', 'SOLUSDC', 'SOLUSDT', 'SOLBTC', 'XRPUSD', 'XRPUSDT',
            'DOGEUSD', 'DOGEUSDT', 'AVAXUSD', 'AVAXUSDT', 'SHIBUSD', 'SHIBUSDT',
            'DOTUSDT', 'LINKUSD', 'LINKUSDT', 'LTCUSD', 'LTCUSDT', 'UNIUSD', 'UNIUSDT',
            'ATOMUSD', 'ATOMUSDT', 'ALGOUSD', 'ALGOUSDT', 'VETUSD', 'VETUSDT'
        ]
        
        self.timeframes = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']
        self.mtf_combinations = [
            (['1m', '5m', '15m'], 'scalping'),
            (['5m', '15m', '30m'], 'short_term'),
            (['15m', '1h', '4h'], 'intraday'),
            (['1h', '4h', '1d'], 'swing'),
            (['4h', '1d', '1w'], 'position'),
            (['1d', '1w', '1M'], 'long_term')
        ]
        
        # Progress tracking with thread safety
        self._progress_lock = threading.Lock()
        self.total_combinations = len(self.all_pairs) * len(self.mtf_combinations)
        self.completed = 0
        self.current_symbol = None
        self.current_strategy = None
        self.start_time = None
        self.status = 'not_started'
        self.error_count = 0
        self.max_errors = 50
        
        # Initialize Binance client using your existing API rotation
        self.client = self._initialize_binance_client()
        
        self.logger.info(f"Backtester initialized: {len(self.all_pairs)} pairs, {self.total_combinations} combinations")
    
    def _initialize_binance_client(self) -> Optional[Client]:
        """Initialize Binance client using existing API rotation"""
        try:
            binance_creds = get_api_key('binance')
            if binance_creds:
                return Client(
                    binance_creds['api_key'], 
                    binance_creds['api_secret'], 
                    testnet=True
                )
        except Exception as e:
            self.logger.warning(f"Failed to initialize Binance client: {e}")
        return None
    
    def _initialize_database(self):
        """Initialize database schema"""
        schema = '''
        CREATE TABLE IF NOT EXISTS historical_backtests (
            id INTEGER PRIMARY KEY,
            symbol TEXT,
            timeframes TEXT,
            strategy_type TEXT,
            start_date TEXT,
            end_date TEXT,
            total_candles INTEGER,
            total_trades INTEGER,
            winning_trades INTEGER,
            win_rate REAL,
            total_return_pct REAL,
            max_drawdown REAL,
            sharpe_ratio REAL,
            avg_trade_duration_hours REAL,
            volatility REAL,
            best_trade_pct REAL,
            worst_trade_pct REAL,
            confluence_strength REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS backtest_progress (
            id INTEGER PRIMARY KEY,
            status TEXT,
            current_symbol TEXT,
            current_strategy TEXT,
            completed INTEGER,
            total INTEGER,
            error_count INTEGER DEFAULT 0,
            start_time TEXT,
            completion_time TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_backtests_symbol ON historical_backtests(symbol);
        CREATE INDEX IF NOT EXISTS idx_backtests_strategy ON historical_backtests(strategy_type);
        CREATE INDEX IF NOT EXISTS idx_backtests_sharpe ON historical_backtests(sharpe_ratio);
        '''
        self.db_manager.initialize_schema(schema)
    
    def cleanup(self):
        """Cleanup resources"""
        try:
            if hasattr(self, 'db_manager'):
                self.db_manager.close_all()
        except Exception as e:
            self.logger.error(f"Cleanup error: {e}")

class V3TradingController:
    """V3 Trading Controller with enhanced database and async handling"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Validate configuration
        if not self._validate_basic_config():
            raise ValueError("Configuration validation failed")
        
        # Initialize managers
        self.task_manager = AsyncTaskManager()
        self.db_manager = DatabaseManager('data/trading_metrics.db')
        self._initialize_database()
        
        # Thread-safe state management
        self._state_lock = threading.Lock()
        self._shutdown_event = threading.Event()
        
        # Initialize system state
        self.is_running = False
        self.is_initialized = False
        self.initialization_progress = 0
        
        # Initialize persistence system
        self.pnl_persistence = PnLPersistence()
        
        # Load persistent data
        self.metrics = self._load_persistent_metrics()
        
        # Initialize data structures with size limits to prevent memory leaks
        self.open_positions = {}
        self.recent_trades = deque(maxlen=100)  # Prevent unlimited growth
        self.top_strategies = []
        self.ml_trained_strategies = []
        
        # Progress tracking
        self.backtest_progress = self._initialize_backtest_progress()
        
        # System data
        self.external_data_status = self._initialize_external_data()
        self.scanner_data = {'active_pairs': 0, 'opportunities': 0, 'best_opportunity': 'None', 'confidence': 0}
        self.system_resources = {'cpu_usage': 0.0, 'memory_usage': 0.0, 'api_calls_today': 0, 'data_points_processed': 0}
        
        # Configuration
        self.testnet_mode = os.getenv('TESTNET', 'true').lower() == 'true'
        self.trading_mode = os.getenv('DEFAULT_TRADING_MODE', 'PAPER_TRADING')
        self.max_positions = int(os.getenv('MAX_TOTAL_POSITIONS', '3'))
        
        # Components (lazy initialization)
        self.ai_brain = None
        self.trading_engine = None
        self.external_data_collector = None
        self.comprehensive_backtester = None
        
        # Thread executor for blocking operations
        self._executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="V3Controller")
        
        self.logger.info("Enhanced V3 Trading Controller initialized - REAL DATA ONLY")
    
    def _validate_basic_config(self) -> bool:
        """Basic configuration validation"""
        required_vars = ['BINANCE_API_KEY_1', 'BINANCE_API_SECRET_1']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        if missing_vars:
            self.logger.error(f"Missing required environment variables: {missing_vars}")
            return False
            
        # Validate numeric configs
        try:
            max_pos = int(os.getenv('MAX_TOTAL_POSITIONS', '3'))
            if not 1 <= max_pos <= 50:
                self.logger.error("MAX_TOTAL_POSITIONS must be between 1 and 50")
                return False
                
            trade_amount = float(os.getenv('TRADE_AMOUNT_USDT', '10.0'))
            if trade_amount <= 0:
                self.logger.error("TRADE_AMOUNT_USDT must be positive")
                return False
                
        except ValueError as e:
            self.logger.error(f"Configuration validation error: {e}")
            return False
            
        return True
    
    def _initialize_database(self):
        """Initialize trading metrics database"""
        schema = '''
        CREATE TABLE IF NOT EXISTS trading_metrics (
            id INTEGER PRIMARY KEY,
            key TEXT UNIQUE,
            value REAL,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS trade_history (
            id INTEGER PRIMARY KEY,
            symbol TEXT,
            side TEXT,
            quantity REAL,
            entry_price REAL,
            exit_price REAL,
            pnl REAL,
            timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
            strategy TEXT,
            confidence REAL
        );
        
        CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trade_history(timestamp);
        CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trade_history(symbol);
        '''
        self.db_manager.initialize_schema(schema)
    
    def _load_persistent_metrics(self) -> Dict:
        """Load persistent metrics with error handling"""
        try:
            saved_metrics = self.pnl_persistence.load_metrics()
        except Exception as e:
            self.logger.warning(f"Failed to load PnL persistence: {e}")
            saved_metrics = {}
        
        # Load additional metrics from database
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT key, value FROM trading_metrics')
                db_metrics = {row[0]: row[1] for row in cursor.fetchall()}
                saved_metrics.update(db_metrics)
        except Exception as e:
            self.logger.warning(f"Failed to load metrics from database: {e}")
        
        return {
            'active_positions': int(saved_metrics.get('active_positions', 0)),
            'daily_trades': 0,
            'total_trades': int(saved_metrics.get('total_trades', 0)),
            'winning_trades': int(saved_metrics.get('winning_trades', 0)),
            'total_pnl': float(saved_metrics.get('total_pnl', 0.0)),
            'win_rate': float(saved_metrics.get('win_rate', 0.0)),
            'daily_pnl': 0.0,
            'best_trade': float(saved_metrics.get('best_trade', 0.0)),
            'cpu_usage': 0.0,
            'memory_usage': 0.0,
            'enable_ml_enhancement': True,
            'real_testnet_connected': False,
            'multi_pair_scanning': True,
            'api_rotation_active': True,
            'comprehensive_backtest_completed': bool(saved_metrics.get('comprehensive_backtest_completed', False)),
            'ml_training_completed': bool(saved_metrics.get('ml_training_completed', False))
        }
    
    def _initialize_backtest_progress(self) -> Dict:
        """Initialize backtesting progress tracking"""
        return {
            'status': 'not_started',
            'completed': 0,
            'total': 0,
            'current_symbol': None,
            'current_strategy': None,
            'progress_percent': 0,
            'eta_minutes': None,
            'error_count': 0
        }
    
    def _initialize_external_data(self) -> Dict:
        """Initialize external data status tracking"""
        return {
            'api_status': {
                'binance': True,
                'alpha_vantage': False,
                'news_api': False,
                'fred_api': False,
                'twitter_api': False,
                'reddit_api': False
            },
            'working_apis': 1,
            'total_apis': 6,
            'latest_data': {
                'market_sentiment': {'overall_sentiment': 0.0, 'bullish_indicators': 0, 'bearish_indicators': 0},
                'news_sentiment': {'articles_analyzed': 0, 'positive_articles': 0, 'negative_articles': 0},
                'economic_indicators': {'gdp_growth': 0.0, 'inflation_rate': 0.0, 'unemployment_rate': 0.0, 'interest_rate': 0.0},
                'social_media_sentiment': {'twitter_mentions': 0, 'reddit_posts': 0, 'overall_social_sentiment': 0.0}
            }
        }
    
    async def initialize_system(self) -> bool:
        """Initialize V3 system with enhanced error handling"""
        try:
            self.logger.info("Initializing Enhanced V3 Trading System")
            
            self.initialization_progress = 20
            await self._initialize_trading_components()
            
            self.initialization_progress = 60
            await self._initialize_backtester()
            
            self.initialization_progress = 80
            await self._load_existing_strategies()
            
            # Start background tasks
            await self.task_manager.create_task(
                self._background_update_loop(),
                "background_updates",
                self._handle_background_error
            )
            
            self.initialization_progress = 100
            self.is_initialized = True
            
            self.logger.info("Enhanced V3 System initialized successfully!")
            return True
            
        except Exception as e:
            self.logger.error(f"System initialization failed: {e}", exc_info=True)
            return False
    
    async def _handle_background_error(self, error: Exception):
        """Handle background task errors"""
        self.logger.error(f"Background task error: {error}")
        # Restart background tasks after a delay
        await asyncio.sleep(10)
        await self.task_manager.create_task(
            self._background_update_loop(),
            "background_updates",
            self._handle_background_error
        )
    
    async def _initialize_trading_components(self):
        """Initialize trading components"""
        try:
            # Initialize external data collector
            try:
                from external_data_collector import ExternalDataCollector
                self.external_data_collector = ExternalDataCollector()
                print("External data collector initialized")
            except:
                print("External data collector not available")
            
            # Initialize AI Brain
            try:
                from advanced_ml_engine import AdvancedMLEngine
                self.ai_brain = AdvancedMLEngine(
                    config={'real_data_mode': True, 'testnet': self.testnet_mode},
                    credentials={'binance_testnet': self.testnet_mode},
                    test_mode=False
                )
                print("AI Brain initialized")
            except Exception as e:
                print(f"AI Brain initialization failed: {e}")
            
            # Initialize trading engine
            try:
                from intelligent_trading_engine import IntelligentTradingEngine
                self.trading_engine = IntelligentTradingEngine(
                    data_manager=None,
                    data_collector=self.external_data_collector,
                    market_analyzer=None,
                    ml_engine=self.ai_brain
                )
                
                if hasattr(self.trading_engine, 'client') and self.trading_engine.client:
                    try:
                        ticker = self.trading_engine.client.get_symbol_ticker(symbol="BTCUSDT")
                        current_btc = float(ticker['price'])
                        print(f"Real Binance connection: ${current_btc:,.2f} BTC")
                        self.metrics['real_testnet_connected'] = True
                    except:
                        self.metrics['real_testnet_connected'] = False
                        
            except Exception as e:
                print(f"Trading engine initialization failed: {e}")
            
        except Exception as e:
            print(f"Component initialization error: {e}")
    
    async def _initialize_backtester(self):
        """Initialize comprehensive backtester"""
        try:
            self.comprehensive_backtester = EnhancedComprehensiveMultiTimeframeBacktester(controller=self)
            print("Comprehensive backtester initialized")
        except Exception as e:
            print(f"Backtester initialization error: {e}")
    
    async def _load_existing_strategies(self):
        """Load existing strategies from database"""
        try:
            if os.path.exists('data/comprehensive_backtest.db'):
                conn = sqlite3.connect('data/comprehensive_backtest.db')
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT symbol, timeframes, strategy_type, total_return_pct, win_rate, sharpe_ratio, total_trades
                    FROM historical_backtests 
                    WHERE total_trades >= 20 AND sharpe_ratio > 1.0
                    ORDER BY sharpe_ratio DESC
                    LIMIT 15
                ''')
                
                strategies = cursor.fetchall()
                self.top_strategies = []
                self.ml_trained_strategies = []
                
                for strategy in strategies:
                    strategy_data = {
                        'name': f"{strategy[2]}_MTF",
                        'symbol': strategy[0],
                        'timeframes': strategy[1],
                        'strategy_type': strategy[2],
                        'return_pct': strategy[3],
                        'win_rate': strategy[4],
                        'sharpe_ratio': strategy[5],
                        'total_trades': strategy[6],
                        'expected_win_rate': strategy[4]
                    }
                    
                    self.top_strategies.append(strategy_data)
                    
                    if strategy[4] > 60 and strategy[5] > 1.2:
                        self.ml_trained_strategies.append(strategy_data)
                
                conn.close()
                
                if len(self.ml_trained_strategies) > 0:
                    self.metrics['ml_training_completed'] = True
                
                print(f"Loaded {len(self.top_strategies)} strategies, {len(self.ml_trained_strategies)} ML-trained")
            
        except Exception as e:
            print(f"Strategy loading error: {e}")
    
    async def _background_update_loop(self):
        """Background loop for updating metrics and data - NO FAKE TRADES"""
        while not self._shutdown_event.is_set():
            try:
                await self._update_real_time_data()
                await asyncio.sleep(5)
            except Exception as e:
                self.logger.error(f"Background update error: {e}")
                await asyncio.sleep(10)
    
    async def _update_real_time_data(self):
        """Update real-time data for dashboard - REAL DATA ONLY"""
        try:
            # Update system resources
            self.system_resources['cpu_usage'] = psutil.cpu_percent(interval=0.1)
            self.system_resources['memory_usage'] = psutil.virtual_memory().percent
            
            # Update external data status - check real API status when possible
            for api in self.external_data_status['api_status']:
                if api != 'binance':
                    # For now, set to False until real API checks are implemented
                    self.external_data_status['api_status'][api] = False
            
            self.external_data_status['working_apis'] = sum(self.external_data_status['api_status'].values())
            
            # Update scanner data - only use real scanner results
            if self.trading_engine and hasattr(self.trading_engine, 'get_scanner_data'):
                try:
                    scanner_result = self.trading_engine.get_scanner_data()
                    if scanner_result:
                        self.scanner_data.update(scanner_result)
                except:
                    # Keep default values if scanner not available
                    pass
            
            # NO FAKE TRADE SIMULATION - REMOVED _simulate_trade() CALL
            # Only real trades from actual trading engine will update metrics
                
        except Exception as e:
            self.logger.error(f"Real-time update error: {e}")
    
    def _is_trading_allowed(self) -> bool:
        """Check if trading is currently allowed"""
        if self.backtest_progress['status'] == 'in_progress':
            return False
        if not self.metrics.get('comprehensive_backtest_completed', False):
            return False
        if not self.metrics.get('ml_training_completed', False):
            return False
        return True
    
    # REMOVED: _simulate_trade() function completely - no more fake trades
    
    def record_real_trade(self, trade_data: Dict):
        """Record an actual trade from the trading engine"""
        try:
            # Validate required fields
            required_fields = ['symbol', 'side', 'quantity', 'entry_price', 'exit_price', 'pnl']
            if not all(field in trade_data for field in required_fields):
                self.logger.error(f"Invalid trade data: missing required fields")
                return False
            
            pnl = float(trade_data['pnl'])
            
            # Update metrics with real trade
            self.metrics['total_trades'] += 1
            self.metrics['daily_trades'] += 1
            if pnl > 0:
                self.metrics['winning_trades'] += 1
            
            self.metrics['total_pnl'] += pnl
            self.metrics['daily_pnl'] += pnl
            self.metrics['win_rate'] = (self.metrics['winning_trades'] / self.metrics['total_trades']) * 100
            
            if pnl > self.metrics['best_trade']:
                self.metrics['best_trade'] = pnl
            
            # Add to recent trades
            trade = {
                'id': len(self.recent_trades) + 1,
                'symbol': trade_data['symbol'],
                'side': trade_data['side'],
                'quantity': trade_data['quantity'],
                'entry_price': trade_data['entry_price'],
                'exit_price': trade_data['exit_price'],
                'profit_loss': pnl,
                'profit_pct': (pnl / (trade_data['entry_price'] * trade_data['quantity'])) * 100,
                'is_win': pnl > 0,
                'confidence': trade_data.get('confidence', 0),
                'timestamp': datetime.now().isoformat(),
                'source': trade_data.get('strategy', 'REAL_TRADE'),
                'session_id': 'V3_REAL_SESSION',
                'exit_time': datetime.now().isoformat(),
                'hold_duration_human': trade_data.get('duration', 'Unknown'),
                'exit_reason': trade_data.get('exit_reason', 'Manual')
            }
            
            self.recent_trades.append(trade)
            
            # Save to database
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO trade_history 
                    (symbol, side, quantity, entry_price, exit_price, pnl, strategy, confidence)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    trade_data['symbol'],
                    trade_data['side'],
                    trade_data['quantity'],
                    trade_data['entry_price'],
                    trade_data['exit_price'],
                    pnl,
                    trade_data.get('strategy', 'MANUAL'),
                    trade_data.get('confidence', 0)
                ))
            
            # Save metrics
            self.save_current_metrics()
            
            self.logger.info(f"Real trade recorded: {trade_data['side']} {trade_data['symbol']} -> ${pnl:+.2f}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to record real trade: {e}")
            return False
    
    def save_current_metrics(self):
        """Thread-safe metrics saving"""
        with self._state_lock:
            try:
                # Save to database
                with self.db_manager.get_connection() as conn:
                    cursor = conn.cursor()
                    for key, value in self.metrics.items():
                        if isinstance(value, (int, float)):
                            cursor.execute(
                                'INSERT OR REPLACE INTO trading_metrics (key, value) VALUES (?, ?)',
                                (key, float(value))
                            )
                
                # Also save via PnL persistence
                try:
                    self.pnl_persistence.save_metrics(self.metrics)
                except Exception as e:
                    self.logger.warning(f"PnL persistence save failed: {e}")
                
            except Exception as e:
                self.logger.error(f"Failed to save metrics: {e}")
    
    async def start_trading(self):
        """Start trading operations"""
        try:
            if not self._is_trading_allowed():
                return {"success": False, "error": "Trading not allowed - complete backtesting and ML training first"}
            
            self.is_running = True
            self.logger.info("Trading started")
            return {"success": True, "message": "Trading started"}
            
        except Exception as e:
            self.logger.error(f"Failed to start trading: {e}")
            return {"success": False, "error": str(e)}
    
    async def stop_trading(self):
        """Stop trading operations"""
        try:
            self.is_running = False
            self.logger.info("Trading stopped")
            return {"success": True, "message": "Trading stopped"}
            
        except Exception as e:
            self.logger.error(f"Failed to stop trading: {e}")
            return {"success": False, "error": str(e)}
    
    async def shutdown(self):
        """Enhanced shutdown with proper cleanup"""
        self.logger.info("Starting enhanced shutdown sequence")
        
        try:
            # Set shutdown flag
            self._shutdown_event.set()
            
            # Stop trading
            if self.is_running:
                self.is_running = False
                await asyncio.sleep(1)
            
            # Shutdown task manager
            await self.task_manager.shutdown_all(timeout=10.0)
            
            # Save final state
            self.save_current_metrics()
            
            # Cleanup components
            if self.comprehensive_backtester:
                self.comprehensive_backtester.cleanup()
            
            # Close database connections
            self.db_manager.close_all()
            
            # Shutdown thread executor
            self._executor.shutdown(wait=True, timeout=5.0)
            
            self.logger.info("Enhanced shutdown completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}", exc_info=True)
    
    def __del__(self):
        """Cleanup on destruction"""
        try:
            if hasattr(self, 'db_manager'):
                self.db_manager.close_all()
            if hasattr(self, '_executor'):
                self._executor.shutdown(wait=False)
        except:
            pass
    
    def run_flask_app(self):
        """Run Flask app - basic implementation"""
        from flask import Flask, jsonify, render_template_string, request
        from flask_cors import CORS
        
        app = Flask(__name__)
        CORS(app)
        
        # Basic dashboard HTML
        dashboard_html = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>V3 Trading System - Real Data Only</title>
            <style>
                body { font-family: Arial; margin: 20px; background: #1a1a1a; color: white; }
                .container { max-width: 1200px; margin: 0 auto; }
                .notice { background: #1a4a1a; padding: 15px; border-radius: 5px; margin-bottom: 20px; border-left: 4px solid #4CAF50; }
                .status { padding: 10px; margin: 10px 0; border-radius: 5px; background: #333; }
                .metrics { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; }
                .metric { background: #333; padding: 15px; border-radius: 5px; }
                .metric h3 { margin: 0 0 10px 0; color: #4CAF50; }
                .metric .value { font-size: 24px; font-weight: bold; }
            </style>
            <script>
                setInterval(() => {
                    fetch('/api/status').then(r => r.json()).then(data => {
                        document.getElementById('status').innerHTML = data.status;
                        document.getElementById('metrics').innerHTML = 
                            Object.entries(data.metrics || {}).map(([k,v]) => 
                                `<div class="metric"><h3>${k}</h3><div class="value">${v}</div></div>`
                            ).join('');
                    });
                }, 2000);
            </script>
        </head>
        <body>
            <div class="container">
                <h1>V3 Trading System Dashboard</h1>
                <div class="notice">
                    <strong>REAL DATA ONLY:</strong> This system only displays actual trading results. 
                    No simulated or fake data is generated. PnL updates only from real trades.
                </div>
                <div id="status" class="status">Loading...</div>
                <div id="metrics" class="metrics"></div>
            </div>
        </body>
        </html>
        """
        
        @app.route('/')
        def dashboard():
            return render_template_string(dashboard_html)
        
        @app.route('/api/status')
        def api_status():
            return jsonify({
                'status': 'V3 System Running - Real Data Only',
                'initialized': self.is_initialized,
                'trading': self.is_running,
                'real_data_only': True,
                'simulation_disabled': True,
                'metrics': {
                    'Total PnL': f"${self.metrics['total_pnl']:+.2f}",
                    'Win Rate': f"{self.metrics['win_rate']:.1f}%",
                    'Total Trades': self.metrics['total_trades'],
                    'Daily Trades': self.metrics['daily_trades'],
                    'Best Trade': f"${self.metrics['best_trade']:+.2f}",
                    'CPU Usage': f"{self.system_resources['cpu_usage']:.1f}%",
                    'Memory Usage': f"{self.system_resources['memory_usage']:.1f}%"
                }
            })
        
        @app.route('/health')
        def health():
            return jsonify({
                'status': 'healthy', 
                'system': 'V3 Trading System',
                'mode': 'REAL_DATA_ONLY',
                'simulation': False
            })
        
        # Start Flask app
        port = int(os.getenv('FLASK_PORT', '8102'))
        print(f"Starting Flask app on port {port} - REAL DATA ONLY MODE")
        app.run(host='0.0.0.0', port=port, debug=False, threaded=True)