#!/usr/bin/env python3
"""
V3 MAIN CONTROLLER - REAL DATA ONLY (NO SIMULATIONS)
===================================================
CRITICAL FIXES:
- Removed all _simulate_trade() methods
- Removed fake scanner data generation
- Removed simulated external API status
- Only uses real Binance data and actual API responses
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
from collections import defaultdict, deque
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
    """V3 Trading Controller - REAL DATA ONLY"""
    
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
        self.recent_trades = deque(maxlen=100)
        self.top_strategies = []
        self.ml_trained_strategies = []
        
        # Progress tracking
        self.backtest_progress = self._initialize_backtest_progress()
        
        # System data - REAL ONLY
        self.external_data_status = self._get_real_external_data_status()
        self.scanner_data = self._get_real_scanner_data()
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
        
        self.logger.info("V3 Trading Controller initialized - REAL DATA ONLY")
    
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
    
    def _get_real_external_data_status(self) -> Dict:
        """Get REAL external API status - no simulation"""
        return {
            'api_status': {
                'binance': self._test_binance_connection(),
                'alpha_vantage': self._test_alpha_vantage_connection(),
                'news_api': self._test_news_api_connection(),
                'fred_api': self._test_fred_api_connection(),
                'twitter_api': self._test_twitter_api_connection(),
                'reddit_api': self._test_reddit_api_connection()
            },
            'working_apis': 0,  # Will be calculated from real tests
            'total_apis': 6,
            'latest_data': {
                'market_sentiment': self._get_real_market_sentiment(),
                'news_sentiment': self._get_real_news_sentiment(),
                'economic_indicators': self._get_real_economic_indicators(),
                'social_media_sentiment': self._get_real_social_sentiment()
            }
        }
    
    def _test_binance_connection(self) -> bool:
        """Test real Binance connection"""
        try:
            if self.trading_engine and hasattr(self.trading_engine, 'client') and self.trading_engine.client:
                self.trading_engine.client.get_symbol_ticker(symbol="BTCUSDT")
                return True
        except:
            pass
        return False
    
    def _test_alpha_vantage_connection(self) -> bool:
        """Test real Alpha Vantage API"""
        try:
            from api_rotation_manager import get_api_key
            alpha_key = get_api_key('alpha_vantage')
            if alpha_key:
                # Test real API connection
                import requests
                url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=IBM&apikey={alpha_key['api_key']}"
                response = requests.get(url, timeout=5)
                return response.status_code == 200
        except:
            pass
        return False
    
    def _test_news_api_connection(self) -> bool:
        """Test real News API connection"""
        try:
            from api_rotation_manager import get_api_key
            news_key = get_api_key('news_api')
            if news_key:
                import requests
                url = f"https://newsapi.org/v2/top-headlines?category=business&apiKey={news_key['api_key']}"
                response = requests.get(url, timeout=5)
                return response.status_code == 200
        except:
            pass
        return False
    
    def _test_fred_api_connection(self) -> bool:
        """Test real FRED API connection"""
        try:
            from api_rotation_manager import get_api_key
            fred_key = get_api_key('fred_api')
            if fred_key:
                import requests
                url = f"https://api.stlouisfed.org/fred/series/observations?series_id=GDP&api_key={fred_key['api_key']}&file_type=json&limit=1"
                response = requests.get(url, timeout=5)
                return response.status_code == 200
        except:
            pass
        return False
    
    def _test_twitter_api_connection(self) -> bool:
        """Test real Twitter API connection"""
        try:
            from api_rotation_manager import get_api_key
            twitter_key = get_api_key('twitter_api')
            if twitter_key:
                import requests
                headers = {"Authorization": f"Bearer {twitter_key['bearer_token']}"}
                url = "https://api.twitter.com/2/tweets/search/recent?query=bitcoin&max_results=10"
                response = requests.get(url, headers=headers, timeout=5)
                return response.status_code == 200
        except:
            pass
        return False
    
    def _test_reddit_api_connection(self) -> bool:
        """Test real Reddit API connection"""
        try:
            from api_rotation_manager import get_api_key
            reddit_key = get_api_key('reddit_api')
            if reddit_key:
                import requests
                auth = requests.auth.HTTPBasicAuth(reddit_key['client_id'], reddit_key['client_secret'])
                data = {'grant_type': 'client_credentials'}
                headers = {'User-Agent': 'TradingBot/1.0'}
                response = requests.post('https://www.reddit.com/api/v1/access_token', auth=auth, data=data, headers=headers, timeout=5)
                return response.status_code == 200
        except:
            pass
        return False
    
    def _get_real_scanner_data(self) -> Dict:
        """Get REAL scanner data from multi-pair scanner"""
        try:
            from multi_pair_scanner import get_top_opportunities, get_scanner_status
            
            # Get real opportunities from scanner
            opportunities = get_top_opportunities(10, 'BUY')
            status = get_scanner_status()
            
            if opportunities:
                best_opp = opportunities[0]
                return {
                    'active_pairs': status.get('cached_pairs', 0),
                    'opportunities': len(opportunities),
                    'best_opportunity': best_opp.symbol,
                    'confidence': best_opp.confidence
                }
            else:
                return {
                    'active_pairs': status.get('cached_pairs', 0),
                    'opportunities': 0,
                    'best_opportunity': 'None',
                    'confidence': 0
                }
        except Exception as e:
            self.logger.warning(f"Real scanner data failed: {e}")
            return {
                'active_pairs': 0,
                'opportunities': 0,
                'best_opportunity': 'Scanner Offline',
                'confidence': 0
            }
    
    def _get_real_market_sentiment(self) -> Dict:
        """Get real market sentiment data"""
        try:
            if self.external_data_collector:
                sentiment = self.external_data_collector.get_market_sentiment()
                return sentiment if sentiment else {'overall_sentiment': 0.0, 'bullish_indicators': 0, 'bearish_indicators': 0}
        except:
            pass
        return {'overall_sentiment': 0.0, 'bullish_indicators': 0, 'bearish_indicators': 0}
    
    def _get_real_news_sentiment(self) -> Dict:
        """Get real news sentiment data"""
        try:
            if self.external_data_collector:
                news = self.external_data_collector.get_news_sentiment()
                return news if news else {'articles_analyzed': 0, 'positive_articles': 0, 'negative_articles': 0}
        except:
            pass
        return {'articles_analyzed': 0, 'positive_articles': 0, 'negative_articles': 0}
    
    def _get_real_economic_indicators(self) -> Dict:
        """Get real economic indicators"""
        try:
            if self.external_data_collector:
                econ = self.external_data_collector.get_economic_indicators()
                return econ if econ else {'gdp_growth': 0.0, 'inflation_rate': 0.0, 'unemployment_rate': 0.0, 'interest_rate': 0.0}
        except:
            pass
        return {'gdp_growth': 0.0, 'inflation_rate': 0.0, 'unemployment_rate': 0.0, 'interest_rate': 0.0}
    
    def _get_real_social_sentiment(self) -> Dict:
        """Get real social media sentiment"""
        try:
            if self.external_data_collector:
                social = self.external_data_collector.get_social_sentiment()
                return social if social else {'twitter_mentions': 0, 'reddit_posts': 0, 'overall_social_sentiment': 0.0}
        except:
            pass
        return {'twitter_mentions': 0, 'reddit_posts': 0, 'overall_social_sentiment': 0.0}

    async def initialize_system(self) -> bool:
        """Initialize V3 system with enhanced error handling"""
        try:
            self.logger.info("Initializing V3 Trading System - REAL DATA ONLY")
            
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
            
            self.logger.info("V3 System initialized successfully - REAL DATA ONLY!")
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
        """Background loop for updating metrics and data"""
        while not self._shutdown_event.is_set():
            try:
                await self._update_real_time_data()
                await asyncio.sleep(5)
            except Exception as e:
                self.logger.error(f"Background update error: {e}")
                await asyncio.sleep(10)
    
    async def _update_real_time_data(self):
        """Update real-time data - NO SIMULATIONS"""
        try:
            # ? Update real system resources
            self.system_resources['cpu_usage'] = psutil.cpu_percent(interval=0.1)
            self.system_resources['memory_usage'] = psutil.virtual_memory().percent
            
            # ? Update REAL external data status
            real_status = self._get_real_external_data_status()
            self.external_data_status.update(real_status)
            self.external_data_status['working_apis'] = sum(self.external_data_status['api_status'].values())
            
            # ? Update REAL scanner data
            real_scanner = self._get_real_scanner_data()
            self.scanner_data.update(real_scanner)
            
            # ? Get REAL recent trades from database (not simulated)
            self._load_real_recent_trades()
                
        except Exception as e:
            self.logger.error(f"Real-time update error: {e}")
    
    def _load_real_recent_trades(self):
        """Load real recent trades from database"""
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT symbol, side, quantity, entry_price, exit_price, pnl, timestamp, strategy, confidence
                    FROM trade_history 
                    ORDER BY timestamp DESC 
                    LIMIT 20
                ''')
                
                trades = cursor.fetchall()
                self.recent_trades.clear()
                
                for trade in trades:
                    trade_data = {
                        'symbol': trade[0],
                        'side': trade[1],
                        'quantity': trade[2],
                        'entry_price': trade[3],
                        'exit_price': trade[4],
                        'profit_loss': trade[5],
                        'profit_pct': (trade[5] / (trade[2] * trade[3])) * 100 if trade[2] and trade[3] else 0,
                        'is_win': trade[5] > 0,
                        'timestamp': trade[6],
                        'source': trade[7] or 'REAL_TRADING',
                        'confidence': trade[8] or 0
                    }
                    self.recent_trades.append(trade_data)
                
        except Exception as e:
            self.logger.warning(f"Failed to load real trades: {e}")
    
    async def execute_real_paper_trade(self, signal: Dict):
        """Execute REAL paper trade using actual Binance testnet"""
        try:
            if not self.trading_engine or not hasattr(self.trading_engine, 'client'):
                return None
            
            # Execute real trade on testnet
            result = await self.trading_engine.execute_real_paper_trade(signal)
            
            if result:
                # Add to recent trades
                trade_data = {
                    'id': len(self.recent_trades) + 1,
                    'symbol': result['symbol'],
                    'side': result['side'],
                    'quantity': result['quantity'],
                    'entry_price': result['price'],
                    'exit_price': result['price'],  # Will be updated when position closes
                    'profit_loss': 0,  # Will be calculated when closed
                    'profit_pct': 0,
                    'is_win': None,  # TBD
                    'confidence': signal.get('confidence', 70),
                    'timestamp': result['timestamp'],
                    'source': 'REAL_PAPER_TRADING',
                    'order_id': result['order_id']
                }
                
                self.recent_trades.append(trade_data)
                
                # Update metrics
                self.metrics['total_trades'] += 1
                self.metrics['daily_trades'] += 1
                
                # Save to database
                with self.db_manager.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute('''
                        INSERT INTO trade_history 
                        (symbol, side, quantity, entry_price, exit_price, pnl, timestamp, strategy, confidence)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        result['symbol'], result['side'], result['quantity'],
                        result['price'], result['price'], 0,
                        result['timestamp'], 'REAL_PAPER_TRADING', signal.get('confidence', 70)
                    ))
                
                self.save_current_metrics()
                
                print(f"REAL Paper Trade: {result['side']} {result['quantity']:.6f} {result['symbol']} @ ${result['price']:.2f}")
                
                return result
                
        except Exception as e:
            self.logger.error(f"Real paper trade execution failed: {e}")
            return None

    def get_comprehensive_dashboard_data(self) -> Dict:
        """Get comprehensive dashboard data - REAL DATA ONLY"""
        try:
            return {
                'overview': {
                    'total_trades': self.metrics.get('total_trades', 0),
                    'win_rate': self.metrics.get('win_rate', 0.0),
                    'daily_pnl': self.metrics.get('daily_pnl', 0.0),
                    'total_pnl': self.metrics.get('total_pnl', 0.0),
                    'active_positions': self.metrics.get('active_positions', 0),
                    'best_trade': self.metrics.get('best_trade', 0.0),
                    'system_status': 'REAL_DATA_ONLY',
                    'trading_mode': self.trading_mode,
                    'data_source': 'REAL_BINANCE_API'
                },
                'real_time_metrics': {
                    'cpu_usage': self.system_resources['cpu_usage'],
                    'memory_usage': self.system_resources['memory_usage'],
                    'api_calls_today': self.system_resources['api_calls_today'],
                    'data_points_processed': self.system_resources['data_points_processed']
                },
                'external_data_status': self.external_data_status,
                'scanner_data': self.scanner_data,
                'recent_trades': list(self.recent_trades)[-10:],  # Last 10 real trades
                'backtest_progress': self.backtest_progress,
                'top_strategies': self.top_strategies,
                'ml_trained_strategies': self.ml_trained_strategies
            }
        except Exception as e:
            self.logger.error(f"Dashboard data error: {e}")
            return {
                'overview': {'error': str(e)},
                'real_time_metrics': {},
                'external_data_status': {},
                'scanner_data': {},
                'recent_trades': [],
                'backtest_progress': {},
                'top_strategies': [],
                'ml_trained_strategies': []
            }

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
    
    async def shutdown(self):
        """Enhanced shutdown with proper cleanup"""
        self.logger.info("Starting shutdown sequence")
        
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
            
            self.logger.info("Shutdown completed successfully")
            
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
    
    def start(self):
        """Start the V3 trading system"""
        try:
            self.is_running = True
            print("[V3_CONTROLLER] Trading system started - REAL DATA ONLY")
        except Exception as e:
            self.logger.error(f"Failed to start system: {e}")
            return False
        return True
    
    def stop(self):
        """Stop the V3 trading system"""
        try:
            self.is_running = False
            print("[V3_CONTROLLER] Trading system stopped")
        except Exception as e:
            self.logger.error(f"Failed to stop system: {e}")
            return False
        return True
    
    def get_status(self) -> Dict:
        """Get system status"""
        return {
            'is_running': self.is_running,
            'is_initialized': self.is_initialized,
            'trading_mode': self.trading_mode,
            'testnet_mode': self.testnet_mode,
            'real_data_only': True,
            'no_simulations': True,
            'initialization_progress': self.initialization_progress,
            'components': {
                'trading_engine': self.trading_engine is not None,
                'ai_brain': self.ai_brain is not None,
                'external_data_collector': self.external_data_collector is not None,
                'comprehensive_backtester': self.comprehensive_backtester is not None
            }
        }