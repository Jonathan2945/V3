#!/usr/bin/env python3
"""
V3 Binance Exchange Manager - Performance Optimized
Enhanced with database connection pooling and server optimization
"""

import asyncio
import time
import logging
import threading
import sqlite3
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict, deque
from functools import lru_cache, wraps
import concurrent.futures
import hashlib
import hmac
import base64
from urllib.parse import urlencode
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import psutil
import queue

class DatabaseConnectionPool:
    """High-performance database connection pool"""
    
    def __init__(self, db_path: str, max_connections: int = 10):
        self.db_path = db_path
        self.max_connections = max_connections
        self.connections = queue.Queue(maxsize=max_connections)
        self.total_connections = 0
        self.lock = threading.Lock()
        
        # Initialize the pool
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Initialize the connection pool"""
        try:
            for _ in range(self.max_connections):
                conn = self._create_connection()
                if conn:
                    self.connections.put(conn)
                    self.total_connections += 1
            
            logging.info(f"Database pool initialized with {self.total_connections} connections")
            
        except Exception as e:
            logging.error(f"Pool initialization error: {e}")
    
    def _create_connection(self) -> Optional[sqlite3.Connection]:
        """Create a new database connection"""
        try:
            conn = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=30.0
            )
            
            # Enable WAL mode for better concurrency
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=NORMAL')
            conn.execute('PRAGMA cache_size=10000')
            conn.execute('PRAGMA temp_store=MEMORY')
            
            # Create tables if they don't exist
            self._create_tables(conn)
            
            return conn
            
        except Exception as e:
            logging.error(f"Connection creation error: {e}")
            return None
    
    def _create_tables(self, conn: sqlite3.Connection):
        """Create necessary tables"""
        try:
            # Orders table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id TEXT UNIQUE,
                    symbol TEXT,
                    side TEXT,
                    type TEXT,
                    quantity REAL,
                    price REAL,
                    status TEXT,
                    timestamp DATETIME,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Account info table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS account_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME,
                    total_balance REAL,
                    available_balance REAL,
                    locked_balance REAL,
                    data_json TEXT
                )
            ''')
            
            # API metrics table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS api_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    endpoint TEXT,
                    method TEXT,
                    response_time REAL,
                    status_code INTEGER,
                    timestamp DATETIME,
                    success BOOLEAN
                )
            ''')
            
            # Create indexes
            conn.execute('CREATE INDEX IF NOT EXISTS idx_orders_symbol ON orders(symbol)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_orders_timestamp ON orders(timestamp)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_metrics_endpoint ON api_metrics(endpoint)')
            
            conn.commit()
            
        except Exception as e:
            logging.error(f"Table creation error: {e}")
    
    def get_connection(self) -> Optional[sqlite3.Connection]:
        """Get a connection from the pool"""
        try:
            # Try to get existing connection
            if not self.connections.empty():
                return self.connections.get(timeout=5)
            
            # If pool is empty, create new connection if under limit
            with self.lock:
                if self.total_connections < self.max_connections:
                    conn = self._create_connection()
                    if conn:
                        self.total_connections += 1
                        return conn
            
            # Wait for connection to become available
            return self.connections.get(timeout=10)
            
        except queue.Empty:
            logging.warning("No database connections available")
            return None
        except Exception as e:
            logging.error(f"Error getting connection: {e}")
            return None
    
    def return_connection(self, conn: sqlite3.Connection):
        """Return a connection to the pool"""
        try:
            if conn and not self.connections.full():
                # Test connection is still valid
                conn.execute('SELECT 1')
                self.connections.put(conn)
            elif conn:
                # Pool is full, close connection
                conn.close()
                with self.lock:
                    self.total_connections -= 1
                    
        except Exception as e:
            logging.error(f"Error returning connection: {e}")
            if conn:
                try:
                    conn.close()
                except:
                    pass
                with self.lock:
                    self.total_connections -= 1
    
    def execute_query(self, query: str, params: tuple = None, fetch: bool = False) -> Any:
        """Execute query using connection pool"""
        conn = None
        try:
            conn = self.get_connection()
            if not conn:
                raise Exception("No database connection available")
            
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            if fetch:
                result = cursor.fetchall()
                return result
            else:
                conn.commit()
                return cursor.rowcount
                
        except Exception as e:
            logging.error(f"Query execution error: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                self.return_connection(conn)
    
    def close_all(self):
        """Close all connections in the pool"""
        try:
            while not self.connections.empty():
                conn = self.connections.get()
                conn.close()
            
            self.total_connections = 0
            logging.info("All database connections closed")
            
        except Exception as e:
            logging.error(f"Error closing connections: {e}")

class BinanceAPIClient:
    """Optimized Binance API client with connection pooling"""
    
    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = 'https://api.binance.com'
        
        # Setup session with connection pooling
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(
            pool_connections=20,
            pool_maxsize=20,
            max_retries=retry_strategy
        )
        self.session.mount("https://", adapter)
        
        # Request tracking
        self.request_weights = deque(maxlen=1200)  # Track last 1200 requests
        self.last_request_time = 0
        
    def _generate_signature(self, query_string: str) -> str:
        """Generate API signature"""
        return hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
    
    def _prepare_request(self, endpoint: str, params: Dict = None, signed: bool = False) -> Tuple[str, Dict]:
        """Prepare API request"""
        url = f"{self.base_url}{endpoint}"
        headers = {
            'X-MBX-APIKEY': self.api_key,
            'Content-Type': 'application/json'
        }
        
        if params:
            if signed:
                params['timestamp'] = int(time.time() * 1000)
                query_string = urlencode(params)
                params['signature'] = self._generate_signature(query_string)
            
            url += f"?{urlencode(params)}"
        
        return url, headers
    
    def make_request(self, endpoint: str, method: str = 'GET', params: Dict = None, signed: bool = False) -> Optional[Dict]:
        """Make API request with rate limiting and error handling"""
        try:
            # Rate limiting
            current_time = time.time()
            if current_time - self.last_request_time < 0.1:  # Max 10 requests per second
                time.sleep(0.1)
            
            # Track request weight
            self.request_weights.append(current_time)
            
            # Check if we're approaching rate limits
            recent_requests = sum(1 for t in self.request_weights if current_time - t < 60)
            if recent_requests > 1000:  # Close to 1200 limit
                time.sleep(1)
            
            url, headers = self._prepare_request(endpoint, params, signed)
            
            start_time = time.time()
            if method.upper() == 'GET':
                response = self.session.get(url, headers=headers, timeout=30)
            elif method.upper() == 'POST':
                response = self.session.post(url, headers=headers, timeout=30)
            else:
                raise ValueError(f"Unsupported method: {method}")
            
            response_time = time.time() - start_time
            self.last_request_time = time.time()
            
            # Log API metrics
            self._log_api_metrics(endpoint, method, response_time, response.status_code, response.ok)
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Binance API request failed: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error in API request: {e}")
            return None
    
    def _log_api_metrics(self, endpoint: str, method: str, response_time: float, status_code: int, success: bool):
        """Log API metrics for monitoring"""
        # This would be handled by the database pool in the main manager
        pass

class BinanceExchangeManager:
    """
    Enhanced Binance Exchange Manager with Performance Optimization
    Optimized for 8 vCPU / 24GB server specifications
    """
    
    def __init__(self, config_manager=None):
        self.config = config_manager
        
        # Database connection pool
        self.db_pool = DatabaseConnectionPool('binance_data.db', max_connections=15)
        
        # API clients with rotation
        self.api_clients = self._initialize_api_clients()
        self.current_client_index = 0
        
        # Performance optimization
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
        
        # Trading state
        self.active_orders = {}
        self.order_cache = {}
        self.account_info_cache = {'data': None, 'timestamp': 0}
        
        # Performance tracking
        self.performance_metrics = {
            'total_orders': 0,
            'successful_orders': 0,
            'failed_orders': 0,
            'avg_execution_time': 0.0,
            'last_update': datetime.now()
        }
        
        # Risk management
        self.risk_limits = {
            'max_order_size': 1000.0,
            'max_daily_orders': 100,
            'max_open_positions': 5
        }
        
        # Start background tasks
        self._start_background_tasks()
    
    def _initialize_api_clients(self) -> List[BinanceAPIClient]:
        """Initialize API clients from configuration"""
        try:
            clients = []
            
            if self.config:
                for i in range(1, 4):  # Up to 3 API keys
                    api_key = self.config.get(f'BINANCE_API_KEY_{i}')
                    api_secret = self.config.get(f'BINANCE_API_SECRET_{i}')
                    
                    if api_key and api_secret:
                        client = BinanceAPIClient(api_key, api_secret)
                        clients.append(client)
            
            if not clients:
                logging.warning("No Binance API keys configured")
            else:
                logging.info(f"Initialized {len(clients)} Binance API clients")
            
            return clients
            
        except Exception as e:
            logging.error(f"Error initializing API clients: {e}")
            return []
    
    def _get_api_client(self) -> Optional[BinanceAPIClient]:
        """Get next API client for rotation"""
        if not self.api_clients:
            return None
        
        client = self.api_clients[self.current_client_index]
        self.current_client_index = (self.current_client_index + 1) % len(self.api_clients)
        return client
    
    async def get_account_info_async(self, use_cache: bool = True) -> Optional[Dict]:
        """Get account information asynchronously with caching"""
        try:
            # Check cache first
            if use_cache and self.account_info_cache['data']:
                cache_age = time.time() - self.account_info_cache['timestamp']
                if cache_age < 30:  # Cache for 30 seconds
                    return self.account_info_cache['data']
            
            client = self._get_api_client()
            if not client:
                return None
            
            # Execute in thread pool
            loop = asyncio.get_event_loop()
            account_info = await loop.run_in_executor(
                self.executor,
                client.make_request,
                '/api/v3/account',
                'GET',
                {},
                True  # Signed request
            )
            
            if account_info:
                # Update cache
                self.account_info_cache = {
                    'data': account_info,
                    'timestamp': time.time()
                }
                
                # Store in database
                self._store_account_snapshot_async(account_info)
            
            return account_info
            
        except Exception as e:
            logging.error(f"Error getting account info: {e}")
            return None
    
    def _store_account_snapshot_async(self, account_info: Dict):
        """Store account snapshot in database asynchronously"""
        def store_snapshot():
            try:
                # Calculate totals
                total_balance = 0.0
                available_balance = 0.0
                locked_balance = 0.0
                
                for balance in account_info.get('balances', []):
                    free = float(balance.get('free', 0))
                    locked = float(balance.get('locked', 0))
                    available_balance += free
                    locked_balance += locked
                    total_balance += free + locked
                
                # Store in database using connection pool
                self.db_pool.execute_query(
                    '''INSERT INTO account_snapshots 
                       (timestamp, total_balance, available_balance, locked_balance, data_json)
                       VALUES (?, ?, ?, ?, ?)''',
                    (datetime.now(), total_balance, available_balance, locked_balance, json.dumps(account_info))
                )
                
            except Exception as e:
                logging.error(f"Error storing account snapshot: {e}")
        
        self.executor.submit(store_snapshot)
    
    async def place_order_async(self, symbol: str, side: str, quantity: float, 
                               price: float = None, order_type: str = 'MARKET') -> Optional[Dict]:
        """Place order asynchronously with full error handling"""
        try:
            # Validate order parameters
            if not self._validate_order_params(symbol, side, quantity, price, order_type):
                return None
            
            # Check risk limits
            if not self._check_risk_limits(symbol, quantity):
                logging.warning(f"Order rejected due to risk limits: {symbol} {quantity}")
                return None
            
            client = self._get_api_client()
            if not client:
                return None
            
            # Prepare order parameters
            params = {
                'symbol': symbol.upper(),
                'side': side.upper(),
                'type': order_type.upper(),
                'quantity': str(quantity)
            }
            
            if order_type.upper() == 'LIMIT' and price:
                params['price'] = str(price)
                params['timeInForce'] = 'GTC'
            
            # Execute order in thread pool
            loop = asyncio.get_event_loop()
            start_time = time.time()
            
            order_result = await loop.run_in_executor(
                self.executor,
                client.make_request,
                '/api/v3/order',
                'POST',
                params,
                True  # Signed request
            )
            
            execution_time = time.time() - start_time
            
            if order_result:
                # Update performance metrics
                self._update_order_metrics(True, execution_time)
                
                # Store order in database
                self._store_order_async(order_result)
                
                # Update active orders cache
                order_id = order_result.get('orderId')
                if order_id:
                    self.active_orders[order_id] = order_result
                
                logging.info(f"Order placed successfully: {order_id}")
            else:
                self._update_order_metrics(False, execution_time)
            
            return order_result
            
        except Exception as e:
            logging.error(f"Error placing order: {e}")
            self._update_order_metrics(False, 0)
            return None
    
    def _validate_order_params(self, symbol: str, side: str, quantity: float, 
                              price: float, order_type: str) -> bool:
        """Validate order parameters"""
        try:
            # Basic validation
            if not symbol or not side or quantity <= 0:
                return False
            
            if side.upper() not in ['BUY', 'SELL']:
                return False
            
            if order_type.upper() not in ['MARKET', 'LIMIT', 'STOP_LOSS', 'STOP_LOSS_LIMIT']:
                return False
            
            if order_type.upper() in ['LIMIT', 'STOP_LOSS_LIMIT'] and not price:
                return False
            
            return True
            
        except Exception as e:
            logging.error(f"Order validation error: {e}")
            return False
    
    def _check_risk_limits(self, symbol: str, quantity: float) -> bool:
        """Check risk limits before placing order"""
        try:
            # Check order size limit
            if quantity > self.risk_limits['max_order_size']:
                return False
            
            # Check daily order count
            today = datetime.now().date()
            daily_orders = self.db_pool.execute_query(
                'SELECT COUNT(*) FROM orders WHERE DATE(created_at) = ?',
                (today,),
                fetch=True
            )
            
            if daily_orders and daily_orders[0][0] >= self.risk_limits['max_daily_orders']:
                return False
            
            # Check open positions count
            if len(self.active_orders) >= self.risk_limits['max_open_positions']:
                return False
            
            return True
            
        except Exception as e:
            logging.error(f"Risk check error: {e}")
            return False
    
    def _store_order_async(self, order_data: Dict):
        """Store order in database asynchronously"""
        def store_order():
            try:
                self.db_pool.execute_query(
                    '''INSERT OR REPLACE INTO orders 
                       (order_id, symbol, side, type, quantity, price, status, timestamp)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                    (
                        order_data.get('orderId'),
                        order_data.get('symbol'),
                        order_data.get('side'),
                        order_data.get('type'),
                        float(order_data.get('origQty', 0)),
                        float(order_data.get('price', 0)),
                        order_data.get('status'),
                        datetime.fromtimestamp(order_data.get('transactTime', 0) / 1000)
                    )
                )
                
            except Exception as e:
                logging.error(f"Error storing order: {e}")
        
        self.executor.submit(store_order)
    
    def _update_order_metrics(self, success: bool, execution_time: float):
        """Update order execution metrics"""
        try:
            self.performance_metrics['total_orders'] += 1
            
            if success:
                self.performance_metrics['successful_orders'] += 1
            else:
                self.performance_metrics['failed_orders'] += 1
            
            # Update rolling average execution time
            if self.performance_metrics['avg_execution_time'] == 0:
                self.performance_metrics['avg_execution_time'] = execution_time
            else:
                self.performance_metrics['avg_execution_time'] = (
                    self.performance_metrics['avg_execution_time'] * 0.9 + execution_time * 0.1
                )
            
            self.performance_metrics['last_update'] = datetime.now()
            
        except Exception as e:
            logging.error(f"Error updating metrics: {e}")
    
    async def get_order_status_async(self, symbol: str, order_id: str) -> Optional[Dict]:
        """Get order status asynchronously"""
        try:
            # Check cache first
            if order_id in self.order_cache:
                cache_entry = self.order_cache[order_id]
                if time.time() - cache_entry['timestamp'] < 10:  # 10 second cache
                    return cache_entry['data']
            
            client = self._get_api_client()
            if not client:
                return None
            
            params = {
                'symbol': symbol.upper(),
                'orderId': order_id
            }
            
            loop = asyncio.get_event_loop()
            order_status = await loop.run_in_executor(
                self.executor,
                client.make_request,
                '/api/v3/order',
                'GET',
                params,
                True
            )
            
            if order_status:
                # Update cache
                self.order_cache[order_id] = {
                    'data': order_status,
                    'timestamp': time.time()
                }
                
                # Update database if status changed
                self._update_order_status_async(order_status)
            
            return order_status
            
        except Exception as e:
            logging.error(f"Error getting order status: {e}")
            return None
    
    def _update_order_status_async(self, order_data: Dict):
        """Update order status in database asynchronously"""
        def update_status():
            try:
                self.db_pool.execute_query(
                    'UPDATE orders SET status = ? WHERE order_id = ?',
                    (order_data.get('status'), order_data.get('orderId'))
                )
                
            except Exception as e:
                logging.error(f"Error updating order status: {e}")
        
        self.executor.submit(update_status)
    
    def _start_background_tasks(self):
        """Start background monitoring and maintenance tasks"""
        def background_worker():
            while True:
                try:
                    self._cleanup_cache()
                    self._monitor_orders()
                    self._log_performance_metrics()
                    time.sleep(60)  # Run every minute
                except Exception as e:
                    logging.error(f"Background task error: {e}")
                    time.sleep(30)
        
        thread = threading.Thread(target=background_worker, daemon=True)
        thread.start()
    
    def _cleanup_cache(self):
        """Clean up expired cache entries"""
        try:
            current_time = time.time()
            
            # Clean order cache
            expired_orders = [
                order_id for order_id, cache_entry in self.order_cache.items()
                if current_time - cache_entry['timestamp'] > 300  # 5 minutes
            ]
            
            for order_id in expired_orders:
                del self.order_cache[order_id]
            
            # Clean account info cache if old
            if current_time - self.account_info_cache['timestamp'] > 300:
                self.account_info_cache = {'data': None, 'timestamp': 0}
            
        except Exception as e:
            logging.error(f"Cache cleanup error: {e}")
    
    def _monitor_orders(self):
        """Monitor active orders for status changes"""
        try:
            # This would check active orders and update their status
            # For now, just clean up completed orders from active list
            completed_orders = []
            
            for order_id in self.active_orders:
                # In a real implementation, you would check order status
                # and remove completed orders
                pass
            
            for order_id in completed_orders:
                del self.active_orders[order_id]
                
        except Exception as e:
            logging.error(f"Order monitoring error: {e}")
    
    def _log_performance_metrics(self):
        """Log current performance metrics"""
        try:
            metrics = self.performance_metrics.copy()
            
            success_rate = 0
            if metrics['total_orders'] > 0:
                success_rate = metrics['successful_orders'] / metrics['total_orders'] * 100
            
            logging.info(
                f"Exchange metrics - Total: {metrics['total_orders']}, "
                f"Success: {success_rate:.1f}%, "
                f"Avg execution: {metrics['avg_execution_time']:.3f}s"
            )
            
        except Exception as e:
            logging.error(f"Metrics logging error: {e}")
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get comprehensive performance summary"""
        try:
            summary = self.performance_metrics.copy()
            
            # Add success rate
            if summary['total_orders'] > 0:
                summary['success_rate'] = summary['successful_orders'] / summary['total_orders']
                summary['failure_rate'] = summary['failed_orders'] / summary['total_orders']
            else:
                summary['success_rate'] = 0
                summary['failure_rate'] = 0
            
            # Add database stats
            summary['db_connections'] = self.db_pool.total_connections
            summary['active_orders_count'] = len(self.active_orders)
            summary['cached_orders_count'] = len(self.order_cache)
            
            return summary
            
        except Exception as e:
            logging.error(f"Error getting performance summary: {e}")
            return {}
    
    def optimize_for_server_specs(self):
        """Optimize for 8 vCPU / 24GB server specifications"""
        try:
            # Adjust thread pool size
            cpu_count = psutil.cpu_count()
            optimal_workers = min(cpu_count * 2, 16)  # 2x CPU cores, max 16
            
            if self.executor._max_workers != optimal_workers:
                self.executor.shutdown(wait=False)
                self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=optimal_workers)
            
            # Adjust database connection pool
            memory_gb = psutil.virtual_memory().total / (1024**3)
            if memory_gb >= 24:
                # Increase connection pool for high memory systems
                if self.db_pool.max_connections < 20:
                    self.db_pool.max_connections = 20
            
            logging.info(f"Exchange manager optimized for {cpu_count} CPUs, {memory_gb:.1f}GB RAM")
            
        except Exception as e:
            logging.error(f"Server optimization error: {e}")
    
    def __del__(self):
        """Cleanup when object is destroyed"""
        try:
            if hasattr(self, 'db_pool'):
                self.db_pool.close_all()
            if hasattr(self, 'executor'):
                self.executor.shutdown(wait=False)
        except:
            pass

# Export main class
__all__ = ['BinanceExchangeManager', 'DatabaseConnectionPool', 'BinanceAPIClient']

if __name__ == "__main__":
    # Performance test
    manager = BinanceExchangeManager()
    manager.optimize_for_server_specs()
    
    # Test database operations
    summary = manager.get_performance_summary()
    print(f"Performance Summary: {json.dumps(summary, indent=2, default=str)}")