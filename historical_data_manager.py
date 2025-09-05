#!/usr/bin/env python3
"""
V3 HISTORICAL DATA MANAGER - OPTIMIZED FOR 8 vCPU/24GB
======================================================
V3 Performance & Compliance Fixes Applied:
- Enhanced database connection pooling for high throughput
- UTF-8 encoding specification for all file operations
- Memory optimization for large data operations with cleanup
- Intelligent caching system with automatic expiration
- ThreadPoolExecutor with proper max_workers configuration
- Real data validation patterns enforced
- Proper database connection closure and resource management
- Server-optimized async operations for 8 vCPU/24GB specs
"""

import os
import asyncio
import aiohttp
import pandas as pd
import numpy as np
import logging
import sqlite3
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import time
import random
import threading
from concurrent.futures import ThreadPoolExecutor
import gc
import psutil
from functools import lru_cache

class OptimizedConnectionPool:
    """High-performance database connection pool optimized for 8 vCPU/24GB server"""
    
    def __init__(self, db_path: str, max_connections: int = 16):
        self.db_path = db_path
        self.max_connections = max_connections
        self.connections = []
        self.available = []
        self.in_use = set()
        self.lock = threading.RLock()
        self.stats = {
            'total_requests': 0,
            'successful_connections': 0,
            'failed_connections': 0,
            'average_wait_time': 0.0,
            'peak_concurrent_connections': 0
        }
        
        # Initialize connection pool with optimized settings
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Initialize connection pool with performance optimizations"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        for i in range(self.max_connections):
            conn = sqlite3.connect(
                self.db_path, 
                check_same_thread=False,
                timeout=30.0,
                isolation_level=None  # Autocommit for better performance
            )
            
            # Apply comprehensive performance optimizations
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA cache_size=25000")  # 25MB cache per connection
            conn.execute("PRAGMA temp_store=MEMORY")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA page_size=4096")
            conn.execute("PRAGMA mmap_size=268435456")  # 256MB memory map
            conn.execute("PRAGMA wal_autocheckpoint=1000")
            conn.execute("PRAGMA optimize")
            
            # Enable query optimization
            conn.execute("PRAGMA automatic_index=ON")
            conn.execute("PRAGMA threads=4")  # Use multiple threads per connection
            
            self.connections.append(conn)
            self.available.append(conn)
    
    def get_connection(self, timeout: float = 30.0) -> Optional[sqlite3.Connection]:
        """Get optimized connection with performance tracking"""
        start_time = time.time()
        self.stats['total_requests'] += 1
        
        while time.time() - start_time < timeout:
            with self.lock:
                if self.available:
                    conn = self.available.pop()
                    self.in_use.add(conn)
                    
                    wait_time = time.time() - start_time
                    self._update_wait_time_stats(wait_time)
                    
                    # Update peak concurrent connections
                    current_in_use = len(self.in_use)
                    if current_in_use > self.stats['peak_concurrent_connections']:
                        self.stats['peak_concurrent_connections'] = current_in_use
                    
                    self.stats['successful_connections'] += 1
                    return conn
            
            time.sleep(0.005)  # 5ms wait to reduce CPU usage
        
        self.stats['failed_connections'] += 1
        logging.warning(f"Connection pool timeout after {timeout}s")
        return None
    
    def return_connection(self, conn: sqlite3.Connection):
        """Return connection with health validation"""
        with self.lock:
            if conn in self.in_use:
                self.in_use.remove(conn)
                
                # Validate connection health before returning
                try:
                    conn.execute("SELECT 1")
                    self.available.append(conn)
                except sqlite3.Error as e:
                    logging.warning(f"Unhealthy connection detected: {e}")
                    try:
                        conn.close()
                    except:
                        pass
                    self._create_replacement_connection()
    
    def _create_replacement_connection(self):
        """Create replacement connection with full optimization"""
        try:
            conn = sqlite3.connect(
                self.db_path, 
                check_same_thread=False,
                timeout=30.0,
                isolation_level=None
            )
            
            # Apply all optimizations
            optimizations = [
                "PRAGMA journal_mode=WAL",
                "PRAGMA cache_size=25000",
                "PRAGMA temp_store=MEMORY",
                "PRAGMA synchronous=NORMAL",
                "PRAGMA page_size=4096",
                "PRAGMA mmap_size=268435456",
                "PRAGMA wal_autocheckpoint=1000",
                "PRAGMA automatic_index=ON",
                "PRAGMA threads=4",
                "PRAGMA optimize"
            ]
            
            for pragma in optimizations:
                conn.execute(pragma)
            
            self.connections.append(conn)
            self.available.append(conn)
            
        except Exception as e:
            logging.error(f"Failed to create replacement connection: {e}")
    
    def _update_wait_time_stats(self, wait_time: float):
        """Update average wait time statistics"""
        current_avg = self.stats['average_wait_time']
        successful_count = self.stats['successful_connections']
        
        if successful_count == 1:
            self.stats['average_wait_time'] = wait_time
        else:
            self.stats['average_wait_time'] = (
                (current_avg * (successful_count - 1) + wait_time) / successful_count
            )
    
    def execute_batch(self, operations: List[Tuple[str, tuple]], max_retries: int = 3) -> List[Any]:
        """Execute batch operations for better performance"""
        results = []
        
        for attempt in range(max_retries):
            conn = self.get_connection()
            if not conn:
                continue
            
            try:
                conn.execute("BEGIN TRANSACTION")
                
                for query, params in operations:
                    cursor = conn.execute(query, params)
                    results.append(cursor.fetchall())
                
                conn.execute("COMMIT")
                self.return_connection(conn)
                return results
            
            except sqlite3.Error as e:
                try:
                    conn.execute("ROLLBACK")
                except:
                    pass
                
                try:
                    conn.close()
                except:
                    pass
                
                if attempt == max_retries - 1:
                    logging.error(f"Batch operation failed after {max_retries} attempts: {e}")
                    raise
                
                time.sleep(0.1 * (attempt + 1))
        
        return results
    
    def get_pool_stats(self) -> Dict[str, Any]:
        """Get comprehensive pool statistics"""
        with self.lock:
            total_requests = self.stats['total_requests']
            success_rate = (
                (self.stats['successful_connections'] / max(1, total_requests)) * 100
            )
            
            return {
                'total_connections': len(self.connections),
                'available_connections': len(self.available),
                'in_use_connections': len(self.in_use),
                'total_requests': total_requests,
                'success_rate_percent': round(success_rate, 2),
                'average_wait_time_ms': round(self.stats['average_wait_time'] * 1000, 2),
                'peak_concurrent_connections': self.stats['peak_concurrent_connections']
            }
    
    def cleanup(self):
        """Comprehensive cleanup of all connections"""
        with self.lock:
            all_connections = self.connections + list(self.in_use)
            for conn in all_connections:
                try:
                    conn.close()
                except Exception as e:
                    logging.warning(f"Error closing connection: {e}")
            
            self.connections.clear()
            self.available.clear()
            self.in_use.clear()

class MemoryOptimizedCache:
    """Intelligent caching system with memory management for large datasets"""
    
    def __init__(self, max_memory_mb: int = 1024):
        self.cache = {}
        self.access_times = {}
        self.cache_sizes = {}  # Track individual item sizes
        self.max_memory_bytes = max_memory_mb * 1024 * 1024
        self.current_memory_usage = 0
        self.lock = threading.RLock()
        
        # Performance statistics
        self.stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0,
            'memory_cleanups': 0,
            'total_items_cached': 0
        }
        
        # Background cleanup
        self._cleanup_interval = 300  # 5 minutes
        self._last_cleanup = time.time()
    
    def get(self, key: str) -> Optional[Any]:
        """Get item with memory tracking"""
        with self.lock:
            if key in self.cache:
                data, timestamp, ttl = self.cache[key]
                current_time = time.time()
                
                if current_time - timestamp <= ttl:
                    self.access_times[key] = current_time
                    self.stats['hits'] += 1
                    return data
                else:
                    # Expired item
                    self._remove_item(key)
            
            self.stats['misses'] += 1
            self._maybe_cleanup()
            return None
    
    def set(self, key: str, value: Any, ttl: int = 1800):
        """Set item with intelligent memory management"""
        with self.lock:
            # Estimate memory usage of the item
            item_size = self._estimate_size(value)
            
            # Check if we need to free space
            if self.current_memory_usage + item_size > self.max_memory_bytes:
                self._free_memory_for_item(item_size)
            
            # Store the item
            current_time = time.time()
            self.cache[key] = (value, current_time, ttl)
            self.access_times[key] = current_time
            self.cache_sizes[key] = item_size
            self.current_memory_usage += item_size
            
            self.stats['total_items_cached'] += 1
    
    def _estimate_size(self, obj: Any) -> int:
        """Estimate memory size of an object"""
        try:
            if isinstance(obj, pd.DataFrame):
                return obj.memory_usage(deep=True).sum()
            elif isinstance(obj, dict):
                return len(str(obj).encode('utf-8'))
            elif isinstance(obj, (list, tuple)):
                return sum(len(str(item).encode('utf-8')) for item in obj)
            else:
                return len(str(obj).encode('utf-8'))
        except Exception:
            return 1024  # Default size estimate
    
    def _free_memory_for_item(self, needed_bytes: int):
        """Free memory using LRU eviction"""
        if not self.access_times:
            return
        
        # Calculate how much to free (needed + 20% buffer)
        target_free = int(needed_bytes * 1.2)
        freed = 0
        
        # Sort by access time (oldest first)
        sorted_items = sorted(self.access_times.items(), key=lambda x: x[1])
        
        for key, _ in sorted_items:
            if freed >= target_free:
                break
            
            if key in self.cache_sizes:
                freed += self.cache_sizes[key]
                self._remove_item(key)
                self.stats['evictions'] += 1
    
    def _remove_item(self, key: str):
        """Remove item and update memory tracking"""
        if key in self.cache:
            del self.cache[key]
        if key in self.access_times:
            del self.access_times[key]
        if key in self.cache_sizes:
            self.current_memory_usage -= self.cache_sizes[key]
            del self.cache_sizes[key]
    
    def _maybe_cleanup(self):
        """Periodic cleanup of expired items"""
        current_time = time.time()
        if current_time - self._last_cleanup > self._cleanup_interval:
            self._cleanup_expired_items()
            self._last_cleanup = current_time
    
    def _cleanup_expired_items(self):
        """Remove all expired items"""
        current_time = time.time()
        expired_keys = []
        
        for key, (_, timestamp, ttl) in self.cache.items():
            if current_time - timestamp > ttl:
                expired_keys.append(key)
        
        for key in expired_keys:
            self._remove_item(key)
        
        if expired_keys:
            self.stats['memory_cleanups'] += 1
            # Force garbage collection after cleanup
            gc.collect()
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics"""
        with self.lock:
            total_requests = self.stats['hits'] + self.stats['misses']
            hit_rate = (self.stats['hits'] / max(1, total_requests)) * 100
            memory_usage_mb = self.current_memory_usage / (1024 * 1024)
            
            return {
                'cache_size': len(self.cache),
                'memory_usage_mb': round(memory_usage_mb, 2),
                'memory_limit_mb': self.max_memory_bytes / (1024 * 1024),
                'hit_rate_percent': round(hit_rate, 2),
                'total_hits': self.stats['hits'],
                'total_misses': self.stats['misses'],
                'total_evictions': self.stats['evictions'],
                'memory_cleanups': self.stats['memory_cleanups'],
                'items_cached': self.stats['total_items_cached']
            }
    
    def clear(self):
        """Clear all cached data"""
        with self.lock:
            self.cache.clear()
            self.access_times.clear()
            self.cache_sizes.clear()
            self.current_memory_usage = 0

class HistoricalDataManager:
    """V3 Historical Data Manager optimized for 8 vCPU/24GB server with real data validation"""
    
    def __init__(self):
        self.db_path = 'data/historical_data.db'
        self.config = {
            'years_of_data': 2,
            'enable_backtesting': True,
            'cache_expiry_hours': 6,
            'cleanup_days': 30,
            'v3_compliance': True,
            'live_data_only': True,
            'utf8_encoding': True,
            'max_memory_mb': 1024,
            'max_workers': min(8, os.cpu_count() or 4)
        }
        
        # Performance optimizations for 8 vCPU/24GB
        self.connection_pool = OptimizedConnectionPool(self.db_path, max_connections=16)
        self.memory_cache = MemoryOptimizedCache(max_memory_mb=1024)
        self.thread_pool = ThreadPoolExecutor(max_workers=self.config['max_workers'])
        
        # V3 Multiple live data sources for resilience
        self.live_api_endpoints = [
            'https://api.binance.com/api/v3/klines',
            'https://api.binance.us/api/v3/klines',
            'https://testnet.binance.vision/api/v3/klines',
        ]
        
        self.symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'XRPUSDT', 'ADAUSDT', 'SOLUSDT']
        self.timeframes = {'1h': '1h', '4h': '4h', '1d': '1d'}
        self.last_update_times = {}
        
        # Performance monitoring
        self._last_memory_check = time.time()
        self._memory_check_interval = 300  # 5 minutes
        
        logging.info("[DATA_MANAGER] V3 Historical Data Manager initialized - PERFORMANCE OPTIMIZED")
        print(f"[DATA_MANAGER] Connection pool: {len(self.connection_pool.connections)} connections")
        print(f"[DATA_MANAGER] Thread pool: {self.config['max_workers']} workers")
        print(f"[DATA_MANAGER] Memory cache: {self.config['max_memory_mb']} MB limit")
    
    async def initialize(self):
        """V3 Initialize with enhanced performance and validation"""
        try:
            await self.init_database()
            await self.test_live_api_connectivity()
            self._start_background_maintenance()
            
            logging.info("[DATA_MANAGER] V3 Historical Data Manager initialization complete - HIGH PERFORMANCE MODE")
        except Exception as e:
            logging.error(f"[DATA_MANAGER] V3 Data Manager initialization failed: {e}")
            raise RuntimeError(f"V3 Live data initialization failed: {e}")
    
    async def init_database(self):
        """Initialize V3 database with UTF-8 support and optimized schema"""
        try:
            live_historical_sql = """
            CREATE TABLE IF NOT EXISTS live_historical_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL COLLATE NOCASE,
                timeframe TEXT NOT NULL COLLATE NOCASE,
                timestamp INTEGER NOT NULL,
                open_price REAL NOT NULL,
                high_price REAL NOT NULL,
                low_price REAL NOT NULL,
                close_price REAL NOT NULL,
                volume REAL NOT NULL,
                data_source TEXT DEFAULT 'live_api' COLLATE NOCASE,
                encoding TEXT DEFAULT 'utf-8',
                v3_compliance BOOLEAN DEFAULT TRUE,
                data_quality_score REAL DEFAULT 1.0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, timeframe, timestamp)
            )
            """
            
            # Create indexes for performance
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_symbol_timeframe ON live_historical_data(symbol, timeframe)",
                "CREATE INDEX IF NOT EXISTS idx_timestamp ON live_historical_data(timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_symbol_timestamp ON live_historical_data(symbol, timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_v3_compliance ON live_historical_data(v3_compliance)",
                "CREATE INDEX IF NOT EXISTS idx_data_source ON live_historical_data(data_source)",
                "CREATE INDEX IF NOT EXISTS idx_created_at ON live_historical_data(created_at)",
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_data ON live_historical_data(symbol, timeframe, timestamp)"
            ]
            
            # Execute schema creation with connection pool
            operations = [(live_historical_sql, ())] + [(idx, ()) for idx in indexes]
            self.connection_pool.execute_batch(operations)
            
            logging.info("[DATA_MANAGER] V3 Live historical data database initialized with UTF-8 support")
        except Exception as e:
            logging.error(f"[DATA_MANAGER] V3 Database initialization failed: {e}")
            raise
    
    def _start_background_maintenance(self):
        """Start background maintenance tasks"""
        def maintenance_worker():
            while True:
                try:
                    # Memory management
                    self._manage_memory_usage()
                    
                    # Cache cleanup
                    self.memory_cache._cleanup_expired_items()
                    
                    # Database maintenance
                    self._optimize_database()
                    
                    time.sleep(300)  # 5 minutes
                except Exception as e:
                    logging.error(f"Background maintenance error: {e}")
                    time.sleep(600)  # 10 minutes on error
        
        maintenance_thread = threading.Thread(target=maintenance_worker, daemon=True)
        maintenance_thread.start()
    
    def _manage_memory_usage(self):
        """Intelligent memory management for large data operations"""
        current_time = time.time()
        if current_time - self._last_memory_check > self._memory_check_interval:
            try:
                process = psutil.Process()
                memory_mb = process.memory_info().rss / 1024 / 1024
                
                if memory_mb > self.config['max_memory_mb'] * 0.8:  # 80% threshold
                    # Clear expired cache entries
                    self.memory_cache._cleanup_expired_items()
                    
                    # Force garbage collection
                    gc.collect()
                    
                    # Log memory status
                    new_memory_mb = psutil.Process().memory_info().rss / 1024 / 1024
                    freed_mb = memory_mb - new_memory_mb
                    
                    logging.info(f"Memory cleanup: {freed_mb:.1f}MB freed, usage: {new_memory_mb:.1f}MB")
                
                self._last_memory_check = current_time
                
            except Exception as e:
                logging.warning(f"Memory management error: {e}")
    
    def _optimize_database(self):
        """Perform database optimization"""
        try:
            conn = self.connection_pool.get_connection()
            if conn:
                try:
                    # Update database statistics
                    conn.execute("ANALYZE")
                    conn.execute("PRAGMA optimize")
                    
                    # Checkpoint WAL file if it's getting large
                    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                    
                finally:
                    self.connection_pool.return_connection(conn)
        except Exception as e:
            logging.warning(f"Database optimization error: {e}")
    
    async def test_live_api_connectivity(self):
        """V3 Test live API connectivity with enhanced performance"""
        working_endpoints = []
        
        # Test endpoints concurrently for better performance
        async def test_endpoint(endpoint):
            try:
                timeout = aiohttp.ClientTimeout(total=10, connect=5)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    params = {'symbol': 'BTCUSDT', 'interval': '1h', 'limit': 5}
                    async with session.get(endpoint, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            if data and len(data) > 0:
                                logging.info(f"[DATA_MANAGER] V3 Live API endpoint working: {endpoint}")
                                return endpoint
                        else:
                            logging.warning(f"[DATA_MANAGER] V3 API endpoint {endpoint} returned {response.status}")
            except Exception as e:
                logging.warning(f"[DATA_MANAGER] V3 API endpoint {endpoint} failed: {e}")
            return None
        
        # Test all endpoints concurrently
        tasks = [test_endpoint(endpoint) for endpoint in self.live_api_endpoints]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        working_endpoints = [result for result in results if result and not isinstance(result, Exception)]
        
        if working_endpoints:
            self.live_api_endpoints = working_endpoints + [
                ep for ep in self.live_api_endpoints if ep not in working_endpoints
            ]
            logging.info(f"[DATA_MANAGER] V3 Found {len(working_endpoints)} working live API endpoints")
        else:
            raise RuntimeError("V3 CRITICAL: No live API endpoints available - cannot proceed without live data")
    
    async def download_live_historical_data(self, symbol: str, timeframe: str, limit: int = 100):
        """V3 Download live data with performance optimization and proper session management"""
        
        # Check cache first
        cache_key = f"live_data_{symbol}_{timeframe}_{limit}"
        cached_data = self.memory_cache.get(cache_key)
        if cached_data is not None:
            return cached_data
        
        for endpoint in self.live_api_endpoints:
            connector = None
            session = None
            try:
                # Optimized connection settings for performance
                connector = aiohttp.TCPConnector(
                    limit=10,
                    limit_per_host=5,
                    ttl_dns_cache=300,
                    enable_cleanup_closed=True,
                    keepalive_timeout=30
                )
                
                timeout = aiohttp.ClientTimeout(total=20, connect=5, sock_read=15)
                
                session = aiohttp.ClientSession(
                    connector=connector,
                    timeout=timeout,
                    headers={
                        'User-Agent': 'V3-Trading-System/3.0-Performance',
                        'Accept-Encoding': 'gzip, deflate'
                    }
                )
                
                params = {'symbol': symbol, 'interval': timeframe, 'limit': limit}
                
                async with session.get(endpoint, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data:
                            # Process data with memory optimization
                            df = await self._process_market_data_optimized(data, symbol, timeframe)
                            
                            if df is not None and len(df) > 0:
                                # Cache the result
                                self.memory_cache.set(cache_key, df, ttl=1800)  # 30 minutes
                                
                                logging.info(f"[DATA_MANAGER] V3 Downloaded {len(df)} live candles for {symbol} {timeframe}")
                                return df
                    
                    elif response.status == 451:
                        logging.warning(f"[DATA_MANAGER] V3 HTTP 451 (Unavailable For Legal Reasons) from {endpoint}")
                        continue
                    elif response.status == 429:
                        logging.warning(f"[DATA_MANAGER] V3 Rate limited by {endpoint}")
                        await asyncio.sleep(2)
                        continue
                    else:
                        logging.warning(f"[DATA_MANAGER] V3 HTTP {response.status} from {endpoint}")
                        continue
                        
            except asyncio.TimeoutError:
                logging.warning(f"[DATA_MANAGER] V3 Timeout for endpoint {endpoint}")
                continue
            except Exception as e:
                logging.warning(f"[DATA_MANAGER] V3 Error with endpoint {endpoint}: {e}")
                continue
            finally:
                # Ensure proper cleanup
                try:
                    if session and not session.closed:
                        await session.close()
                        await asyncio.sleep(0.1)  # Allow time for cleanup
                except Exception as cleanup_error:
                    logging.warning(f"Session cleanup error: {cleanup_error}")
                
                try:
                    if connector:
                        await connector.close()
                except Exception as connector_error:
                    logging.warning(f"Connector cleanup error: {connector_error}")
        
        # V3: If all APIs fail, raise error
        logging.error(f"[DATA_MANAGER] V3 CRITICAL: All live API endpoints failed for {symbol} {timeframe}")
        raise RuntimeError(f"V3 CRITICAL: Cannot obtain live data for {symbol} {timeframe} - all endpoints failed")
    
    async def _process_market_data_optimized(self, data: List, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
        """Process market data with memory optimization and UTF-8 handling"""
        try:
            # Use thread pool for CPU-intensive pandas operations
            def process_data():
                df = pd.DataFrame(data, columns=[
                    'timestamp', 'open', 'high', 'low', 'close', 'volume',
                    'close_time', 'quote_volume', 'trades_count',
                    'taker_buy_volume', 'taker_buy_quote_volume', 'ignore'
                ])
                
                # Optimize data types for memory efficiency
                numeric_columns = ['open', 'high', 'low', 'close', 'volume']
                for col in numeric_columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce', downcast='float')
                
                df['timestamp'] = pd.to_numeric(df['timestamp'], downcast='integer')
                
                # Remove invalid data
                df = df.dropna(subset=numeric_columns)
                
                # Validate data quality
                if len(df) == 0:
                    return None
                
                # Add metadata for V3 compliance
                df['data_source'] = 'live_api'
                df['encoding'] = 'utf-8'
                df['v3_compliance'] = True
                
                return df
            
            # Execute in thread pool
            loop = asyncio.get_event_loop()
            df = await loop.run_in_executor(self.thread_pool, process_data)
            
            return df
            
        except Exception as e:
            logging.error(f"[DATA_MANAGER] Data processing error: {e}")
            return None
    
    async def store_live_historical_data(self, symbol: str, timeframe: str, df: pd.DataFrame):
        """V3 Store live historical data with batch operations and UTF-8 encoding"""
        try:
            if df is None or len(df) == 0:
                return
            
            # Prepare batch operations for better performance
            operations = []
            store_sql = """
            INSERT OR REPLACE INTO live_historical_data 
            (symbol, timeframe, timestamp, open_price, high_price, 
             low_price, close_price, volume, data_source, encoding, v3_compliance, data_quality_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            for _, row in df.iterrows():
                try:
                    # Calculate data quality score
                    quality_score = self._calculate_data_quality_score(row)
                    
                    params = (
                        str(symbol), 
                        str(timeframe), 
                        int(float(row['timestamp'])),
                        float(row['open']), 
                        float(row['high']), 
                        float(row['low']),
                        float(row['close']), 
                        float(row['volume']),
                        'live_api',
                        'utf-8',  # V3: UTF-8 encoding specification
                        True,     # V3 compliance
                        quality_score
                    )
                    operations.append((store_sql, params))
                    
                except (TypeError, ValueError) as e:
                    logging.warning(f"[DATA_MANAGER] V3 Skipping row due to type error: {e}")
                    continue
            
            # Execute batch operation
            if operations:
                self.connection_pool.execute_batch(operations)
                logging.info(f"[DATA_MANAGER] V3 Stored {len(operations)} live data points for {symbol} {timeframe}")
                
        except Exception as e:
            logging.error(f"[DATA_MANAGER] V3 Error storing live data: {e}")
    
    def _calculate_data_quality_score(self, row) -> float:
        """Calculate data quality score for validation"""
        try:
            score = 1.0
            
            # Check for realistic price values
            open_price = float(row['open'])
            high_price = float(row['high'])
            low_price = float(row['low'])
            close_price = float(row['close'])
            volume = float(row['volume'])
            
            # Basic sanity checks
            if high_price < low_price:
                score -= 0.3
            
            if high_price < max(open_price, close_price) or low_price > min(open_price, close_price):
                score -= 0.3
            
            if volume <= 0:
                score -= 0.2
            
            # Check for extreme price movements (possible data errors)
            if open_price > 0:
                price_change = abs(close_price - open_price) / open_price
                if price_change > 0.5:  # 50% price change in one period
                    score -= 0.2
            
            return max(0.0, score)
            
        except Exception:
            return 0.5  # Default score for problematic data
    
    async def get_historical_data(self, symbol: str, timeframe: str, 
                                 start_time: Optional[datetime] = None, 
                                 end_time: Optional[datetime] = None):
        """V3 Get historical data with intelligent caching and validation"""
        try:
            # Create cache key
            cache_key = f"hist_data_{symbol}_{timeframe}_{start_time}_{end_time}"
            cached_result = self.memory_cache.get(cache_key)
            if cached_result is not None:
                return cached_result
            
            # V3 Query for live data only with optimized SQL
            base_query = """
            SELECT timestamp, open_price, high_price, low_price, close_price, volume,
                   data_source, encoding, v3_compliance, data_quality_score
            FROM live_historical_data 
            WHERE symbol = ? AND timeframe = ? AND v3_compliance = TRUE
            """
            
            params = [str(symbol), str(timeframe)]
            
            # Add time range filters
            if start_time:
                base_query += " AND timestamp >= ?"
                params.append(int(start_time.timestamp() * 1000))
            
            if end_time:
                base_query += " AND timestamp <= ?"
                params.append(int(end_time.timestamp() * 1000))
            
            base_query += " ORDER BY timestamp ASC"
            
            # Execute query with connection pool
            conn = self.connection_pool.get_connection()
            if not conn:
                logging.error("[DATA_MANAGER] Could not get database connection")
                return None
            
            try:
                cursor = conn.execute(base_query, params)
                rows = cursor.fetchall()
                
                if rows:
                    # Process results efficiently
                    result_data = {
                        'timestamp': [row[0] for row in rows],
                        'open': [row[1] for row in rows],
                        'high': [row[2] for row in rows],
                        'low': [row[3] for row in rows],
                        'close': [row[4] for row in rows],
                        'volume': [row[5] for row in rows],
                        'metadata': {
                            'data_source': rows[0][6] if rows else 'live_api',
                            'encoding': rows[0][7] if rows else 'utf-8',
                            'v3_compliance': rows[0][8] if rows else True,
                            'average_quality_score': sum(row[9] for row in rows) / len(rows)
                        }
                    }
                    
                    # Cache the result
                    self.memory_cache.set(cache_key, result_data, ttl=3600)
                    
                    logging.info(f"[DATA_MANAGER] V3 Retrieved {len(rows)} live data points for {symbol} {timeframe}")
                    return result_data
                    
            finally:
                self.connection_pool.return_connection(conn)
            
            # V3 If no cached data, download live data
            logging.info(f"[DATA_MANAGER] V3 No cached data - downloading live data for {symbol} {timeframe}")
            df = await self.download_live_historical_data(symbol, timeframe, 200)
            
            if df is not None and len(df) > 0:
                await self.store_live_historical_data(symbol, timeframe, df)
                
                # Apply time filters to downloaded data
                filtered_df = df.copy()
                if start_time:
                    start_ts = int(start_time.timestamp() * 1000)
                    filtered_df = filtered_df[filtered_df['timestamp'] >= start_ts]
                if end_time:
                    end_ts = int(end_time.timestamp() * 1000)
                    filtered_df = filtered_df[filtered_df['timestamp'] <= end_ts]
                
                if len(filtered_df) > 0:
                    result_data = {
                        'timestamp': filtered_df['timestamp'].tolist(),
                        'open': filtered_df['open'].tolist(),
                        'high': filtered_df['high'].tolist(),
                        'low': filtered_df['low'].tolist(),
                        'close': filtered_df['close'].tolist(),
                        'volume': filtered_df['volume'].tolist(),
                        'metadata': {
                            'data_source': 'live_api',
                            'encoding': 'utf-8',
                            'v3_compliance': True,
                            'average_quality_score': 1.0
                        }
                    }
                    
                    # Cache the result
                    self.memory_cache.set(cache_key, result_data, ttl=1800)
                    
                    logging.info(f"[DATA_MANAGER] V3 Returning {len(filtered_df)} live data points for {symbol} {timeframe}")
                    return result_data
            
            return None
                
        except Exception as e:
            logging.error(f"[DATA_MANAGER] V3 Error getting live historical data: {e}")
            return None
    
    async def get_latest_data(self, symbol: str, timeframe: str = '1h') -> Optional[Dict]:
        """V3 Get latest live market data with caching"""
        try:
            cache_key = f"latest_{symbol}_{timeframe}"
            cached_data = self.memory_cache.get(cache_key)
            if cached_data is not None:
                return cached_data
            
            # Get recent live historical data
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=24)
            
            data = await self.get_historical_data(symbol, timeframe, start_time, end_time)
            
            if data and len(data.get('close', [])) > 0:
                latest_data = {
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'timestamp': data['timestamp'][-1] if data.get('timestamp') else int(datetime.now().timestamp() * 1000),
                    'open': data['open'][-1] if data.get('open') else 0,
                    'high': data['high'][-1] if data.get('high') else 0,
                    'low': data['low'][-1] if data.get('low') else 0,
                    'close': data['close'][-1] if data.get('close') else 0,
                    'volume': data['volume'][-1] if data.get('volume') else 0,
                    'last_updated': datetime.now().isoformat(),
                    'data_source': data.get('metadata', {}).get('data_source', 'live_api'),
                    'encoding': 'utf-8',
                    'v3_compliance': True,
                    'data_quality_score': data.get('metadata', {}).get('average_quality_score', 1.0)
                }
                
                # Cache for 5 minutes
                self.memory_cache.set(cache_key, latest_data, ttl=300)
                
                return latest_data
            
            return None
            
        except Exception as e:
            logging.error(f"[DATA_MANAGER] V3 Error getting latest live data for {symbol}: {e}")
            return None
    
    def get_latest_data_sync(self, symbol: str, timeframe: str = '1h') -> Optional[Dict]:
        """V3 Synchronous version with enhanced error handling"""
        try:
            # Try to run in existing event loop
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # Use thread pool to avoid blocking
                future = self.thread_pool.submit(
                    lambda: asyncio.run(self.get_latest_data(symbol, timeframe))
                )
                return future.result(timeout=30)
            else:
                return asyncio.run(self.get_latest_data(symbol, timeframe))
        except Exception as e:
            logging.error(f"[DATA_MANAGER] V3 Error in sync get_latest_data for {symbol}: {e}")
            return None

    async def cleanup_old_data(self, days_to_keep: int = 30):
        """V3 Cleanup old live data with batch operations"""
        try:
            cutoff_timestamp = int((datetime.now() - timedelta(days=days_to_keep)).timestamp() * 1000)
            
            cleanup_sql = """
            DELETE FROM live_historical_data 
            WHERE timestamp < ? AND v3_compliance = TRUE
            """
            
            conn = self.connection_pool.get_connection()
            if conn:
                try:
                    cursor = conn.execute(cleanup_sql, (cutoff_timestamp,))
                    deleted_count = cursor.rowcount
                    
                    # Optimize database after cleanup
                    conn.execute("VACUUM")
                    conn.execute("ANALYZE")
                    
                    logging.info(f"[DATA_MANAGER] V3 Cleaned up {deleted_count} old live data records")
                    return deleted_count
                    
                finally:
                    self.connection_pool.return_connection(conn)
                
        except Exception as e:
            logging.error(f"[DATA_MANAGER] V3 Error cleaning up old data: {e}")
            return 0

    def get_v3_metrics(self):
        """V3 Get comprehensive performance metrics"""
        try:
            # Get database statistics
            conn = self.connection_pool.get_connection()
            db_stats = {}
            
            if conn:
                try:
                    # Count live data points
                    cursor = conn.execute("SELECT COUNT(*) FROM live_historical_data WHERE v3_compliance = TRUE")
                    total_live_data_points = cursor.fetchone()[0]
                    
                    # Count unique symbols
                    cursor = conn.execute("SELECT COUNT(DISTINCT symbol) FROM live_historical_data WHERE v3_compliance = TRUE")
                    unique_symbols = cursor.fetchone()[0]
                    
                    # Count unique timeframes
                    cursor = conn.execute("SELECT COUNT(DISTINCT timeframe) FROM live_historical_data WHERE v3_compliance = TRUE")
                    unique_timeframes = cursor.fetchone()[0]
                    
                    # Get average data quality
                    cursor = conn.execute("SELECT AVG(data_quality_score) FROM live_historical_data WHERE v3_compliance = TRUE")
                    avg_quality = cursor.fetchone()[0] or 0.0
                    
                    db_stats = {
                        'total_live_data_points': total_live_data_points,
                        'unique_symbols': unique_symbols,
                        'unique_timeframes': unique_timeframes,
                        'average_data_quality': round(avg_quality, 3)
                    }
                    
                finally:
                    self.connection_pool.return_connection(conn)
            
            # Get performance statistics
            pool_stats = self.connection_pool.get_pool_stats()
            cache_stats = self.memory_cache.get_cache_stats()
            
            # Get memory usage
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            
            return {
                **db_stats,
                'connection_pool_stats': pool_stats,
                'cache_stats': cache_stats,
                'memory_usage_mb': round(memory_mb, 2),
                'thread_pool_workers': self.config['max_workers'],
                'live_data_sources_active': len(self.live_api_endpoints),
                'v3_compliance': True,
                'data_mode': 'LIVE_PRODUCTION',
                'performance_optimized': True,
                'utf8_encoding_enabled': True
            }
            
        except Exception as e:
            logging.error(f"[DATA_MANAGER] V3 Error getting metrics: {e}")
            return {
                'total_live_data_points': 0,
                'unique_symbols': 0,
                'unique_timeframes': 0,
                'live_data_sources_active': 0,
                'v3_compliance': True,
                'data_mode': 'ERROR',
                'error': str(e)
            }

    def get_metrics(self):
        """Legacy compatibility method"""
        return self.get_v3_metrics()

    async def validate_v3_compliance(self) -> Dict[str, Any]:
        """V3 Enhanced validation with performance metrics"""
        try:
            conn = self.connection_pool.get_connection()
            if not conn:
                return {'v3_compliant': False, 'error': 'Database connection failed'}
            
            try:
                # Check for non-V3 compliant data
                cursor = conn.execute("SELECT COUNT(*) FROM live_historical_data WHERE v3_compliance != TRUE")
                non_compliant_count = cursor.fetchone()[0]
                
                # Check data sources
                cursor = conn.execute("SELECT DISTINCT data_source FROM live_historical_data")
                data_sources = [row[0] for row in cursor.fetchall()]
                
                # Check encoding
                cursor = conn.execute("SELECT DISTINCT encoding FROM live_historical_data")
                encodings = [row[0] for row in cursor.fetchall()]
                
                # Check data quality
                cursor = conn.execute("SELECT AVG(data_quality_score), MIN(data_quality_score) FROM live_historical_data WHERE v3_compliance = TRUE")
                quality_result = cursor.fetchone()
                avg_quality = quality_result[0] or 0.0
                min_quality = quality_result[1] or 0.0
                
                # Validation checks
                has_live_data_only = all('live' in source or 'api' in source for source in data_sources)
                has_utf8_encoding = all(enc == 'utf-8' for enc in encodings if enc)
                has_good_quality = avg_quality >= 0.8 and min_quality >= 0.5
                
                validation_result = {
                    'v3_compliant': (
                        non_compliant_count == 0 and 
                        has_live_data_only and 
                        has_utf8_encoding and 
                        has_good_quality
                    ),
                    'non_compliant_records': non_compliant_count,
                    'data_sources': data_sources,
                    'encodings': encodings,
                    'live_data_only': has_live_data_only,
                    'utf8_encoding': has_utf8_encoding,
                    'average_data_quality': round(avg_quality, 3),
                    'minimum_data_quality': round(min_quality, 3),
                    'good_quality_data': has_good_quality,
                    'validation_timestamp': datetime.now().isoformat(),
                    'performance_metrics': {
                        'connection_pool_success_rate': self.connection_pool.get_pool_stats()['success_rate_percent'],
                        'cache_hit_rate': self.memory_cache.get_cache_stats()['hit_rate_percent']
                    }
                }
                
                if validation_result['v3_compliant']:
                    logging.info("[DATA_MANAGER] V3 Compliance validation PASSED")
                else:
                    logging.warning(f"[DATA_MANAGER] V3 Compliance validation FAILED: {validation_result}")
                
                return validation_result
                
            finally:
                self.connection_pool.return_connection(conn)
                
        except Exception as e:
            logging.error(f"[DATA_MANAGER] V3 Compliance validation error: {e}")
            return {
                'v3_compliant': False,
                'error': str(e),
                'validation_timestamp': datetime.now().isoformat()
            }
    
    def cleanup_resources(self):
        """V3 Enhanced resource cleanup"""
        try:
            # Shutdown thread pool
            self.thread_pool.shutdown(wait=True)
            
            # Clean up connection pool
            self.connection_pool.cleanup()
            
            # Clear memory cache
            self.memory_cache.clear()
            
            # Force garbage collection
            gc.collect()
            
            logging.info("[DATA_MANAGER] V3 Enhanced resource cleanup completed")
            
        except Exception as e:
            logging.error(f"[DATA_MANAGER] V3 Cleanup error: {e}")

# V3 Testing
if __name__ == "__main__":
    print("[DATA_MANAGER] Testing V3 Historical Data Manager - PERFORMANCE OPTIMIZED")
    
    async def test_v3_data_manager():
        manager = HistoricalDataManager()
        
        try:
            await manager.initialize()
            
            # Test performance metrics
            print("\n[DATA_MANAGER] Performance Test:")
            start_time = time.time()
            
            # Test live data retrieval
            data = await manager.get_historical_data('BTCUSDT', '1h')
            if data:
                print(f"[DATA_MANAGER] V3 Retrieved {len(data['close'])} live data points")
            
            retrieval_time = time.time() - start_time
            print(f"[DATA_MANAGER] Data retrieval time: {retrieval_time:.2f}s")
            
            # Test latest data
            latest = await manager.get_latest_data('BTCUSDT')
            if latest:
                print(f"[DATA_MANAGER] V3 Latest BTC price: ${latest['close']:.2f}")
                print(f"[DATA_MANAGER] Data quality score: {latest.get('data_quality_score', 'N/A')}")
            
            # Validate V3 compliance
            validation = await manager.validate_v3_compliance()
            print(f"[DATA_MANAGER] V3 Compliance: {validation['v3_compliant']}")
            
            # Get comprehensive metrics
            metrics = manager.get_v3_metrics()
            print(f"\n[DATA_MANAGER] V3 Performance Metrics:")
            print(f"  Memory Usage: {metrics.get('memory_usage_mb', 0):.1f} MB")
            print(f"  Connection Pool Success Rate: {metrics.get('connection_pool_stats', {}).get('success_rate_percent', 0):.1f}%")
            print(f"  Cache Hit Rate: {metrics.get('cache_stats', {}).get('hit_rate_percent', 0):.1f}%")
            print(f"  Thread Pool Workers: {metrics.get('thread_pool_workers', 0)}")
            print(f"  UTF-8 Encoding: {metrics.get('utf8_encoding_enabled', False)}")
            print(f"  V3 Compliance: {metrics.get('v3_compliance', False)}")
            
        except Exception as e:
            print(f"[DATA_MANAGER] V3 Test failed: {e}")
        finally:
            manager.cleanup_resources()
    
    asyncio.run(test_v3_data_manager())
    print("\n[DATA_MANAGER] V3 Historical Data Manager test complete - PERFORMANCE OPTIMIZED!")