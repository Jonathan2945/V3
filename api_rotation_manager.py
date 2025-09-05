#!/usr/bin/env python3
"""
V3 API Rotation Manager - Performance Optimized
Enhanced with caching, connection pooling, and server optimization
"""

import asyncio
import time
import logging
import threading
import json
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict, deque
from functools import lru_cache, wraps
import concurrent.futures
import hashlib
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import psutil

class APICache:
    """High-performance caching system for API responses"""
    
    def __init__(self, max_size: int = 2000, ttl_seconds: int = 300):
        self.cache = {}
        self.timestamps = {}
        self.access_counts = {}
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self.lock = threading.RLock()
        
    def _cleanup_expired(self):
        """Remove expired cache entries"""
        current_time = time.time()
        expired_keys = [
            key for key, timestamp in self.timestamps.items()
            if current_time - timestamp > self.ttl_seconds
        ]
        for key in expired_keys:
            self.cache.pop(key, None)
            self.timestamps.pop(key, None)
            self.access_counts.pop(key, None)
    
    def get(self, key: str) -> Optional[Any]:
        """Get cached value if not expired"""
        with self.lock:
            self._cleanup_expired()
            if key in self.cache:
                self.access_counts[key] = self.access_counts.get(key, 0) + 1
                return self.cache[key]
            return None
    
    def set(self, key: str, value: Any):
        """Set cache value with timestamp"""
        with self.lock:
            if len(self.cache) >= self.max_size:
                self._cleanup_expired()
                if len(self.cache) >= self.max_size:
                    # Remove least accessed entries
                    least_accessed = min(self.access_counts.keys(), key=self.access_counts.get)
                    self.cache.pop(least_accessed, None)
                    self.timestamps.pop(least_accessed, None)
                    self.access_counts.pop(least_accessed, None)
            
            self.cache[key] = value
            self.timestamps[key] = time.time()
            self.access_counts[key] = 1

def cache_api_response(ttl_seconds: int = 300):
    """Decorator for caching API responses"""
    def decorator(func):
        cache = APICache(ttl_seconds=ttl_seconds)
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Create cache key from function name and arguments
            cache_key = f"{func.__name__}_{hashlib.md5(str(args).encode() + str(kwargs).encode()).hexdigest()}"
            
            # Try to get from cache first
            cached_result = cache.get(cache_key)
            if cached_result is not None:
                return cached_result
            
            # If not in cache, execute function and cache result
            result = func(*args, **kwargs)
            if result is not None:  # Only cache successful responses
                cache.set(cache_key, result)
            return result
        
        return wrapper
    return decorator

class ConnectionPool:
    """Optimized connection pool for API requests"""
    
    def __init__(self, max_connections: int = 20, max_retries: int = 3):
        self.session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(
            pool_connections=max_connections,
            pool_maxsize=max_connections,
            max_retries=retry_strategy
        )
        
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        
        # Default headers
        self.session.headers.update({
            'User-Agent': 'V3TradingSystem/1.0',
            'Connection': 'keep-alive'
        })
    
    def get(self, url: str, headers: Dict = None, timeout: int = 30) -> requests.Response:
        """Make GET request with connection pooling"""
        try:
            response = self.session.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")
            raise
    
    def post(self, url: str, data: Dict = None, headers: Dict = None, timeout: int = 30) -> requests.Response:
        """Make POST request with connection pooling"""
        try:
            response = self.session.post(url, json=data, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")
            raise

class APIRotationManager:
    """
    Enhanced API Rotation Manager with Performance Optimization
    Optimized for 8 vCPU / 24GB server specifications
    """
    
    def __init__(self, config_manager=None):
        self.config = config_manager
        
        # Performance optimization
        self.api_cache = APICache(max_size=3000, ttl_seconds=300)
        self.connection_pool = ConnectionPool(max_connections=25)
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=8)
        
        # API rotation state
        self.api_keys = self._load_api_keys()
        self.api_status = {}
        self.rotation_strategy = 'ROUND_ROBIN'
        self.rate_limits = defaultdict(dict)
        self.current_api_index = defaultdict(int)
        
        # Performance tracking
        self.api_performance = defaultdict(lambda: {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'avg_response_time': 0.0,
            'last_used': None,
            'error_rate': 0.0
        })
        
        # Rate limiting
        self.request_history = defaultdict(lambda: deque(maxlen=1000))
        self.cooldown_periods = defaultdict(float)
        
        # Database connection pool for metrics
        self.db_pool = self._create_db_pool()
        
        # Start background monitoring
        self._start_background_monitoring()
    
    def _create_db_pool(self):
        """Create database connection pool"""
        try:
            # Simple connection pool implementation
            pool = []
            for _ in range(5):  # 5 connections in pool
                conn = sqlite3.connect('api_metrics.db', check_same_thread=False)
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS api_metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        api_service TEXT,
                        api_key_hash TEXT,
                        timestamp DATETIME,
                        response_time REAL,
                        status_code INTEGER,
                        success BOOLEAN
                    )
                ''')
                conn.commit()
                pool.append(conn)
            return pool
        except Exception as e:
            logging.error(f"Database pool creation error: {e}")
            return []
    
    def _get_db_connection(self):
        """Get database connection from pool"""
        if self.db_pool:
            return self.db_pool.pop()
        return None
    
    def _return_db_connection(self, conn):
        """Return database connection to pool"""
        if conn and len(self.db_pool) < 5:
            self.db_pool.append(conn)
    
    def _load_api_keys(self) -> Dict[str, List[Dict]]:
        """Load API keys from configuration with caching"""
        try:
            api_keys = {
                'binance': [],
                'alpha_vantage': [],
                'news_api': [],
                'fred': [],
                'twitter': [],
                'reddit': []
            }
            
            if self.config:
                # Binance keys
                for i in range(1, 4):
                    key = self.config.get(f'BINANCE_API_KEY_{i}')
                    secret = self.config.get(f'BINANCE_API_SECRET_{i}')
                    if key and secret:
                        api_keys['binance'].append({
                            'key': key,
                            'secret': secret,
                            'index': i,
                            'weight_limit': 1200,
                            'requests_per_minute': 1200
                        })
                
                # Alpha Vantage keys
                for i in range(1, 4):
                    key = self.config.get(f'ALPHA_VANTAGE_API_KEY_{i}')
                    if key:
                        api_keys['alpha_vantage'].append({
                            'key': key,
                            'index': i,
                            'requests_per_minute': 5
                        })
                
                # News API keys
                for i in range(1, 4):
                    key = self.config.get(f'NEWS_API_KEY_{i}')
                    if key:
                        api_keys['news_api'].append({
                            'key': key,
                            'index': i,
                            'requests_per_hour': 1000
                        })
            
            logging.info(f"Loaded API keys: {sum(len(keys) for keys in api_keys.values())} total")
            return api_keys
            
        except Exception as e:
            logging.error(f"Error loading API keys: {e}")
            return {}
    
    @cache_api_response(ttl_seconds=60)
    def get_optimal_api_key(self, service: str) -> Optional[Dict]:
        """Get optimal API key for service with caching"""
        try:
            if service not in self.api_keys or not self.api_keys[service]:
                return None
            
            available_keys = []
            current_time = time.time()
            
            for api_key_info in self.api_keys[service]:
                key_id = f"{service}_{api_key_info['index']}"
                
                # Check if key is in cooldown
                if key_id in self.cooldown_periods and current_time < self.cooldown_periods[key_id]:
                    continue
                
                # Check rate limits
                if self._is_rate_limited(key_id, api_key_info):
                    continue
                
                available_keys.append(api_key_info)
            
            if not available_keys:
                return None
            
            # Select best key based on performance
            best_key = self._select_best_performing_key(service, available_keys)
            return best_key
            
        except Exception as e:
            logging.error(f"Error getting optimal API key: {e}")
            return None
    
    @cache_api_response(ttl_seconds=30)
    def _is_rate_limited(self, key_id: str, api_key_info: Dict) -> bool:
        """Check if API key is rate limited with caching"""
        try:
            current_time = time.time()
            
            # Get request history for this key
            history = self.request_history[key_id]
            
            # Remove old requests (older than rate limit window)
            rate_limit_window = 60  # 1 minute default
            if 'requests_per_minute' in api_key_info:
                limit = api_key_info['requests_per_minute']
                recent_requests = sum(1 for timestamp in history if current_time - timestamp < rate_limit_window)
                return recent_requests >= limit
            
            if 'requests_per_hour' in api_key_info:
                rate_limit_window = 3600  # 1 hour
                limit = api_key_info['requests_per_hour']
                recent_requests = sum(1 for timestamp in history if current_time - timestamp < rate_limit_window)
                return recent_requests >= limit
            
            return False
            
        except Exception as e:
            logging.error(f"Error checking rate limit: {e}")
            return True  # Conservative approach
    
    def _select_best_performing_key(self, service: str, available_keys: List[Dict]) -> Dict:
        """Select the best performing API key"""
        try:
            if len(available_keys) == 1:
                return available_keys[0]
            
            # Score keys based on performance metrics
            scored_keys = []
            for key_info in available_keys:
                key_id = f"{service}_{key_info['index']}"
                perf = self.api_performance[key_id]
                
                # Calculate score (higher is better)
                success_rate = perf['successful_requests'] / max(perf['total_requests'], 1)
                response_time_score = 1.0 / (perf['avg_response_time'] + 0.1)  # Avoid division by zero
                
                score = success_rate * 0.7 + response_time_score * 0.3
                scored_keys.append((score, key_info))
            
            # Sort by score and return best
            scored_keys.sort(reverse=True)
            return scored_keys[0][1]
            
        except Exception as e:
            logging.error(f"Error selecting best key: {e}")
            return available_keys[0]  # Fallback to first available
    
    async def make_api_request_async(self, service: str, endpoint: str, method: str = 'GET', 
                                   params: Dict = None, data: Dict = None) -> Optional[Dict]:
        """Make asynchronous API request with optimization"""
        try:
            # Get optimal API key
            api_key_info = self.get_optimal_api_key(service)
            if not api_key_info:
                raise Exception(f"No available API keys for {service}")
            
            key_id = f"{service}_{api_key_info['index']}"
            
            # Prepare request
            url, headers = self._prepare_request(service, endpoint, api_key_info, params)
            
            # Execute request asynchronously
            loop = asyncio.get_event_loop()
            start_time = time.time()
            
            if method.upper() == 'GET':
                response = await loop.run_in_executor(
                    self.executor,
                    self.connection_pool.get,
                    url,
                    headers
                )
            else:
                response = await loop.run_in_executor(
                    self.executor,
                    self.connection_pool.post,
                    url,
                    data,
                    headers
                )
            
            response_time = time.time() - start_time
            
            # Update metrics
            self._update_api_metrics(key_id, response_time, response.status_code, True)
            
            # Update request history
            self.request_history[key_id].append(time.time())
            
            return response.json()
            
        except Exception as e:
            if 'key_id' in locals():
                self._update_api_metrics(key_id, 0, 0, False)
            logging.error(f"API request failed: {e}")
            return None
    
    @cache_api_response(ttl_seconds=60)
    def _prepare_request(self, service: str, endpoint: str, api_key_info: Dict, params: Dict = None) -> Tuple[str, Dict]:
        """Prepare API request URL and headers with caching"""
        try:
            headers = {'Content-Type': 'application/json'}
            
            if service == 'binance':
                base_url = 'https://api.binance.com'
                headers['X-MBX-APIKEY'] = api_key_info['key']
                url = f"{base_url}{endpoint}"
                
            elif service == 'alpha_vantage':
                base_url = 'https://www.alphavantage.co/query'
                if params:
                    params['apikey'] = api_key_info['key']
                else:
                    params = {'apikey': api_key_info['key']}
                url = f"{base_url}?{self._build_query_string(params)}"
                
            elif service == 'news_api':
                base_url = 'https://newsapi.org/v2'
                headers['X-API-Key'] = api_key_info['key']
                url = f"{base_url}{endpoint}"
                
            else:
                raise Exception(f"Unknown service: {service}")
            
            return url, headers
            
        except Exception as e:
            logging.error(f"Error preparing request: {e}")
            raise
    
    def _build_query_string(self, params: Dict) -> str:
        """Build query string from parameters"""
        if not params:
            return ""
        return "&".join([f"{k}={v}" for k, v in params.items()])
    
    def _update_api_metrics(self, key_id: str, response_time: float, status_code: int, success: bool):
        """Update API performance metrics"""
        try:
            perf = self.api_performance[key_id]
            perf['total_requests'] += 1
            perf['last_used'] = datetime.now()
            
            if success:
                perf['successful_requests'] += 1
                # Update rolling average response time
                if perf['avg_response_time'] == 0:
                    perf['avg_response_time'] = response_time
                else:
                    perf['avg_response_time'] = (perf['avg_response_time'] * 0.9) + (response_time * 0.1)
            else:
                perf['failed_requests'] += 1
            
            perf['error_rate'] = perf['failed_requests'] / perf['total_requests']
            
            # Store in database asynchronously
            self._store_metrics_async(key_id, response_time, status_code, success)
            
        except Exception as e:
            logging.error(f"Error updating metrics: {e}")
    
    def _store_metrics_async(self, key_id: str, response_time: float, status_code: int, success: bool):
        """Store metrics in database asynchronously"""
        def store_metrics():
            conn = self._get_db_connection()
            if conn:
                try:
                    conn.execute('''
                        INSERT INTO api_metrics (api_service, api_key_hash, timestamp, response_time, status_code, success)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (key_id, hashlib.md5(key_id.encode()).hexdigest()[:8], datetime.now(), response_time, status_code, success))
                    conn.commit()
                except Exception as e:
                    logging.error(f"Database storage error: {e}")
                finally:
                    self._return_db_connection(conn)
        
        self.executor.submit(store_metrics)
    
    def _start_background_monitoring(self):
        """Start background monitoring and optimization"""
        def monitor_worker():
            while True:
                try:
                    self._monitor_api_health()
                    self._optimize_rotation_strategy()
                    self._cleanup_old_data()
                    time.sleep(60)  # Run every minute
                except Exception as e:
                    logging.error(f"Background monitoring error: {e}")
                    time.sleep(30)
        
        thread = threading.Thread(target=monitor_worker, daemon=True)
        thread.start()
    
    def _monitor_api_health(self):
        """Monitor API health and adjust cooldowns"""
        try:
            current_time = time.time()
            
            for key_id, perf in self.api_performance.items():
                # Put poorly performing keys in cooldown
                if perf['error_rate'] > 0.5 and perf['total_requests'] > 10:
                    cooldown_duration = 300  # 5 minutes
                    self.cooldown_periods[key_id] = current_time + cooldown_duration
                    logging.warning(f"API key {key_id} put in cooldown due to high error rate")
                
                # Remove expired cooldowns
                if key_id in self.cooldown_periods and current_time > self.cooldown_periods[key_id]:
                    del self.cooldown_periods[key_id]
                    logging.info(f"API key {key_id} cooldown expired")
        
        except Exception as e:
            logging.error(f"API health monitoring error: {e}")
    
    def _optimize_rotation_strategy(self):
        """Optimize rotation strategy based on performance"""
        try:
            # Analyze performance patterns and adjust strategy if needed
            total_requests = sum(perf['total_requests'] for perf in self.api_performance.values())
            
            if total_requests > 1000:  # Only optimize after sufficient data
                # Calculate overall performance metrics
                avg_success_rate = sum(
                    perf['successful_requests'] / max(perf['total_requests'], 1) 
                    for perf in self.api_performance.values()
                ) / max(len(self.api_performance), 1)
                
                if avg_success_rate < 0.9:  # Less than 90% success
                    logging.warning(f"Low overall success rate: {avg_success_rate:.2%}")
                    # Could implement strategy changes here
        
        except Exception as e:
            logging.error(f"Strategy optimization error: {e}")
    
    def _cleanup_old_data(self):
        """Clean up old performance data"""
        try:
            current_time = time.time()
            cutoff_time = current_time - 3600  # 1 hour ago
            
            # Clean up request history
            for key_id in self.request_history:
                self.request_history[key_id] = deque(
                    [ts for ts in self.request_history[key_id] if ts > cutoff_time],
                    maxlen=1000
                )
        
        except Exception as e:
            logging.error(f"Data cleanup error: {e}")
    
    def get_rotation_status(self) -> Dict[str, Any]:
        """Get current rotation status and performance metrics"""
        try:
            status = {
                'total_api_keys': sum(len(keys) for keys in self.api_keys.values()),
                'active_keys': len([k for k, p in self.api_performance.items() if p['total_requests'] > 0]),
                'keys_in_cooldown': len(self.cooldown_periods),
                'rotation_strategy': self.rotation_strategy,
                'performance_summary': {}
            }
            
            # Add performance summary
            for service, keys in self.api_keys.items():
                service_perf = {
                    'total_keys': len(keys),
                    'avg_success_rate': 0.0,
                    'avg_response_time': 0.0,
                    'total_requests': 0
                }
                
                service_requests = 0
                service_success = 0
                service_response_time = 0
                
                for key_info in keys:
                    key_id = f"{service}_{key_info['index']}"
                    perf = self.api_performance[key_id]
                    service_requests += perf['total_requests']
                    service_success += perf['successful_requests']
                    service_response_time += perf['avg_response_time']
                
                if service_requests > 0:
                    service_perf['avg_success_rate'] = service_success / service_requests
                    service_perf['avg_response_time'] = service_response_time / len(keys)
                    service_perf['total_requests'] = service_requests
                
                status['performance_summary'][service] = service_perf
            
            return status
            
        except Exception as e:
            logging.error(f"Error getting rotation status: {e}")
            return {}
    
    def optimize_for_server_specs(self):
        """Optimize for 8 vCPU / 24GB server specifications"""
        try:
            # Adjust thread pool size
            cpu_count = psutil.cpu_count()
            optimal_workers = min(cpu_count, 10)
            
            if self.executor._max_workers != optimal_workers:
                self.executor.shutdown(wait=False)
                self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=optimal_workers)
            
            # Adjust cache sizes based on available memory
            memory_gb = psutil.virtual_memory().total / (1024**3)
            if memory_gb >= 24:
                self.api_cache.max_size = 5000
                # Increase connection pool size
                self.connection_pool = ConnectionPool(max_connections=50)
            
            logging.info(f"API rotation optimized for {cpu_count} CPUs, {memory_gb:.1f}GB RAM")
            
        except Exception as e:
            logging.error(f"Server optimization error: {e}")

# Export main class
__all__ = ['APIRotationManager', 'APICache', 'ConnectionPool']

if __name__ == "__main__":
    # Performance test
    manager = APIRotationManager()
    manager.optimize_for_server_specs()
    
    # Test API rotation
    status = manager.get_rotation_status()
    print(f"API Rotation Status: {json.dumps(status, indent=2)}")