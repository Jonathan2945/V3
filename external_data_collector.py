#!/usr/bin/env python3
"""
V3 External Data Collector - Performance Optimized
Enhanced with database transaction management and data validation
"""

import asyncio
import time
import logging
import threading
import sqlite3
import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Union
from collections import defaultdict, deque
from functools import lru_cache, wraps
import concurrent.futures
import hashlib
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import psutil
import queue
from dataclasses import dataclass
import pandas as pd
import numpy as np

@dataclass
class DataValidationResult:
    """Result of data validation"""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    cleaned_data: Optional[Dict] = None

class DataValidator:
    """Comprehensive data validation system"""
    
    @staticmethod
    def validate_price_data(data: Dict) -> DataValidationResult:
        """Validate price/OHLCV data"""
        errors = []
        warnings = []
        cleaned_data = data.copy()
        
        try:
            # Required fields for price data
            required_fields = ['symbol', 'price', 'timestamp']
            for field in required_fields:
                if field not in data:
                    errors.append(f"Missing required field: {field}")
            
            if errors:
                return DataValidationResult(False, errors, warnings)
            
            # Validate price is numeric and positive
            try:
                price = float(data['price'])
                if price <= 0:
                    errors.append("Price must be positive")
                cleaned_data['price'] = price
            except (ValueError, TypeError):
                errors.append("Price must be a valid number")
            
            # Validate timestamp
            try:
                if isinstance(data['timestamp'], str):
                    timestamp = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
                elif isinstance(data['timestamp'], (int, float)):
                    timestamp = datetime.fromtimestamp(data['timestamp'])
                else:
                    timestamp = data['timestamp']
                
                # Check if timestamp is too old or in future
                now = datetime.now()
                if timestamp < now - timedelta(days=30):
                    warnings.append("Timestamp is older than 30 days")
                elif timestamp > now + timedelta(minutes=5):
                    warnings.append("Timestamp is in the future")
                
                cleaned_data['timestamp'] = timestamp
                
            except (ValueError, TypeError):
                errors.append("Invalid timestamp format")
            
            # Validate symbol format
            symbol = str(data['symbol']).upper()
            if not re.match(r'^[A-Z]{3,10}USDT?$', symbol):
                warnings.append(f"Unusual symbol format: {symbol}")
            cleaned_data['symbol'] = symbol
            
            # Validate OHLCV data if present
            ohlcv_fields = ['open', 'high', 'low', 'close', 'volume']
            for field in ohlcv_fields:
                if field in data:
                    try:
                        value = float(data[field])
                        if value < 0:
                            errors.append(f"{field} cannot be negative")
                        cleaned_data[field] = value
                    except (ValueError, TypeError):
                        errors.append(f"{field} must be a valid number")
            
            # Logical validation for OHLCV
            if all(field in cleaned_data for field in ['open', 'high', 'low', 'close']):
                o, h, l, c = cleaned_data['open'], cleaned_data['high'], cleaned_data['low'], cleaned_data['close']
                if not (l <= o <= h and l <= c <= h):
                    errors.append("OHLC values are logically inconsistent")
            
            is_valid = len(errors) == 0
            return DataValidationResult(is_valid, errors, warnings, cleaned_data)
            
        except Exception as e:
            return DataValidationResult(False, [f"Validation error: {str(e)}"], warnings)
    
    @staticmethod
    def validate_news_data(data: Dict) -> DataValidationResult:
        """Validate news article data"""
        errors = []
        warnings = []
        cleaned_data = data.copy()
        
        try:
            # Required fields
            required_fields = ['title', 'content', 'source', 'published_at']
            for field in required_fields:
                if field not in data or not data[field]:
                    errors.append(f"Missing required field: {field}")
            
            if errors:
                return DataValidationResult(False, errors, warnings)
            
            # Clean and validate title
            title = str(data['title']).strip()
            if len(title) < 10:
                warnings.append("Title is very short")
            elif len(title) > 500:
                warnings.append("Title is very long")
                title = title[:500]  # Truncate
            cleaned_data['title'] = title
            
            # Clean and validate content
            content = str(data['content']).strip()
            if len(content) < 50:
                warnings.append("Content is very short")
            elif len(content) > 10000:
                content = content[:10000]  # Truncate long content
                warnings.append("Content was truncated")
            cleaned_data['content'] = content
            
            # Validate source
            source = str(data['source']).strip()
            if len(source) < 2:
                errors.append("Source must be specified")
            cleaned_data['source'] = source
            
            # Validate timestamp
            try:
                if isinstance(data['published_at'], str):
                    published_at = datetime.fromisoformat(data['published_at'].replace('Z', '+00:00'))
                else:
                    published_at = data['published_at']
                
                # Check if too old
                if published_at < datetime.now() - timedelta(days=7):
                    warnings.append("News article is older than 7 days")
                
                cleaned_data['published_at'] = published_at
                
            except (ValueError, TypeError):
                errors.append("Invalid published_at timestamp")
            
            # Sentiment analysis if present
            if 'sentiment' in data:
                try:
                    sentiment = float(data['sentiment'])
                    if not -1 <= sentiment <= 1:
                        warnings.append("Sentiment score outside expected range [-1, 1]")
                    cleaned_data['sentiment'] = sentiment
                except (ValueError, TypeError):
                    warnings.append("Invalid sentiment score, removing")
                    cleaned_data.pop('sentiment', None)
            
            is_valid = len(errors) == 0
            return DataValidationResult(is_valid, errors, warnings, cleaned_data)
            
        except Exception as e:
            return DataValidationResult(False, [f"Validation error: {str(e)}"], warnings)
    
    @staticmethod
    def validate_social_data(data: Dict) -> DataValidationResult:
        """Validate social media data"""
        errors = []
        warnings = []
        cleaned_data = data.copy()
        
        try:
            # Required fields
            required_fields = ['platform', 'content', 'timestamp', 'engagement']
            for field in required_fields:
                if field not in data:
                    errors.append(f"Missing required field: {field}")
            
            if errors:
                return DataValidationResult(False, errors, warnings)
            
            # Validate platform
            valid_platforms = ['twitter', 'reddit', 'telegram', 'discord']
            platform = str(data['platform']).lower()
            if platform not in valid_platforms:
                warnings.append(f"Unknown platform: {platform}")
            cleaned_data['platform'] = platform
            
            # Clean content
            content = str(data['content']).strip()
            if len(content) < 5:
                warnings.append("Content is very short")
            elif len(content) > 5000:
                content = content[:5000]
                warnings.append("Content was truncated")
            cleaned_data['content'] = content
            
            # Validate engagement metrics
            try:
                engagement = data['engagement']
                if isinstance(engagement, dict):
                    for metric, value in engagement.items():
                        try:
                            engagement[metric] = max(0, int(value))  # Ensure non-negative
                        except (ValueError, TypeError):
                            warnings.append(f"Invalid engagement metric: {metric}")
                            engagement[metric] = 0
                else:
                    engagement = max(0, int(engagement))
                cleaned_data['engagement'] = engagement
            except (ValueError, TypeError):
                errors.append("Invalid engagement data")
            
            # Validate timestamp
            try:
                if isinstance(data['timestamp'], str):
                    timestamp = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
                else:
                    timestamp = data['timestamp']
                
                if timestamp > datetime.now() + timedelta(minutes=5):
                    warnings.append("Timestamp is in the future")
                
                cleaned_data['timestamp'] = timestamp
                
            except (ValueError, TypeError):
                errors.append("Invalid timestamp")
            
            is_valid = len(errors) == 0
            return DataValidationResult(is_valid, errors, warnings, cleaned_data)
            
        except Exception as e:
            return DataValidationResult(False, [f"Validation error: {str(e)}"], warnings)

class DatabaseManager:
    """Enhanced database manager with proper transaction handling"""
    
    def __init__(self, db_path: str, max_connections: int = 10):
        self.db_path = db_path
        self.max_connections = max_connections
        self.connection_pool = queue.Queue(maxsize=max_connections)
        self.total_connections = 0
        self.lock = threading.Lock()
        
        # Transaction statistics
        self.transaction_stats = {
            'total_transactions': 0,
            'successful_transactions': 0,
            'failed_transactions': 0,
            'rollbacks': 0
        }
        
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Initialize database connection pool"""
        try:
            for _ in range(self.max_connections):
                conn = self._create_connection()
                if conn:
                    self.connection_pool.put(conn)
                    self.total_connections += 1
            
            logging.info(f"Database pool initialized with {self.total_connections} connections")
            
        except Exception as e:
            logging.error(f"Database pool initialization error: {e}")
    
    def _create_connection(self) -> Optional[sqlite3.Connection]:
        """Create new database connection with optimizations"""
        try:
            conn = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=30.0,
                isolation_level=None  # Enable autocommit mode
            )
            
            # Optimize database settings
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=NORMAL')
            conn.execute('PRAGMA cache_size=10000')
            conn.execute('PRAGMA temp_store=MEMORY')
            conn.execute('PRAGMA mmap_size=268435456')  # 256MB
            
            # Create tables
            self._create_tables(conn)
            
            return conn
            
        except Exception as e:
            logging.error(f"Database connection creation error: {e}")
            return None
    
    def _create_tables(self, conn: sqlite3.Connection):
        """Create all necessary tables"""
        try:
            # Price data table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS price_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    price REAL NOT NULL,
                    open_price REAL,
                    high_price REAL,
                    low_price REAL,
                    close_price REAL,
                    volume REAL,
                    timestamp DATETIME NOT NULL,
                    source TEXT NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, timestamp, source)
                )
            ''')
            
            # News data table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS news_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    content TEXT NOT NULL,
                    source TEXT NOT NULL,
                    url TEXT,
                    published_at DATETIME NOT NULL,
                    sentiment REAL,
                    relevance_score REAL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(title, source, published_at)
                )
            ''')
            
            # Social media data table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS social_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform TEXT NOT NULL,
                    content TEXT NOT NULL,
                    author TEXT,
                    engagement_count INTEGER,
                    engagement_data TEXT,
                    timestamp DATETIME NOT NULL,
                    sentiment REAL,
                    hashtags TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Economic data table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS economic_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    indicator TEXT NOT NULL,
                    value REAL NOT NULL,
                    period TEXT,
                    country TEXT,
                    timestamp DATETIME NOT NULL,
                    source TEXT NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(indicator, period, timestamp, source)
                )
            ''')
            
            # Data validation log
            conn.execute('''
                CREATE TABLE IF NOT EXISTS validation_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT NOT NULL,
                    record_id INTEGER,
                    validation_result TEXT NOT NULL,
                    errors TEXT,
                    warnings TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create indexes for performance
            indexes = [
                'CREATE INDEX IF NOT EXISTS idx_price_symbol_time ON price_data(symbol, timestamp)',
                'CREATE INDEX IF NOT EXISTS idx_news_published ON news_data(published_at)',
                'CREATE INDEX IF NOT EXISTS idx_social_platform_time ON social_data(platform, timestamp)',
                'CREATE INDEX IF NOT EXISTS idx_economic_indicator ON economic_data(indicator, timestamp)',
            ]
            
            for index_sql in indexes:
                conn.execute(index_sql)
            
            conn.commit()
            
        except Exception as e:
            logging.error(f"Table creation error: {e}")
    
    def get_connection(self) -> Optional[sqlite3.Connection]:
        """Get connection from pool"""
        try:
            if not self.connection_pool.empty():
                return self.connection_pool.get(timeout=5)
            
            # Create new connection if under limit
            with self.lock:
                if self.total_connections < self.max_connections:
                    conn = self._create_connection()
                    if conn:
                        self.total_connections += 1
                        return conn
            
            # Wait for available connection
            return self.connection_pool.get(timeout=10)
            
        except queue.Empty:
            logging.warning("No database connections available")
            return None
        except Exception as e:
            logging.error(f"Error getting connection: {e}")
            return None
    
    def return_connection(self, conn: sqlite3.Connection):
        """Return connection to pool"""
        try:
            if conn and not self.connection_pool.full():
                # Test connection is still valid
                conn.execute('SELECT 1')
                self.connection_pool.put(conn)
            elif conn:
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
    
    def execute_transaction(self, operations: List[Tuple[str, tuple]], commit: bool = True) -> bool:
        """Execute multiple operations in a single transaction"""
        conn = None
        try:
            conn = self.get_connection()
            if not conn:
                return False
            
            # Start explicit transaction
            conn.execute('BEGIN')
            
            results = []
            for sql, params in operations:
                cursor = conn.cursor()
                cursor.execute(sql, params)
                results.append(cursor.rowcount)
            
            if commit:
                conn.commit()
                self.transaction_stats['successful_transactions'] += 1
            else:
                conn.rollback()
                self.transaction_stats['rollbacks'] += 1
            
            self.transaction_stats['total_transactions'] += 1
            return True
            
        except Exception as e:
            self.transaction_stats['failed_transactions'] += 1
            if conn:
                try:
                    conn.rollback()
                    self.transaction_stats['rollbacks'] += 1
                except:
                    pass
            logging.error(f"Transaction error: {e}")
            return False
        finally:
            if conn:
                self.return_connection(conn)
    
    def insert_validated_data(self, table: str, data: Dict, validation_result: DataValidationResult) -> bool:
        """Insert data with validation logging"""
        try:
            if not validation_result.is_valid:
                logging.warning(f"Attempting to insert invalid data into {table}: {validation_result.errors}")
                return False
            
            cleaned_data = validation_result.cleaned_data or data
            
            # Prepare insert operations
            operations = []
            
            if table == 'price_data':
                operations.append((
                    '''INSERT OR REPLACE INTO price_data 
                       (symbol, price, open_price, high_price, low_price, close_price, volume, timestamp, source)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (
                        cleaned_data['symbol'],
                        cleaned_data['price'],
                        cleaned_data.get('open'),
                        cleaned_data.get('high'),
                        cleaned_data.get('low'),
                        cleaned_data.get('close'),
                        cleaned_data.get('volume'),
                        cleaned_data['timestamp'],
                        cleaned_data.get('source', 'unknown')
                    )
                ))
                
            elif table == 'news_data':
                operations.append((
                    '''INSERT OR REPLACE INTO news_data 
                       (title, content, source, url, published_at, sentiment, relevance_score)
                       VALUES (?, ?, ?, ?, ?, ?, ?)''',
                    (
                        cleaned_data['title'],
                        cleaned_data['content'],
                        cleaned_data['source'],
                        cleaned_data.get('url'),
                        cleaned_data['published_at'],
                        cleaned_data.get('sentiment'),
                        cleaned_data.get('relevance_score')
                    )
                ))
                
            elif table == 'social_data':
                engagement_data = cleaned_data.get('engagement', {})
                if isinstance(engagement_data, dict):
                    engagement_json = json.dumps(engagement_data)
                    engagement_count = sum(engagement_data.values()) if engagement_data else 0
                else:
                    engagement_json = None
                    engagement_count = int(engagement_data)
                
                operations.append((
                    '''INSERT INTO social_data 
                       (platform, content, author, engagement_count, engagement_data, timestamp, sentiment, hashtags)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                    (
                        cleaned_data['platform'],
                        cleaned_data['content'],
                        cleaned_data.get('author'),
                        engagement_count,
                        engagement_json,
                        cleaned_data['timestamp'],
                        cleaned_data.get('sentiment'),
                        cleaned_data.get('hashtags')
                    )
                ))
            
            # Add validation log entry
            operations.append((
                '''INSERT INTO validation_log 
                   (table_name, validation_result, errors, warnings)
                   VALUES (?, ?, ?, ?)''',
                (
                    table,
                    'valid' if validation_result.is_valid else 'invalid',
                    json.dumps(validation_result.errors) if validation_result.errors else None,
                    json.dumps(validation_result.warnings) if validation_result.warnings else None
                )
            ))
            
            # Execute transaction
            success = self.execute_transaction(operations, commit=True)
            
            if validation_result.warnings:
                logging.warning(f"Data inserted with warnings: {validation_result.warnings}")
            
            return success
            
        except Exception as e:
            logging.error(f"Error inserting validated data: {e}")
            return False
    
    def get_transaction_stats(self) -> Dict[str, Any]:
        """Get transaction statistics"""
        stats = self.transaction_stats.copy()
        if stats['total_transactions'] > 0:
            stats['success_rate'] = stats['successful_transactions'] / stats['total_transactions']
            stats['failure_rate'] = stats['failed_transactions'] / stats['total_transactions']
        else:
            stats['success_rate'] = 0
            stats['failure_rate'] = 0
        
        stats['active_connections'] = self.total_connections
        return stats

class ExternalDataCollector:
    """
    Enhanced External Data Collector with Performance Optimization
    Optimized for 8 vCPU / 24GB server specifications
    """
    
    def __init__(self, config_manager=None):
        self.config = config_manager
        
        # Database manager with connection pooling
        self.db_manager = DatabaseManager('external_data.db', max_connections=15)
        
        # Data validator
        self.validator = DataValidator()
        
        # Performance optimization
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=12)
        
        # HTTP session with connection pooling
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(
            pool_connections=25,
            pool_maxsize=25,
            max_retries=retry_strategy
        )
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        
        # Data collection state
        self.collection_stats = defaultdict(lambda: {
            'total_collected': 0,
            'successful_validations': 0,
            'failed_validations': 0,
            'database_inserts': 0,
            'last_collection': None
        })
        
        # Rate limiting
        self.rate_limits = defaultdict(lambda: deque(maxlen=100))
        
        # API keys rotation
        self.api_keys = self._load_api_keys()
        self.current_key_index = defaultdict(int)
        
        # Start background tasks
        self._start_background_tasks()
    
    def _load_api_keys(self) -> Dict[str, List[str]]:
        """Load API keys from configuration"""
        try:
            keys = {
                'alpha_vantage': [],
                'news_api': [],
                'fred': [],
                'twitter': [],
                'reddit': []
            }
            
            if self.config:
                # Alpha Vantage keys
                for i in range(1, 4):
                    key = self.config.get(f'ALPHA_VANTAGE_API_KEY_{i}')
                    if key:
                        keys['alpha_vantage'].append(key)
                
                # News API keys
                for i in range(1, 4):
                    key = self.config.get(f'NEWS_API_KEY_{i}')
                    if key:
                        keys['news_api'].append(key)
                
                # FRED keys
                for i in range(1, 4):
                    key = self.config.get(f'FRED_API_KEY_{i}')
                    if key:
                        keys['fred'].append(key)
                
                # Twitter bearer tokens
                for i in range(1, 4):
                    token = self.config.get(f'TWITTER_BEARER_TOKEN_{i}')
                    if token:
                        keys['twitter'].append(token)
                
                # Reddit credentials
                for i in range(1, 4):
                    client_id = self.config.get(f'REDDIT_CLIENT_ID_{i}')
                    client_secret = self.config.get(f'REDDIT_CLIENT_SECRET_{i}')
                    if client_id and client_secret:
                        keys['reddit'].append(f"{client_id}:{client_secret}")
            
            total_keys = sum(len(key_list) for key_list in keys.values())
            logging.info(f"Loaded {total_keys} external API keys")
            
            return keys
            
        except Exception as e:
            logging.error(f"Error loading API keys: {e}")
            return {}
    
    def _get_api_key(self, service: str) -> Optional[str]:
        """Get next API key for rotation"""
        if service not in self.api_keys or not self.api_keys[service]:
            return None
        
        keys = self.api_keys[service]
        key = keys[self.current_key_index[service]]
        self.current_key_index[service] = (self.current_key_index[service] + 1) % len(keys)
        return key
    
    def _check_rate_limit(self, service: str, requests_per_minute: int = 60) -> bool:
        """Check if service is rate limited"""
        current_time = time.time()
        
        # Clean old requests
        service_history = self.rate_limits[service]
        while service_history and current_time - service_history[0] > 60:
            service_history.popleft()
        
        # Check if under limit
        if len(service_history) >= requests_per_minute:
            return True  # Rate limited
        
        # Add current request
        service_history.append(current_time)
        return False
    
    async def collect_price_data_async(self, symbols: List[str]) -> Dict[str, Any]:
        """Collect price data asynchronously with validation"""
        try:
            if self._check_rate_limit('alpha_vantage', 5):  # 5 requests per minute
                logging.warning("Alpha Vantage rate limit reached")
                return {'status': 'rate_limited', 'collected': 0}
            
            api_key = self._get_api_key('alpha_vantage')
            if not api_key:
                return {'status': 'no_api_key', 'collected': 0}
            
            results = {
                'status': 'success',
                'collected': 0,
                'validated': 0,
                'inserted': 0,
                'errors': []
            }
            
            # Collect data for each symbol
            tasks = []
            for symbol in symbols[:5]:  # Limit to 5 symbols to respect rate limits
                task = self._fetch_symbol_data_async(symbol, api_key)
                tasks.append(task)
            
            symbol_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            for i, result in enumerate(symbol_results):
                if isinstance(result, Exception):
                    results['errors'].append(f"Error fetching {symbols[i]}: {str(result)}")
                    continue
                
                if not result:
                    continue
                
                results['collected'] += 1
                
                # Validate data
                validation_result = self.validator.validate_price_data(result)
                
                if validation_result.is_valid:
                    results['validated'] += 1
                    
                    # Insert into database
                    success = self.db_manager.insert_validated_data('price_data', result, validation_result)
                    if success:
                        results['inserted'] += 1
                else:
                    results['errors'].append(f"Validation failed for {result.get('symbol', 'unknown')}: {validation_result.errors}")
            
            # Update statistics
            self.collection_stats['price_data']['total_collected'] += results['collected']
            self.collection_stats['price_data']['successful_validations'] += results['validated']
            self.collection_stats['price_data']['database_inserts'] += results['inserted']
            self.collection_stats['price_data']['last_collection'] = datetime.now()
            
            return results
            
        except Exception as e:
            logging.error(f"Error collecting price data: {e}")
            return {'status': 'error', 'error': str(e), 'collected': 0}
    
    async def _fetch_symbol_data_async(self, symbol: str, api_key: str) -> Optional[Dict]:
        """Fetch data for a single symbol"""
        try:
            url = "https://www.alphavantage.co/query"
            params = {
                'function': 'GLOBAL_QUOTE',
                'symbol': symbol,
                'apikey': api_key
            }
            
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                self.executor,
                self.session.get,
                url,
                params,
                30  # timeout
            )
            
            response.raise_for_status()
            data = response.json()
            
            if 'Global Quote' not in data:
                return None
            
            quote = data['Global Quote']
            
            # Transform to standard format
            return {
                'symbol': symbol,
                'price': float(quote.get('05. price', 0)),
                'open': float(quote.get('02. open', 0)),
                'high': float(quote.get('03. high', 0)),
                'low': float(quote.get('04. low', 0)),
                'close': float(quote.get('05. price', 0)),
                'volume': float(quote.get('06. volume', 0)),
                'timestamp': datetime.now(),
                'source': 'alpha_vantage'
            }
            
        except Exception as e:
            logging.error(f"Error fetching symbol data for {symbol}: {e}")
            return None
    
    async def collect_news_data_async(self, keywords: List[str] = None) -> Dict[str, Any]:
        """Collect news data with validation and database commits"""
        try:
            if self._check_rate_limit('news_api', 100):  # 100 requests per hour
                return {'status': 'rate_limited', 'collected': 0}
            
            api_key = self._get_api_key('news_api')
            if not api_key:
                return {'status': 'no_api_key', 'collected': 0}
            
            results = {
                'status': 'success',
                'collected': 0,
                'validated': 0,
                'inserted': 0,
                'errors': []
            }
            
            # Default keywords for crypto news
            if not keywords:
                keywords = ['bitcoin', 'cryptocurrency', 'blockchain', 'ethereum', 'crypto']
            
            # Fetch news for each keyword
            for keyword in keywords[:3]:  # Limit keywords to manage rate limits
                try:
                    url = "https://newsapi.org/v2/everything"
                    params = {
                        'q': keyword,
                        'language': 'en',
                        'sortBy': 'publishedAt',
                        'pageSize': 20,
                        'from': (datetime.now() - timedelta(days=1)).isoformat()
                    }
                    headers = {'X-API-Key': api_key}
                    
                    loop = asyncio.get_event_loop()
                    response = await loop.run_in_executor(
                        self.executor,
                        lambda: self.session.get(url, params=params, headers=headers, timeout=30)
                    )
                    
                    response.raise_for_status()
                    data = response.json()
                    
                    articles = data.get('articles', [])
                    results['collected'] += len(articles)
                    
                    # Process each article
                    for article in articles:
                        try:
                            # Transform to standard format
                            news_data = {
                                'title': article.get('title', ''),
                                'content': article.get('description', '') or article.get('content', ''),
                                'source': article.get('source', {}).get('name', 'unknown'),
                                'url': article.get('url'),
                                'published_at': article.get('publishedAt'),
                                'relevance_score': 0.5  # Default relevance
                            }
                            
                            # Validate data
                            validation_result = self.validator.validate_news_data(news_data)
                            
                            if validation_result.is_valid:
                                results['validated'] += 1
                                
                                # Insert into database with proper transaction handling
                                success = self.db_manager.insert_validated_data('news_data', news_data, validation_result)
                                if success:
                                    results['inserted'] += 1
                            else:
                                results['errors'].append(f"News validation failed: {validation_result.errors}")
                        
                        except Exception as e:
                            results['errors'].append(f"Error processing article: {str(e)}")
                
                except Exception as e:
                    results['errors'].append(f"Error fetching news for {keyword}: {str(e)}")
            
            # Update statistics
            self.collection_stats['news_data']['total_collected'] += results['collected']
            self.collection_stats['news_data']['successful_validations'] += results['validated']
            self.collection_stats['news_data']['database_inserts'] += results['inserted']
            self.collection_stats['news_data']['last_collection'] = datetime.now()
            
            return results
            
        except Exception as e:
            logging.error(f"Error collecting news data: {e}")
            return {'status': 'error', 'error': str(e), 'collected': 0}
    
    async def collect_social_data_async(self, platforms: List[str] = None) -> Dict[str, Any]:
        """Collect social media data with validation"""
        try:
            if not platforms:
                platforms = ['reddit']  # Start with Reddit as it's more accessible
            
            results = {
                'status': 'success',
                'collected': 0,
                'validated': 0,
                'inserted': 0,
                'errors': []
            }
            
            for platform in platforms:
                if platform == 'reddit':
                    platform_results = await self._collect_reddit_data_async()
                    
                    results['collected'] += platform_results.get('collected', 0)
                    results['validated'] += platform_results.get('validated', 0)
                    results['inserted'] += platform_results.get('inserted', 0)
                    results['errors'].extend(platform_results.get('errors', []))
            
            return results
            
        except Exception as e:
            logging.error(f"Error collecting social data: {e}")
            return {'status': 'error', 'error': str(e), 'collected': 0}
    
    async def _collect_reddit_data_async(self) -> Dict[str, Any]:
        """Collect Reddit data from cryptocurrency subreddits"""
        try:
            results = {'collected': 0, 'validated': 0, 'inserted': 0, 'errors': []}
            
            # Use Reddit's public JSON API (no authentication required for public posts)
            subreddits = ['cryptocurrency', 'bitcoin', 'ethereum', 'CryptoMarkets']
            
            for subreddit in subreddits:
                try:
                    url = f"https://www.reddit.com/r/{subreddit}/hot.json?limit=25"
                    headers = {'User-Agent': 'V3TradingSystem/1.0'}
                    
                    loop = asyncio.get_event_loop()
                    response = await loop.run_in_executor(
                        self.executor,
                        lambda: self.session.get(url, headers=headers, timeout=30)
                    )
                    
                    response.raise_for_status()
                    data = response.json()
                    
                    posts = data.get('data', {}).get('children', [])
                    results['collected'] += len(posts)
                    
                    for post_container in posts:
                        try:
                            post = post_container.get('data', {})
                            
                            social_data = {
                                'platform': 'reddit',
                                'content': f"{post.get('title', '')} {post.get('selftext', '')}".strip(),
                                'author': post.get('author', 'unknown'),
                                'engagement': {
                                    'upvotes': post.get('ups', 0),
                                    'downvotes': post.get('downs', 0),
                                    'comments': post.get('num_comments', 0),
                                    'score': post.get('score', 0)
                                },
                                'timestamp': datetime.fromtimestamp(post.get('created_utc', 0)),
                                'hashtags': subreddit
                            }
                            
                            # Validate data
                            validation_result = self.validator.validate_social_data(social_data)
                            
                            if validation_result.is_valid:
                                results['validated'] += 1
                                
                                # Insert with proper transaction handling
                                success = self.db_manager.insert_validated_data('social_data', social_data, validation_result)
                                if success:
                                    results['inserted'] += 1
                            else:
                                results['errors'].append(f"Social validation failed: {validation_result.errors}")
                        
                        except Exception as e:
                            results['errors'].append(f"Error processing Reddit post: {str(e)}")
                
                except Exception as e:
                    results['errors'].append(f"Error fetching Reddit data for {subreddit}: {str(e)}")
                
                # Rate limiting between subreddits
                await asyncio.sleep(1)
            
            return results
            
        except Exception as e:
            logging.error(f"Error collecting Reddit data: {e}")
            return {'collected': 0, 'validated': 0, 'inserted': 0, 'errors': [str(e)]}
    
    def _start_background_tasks(self):
        """Start background monitoring and cleanup tasks"""
        def background_worker():
            while True:
                try:
                    self._cleanup_old_data()
                    self._monitor_database_health()
                    self._log_collection_stats()
                    time.sleep(300)  # Run every 5 minutes
                except Exception as e:
                    logging.error(f"Background task error: {e}")
                    time.sleep(60)
        
        thread = threading.Thread(target=background_worker, daemon=True)
        thread.start()
    
    def _cleanup_old_data(self):
        """Clean up old data to manage database size"""
        try:
            cutoff_date = datetime.now() - timedelta(days=30)
            
            cleanup_operations = [
                ('DELETE FROM price_data WHERE created_at < ?', (cutoff_date,)),
                ('DELETE FROM news_data WHERE created_at < ?', (cutoff_date,)),
                ('DELETE FROM social_data WHERE created_at < ?', (cutoff_date,)),
                ('DELETE FROM validation_log WHERE timestamp < ?', (cutoff_date,))
            ]
            
            success = self.db_manager.execute_transaction(cleanup_operations, commit=True)
            if success:
                logging.info("Database cleanup completed successfully")
            
        except Exception as e:
            logging.error(f"Database cleanup error: {e}")
    
    def _monitor_database_health(self):
        """Monitor database health and performance"""
        try:
            stats = self.db_manager.get_transaction_stats()
            
            if stats['total_transactions'] > 0:
                success_rate = stats['success_rate'] * 100
                if success_rate < 90:
                    logging.warning(f"Database success rate low: {success_rate:.1f}%")
                
                logging.info(f"DB Health - Transactions: {stats['total_transactions']}, "
                           f"Success: {success_rate:.1f}%, Connections: {stats['active_connections']}")
        
        except Exception as e:
            logging.error(f"Database health monitoring error: {e}")
    
    def _log_collection_stats(self):
        """Log data collection statistics"""
        try:
            total_collected = sum(stats['total_collected'] for stats in self.collection_stats.values())
            total_inserted = sum(stats['database_inserts'] for stats in self.collection_stats.values())
            
            logging.info(f"Collection Stats - Total collected: {total_collected}, "
                        f"Inserted: {total_inserted}, Sources: {len(self.collection_stats)}")
            
            for source, stats in self.collection_stats.items():
                if stats['total_collected'] > 0:
                    success_rate = stats['successful_validations'] / stats['total_collected'] * 100
                    logging.info(f"{source}: {stats['total_collected']} collected, "
                               f"{success_rate:.1f}% valid, {stats['database_inserts']} inserted")
        
        except Exception as e:
            logging.error(f"Stats logging error: {e}")
    
    def get_collection_summary(self) -> Dict[str, Any]:
        """Get comprehensive collection summary"""
        try:
            summary = {
                'total_sources': len(self.collection_stats),
                'database_stats': self.db_manager.get_transaction_stats(),
                'collection_stats': dict(self.collection_stats),
                'api_keys_loaded': {service: len(keys) for service, keys in self.api_keys.items()},
                'last_updated': datetime.now().isoformat()
            }
            
            # Calculate totals
            summary['totals'] = {
                'collected': sum(stats['total_collected'] for stats in self.collection_stats.values()),
                'validated': sum(stats['successful_validations'] for stats in self.collection_stats.values()),
                'inserted': sum(stats['database_inserts'] for stats in self.collection_stats.values())
            }
            
            return summary
            
        except Exception as e:
            logging.error(f"Error getting collection summary: {e}")
            return {}
    
    def optimize_for_server_specs(self):
        """Optimize for 8 vCPU / 24GB server specifications"""
        try:
            cpu_count = psutil.cpu_count()
            memory_gb = psutil.virtual_memory().total / (1024**3)
            
            # Adjust thread pool size
            optimal_workers = min(cpu_count * 2, 16)
            if self.executor._max_workers != optimal_workers:
                self.executor.shutdown(wait=False)
                self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=optimal_workers)
            
            # Adjust database connections for high memory systems
            if memory_gb >= 24:
                self.db_manager.max_connections = 20
            
            logging.info(f"External data collector optimized for {cpu_count} CPUs, {memory_gb:.1f}GB RAM")
            
        except Exception as e:
            logging.error(f"Server optimization error: {e}")

# Export main class
__all__ = ['ExternalDataCollector', 'DataValidator', 'DatabaseManager']

if __name__ == "__main__":
    # Performance test
    collector = ExternalDataCollector()
    collector.optimize_for_server_specs()
    
    # Test data collection
    import asyncio
    
    async def test_collection():
        results = await collector.collect_price_data_async(['BTCUSDT', 'ETHUSDT'])
        print(f"Price data collection: {results}")
        
        news_results = await collector.collect_news_data_async(['bitcoin'])
        print(f"News data collection: {news_results}")
    
    asyncio.run(test_collection())