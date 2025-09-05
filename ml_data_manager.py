#!/usr/bin/env python3
"""
V3 ML Data Manager - Performance Optimized
Enhanced with async patterns, database connection pooling, and transaction management
"""

import asyncio
import time
import logging
import threading
import sqlite3
import json
import pickle
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Union
from collections import defaultdict, deque
from functools import lru_cache, wraps
import concurrent.futures
import hashlib
import psutil
import queue
from dataclasses import dataclass
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
import joblib
import gc

@dataclass
class MLDataset:
    """ML Dataset container with metadata"""
    features: np.ndarray
    targets: np.ndarray
    feature_names: List[str]
    timestamp: datetime
    metadata: Dict[str, Any]
    
    def __post_init__(self):
        self.shape = self.features.shape
        self.target_shape = self.targets.shape if self.targets is not None else None

class AsyncDatabasePool:
    """Async-optimized database connection pool for ML operations"""
    
    def __init__(self, db_path: str, max_connections: int = 15):
        self.db_path = db_path
        self.max_connections = max_connections
        self.connections = queue.Queue(maxsize=max_connections)
        self.total_connections = 0
        self.lock = threading.Lock()
        
        # Transaction tracking
        self.transaction_metrics = {
            'total_operations': 0,
            'successful_commits': 0,
            'failed_commits': 0,
            'rollbacks': 0,
            'avg_operation_time': 0.0
        }
        
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Initialize the async database pool"""
        try:
            for _ in range(self.max_connections):
                conn = self._create_optimized_connection()
                if conn:
                    self.connections.put(conn)
                    self.total_connections += 1
            
            logging.info(f"ML Database pool initialized with {self.total_connections} connections")
            
        except Exception as e:
            logging.error(f"ML Database pool initialization error: {e}")
    
    def _create_optimized_connection(self) -> Optional[sqlite3.Connection]:
        """Create optimized database connection for ML operations"""
        try:
            conn = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=60.0,  # Longer timeout for ML operations
                isolation_level=None  # Enable autocommit
            )
            
            # Optimize for ML workloads
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=NORMAL')
            conn.execute('PRAGMA cache_size=50000')  # Larger cache for ML
            conn.execute('PRAGMA temp_store=MEMORY')
            conn.execute('PRAGMA mmap_size=1073741824')  # 1GB mmap
            conn.execute('PRAGMA page_size=32768')  # Larger page size
            
            # Enable foreign keys and optimize for bulk operations
            conn.execute('PRAGMA foreign_keys=ON')
            conn.execute('PRAGMA optimize')
            
            self._create_ml_tables(conn)
            return conn
            
        except Exception as e:
            logging.error(f"ML Database connection creation error: {e}")
            return None
    
    def _create_ml_tables(self, conn: sqlite3.Connection):
        """Create ML-specific tables"""
        try:
            # Feature datasets table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS ml_features (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dataset_name TEXT NOT NULL,
                    symbol TEXT,
                    timeframe TEXT,
                    feature_names TEXT NOT NULL,
                    feature_data BLOB NOT NULL,
                    target_data BLOB,
                    metadata TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Model artifacts table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS ml_models (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    model_name TEXT UNIQUE NOT NULL,
                    model_type TEXT NOT NULL,
                    model_data BLOB NOT NULL,
                    scaler_data BLOB,
                    performance_metrics TEXT,
                    training_config TEXT,
                    feature_importance TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Training history table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS training_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    model_name TEXT NOT NULL,
                    epoch INTEGER,
                    loss REAL,
                    accuracy REAL,
                    val_loss REAL,
                    val_accuracy REAL,
                    learning_rate REAL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Feature engineering cache
            conn.execute('''
                CREATE TABLE IF NOT EXISTS feature_cache (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cache_key TEXT UNIQUE NOT NULL,
                    data_hash TEXT NOT NULL,
                    cached_features BLOB NOT NULL,
                    expiry_time DATETIME NOT NULL,
                    access_count INTEGER DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Model predictions log
            conn.execute('''
                CREATE TABLE IF NOT EXISTS predictions_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    model_name TEXT NOT NULL,
                    symbol TEXT,
                    prediction_data TEXT NOT NULL,
                    confidence REAL,
                    actual_result REAL,
                    prediction_time DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create indexes for performance
            indexes = [
                'CREATE INDEX IF NOT EXISTS idx_features_dataset ON ml_features(dataset_name, symbol)',
                'CREATE INDEX IF NOT EXISTS idx_features_time ON ml_features(created_at)',
                'CREATE INDEX IF NOT EXISTS idx_models_name ON ml_models(model_name)',
                'CREATE INDEX IF NOT EXISTS idx_training_model ON training_history(model_name, timestamp)',
                'CREATE INDEX IF NOT EXISTS idx_cache_key ON feature_cache(cache_key)',
                'CREATE INDEX IF NOT EXISTS idx_predictions_model ON predictions_log(model_name, symbol)',
            ]
            
            for index_sql in indexes:
                conn.execute(index_sql)
            
            conn.commit()
            
        except Exception as e:
            logging.error(f"ML table creation error: {e}")
    
    async def execute_async_transaction(self, operations: List[Tuple[str, tuple]], 
                                      commit: bool = True) -> bool:
        """Execute database operations asynchronously with proper transaction handling"""
        start_time = time.time()
        conn = None
        
        try:
            # Get connection from pool
            conn = await self._get_connection_async()
            if not conn:
                return False
            
            # Start explicit transaction
            conn.execute('BEGIN IMMEDIATE')
            
            # Execute all operations
            for sql, params in operations:
                cursor = conn.cursor()
                cursor.execute(sql, params)
            
            if commit:
                conn.commit()
                self.transaction_metrics['successful_commits'] += 1
            else:
                conn.rollback()
                self.transaction_metrics['rollbacks'] += 1
            
            operation_time = time.time() - start_time
            self._update_metrics(operation_time, True)
            
            return True
            
        except Exception as e:
            self.transaction_metrics['failed_commits'] += 1
            if conn:
                try:
                    conn.rollback()
                    self.transaction_metrics['rollbacks'] += 1
                except:
                    pass
            
            operation_time = time.time() - start_time
            self._update_metrics(operation_time, False)
            
            logging.error(f"Async transaction error: {e}")
            return False
            
        finally:
            if conn:
                await self._return_connection_async(conn)
    
    async def _get_connection_async(self) -> Optional[sqlite3.Connection]:
        """Get database connection asynchronously"""
        try:
            loop = asyncio.get_event_loop()
            
            # Try to get existing connection
            if not self.connections.empty():
                return await loop.run_in_executor(None, self.connections.get)
            
            # Create new connection if under limit
            with self.lock:
                if self.total_connections < self.max_connections:
                    conn = self._create_optimized_connection()
                    if conn:
                        self.total_connections += 1
                        return conn
            
            # Wait for available connection
            return await loop.run_in_executor(None, lambda: self.connections.get(timeout=30))
            
        except Exception as e:
            logging.error(f"Error getting async connection: {e}")
            return None
    
    async def _return_connection_async(self, conn: sqlite3.Connection):
        """Return connection to pool asynchronously"""
        try:
            loop = asyncio.get_event_loop()
            
            if conn and not self.connections.full():
                # Test connection validity
                await loop.run_in_executor(None, lambda: conn.execute('SELECT 1'))
                await loop.run_in_executor(None, self.connections.put, conn)
            elif conn:
                await loop.run_in_executor(None, conn.close)
                with self.lock:
                    self.total_connections -= 1
                    
        except Exception as e:
            logging.error(f"Error returning async connection: {e}")
            if conn:
                try:
                    await asyncio.get_event_loop().run_in_executor(None, conn.close)
                except:
                    pass
                with self.lock:
                    self.total_connections -= 1
    
    def _update_metrics(self, operation_time: float, success: bool):
        """Update transaction metrics"""
        self.transaction_metrics['total_operations'] += 1
        
        # Update rolling average
        if self.transaction_metrics['avg_operation_time'] == 0:
            self.transaction_metrics['avg_operation_time'] = operation_time
        else:
            self.transaction_metrics['avg_operation_time'] = (
                self.transaction_metrics['avg_operation_time'] * 0.9 + operation_time * 0.1
            )

class FeatureEngineer:
    """Async feature engineering with caching"""
    
    def __init__(self, db_pool: AsyncDatabasePool):
        self.db_pool = db_pool
        self.feature_cache = {}
        self.cache_lock = threading.RLock()
    
    async def engineer_features_async(self, data: pd.DataFrame, 
                                    feature_config: Dict[str, Any]) -> np.ndarray:
        """Engineer features asynchronously with caching"""
        try:
            # Create cache key
            data_hash = hashlib.md5(str(data.values.tobytes()).encode()).hexdigest()
            config_hash = hashlib.md5(str(feature_config).encode()).hexdigest()
            cache_key = f"{data_hash}_{config_hash}"
            
            # Check cache first
            cached_features = await self._get_cached_features_async(cache_key)
            if cached_features is not None:
                return cached_features
            
            # Engineer features
            features = await self._compute_features_async(data, feature_config)
            
            # Cache results
            await self._cache_features_async(cache_key, features, data_hash)
            
            return features
            
        except Exception as e:
            logging.error(f"Feature engineering error: {e}")
            return None
    
    async def _get_cached_features_async(self, cache_key: str) -> Optional[np.ndarray]:
        """Get cached features asynchronously"""
        try:
            # Check memory cache first
            with self.cache_lock:
                if cache_key in self.feature_cache:
                    cache_entry = self.feature_cache[cache_key]
                    if cache_entry['expiry'] > time.time():
                        return cache_entry['features']
                    else:
                        del self.feature_cache[cache_key]
            
            # Check database cache
            query = 'SELECT cached_features FROM feature_cache WHERE cache_key = ? AND expiry_time > ?'
            params = (cache_key, datetime.now())
            
            conn = await self.db_pool._get_connection_async()
            if conn:
                try:
                    cursor = conn.cursor()
                    cursor.execute(query, params)
                    result = cursor.fetchone()
                    
                    if result:
                        # Update access count
                        conn.execute('UPDATE feature_cache SET access_count = access_count + 1 WHERE cache_key = ?', 
                                   (cache_key,))
                        conn.commit()
                        
                        features = pickle.loads(result[0])
                        
                        # Update memory cache
                        with self.cache_lock:
                            self.feature_cache[cache_key] = {
                                'features': features,
                                'expiry': time.time() + 3600  # 1 hour memory cache
                            }
                        
                        return features
                        
                finally:
                    await self.db_pool._return_connection_async(conn)
            
            return None
            
        except Exception as e:
            logging.error(f"Cache retrieval error: {e}")
            return None
    
    async def _cache_features_async(self, cache_key: str, features: np.ndarray, data_hash: str):
        """Cache features asynchronously"""
        try:
            # Cache in memory
            with self.cache_lock:
                self.feature_cache[cache_key] = {
                    'features': features,
                    'expiry': time.time() + 3600  # 1 hour
                }
            
            # Cache in database
            expiry_time = datetime.now() + timedelta(hours=24)  # 24 hour database cache
            features_blob = pickle.dumps(features)
            
            operations = [(
                '''INSERT OR REPLACE INTO feature_cache 
                   (cache_key, data_hash, cached_features, expiry_time, access_count)
                   VALUES (?, ?, ?, ?, 1)''',
                (cache_key, data_hash, features_blob, expiry_time)
            )]
            
            await self.db_pool.execute_async_transaction(operations, commit=True)
            
        except Exception as e:
            logging.error(f"Feature caching error: {e}")
    
    async def _compute_features_async(self, data: pd.DataFrame, 
                                    feature_config: Dict[str, Any]) -> np.ndarray:
        """Compute features asynchronously using thread pool"""
        try:
            loop = asyncio.get_event_loop()
            
            # Run feature computation in thread pool to avoid blocking
            executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
            features = await loop.run_in_executor(
                executor,
                self._compute_features_sync,
                data,
                feature_config
            )
            
            return features
            
        except Exception as e:
            logging.error(f"Async feature computation error: {e}")
            return None
    
    def _compute_features_sync(self, data: pd.DataFrame, feature_config: Dict[str, Any]) -> np.ndarray:
        """Synchronous feature computation"""
        try:
            features_list = []
            
            # Technical indicators
            if 'technical' in feature_config:
                tech_features = self._compute_technical_features(data)
                features_list.append(tech_features)
            
            # Price features
            if 'price' in feature_config:
                price_features = self._compute_price_features(data)
                features_list.append(price_features)
            
            # Volume features
            if 'volume' in feature_config:
                volume_features = self._compute_volume_features(data)
                features_list.append(volume_features)
            
            # Statistical features
            if 'statistical' in feature_config:
                stat_features = self._compute_statistical_features(data)
                features_list.append(stat_features)
            
            # Combine all features
            if features_list:
                features = np.column_stack(features_list)
                return features
            else:
                return np.array([])
                
        except Exception as e:
            logging.error(f"Feature computation error: {e}")
            return np.array([])
    
    def _compute_technical_features(self, data: pd.DataFrame) -> np.ndarray:
        """Compute technical indicator features"""
        try:
            features = []
            
            if 'close' in data.columns:
                close = data['close'].values
                
                # Moving averages
                for period in [5, 10, 20, 50]:
                    if len(close) >= period:
                        ma = pd.Series(close).rolling(window=period).mean().fillna(method='bfill')
                        features.append(ma.values)
                
                # RSI
                if len(close) >= 14:
                    delta = pd.Series(close).diff()
                    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
                    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
                    rs = gain / loss
                    rsi = 100 - (100 / (1 + rs))
                    features.append(rsi.fillna(50).values)
            
            return np.column_stack(features) if features else np.array([]).reshape(len(data), 0)
            
        except Exception as e:
            logging.error(f"Technical features error: {e}")
            return np.array([]).reshape(len(data), 0)
    
    def _compute_price_features(self, data: pd.DataFrame) -> np.ndarray:
        """Compute price-based features"""
        try:
            features = []
            
            price_cols = ['open', 'high', 'low', 'close']
            available_cols = [col for col in price_cols if col in data.columns]
            
            if available_cols:
                # Price returns
                for col in available_cols:
                    returns = data[col].pct_change().fillna(0)
                    features.append(returns.values)
                
                # Price ranges
                if 'high' in data.columns and 'low' in data.columns:
                    price_range = (data['high'] - data['low']) / data['low']
                    features.append(price_range.fillna(0).values)
            
            return np.column_stack(features) if features else np.array([]).reshape(len(data), 0)
            
        except Exception as e:
            logging.error(f"Price features error: {e}")
            return np.array([]).reshape(len(data), 0)
    
    def _compute_volume_features(self, data: pd.DataFrame) -> np.ndarray:
        """Compute volume-based features"""
        try:
            features = []
            
            if 'volume' in data.columns:
                volume = data['volume'].values
                
                # Volume moving averages
                for period in [5, 10, 20]:
                    if len(volume) >= period:
                        vol_ma = pd.Series(volume).rolling(window=period).mean().fillna(method='bfill')
                        features.append(vol_ma.values)
                
                # Volume changes
                vol_change = pd.Series(volume).pct_change().fillna(0)
                features.append(vol_change.values)
            
            return np.column_stack(features) if features else np.array([]).reshape(len(data), 0)
            
        except Exception as e:
            logging.error(f"Volume features error: {e}")
            return np.array([]).reshape(len(data), 0)
    
    def _compute_statistical_features(self, data: pd.DataFrame) -> np.ndarray:
        """Compute statistical features"""
        try:
            features = []
            
            numeric_cols = data.select_dtypes(include=[np.number]).columns
            
            for col in numeric_cols:
                if col in data.columns:
                    values = data[col].values
                    
                    # Rolling statistics
                    for window in [5, 10, 20]:
                        if len(values) >= window:
                            rolling_std = pd.Series(values).rolling(window=window).std().fillna(0)
                            features.append(rolling_std.values)
            
            return np.column_stack(features) if features else np.array([]).reshape(len(data), 0)
            
        except Exception as e:
            logging.error(f"Statistical features error: {e}")
            return np.array([]).reshape(len(data), 0)

class MLDataManager:
    """
    Enhanced ML Data Manager with Performance Optimization
    Optimized for 8 vCPU / 24GB server specifications with async patterns
    """
    
    def __init__(self, config_manager=None):
        self.config = config_manager
        
        # Async database pool
        self.db_pool = AsyncDatabasePool('ml_data.db', max_connections=20)
        
        # Feature engineering
        self.feature_engineer = FeatureEngineer(self.db_pool)
        
        # Performance optimization
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=16)
        
        # Data processing state
        self.datasets = {}
        self.models = {}
        self.scalers = {}
        
        # Performance tracking
        self.processing_stats = {
            'datasets_created': 0,
            'features_engineered': 0,
            'models_trained': 0,
            'predictions_made': 0,
            'cache_hits': 0,
            'cache_misses': 0
        }
        
        # Memory management
        self.memory_monitor = MemoryMonitor()
        
        # Start background tasks
        self._start_background_tasks()
    
    async def create_dataset_async(self, data_source: str, symbols: List[str], 
                                 timeframes: List[str], feature_config: Dict[str, Any]) -> Optional[str]:
        """Create ML dataset asynchronously with proper transaction handling"""
        try:
            dataset_name = f"{data_source}_{hash(str(symbols + timeframes))}"
            
            logging.info(f"Creating dataset: {dataset_name}")
            
            # Process data for each symbol/timeframe combination
            all_features = []
            all_targets = []
            feature_names = []
            
            for symbol in symbols:
                for timeframe in timeframes:
                    # Fetch data asynchronously
                    market_data = await self._fetch_market_data_async(symbol, timeframe)
                    if market_data is None or market_data.empty:
                        continue
                    
                    # Engineer features asynchronously
                    features = await self.feature_engineer.engineer_features_async(
                        market_data, feature_config
                    )
                    
                    if features is None or features.size == 0:
                        continue
                    
                    # Create targets (next period returns)
                    targets = await self._create_targets_async(market_data)
                    
                    if targets is not None:
                        all_features.append(features)
                        all_targets.append(targets)
                        
                        # Track feature names for first iteration
                        if not feature_names:
                            feature_names = self._generate_feature_names(feature_config)
            
            if not all_features:
                logging.warning(f"No features generated for dataset {dataset_name}")
                return None
            
            # Combine all features and targets
            combined_features = np.vstack(all_features)
            combined_targets = np.hstack(all_targets)
            
            # Create dataset object
            dataset = MLDataset(
                features=combined_features,
                targets=combined_targets,
                feature_names=feature_names,
                timestamp=datetime.now(),
                metadata={
                    'symbols': symbols,
                    'timeframes': timeframes,
                    'feature_config': feature_config,
                    'data_source': data_source
                }
            )
            
            # Store dataset in database with proper transaction handling
            success = await self._store_dataset_async(dataset_name, dataset)
            
            if success:
                self.datasets[dataset_name] = dataset
                self.processing_stats['datasets_created'] += 1
                logging.info(f"Dataset created successfully: {dataset_name} with shape {dataset.shape}")
                return dataset_name
            else:
                logging.error(f"Failed to store dataset: {dataset_name}")
                return None
                
        except Exception as e:
            logging.error(f"Error creating dataset: {e}")
            return None
    
    async def _fetch_market_data_async(self, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
        """Fetch market data asynchronously"""
        try:
            # This would integrate with your data sources
            # For now, return a mock query to the database
            query = '''
                SELECT timestamp, open_price as open, high_price as high, 
                       low_price as low, close_price as close, volume
                FROM price_data 
                WHERE symbol = ? 
                ORDER BY timestamp DESC 
                LIMIT 1000
            '''
            
            conn = await self.db_pool._get_connection_async()
            if not conn:
                return None
            
            try:
                df = pd.read_sql_query(query, conn, params=(symbol,))
                
                if not df.empty:
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df = df.sort_values('timestamp').reset_index(drop=True)
                    return df
                else:
                    # Generate sample data for testing
                    dates = pd.date_range(start='2024-01-01', periods=100, freq='1H')
                    np.random.seed(42)
                    base_price = 50000
                    prices = base_price + np.cumsum(np.random.normal(0, 100, len(dates)))
                    
                    return pd.DataFrame({
                        'timestamp': dates,
                        'open': prices * (1 + np.random.normal(0, 0.001, len(dates))),
                        'high': prices * (1 + np.abs(np.random.normal(0, 0.002, len(dates)))),
                        'low': prices * (1 - np.abs(np.random.normal(0, 0.002, len(dates)))),
                        'close': prices,
                        'volume': np.random.uniform(1000, 10000, len(dates))
                    })
                    
            finally:
                await self.db_pool._return_connection_async(conn)
                
        except Exception as e:
            logging.error(f"Error fetching market data for {symbol}: {e}")
            return None
    
    async def _create_targets_async(self, data: pd.DataFrame) -> Optional[np.ndarray]:
        """Create target variables asynchronously"""
        try:
            loop = asyncio.get_event_loop()
            
            # Run target creation in thread pool
            targets = await loop.run_in_executor(
                self.executor,
                self._create_targets_sync,
                data
            )
            
            return targets
            
        except Exception as e:
            logging.error(f"Error creating targets: {e}")
            return None
    
    def _create_targets_sync(self, data: pd.DataFrame) -> np.ndarray:
        """Create target variables synchronously"""
        try:
            if 'close' not in data.columns:
                return None
            
            close_prices = data['close'].values
            
            # Create next period return targets
            returns = np.diff(close_prices) / close_prices[:-1]
            
            # Binary classification: 1 if return > 0, 0 otherwise
            targets = (returns > 0).astype(int)
            
            # Pad with last value to match feature length
            targets = np.append(targets, targets[-1])
            
            return targets
            
        except Exception as e:
            logging.error(f"Error in target creation: {e}")
            return None
    
    def _generate_feature_names(self, feature_config: Dict[str, Any]) -> List[str]:
        """Generate feature names based on configuration"""
        try:
            names = []
            
            if 'technical' in feature_config:
                for period in [5, 10, 20, 50]:
                    names.append(f'ma_{period}')
                names.append('rsi_14')
            
            if 'price' in feature_config:
                for col in ['open', 'high', 'low', 'close']:
                    names.append(f'{col}_return')
                names.append('price_range')
            
            if 'volume' in feature_config:
                for period in [5, 10, 20]:
                    names.append(f'vol_ma_{period}')
                names.append('vol_change')
            
            if 'statistical' in feature_config:
                for window in [5, 10, 20]:
                    names.append(f'rolling_std_{window}')
            
            return names
            
        except Exception as e:
            logging.error(f"Error generating feature names: {e}")
            return []
    
    async def _store_dataset_async(self, dataset_name: str, dataset: MLDataset) -> bool:
        """Store dataset in database with proper transaction handling"""
        try:
            # Serialize data
            features_blob = pickle.dumps(dataset.features)
            targets_blob = pickle.dumps(dataset.targets)
            feature_names_json = json.dumps(dataset.feature_names)
            metadata_json = json.dumps(dataset.metadata, default=str)
            
            # Prepare database operations
            operations = [
                (
                    '''INSERT OR REPLACE INTO ml_features 
                       (dataset_name, feature_names, feature_data, target_data, metadata, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?)''',
                    (
                        dataset_name,
                        feature_names_json,
                        features_blob,
                        targets_blob,
                        metadata_json,
                        datetime.now()
                    )
                )
            ]
            
            # Execute with proper transaction handling
            success = await self.db_pool.execute_async_transaction(operations, commit=True)
            
            if success:
                logging.info(f"Dataset stored successfully: {dataset_name}")
            else:
                logging.error(f"Failed to store dataset: {dataset_name}")
            
            return success
            
        except Exception as e:
            logging.error(f"Error storing dataset {dataset_name}: {e}")
            return False
    
    async def load_dataset_async(self, dataset_name: str) -> Optional[MLDataset]:
        """Load dataset asynchronously"""
        try:
            # Check memory cache first
            if dataset_name in self.datasets:
                return self.datasets[dataset_name]
            
            # Load from database
            query = '''
                SELECT feature_names, feature_data, target_data, metadata, created_at
                FROM ml_features 
                WHERE dataset_name = ?
                ORDER BY updated_at DESC 
                LIMIT 1
            '''
            
            conn = await self.db_pool._get_connection_async()
            if not conn:
                return None
            
            try:
                cursor = conn.cursor()
                cursor.execute(query, (dataset_name,))
                result = cursor.fetchone()
                
                if result:
                    feature_names = json.loads(result[0])
                    features = pickle.loads(result[1])
                    targets = pickle.loads(result[2])
                    metadata = json.loads(result[3])
                    created_at = datetime.fromisoformat(result[4])
                    
                    dataset = MLDataset(
                        features=features,
                        targets=targets,
                        feature_names=feature_names,
                        timestamp=created_at,
                        metadata=metadata
                    )
                    
                    # Cache in memory
                    self.datasets[dataset_name] = dataset
                    
                    return dataset
                    
            finally:
                await self.db_pool._return_connection_async(conn)
            
            return None
            
        except Exception as e:
            logging.error(f"Error loading dataset {dataset_name}: {e}")
            return None
    
    async def train_model_async(self, dataset_name: str, model_config: Dict[str, Any]) -> Optional[str]:
        """Train ML model asynchronously with progress tracking"""
        try:
            # Load dataset
            dataset = await self.load_dataset_async(dataset_name)
            if dataset is None:
                logging.error(f"Dataset not found: {dataset_name}")
                return None
            
            # Prepare data
            X, y = dataset.features, dataset.targets
            
            # Split data
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42, stratify=y
            )
            
            # Scale features
            scaler = StandardScaler()
            X_train_scaled = scaler.fit_transform(X_train)
            X_test_scaled = scaler.transform(X_test)
            
            # Train model asynchronously
            model, performance = await self._train_model_async_impl(
                X_train_scaled, y_train, X_test_scaled, y_test, model_config
            )
            
            if model is None:
                return None
            
            # Generate model name
            model_name = f"{dataset_name}_{model_config.get('type', 'unknown')}_{int(time.time())}"
            
            # Store model with proper transaction handling
            success = await self._store_model_async(model_name, model, scaler, performance, model_config)
            
            if success:
                self.models[model_name] = model
                self.scalers[model_name] = scaler
                self.processing_stats['models_trained'] += 1
                logging.info(f"Model trained successfully: {model_name}")
                return model_name
            
            return None
            
        except Exception as e:
            logging.error(f"Error training model: {e}")
            return None
    
    async def _train_model_async_impl(self, X_train: np.ndarray, y_train: np.ndarray,
                                     X_test: np.ndarray, y_test: np.ndarray,
                                     model_config: Dict[str, Any]) -> Tuple[Any, Dict]:
        """Train model implementation asynchronously"""
        try:
            loop = asyncio.get_event_loop()
            
            # Train in thread pool to avoid blocking
            result = await loop.run_in_executor(
                self.executor,
                self._train_model_sync,
                X_train, y_train, X_test, y_test, model_config
            )
            
            return result
            
        except Exception as e:
            logging.error(f"Async model training error: {e}")
            return None, {}
    
    def _train_model_sync(self, X_train: np.ndarray, y_train: np.ndarray,
                         X_test: np.ndarray, y_test: np.ndarray,
                         model_config: Dict[str, Any]) -> Tuple[Any, Dict]:
        """Synchronous model training"""
        try:
            from sklearn.ensemble import RandomForestClassifier
            from sklearn.linear_model import LogisticRegression
            from sklearn.svm import SVC
            
            model_type = model_config.get('type', 'random_forest')
            
            # Create model based on type
            if model_type == 'random_forest':
                model = RandomForestClassifier(
                    n_estimators=model_config.get('n_estimators', 100),
                    max_depth=model_config.get('max_depth', 10),
                    random_state=42,
                    n_jobs=-1
                )
            elif model_type == 'logistic_regression':
                model = LogisticRegression(
                    C=model_config.get('C', 1.0),
                    max_iter=model_config.get('max_iter', 1000),
                    random_state=42
                )
            elif model_type == 'svm':
                model = SVC(
                    C=model_config.get('C', 1.0),
                    kernel=model_config.get('kernel', 'rbf'),
                    probability=True,
                    random_state=42
                )
            else:
                raise ValueError(f"Unknown model type: {model_type}")
            
            # Train model
            model.fit(X_train, y_train)
            
            # Evaluate performance
            y_pred = model.predict(X_test)
            y_pred_proba = model.predict_proba(X_test)[:, 1] if hasattr(model, 'predict_proba') else None
            
            performance = {
                'accuracy': accuracy_score(y_test, y_pred),
                'precision': precision_score(y_test, y_pred, average='weighted'),
                'recall': recall_score(y_test, y_pred, average='weighted'),
                'f1_score': f1_score(y_test, y_pred, average='weighted'),
                'test_samples': len(y_test),
                'train_samples': len(y_train)
            }
            
            # Feature importance if available
            if hasattr(model, 'feature_importances_'):
                performance['feature_importance'] = model.feature_importances_.tolist()
            
            return model, performance
            
        except Exception as e:
            logging.error(f"Model training error: {e}")
            return None, {}
    
    async def _store_model_async(self, model_name: str, model: Any, scaler: Any,
                                performance: Dict, config: Dict) -> bool:
        """Store trained model with proper transaction handling"""
        try:
            # Serialize model and scaler
            model_blob = pickle.dumps(model)
            scaler_blob = pickle.dumps(scaler)
            performance_json = json.dumps(performance, default=str)
            config_json = json.dumps(config, default=str)
            
            # Get feature importance if available
            feature_importance = None
            if hasattr(model, 'feature_importances_'):
                feature_importance = json.dumps(model.feature_importances_.tolist())
            
            # Prepare database operations
            operations = [
                (
                    '''INSERT OR REPLACE INTO ml_models 
                       (model_name, model_type, model_data, scaler_data, 
                        performance_metrics, training_config, feature_importance, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                    (
                        model_name,
                        config.get('type', 'unknown'),
                        model_blob,
                        scaler_blob,
                        performance_json,
                        config_json,
                        feature_importance,
                        datetime.now()
                    )
                )
            ]
            
            # Execute with proper transaction handling
            success = await self.db_pool.execute_async_transaction(operations, commit=True)
            
            return success
            
        except Exception as e:
            logging.error(f"Error storing model {model_name}: {e}")
            return False
    
    def _start_background_tasks(self):
        """Start background monitoring and cleanup tasks"""
        def background_worker():
            while True:
                try:
                    self._cleanup_cache()
                    self._monitor_memory_usage()
                    self._log_processing_stats()
                    time.sleep(300)  # Run every 5 minutes
                except Exception as e:
                    logging.error(f"Background task error: {e}")
                    time.sleep(60)
        
        thread = threading.Thread(target=background_worker, daemon=True)
        thread.start()
    
    def _cleanup_cache(self):
        """Clean up expired cache entries and manage memory"""
        try:
            # Clean feature cache memory
            current_time = time.time()
            expired_keys = []
            
            with self.feature_engineer.cache_lock:
                for key, cache_entry in self.feature_engineer.feature_cache.items():
                    if cache_entry['expiry'] < current_time:
                        expired_keys.append(key)
                
                for key in expired_keys:
                    del self.feature_engineer.feature_cache[key]
            
            # Force garbage collection if memory usage is high
            memory_percent = psutil.virtual_memory().percent
            if memory_percent > 85:
                gc.collect()
                logging.warning(f"High memory usage ({memory_percent}%), forced garbage collection")
            
        except Exception as e:
            logging.error(f"Cache cleanup error: {e}")
    
    def _monitor_memory_usage(self):
        """Monitor memory usage and optimize accordingly"""
        try:
            memory_info = psutil.virtual_memory()
            
            if memory_info.percent > 90:
                # Clear some datasets from memory
                dataset_keys = list(self.datasets.keys())
                for key in dataset_keys[:len(dataset_keys)//2]:  # Clear half
                    del self.datasets[key]
                
                gc.collect()
                logging.warning("High memory usage, cleared datasets from memory")
        
        except Exception as e:
            logging.error(f"Memory monitoring error: {e}")
    
    def _log_processing_stats(self):
        """Log processing statistics"""
        try:
            stats = self.processing_stats.copy()
            db_stats = self.db_pool.transaction_metrics
            
            logging.info(f"ML Processing Stats - Datasets: {stats['datasets_created']}, "
                        f"Models: {stats['models_trained']}, "
                        f"DB Success Rate: {db_stats.get('successful_commits', 0) / max(db_stats.get('total_operations', 1), 1) * 100:.1f}%")
        
        except Exception as e:
            logging.error(f"Stats logging error: {e}")
    
    def get_processing_summary(self) -> Dict[str, Any]:
        """Get comprehensive processing summary"""
        try:
            return {
                'processing_stats': self.processing_stats.copy(),
                'database_stats': self.db_pool.transaction_metrics.copy(),
                'memory_usage': psutil.virtual_memory().percent,
                'cached_datasets': len(self.datasets),
                'cached_models': len(self.models),
                'feature_cache_size': len(self.feature_engineer.feature_cache),
                'last_updated': datetime.now().isoformat()
            }
        
        except Exception as e:
            logging.error(f"Error getting processing summary: {e}")
            return {}
    
    def optimize_for_server_specs(self):
        """Optimize for 8 vCPU / 24GB server specifications"""
        try:
            cpu_count = psutil.cpu_count()
            memory_gb = psutil.virtual_memory().total / (1024**3)
            
            # Adjust thread pool size
            optimal_workers = min(cpu_count * 2, 20)
            if self.executor._max_workers != optimal_workers:
                self.executor.shutdown(wait=False)
                self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=optimal_workers)
            
            # Adjust database connections for high memory systems
            if memory_gb >= 24:
                self.db_pool.max_connections = 25
            
            logging.info(f"ML Data Manager optimized for {cpu_count} CPUs, {memory_gb:.1f}GB RAM")
            
        except Exception as e:
            logging.error(f"Server optimization error: {e}")

class MemoryMonitor:
    """Monitor and optimize memory usage"""
    
    def __init__(self):
        self.memory_history = deque(maxlen=60)  # Last 60 measurements
    
    def get_memory_status(self) -> Dict[str, Any]:
        """Get current memory status"""
        try:
            memory = psutil.virtual_memory()
            self.memory_history.append(memory.percent)
            
            return {
                'current_percent': memory.percent,
                'available_gb': memory.available / (1024**3),
                'used_gb': memory.used / (1024**3),
                'average_5min': sum(list(self.memory_history)[-5:]) / min(5, len(self.memory_history)),
                'threshold_warning': memory.percent > 85,
                'threshold_critical': memory.percent > 95
            }
            
        except Exception as e:
            logging.error(f"Memory monitoring error: {e}")
            return {}

# Export main classes
__all__ = ['MLDataManager', 'FeatureEngineer', 'AsyncDatabasePool', 'MLDataset']

if __name__ == "__main__":
    # Performance test
    async def test_ml_manager():
        manager = MLDataManager()
        manager.optimize_for_server_specs()
        
        # Test dataset creation
        feature_config = {
            'technical': True,
            'price': True,
            'volume': True,
            'statistical': True
        }
        
        dataset_name = await manager.create_dataset_async(
            'test_data',
            ['BTCUSDT', 'ETHUSDT'],
            ['1h', '4h'],
            feature_config
        )
        
        print(f"Dataset created: {dataset_name}")
        
        # Test model training
        if dataset_name:
            model_config = {
                'type': 'random_forest',
                'n_estimators': 50,
                'max_depth': 10
            }
            
            model_name = await manager.train_model_async(dataset_name, model_config)
            print(f"Model trained: {model_name}")
        
        # Get summary
        summary = manager.get_processing_summary()
        print(f"Processing Summary: {json.dumps(summary, indent=2, default=str)}")
    
    # Run test
    asyncio.run(test_ml_manager())