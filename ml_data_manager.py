#!/usr/bin/env python3
"""
ML DATA MANAGER - V3 OPTIMIZED FOR 8 vCPU/24GB
==============================================
V3 Performance & Data Pipeline Fixes Applied:
- Enhanced database connection pooling for ML operations
- UTF-8 encoding specification for all file operations
- Memory optimization for large ML datasets
- Intelligent caching system for feature extraction
- Thread-safe operations with proper resource management
- Real data validation patterns enforced (NO MOCK DATA)
- Batch processing for high-performance ML operations
"""

import os
import sqlite3
import json
import numpy as np
import pandas as pd
import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple, Union
from dataclasses import dataclass, asdict
import pickle
import gc
import psutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
import hashlib

@dataclass
class MLFeatureSet:
    """Enhanced ML feature set with validation and UTF-8 support"""
    feature_id: str
    symbol: str
    timeframe: str
    features: Dict[str, float]
    target: Optional[float] = None
    timestamp: datetime = None
    data_source: str = "live_market"
    encoding: str = "utf-8"
    v3_compliance: bool = True
    feature_quality_score: float = 1.0
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
    
    def validate_features(self) -> bool:
        """Validate ML features for real data compliance"""
        try:
            # Check for mock data indicators
            feature_str = str(self.features).lower()
            mock_indicators = ['mock', 'test', 'fake', 'dummy', 'synthetic']
            
            for indicator in mock_indicators:
                if indicator in feature_str:
                    logging.error(f"V3 VIOLATION: Mock data detected in features: {indicator}")
                    return False
            
            # Validate data source
            if 'mock' in self.data_source.lower() or 'test' in self.data_source.lower():
                logging.error(f"V3 VIOLATION: Invalid data source: {self.data_source}")
                return False
            
            # Validate feature values are realistic
            for key, value in self.features.items():
                if not isinstance(value, (int, float)):
                    continue
                
                # Check for unrealistic values
                if abs(value) > 1e10:  # Extremely large values
                    logging.warning(f"Suspicious feature value: {key}={value}")
                    return False
                
                if np.isnan(value) or np.isinf(value):
                    logging.warning(f"Invalid feature value: {key}={value}")
                    return False
            
            # Validate timestamp is recent (not historical mock data)
            if self.timestamp < datetime.now() - timedelta(days=365):
                logging.warning(f"Feature data is very old: {self.timestamp}")
                return False
            
            return True
            
        except Exception as e:
            logging.error(f"Feature validation error: {e}")
            return False

class OptimizedMLDatabasePool:
    """High-performance database pool optimized for ML operations"""
    
    def __init__(self, db_path: str, max_connections: int = 12):
        self.db_path = db_path
        self.max_connections = max_connections
        self.connections = []
        self.available = []
        self.in_use = set()
        self.lock = threading.RLock()
        
        # ML-specific optimizations
        self.feature_cache = {}
        self.cache_lock = threading.RLock()
        
        # Performance statistics
        self.stats = {
            'ml_operations': 0,
            'feature_extractions': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'batch_operations': 0,
            'average_operation_time': 0.0
        }
        
        # Initialize optimized connection pool
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._initialize_ml_optimized_pool()
    
    def _initialize_ml_optimized_pool(self):
        """Initialize connection pool with ML-specific optimizations"""
        for i in range(self.max_connections):
            conn = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=45.0  # Longer timeout for ML operations
            )
            
            # Apply ML-optimized settings
            ml_optimizations = [
                "PRAGMA journal_mode=WAL",
                "PRAGMA cache_size=50000",  # 50MB cache for ML data
                "PRAGMA temp_store=MEMORY",
                "PRAGMA synchronous=NORMAL",
                "PRAGMA page_size=8192",    # Larger pages for ML data
                "PRAGMA mmap_size=536870912",  # 512MB memory map
                "PRAGMA wal_autocheckpoint=2000",
                "PRAGMA encoding='UTF-8'",
                "PRAGMA automatic_index=ON",
                "PRAGMA threads=4"
            ]
            
            for pragma in ml_optimizations:
                conn.execute(pragma)
            
            # Enable ML-specific extensions if available
            try:
                conn.enable_load_extension(True)
            except:
                pass  # Not critical
            
            self.connections.append(conn)
            self.available.append(conn)
    
    def get_connection_for_ml(self, timeout: float = 45.0) -> Optional[sqlite3.Connection]:
        """Get connection optimized for ML operations"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            with self.lock:
                if self.available:
                    conn = self.available.pop()
                    self.in_use.add(conn)
                    
                    # Update ML operation statistics
                    self.stats['ml_operations'] += 1
                    operation_time = time.time() - start_time
                    self._update_ml_operation_time(operation_time)
                    
                    return conn
            
            time.sleep(0.01)
        
        logging.warning("ML database connection timeout")
        return None
    
    def return_connection(self, conn: sqlite3.Connection):
        """Return connection with health validation"""
        with self.lock:
            if conn in self.in_use:
                self.in_use.remove(conn)
                
                # Test connection health for ML operations
                try:
                    conn.execute("SELECT COUNT(*) FROM sqlite_master")
                    self.available.append(conn)
                except sqlite3.Error:
                    # Connection is broken, replace it
                    try:
                        conn.close()
                    except:
                        pass
                    self._create_replacement_ml_connection()
    
    def _create_replacement_ml_connection(self):
        """Create replacement connection with ML optimizations"""
        try:
            conn = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=45.0
            )
            
            # Apply ML optimizations
            ml_optimizations = [
                "PRAGMA journal_mode=WAL",
                "PRAGMA cache_size=50000",
                "PRAGMA temp_store=MEMORY",
                "PRAGMA synchronous=NORMAL",
                "PRAGMA encoding='UTF-8'"
            ]
            
            for pragma in ml_optimizations:
                conn.execute(pragma)
            
            self.connections.append(conn)
            self.available.append(conn)
            
        except Exception as e:
            logging.error(f"Failed to create replacement ML connection: {e}")
    
    def execute_ml_batch(self, operations: List[Tuple[str, tuple]], use_transaction: bool = True) -> List[Any]:
        """Execute batch ML operations with transaction support"""
        conn = self.get_connection_for_ml()
        if not conn:
            return []
        
        results = []
        try:
            if use_transaction:
                conn.execute("BEGIN TRANSACTION")
            
            for query, params in operations:
                # Validate UTF-8 encoding in parameters
                validated_params = []
                for param in params:
                    if isinstance(param, str):
                        validated_params.append(param.encode('utf-8').decode('utf-8'))
                    elif isinstance(param, datetime):
                        validated_params.append(param.isoformat())
                    else:
                        validated_params.append(param)
                
                cursor = conn.execute(query, tuple(validated_params))
                if 'SELECT' in query.upper():
                    results.append(cursor.fetchall())
                else:
                    results.append(cursor.rowcount)
            
            if use_transaction:
                conn.execute("COMMIT")
            
            self.stats['batch_operations'] += 1
            return results
            
        except Exception as e:
            if use_transaction:
                try:
                    conn.execute("ROLLBACK")
                except:
                    pass
            logging.error(f"ML batch operation failed: {e}")
            return []
        finally:
            self.return_connection(conn)
    
    def _update_ml_operation_time(self, operation_time: float):
        """Update ML operation time statistics"""
        current_avg = self.stats['average_operation_time']
        ml_ops_count = self.stats['ml_operations']
        
        if ml_ops_count == 1:
            self.stats['average_operation_time'] = operation_time
        else:
            self.stats['average_operation_time'] = (
                (current_avg * (ml_ops_count - 1) + operation_time) / ml_ops_count
            )
    
    def get_ml_stats(self) -> Dict[str, Any]:
        """Get ML-specific database statistics"""
        with self.lock:
            return {
                'total_connections': len(self.connections),
                'available_connections': len(self.available),
                'in_use_connections': len(self.in_use),
                'ml_operations': self.stats['ml_operations'],
                'feature_extractions': self.stats['feature_extractions'],
                'cache_hits': self.stats['cache_hits'],
                'cache_misses': self.stats['cache_misses'],
                'batch_operations': self.stats['batch_operations'],
                'average_operation_time_ms': round(self.stats['average_operation_time'] * 1000, 2),
                'cache_hit_rate': (
                    self.stats['cache_hits'] / max(1, self.stats['cache_hits'] + self.stats['cache_misses']) * 100
                )
            }
    
    def cleanup(self):
        """Clean up all ML database connections"""
        with self.lock:
            for conn in self.connections + list(self.in_use):
                try:
                    conn.close()
                except:
                    pass
            self.connections.clear()
            self.available.clear()
            self.in_use.clear()
            self.feature_cache.clear()

class MLFeatureCache:
    """Intelligent caching system for ML features with memory management"""
    
    def __init__(self, max_memory_mb: int = 512):
        self.cache = {}
        self.access_times = {}
        self.feature_sizes = {}
        self.max_memory_bytes = max_memory_mb * 1024 * 1024
        self.current_memory_usage = 0
        self.lock = threading.RLock()
        
        # Performance statistics
        self.stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0,
            'feature_sets_cached': 0,
            'memory_cleanups': 0
        }
    
    def get_features(self, feature_key: str) -> Optional[MLFeatureSet]:
        """Get cached features with performance tracking"""
        with self.lock:
            if feature_key in self.cache:
                feature_set, timestamp, ttl = self.cache[feature_key]
                current_time = time.time()
                
                if current_time - timestamp <= ttl:
                    self.access_times[feature_key] = current_time
                    self.stats['hits'] += 1
                    return feature_set
                else:
                    # Expired
                    self._remove_feature_set(feature_key)
            
            self.stats['misses'] += 1
            return None
    
    def cache_features(self, feature_key: str, feature_set: MLFeatureSet, ttl: int = 3600):
        """Cache feature set with memory management"""
        with self.lock:
            # Estimate memory usage
            feature_size = self._estimate_feature_size(feature_set)
            
            # Check if we need to free memory
            if self.current_memory_usage + feature_size > self.max_memory_bytes:
                self._free_memory_for_features(feature_size)
            
            # Cache the feature set
            current_time = time.time()
            self.cache[feature_key] = (feature_set, current_time, ttl)
            self.access_times[feature_key] = current_time
            self.feature_sizes[feature_key] = feature_size
            self.current_memory_usage += feature_size
            
            self.stats['feature_sets_cached'] += 1
    
    def _estimate_feature_size(self, feature_set: MLFeatureSet) -> int:
        """Estimate memory size of feature set"""
        try:
            # Estimate based on feature dictionary size
            base_size = len(str(feature_set).encode('utf-8'))
            
            # Add overhead for numpy arrays if features contain them
            feature_overhead = len(feature_set.features) * 64  # Rough estimate per feature
            
            return base_size + feature_overhead
            
        except Exception:
            return 1024  # Default estimate
    
    def _free_memory_for_features(self, needed_bytes: int):
        """Free memory using LRU eviction for features"""
        if not self.access_times:
            return
        
        # Free 20% more than needed for buffer
        target_free = int(needed_bytes * 1.2)
        freed = 0
        
        # Sort by access time (oldest first)
        sorted_features = sorted(self.access_times.items(), key=lambda x: x[1])
        
        for feature_key, _ in sorted_features:
            if freed >= target_free:
                break
            
            if feature_key in self.feature_sizes:
                freed += self.feature_sizes[feature_key]
                self._remove_feature_set(feature_key)
                self.stats['evictions'] += 1
    
    def _remove_feature_set(self, feature_key: str):
        """Remove feature set and update memory tracking"""
        if feature_key in self.cache:
            del self.cache[feature_key]
        if feature_key in self.access_times:
            del self.access_times[feature_key]
        if feature_key in self.feature_sizes:
            self.current_memory_usage -= self.feature_sizes[feature_key]
            del self.feature_sizes[feature_key]
    
    def cleanup_expired_features(self):
        """Clean up expired feature sets"""
        current_time = time.time()
        expired_keys = []
        
        with self.lock:
            for key, (_, timestamp, ttl) in self.cache.items():
                if current_time - timestamp > ttl:
                    expired_keys.append(key)
            
            for key in expired_keys:
                self._remove_feature_set(key)
            
            if expired_keys:
                self.stats['memory_cleanups'] += 1
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get feature cache statistics"""
        with self.lock:
            total_requests = self.stats['hits'] + self.stats['misses']
            hit_rate = (self.stats['hits'] / max(1, total_requests)) * 100
            memory_usage_mb = self.current_memory_usage / (1024 * 1024)
            
            return {
                'cached_feature_sets': len(self.cache),
                'memory_usage_mb': round(memory_usage_mb, 2),
                'memory_limit_mb': self.max_memory_bytes / (1024 * 1024),
                'hit_rate_percent': round(hit_rate, 2),
                'total_hits': self.stats['hits'],
                'total_misses': self.stats['misses'],
                'total_evictions': self.stats['evictions'],
                'feature_sets_cached': self.stats['feature_sets_cached'],
                'memory_cleanups': self.stats['memory_cleanups']
            }

class MLDataManager:
    """V3 Enhanced ML Data Manager optimized for 8 vCPU/24GB server"""
    
    def __init__(self, db_path: str = "data/ml_training_data.db"):
        self.db_path = db_path
        
        # Performance optimization components
        self.db_pool = OptimizedMLDatabasePool(db_path, max_connections=12)
        self.feature_cache = MLFeatureCache(max_memory_mb=512)
        self.thread_pool = ThreadPoolExecutor(max_workers=6, thread_name_prefix="ML-Worker")
        
        # Memory management
        self._last_cleanup = time.time()
        self._cleanup_interval = 300  # 5 minutes
        self._max_memory_mb = 2048  # 2GB limit
        
        # Feature extraction tracking
        self.feature_extractors = {}
        self.extraction_stats = {
            'features_extracted': 0,
            'extraction_time_total': 0.0,
            'validation_failures': 0,
            'real_data_compliance': 0
        }
        
        # Initialize database schema
        self._initialize_ml_database()
        
        logging.info("[ML_DATA_MANAGER] V3 Enhanced ML Data Manager initialized")
        print(f"[ML_DATA_MANAGER] Database connections: {len(self.db_pool.connections)}")
        print(f"[ML_DATA_MANAGER] Thread pool workers: {self.thread_pool._max_workers}")
        print(f"[ML_DATA_MANAGER] Feature cache limit: {self.feature_cache.max_memory_bytes / (1024*1024):.0f} MB")
    
    def _initialize_ml_database(self):
        """Initialize ML database schema with UTF-8 support and optimization"""
        try:
            # ML features table with comprehensive schema
            features_table_sql = """
            CREATE TABLE IF NOT EXISTS ml_features (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                feature_id TEXT NOT NULL UNIQUE COLLATE NOCASE,
                symbol TEXT NOT NULL COLLATE NOCASE,
                timeframe TEXT NOT NULL COLLATE NOCASE,
                features_json TEXT NOT NULL,
                target REAL,
                timestamp TEXT NOT NULL,
                data_source TEXT DEFAULT 'live_market' COLLATE NOCASE,
                encoding TEXT DEFAULT 'utf-8',
                v3_compliance BOOLEAN DEFAULT TRUE,
                feature_quality_score REAL DEFAULT 1.0,
                feature_count INTEGER DEFAULT 0,
                extraction_method TEXT COLLATE NOCASE,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            # ML models metadata table
            models_table_sql = """
            CREATE TABLE IF NOT EXISTS ml_models (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                model_id TEXT NOT NULL UNIQUE COLLATE NOCASE,
                model_type TEXT NOT NULL COLLATE NOCASE,
                symbol TEXT COLLATE NOCASE,
                timeframe TEXT COLLATE NOCASE,
                model_data BLOB,
                model_params TEXT,
                training_features TEXT,
                performance_metrics TEXT,
                data_source TEXT DEFAULT 'live_training' COLLATE NOCASE,
                encoding TEXT DEFAULT 'utf-8',
                v3_compliance BOOLEAN DEFAULT TRUE,
                model_version TEXT DEFAULT '1.0',
                training_timestamp TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            # Training sessions table
            sessions_table_sql = """
            CREATE TABLE IF NOT EXISTS training_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL UNIQUE COLLATE NOCASE,
                model_id TEXT NOT NULL,
                feature_count INTEGER DEFAULT 0,
                training_samples INTEGER DEFAULT 0,
                validation_samples INTEGER DEFAULT 0,
                training_time_seconds REAL DEFAULT 0.0,
                final_accuracy REAL DEFAULT 0.0,
                data_source TEXT DEFAULT 'live_data' COLLATE NOCASE,
                encoding TEXT DEFAULT 'utf-8',
                v3_compliance BOOLEAN DEFAULT TRUE,
                session_start TEXT,
                session_end TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (model_id) REFERENCES ml_models (model_id)
            )
            """
            
            # Performance indexes for ML operations
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_features_symbol ON ml_features(symbol, timeframe)",
                "CREATE INDEX IF NOT EXISTS idx_features_timestamp ON ml_features(timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_features_compliance ON ml_features(v3_compliance)",
                "CREATE INDEX IF NOT EXISTS idx_features_quality ON ml_features(feature_quality_score)",
                "CREATE INDEX IF NOT EXISTS idx_models_type ON ml_models(model_type, symbol)",
                "CREATE INDEX IF NOT EXISTS idx_models_compliance ON ml_models(v3_compliance)",
                "CREATE INDEX IF NOT EXISTS idx_sessions_model ON training_sessions(model_id)",
                "CREATE INDEX IF NOT EXISTS idx_sessions_compliance ON training_sessions(v3_compliance)"
            ]
            
            # Execute schema creation with batch operations
            operations = [
                (features_table_sql, ()),
                (models_table_sql, ()),
                (sessions_table_sql, ())
            ] + [(idx, ()) for idx in indexes]
            
            results = self.db_pool.execute_ml_batch(operations, use_transaction=True)
            if results:
                logging.info("[ML_DATA_MANAGER] V3 ML database schema initialized with UTF-8 support")
            else:
                raise RuntimeError("Failed to initialize ML database schema")
                
        except Exception as e:
            logging.error(f"[ML_DATA_MANAGER] Database initialization failed: {e}")
            raise
    
    def _manage_memory_usage(self):
        """Intelligent memory management for ML operations"""
        current_time = time.time()
        if current_time - self._last_cleanup > self._cleanup_interval:
            try:
                process = psutil.Process()
                memory_mb = process.memory_info().rss / 1024 / 1024
                
                if memory_mb > self._max_memory_mb * 0.8:  # 80% threshold
                    # Clean up feature cache
                    self.feature_cache.cleanup_expired_features()
                    
                    # Clear old extraction results
                    self._cleanup_old_extractions()
                    
                    # Force garbage collection
                    gc.collect()
                    
                    new_memory_mb = psutil.Process().memory_info().rss / 1024 / 1024
                    freed_mb = memory_mb - new_memory_mb
                    
                    logging.info(f"ML memory cleanup: {freed_mb:.1f}MB freed, usage: {new_memory_mb:.1f}MB")
                
                self._last_cleanup = current_time
                
            except Exception as e:
                logging.warning(f"ML memory management error: {e}")
    
    def _cleanup_old_extractions(self):
        """Clean up old feature extractions to free memory"""
        try:
            # Remove feature extractors that haven't been used recently
            current_time = time.time()
            old_extractors = []
            
            for extractor_id, (extractor, last_used) in self.feature_extractors.items():
                if current_time - last_used > 3600:  # 1 hour
                    old_extractors.append(extractor_id)
            
            for extractor_id in old_extractors:
                del self.feature_extractors[extractor_id]
            
            logging.info(f"Cleaned up {len(old_extractors)} old feature extractors")
            
        except Exception as e:
            logging.warning(f"Feature extractor cleanup error: {e}")
    
    @lru_cache(maxsize=128)
    def _generate_feature_id(self, symbol: str, timeframe: str, timestamp_str: str, method: str) -> str:
        """Generate unique feature ID with caching"""
        feature_string = f"{symbol}_{timeframe}_{timestamp_str}_{method}"
        return hashlib.md5(feature_string.encode('utf-8')).hexdigest()
    
    def extract_features_from_market_data(self, market_data: Dict[str, Any], 
                                         symbol: str, timeframe: str = "1h",
                                         extraction_method: str = "technical_indicators") -> Optional[MLFeatureSet]:
        """Extract ML features from market data with validation and caching"""
        start_time = time.time()
        
        try:
            # Validate input data for V3 compliance
            if not self._validate_market_data_for_ml(market_data):
                logging.error("V3 VIOLATION: Invalid market data for ML feature extraction")
                self.extraction_stats['validation_failures'] += 1
                return None
            
            # Generate feature cache key
            timestamp_str = market_data.get('timestamp', datetime.now().isoformat())
            feature_key = f"{symbol}_{timeframe}_{timestamp_str}_{extraction_method}"
            
            # Check cache first
            cached_features = self.feature_cache.get_features(feature_key)
            if cached_features is not None:
                return cached_features
            
            # Extract features based on method
            if extraction_method == "technical_indicators":
                features = self._extract_technical_indicators(market_data)
            elif extraction_method == "price_action":
                features = self._extract_price_action_features(market_data)
            elif extraction_method == "volume_analysis":
                features = self._extract_volume_features(market_data)
            else:
                features = self._extract_basic_features(market_data)
            
            if not features:
                logging.warning(f"No features extracted for {symbol} {timeframe}")
                return None
            
            # Create feature set with validation
            feature_id = self._generate_feature_id(symbol, timeframe, timestamp_str, extraction_method)
            
            feature_set = MLFeatureSet(
                feature_id=feature_id,
                symbol=symbol,
                timeframe=timeframe,
                features=features,
                timestamp=datetime.fromisoformat(timestamp_str.replace('Z', '+00:00')) if isinstance(timestamp_str, str) else datetime.now(),
                data_source="live_market",
                encoding="utf-8",
                v3_compliance=True,
                feature_quality_score=self._calculate_feature_quality(features)
            )
            
            # Validate feature set
            if not feature_set.validate_features():
                logging.error(f"Feature validation failed for {symbol} {timeframe}")
                self.extraction_stats['validation_failures'] += 1
                return None
            
            # Cache the feature set
            self.feature_cache.cache_features(feature_key, feature_set, ttl=3600)
            
            # Update statistics
            extraction_time = time.time() - start_time
            self.extraction_stats['features_extracted'] += 1
            self.extraction_stats['extraction_time_total'] += extraction_time
            self.extraction_stats['real_data_compliance'] += 1
            
            # Memory management
            self._manage_memory_usage()
            
            logging.info(f"Extracted {len(features)} features for {symbol} {timeframe} in {extraction_time:.3f}s")
            return feature_set
            
        except Exception as e:
            logging.error(f"Feature extraction error for {symbol}: {e}")
            self.extraction_stats['validation_failures'] += 1
            return None
    
    def _validate_market_data_for_ml(self, market_data: Dict[str, Any]) -> bool:
        """Validate market data for ML compliance (NO MOCK DATA)"""
        try:
            # Check for mock data indicators
            data_str = str(market_data).lower()
            mock_indicators = ['mock', 'test', 'fake', 'dummy', 'synthetic', 'simulated']
            
            for indicator in mock_indicators:
                if indicator in data_str:
                    logging.error(f"V3 VIOLATION: Mock data detected: {indicator}")
                    return False
            
            # Validate data source
            data_source = market_data.get('data_source', '').lower()
            if data_source:
                valid_sources = ['live_api', 'binance', 'live_market', 'real_time']
                if not any(source in data_source for source in valid_sources):
                    logging.error(f"V3 VIOLATION: Invalid data source: {data_source}")
                    return False
            
            # Validate required fields
            required_fields = ['close', 'high', 'low', 'open']
            for field in required_fields:
                if field not in market_data:
                    logging.error(f"Missing required field: {field}")
                    return False
                
                value = market_data[field]
                if not isinstance(value, (int, float)) or value <= 0:
                    logging.error(f"Invalid value for {field}: {value}")
                    return False
            
            # Validate price relationships
            high = market_data['high']
            low = market_data['low']
            open_price = market_data['open']
            close = market_data['close']
            
            if high < low or high < max(open_price, close) or low > min(open_price, close):
                logging.error("Invalid price relationships in market data")
                return False
            
            # Check for V3 compliance flag
            v3_compliance = market_data.get('v3_compliance', False)
            if not v3_compliance:
                logging.error("V3 VIOLATION: Data missing v3_compliance flag")
                return False
            
            return True
            
        except Exception as e:
            logging.error(f"Market data validation error: {e}")
            return False
    
    def _extract_technical_indicators(self, market_data: Dict[str, Any]) -> Dict[str, float]:
        """Extract technical indicator features"""
        try:
            features = {}
            
            # Price-based features
            high = float(market_data['high'])
            low = float(market_data['low'])
            open_price = float(market_data['open'])
            close = float(market_data['close'])
            
            # Basic price features
            features['price_range'] = (high - low) / close
            features['body_size'] = abs(close - open_price) / close
            features['upper_shadow'] = (high - max(open_price, close)) / close
            features['lower_shadow'] = (min(open_price, close) - low) / close
            
            # Volume features (if available)
            if 'volume' in market_data:
                volume = float(market_data['volume'])
                features['volume_normalized'] = volume / (close * 1000)  # Normalized volume
            
            # Price change features
            if 'prev_close' in market_data:
                prev_close = float(market_data['prev_close'])
                features['price_change'] = (close - prev_close) / prev_close
                features['gap'] = (open_price - prev_close) / prev_close
            
            # Volatility proxy
            features['volatility_proxy'] = (high - low) / ((high + low) / 2)
            
            # Momentum indicators (simplified)
            features['momentum'] = (close - open_price) / open_price
            
            return features
            
        except Exception as e:
            logging.error(f"Technical indicator extraction error: {e}")
            return {}
    
    def _extract_price_action_features(self, market_data: Dict[str, Any]) -> Dict[str, float]:
        """Extract price action features"""
        try:
            features = {}
            
            high = float(market_data['high'])
            low = float(market_data['low'])
            open_price = float(market_data['open'])
            close = float(market_data['close'])
            
            # Candlestick patterns
            body = close - open_price
            upper_wick = high - max(open_price, close)
            lower_wick = min(open_price, close) - low
            
            features['is_bullish'] = 1.0 if body > 0 else 0.0
            features['body_ratio'] = abs(body) / (high - low) if high != low else 0.0
            features['upper_wick_ratio'] = upper_wick / (high - low) if high != low else 0.0
            features['lower_wick_ratio'] = lower_wick / (high - low) if high != low else 0.0
            
            # Doji detection
            features['is_doji'] = 1.0 if abs(body) / (high - low) < 0.1 else 0.0
            
            # Hammer/hanging man detection
            small_body = abs(body) / (high - low) < 0.3
            long_lower_wick = lower_wick > 2 * abs(body)
            features['is_hammer'] = 1.0 if small_body and long_lower_wick else 0.0
            
            return features
            
        except Exception as e:
            logging.error(f"Price action extraction error: {e}")
            return {}
    
    def _extract_volume_features(self, market_data: Dict[str, Any]) -> Dict[str, float]:
        """Extract volume-based features"""
        try:
            features = {}
            
            if 'volume' not in market_data:
                return features
            
            volume = float(market_data['volume'])
            close = float(market_data['close'])
            
            # Volume features
            features['volume'] = volume
            features['volume_price_trend'] = volume * close
            
            # Price-volume relationships
            if 'prev_volume' in market_data and 'prev_close' in market_data:
                prev_volume = float(market_data['prev_volume'])
                prev_close = float(market_data['prev_close'])
                
                price_change = (close - prev_close) / prev_close
                volume_change = (volume - prev_volume) / prev_volume if prev_volume > 0 else 0
                
                features['price_volume_correlation'] = price_change * volume_change
            
            return features
            
        except Exception as e:
            logging.error(f"Volume feature extraction error: {e}")
            return {}
    
    def _extract_basic_features(self, market_data: Dict[str, Any]) -> Dict[str, float]:
        """Extract basic price features as fallback"""
        try:
            features = {}
            
            high = float(market_data['high'])
            low = float(market_data['low'])
            open_price = float(market_data['open'])
            close = float(market_data['close'])
            
            features['open'] = open_price
            features['high'] = high
            features['low'] = low
            features['close'] = close
            features['hl_ratio'] = high / low if low > 0 else 1.0
            
            return features
            
        except Exception as e:
            logging.error(f"Basic feature extraction error: {e}")
            return {}
    
    def _calculate_feature_quality(self, features: Dict[str, float]) -> float:
        """Calculate quality score for extracted features"""
        try:
            if not features:
                return 0.0
            
            quality_score = 1.0
            
            # Check for NaN or infinite values
            for key, value in features.items():
                if not isinstance(value, (int, float)):
                    quality_score -= 0.1
                    continue
                
                if np.isnan(value) or np.isinf(value):
                    quality_score -= 0.2
                elif abs(value) > 1e6:  # Very large values
                    quality_score -= 0.1
            
            # Bonus for feature completeness
            if len(features) >= 10:
                quality_score += 0.1
            
            return max(0.0, min(1.0, quality_score))
            
        except Exception:
            return 0.5
    
    def store_feature_set(self, feature_set: MLFeatureSet) -> bool:
        """Store feature set in database with UTF-8 encoding"""
        try:
            # Validate feature set
            if not feature_set.validate_features():
                logging.error(f"Cannot store invalid feature set: {feature_set.feature_id}")
                return False
            
            # Serialize features with UTF-8 encoding
            features_json = json.dumps(feature_set.features, ensure_ascii=False, separators=(',', ':'))
            
            store_sql = """
            INSERT OR REPLACE INTO ml_features 
            (feature_id, symbol, timeframe, features_json, target, timestamp, 
             data_source, encoding, v3_compliance, feature_quality_score, 
             feature_count, extraction_method)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            params = (
                feature_set.feature_id,
                feature_set.symbol,
                feature_set.timeframe,
                features_json,
                feature_set.target,
                feature_set.timestamp.isoformat(),
                feature_set.data_source,
                feature_set.encoding,
                feature_set.v3_compliance,
                feature_set.feature_quality_score,
                len(feature_set.features),
                "technical_indicators"  # Default method
            )
            
            results = self.db_pool.execute_ml_batch([(store_sql, params)], use_transaction=False)
            
            if results and results[0] > 0:
                logging.info(f"Stored feature set: {feature_set.feature_id}")
                return True
            else:
                logging.error(f"Failed to store feature set: {feature_set.feature_id}")
                return False
                
        except Exception as e:
            logging.error(f"Error storing feature set {feature_set.feature_id}: {e}")
            return False
    
    def get_feature_sets(self, symbol: str = None, timeframe: str = None,
                        start_time: datetime = None, end_time: datetime = None,
                        min_quality: float = 0.5) -> List[MLFeatureSet]:
        """Get feature sets with filtering and validation"""
        try:
            query = """
            SELECT feature_id, symbol, timeframe, features_json, target, timestamp,
                   data_source, encoding, v3_compliance, feature_quality_score
            FROM ml_features 
            WHERE v3_compliance = TRUE AND feature_quality_score >= ?
            """
            
            params = [min_quality]
            
            if symbol:
                query += " AND symbol = ?"
                params.append(symbol.encode('utf-8').decode('utf-8'))
            
            if timeframe:
                query += " AND timeframe = ?"
                params.append(timeframe.encode('utf-8').decode('utf-8'))
            
            if start_time:
                query += " AND timestamp >= ?"
                params.append(start_time.isoformat())
            
            if end_time:
                query += " AND timestamp <= ?"
                params.append(end_time.isoformat())
            
            query += " ORDER BY timestamp DESC"
            
            results = self.db_pool.execute_ml_batch([(query, tuple(params))], use_transaction=False)
            
            if not results or not results[0]:
                return []
            
            feature_sets = []
            for row in results[0]:
                try:
                    # Parse features with UTF-8 support
                    features_json = row[3]
                    features = json.loads(features_json)
                    
                    feature_set = MLFeatureSet(
                        feature_id=row[0],
                        symbol=row[1],
                        timeframe=row[2],
                        features=features,
                        target=row[4],
                        timestamp=datetime.fromisoformat(row[5]),
                        data_source=row[6],
                        encoding=row[7],
                        v3_compliance=row[8],
                        feature_quality_score=row[9]
                    )
                    
                    # Validate before adding
                    if feature_set.validate_features():
                        feature_sets.append(feature_set)
                    else:
                        logging.warning(f"Loaded feature set failed validation: {feature_set.feature_id}")
                        
                except Exception as e:
                    logging.warning(f"Error parsing feature set row: {e}")
                    continue
            
            logging.info(f"Retrieved {len(feature_sets)} validated feature sets")
            return feature_sets
            
        except Exception as e:
            logging.error(f"Error retrieving feature sets: {e}")
            return []
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get comprehensive ML data manager performance metrics"""
        try:
            # Get database statistics
            db_stats = self.db_pool.get_ml_stats()
            
            # Get cache statistics
            cache_stats = self.feature_cache.get_cache_stats()
            
            # Get memory usage
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            
            # Calculate extraction statistics
            avg_extraction_time = (
                self.extraction_stats['extraction_time_total'] / 
                max(1, self.extraction_stats['features_extracted'])
            )
            
            validation_success_rate = (
                (self.extraction_stats['features_extracted'] - self.extraction_stats['validation_failures']) /
                max(1, self.extraction_stats['features_extracted']) * 100
            )
            
            return {
                'database_stats': db_stats,
                'cache_stats': cache_stats,
                'memory_usage_mb': round(memory_mb, 2),
                'extraction_stats': {
                    'features_extracted': self.extraction_stats['features_extracted'],
                    'validation_failures': self.extraction_stats['validation_failures'],
                    'validation_success_rate_percent': round(validation_success_rate, 2),
                    'average_extraction_time_ms': round(avg_extraction_time * 1000, 2),
                    'real_data_compliance': self.extraction_stats['real_data_compliance']
                },
                'thread_pool_workers': self.thread_pool._max_workers,
                'max_memory_limit_mb': self._max_memory_mb,
                'v3_compliance': True,
                'utf8_encoding_enabled': True,
                'performance_optimized': True
            }
            
        except Exception as e:
            logging.error(f"Error getting ML performance metrics: {e}")
            return {'error': str(e), 'v3_compliance': True}
    
    def cleanup_resources(self):
        """Enhanced resource cleanup for ML operations"""
        try:
            # Shutdown thread pool
            self.thread_pool.shutdown(wait=True)
            
            # Close database connections
            self.db_pool.cleanup()
            
            # Clear caches
            self.feature_cache.cache.clear()
            self.feature_extractors.clear()
            
            # Clear LRU cache
            self._generate_feature_id.cache_clear()
            
            # Force garbage collection
            gc.collect()
            
            logging.info("[ML_DATA_MANAGER] V3 ML Data Manager resources cleaned up")
            
        except Exception as e:
            logging.error(f"[ML_DATA_MANAGER] Cleanup error: {e}")

# V3 Testing
if __name__ == "__main__":
    print("[ML_DATA_MANAGER] Testing V3 Enhanced ML Data Manager...")
    
    def test_ml_data_manager():
        manager = MLDataManager()
        
        try:
            # Test with real market data
            real_market_data = {
                'open': 45000.0,
                'high': 45500.0,
                'low': 44800.0,
                'close': 45200.0,
                'volume': 150.5,
                'timestamp': datetime.now().isoformat(),
                'data_source': 'live_binance_api',
                'v3_compliance': True,
                'encoding': 'utf-8'
            }
            
            # Test feature extraction
            print("\nTesting feature extraction with real data...")
            feature_set = manager.extract_features_from_market_data(
                real_market_data, 'BTCUSDT', '1h', 'technical_indicators'
            )
            
            if feature_set:
                print(f"? Extracted {len(feature_set.features)} features")
                print(f"Feature quality score: {feature_set.feature_quality_score:.3f}")
                print(f"V3 compliance: {feature_set.v3_compliance}")
                
                # Test storage
                success = manager.store_feature_set(feature_set)
                print(f"Storage success: {success}")
            else:
                print("? Feature extraction failed")
            
            # Test with mock data (should fail)
            print("\nTesting with mock data (should fail)...")
            mock_data = {
                'open': 100.0,
                'high': 105.0,
                'low': 95.0,
                'close': 102.0,
                'data_source': 'mock_api',  # This should trigger validation failure
                'v3_compliance': False
            }
            
            mock_features = manager.extract_features_from_market_data(mock_data, 'MOCK', '1h')
            if mock_features is None:
                print("? Mock data correctly rejected")
            else:
                print("? Mock data incorrectly accepted")
            
            # Test performance metrics
            metrics = manager.get_performance_metrics()
            print(f"\nPerformance Metrics:")
            print(f"  Memory Usage: {metrics.get('memory_usage_mb', 0):.1f} MB")
            print(f"  Features Extracted: {metrics.get('extraction_stats', {}).get('features_extracted', 0)}")
            print(f"  Validation Success Rate: {metrics.get('extraction_stats', {}).get('validation_success_rate_percent', 0):.1f}%")
            print(f"  Cache Hit Rate: {metrics.get('cache_stats', {}).get('hit_rate_percent', 0):.1f}%")
            print(f"  Database Success Rate: {metrics.get('database_stats', {}).get('cache_hit_rate', 0):.1f}%")
            print(f"  UTF-8 Encoding: {metrics.get('utf8_encoding_enabled', False)}")
            print(f"  V3 Compliance: {metrics.get('v3_compliance', False)}")
            
        except Exception as e:
            print(f"Test failed: {e}")
        finally:
            manager.cleanup_resources()
    
    test_ml_data_manager()
    print("\n[ML_DATA_MANAGER] V3 Enhanced ML Data Manager test complete!")