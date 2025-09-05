#!/usr/bin/env python3
"""
V3 Historical Data Manager - Performance Optimized
Enhanced with proper database transaction management and commit handling
"""

import asyncio
import time
import logging
import threading
import sqlite3
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict, deque
from functools import lru_cache, wraps
import concurrent.futures
import hashlib
import psutil
import queue
from dataclasses import dataclass
import pickle
import gzip
import os

class TransactionManager:
    """Enhanced transaction manager with proper commit handling"""
    
    def __init__(self, db_path: str, max_connections: int = 20):
        self.db_path = db_path
        self.max_connections = max_connections
        self.connections = queue.Queue(maxsize=max_connections)
        self.total_connections = 0
        self.lock = threading.Lock()
        
        # Transaction statistics
        self.transaction_stats = {
            'total_transactions': 0,
            'successful_commits': 0,
            'failed_commits': 0,
            'rollbacks': 0,
            'auto_commits': 0,
            'manual_commits': 0,
            'avg_transaction_time': 0.0
        }
        
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Initialize the connection pool with proper transaction settings"""
        try:
            for _ in range(min(8, self.max_connections)):
                conn = self._create_connection()
                if conn:
                    self.connections.put(conn)
                    self.total_connections += 1
            
            logging.info(f"Historical data transaction pool initialized with {self.total_connections} connections")
            
        except Exception as e:
            logging.error(f"Transaction pool initialization error: {e}")
    
    def _create_connection(self) -> Optional[sqlite3.Connection]:
        """Create connection with proper transaction settings"""
        try:
            conn = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=60.0,
                isolation_level='DEFERRED'  # Enable explicit transaction control
            )
            
            # Optimize for historical data workloads
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=NORMAL')
            conn.execute('PRAGMA cache_size=50000')  # Large cache for historical data
            conn.execute('PRAGMA temp_store=MEMORY')
            conn.execute('PRAGMA mmap_size=1073741824')  # 1GB mmap
            conn.execute('PRAGMA page_size=32768')  # Large page size for bulk data
            conn.execute('PRAGMA wal_autocheckpoint=1000')
            
            # Create historical data tables
            self._create_historical_tables(conn)
            
            return conn
            
        except Exception as e:
            logging.error(f"Historical data connection creation error: {e}")
            return None
    
    def _create_historical_tables(self, conn: sqlite3.Connection):
        """Create historical data tables with proper indexing"""
        try:
            # Main historical data table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS historical_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    timestamp DATETIME NOT NULL,
                    open_price REAL NOT NULL,
                    high_price REAL NOT NULL,
                    low_price REAL NOT NULL,
                    close_price REAL NOT NULL,
                    volume REAL NOT NULL,
                    quote_volume REAL,
                    trades_count INTEGER,
                    taker_buy_volume REAL,
                    taker_buy_quote_volume REAL,
                    data_source TEXT NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, timeframe, timestamp, data_source)
                )
            ''')
            
            # Data quality tracking table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS data_quality (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    date_range_start DATETIME NOT NULL,
                    date_range_end DATETIME NOT NULL,
                    total_records INTEGER NOT NULL,
                    missing_records INTEGER DEFAULT 0,
                    duplicate_records INTEGER DEFAULT 0,
                    quality_score REAL DEFAULT 100.0,
                    last_validation DATETIME DEFAULT CURRENT_TIMESTAMP,
                    validation_status TEXT DEFAULT 'pending',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Data synchronization tracking
            conn.execute('''
                CREATE TABLE IF NOT EXISTS sync_status (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    last_sync_timestamp DATETIME,
                    sync_status TEXT DEFAULT 'pending',
                    records_synced INTEGER DEFAULT 0,
                    sync_duration REAL DEFAULT 0.0,
                    error_message TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, timeframe)
                )
            ''')
            
            # Compressed historical data for long-term storage
            conn.execute('''
                CREATE TABLE IF NOT EXISTS compressed_historical_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    date_range_start DATETIME NOT NULL,
                    date_range_end DATETIME NOT NULL,
                    compressed_data BLOB NOT NULL,
                    compression_ratio REAL,
                    record_count INTEGER NOT NULL,
                    checksum TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, timeframe, date_range_start)
                )
            ''')
            
            # Transaction log for audit trail
            conn.execute('''
                CREATE TABLE IF NOT EXISTS transaction_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    transaction_id TEXT NOT NULL,
                    operation_type TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    record_count INTEGER,
                    transaction_status TEXT NOT NULL,
                    execution_time REAL,
                    error_message TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create indexes for performance
            indexes = [
                'CREATE INDEX IF NOT EXISTS idx_historical_symbol_timeframe ON historical_data(symbol, timeframe)',
                'CREATE INDEX IF NOT EXISTS idx_historical_timestamp ON historical_data(timestamp)',
                'CREATE INDEX IF NOT EXISTS idx_historical_symbol_time ON historical_data(symbol, timeframe, timestamp)',
                'CREATE INDEX IF NOT EXISTS idx_quality_symbol ON data_quality(symbol, timeframe)',
                'CREATE INDEX IF NOT EXISTS idx_sync_status ON sync_status(symbol, timeframe)',
                'CREATE INDEX IF NOT EXISTS idx_compressed_symbol ON compressed_historical_data(symbol, timeframe)',
                'CREATE INDEX IF NOT EXISTS idx_transaction_log_id ON transaction_log(transaction_id)',
                'CREATE INDEX IF NOT EXISTS idx_transaction_log_time ON transaction_log(created_at)'
            ]
            
            for index_sql in indexes:
                conn.execute(index_sql)
            
            # Explicit commit for table creation
            conn.commit()
            
        except Exception as e:
            logging.error(f"Historical table creation error: {e}")
            conn.rollback()
    
    def get_connection(self) -> Optional[sqlite3.Connection]:
        """Get connection from pool"""
        try:
            if not self.connections.empty():
                return self.connections.get(timeout=5)
            
            # Create new connection if under limit
            with self.lock:
                if self.total_connections < self.max_connections:
                    conn = self._create_connection()
                    if conn:
                        self.total_connections += 1
                        return conn
            
            # Wait for available connection
            return self.connections.get(timeout=30)
            
        except queue.Empty:
            logging.warning("No database connections available for historical data")
            return None
        except Exception as e:
            logging.error(f"Error getting database connection: {e}")
            return None
    
    def return_connection(self, conn: sqlite3.Connection):
        """Return connection to pool"""
        try:
            if conn and not self.connections.full():
                # Test connection is still valid
                conn.execute('SELECT 1')
                self.connections.put(conn)
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
    
    def execute_transaction(self, operations: List[Tuple[str, tuple]], 
                          commit_strategy: str = 'auto') -> Dict[str, Any]:
        """Execute transaction with proper commit handling"""
        start_time = time.time()
        transaction_id = hashlib.md5(f"{time.time()}_{threading.current_thread().ident}".encode()).hexdigest()[:12]
        
        conn = None
        result = {
            'success': False,
            'transaction_id': transaction_id,
            'records_affected': 0,
            'execution_time': 0.0,
            'commit_strategy': commit_strategy,
            'error': None
        }
        
        try:
            conn = self.get_connection()
            if not conn:
                raise Exception("No database connection available")
            
            # Start explicit transaction
            conn.execute('BEGIN IMMEDIATE')
            
            total_affected = 0
            for sql, params in operations:
                cursor = conn.cursor()
                cursor.execute(sql, params)
                total_affected += cursor.rowcount
            
            result['records_affected'] = total_affected
            
            # Handle commit strategy
            if commit_strategy == 'auto':
                conn.commit()
                result['success'] = True
                self.transaction_stats['successful_commits'] += 1
                self.transaction_stats['auto_commits'] += 1
                
            elif commit_strategy == 'manual':
                # Don't commit yet, let caller handle it
                result['success'] = True
                self.transaction_stats['manual_commits'] += 1
                
            elif commit_strategy == 'batch':
                # Commit only if batch size is met
                if total_affected >= 100:  # Batch size threshold
                    conn.commit()
                    result['success'] = True
                    self.transaction_stats['successful_commits'] += 1
                else:
                    result['success'] = True  # Success but no commit yet
                    
            else:
                # Default to auto commit
                conn.commit()
                result['success'] = True
                self.transaction_stats['successful_commits'] += 1
                self.transaction_stats['auto_commits'] += 1
            
            execution_time = time.time() - start_time
            result['execution_time'] = execution_time
            
            # Log transaction
            self._log_transaction(transaction_id, 'bulk_insert', 'historical_data', 
                                total_affected, 'success', execution_time)
            
            # Update statistics
            self.transaction_stats['total_transactions'] += 1
            self._update_avg_transaction_time(execution_time)
            
            return result
            
        except Exception as e:
            execution_time = time.time() - start_time
            result['execution_time'] = execution_time
            result['error'] = str(e)
            
            if conn:
                try:
                    conn.rollback()
                    self.transaction_stats['rollbacks'] += 1
                except:
                    pass
            
            self.transaction_stats['failed_commits'] += 1
            self.transaction_stats['total_transactions'] += 1
            
            # Log failed transaction
            self._log_transaction(transaction_id, 'bulk_insert', 'historical_data', 
                                0, 'failed', execution_time, str(e))
            
            logging.error(f"Transaction {transaction_id} failed: {e}")
            return result
            
        finally:
            if conn:
                self.return_connection(conn)
    
    def execute_single_operation(self, sql: str, params: tuple, 
                                commit: bool = True) -> Dict[str, Any]:
        """Execute single operation with commit control"""
        start_time = time.time()
        transaction_id = hashlib.md5(f"{time.time()}_{sql[:20]}".encode()).hexdigest()[:8]
        
        conn = None
        result = {
            'success': False,
            'transaction_id': transaction_id,
            'records_affected': 0,
            'execution_time': 0.0,
            'committed': commit,
            'error': None
        }
        
        try:
            conn = self.get_connection()
            if not conn:
                raise Exception("No database connection available")
            
            cursor = conn.cursor()
            cursor.execute(sql, params)
            result['records_affected'] = cursor.rowcount
            
            if commit:
                conn.commit()
                self.transaction_stats['successful_commits'] += 1
                self.transaction_stats['auto_commits'] += 1
            
            result['success'] = True
            execution_time = time.time() - start_time
            result['execution_time'] = execution_time
            
            self.transaction_stats['total_transactions'] += 1
            self._update_avg_transaction_time(execution_time)
            
            return result
            
        except Exception as e:
            execution_time = time.time() - start_time
            result['execution_time'] = execution_time
            result['error'] = str(e)
            
            if conn:
                try:
                    conn.rollback()
                    self.transaction_stats['rollbacks'] += 1
                except:
                    pass
            
            self.transaction_stats['failed_commits'] += 1
            self.transaction_stats['total_transactions'] += 1
            
            logging.error(f"Single operation {transaction_id} failed: {e}")
            return result
            
        finally:
            if conn:
                self.return_connection(conn)
    
    def _log_transaction(self, transaction_id: str, operation_type: str, 
                        table_name: str, record_count: int, status: str,
                        execution_time: float, error_message: str = None):
        """Log transaction for audit trail"""
        try:
            conn = self.get_connection()
            if conn:
                try:
                    conn.execute('''
                        INSERT INTO transaction_log 
                        (transaction_id, operation_type, table_name, record_count, 
                         transaction_status, execution_time, error_message)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (transaction_id, operation_type, table_name, record_count,
                          status, execution_time, error_message))
                    conn.commit()  # Always commit transaction logs
                finally:
                    self.return_connection(conn)
        except Exception as e:
            logging.error(f"Error logging transaction: {e}")
    
    def _update_avg_transaction_time(self, execution_time: float):
        """Update rolling average transaction time"""
        if self.transaction_stats['avg_transaction_time'] == 0:
            self.transaction_stats['avg_transaction_time'] = execution_time
        else:
            self.transaction_stats['avg_transaction_time'] = (
                self.transaction_stats['avg_transaction_time'] * 0.9 + execution_time * 0.1
            )
    
    def get_transaction_stats(self) -> Dict[str, Any]:
        """Get transaction statistics"""
        stats = self.transaction_stats.copy()
        
        if stats['total_transactions'] > 0:
            stats['success_rate'] = stats['successful_commits'] / stats['total_transactions']
            stats['rollback_rate'] = stats['rollbacks'] / stats['total_transactions']
        else:
            stats['success_rate'] = 0
            stats['rollback_rate'] = 0
        
        stats['active_connections'] = self.total_connections
        return stats

@dataclass
class HistoricalDataPoint:
    """Historical data point structure"""
    symbol: str
    timeframe: str
    timestamp: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    quote_volume: Optional[float] = None
    trades_count: Optional[int] = None
    data_source: str = 'unknown'

class DataCompressor:
    """Compress historical data for long-term storage"""
    
    @staticmethod
    def compress_data(data_points: List[HistoricalDataPoint]) -> bytes:
        """Compress historical data points"""
        try:
            # Convert to DataFrame for efficient storage
            df_data = []
            for point in data_points:
                df_data.append({
                    'timestamp': point.timestamp.isoformat(),
                    'open': point.open_price,
                    'high': point.high_price,
                    'low': point.low_price,
                    'close': point.close_price,
                    'volume': point.volume,
                    'quote_volume': point.quote_volume,
                    'trades_count': point.trades_count
                })
            
            df = pd.DataFrame(df_data)
            
            # Serialize to bytes
            serialized = pickle.dumps(df)
            
            # Compress with gzip
            compressed = gzip.compress(serialized)
            
            return compressed
            
        except Exception as e:
            logging.error(f"Error compressing data: {e}")
            return b''
    
    @staticmethod
    def decompress_data(compressed_data: bytes, symbol: str, 
                       timeframe: str, data_source: str) -> List[HistoricalDataPoint]:
        """Decompress historical data points"""
        try:
            # Decompress
            decompressed = gzip.decompress(compressed_data)
            
            # Deserialize
            df = pickle.loads(decompressed)
            
            # Convert back to data points
            data_points = []
            for _, row in df.iterrows():
                point = HistoricalDataPoint(
                    symbol=symbol,
                    timeframe=timeframe,
                    timestamp=datetime.fromisoformat(row['timestamp']),
                    open_price=row['open'],
                    high_price=row['high'],
                    low_price=row['low'],
                    close_price=row['close'],
                    volume=row['volume'],
                    quote_volume=row['quote_volume'],
                    trades_count=row['trades_count'],
                    data_source=data_source
                )
                data_points.append(point)
            
            return data_points
            
        except Exception as e:
            logging.error(f"Error decompressing data: {e}")
            return []

class HistoricalDataManager:
    """
    Enhanced Historical Data Manager with Performance Optimization
    Optimized for 8 vCPU / 24GB server specifications with proper transaction management
    """
    
    def __init__(self, config_manager=None):
        self.config = config_manager
        
        # Transaction manager for proper commit handling
        self.transaction_manager = TransactionManager('historical_data.db', max_connections=25)
        
        # Data compression utility
        self.compressor = DataCompressor()
        
        # Performance optimization
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=20)
        
        # Data caching
        self.data_cache = {}
        self.cache_lock = threading.RLock()
        self.cache_stats = {'hits': 0, 'misses': 0}
        
        # Performance tracking
        self.manager_stats = {
            'total_inserts': 0,
            'successful_inserts': 0,
            'failed_inserts': 0,
            'total_queries': 0,
            'cache_hit_rate': 0.0,
            'avg_insert_time': 0.0,
            'avg_query_time': 0.0,
            'data_compression_ratio': 0.0
        }
        
        # Start background tasks
        self._start_background_tasks()
    
    async def store_historical_data_async(self, data_points: List[HistoricalDataPoint], 
                                        commit_strategy: str = 'auto') -> Dict[str, Any]:
        """Store historical data with proper transaction management"""
        start_time = time.time()
        
        try:
            if not data_points:
                return {'success': False, 'error': 'No data points provided'}
            
            # Validate data points
            validated_points = await self._validate_data_points_async(data_points)
            if not validated_points:
                return {'success': False, 'error': 'No valid data points after validation'}
            
            # Prepare insert operations
            operations = []
            for point in validated_points:
                operations.append((
                    '''INSERT OR REPLACE INTO historical_data 
                       (symbol, timeframe, timestamp, open_price, high_price, low_price, 
                        close_price, volume, quote_volume, trades_count, taker_buy_volume, 
                        taker_buy_quote_volume, data_source, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (
                        point.symbol, point.timeframe, point.timestamp,
                        point.open_price, point.high_price, point.low_price, point.close_price,
                        point.volume, point.quote_volume, point.trades_count,
                        None, None,  # taker_buy_volume, taker_buy_quote_volume
                        point.data_source, datetime.now()
                    )
                ))
            
            # Execute transaction with proper commit handling
            result = await asyncio.get_event_loop().run_in_executor(
                self.executor,
                self.transaction_manager.execute_transaction,
                operations,
                commit_strategy
            )
            
            if result['success']:
                # Update sync status
                await self._update_sync_status_async(validated_points, result)
                
                # Update data quality metrics
                await self._update_data_quality_async(validated_points)
                
                # Compress old data if needed
                if len(validated_points) > 1000:
                    await self._compress_old_data_async(validated_points[0].symbol, validated_points[0].timeframe)
            
            # Update performance statistics
            execution_time = time.time() - start_time
            self._update_insert_stats(result['success'], execution_time, len(validated_points))
            
            result['validation_summary'] = {
                'original_count': len(data_points),
                'validated_count': len(validated_points),
                'execution_time': execution_time
            }
            
            return result
            
        except Exception as e:
            execution_time = time.time() - start_time
            self._update_insert_stats(False, execution_time, 0)
            logging.error(f"Error storing historical data: {e}")
            return {
                'success': False,
                'error': str(e),
                'execution_time': execution_time
            }
    
    async def _validate_data_points_async(self, data_points: List[HistoricalDataPoint]) -> List[HistoricalDataPoint]:
        """Validate data points asynchronously"""
        try:
            loop = asyncio.get_event_loop()
            
            def validate_points():
                validated = []
                
                for point in data_points:
                    # Basic validation
                    if not point.symbol or not point.timeframe:
                        continue
                    
                    # Price validation
                    if (point.open_price <= 0 or point.high_price <= 0 or 
                        point.low_price <= 0 or point.close_price <= 0):
                        continue
                    
                    # OHLC logic validation
                    if not (point.low_price <= point.open_price <= point.high_price and
                            point.low_price <= point.close_price <= point.high_price):
                        continue
                    
                    # Volume validation
                    if point.volume < 0:
                        continue
                    
                    # Timestamp validation
                    if point.timestamp > datetime.now():
                        continue
                    
                    validated.append(point)
                
                return validated
            
            return await loop.run_in_executor(self.executor, validate_points)
            
        except Exception as e:
            logging.error(f"Error validating data points: {e}")
            return []
    
    async def _update_sync_status_async(self, data_points: List[HistoricalDataPoint], 
                                      transaction_result: Dict[str, Any]):
        """Update synchronization status with proper commit"""
        try:
            if not data_points:
                return
            
            # Group by symbol and timeframe
            symbol_tf_groups = defaultdict(list)
            for point in data_points:
                key = f"{point.symbol}_{point.timeframe}"
                symbol_tf_groups[key].append(point)
            
            # Update sync status for each group
            for key, points in symbol_tf_groups.items():
                symbol = points[0].symbol
                timeframe = points[0].timeframe
                
                latest_timestamp = max(point.timestamp for point in points)
                
                result = self.transaction_manager.execute_single_operation(
                    '''INSERT OR REPLACE INTO sync_status 
                       (symbol, timeframe, last_sync_timestamp, sync_status, 
                        records_synced, sync_duration, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)''',
                    (symbol, timeframe, latest_timestamp, 'completed',
                     len(points), transaction_result['execution_time'], datetime.now()),
                    commit=True  # Always commit sync status updates
                )
                
                if not result['success']:
                    logging.error(f"Failed to update sync status for {symbol} {timeframe}")
        
        except Exception as e:
            logging.error(f"Error updating sync status: {e}")
    
    async def _update_data_quality_async(self, data_points: List[HistoricalDataPoint]):
        """Update data quality metrics with proper commit"""
        try:
            if not data_points:
                return
            
            # Group by symbol and timeframe
            symbol_tf_groups = defaultdict(list)
            for point in data_points:
                key = f"{point.symbol}_{point.timeframe}"
                symbol_tf_groups[key].append(point)
            
            # Calculate quality metrics for each group
            for key, points in symbol_tf_groups.items():
                symbol = points[0].symbol
                timeframe = points[0].timeframe
                
                start_time = min(point.timestamp for point in points)
                end_time = max(point.timestamp for point in points)
                
                # Check for missing data points (simplified)
                expected_count = len(points)  # This should be calculated based on timeframe
                actual_count = len(points)
                missing_count = max(0, expected_count - actual_count)
                
                # Calculate quality score
                quality_score = 100.0
                if expected_count > 0:
                    quality_score = (actual_count / expected_count) * 100
                
                result = self.transaction_manager.execute_single_operation(
                    '''INSERT OR REPLACE INTO data_quality 
                       (symbol, timeframe, date_range_start, date_range_end, 
                        total_records, missing_records, quality_score, 
                        last_validation, validation_status)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (symbol, timeframe, start_time, end_time, actual_count,
                     missing_count, quality_score, datetime.now(), 'validated'),
                    commit=True  # Always commit quality updates
                )
                
                if not result['success']:
                    logging.error(f"Failed to update data quality for {symbol} {timeframe}")
        
        except Exception as e:
            logging.error(f"Error updating data quality: {e}")
    
    async def _compress_old_data_async(self, symbol: str, timeframe: str):
        """Compress old historical data for storage efficiency"""
        try:
            # Get old data (older than 30 days)
            cutoff_date = datetime.now() - timedelta(days=30)
            
            conn = self.transaction_manager.get_connection()
            if not conn:
                return
            
            try:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT symbol, timeframe, timestamp, open_price, high_price, 
                           low_price, close_price, volume, quote_volume, trades_count, data_source
                    FROM historical_data 
                    WHERE symbol = ? AND timeframe = ? AND timestamp < ?
                    ORDER BY timestamp
                ''', (symbol, timeframe, cutoff_date))
                
                rows = cursor.fetchall()
                
                if len(rows) < 100:  # Not enough data to compress
                    return
                
                # Convert to data points
                data_points = []
                for row in rows:
                    point = HistoricalDataPoint(
                        symbol=row[0], timeframe=row[1], timestamp=datetime.fromisoformat(row[2]),
                        open_price=row[3], high_price=row[4], low_price=row[5],
                        close_price=row[6], volume=row[7], quote_volume=row[8],
                        trades_count=row[9], data_source=row[10]
                    )
                    data_points.append(point)
                
                # Compress data
                compressed_data = self.compressor.compress_data(data_points)
                if not compressed_data:
                    return
                
                # Calculate compression ratio
                original_size = len(pickle.dumps(data_points))
                compression_ratio = len(compressed_data) / original_size
                
                # Store compressed data
                start_time = min(point.timestamp for point in data_points)
                end_time = max(point.timestamp for point in data_points)
                checksum = hashlib.md5(compressed_data).hexdigest()
                
                cursor.execute('''
                    INSERT OR REPLACE INTO compressed_historical_data 
                    (symbol, timeframe, date_range_start, date_range_end, 
                     compressed_data, compression_ratio, record_count, checksum)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (symbol, timeframe, start_time, end_time, compressed_data,
                      compression_ratio, len(data_points), checksum))
                
                # Delete old uncompressed data
                cursor.execute('''
                    DELETE FROM historical_data 
                    WHERE symbol = ? AND timeframe = ? AND timestamp < ?
                ''', (symbol, timeframe, cutoff_date))
                
                # Commit compression transaction
                conn.commit()
                
                # Update compression stats
                self.manager_stats['data_compression_ratio'] = (
                    self.manager_stats['data_compression_ratio'] * 0.9 + compression_ratio * 0.1
                )
                
                logging.info(f"Compressed {len(data_points)} records for {symbol} {timeframe}, "
                           f"compression ratio: {compression_ratio:.2f}")
                
            finally:
                self.transaction_manager.return_connection(conn)
        
        except Exception as e:
            logging.error(f"Error compressing old data: {e}")
    
    async def get_historical_data_async(self, symbol: str, timeframe: str, 
                                      start_time: datetime = None, end_time: datetime = None,
                                      limit: int = 1000) -> List[HistoricalDataPoint]:
        """Get historical data with caching and proper transaction handling"""
        start_query_time = time.time()
        
        try:
            # Create cache key
            cache_key = f"{symbol}_{timeframe}_{start_time}_{end_time}_{limit}"
            
            # Check cache first
            cached_data = self._get_cached_data(cache_key)
            if cached_data:
                self.cache_stats['hits'] += 1
                return cached_data
            
            self.cache_stats['misses'] += 1
            
            # Build query
            if start_time is None:
                start_time = datetime.now() - timedelta(days=30)
            if end_time is None:
                end_time = datetime.now()
            
            # Query from regular table first
            data_points = await self._query_regular_data_async(
                symbol, timeframe, start_time, end_time, limit
            )
            
            # If not enough data, try compressed storage
            if len(data_points) < limit:
                compressed_data = await self._query_compressed_data_async(
                    symbol, timeframe, start_time, end_time, limit - len(data_points)
                )
                data_points.extend(compressed_data)
            
            # Sort by timestamp
            data_points.sort(key=lambda x: x.timestamp)
            
            # Cache the results
            self._cache_data(cache_key, data_points)
            
            # Update query statistics
            query_time = time.time() - start_query_time
            self._update_query_stats(True, query_time)
            
            return data_points
            
        except Exception as e:
            query_time = time.time() - start_query_time
            self._update_query_stats(False, query_time)
            logging.error(f"Error getting historical data: {e}")
            return []
    
    async def _query_regular_data_async(self, symbol: str, timeframe: str,
                                      start_time: datetime, end_time: datetime,
                                      limit: int) -> List[HistoricalDataPoint]:
        """Query regular historical data table"""
        try:
            loop = asyncio.get_event_loop()
            
            def query_data():
                conn = self.transaction_manager.get_connection()
                if not conn:
                    return []
                
                try:
                    cursor = conn.cursor()
                    cursor.execute('''
                        SELECT symbol, timeframe, timestamp, open_price, high_price, 
                               low_price, close_price, volume, quote_volume, trades_count, data_source
                        FROM historical_data 
                        WHERE symbol = ? AND timeframe = ? AND timestamp BETWEEN ? AND ?
                        ORDER BY timestamp DESC
                        LIMIT ?
                    ''', (symbol, timeframe, start_time, end_time, limit))
                    
                    rows = cursor.fetchall()
                    
                    data_points = []
                    for row in rows:
                        point = HistoricalDataPoint(
                            symbol=row[0], timeframe=row[1], 
                            timestamp=datetime.fromisoformat(row[2]) if isinstance(row[2], str) else row[2],
                            open_price=row[3], high_price=row[4], low_price=row[5],
                            close_price=row[6], volume=row[7], quote_volume=row[8],
                            trades_count=row[9], data_source=row[10]
                        )
                        data_points.append(point)
                    
                    return data_points
                    
                finally:
                    self.transaction_manager.return_connection(conn)
            
            return await loop.run_in_executor(self.executor, query_data)
            
        except Exception as e:
            logging.error(f"Error querying regular data: {e}")
            return []
    
    async def _query_compressed_data_async(self, symbol: str, timeframe: str,
                                         start_time: datetime, end_time: datetime,
                                         limit: int) -> List[HistoricalDataPoint]:
        """Query compressed historical data"""
        try:
            loop = asyncio.get_event_loop()
            
            def query_compressed():
                conn = self.transaction_manager.get_connection()
                if not conn:
                    return []
                
                try:
                    cursor = conn.cursor()
                    cursor.execute('''
                        SELECT compressed_data, record_count, checksum
                        FROM compressed_historical_data 
                        WHERE symbol = ? AND timeframe = ? 
                        AND date_range_start <= ? AND date_range_end >= ?
                        ORDER BY date_range_start DESC
                        LIMIT 5
                    ''', (symbol, timeframe, end_time, start_time))
                    
                    rows = cursor.fetchall()
                    all_points = []
                    
                    for row in rows:
                        compressed_data = row[0]
                        expected_count = row[1]
                        stored_checksum = row[2]
                        
                        # Verify checksum
                        calculated_checksum = hashlib.md5(compressed_data).hexdigest()
                        if calculated_checksum != stored_checksum:
                            logging.warning(f"Checksum mismatch for compressed data")
                            continue
                        
                        # Decompress data
                        decompressed_points = self.compressor.decompress_data(
                            compressed_data, symbol, timeframe, 'compressed'
                        )
                        
                        # Filter by time range
                        filtered_points = [
                            point for point in decompressed_points
                            if start_time <= point.timestamp <= end_time
                        ]
                        
                        all_points.extend(filtered_points)
                        
                        if len(all_points) >= limit:
                            break
                    
                    return all_points[:limit]
                    
                finally:
                    self.transaction_manager.return_connection(conn)
            
            return await loop.run_in_executor(self.executor, query_compressed)
            
        except Exception as e:
            logging.error(f"Error querying compressed data: {e}")
            return []
    
    def _get_cached_data(self, cache_key: str) -> Optional[List[HistoricalDataPoint]]:
        """Get data from cache"""
        try:
            with self.cache_lock:
                if cache_key in self.data_cache:
                    cache_entry = self.data_cache[cache_key]
                    if time.time() - cache_entry['timestamp'] < 300:  # 5 minute cache
                        return cache_entry['data']
                    else:
                        del self.data_cache[cache_key]
            return None
        except Exception as e:
            logging.error(f"Error getting cached data: {e}")
            return None
    
    def _cache_data(self, cache_key: str, data_points: List[HistoricalDataPoint]):
        """Cache data in memory"""
        try:
            with self.cache_lock:
                # Limit cache size
                if len(self.data_cache) > 100:
                    # Remove oldest entries
                    oldest_key = min(self.data_cache.keys(), 
                                   key=lambda k: self.data_cache[k]['timestamp'])
                    del self.data_cache[oldest_key]
                
                self.data_cache[cache_key] = {
                    'data': data_points,
                    'timestamp': time.time()
                }
        except Exception as e:
            logging.error(f"Error caching data: {e}")
    
    def _update_insert_stats(self, success: bool, execution_time: float, record_count: int):
        """Update insert performance statistics"""
        try:
            self.manager_stats['total_inserts'] += 1
            
            if success:
                self.manager_stats['successful_inserts'] += 1
            else:
                self.manager_stats['failed_inserts'] += 1
            
            # Update rolling average insert time
            if self.manager_stats['avg_insert_time'] == 0:
                self.manager_stats['avg_insert_time'] = execution_time
            else:
                self.manager_stats['avg_insert_time'] = (
                    self.manager_stats['avg_insert_time'] * 0.9 + execution_time * 0.1
                )
        
        except Exception as e:
            logging.error(f"Error updating insert stats: {e}")
    
    def _update_query_stats(self, success: bool, query_time: float):
        """Update query performance statistics"""
        try:
            self.manager_stats['total_queries'] += 1
            
            # Update rolling average query time
            if self.manager_stats['avg_query_time'] == 0:
                self.manager_stats['avg_query_time'] = query_time
            else:
                self.manager_stats['avg_query_time'] = (
                    self.manager_stats['avg_query_time'] * 0.9 + query_time * 0.1
                )
            
            # Update cache hit rate
            total_cache_requests = self.cache_stats['hits'] + self.cache_stats['misses']
            if total_cache_requests > 0:
                self.manager_stats['cache_hit_rate'] = self.cache_stats['hits'] / total_cache_requests
        
        except Exception as e:
            logging.error(f"Error updating query stats: {e}")
    
    def _start_background_tasks(self):
        """Start background maintenance tasks"""
        def background_worker():
            while True:
                try:
                    self._cleanup_cache()
                    self._vacuum_database()
                    self._log_performance_metrics()
                    time.sleep(600)  # Run every 10 minutes
                except Exception as e:
                    logging.error(f"Background task error: {e}")
                    time.sleep(300)
        
        thread = threading.Thread(target=background_worker, daemon=True)
        thread.start()
    
    def _cleanup_cache(self):
        """Clean up expired cache entries"""
        try:
            current_time = time.time()
            expired_keys = []
            
            with self.cache_lock:
                for key, cache_entry in self.data_cache.items():
                    if current_time - cache_entry['timestamp'] > 600:  # 10 minutes
                        expired_keys.append(key)
                
                for key in expired_keys:
                    del self.data_cache[key]
            
            if expired_keys:
                logging.info(f"Cleaned up {len(expired_keys)} expired cache entries")
        
        except Exception as e:
            logging.error(f"Cache cleanup error: {e}")
    
    def _vacuum_database(self):
        """Vacuum database for optimization"""
        try:
            conn = self.transaction_manager.get_connection()
            if conn:
                try:
                    conn.execute('VACUUM')
                    conn.execute('PRAGMA optimize')
                    conn.commit()  # Commit vacuum operation
                    logging.info("Database vacuum completed")
                finally:
                    self.transaction_manager.return_connection(conn)
        except Exception as e:
            logging.error(f"Database vacuum error: {e}")
    
    def _log_performance_metrics(self):
        """Log current performance metrics"""
        try:
            stats = self.manager_stats.copy()
            transaction_stats = self.transaction_manager.get_transaction_stats()
            
            insert_success_rate = 0
            if stats['total_inserts'] > 0:
                insert_success_rate = stats['successful_inserts'] / stats['total_inserts'] * 100
            
            logging.info(f"Historical Data Manager Metrics - "
                        f"Inserts: {stats['total_inserts']} ({insert_success_rate:.1f}% success), "
                        f"Queries: {stats['total_queries']}, "
                        f"Cache Hit Rate: {stats['cache_hit_rate']:.1%}, "
                        f"Avg Insert Time: {stats['avg_insert_time']:.3f}s, "
                        f"Transaction Success Rate: {transaction_stats['success_rate']:.1%}")
        
        except Exception as e:
            logging.error(f"Performance logging error: {e}")
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get comprehensive performance summary"""
        try:
            summary = {
                'manager_stats': self.manager_stats.copy(),
                'transaction_stats': self.transaction_manager.get_transaction_stats(),
                'cache_stats': self.cache_stats.copy(),
                'cache_size': len(self.data_cache),
                'system_resources': {
                    'memory_percent': psutil.virtual_memory().percent,
                    'cpu_percent': psutil.cpu_percent(),
                    'thread_pool_workers': self.executor._max_workers
                },
                'last_updated': datetime.now().isoformat()
            }
            
            return summary
            
        except Exception as e:
            logging.error(f"Error getting performance summary: {e}")
            return {}
    
    def optimize_for_server_specs(self):
        """Optimize for 8 vCPU / 24GB server specifications"""
        try:
            cpu_count = psutil.cpu_count()
            memory_gb = psutil.virtual_memory().total / (1024**3)
            
            # Adjust thread pool size
            optimal_workers = min(cpu_count * 3, 24)  # 3x CPU cores for I/O heavy operations
            if self.executor._max_workers != optimal_workers:
                self.executor.shutdown(wait=False)
                self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=optimal_workers)
            
            # Adjust database connections for high memory systems
            if memory_gb >= 24:
                if self.transaction_manager.max_connections < 30:
                    self.transaction_manager.max_connections = 30
            
            logging.info(f"Historical data manager optimized for {cpu_count} CPUs, {memory_gb:.1f}GB RAM")
            
        except Exception as e:
            logging.error(f"Server optimization error: {e}")

# Export main classes
__all__ = ['HistoricalDataManager', 'TransactionManager', 'HistoricalDataPoint', 'DataCompressor']

if __name__ == "__main__":
    # Performance test
    async def test_historical_data_manager():
        manager = HistoricalDataManager()
        manager.optimize_for_server_specs()
        
        # Test data storage
        test_data = []
        for i in range(100):
            point = HistoricalDataPoint(
                symbol='BTCUSDT',
                timeframe='1h',
                timestamp=datetime.now() - timedelta(hours=i),
                open_price=50000 + i,
                high_price=50100 + i,
                low_price=49900 + i,
                close_price=50050 + i,
                volume=1000 + i,
                data_source='test'
            )
            test_data.append(point)
        
        # Store data with proper commit handling
        result = await manager.store_historical_data_async(test_data, commit_strategy='auto')
        print(f"Storage result: {result['success']}, records: {result.get('records_affected', 0)}")
        
        # Query data
        retrieved_data = await manager.get_historical_data_async('BTCUSDT', '1h', limit=50)
        print(f"Retrieved {len(retrieved_data)} records")
        
        # Get performance summary
        summary = manager.get_performance_summary()
        print(f"Performance Summary: {json.dumps(summary, indent=2, default=str)}")
    
    # Run test
    asyncio.run(test_historical_data_manager())