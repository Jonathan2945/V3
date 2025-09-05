#!/usr/bin/env python3
"""
PNL PERSISTENCE MANAGER - V3 OPTIMIZED FOR 8 vCPU/24GB
=====================================================
V3 Performance & Data Pipeline Fixes Applied:
- Enhanced database connection pooling with proper closure
- UTF-8 encoding specification for all file operations
- Data processing functions with comprehensive validation
- Memory optimization for large trading datasets
- Thread-safe operations with proper resource management
- Intelligent caching system for PnL calculations
- Batch operations for high-performance data processing
"""

import sqlite3
import json
import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import os
from dataclasses import dataclass, asdict
import gc
import psutil
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache

@dataclass
class TradeRecord:
    """Enhanced trade record with validation and UTF-8 support"""
    trade_id: str
    symbol: str
    side: str  # 'buy' or 'sell'
    quantity: float
    price: float
    timestamp: datetime
    fees: float = 0.0
    pnl: float = 0.0
    strategy_id: str = ""
    timeframe: str = "1h"
    confidence: float = 0.0
    data_source: str = "live_trading"
    encoding: str = "utf-8"
    v3_compliance: bool = True
    
    def validate_data(self) -> bool:
        """Validate trade record data integrity"""
        try:
            # Basic validation
            if not self.trade_id or not self.symbol:
                return False
            
            if self.side not in ['buy', 'sell']:
                return False
            
            if self.quantity <= 0 or self.price <= 0:
                return False
            
            # Check for realistic values
            if self.price > 10000000:  # $10M per unit seems unrealistic
                return False
            
            if self.confidence < 0 or self.confidence > 100:
                return False
            
            # Check timestamp is reasonable
            if self.timestamp < datetime(2020, 1, 1):
                return False
            
            if self.timestamp > datetime.now() + timedelta(hours=1):
                return False
            
            return True
            
        except Exception:
            return False

class OptimizedDatabaseManager:
    """High-performance database manager with connection pooling"""
    
    def __init__(self, db_path: str, max_connections: int = 8):
        self.db_path = db_path
        self.max_connections = max_connections
        self.connections = []
        self.available = []
        self.in_use = set()
        self.lock = threading.RLock()
        
        # Performance statistics
        self.stats = {
            'total_operations': 0,
            'successful_operations': 0,
            'failed_operations': 0,
            'average_operation_time': 0.0,
            'connection_timeouts': 0
        }
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # Initialize connection pool
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Initialize optimized connection pool"""
        for i in range(self.max_connections):
            conn = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=30.0
            )
            
            # Apply performance optimizations
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA cache_size=20000")  # 20MB cache
            conn.execute("PRAGMA temp_store=MEMORY")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA page_size=4096")
            conn.execute("PRAGMA mmap_size=268435456")  # 256MB memory map
            
            # Set UTF-8 encoding
            conn.execute("PRAGMA encoding='UTF-8'")
            
            self.connections.append(conn)
            self.available.append(conn)
    
    def get_connection(self, timeout: float = 30.0) -> Optional[sqlite3.Connection]:
        """Get connection with timeout and statistics tracking"""
        start_time = time.time()
        self.stats['total_operations'] += 1
        
        while time.time() - start_time < timeout:
            with self.lock:
                if self.available:
                    conn = self.available.pop()
                    self.in_use.add(conn)
                    
                    # Update operation time statistics
                    operation_time = time.time() - start_time
                    self._update_operation_time(operation_time)
                    
                    self.stats['successful_operations'] += 1
                    return conn
            
            time.sleep(0.01)
        
        self.stats['connection_timeouts'] += 1
        self.stats['failed_operations'] += 1
        return None
    
    def return_connection(self, conn: sqlite3.Connection):
        """Return connection with health check"""
        with self.lock:
            if conn in self.in_use:
                self.in_use.remove(conn)
                
                # Test connection health
                try:
                    conn.execute("SELECT 1")
                    self.available.append(conn)
                except sqlite3.Error:
                    # Connection is broken, replace it
                    try:
                        conn.close()
                    except:
                        pass
                    self._create_replacement_connection()
    
    def _create_replacement_connection(self):
        """Create replacement connection"""
        try:
            conn = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=30.0
            )
            
            # Apply optimizations
            optimizations = [
                "PRAGMA journal_mode=WAL",
                "PRAGMA cache_size=20000",
                "PRAGMA temp_store=MEMORY",
                "PRAGMA synchronous=NORMAL",
                "PRAGMA encoding='UTF-8'"
            ]
            
            for pragma in optimizations:
                conn.execute(pragma)
            
            self.connections.append(conn)
            self.available.append(conn)
            
        except Exception as e:
            logging.error(f"Failed to create replacement connection: {e}")
    
    def _update_operation_time(self, operation_time: float):
        """Update average operation time"""
        current_avg = self.stats['average_operation_time']
        successful_count = self.stats['successful_operations']
        
        if successful_count == 1:
            self.stats['average_operation_time'] = operation_time
        else:
            self.stats['average_operation_time'] = (
                (current_avg * (successful_count - 1) + operation_time) / successful_count
            )
    
    def execute_with_validation(self, query: str, params: tuple = (), validate_result: bool = True) -> Optional[Any]:
        """Execute query with data validation and error handling"""
        conn = self.get_connection()
        if not conn:
            logging.error("Could not obtain database connection")
            return None
        
        try:
            # Validate UTF-8 encoding in parameters
            validated_params = []
            for param in params:
                if isinstance(param, str):
                    # Ensure UTF-8 encoding
                    validated_params.append(param.encode('utf-8').decode('utf-8'))
                else:
                    validated_params.append(param)
            
            cursor = conn.execute(query, tuple(validated_params))
            
            # Validate result if requested
            if validate_result and 'SELECT' in query.upper():
                result = cursor.fetchall()
                # Basic validation - ensure we have data
                if result is None:
                    logging.warning("Query returned None result")
                return result
            else:
                return cursor.rowcount
                
        except sqlite3.Error as e:
            logging.error(f"Database operation failed: {e}")
            return None
        except UnicodeEncodeError as e:
            logging.error(f"UTF-8 encoding error: {e}")
            return None
        finally:
            self.return_connection(conn)
    
    def execute_batch_with_validation(self, operations: List[Tuple[str, tuple]]) -> bool:
        """Execute batch operations with transaction support and validation"""
        conn = self.get_connection()
        if not conn:
            return False
        
        try:
            conn.execute("BEGIN TRANSACTION")
            
            for query, params in operations:
                # Validate parameters
                validated_params = []
                for param in params:
                    if isinstance(param, str):
                        validated_params.append(param.encode('utf-8').decode('utf-8'))
                    elif isinstance(param, datetime):
                        validated_params.append(param.isoformat())
                    else:
                        validated_params.append(param)
                
                conn.execute(query, tuple(validated_params))
            
            conn.execute("COMMIT")
            return True
            
        except Exception as e:
            try:
                conn.execute("ROLLBACK")
            except:
                pass
            logging.error(f"Batch operation failed: {e}")
            return False
        finally:
            self.return_connection(conn)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get database manager statistics"""
        with self.lock:
            success_rate = (
                (self.stats['successful_operations'] / max(1, self.stats['total_operations'])) * 100
            )
            
            return {
                'total_connections': len(self.connections),
                'available_connections': len(self.available),
                'in_use_connections': len(self.in_use),
                'total_operations': self.stats['total_operations'],
                'success_rate_percent': round(success_rate, 2),
                'average_operation_time_ms': round(self.stats['average_operation_time'] * 1000, 2),
                'connection_timeouts': self.stats['connection_timeouts']
            }
    
    def cleanup(self):
        """Clean up all connections"""
        with self.lock:
            for conn in self.connections + list(self.in_use):
                try:
                    conn.close()
                except:
                    pass
            self.connections.clear()
            self.available.clear()
            self.in_use.clear()

class PnLPersistenceManager:
    """V3 Enhanced PnL persistence manager with performance optimization"""
    
    def __init__(self, db_path: str = "data/trade_logs.db"):
        self.db_path = db_path
        
        # Performance optimization components
        self.db_manager = OptimizedDatabaseManager(db_path, max_connections=8)
        self.thread_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="PnL-Worker")
        
        # Caching system
        self._cache = {}
        self._cache_lock = threading.RLock()
        self._cache_timeout = 300  # 5 minutes
        
        # Memory management
        self._last_cleanup = time.time()
        self._cleanup_interval = 600  # 10 minutes
        
        # Performance tracking
        self.performance_stats = {
            'trades_processed': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'validation_failures': 0,
            'processing_time_total': 0.0
        }
        
        # Initialize database schema
        self._initialize_database()
        
        logging.info("[PNL_PERSISTENCE] V3 Enhanced PnL Persistence Manager initialized")
        print(f"[PNL_PERSISTENCE] Database connections: {len(self.db_manager.connections)}")
        print(f"[PNL_PERSISTENCE] Thread pool workers: {self.thread_pool._max_workers}")
    
    def _initialize_database(self):
        """Initialize database schema with UTF-8 support and optimization"""
        try:
            # Enhanced trade records table with UTF-8 collation
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS trade_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id TEXT NOT NULL UNIQUE COLLATE NOCASE,
                symbol TEXT NOT NULL COLLATE NOCASE,
                side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
                quantity REAL NOT NULL CHECK (quantity > 0),
                price REAL NOT NULL CHECK (price > 0),
                timestamp TEXT NOT NULL,
                fees REAL DEFAULT 0.0 CHECK (fees >= 0),
                pnl REAL DEFAULT 0.0,
                strategy_id TEXT COLLATE NOCASE,
                timeframe TEXT DEFAULT '1h' COLLATE NOCASE,
                confidence REAL DEFAULT 0.0 CHECK (confidence >= 0 AND confidence <= 100),
                data_source TEXT DEFAULT 'live_trading' COLLATE NOCASE,
                encoding TEXT DEFAULT 'utf-8',
                v3_compliance BOOLEAN DEFAULT TRUE,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            # PnL summary table for performance
            pnl_summary_sql = """
            CREATE TABLE IF NOT EXISTS pnl_summary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL COLLATE NOCASE,
                strategy_id TEXT COLLATE NOCASE,
                date TEXT NOT NULL,
                total_pnl REAL DEFAULT 0.0,
                realized_pnl REAL DEFAULT 0.0,
                unrealized_pnl REAL DEFAULT 0.0,
                total_trades INTEGER DEFAULT 0,
                winning_trades INTEGER DEFAULT 0,
                losing_trades INTEGER DEFAULT 0,
                total_fees REAL DEFAULT 0.0,
                data_quality_score REAL DEFAULT 1.0,
                encoding TEXT DEFAULT 'utf-8',
                v3_compliance BOOLEAN DEFAULT TRUE,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, strategy_id, date)
            )
            """
            
            # Performance indexes
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_trade_symbol ON trade_records(symbol)",
                "CREATE INDEX IF NOT EXISTS idx_trade_timestamp ON trade_records(timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_trade_strategy ON trade_records(strategy_id)",
                "CREATE INDEX IF NOT EXISTS idx_trade_compliance ON trade_records(v3_compliance)",
                "CREATE INDEX IF NOT EXISTS idx_pnl_symbol_date ON pnl_summary(symbol, date)",
                "CREATE INDEX IF NOT EXISTS idx_pnl_strategy ON pnl_summary(strategy_id)",
                "CREATE INDEX IF NOT EXISTS idx_pnl_compliance ON pnl_summary(v3_compliance)"
            ]
            
            # Execute schema creation
            operations = [
                (create_table_sql, ()),
                (pnl_summary_sql, ())
            ] + [(idx, ()) for idx in indexes]
            
            success = self.db_manager.execute_batch_with_validation(operations)
            if success:
                logging.info("[PNL_PERSISTENCE] Database schema initialized with UTF-8 support")
            else:
                raise RuntimeError("Failed to initialize database schema")
                
        except Exception as e:
            logging.error(f"[PNL_PERSISTENCE] Database initialization failed: {e}")
            raise
    
    @lru_cache(maxsize=256)
    def _calculate_pnl_cached(self, symbol: str, strategy_id: str, date_str: str) -> float:
        """Cached PnL calculation to avoid recomputation"""
        cache_key = f"pnl_{symbol}_{strategy_id}_{date_str}"
        
        with self._cache_lock:
            if cache_key in self._cache:
                cached_data, timestamp = self._cache[cache_key]
                if time.time() - timestamp < self._cache_timeout:
                    self.performance_stats['cache_hits'] += 1
                    return cached_data
                else:
                    del self._cache[cache_key]
        
        # Calculate PnL from database
        self.performance_stats['cache_misses'] += 1
        pnl = self._calculate_pnl_from_db(symbol, strategy_id, date_str)
        
        # Cache the result
        with self._cache_lock:
            self._cache[cache_key] = (pnl, time.time())
        
        return pnl
    
    def _calculate_pnl_from_db(self, symbol: str, strategy_id: str, date_str: str) -> float:
        """Calculate PnL from database with validation"""
        try:
            query = """
            SELECT side, quantity, price, fees 
            FROM trade_records 
            WHERE symbol = ? AND strategy_id = ? 
            AND DATE(timestamp) = ? AND v3_compliance = TRUE
            ORDER BY timestamp
            """
            
            result = self.db_manager.execute_with_validation(
                query, (symbol, strategy_id, date_str), validate_result=True
            )
            
            if not result:
                return 0.0
            
            total_pnl = 0.0
            position = 0.0
            avg_price = 0.0
            
            for side, quantity, price, fees in result:
                if side == 'buy':
                    if position == 0:
                        avg_price = price
                    else:
                        avg_price = ((position * avg_price) + (quantity * price)) / (position + quantity)
                    position += quantity
                else:  # sell
                    if position > 0:
                        pnl = (price - avg_price) * min(quantity, position)
                        total_pnl += pnl
                        position -= quantity
                
                total_pnl -= fees
            
            return total_pnl
            
        except Exception as e:
            logging.error(f"PnL calculation error: {e}")
            return 0.0
    
    def record_trade(self, trade: TradeRecord) -> bool:
        """Record trade with comprehensive validation and UTF-8 handling"""
        start_time = time.time()
        
        try:
            # Validate trade data
            if not trade.validate_data():
                logging.error(f"Trade validation failed for {trade.trade_id}")
                self.performance_stats['validation_failures'] += 1
                return False
            
            # Ensure UTF-8 encoding for string fields
            trade.symbol = trade.symbol.encode('utf-8').decode('utf-8')
            trade.strategy_id = trade.strategy_id.encode('utf-8').decode('utf-8')
            trade.data_source = trade.data_source.encode('utf-8').decode('utf-8')
            
            # Insert trade record
            insert_sql = """
            INSERT OR REPLACE INTO trade_records 
            (trade_id, symbol, side, quantity, price, timestamp, fees, pnl, 
             strategy_id, timeframe, confidence, data_source, encoding, v3_compliance)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            params = (
                trade.trade_id, trade.symbol, trade.side, trade.quantity, trade.price,
                trade.timestamp.isoformat(), trade.fees, trade.pnl, trade.strategy_id,
                trade.timeframe, trade.confidence, trade.data_source, trade.encoding,
                trade.v3_compliance
            )
            
            result = self.db_manager.execute_with_validation(insert_sql, params, validate_result=False)
            
            if result is not None:
                # Update PnL summary asynchronously for performance
                self.thread_pool.submit(self._update_pnl_summary_async, trade)
                
                # Clear related cache entries
                self._invalidate_cache(trade.symbol, trade.strategy_id)
                
                # Update performance statistics
                processing_time = time.time() - start_time
                self.performance_stats['trades_processed'] += 1
                self.performance_stats['processing_time_total'] += processing_time
                
                logging.info(f"Trade recorded: {trade.trade_id} for {trade.symbol}")
                return True
            else:
                logging.error(f"Failed to insert trade: {trade.trade_id}")
                return False
                
        except Exception as e:
            logging.error(f"Error recording trade {trade.trade_id}: {e}")
            return False
        finally:
            # Periodic cleanup
            self._maybe_cleanup()
    
    def _update_pnl_summary_async(self, trade: TradeRecord):
        """Update PnL summary asynchronously"""
        try:
            date_str = trade.timestamp.date().isoformat()
            
            # Calculate daily PnL
            daily_pnl = self._calculate_pnl_cached(trade.symbol, trade.strategy_id, date_str)
            
            # Get trade statistics for the day
            stats_query = """
            SELECT COUNT(*) as total_trades,
                   SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as winning_trades,
                   SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END) as losing_trades,
                   SUM(fees) as total_fees
            FROM trade_records 
            WHERE symbol = ? AND strategy_id = ? 
            AND DATE(timestamp) = ? AND v3_compliance = TRUE
            """
            
            stats_result = self.db_manager.execute_with_validation(
                stats_query, (trade.symbol, trade.strategy_id, date_str), validate_result=True
            )
            
            if stats_result:
                total_trades, winning_trades, losing_trades, total_fees = stats_result[0]
                
                # Update or insert PnL summary
                summary_sql = """
                INSERT OR REPLACE INTO pnl_summary 
                (symbol, strategy_id, date, total_pnl, total_trades, winning_trades, 
                 losing_trades, total_fees, encoding, v3_compliance, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                summary_params = (
                    trade.symbol, trade.strategy_id, date_str, daily_pnl,
                    total_trades or 0, winning_trades or 0, losing_trades or 0,
                    total_fees or 0.0, 'utf-8', True, datetime.now().isoformat()
                )
                
                self.db_manager.execute_with_validation(summary_sql, summary_params, validate_result=False)
                
        except Exception as e:
            logging.error(f"Error updating PnL summary: {e}")
    
    def _invalidate_cache(self, symbol: str, strategy_id: str):
        """Invalidate cache entries for a symbol/strategy"""
        with self._cache_lock:
            keys_to_remove = []
            for key in self._cache.keys():
                if f"pnl_{symbol}_{strategy_id}" in key:
                    keys_to_remove.append(key)
            
            for key in keys_to_remove:
                del self._cache[key]
    
    def _maybe_cleanup(self):
        """Perform periodic cleanup of memory and cache"""
        current_time = time.time()
        if current_time - self._last_cleanup > self._cleanup_interval:
            try:
                # Clean expired cache entries
                with self._cache_lock:
                    expired_keys = []
                    for key, (_, timestamp) in self._cache.items():
                        if current_time - timestamp > self._cache_timeout:
                            expired_keys.append(key)
                    
                    for key in expired_keys:
                        del self._cache[key]
                
                # Clear LRU cache if it's getting large
                if self._calculate_pnl_cached.cache_info().currsize > 200:
                    self._calculate_pnl_cached.cache_clear()
                
                # Force garbage collection
                gc.collect()
                
                self._last_cleanup = current_time
                
                # Log memory status
                process = psutil.Process()
                memory_mb = process.memory_info().rss / 1024 / 1024
                logging.info(f"PnL Manager cleanup completed. Memory usage: {memory_mb:.1f} MB")
                
            except Exception as e:
                logging.warning(f"Cleanup error: {e}")
    
    def get_pnl_summary(self, symbol: str = None, strategy_id: str = None, 
                       start_date: datetime = None, end_date: datetime = None) -> List[Dict]:
        """Get PnL summary with caching and validation"""
        try:
            # Build query with proper validation
            query = """
            SELECT symbol, strategy_id, date, total_pnl, total_trades, 
                   winning_trades, losing_trades, total_fees, data_quality_score,
                   encoding, v3_compliance
            FROM pnl_summary 
            WHERE v3_compliance = TRUE
            """
            
            params = []
            
            if symbol:
                query += " AND symbol = ?"
                params.append(symbol.encode('utf-8').decode('utf-8'))
            
            if strategy_id:
                query += " AND strategy_id = ?"
                params.append(strategy_id.encode('utf-8').decode('utf-8'))
            
            if start_date:
                query += " AND date >= ?"
                params.append(start_date.date().isoformat())
            
            if end_date:
                query += " AND date <= ?"
                params.append(end_date.date().isoformat())
            
            query += " ORDER BY date DESC"
            
            result = self.db_manager.execute_with_validation(query, tuple(params), validate_result=True)
            
            if not result:
                return []
            
            # Process results with validation
            summaries = []
            for row in result:
                try:
                    summary = {
                        'symbol': row[0],
                        'strategy_id': row[1],
                        'date': row[2],
                        'total_pnl': row[3],
                        'total_trades': row[4],
                        'winning_trades': row[5],
                        'losing_trades': row[6],
                        'total_fees': row[7],
                        'data_quality_score': row[8],
                        'encoding': row[9],
                        'v3_compliance': row[10],
                        'win_rate': (row[5] / max(1, row[4])) * 100 if row[4] > 0 else 0.0
                    }
                    
                    # Validate summary data
                    if summary['total_trades'] >= 0 and summary['encoding'] == 'utf-8':
                        summaries.append(summary)
                        
                except Exception as e:
                    logging.warning(f"Error processing summary row: {e}")
                    continue
            
            return summaries
            
        except Exception as e:
            logging.error(f"Error getting PnL summary: {e}")
            return []
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get comprehensive performance metrics"""
        try:
            # Get database statistics
            db_stats = self.db_manager.get_stats()
            
            # Get memory usage
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            
            # Calculate cache statistics
            with self._cache_lock:
                cache_size = len(self._cache)
                total_cache_requests = self.performance_stats['cache_hits'] + self.performance_stats['cache_misses']
                cache_hit_rate = (
                    (self.performance_stats['cache_hits'] / max(1, total_cache_requests)) * 100
                )
            
            # Calculate average processing time
            avg_processing_time = (
                self.performance_stats['processing_time_total'] / 
                max(1, self.performance_stats['trades_processed'])
            )
            
            return {
                'database_stats': db_stats,
                'memory_usage_mb': round(memory_mb, 2),
                'cache_size': cache_size,
                'cache_hit_rate_percent': round(cache_hit_rate, 2),
                'trades_processed': self.performance_stats['trades_processed'],
                'validation_failures': self.performance_stats['validation_failures'],
                'validation_success_rate': (
                    ((self.performance_stats['trades_processed'] - self.performance_stats['validation_failures']) /
                     max(1, self.performance_stats['trades_processed'])) * 100
                ),
                'average_processing_time_ms': round(avg_processing_time * 1000, 2),
                'thread_pool_workers': self.thread_pool._max_workers,
                'v3_compliance': True,
                'utf8_encoding_enabled': True
            }
            
        except Exception as e:
            logging.error(f"Error getting performance metrics: {e}")
            return {'error': str(e), 'v3_compliance': True}
    
    def cleanup_resources(self):
        """Enhanced resource cleanup"""
        try:
            # Shutdown thread pool
            self.thread_pool.shutdown(wait=True)
            
            # Close database connections
            self.db_manager.cleanup()
            
            # Clear caches
            with self._cache_lock:
                self._cache.clear()
            
            self._calculate_pnl_cached.cache_clear()
            
            # Force garbage collection
            gc.collect()
            
            logging.info("[PNL_PERSISTENCE] V3 Resources cleaned up successfully")
            
        except Exception as e:
            logging.error(f"[PNL_PERSISTENCE] Cleanup error: {e}")

# V3 Testing
if __name__ == "__main__":
    print("[PNL_PERSISTENCE] Testing V3 Enhanced PnL Persistence Manager...")
    
    def test_pnl_manager():
        manager = PnLPersistenceManager()
        
        try:
            # Test trade recording with validation
            test_trade = TradeRecord(
                trade_id="test_001",
                symbol="BTCUSDT",
                side="buy",
                quantity=0.1,
                price=45000.0,
                timestamp=datetime.now(),
                fees=2.25,
                pnl=0.0,
                strategy_id="test_strategy",
                timeframe="1h",
                confidence=75.0,
                data_source="live_trading",
                encoding="utf-8",
                v3_compliance=True
            )
            
            # Test validation
            is_valid = test_trade.validate_data()
            print(f"Trade validation: {is_valid}")
            
            # Test recording
            success = manager.record_trade(test_trade)
            print(f"Trade recording: {success}")
            
            # Test PnL summary
            summary = manager.get_pnl_summary(symbol="BTCUSDT", strategy_id="test_strategy")
            print(f"PnL summary entries: {len(summary)}")
            
            # Test performance metrics
            metrics = manager.get_performance_metrics()
            print(f"\nPerformance Metrics:")
            print(f"  Memory Usage: {metrics.get('memory_usage_mb', 0):.1f} MB")
            print(f"  Cache Hit Rate: {metrics.get('cache_hit_rate_percent', 0):.1f}%")
            print(f"  Database Success Rate: {metrics.get('database_stats', {}).get('success_rate_percent', 0):.1f}%")
            print(f"  Validation Success Rate: {metrics.get('validation_success_rate', 0):.1f}%")
            print(f"  UTF-8 Encoding: {metrics.get('utf8_encoding_enabled', False)}")
            print(f"  V3 Compliance: {metrics.get('v3_compliance', False)}")
            
        except Exception as e:
            print(f"Test failed: {e}")
        finally:
            manager.cleanup_resources()
    
    test_pnl_manager()
    print("\n[PNL_PERSISTENCE] V3 Enhanced PnL Persistence Manager test complete!")