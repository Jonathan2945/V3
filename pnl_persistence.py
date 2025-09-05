#!/usr/bin/env python3
"""
V3 PnL Persistence Manager - Performance Optimized
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
from decimal import Decimal, ROUND_HALF_UP
import uuid

@dataclass
class TradeRecord:
    """Trade record data structure"""
    trade_id: str
    symbol: str
    side: str  # 'buy' or 'sell'
    quantity: float
    entry_price: float
    exit_price: Optional[float]
    entry_time: datetime
    exit_time: Optional[datetime]
    pnl_realized: Optional[float]
    pnl_unrealized: Optional[float]
    fees: float
    strategy: str
    status: str  # 'open', 'closed', 'partial'

@dataclass
class PnLSnapshot:
    """PnL snapshot data structure"""
    snapshot_id: str
    timestamp: datetime
    total_balance: float
    realized_pnl: float
    unrealized_pnl: float
    total_pnl: float
    open_positions_count: int
    closed_positions_count: int
    win_rate: float
    profit_factor: float
    max_drawdown: float
    sharpe_ratio: float

class DatabaseTransactionManager:
    """Enhanced database transaction manager for PnL persistence"""
    
    def __init__(self, db_path: str, max_connections: int = 15):
        self.db_path = db_path
        self.max_connections = max_connections
        self.connections = queue.Queue(maxsize=max_connections)
        self.total_connections = 0
        self.lock = threading.Lock()
        
        # Transaction tracking with detailed metrics
        self.transaction_metrics = {
            'total_transactions': 0,
            'successful_commits': 0,
            'failed_commits': 0,
            'rollbacks': 0,
            'auto_commits': 0,
            'manual_commits': 0,
            'batch_commits': 0,
            'avg_transaction_time': 0.0,
            'total_records_processed': 0,
            'last_commit_time': None
        }
        
        self._initialize_connection_pool()
    
    def _initialize_connection_pool(self):
        """Initialize connection pool with optimized settings"""
        try:
            for _ in range(min(5, self.max_connections)):
                conn = self._create_optimized_connection()
                if conn:
                    self.connections.put(conn)
                    self.total_connections += 1
            
            logging.info(f"PnL persistence connection pool initialized with {self.total_connections} connections")
            
        except Exception as e:
            logging.error(f"Connection pool initialization error: {e}")
    
    def _create_optimized_connection(self) -> Optional[sqlite3.Connection]:
        """Create optimized database connection for PnL operations"""
        try:
            conn = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=45.0,
                isolation_level='DEFERRED'  # Enable explicit transaction control
            )
            
            # Optimize for PnL workloads
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=NORMAL')
            conn.execute('PRAGMA cache_size=30000')
            conn.execute('PRAGMA temp_store=MEMORY')
            conn.execute('PRAGMA mmap_size=536870912')  # 512MB
            conn.execute('PRAGMA page_size=16384')
            conn.execute('PRAGMA wal_autocheckpoint=1000')
            conn.execute('PRAGMA optimize')
            
            # Enable foreign keys
            conn.execute('PRAGMA foreign_keys=ON')
            
            # Create PnL tables
            self._create_pnl_tables(conn)
            
            return conn
            
        except Exception as e:
            logging.error(f"PnL connection creation error: {e}")
            return None
    
    def _create_pnl_tables(self, conn: sqlite3.Connection):
        """Create PnL persistence tables with proper constraints"""
        try:
            # Trades table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_id TEXT UNIQUE NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
                    quantity REAL NOT NULL CHECK (quantity > 0),
                    entry_price REAL NOT NULL CHECK (entry_price > 0),
                    exit_price REAL CHECK (exit_price IS NULL OR exit_price > 0),
                    entry_time DATETIME NOT NULL,
                    exit_time DATETIME,
                    pnl_realized REAL DEFAULT 0.0,
                    pnl_unrealized REAL DEFAULT 0.0,
                    fees REAL DEFAULT 0.0,
                    strategy TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'closed', 'partial')),
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # PnL snapshots table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS pnl_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_id TEXT UNIQUE NOT NULL,
                    timestamp DATETIME NOT NULL,
                    total_balance REAL NOT NULL DEFAULT 0.0,
                    realized_pnl REAL NOT NULL DEFAULT 0.0,
                    unrealized_pnl REAL NOT NULL DEFAULT 0.0,
                    total_pnl REAL NOT NULL DEFAULT 0.0,
                    open_positions_count INTEGER NOT NULL DEFAULT 0,
                    closed_positions_count INTEGER NOT NULL DEFAULT 0,
                    win_rate REAL DEFAULT 0.0 CHECK (win_rate >= 0 AND win_rate <= 100),
                    profit_factor REAL DEFAULT 0.0,
                    max_drawdown REAL DEFAULT 0.0,
                    sharpe_ratio REAL DEFAULT 0.0,
                    snapshot_data TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Daily PnL summary table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS daily_pnl_summary (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date DATE NOT NULL,
                    daily_realized_pnl REAL NOT NULL DEFAULT 0.0,
                    daily_unrealized_pnl REAL NOT NULL DEFAULT 0.0,
                    daily_total_pnl REAL NOT NULL DEFAULT 0.0,
                    trades_count INTEGER NOT NULL DEFAULT 0,
                    winning_trades INTEGER NOT NULL DEFAULT 0,
                    losing_trades INTEGER NOT NULL DEFAULT 0,
                    total_fees REAL NOT NULL DEFAULT 0.0,
                    largest_win REAL DEFAULT 0.0,
                    largest_loss REAL DEFAULT 0.0,
                    avg_trade_pnl REAL DEFAULT 0.0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(date)
                )
            ''')
            
            # Portfolio balance history
            conn.execute('''
                CREATE TABLE IF NOT EXISTS portfolio_balance_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME NOT NULL,
                    total_balance REAL NOT NULL,
                    available_balance REAL NOT NULL,
                    locked_balance REAL NOT NULL DEFAULT 0.0,
                    equity REAL NOT NULL,
                    margin_used REAL DEFAULT 0.0,
                    margin_available REAL DEFAULT 0.0,
                    balance_change REAL DEFAULT 0.0,
                    balance_change_pct REAL DEFAULT 0.0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Transaction log for audit trail
            conn.execute('''
                CREATE TABLE IF NOT EXISTS transaction_audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    transaction_id TEXT NOT NULL,
                    operation_type TEXT NOT NULL,
                    table_affected TEXT NOT NULL,
                    records_affected INTEGER DEFAULT 0,
                    operation_status TEXT NOT NULL,
                    execution_time REAL DEFAULT 0.0,
                    user_context TEXT,
                    error_details TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create indexes for performance
            indexes = [
                'CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol)',
                'CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status)',
                'CREATE INDEX IF NOT EXISTS idx_trades_entry_time ON trades(entry_time)',
                'CREATE INDEX IF NOT EXISTS idx_trades_trade_id ON trades(trade_id)',
                'CREATE INDEX IF NOT EXISTS idx_snapshots_timestamp ON pnl_snapshots(timestamp)',
                'CREATE INDEX IF NOT EXISTS idx_daily_summary_date ON daily_pnl_summary(date)',
                'CREATE INDEX IF NOT EXISTS idx_balance_history_timestamp ON portfolio_balance_history(timestamp)',
                'CREATE INDEX IF NOT EXISTS idx_audit_log_transaction_id ON transaction_audit_log(transaction_id)',
                'CREATE INDEX IF NOT EXISTS idx_audit_log_timestamp ON transaction_audit_log(created_at)'
            ]
            
            for index_sql in indexes:
                conn.execute(index_sql)
            
            # Explicit commit for table creation
            conn.commit()
            
        except Exception as e:
            logging.error(f"PnL table creation error: {e}")
            conn.rollback()
    
    def get_connection(self) -> Optional[sqlite3.Connection]:
        """Get connection from pool with timeout handling"""
        try:
            if not self.connections.empty():
                return self.connections.get(timeout=3)
            
            # Create new connection if under limit
            with self.lock:
                if self.total_connections < self.max_connections:
                    conn = self._create_optimized_connection()
                    if conn:
                        self.total_connections += 1
                        return conn
            
            # Wait for available connection
            return self.connections.get(timeout=20)
            
        except queue.Empty:
            logging.warning("No PnL database connections available")
            return None
        except Exception as e:
            logging.error(f"Error getting PnL database connection: {e}")
            return None
    
    def return_connection(self, conn: sqlite3.Connection):
        """Return connection to pool with validation"""
        try:
            if conn and not self.connections.full():
                # Test connection validity
                conn.execute('SELECT 1')
                self.connections.put(conn)
            elif conn:
                conn.close()
                with self.lock:
                    self.total_connections -= 1
        except Exception as e:
            logging.error(f"Error returning PnL connection: {e}")
            if conn:
                try:
                    conn.close()
                except:
                    pass
                with self.lock:
                    self.total_connections -= 1
    
    def execute_transactional_operation(self, operations: List[Tuple[str, tuple]], 
                                      commit_mode: str = 'auto') -> Dict[str, Any]:
        """Execute operations with explicit transaction control and proper commits"""
        start_time = time.time()
        transaction_id = str(uuid.uuid4())[:12]
        
        conn = None
        result = {
            'success': False,
            'transaction_id': transaction_id,
            'records_affected': 0,
            'execution_time': 0.0,
            'commit_mode': commit_mode,
            'operations_count': len(operations),
            'error': None
        }
        
        try:
            conn = self.get_connection()
            if not conn:
                raise Exception("No database connection available")
            
            # Start explicit transaction
            conn.execute('BEGIN IMMEDIATE')
            
            total_affected = 0
            operation_results = []
            
            # Execute all operations
            for i, (sql, params) in enumerate(operations):
                try:
                    cursor = conn.cursor()
                    cursor.execute(sql, params)
                    affected = cursor.rowcount
                    total_affected += affected
                    operation_results.append({
                        'operation_index': i,
                        'records_affected': affected,
                        'success': True
                    })
                except Exception as op_error:
                    operation_results.append({
                        'operation_index': i,
                        'records_affected': 0,
                        'success': False,
                        'error': str(op_error)
                    })
                    raise op_error
            
            result['records_affected'] = total_affected
            result['operation_results'] = operation_results
            
            # Handle commit strategy
            if commit_mode == 'auto':
                conn.commit()
                result['success'] = True
                self.transaction_metrics['successful_commits'] += 1
                self.transaction_metrics['auto_commits'] += 1
                
            elif commit_mode == 'manual':
                # Don't commit - let caller handle it
                result['success'] = True
                result['commit_pending'] = True
                self.transaction_metrics['manual_commits'] += 1
                
            elif commit_mode == 'batch':
                # Commit based on batch size
                if total_affected >= 50:  # Batch threshold
                    conn.commit()
                    result['success'] = True
                    self.transaction_metrics['successful_commits'] += 1
                    self.transaction_metrics['batch_commits'] += 1
                else:
                    result['success'] = True
                    result['commit_pending'] = True
                    
            elif commit_mode == 'force':
                # Always commit immediately
                conn.commit()
                result['success'] = True
                self.transaction_metrics['successful_commits'] += 1
                
            else:
                # Default to auto
                conn.commit()
                result['success'] = True
                self.transaction_metrics['successful_commits'] += 1
                self.transaction_metrics['auto_commits'] += 1
            
            execution_time = time.time() - start_time
            result['execution_time'] = execution_time
            
            # Update metrics
            self.transaction_metrics['total_transactions'] += 1
            self.transaction_metrics['total_records_processed'] += total_affected
            self.transaction_metrics['last_commit_time'] = datetime.now().isoformat()
            self._update_avg_execution_time(execution_time)
            
            # Log successful transaction
            self._log_transaction_audit(transaction_id, 'bulk_operation', 'multiple',
                                      total_affected, 'success', execution_time)
            
            return result
            
        except Exception as e:
            execution_time = time.time() - start_time
            result['execution_time'] = execution_time
            result['error'] = str(e)
            
            if conn:
                try:
                    conn.rollback()
                    self.transaction_metrics['rollbacks'] += 1
                except:
                    pass
            
            self.transaction_metrics['failed_commits'] += 1
            self.transaction_metrics['total_transactions'] += 1
            
            # Log failed transaction
            self._log_transaction_audit(transaction_id, 'bulk_operation', 'multiple',
                                      0, 'failed', execution_time, str(e))
            
            logging.error(f"PnL transaction {transaction_id} failed: {e}")
            return result
            
        finally:
            if conn:
                self.return_connection(conn)
    
    def execute_single_commit_operation(self, sql: str, params: tuple) -> Dict[str, Any]:
        """Execute single operation with guaranteed commit"""
        start_time = time.time()
        transaction_id = str(uuid.uuid4())[:8]
        
        conn = None
        result = {
            'success': False,
            'transaction_id': transaction_id,
            'records_affected': 0,
            'execution_time': 0.0,
            'committed': True,
            'error': None
        }
        
        try:
            conn = self.get_connection()
            if not conn:
                raise Exception("No database connection available")
            
            # Execute with explicit transaction
            conn.execute('BEGIN IMMEDIATE')
            cursor = conn.cursor()
            cursor.execute(sql, params)
            result['records_affected'] = cursor.rowcount
            
            # Always commit single operations
            conn.commit()
            result['success'] = True
            
            execution_time = time.time() - start_time
            result['execution_time'] = execution_time
            
            # Update metrics
            self.transaction_metrics['total_transactions'] += 1
            self.transaction_metrics['successful_commits'] += 1
            self.transaction_metrics['auto_commits'] += 1
            self.transaction_metrics['total_records_processed'] += result['records_affected']
            self._update_avg_execution_time(execution_time)
            
            return result
            
        except Exception as e:
            execution_time = time.time() - start_time
            result['execution_time'] = execution_time
            result['error'] = str(e)
            
            if conn:
                try:
                    conn.rollback()
                    self.transaction_metrics['rollbacks'] += 1
                except:
                    pass
            
            self.transaction_metrics['failed_commits'] += 1
            self.transaction_metrics['total_transactions'] += 1
            
            logging.error(f"PnL single operation {transaction_id} failed: {e}")
            return result
            
        finally:
            if conn:
                self.return_connection(conn)
    
    def _log_transaction_audit(self, transaction_id: str, operation_type: str,
                             table_name: str, records_affected: int, status: str,
                             execution_time: float, error_details: str = None):
        """Log transaction for audit trail with separate commit"""
        try:
            # Use a separate connection for audit logging to avoid conflicts
            audit_conn = self._create_optimized_connection()
            if audit_conn:
                try:
                    audit_conn.execute('''
                        INSERT INTO transaction_audit_log 
                        (transaction_id, operation_type, table_affected, records_affected,
                         operation_status, execution_time, error_details)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (transaction_id, operation_type, table_name, records_affected,
                          status, execution_time, error_details))
                    audit_conn.commit()  # Always commit audit logs
                finally:
                    audit_conn.close()
        except Exception as e:
            logging.error(f"Error logging transaction audit: {e}")
    
    def _update_avg_execution_time(self, execution_time: float):
        """Update rolling average execution time"""
        if self.transaction_metrics['avg_transaction_time'] == 0:
            self.transaction_metrics['avg_transaction_time'] = execution_time
        else:
            self.transaction_metrics['avg_transaction_time'] = (
                self.transaction_metrics['avg_transaction_time'] * 0.9 + execution_time * 0.1
            )
    
    def get_transaction_metrics(self) -> Dict[str, Any]:
        """Get comprehensive transaction metrics"""
        metrics = self.transaction_metrics.copy()
        
        if metrics['total_transactions'] > 0:
            metrics['success_rate'] = metrics['successful_commits'] / metrics['total_transactions']
            metrics['failure_rate'] = metrics['failed_commits'] / metrics['total_transactions']
            metrics['rollback_rate'] = metrics['rollbacks'] / metrics['total_transactions']
        else:
            metrics['success_rate'] = 0
            metrics['failure_rate'] = 0
            metrics['rollback_rate'] = 0
        
        metrics['active_connections'] = self.total_connections
        metrics['avg_records_per_transaction'] = (
            metrics['total_records_processed'] / max(metrics['total_transactions'], 1)
        )
        
        return metrics

class PnLCalculator:
    """Enhanced PnL calculation engine with precision handling"""
    
    def __init__(self):
        # Use Decimal for precise financial calculations
        self.decimal_places = 8
        self.rounding_mode = ROUND_HALF_UP
    
    def calculate_realized_pnl(self, trade: TradeRecord) -> float:
        """Calculate realized PnL for closed trade"""
        try:
            if trade.status != 'closed' or trade.exit_price is None:
                return 0.0
            
            entry_value = Decimal(str(trade.quantity)) * Decimal(str(trade.entry_price))
            exit_value = Decimal(str(trade.quantity)) * Decimal(str(trade.exit_price))
            
            if trade.side == 'buy':
                # Long position: profit when exit > entry
                pnl = exit_value - entry_value
            else:
                # Short position: profit when entry > exit
                pnl = entry_value - exit_value
            
            # Subtract fees
            pnl -= Decimal(str(trade.fees))
            
            return float(pnl.quantize(Decimal('0.00000001'), rounding=self.rounding_mode))
            
        except Exception as e:
            logging.error(f"Error calculating realized PnL: {e}")
            return 0.0
    
    def calculate_unrealized_pnl(self, trade: TradeRecord, current_price: float) -> float:
        """Calculate unrealized PnL for open trade"""
        try:
            if trade.status == 'closed':
                return 0.0
            
            entry_value = Decimal(str(trade.quantity)) * Decimal(str(trade.entry_price))
            current_value = Decimal(str(trade.quantity)) * Decimal(str(current_price))
            
            if trade.side == 'buy':
                # Long position
                pnl = current_value - entry_value
            else:
                # Short position
                pnl = entry_value - current_value
            
            # Don't subtract fees for unrealized PnL
            return float(pnl.quantize(Decimal('0.00000001'), rounding=self.rounding_mode))
            
        except Exception as e:
            logging.error(f"Error calculating unrealized PnL: {e}")
            return 0.0
    
    def calculate_portfolio_metrics(self, trades: List[TradeRecord]) -> Dict[str, float]:
        """Calculate comprehensive portfolio metrics"""
        try:
            closed_trades = [t for t in trades if t.status == 'closed']
            open_trades = [t for t in trades if t.status == 'open']
            
            if not closed_trades:
                return {
                    'total_realized_pnl': 0.0,
                    'total_unrealized_pnl': 0.0,
                    'win_rate': 0.0,
                    'profit_factor': 0.0,
                    'avg_win': 0.0,
                    'avg_loss': 0.0,
                    'max_win': 0.0,
                    'max_loss': 0.0,
                    'sharpe_ratio': 0.0,
                    'max_drawdown': 0.0
                }
            
            # Calculate realized metrics
            realized_pnls = [self.calculate_realized_pnl(trade) for trade in closed_trades]
            total_realized = sum(realized_pnls)
            
            winning_trades = [pnl for pnl in realized_pnls if pnl > 0]
            losing_trades = [pnl for pnl in realized_pnls if pnl < 0]
            
            win_rate = len(winning_trades) / len(closed_trades) * 100 if closed_trades else 0
            
            avg_win = sum(winning_trades) / len(winning_trades) if winning_trades else 0
            avg_loss = sum(losing_trades) / len(losing_trades) if losing_trades else 0
            
            total_wins = sum(winning_trades) if winning_trades else 0
            total_losses = abs(sum(losing_trades)) if losing_trades else 0
            
            profit_factor = total_wins / total_losses if total_losses > 0 else float('inf')
            
            # Calculate Sharpe ratio (simplified)
            if len(realized_pnls) > 1:
                returns_std = np.std(realized_pnls)
                avg_return = np.mean(realized_pnls)
                sharpe_ratio = avg_return / returns_std if returns_std > 0 else 0
            else:
                sharpe_ratio = 0
            
            # Calculate max drawdown
            cumulative_pnl = np.cumsum(realized_pnls)
            running_max = np.maximum.accumulate(cumulative_pnl)
            drawdowns = cumulative_pnl - running_max
            max_drawdown = abs(min(drawdowns)) if len(drawdowns) > 0 else 0
            
            return {
                'total_realized_pnl': float(Decimal(str(total_realized)).quantize(
                    Decimal('0.00000001'), rounding=self.rounding_mode)),
                'win_rate': win_rate,
                'profit_factor': profit_factor,
                'avg_win': avg_win,
                'avg_loss': avg_loss,
                'max_win': max(winning_trades) if winning_trades else 0,
                'max_loss': min(losing_trades) if losing_trades else 0,
                'sharpe_ratio': sharpe_ratio,
                'max_drawdown': max_drawdown,
                'total_trades': len(closed_trades),
                'winning_trades': len(winning_trades),
                'losing_trades': len(losing_trades)
            }
            
        except Exception as e:
            logging.error(f"Error calculating portfolio metrics: {e}")
            return {}

class PnLPersistenceManager:
    """
    Enhanced PnL Persistence Manager with Performance Optimization
    Optimized for 8 vCPU / 24GB server specifications with proper transaction management
    """
    
    def __init__(self, config_manager=None):
        self.config = config_manager
        
        # Database transaction manager
        self.db_manager = DatabaseTransactionManager('pnl_data.db', max_connections=20)
        
        # PnL calculator
        self.pnl_calculator = PnLCalculator()
        
        # Performance optimization
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=12)
        
        # In-memory caching
        self.trade_cache = {}
        self.pnl_cache = {}
        self.cache_lock = threading.RLock()
        
        # Performance tracking
        self.persistence_stats = {
            'total_trades_stored': 0,
            'total_snapshots_created': 0,
            'successful_operations': 0,
            'failed_operations': 0,
            'avg_operation_time': 0.0,
            'cache_hit_rate': 0.0,
            'last_operation_time': None
        }
        
        # Start background tasks
        self._start_background_tasks()
    
    async def store_trade_async(self, trade: TradeRecord, commit_immediately: bool = True) -> Dict[str, Any]:
        """Store trade record with proper transaction handling"""
        start_time = time.time()
        
        try:
            # Validate trade record
            if not self._validate_trade_record(trade):
                return {'success': False, 'error': 'Invalid trade record'}
            
            # Calculate PnL if trade is closed
            if trade.status == 'closed' and trade.exit_price is not None:
                trade.pnl_realized = self.pnl_calculator.calculate_realized_pnl(trade)
                trade.pnl_unrealized = 0.0
            elif trade.status == 'open':
                trade.pnl_realized = 0.0
                # Unrealized PnL would be calculated separately with current market price
            
            # Prepare database operation
            operations = [(
                '''INSERT OR REPLACE INTO trades 
                   (trade_id, symbol, side, quantity, entry_price, exit_price, entry_time, exit_time,
                    pnl_realized, pnl_unrealized, fees, strategy, status, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (
                    trade.trade_id, trade.symbol, trade.side, trade.quantity,
                    trade.entry_price, trade.exit_price, trade.entry_time, trade.exit_time,
                    trade.pnl_realized, trade.pnl_unrealized, trade.fees,
                    trade.strategy, trade.status, datetime.now()
                )
            )]
            
            # Execute with proper commit handling
            commit_mode = 'force' if commit_immediately else 'batch'
            result = await asyncio.get_event_loop().run_in_executor(
                self.executor,
                self.db_manager.execute_transactional_operation,
                operations,
                commit_mode
            )
            
            if result['success']:
                # Update cache
                self._cache_trade(trade)
                
                # Update daily summary if trade is closed
                if trade.status == 'closed':
                    await self._update_daily_summary_async(trade)
                
                # Update statistics
                self.persistence_stats['total_trades_stored'] += 1
                self.persistence_stats['successful_operations'] += 1
            else:
                self.persistence_stats['failed_operations'] += 1
            
            execution_time = time.time() - start_time
            self._update_operation_stats(execution_time)
            
            result['trade_id'] = trade.trade_id
            result['execution_time'] = execution_time
            
            return result
            
        except Exception as e:
            execution_time = time.time() - start_time
            self._update_operation_stats(execution_time)
            self.persistence_stats['failed_operations'] += 1
            
            logging.error(f"Error storing trade {trade.trade_id}: {e}")
            return {
                'success': False,
                'error': str(e),
                'trade_id': trade.trade_id,
                'execution_time': execution_time
            }
    
    async def _update_daily_summary_async(self, trade: TradeRecord):
        """Update daily PnL summary with proper commit"""
        try:
            trade_date = trade.exit_time.date() if trade.exit_time else trade.entry_time.date()
            
            # Get existing summary for the date
            existing_summary = await self._get_daily_summary_async(trade_date)
            
            # Calculate new values
            new_realized_pnl = (existing_summary.get('daily_realized_pnl', 0.0) + 
                              (trade.pnl_realized or 0.0))
            new_trades_count = existing_summary.get('trades_count', 0) + 1
            
            is_winning_trade = (trade.pnl_realized or 0.0) > 0
            new_winning_trades = existing_summary.get('winning_trades', 0) + (1 if is_winning_trade else 0)
            new_losing_trades = existing_summary.get('losing_trades', 0) + (0 if is_winning_trade else 1)
            
            new_total_fees = existing_summary.get('total_fees', 0.0) + trade.fees
            
            # Update largest win/loss
            current_pnl = trade.pnl_realized or 0.0
            new_largest_win = max(existing_summary.get('largest_win', 0.0), current_pnl)
            new_largest_loss = min(existing_summary.get('largest_loss', 0.0), current_pnl)
            
            # Calculate average trade PnL
            new_avg_trade_pnl = new_realized_pnl / new_trades_count if new_trades_count > 0 else 0.0
            
            # Execute update with guaranteed commit
            result = self.db_manager.execute_single_commit_operation(
                '''INSERT OR REPLACE INTO daily_pnl_summary 
                   (date, daily_realized_pnl, daily_unrealized_pnl, daily_total_pnl,
                    trades_count, winning_trades, losing_trades, total_fees,
                    largest_win, largest_loss, avg_trade_pnl, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (trade_date, new_realized_pnl, 0.0, new_realized_pnl,
                 new_trades_count, new_winning_trades, new_losing_trades, new_total_fees,
                 new_largest_win, new_largest_loss, new_avg_trade_pnl, datetime.now())
            )
            
            if not result['success']:
                logging.error(f"Failed to update daily summary for {trade_date}")
        
        except Exception as e:
            logging.error(f"Error updating daily summary: {e}")
    
    async def _get_daily_summary_async(self, date) -> Dict[str, Any]:
        """Get existing daily summary"""
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                return {}
            
            try:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT daily_realized_pnl, trades_count, winning_trades, losing_trades,
                           total_fees, largest_win, largest_loss
                    FROM daily_pnl_summary 
                    WHERE date = ?
                ''', (date,))
                
                row = cursor.fetchone()
                if row:
                    return {
                        'daily_realized_pnl': row[0],
                        'trades_count': row[1],
                        'winning_trades': row[2],
                        'losing_trades': row[3],
                        'total_fees': row[4],
                        'largest_win': row[5],
                        'largest_loss': row[6]
                    }
                
                return {}
                
            finally:
                self.db_manager.return_connection(conn)
        
        except Exception as e:
            logging.error(f"Error getting daily summary: {e}")
            return {}
    
    async def create_pnl_snapshot_async(self, current_prices: Dict[str, float] = None) -> Dict[str, Any]:
        """Create comprehensive PnL snapshot with proper commit"""
        start_time = time.time()
        
        try:
            snapshot_id = str(uuid.uuid4())
            timestamp = datetime.now()
            
            # Get all trades
            all_trades = await self._get_all_trades_async()
            
            # Calculate metrics
            portfolio_metrics = self.pnl_calculator.calculate_portfolio_metrics(all_trades)
            
            # Calculate unrealized PnL if current prices provided
            total_unrealized_pnl = 0.0
            open_positions_count = 0
            
            if current_prices:
                open_trades = [t for t in all_trades if t.status == 'open']
                for trade in open_trades:
                    if trade.symbol in current_prices:
                        unrealized_pnl = self.pnl_calculator.calculate_unrealized_pnl(
                            trade, current_prices[trade.symbol]
                        )
                        total_unrealized_pnl += unrealized_pnl
                        
                        # Update trade's unrealized PnL
                        await self._update_trade_unrealized_pnl_async(trade.trade_id, unrealized_pnl)
                
                open_positions_count = len(open_trades)
            
            # Create snapshot
            snapshot = PnLSnapshot(
                snapshot_id=snapshot_id,
                timestamp=timestamp,
                total_balance=10000.0 + portfolio_metrics.get('total_realized_pnl', 0.0),  # Base balance + realized PnL
                realized_pnl=portfolio_metrics.get('total_realized_pnl', 0.0),
                unrealized_pnl=total_unrealized_pnl,
                total_pnl=portfolio_metrics.get('total_realized_pnl', 0.0) + total_unrealized_pnl,
                open_positions_count=open_positions_count,
                closed_positions_count=portfolio_metrics.get('total_trades', 0),
                win_rate=portfolio_metrics.get('win_rate', 0.0),
                profit_factor=portfolio_metrics.get('profit_factor', 0.0),
                max_drawdown=portfolio_metrics.get('max_drawdown', 0.0),
                sharpe_ratio=portfolio_metrics.get('sharpe_ratio', 0.0)
            )
            
            # Store snapshot with guaranteed commit
            snapshot_data = json.dumps(portfolio_metrics, default=str)
            
            result = self.db_manager.execute_single_commit_operation(
                '''INSERT INTO pnl_snapshots 
                   (snapshot_id, timestamp, total_balance, realized_pnl, unrealized_pnl, total_pnl,
                    open_positions_count, closed_positions_count, win_rate, profit_factor,
                    max_drawdown, sharpe_ratio, snapshot_data)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (snapshot.snapshot_id, snapshot.timestamp, snapshot.total_balance,
                 snapshot.realized_pnl, snapshot.unrealized_pnl, snapshot.total_pnl,
                 snapshot.open_positions_count, snapshot.closed_positions_count,
                 snapshot.win_rate, snapshot.profit_factor, snapshot.max_drawdown,
                 snapshot.sharpe_ratio, snapshot_data)
            )
            
            if result['success']:
                self.persistence_stats['total_snapshots_created'] += 1
                self.persistence_stats['successful_operations'] += 1
                
                # Cache snapshot
                self._cache_snapshot(snapshot)
            else:
                self.persistence_stats['failed_operations'] += 1
            
            execution_time = time.time() - start_time
            self._update_operation_stats(execution_time)
            
            result['snapshot'] = snapshot
            result['execution_time'] = execution_time
            
            return result
            
        except Exception as e:
            execution_time = time.time() - start_time
            self._update_operation_stats(execution_time)
            self.persistence_stats['failed_operations'] += 1
            
            logging.error(f"Error creating PnL snapshot: {e}")
            return {
                'success': False,
                'error': str(e),
                'execution_time': execution_time
            }
    
    async def _update_trade_unrealized_pnl_async(self, trade_id: str, unrealized_pnl: float):
        """Update trade's unrealized PnL with commit"""
        try:
            result = self.db_manager.execute_single_commit_operation(
                'UPDATE trades SET pnl_unrealized = ?, updated_at = ? WHERE trade_id = ?',
                (unrealized_pnl, datetime.now(), trade_id)
            )
            
            if not result['success']:
                logging.error(f"Failed to update unrealized PnL for trade {trade_id}")
        
        except Exception as e:
            logging.error(f"Error updating unrealized PnL: {e}")
    
    async def _get_all_trades_async(self) -> List[TradeRecord]:
        """Get all trades from database"""
        try:
            loop = asyncio.get_event_loop()
            
            def get_trades():
                conn = self.db_manager.get_connection()
                if not conn:
                    return []
                
                try:
                    cursor = conn.cursor()
                    cursor.execute('''
                        SELECT trade_id, symbol, side, quantity, entry_price, exit_price,
                               entry_time, exit_time, pnl_realized, pnl_unrealized,
                               fees, strategy, status
                        FROM trades
                        ORDER BY entry_time
                    ''')
                    
                    rows = cursor.fetchall()
                    trades = []
                    
                    for row in rows:
                        trade = TradeRecord(
                            trade_id=row[0], symbol=row[1], side=row[2], quantity=row[3],
                            entry_price=row[4], exit_price=row[5],
                            entry_time=datetime.fromisoformat(row[6]) if isinstance(row[6], str) else row[6],
                            exit_time=datetime.fromisoformat(row[7]) if row[7] and isinstance(row[7], str) else row[7],
                            pnl_realized=row[8], pnl_unrealized=row[9],
                            fees=row[10], strategy=row[11], status=row[12]
                        )
                        trades.append(trade)
                    
                    return trades
                    
                finally:
                    self.db_manager.return_connection(conn)
            
            return await loop.run_in_executor(self.executor, get_trades)
            
        except Exception as e:
            logging.error(f"Error getting all trades: {e}")
            return []
    
    def _validate_trade_record(self, trade: TradeRecord) -> bool:
        """Validate trade record"""
        try:
            if not trade.trade_id or not trade.symbol:
                return False
            
            if trade.side not in ['buy', 'sell']:
                return False
            
            if trade.quantity <= 0 or trade.entry_price <= 0:
                return False
            
            if trade.status == 'closed' and (not trade.exit_price or trade.exit_price <= 0):
                return False
            
            if trade.fees < 0:
                return False
            
            return True
            
        except Exception as e:
            logging.error(f"Error validating trade record: {e}")
            return False
    
    def _cache_trade(self, trade: TradeRecord):
        """Cache trade in memory"""
        try:
            with self.cache_lock:
                # Limit cache size
                if len(self.trade_cache) > 1000:
                    # Remove oldest entries
                    oldest_key = min(self.trade_cache.keys(), 
                                   key=lambda k: self.trade_cache[k]['timestamp'])
                    del self.trade_cache[oldest_key]
                
                self.trade_cache[trade.trade_id] = {
                    'trade': trade,
                    'timestamp': time.time()
                }
        except Exception as e:
            logging.error(f"Error caching trade: {e}")
    
    def _cache_snapshot(self, snapshot: PnLSnapshot):
        """Cache snapshot in memory"""
        try:
            with self.cache_lock:
                # Keep only last 10 snapshots
                if len(self.pnl_cache) > 10:
                    oldest_key = min(self.pnl_cache.keys(), 
                                   key=lambda k: self.pnl_cache[k]['timestamp'])
                    del self.pnl_cache[oldest_key]
                
                self.pnl_cache[snapshot.snapshot_id] = {
                    'snapshot': snapshot,
                    'timestamp': time.time()
                }
        except Exception as e:
            logging.error(f"Error caching snapshot: {e}")
    
    def _update_operation_stats(self, execution_time: float):
        """Update operation performance statistics"""
        try:
            # Update rolling average
            if self.persistence_stats['avg_operation_time'] == 0:
                self.persistence_stats['avg_operation_time'] = execution_time
            else:
                self.persistence_stats['avg_operation_time'] = (
                    self.persistence_stats['avg_operation_time'] * 0.9 + execution_time * 0.1
                )
            
            self.persistence_stats['last_operation_time'] = datetime.now().isoformat()
        
        except Exception as e:
            logging.error(f"Error updating operation stats: {e}")
    
    def _start_background_tasks(self):
        """Start background maintenance tasks"""
        def background_worker():
            while True:
                try:
                    self._cleanup_cache()
                    self._log_performance_metrics()
                    self._cleanup_old_data()
                    time.sleep(300)  # Run every 5 minutes
                except Exception as e:
                    logging.error(f"PnL background task error: {e}")
                    time.sleep(120)
        
        thread = threading.Thread(target=background_worker, daemon=True)
        thread.start()
    
    def _cleanup_cache(self):
        """Clean up expired cache entries"""
        try:
            current_time = time.time()
            
            with self.cache_lock:
                # Clean trade cache
                expired_trade_keys = [
                    key for key, cache_entry in self.trade_cache.items()
                    if current_time - cache_entry['timestamp'] > 600  # 10 minutes
                ]
                
                for key in expired_trade_keys:
                    del self.trade_cache[key]
                
                # Clean snapshot cache
                expired_snapshot_keys = [
                    key for key, cache_entry in self.pnl_cache.items()
                    if current_time - cache_entry['timestamp'] > 1800  # 30 minutes
                ]
                
                for key in expired_snapshot_keys:
                    del self.pnl_cache[key]
        
        except Exception as e:
            logging.error(f"PnL cache cleanup error: {e}")
    
    def _cleanup_old_data(self):
        """Clean up old audit logs and optimize database"""
        try:
            # Remove old audit logs (older than 30 days)
            cutoff_date = datetime.now() - timedelta(days=30)
            
            result = self.db_manager.execute_single_commit_operation(
                'DELETE FROM transaction_audit_log WHERE created_at < ?',
                (cutoff_date,)
            )
            
            if result['success'] and result['records_affected'] > 0:
                logging.info(f"Cleaned up {result['records_affected']} old audit log entries")
        
        except Exception as e:
            logging.error(f"PnL data cleanup error: {e}")
    
    def _log_performance_metrics(self):
        """Log current performance metrics"""
        try:
            stats = self.persistence_stats.copy()
            db_stats = self.db_manager.get_transaction_metrics()
            
            success_rate = 0
            if stats['successful_operations'] + stats['failed_operations'] > 0:
                success_rate = stats['successful_operations'] / (
                    stats['successful_operations'] + stats['failed_operations']
                ) * 100
            
            logging.info(f"PnL Persistence Metrics - "
                        f"Trades Stored: {stats['total_trades_stored']}, "
                        f"Snapshots: {stats['total_snapshots_created']}, "
                        f"Success Rate: {success_rate:.1f}%, "
                        f"Avg Operation Time: {stats['avg_operation_time']:.3f}s, "
                        f"DB Success Rate: {db_stats['success_rate']:.1%}")
        
        except Exception as e:
            logging.error(f"Performance logging error: {e}")
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get comprehensive performance summary"""
        try:
            summary = {
                'persistence_stats': self.persistence_stats.copy(),
                'database_stats': self.db_manager.get_transaction_metrics(),
                'cache_stats': {
                    'trade_cache_size': len(self.trade_cache),
                    'snapshot_cache_size': len(self.pnl_cache)
                },
                'system_resources': {
                    'memory_percent': psutil.virtual_memory().percent,
                    'cpu_percent': psutil.cpu_percent(),
                    'thread_pool_workers': self.executor._max_workers
                },
                'last_updated': datetime.now().isoformat()
            }
            
            return summary
            
        except Exception as e:
            logging.error(f"Error getting PnL performance summary: {e}")
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
                if self.db_manager.max_connections < 25:
                    self.db_manager.max_connections = 25
            
            logging.info(f"PnL persistence manager optimized for {cpu_count} CPUs, {memory_gb:.1f}GB RAM")
            
        except Exception as e:
            logging.error(f"PnL server optimization error: {e}")

# Export main classes
__all__ = ['PnLPersistenceManager', 'DatabaseTransactionManager', 'TradeRecord', 'PnLSnapshot', 'PnLCalculator']

if __name__ == "__main__":
    # Performance test
    async def test_pnl_persistence():
        manager = PnLPersistenceManager()
        manager.optimize_for_server_specs()
        
        # Test trade storage
        test_trade = TradeRecord(
            trade_id=str(uuid.uuid4()),
            symbol='BTCUSDT',
            side='buy',
            quantity=0.1,
            entry_price=50000.0,
            exit_price=51000.0,
            entry_time=datetime.now() - timedelta(hours=1),
            exit_time=datetime.now(),
            pnl_realized=None,  # Will be calculated
            pnl_unrealized=None,
            fees=5.0,
            strategy='test_strategy',
            status='closed'
        )
        
        # Store trade with proper commit
        result = await manager.store_trade_async(test_trade, commit_immediately=True)
        print(f"Trade storage result: {result['success']}")
        
        # Create PnL snapshot
        snapshot_result = await manager.create_pnl_snapshot_async({'BTCUSDT': 51500.0})
        print(f"Snapshot creation result: {snapshot_result['success']}")
        
        # Get performance summary
        summary = manager.get_performance_summary()
        print(f"Performance Summary: {json.dumps(summary, indent=2, default=str)}")
    
    # Run test
    asyncio.run(test_pnl_persistence())