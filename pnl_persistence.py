#!/usr/bin/env python3
"""
PNL PERSISTENCE MANAGER - REAL DATA ONLY
=======================================
Handles persistence of real trading metrics and P&L data
NO MOCK DATA - All metrics are from real trading operations
"""

import os
import json
import sqlite3
import logging
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
from decimal import Decimal
import pandas as pd
import time
from contextlib import contextmanager

class PnLPersistence:
    """Manages persistence of real trading P&L and metrics"""
    
    def __init__(self, db_path: str = "data/trade_logs.db"):
        self.logger = logging.getLogger(__name__)
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Thread safety
        self._lock = threading.Lock()
        
        # Initialize database
        self._initialize_database()
        
        # Cache for frequent operations
        self._metrics_cache = {}
        self._cache_timestamp = 0
        self._cache_ttl = 30  # 30 seconds
        
        self.logger.info("PnL Persistence initialized - REAL DATA ONLY")
    
    def _initialize_database(self):
        """Initialize database schema for real trading data"""
        with sqlite3.connect(self.db_path) as conn:
            # Trades table - stores all real trades
            conn.execute('''
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_id TEXT UNIQUE,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    quantity REAL NOT NULL,
                    entry_price REAL NOT NULL,
                    exit_price REAL,
                    entry_time TEXT NOT NULL,
                    exit_time TEXT,
                    pnl REAL DEFAULT 0,
                    pnl_percent REAL DEFAULT 0,
                    fees REAL DEFAULT 0,
                    strategy TEXT,
                    confidence REAL,
                    status TEXT DEFAULT 'open',
                    notes TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Daily metrics table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS daily_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT UNIQUE NOT NULL,
                    total_trades INTEGER DEFAULT 0,
                    winning_trades INTEGER DEFAULT 0,
                    losing_trades INTEGER DEFAULT 0,
                    total_pnl REAL DEFAULT 0,
                    total_fees REAL DEFAULT 0,
                    win_rate REAL DEFAULT 0,
                    avg_win REAL DEFAULT 0,
                    avg_loss REAL DEFAULT 0,
                    best_trade REAL DEFAULT 0,
                    worst_trade REAL DEFAULT 0,
                    profit_factor REAL DEFAULT 0,
                    max_drawdown REAL DEFAULT 0,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Overall metrics table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS overall_metrics (
                    key TEXT PRIMARY KEY,
                    value REAL NOT NULL,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Performance snapshots
            conn.execute('''
                CREATE TABLE IF NOT EXISTS performance_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    total_pnl REAL DEFAULT 0,
                    daily_pnl REAL DEFAULT 0,
                    total_trades INTEGER DEFAULT 0,
                    win_rate REAL DEFAULT 0,
                    active_positions INTEGER DEFAULT 0,
                    account_balance REAL DEFAULT 0,
                    unrealized_pnl REAL DEFAULT 0,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create indexes for better performance
            conn.execute('CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_trades_entry_time ON trades(entry_time)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_daily_metrics_date ON daily_metrics(date)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_performance_timestamp ON performance_snapshots(timestamp)')
    
    @contextmanager
    def get_connection(self):
        """Get database connection with proper locking"""
        with self._lock:
            conn = sqlite3.connect(
                self.db_path,
                timeout=30.0,
                check_same_thread=False
            )
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=NORMAL')
            try:
                yield conn
                conn.commit()
            except Exception as e:
                conn.rollback()
                raise e
            finally:
                conn.close()
    
    def log_trade(self, trade_data: Dict[str, Any]) -> bool:
        """Log a real trade to the database"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Ensure required fields
                required_fields = ['trade_id', 'symbol', 'side', 'quantity', 'entry_price', 'entry_time']
                for field in required_fields:
                    if field not in trade_data:
                        raise ValueError(f"Missing required field: {field}")
                
                # Insert trade
                cursor.execute('''
                    INSERT OR REPLACE INTO trades 
                    (trade_id, symbol, side, quantity, entry_price, exit_price, entry_time, 
                     exit_time, pnl, pnl_percent, fees, strategy, confidence, status, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    trade_data['trade_id'],
                    trade_data['symbol'],
                    trade_data['side'],
                    float(trade_data['quantity']),
                    float(trade_data['entry_price']),
                    float(trade_data.get('exit_price', 0)) if trade_data.get('exit_price') else None,
                    trade_data['entry_time'],
                    trade_data.get('exit_time'),
                    float(trade_data.get('pnl', 0)),
                    float(trade_data.get('pnl_percent', 0)),
                    float(trade_data.get('fees', 0)),
                    trade_data.get('strategy'),
                    float(trade_data.get('confidence', 0)) if trade_data.get('confidence') else None,
                    trade_data.get('status', 'open'),
                    trade_data.get('notes')
                ))
                
                self.logger.info(f"Logged real trade: {trade_data['trade_id']} - {trade_data['symbol']}")
                
                # Update daily metrics if trade is closed
                if trade_data.get('status') == 'closed' and trade_data.get('exit_time'):
                    self._update_daily_metrics(trade_data)
                
                # Clear cache
                self._metrics_cache.clear()
                
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to log trade: {e}")
            return False
    
    def update_trade(self, trade_id: str, updates: Dict[str, Any]) -> bool:
        """Update an existing trade with new information"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Build update query
                set_clauses = []
                values = []
                
                for key, value in updates.items():
                    if key in ['quantity', 'entry_price', 'exit_price', 'pnl', 'pnl_percent', 'fees', 'confidence']:
                        set_clauses.append(f"{key} = ?")
                        values.append(float(value) if value is not None else None)
                    elif key in ['symbol', 'side', 'entry_time', 'exit_time', 'strategy', 'status', 'notes']:
                        set_clauses.append(f"{key} = ?")
                        values.append(value)
                
                set_clauses.append("updated_at = ?")
                values.append(datetime.now().isoformat())
                values.append(trade_id)
                
                query = f"UPDATE trades SET {', '.join(set_clauses)} WHERE trade_id = ?"
                cursor.execute(query, values)
                
                if cursor.rowcount > 0:
                    self.logger.info(f"Updated trade: {trade_id}")
                    
                    # Update daily metrics if trade was closed
                    if updates.get('status') == 'closed':
                        cursor.execute('SELECT * FROM trades WHERE trade_id = ?', (trade_id,))
                        trade_row = cursor.fetchone()
                        if trade_row:
                            trade_data = dict(zip([col[0] for col in cursor.description], trade_row))
                            self._update_daily_metrics(trade_data)
                    
                    # Clear cache
                    self._metrics_cache.clear()
                    return True
                else:
                    self.logger.warning(f"No trade found with ID: {trade_id}")
                    return False
                
        except Exception as e:
            self.logger.error(f"Failed to update trade {trade_id}: {e}")
            return False
    
    def _update_daily_metrics(self, trade_data: Dict[str, Any]):
        """Update daily metrics based on completed trade"""
        try:
            exit_time = trade_data.get('exit_time')
            if not exit_time:
                return
            
            trade_date = datetime.fromisoformat(exit_time).date().isoformat()
            pnl = float(trade_data.get('pnl', 0))
            fees = float(trade_data.get('fees', 0))
            
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Get current daily metrics
                cursor.execute('SELECT * FROM daily_metrics WHERE date = ?', (trade_date,))
                daily_row = cursor.fetchone()
                
                if daily_row:
                    # Update existing record
                    current_metrics = dict(zip([col[0] for col in cursor.description], daily_row))
                    total_trades = current_metrics['total_trades'] + 1
                    total_pnl = current_metrics['total_pnl'] + pnl
                    total_fees = current_metrics['total_fees'] + fees
                    
                    if pnl > 0:
                        winning_trades = current_metrics['winning_trades'] + 1
                        losing_trades = current_metrics['losing_trades']
                    else:
                        winning_trades = current_metrics['winning_trades']
                        losing_trades = current_metrics['losing_trades'] + 1
                    
                    win_rate = (winning_trades / total_trades) * 100 if total_trades > 0 else 0
                    best_trade = max(current_metrics['best_trade'], pnl)
                    worst_trade = min(current_metrics['worst_trade'], pnl)
                    
                    cursor.execute('''
                        UPDATE daily_metrics SET
                        total_trades = ?, winning_trades = ?, losing_trades = ?,
                        total_pnl = ?, total_fees = ?, win_rate = ?,
                        best_trade = ?, worst_trade = ?, updated_at = ?
                        WHERE date = ?
                    ''', (
                        total_trades, winning_trades, losing_trades,
                        total_pnl, total_fees, win_rate,
                        best_trade, worst_trade, datetime.now().isoformat(),
                        trade_date
                    ))
                else:
                    # Create new record
                    winning_trades = 1 if pnl > 0 else 0
                    losing_trades = 1 if pnl <= 0 else 0
                    win_rate = 100.0 if pnl > 0 else 0.0
                    
                    cursor.execute('''
                        INSERT INTO daily_metrics
                        (date, total_trades, winning_trades, losing_trades,
                         total_pnl, total_fees, win_rate, best_trade, worst_trade)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        trade_date, 1, winning_trades, losing_trades,
                        pnl, fees, win_rate, pnl, pnl
                    ))
                
        except Exception as e:
            self.logger.error(f"Failed to update daily metrics: {e}")
    
    def save_metrics(self, metrics: Dict[str, Any]):
        """Save overall trading metrics"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                for key, value in metrics.items():
                    if isinstance(value, (int, float)):
                        cursor.execute('''
                            INSERT OR REPLACE INTO overall_metrics (key, value, updated_at)
                            VALUES (?, ?, ?)
                        ''', (key, float(value), datetime.now().isoformat()))
                
                self.logger.debug("Saved overall metrics to database")
                
                # Update cache
                self._metrics_cache = metrics.copy()
                self._cache_timestamp = time.time()
                
        except Exception as e:
            self.logger.error(f"Failed to save metrics: {e}")
    
    def load_metrics(self) -> Dict[str, Any]:
        """Load overall trading metrics"""
        try:
            # Check cache first
            if (time.time() - self._cache_timestamp < self._cache_ttl and 
                self._metrics_cache):
                return self._metrics_cache.copy()
            
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT key, value FROM overall_metrics')
                
                metrics = {}
                for row in cursor.fetchall():
                    key, value = row
                    metrics[key] = value
                
                # Update cache
                self._metrics_cache = metrics.copy()
                self._cache_timestamp = time.time()
                
                return metrics
                
        except Exception as e:
            self.logger.error(f"Failed to load metrics: {e}")
            return {}
    
    def save_performance_snapshot(self, snapshot: Dict[str, Any]):
        """Save a performance snapshot"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT INTO performance_snapshots
                    (timestamp, total_pnl, daily_pnl, total_trades, win_rate,
                     active_positions, account_balance, unrealized_pnl)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    datetime.now().isoformat(),
                    float(snapshot.get('total_pnl', 0)),
                    float(snapshot.get('daily_pnl', 0)),
                    int(snapshot.get('total_trades', 0)),
                    float(snapshot.get('win_rate', 0)),
                    int(snapshot.get('active_positions', 0)),
                    float(snapshot.get('account_balance', 0)),
                    float(snapshot.get('unrealized_pnl', 0))
                ))
                
                self.logger.debug("Saved performance snapshot")
                
        except Exception as e:
            self.logger.error(f"Failed to save performance snapshot: {e}")
    
    def get_trade_history(self, symbol: str = None, days: int = 30, status: str = None) -> List[Dict]:
        """Get trade history with optional filters"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                query = 'SELECT * FROM trades WHERE 1=1'
                params = []
                
                if symbol:
                    query += ' AND symbol = ?'
                    params.append(symbol)
                
                if status:
                    query += ' AND status = ?'
                    params.append(status)
                
                if days > 0:
                    cutoff_date = (datetime.now() - timedelta(days=days)).isoformat()
                    query += ' AND entry_time >= ?'
                    params.append(cutoff_date)
                
                query += ' ORDER BY entry_time DESC'
                
                cursor.execute(query, params)
                columns = [col[0] for col in cursor.description]
                
                trades = []
                for row in cursor.fetchall():
                    trade = dict(zip(columns, row))
                    trades.append(trade)
                
                return trades
                
        except Exception as e:
            self.logger.error(f"Failed to get trade history: {e}")
            return []
    
    def get_daily_summary(self, days: int = 30) -> List[Dict]:
        """Get daily trading summary"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cutoff_date = (datetime.now() - timedelta(days=days)).date().isoformat()
                
                cursor.execute('''
                    SELECT * FROM daily_metrics 
                    WHERE date >= ? 
                    ORDER BY date DESC
                ''', (cutoff_date,))
                
                columns = [col[0] for col in cursor.description]
                
                summary = []
                for row in cursor.fetchall():
                    day_data = dict(zip(columns, row))
                    summary.append(day_data)
                
                return summary
                
        except Exception as e:
            self.logger.error(f"Failed to get daily summary: {e}")
            return []
    
    def get_performance_chart_data(self, days: int = 30) -> Dict[str, List]:
        """Get performance data for charting"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cutoff_date = (datetime.now() - timedelta(days=days)).isoformat()
                
                cursor.execute('''
                    SELECT timestamp, total_pnl, daily_pnl, total_trades, win_rate
                    FROM performance_snapshots 
                    WHERE timestamp >= ? 
                    ORDER BY timestamp ASC
                ''', (cutoff_date,))
                
                data = {
                    'timestamps': [],
                    'total_pnl': [],
                    'daily_pnl': [],
                    'total_trades': [],
                    'win_rate': []
                }
                
                for row in cursor.fetchall():
                    data['timestamps'].append(row[0])
                    data['total_pnl'].append(row[1])
                    data['daily_pnl'].append(row[2])
                    data['total_trades'].append(row[3])
                    data['win_rate'].append(row[4])
                
                return data
                
        except Exception as e:
            self.logger.error(f"Failed to get performance chart data: {e}")
            return {
                'timestamps': [],
                'total_pnl': [],
                'daily_pnl': [],
                'total_trades': [],
                'win_rate': []
            }
    
    def cleanup_old_data(self, days_to_keep: int = 90):
        """Clean up old performance snapshots to save space"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cutoff_date = (datetime.now() - timedelta(days=days_to_keep)).isoformat()
                
                cursor.execute('''
                    DELETE FROM performance_snapshots 
                    WHERE timestamp < ?
                ''', (cutoff_date,))
                
                deleted_count = cursor.rowcount
                self.logger.info(f"Cleaned up {deleted_count} old performance snapshots")
                
                return deleted_count
                
        except Exception as e:
            self.logger.error(f"Failed to cleanup old data: {e}")
            return 0
    
    def export_data(self, export_path: str = None) -> str:
        """Export all trading data to JSON"""
        try:
            if not export_path:
                export_path = f"data/trading_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            export_data = {
                'export_timestamp': datetime.now().isoformat(),
                'trades': self.get_trade_history(days=0),  # All trades
                'daily_metrics': self.get_daily_summary(days=0),  # All days
                'overall_metrics': self.load_metrics(),
                'performance_data': self.get_performance_chart_data(days=0)  # All data
            }
            
            with open(export_path, 'w') as f:
                json.dump(export_data, f, indent=2, default=str)
            
            self.logger.info(f"Exported trading data to: {export_path}")
            return export_path
            
        except Exception as e:
            self.logger.error(f"Failed to export data: {e}")
            return ""


if __name__ == "__main__":
    # Test the PnL persistence system
    pnl = PnLPersistence()
    
    print("PnL Persistence System - REAL DATA ONLY")
    print("=" * 50)
    
    # Test logging a trade
    test_trade = {
        'trade_id': 'TEST_001',
        'symbol': 'BTCUSDT',
        'side': 'BUY',
        'quantity': 0.001,
        'entry_price': 50000.0,
        'entry_time': datetime.now().isoformat(),
        'strategy': 'TEST_STRATEGY',
        'confidence': 75.0,
        'status': 'open'
    }
    
    success = pnl.log_trade(test_trade)
    print(f"Test trade logged: {success}")
    
    # Load metrics
    metrics = pnl.load_metrics()
    print(f"Loaded metrics: {len(metrics)} items")
    
    # Get trade history
    trades = pnl.get_trade_history(days=1)
    print(f"Recent trades: {len(trades)}")
    
    print("PnL Persistence system test completed")