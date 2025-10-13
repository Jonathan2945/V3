#!/usr/bin/env python3
"""
PNL PERSISTENCE - COMPLETE FIXED VERSION
========================================
Handles trade persistence and metrics
"""
import sqlite3
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
import json

class PnLPersistence:
    """Persistence manager for P&L and trading metrics"""
    
    def __init__(self, db_path: str = "data/trade_logs.db"):
        self.logger = logging.getLogger(f"{__name__}.PnLPersistence")
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        self._init_database()
        self.logger.info(f"PnL Persistence initialized: {self.db_path}")
    
    def get_connection(self):
        """Get database connection"""
        conn = sqlite3.connect(
            self.db_path,
            timeout=30.0,
            check_same_thread=False
        )
        conn.row_factory = sqlite3.Row
        return conn
    
    def _init_database(self):
        """Initialize database schema"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Trades table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS trades (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL,
                        side TEXT NOT NULL,
                        quantity REAL NOT NULL,
                        entry_price REAL NOT NULL,
                        entry_time TEXT NOT NULL,
                        exit_price REAL,
                        exit_time TEXT,
                        pnl REAL DEFAULT 0,
                        pnl_percent REAL DEFAULT 0,
                        status TEXT DEFAULT 'open',
                        strategy TEXT,
                        confidence REAL,
                        notes TEXT,
                        data_source TEXT DEFAULT 'real',
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Performance snapshots table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS performance_snapshots (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT NOT NULL,
                        total_trades INTEGER DEFAULT 0,
                        winning_trades INTEGER DEFAULT 0,
                        losing_trades INTEGER DEFAULT 0,
                        win_rate REAL DEFAULT 0,
                        total_pnl REAL DEFAULT 0,
                        daily_pnl REAL DEFAULT 0,
                        best_trade REAL DEFAULT 0,
                        worst_trade REAL DEFAULT 0,
                        avg_win REAL DEFAULT 0,
                        avg_loss REAL DEFAULT 0,
                        metrics_json TEXT,
                        data_source TEXT DEFAULT 'real',
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Positions table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS positions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL UNIQUE,
                        side TEXT NOT NULL,
                        quantity REAL NOT NULL,
                        entry_price REAL NOT NULL,
                        current_price REAL,
                        unrealized_pnl REAL DEFAULT 0,
                        status TEXT DEFAULT 'open',
                        strategy TEXT,
                        confidence REAL,
                        opened_at TEXT NOT NULL,
                        data_source TEXT DEFAULT 'real',
                        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Create indexes
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_trades_entry_time ON trades(entry_time)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_positions_symbol ON positions(symbol)')
                
                conn.commit()
                self.logger.info("Database schema initialized")
                
        except Exception as e:
            self.logger.error(f"Database initialization error: {e}")
            raise
    
    def load_active_positions(self) -> Dict[str, Dict]:
        """Load active positions from database"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT * FROM positions WHERE status = 'open'
                ''')
                
                positions = {}
                for row in cursor.fetchall():
                    positions[row['symbol']] = dict(row)
                
                return positions
                
        except Exception as e:
            self.logger.error(f"Error loading positions: {e}")
            return {}
    
    def log_trade_entry(self, symbol: str, side: str, price: float, quantity: float,
                       strategy: str = None, confidence: float = None, notes: str = None):
        """Log trade entry"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                entry_time = datetime.now().isoformat()
                
                cursor.execute('''
                    INSERT INTO trades 
                    (symbol, side, quantity, entry_price, entry_time, strategy, confidence, notes, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'open')
                ''', (symbol, side, quantity, price, entry_time, strategy, confidence, notes))
                
                # Also create position entry
                cursor.execute('''
                    INSERT OR REPLACE INTO positions
                    (symbol, side, quantity, entry_price, current_price, status, strategy, confidence, opened_at)
                    VALUES (?, ?, ?, ?, ?, 'open', ?, ?, ?)
                ''', (symbol, side, quantity, price, price, strategy, confidence, entry_time))
                
                conn.commit()
                trade_id = cursor.lastrowid
                
                self.logger.info(f"Trade entry logged: {side} {quantity} {symbol} @ {price}")
                return trade_id
                
        except Exception as e:
            self.logger.error(f"Error logging trade entry: {e}")
            return None
    
    def log_trade_exit(self, symbol: str, exit_price: float, pnl: float):
        """Log trade exit"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                exit_time = datetime.now().isoformat()
                
                # Update most recent open trade for this symbol
                cursor.execute('''
                    UPDATE trades 
                    SET exit_price = ?, exit_time = ?, pnl = ?, status = 'closed'
                    WHERE symbol = ? AND status = 'open'
                    ORDER BY entry_time DESC
                    LIMIT 1
                ''', (exit_price, exit_time, pnl, symbol))
                
                # Remove from positions
                cursor.execute('DELETE FROM positions WHERE symbol = ?', (symbol,))
                
                conn.commit()
                
                self.logger.info(f"Trade exit logged: {symbol} @ {exit_price}, P&L: {pnl}")
                
        except Exception as e:
            self.logger.error(f"Error logging trade exit: {e}")
    
    def update_unrealized_pnl(self, symbol: str, current_price: float):
        """Update unrealized P&L for open position"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT entry_price, quantity, side FROM positions
                    WHERE symbol = ? AND status = 'open'
                ''', (symbol,))
                
                position = cursor.fetchone()
                if not position:
                    return
                
                entry_price = position['entry_price']
                quantity = position['quantity']
                side = position['side']
                
                if side == 'BUY':
                    unrealized_pnl = (current_price - entry_price) * quantity
                else:
                    unrealized_pnl = (entry_price - current_price) * quantity
                
                cursor.execute('''
                    UPDATE positions
                    SET current_price = ?, unrealized_pnl = ?, updated_at = ?
                    WHERE symbol = ? AND status = 'open'
                ''', (current_price, unrealized_pnl, datetime.now().isoformat(), symbol))
                
                conn.commit()
                
        except Exception as e:
            self.logger.error(f"Error updating unrealized P&L: {e}")
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get performance summary"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Overall stats
                cursor.execute('''
                    SELECT 
                        COUNT(*) as total_trades,
                        SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as winning_trades,
                        SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END) as losing_trades,
                        SUM(pnl) as total_pnl,
                        AVG(CASE WHEN pnl > 0 THEN pnl END) as avg_win,
                        AVG(CASE WHEN pnl < 0 THEN pnl END) as avg_loss,
                        MAX(pnl) as best_trade,
                        MIN(pnl) as worst_trade
                    FROM trades WHERE status = 'closed'
                ''')
                
                stats = dict(cursor.fetchone())
                
                # Calculate win rate
                total = stats.get('total_trades', 0)
                wins = stats.get('winning_trades', 0)
                stats['win_rate'] = (wins / total * 100) if total > 0 else 0
                
                # Daily P&L
                cursor.execute('''
                    SELECT SUM(pnl) as daily_pnl
                    FROM trades 
                    WHERE status = 'closed' 
                    AND date(entry_time) = date('now')
                ''')
                
                daily_row = cursor.fetchone()
                stats['daily_pnl'] = daily_row['daily_pnl'] if daily_row['daily_pnl'] else 0
                
                return stats
                
        except Exception as e:
            self.logger.error(f"Error getting performance summary: {e}")
            return {}
    
    def save_performance_snapshot(self):
        """Save current performance snapshot"""
        try:
            summary = self.get_performance_summary()
            
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT INTO performance_snapshots
                    (timestamp, total_trades, winning_trades, losing_trades, win_rate,
                     total_pnl, daily_pnl, best_trade, worst_trade, avg_win, avg_loss,
                     metrics_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    datetime.now().isoformat(),
                    summary.get('total_trades', 0),
                    summary.get('winning_trades', 0),
                    summary.get('losing_trades', 0),
                    summary.get('win_rate', 0),
                    summary.get('total_pnl', 0),
                    summary.get('daily_pnl', 0),
                    summary.get('best_trade', 0),
                    summary.get('worst_trade', 0),
                    summary.get('avg_win', 0),
                    summary.get('avg_loss', 0),
                    json.dumps(summary)
                ))
                
                conn.commit()
                
        except Exception as e:
            self.logger.error(f"Error saving performance snapshot: {e}")
    
    def load_metrics(self) -> Dict[str, Any]:
        """Load metrics from latest snapshot"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT * FROM performance_snapshots
                    ORDER BY timestamp DESC LIMIT 1
                ''')
                
                row = cursor.fetchone()
                if row:
                    return dict(row)
                else:
                    return {}
                    
        except Exception as e:
            self.logger.error(f"Error loading metrics: {e}")
            return {}
    
    def get_recent_trades(self, limit: int = 10) -> List[Dict]:
        """Get recent trades"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT * FROM trades
                    WHERE status = 'closed'
                    ORDER BY exit_time DESC
                    LIMIT ?
                ''', (limit,))
                
                trades = [dict(row) for row in cursor.fetchall()]
                return trades
                
        except Exception as e:
            self.logger.error(f"Error getting recent trades: {e}")
            return []
    
    def get_daily_pnl(self, days: int = 30) -> List[Dict]:
        """Get daily P&L for last N days"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT 
                        date(entry_time) as date,
                        COUNT(*) as trades,
                        SUM(pnl) as total_pnl,
                        SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                        SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END) as losses
                    FROM trades
                    WHERE status = 'closed'
                    AND entry_time >= date('now', '-' || ? || ' days')
                    GROUP BY date(entry_time)
                    ORDER BY date DESC
                ''', (days,))
                
                daily_stats = [dict(row) for row in cursor.fetchall()]
                return daily_stats
                
        except Exception as e:
            self.logger.error(f"Error getting daily P&L: {e}")
            return []
    
    def clear_all_data(self):
        """Clear all data (use with caution)"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('DELETE FROM trades')
                cursor.execute('DELETE FROM positions')
                cursor.execute('DELETE FROM performance_snapshots')
                conn.commit()
                
                self.logger.warning("All persistence data cleared")
                
        except Exception as e:
            self.logger.error(f"Error clearing data: {e}")
    
    def backup_data(self, backup_path: str = None):
        """Backup database"""
        try:
            if backup_path is None:
                backup_dir = Path("backups")
                backup_dir.mkdir(exist_ok=True)
                backup_path = backup_dir / f"trade_logs_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
            
            import shutil
            shutil.copy2(self.db_path, backup_path)
            
            self.logger.info(f"Database backed up to: {backup_path}")
            return str(backup_path)
            
        except Exception as e:
            self.logger.error(f"Error backing up database: {e}")
            return None