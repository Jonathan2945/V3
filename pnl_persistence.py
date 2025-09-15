#!/usr/bin/env python3
"""
V3 Enhanced PnL Persistence System - REAL DATA ONLY
Fixed database schema and all missing methods
"""

import sqlite3
import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import threading
from contextlib import contextmanager

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PnLPersistence:
    """Enhanced PnL persistence with real trading data tracking"""
    
    def __init__(self, db_path: str = "data/trade_logs.db"):
        self.db_path = db_path
        self.data_source = "REAL_TRADING"
        self._lock = threading.Lock()
        
        # Ensure data directory exists
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        # Initialize database
        self._init_database()
        
        # Load existing data
        self.active_positions = self._load_active_positions()
        
        logger.info("PnL Persistence initialized - REAL DATA ONLY")
        logger.info(f"Loaded {len(self.active_positions)} active positions")
        
        # Load metrics
        metrics = self.get_performance_summary()
        total_trades = metrics.get('total_trades', 0)
        total_pnl = metrics.get('total_pnl', 0.0)
        logger.info(f"Metrics loaded: {total_trades} trades, ${total_pnl:.2f} P&L")
    
    @contextmanager
    def get_connection(self):
        """Get database connection with proper error handling"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def _init_database(self):
        """Initialize database with complete schema"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Create trades table with all required columns
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS trades (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL,
                        side TEXT NOT NULL,
                        entry_price REAL NOT NULL,
                        exit_price REAL,
                        quantity REAL NOT NULL,
                        entry_time TEXT NOT NULL,
                        exit_time TEXT,
                        pnl REAL DEFAULT 0.0,
                        confidence REAL DEFAULT 0.0,
                        strategy TEXT,
                        timeframe TEXT,
                        data_source TEXT DEFAULT 'REAL_TRADING',
                        trade_type TEXT DEFAULT 'MANUAL',
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Create active_positions table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS active_positions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL,
                        side TEXT NOT NULL,
                        entry_price REAL NOT NULL,
                        quantity REAL NOT NULL,
                        entry_time TEXT NOT NULL,
                        confidence REAL DEFAULT 0.0,
                        strategy TEXT,
                        timeframe TEXT,
                        unrealized_pnl REAL DEFAULT 0.0,
                        data_source TEXT DEFAULT 'REAL_TRADING',
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Create performance_snapshots table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS performance_snapshots (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        snapshot_time TEXT NOT NULL,
                        total_pnl REAL DEFAULT 0.0,
                        total_trades INTEGER DEFAULT 0,
                        winning_trades INTEGER DEFAULT 0,
                        win_rate REAL DEFAULT 0.0,
                        active_positions INTEGER DEFAULT 0,
                        daily_pnl REAL DEFAULT 0.0,
                        data_source TEXT DEFAULT 'REAL_TRADING',
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Create indexes for performance
                indexes = [
                    'CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol)',
                    'CREATE INDEX IF NOT EXISTS idx_trades_entry_time ON trades(entry_time)',
                    'CREATE INDEX IF NOT EXISTS idx_trades_pnl ON trades(pnl)',
                    'CREATE INDEX IF NOT EXISTS idx_trades_data_source ON trades(data_source)',
                    'CREATE INDEX IF NOT EXISTS idx_trades_trade_type ON trades(trade_type)',
                    'CREATE INDEX IF NOT EXISTS idx_active_positions_symbol ON active_positions(symbol)',
                    'CREATE INDEX IF NOT EXISTS idx_performance_snapshots_time ON performance_snapshots(snapshot_time)'
                ]
                
                for index_sql in indexes:
                    try:
                        cursor.execute(index_sql)
                    except sqlite3.OperationalError as e:
                        if "no such column" in str(e):
                            logger.warning(f"Skipping index creation - column missing: {e}")
                        else:
                            raise
                
                conn.commit()
                logger.info("Database initialized successfully")
                
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            raise
    
    def _load_active_positions(self) -> Dict[str, Dict]:
        """Load active positions from database"""
        positions = {}
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT * FROM active_positions WHERE data_source = ?', (self.data_source,))
                rows = cursor.fetchall()
                
                for row in rows:
                    positions[row['symbol']] = {
                        'id': row['id'],
                        'symbol': row['symbol'],
                        'side': row['side'],
                        'entry_price': row['entry_price'],
                        'quantity': row['quantity'],
                        'entry_time': row['entry_time'],
                        'confidence': row['confidence'],
                        'strategy': row['strategy'],
                        'timeframe': row['timeframe'],
                        'unrealized_pnl': row['unrealized_pnl'],
                        'data_source': row['data_source']
                    }
        except Exception as e:
            logger.error(f"Error loading active positions: {e}")
            
        return positions
    
    def log_trade_entry(self, symbol: str, side: str, price: float, quantity: float, 
                       confidence: float = 0.0, strategy: str = None, timeframe: str = None):
        """Log trade entry to database and active positions"""
        with self._lock:
            try:
                entry_time = datetime.now().isoformat()
                
                # Add to active positions
                position = {
                    'symbol': symbol,
                    'side': side,
                    'entry_price': price,
                    'quantity': quantity,
                    'entry_time': entry_time,
                    'confidence': confidence,
                    'strategy': strategy,
                    'timeframe': timeframe,
                    'unrealized_pnl': 0.0,
                    'data_source': self.data_source
                }
                
                # Save to database
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute('''
                        INSERT INTO active_positions 
                        (symbol, side, entry_price, quantity, entry_time, confidence, strategy, timeframe, unrealized_pnl, data_source)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (symbol, side, price, quantity, entry_time, confidence, strategy, timeframe, 0.0, self.data_source))
                    
                    position['id'] = cursor.lastrowid
                    conn.commit()
                
                self.active_positions[symbol] = position
                
                logger.info(f"Trade entry logged: {side} {quantity} {symbol} @ {price}")
                
            except Exception as e:
                logger.error(f"Error logging trade entry: {e}")
                raise
    
    def log_trade_exit(self, symbol: str, exit_price: float, pnl: float):
        """Log trade exit and calculate final P&L"""
        with self._lock:
            try:
                if symbol not in self.active_positions:
                    logger.warning(f"No active position found for {symbol}")
                    return
                
                position = self.active_positions[symbol]
                exit_time = datetime.now().isoformat()
                
                # Log completed trade
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute('''
                        INSERT INTO trades 
                        (symbol, side, entry_price, exit_price, quantity, entry_time, exit_time, pnl, confidence, strategy, timeframe, data_source, trade_type)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        symbol, position['side'], position['entry_price'], exit_price,
                        position['quantity'], position['entry_time'], exit_time, pnl,
                        position['confidence'], position['strategy'], position['timeframe'],
                        self.data_source, 'AUTOMATED'
                    ))
                    
                    # Remove from active positions
                    cursor.execute('DELETE FROM active_positions WHERE id = ?', (position['id'],))
                    conn.commit()
                
                # Remove from memory
                del self.active_positions[symbol]
                
                logger.info(f"Trade exit logged: {symbol} P&L: ${pnl:.2f}")
                
            except Exception as e:
                logger.error(f"Error logging trade exit: {e}")
                raise
    
    def update_unrealized_pnl(self, symbol: str, current_price: float):
        """Update unrealized P&L for active position"""
        with self._lock:
            try:
                if symbol not in self.active_positions:
                    return
                
                position = self.active_positions[symbol]
                entry_price = position['entry_price']
                quantity = position['quantity']
                side = position['side']
                
                # Calculate unrealized P&L
                if side.upper() == 'BUY':
                    unrealized_pnl = (current_price - entry_price) * quantity
                else:  # SELL
                    unrealized_pnl = (entry_price - current_price) * quantity
                
                position['unrealized_pnl'] = unrealized_pnl
                
                # Update in database
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        'UPDATE active_positions SET unrealized_pnl = ? WHERE id = ?',
                        (unrealized_pnl, position['id'])
                    )
                    conn.commit()
                
            except Exception as e:
                logger.error(f"Error updating unrealized P&L: {e}")
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get comprehensive performance summary"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Get trade statistics
                cursor.execute('''
                    SELECT 
                        COUNT(*) as total_trades,
                        SUM(pnl) as total_pnl,
                        SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as winning_trades,
                        AVG(pnl) as avg_pnl,
                        MAX(pnl) as best_trade,
                        MIN(pnl) as worst_trade
                    FROM trades 
                    WHERE data_source = ?
                ''', (self.data_source,))
                
                trade_stats = cursor.fetchone()
                
                # Get active positions summary
                cursor.execute('''
                    SELECT 
                        COUNT(*) as active_count,
                        SUM(unrealized_pnl) as total_unrealized
                    FROM active_positions 
                    WHERE data_source = ?
                ''', (self.data_source,))
                
                position_stats = cursor.fetchone()
                
                # Calculate metrics
                total_trades = trade_stats['total_trades'] or 0
                total_pnl = trade_stats['total_pnl'] or 0.0
                winning_trades = trade_stats['winning_trades'] or 0
                win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0.0
                
                return {
                    'total_trades': total_trades,
                    'total_pnl': total_pnl,
                    'winning_trades': winning_trades,
                    'win_rate': win_rate,
                    'avg_pnl': trade_stats['avg_pnl'] or 0.0,
                    'best_trade': trade_stats['best_trade'] or 0.0,
                    'worst_trade': trade_stats['worst_trade'] or 0.0,
                    'active_positions': position_stats['active_count'] or 0,
                    'unrealized_pnl': position_stats['total_unrealized'] or 0.0,
                    'data_source': self.data_source
                }
                
        except Exception as e:
            logger.error(f"Error getting performance summary: {e}")
            return {
                'total_trades': 0,
                'total_pnl': 0.0,
                'winning_trades': 0,
                'win_rate': 0.0,
                'avg_pnl': 0.0,
                'best_trade': 0.0,
                'worst_trade': 0.0,
                'active_positions': 0,
                'unrealized_pnl': 0.0,
                'data_source': self.data_source
            }
    
    def save_performance_snapshot(self):
        """Save current performance snapshot"""
        try:
            summary = self.get_performance_summary()
            snapshot_time = datetime.now().isoformat()
            
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO performance_snapshots 
                    (snapshot_time, total_pnl, total_trades, winning_trades, win_rate, active_positions, daily_pnl, data_source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    snapshot_time, summary['total_pnl'], summary['total_trades'],
                    summary['winning_trades'], summary['win_rate'], summary['active_positions'],
                    0.0, self.data_source  # daily_pnl calculation can be added later
                ))
                conn.commit()
                
        except Exception as e:
            logger.error(f"Error saving performance snapshot: {e}")
    
    def load_metrics(self) -> Dict[str, Any]:
        """Load metrics - compatibility method"""
        return self.get_performance_summary()
    
    def get_recent_trades(self, limit: int = 10) -> List[Dict]:
        """Get recent trades"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT * FROM trades 
                    WHERE data_source = ? 
                    ORDER BY exit_time DESC 
                    LIMIT ?
                ''', (self.data_source, limit))
                
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Error getting recent trades: {e}")
            return []
    
    def get_daily_pnl(self, days: int = 30) -> List[Dict]:
        """Get daily P&L for chart data"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT 
                        DATE(exit_time) as trade_date,
                        SUM(pnl) as daily_pnl,
                        COUNT(*) as trade_count
                    FROM trades 
                    WHERE data_source = ? 
                        AND exit_time >= date('now', '-{} days')
                    GROUP BY DATE(exit_time)
                    ORDER BY trade_date DESC
                '''.format(days), (self.data_source,))
                
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Error getting daily P&L: {e}")
            return []
    
    def clear_all_data(self):
        """Clear all trading data - use with caution"""
        with self._lock:
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute('DELETE FROM trades WHERE data_source = ?', (self.data_source,))
                    cursor.execute('DELETE FROM active_positions WHERE data_source = ?', (self.data_source,))
                    cursor.execute('DELETE FROM performance_snapshots WHERE data_source = ?', (self.data_source,))
                    conn.commit()
                
                self.active_positions.clear()
                logger.info("All trading data cleared")
                
            except Exception as e:
                logger.error(f"Error clearing data: {e}")
                raise
    
    def backup_data(self, backup_path: str = None):
        """Backup trading data to file"""
        if not backup_path:
            backup_path = f"data/backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        try:
            summary = self.get_performance_summary()
            recent_trades = self.get_recent_trades(100)
            
            backup_data = {
                'backup_time': datetime.now().isoformat(),
                'summary': summary,
                'recent_trades': recent_trades,
                'active_positions': list(self.active_positions.values()),
                'data_source': self.data_source
            }
            
            os.makedirs(os.path.dirname(backup_path), exist_ok=True)
            with open(backup_path, 'w') as f:
                json.dump(backup_data, f, indent=2)
            
            logger.info(f"Data backed up to {backup_path}")
            return backup_path
            
        except Exception as e:
            logger.error(f"Error backing up data: {e}")
            raise


if __name__ == "__main__":
    # Test the PnL persistence system
    pnl = PnLPersistence()
    
    print("PnL Persistence Test:")
    summary = pnl.get_performance_summary()
    print(f"Total trades: {summary['total_trades']}")
    print(f"Total P&L: ${summary['total_pnl']:.2f}")
    print(f"Win rate: {summary['win_rate']:.1f}%")
    print(f"Active positions: {summary['active_positions']}")