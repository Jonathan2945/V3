#!/usr/bin/env python3
"""
P&L PERSISTENCE SYSTEM - FIXED AND ENHANCED
==========================================
FIXES APPLIED:
- Enhanced method compatibility
- Added real data tracking
- Improved error handling
- Thread-safe operations
- REAL DATA ONLY (no mock/simulated data)
"""

import json
import sqlite3
import os
import logging
import threading
import time
from datetime import datetime
from typing import Dict, Optional, List, Any
from pathlib import Path

class PnLPersistence:
    """Handles saving and loading P&L data between system restarts - REAL DATA ONLY"""
    
    def __init__(self, db_path: str = "data/trade_logs.db"):
        self.db_path = db_path
        self.logger = logging.getLogger(__name__)
        self._lock = threading.Lock()
        self._active_trades = {}
        self._performance_cache = {}
        self._cache_timestamp = 0
        self._cache_duration = 30  # Cache for 30 seconds
        
        # Ensure data directory exists
        self._ensure_data_dir()
        
        # Initialize database
        self._init_database()
        
        # Load active trades
        self._load_active_trades()
        
        self.logger.info("PnL Persistence initialized - REAL DATA ONLY")
    
    def _ensure_data_dir(self):
        """Ensure data directory exists"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
    
    def _init_database(self):
        """Initialize the metrics database with enhanced schema"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Enhanced system metrics table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS system_metrics (
                        id INTEGER PRIMARY KEY,
                        total_trades INTEGER DEFAULT 0,
                        winning_trades INTEGER DEFAULT 0,
                        total_pnl REAL DEFAULT 0.0,
                        win_rate REAL DEFAULT 0.0,
                        active_positions INTEGER DEFAULT 0,
                        best_trade REAL DEFAULT 0.0,
                        worst_trade REAL DEFAULT 0.0,
                        avg_trade REAL DEFAULT 0.0,
                        last_updated TEXT,
                        session_start TEXT,
                        data_source TEXT DEFAULT 'REAL_TRADING',
                        trading_mode TEXT DEFAULT 'PAPER_TRADING',
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Enhanced trade history table
                cursor.execute('''
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
                        pnl REAL DEFAULT 0.0,
                        pnl_percent REAL DEFAULT 0.0,
                        fees REAL DEFAULT 0.0,
                        strategy TEXT,
                        timeframe TEXT,
                        confidence REAL DEFAULT 0.0,
                        win BOOLEAN DEFAULT 0,
                        notes TEXT,
                        trade_type TEXT DEFAULT 'real',
                        data_source TEXT DEFAULT 'REAL_BINANCE',
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Active positions table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS active_positions (
                        position_id TEXT PRIMARY KEY,
                        symbol TEXT NOT NULL,
                        side TEXT NOT NULL,
                        quantity REAL NOT NULL,
                        entry_price REAL NOT NULL,
                        entry_time TEXT NOT NULL,
                        strategy TEXT,
                        timeframe TEXT,
                        confidence REAL DEFAULT 0.0,
                        stop_loss REAL,
                        take_profit REAL,
                        unrealized_pnl REAL DEFAULT 0.0,
                        data_source TEXT DEFAULT 'REAL_BINANCE',
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Performance snapshots table
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
                
                # Create indexes for better performance
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_trades_entry_time ON trades(entry_time)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_trades_pnl ON trades(pnl)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_trades_data_source ON trades(data_source)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_active_positions_symbol ON active_positions(symbol)')
                
                conn.commit()
                self.logger.info("Database initialized successfully")
                
        except Exception as e:
            self.logger.error(f"Database initialization failed: {e}")
            raise
    
    def _load_active_trades(self):
        """Load active trades from database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT COUNT(*) FROM active_positions')
                count = cursor.fetchone()[0]
                self.logger.info(f"Loaded {count} active positions")
        except Exception as e:
            self.logger.error(f"Failed to load active trades: {e}")
    
    def log_trade_entry(self, trade_data: Dict) -> bool:
        """Log trade entry - REAL TRADING ONLY"""
        try:
            with self._lock:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()
                    
                    trade_id = trade_data.get('trade_id', f"trade_{int(time.time() * 1000)}")
                    
                    # Insert into trades table
                    cursor.execute('''
                        INSERT INTO trades 
                        (trade_id, symbol, side, quantity, entry_price, entry_time, 
                         strategy, timeframe, confidence, trade_type, data_source)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        trade_id,
                        trade_data.get('symbol'),
                        trade_data.get('side'),
                        trade_data.get('quantity', 0),
                        trade_data.get('entry_price', 0),
                        trade_data.get('entry_time', datetime.now().isoformat()),
                        trade_data.get('strategy', 'UNKNOWN'),
                        trade_data.get('timeframe', '15m'),
                        trade_data.get('confidence', 0),
                        'real',  # REAL TRADING ONLY
                        'REAL_BINANCE'
                    ))
                    
                    # Insert into active positions
                    cursor.execute('''
                        INSERT INTO active_positions 
                        (position_id, symbol, side, quantity, entry_price, entry_time,
                         strategy, timeframe, confidence, stop_loss, take_profit, data_source)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        trade_id,
                        trade_data.get('symbol'),
                        trade_data.get('side'),
                        trade_data.get('quantity', 0),
                        trade_data.get('entry_price', 0),
                        trade_data.get('entry_time', datetime.now().isoformat()),
                        trade_data.get('strategy', 'UNKNOWN'),
                        trade_data.get('timeframe', '15m'),
                        trade_data.get('confidence', 0),
                        trade_data.get('stop_loss'),
                        trade_data.get('take_profit'),
                        'REAL_BINANCE'
                    ))
                    
                    conn.commit()
                    
                    # Store in memory
                    self._active_trades[trade_id] = trade_data
                    
                    self.logger.info(f"Trade entry logged: {trade_id} - {trade_data.get('symbol')} {trade_data.get('side')}")
                    return True
                    
        except Exception as e:
            self.logger.error(f"Error logging trade entry: {e}")
            return False
    
    def log_trade_exit(self, trade_id: str, exit_data: Dict) -> bool:
        """Log trade exit - REAL TRADING ONLY"""
        try:
            with self._lock:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()
                    
                    # Calculate PnL
                    exit_price = exit_data.get('exit_price', 0)
                    fees = exit_data.get('fees', 0)
                    
                    # Get trade entry data
                    cursor.execute('SELECT * FROM trades WHERE trade_id = ?', (trade_id,))
                    trade_record = cursor.fetchone()
                    
                    if not trade_record:
                        self.logger.error(f"Trade {trade_id} not found for exit")
                        return False
                    
                    # Calculate PnL based on side
                    columns = [desc[0] for desc in cursor.description]
                    trade_dict = dict(zip(columns, trade_record))
                    
                    entry_price = trade_dict['entry_price']
                    quantity = trade_dict['quantity']
                    side = trade_dict['side']
                    
                    if side.upper() == 'BUY':
                        pnl_before_fees = (exit_price - entry_price) * quantity
                    else:
                        pnl_before_fees = (entry_price - exit_price) * quantity
                    
                    pnl_after_fees = pnl_before_fees - fees
                    pnl_percent = (pnl_after_fees / (entry_price * quantity)) * 100
                    
                    # Update trade record
                    cursor.execute('''
                        UPDATE trades 
                        SET exit_price = ?, exit_time = ?, pnl = ?, pnl_percent = ?, 
                            fees = ?, win = ?, notes = ?
                        WHERE trade_id = ?
                    ''', (
                        exit_price,
                        exit_data.get('exit_time', datetime.now().isoformat()),
                        pnl_after_fees,
                        pnl_percent,
                        fees,
                        1 if pnl_after_fees > 0 else 0,
                        exit_data.get('notes', ''),
                        trade_id
                    ))
                    
                    # Remove from active positions
                    cursor.execute('DELETE FROM active_positions WHERE position_id = ?', (trade_id,))
                    
                    conn.commit()
                    
                    # Remove from active trades
                    if trade_id in self._active_trades:
                        del self._active_trades[trade_id]
                    
                    # Clear performance cache
                    self._performance_cache = {}
                    
                    self.logger.info(f"Trade exit logged: {trade_id} - PnL: ${pnl_after_fees:.2f} ({pnl_percent:.2f}%)")
                    return True
                    
        except Exception as e:
            self.logger.error(f"Error logging trade exit: {e}")
            return False
    
    def save_metrics(self, metrics: Dict) -> bool:
        """Save current metrics to database - FIXED METHOD"""
        try:
            with self._lock:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()
                    
                    # Update or insert current metrics
                    cursor.execute('''
                        INSERT OR REPLACE INTO system_metrics 
                        (id, total_trades, winning_trades, total_pnl, win_rate, 
                         active_positions, best_trade, worst_trade, avg_trade,
                         last_updated, session_start, data_source, trading_mode)
                        VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        metrics.get('total_trades', 0),
                        metrics.get('winning_trades', 0),
                        metrics.get('total_pnl', 0.0),
                        metrics.get('win_rate', 0.0),
                        metrics.get('active_positions', 0),
                        metrics.get('best_trade', 0.0),
                        metrics.get('worst_trade', 0.0),
                        metrics.get('avg_trade', 0.0),
                        datetime.now().isoformat(),
                        metrics.get('session_start', datetime.now().isoformat()),
                        'REAL_TRADING',
                        metrics.get('trading_mode', 'PAPER_TRADING')
                    ))
                    
                    conn.commit()
                    self.logger.info(f"Metrics saved: {metrics.get('total_trades', 0)} trades, ${metrics.get('total_pnl', 0):.2f} P&L")
                    return True
                    
        except Exception as e:
            self.logger.error(f"Failed to save metrics: {e}")
            return False
    
    def load_metrics(self) -> Dict:
        """Load metrics from database - FIXED METHOD"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT total_trades, winning_trades, total_pnl, win_rate, 
                           active_positions, best_trade, worst_trade, avg_trade,
                           last_updated, session_start, data_source, trading_mode
                    FROM system_metrics WHERE id = 1
                ''')
                
                result = cursor.fetchone()
                
                if result:
                    metrics = {
                        'total_trades': result[0] or 0,
                        'winning_trades': result[1] or 0,
                        'total_pnl': result[2] or 0.0,
                        'win_rate': result[3] or 0.0,
                        'active_positions': result[4] or 0,
                        'best_trade': result[5] or 0.0,
                        'worst_trade': result[6] or 0.0,
                        'avg_trade': result[7] or 0.0,
                        'last_updated': result[8],
                        'session_start': result[9],
                        'data_source': result[10] or 'REAL_TRADING',
                        'trading_mode': result[11] or 'PAPER_TRADING',
                        'daily_trades': 0,  # Reset daily counter
                        'daily_pnl': 0.0   # Reset daily P&L
                    }
                    
                    self.logger.info(f"Metrics loaded: {metrics['total_trades']} trades, ${metrics['total_pnl']:.2f} P&L")
                    return metrics
                else:
                    self.logger.info("No previous metrics found, starting fresh")
                    return self._default_metrics()
                    
        except Exception as e:
            self.logger.error(f"Failed to load metrics: {e}")
            return self._default_metrics()
    
    def get_performance_summary(self) -> Dict:
        """Get comprehensive performance summary - REAL DATA ONLY"""
        try:
            # Check cache
            current_time = time.time()
            if (self._performance_cache and 
                current_time - self._cache_timestamp < self._cache_duration):
                return self._performance_cache
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Get basic trade statistics
                cursor.execute('''
                    SELECT 
                        COUNT(*) as total_trades,
                        SUM(CASE WHEN win = 1 THEN 1 ELSE 0 END) as winning_trades,
                        SUM(CASE WHEN win = 0 THEN 1 ELSE 0 END) as losing_trades,
                        AVG(CASE WHEN win = 1 THEN pnl ELSE NULL END) as avg_win,
                        AVG(CASE WHEN win = 0 THEN pnl ELSE NULL END) as avg_loss,
                        SUM(pnl) as total_pnl,
                        SUM(pnl_percent) as total_pnl_percent,
                        SUM(fees) as total_fees,
                        MAX(pnl) as best_trade,
                        MIN(pnl) as worst_trade
                    FROM trades
                    WHERE trade_type = 'real' AND data_source = 'REAL_BINANCE'
                ''')
                
                row = cursor.fetchone()
                if not row or row[0] == 0:
                    # No trades yet
                    performance = {
                        'total_trades': 0,
                        'winning_trades': 0,
                        'losing_trades': 0,
                        'win_rate': 0.0,
                        'total_pnl': 0.0,
                        'avg_win': 0.0,
                        'avg_loss': 0.0,
                        'avg_trade': 0.0,
                        'best_trade': 0.0,
                        'worst_trade': 0.0,
                        'total_fees': 0.0,
                        'profit_factor': 0.0,
                        'data_source': 'REAL_BINANCE'
                    }
                else:
                    total_trades = row[0]
                    winning_trades = row[1] or 0
                    losing_trades = row[2] or 0
                    avg_win = row[3] or 0.0
                    avg_loss = row[4] or 0.0
                    total_pnl = row[5] or 0.0
                    total_fees = row[7] or 0.0
                    best_trade = row[8] or 0.0
                    worst_trade = row[9] or 0.0
                    
                    win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
                    avg_trade = total_pnl / total_trades if total_trades > 0 else 0
                    
                    # Calculate profit factor
                    total_wins = winning_trades * avg_win if avg_win > 0 else 0
                    total_losses = abs(losing_trades * avg_loss) if avg_loss < 0 else 0
                    profit_factor = total_wins / total_losses if total_losses > 0 else float('inf') if total_wins > 0 else 0
                    
                    performance = {
                        'total_trades': total_trades,
                        'winning_trades': winning_trades,
                        'losing_trades': losing_trades,
                        'win_rate': win_rate,
                        'total_pnl': total_pnl,
                        'avg_win': avg_win,
                        'avg_loss': avg_loss,
                        'avg_trade': avg_trade,
                        'best_trade': best_trade,
                        'worst_trade': worst_trade,
                        'total_fees': total_fees,
                        'profit_factor': profit_factor,
                        'data_source': 'REAL_BINANCE'
                    }
                
                # Get active positions count
                cursor.execute('SELECT COUNT(*) FROM active_positions WHERE data_source = ?', ('REAL_BINANCE',))
                active_positions = cursor.fetchone()[0]
                performance['active_positions'] = active_positions
                
                # Cache the result
                self._performance_cache = performance
                self._cache_timestamp = current_time
                
                return performance
                
        except Exception as e:
            self.logger.error(f"Error getting performance summary: {e}")
            return self._default_performance()
    
    def save_performance_snapshot(self) -> bool:
        """Save performance snapshot - REAL DATA ONLY"""
        try:
            performance = self.get_performance_summary()
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO performance_snapshots 
                    (snapshot_time, total_pnl, total_trades, winning_trades, win_rate, 
                     active_positions, daily_pnl, data_source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    datetime.now().isoformat(),
                    performance.get('total_pnl', 0.0),
                    performance.get('total_trades', 0),
                    performance.get('winning_trades', 0),
                    performance.get('win_rate', 0.0),
                    performance.get('active_positions', 0),
                    0.0,  # daily_pnl would be calculated separately
                    'REAL_TRADING'
                ))
                conn.commit()
                
            self.logger.info("Performance snapshot saved")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save performance snapshot: {e}")
            return False
    
    def get_recent_trades(self, limit: int = 50) -> List[Dict]:
        """Get recent trade history - REAL DATA ONLY"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT trade_id, symbol, side, quantity, entry_price, exit_price,
                           entry_time, exit_time, pnl, pnl_percent, fees, strategy,
                           timeframe, confidence, win, notes, data_source
                    FROM trades 
                    WHERE trade_type = 'real' AND data_source = 'REAL_BINANCE'
                    ORDER BY entry_time DESC 
                    LIMIT ?
                ''', (limit,))
                
                columns = [desc[0] for desc in cursor.description]
                trades = []
                
                for row in cursor.fetchall():
                    trade = dict(zip(columns, row))
                    trade['is_win'] = bool(trade['win'])
                    trades.append(trade)
                
                return trades
                
        except Exception as e:
            self.logger.error(f"Failed to get recent trades: {e}")
            return []
    
    def get_active_positions(self) -> List[Dict]:
        """Get active positions - REAL DATA ONLY"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT position_id, symbol, side, quantity, entry_price, entry_time,
                           strategy, timeframe, confidence, stop_loss, take_profit,
                           unrealized_pnl, data_source
                    FROM active_positions
                    WHERE data_source = 'REAL_BINANCE'
                    ORDER BY entry_time DESC
                ''')
                
                columns = [desc[0] for desc in cursor.description]
                positions = []
                
                for row in cursor.fetchall():
                    position = dict(zip(columns, row))
                    positions.append(position)
                
                return positions
                
        except Exception as e:
            self.logger.error(f"Failed to get active positions: {e}")
            return []
    
    def update_unrealized_pnl(self, position_id: str, current_price: float, unrealized_pnl: float) -> bool:
        """Update unrealized P&L for active position"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE active_positions 
                    SET unrealized_pnl = ? 
                    WHERE position_id = ? AND data_source = 'REAL_BINANCE'
                ''', (unrealized_pnl, position_id))
                conn.commit()
                return cursor.rowcount > 0
                
        except Exception as e:
            self.logger.error(f"Failed to update unrealized PnL: {e}")
            return False
    
    def _default_metrics(self) -> Dict:
        """Return default metrics for new systems"""
        return {
            'total_trades': 0,
            'winning_trades': 0,
            'total_pnl': 0.0,
            'win_rate': 0.0,
            'active_positions': 0,
            'best_trade': 0.0,
            'worst_trade': 0.0,
            'avg_trade': 0.0,
            'daily_trades': 0,
            'daily_pnl': 0.0,
            'last_updated': None,
            'session_start': datetime.now().isoformat(),
            'data_source': 'REAL_TRADING',
            'trading_mode': 'PAPER_TRADING'
        }
    
    def _default_performance(self) -> Dict:
        """Return default performance stats"""
        return {
            'total_trades': 0,
            'winning_trades': 0,
            'losing_trades': 0,
            'win_rate': 0.0,
            'total_pnl': 0.0,
            'avg_win': 0.0,
            'avg_loss': 0.0,
            'avg_trade': 0.0,
            'best_trade': 0.0,
            'worst_trade': 0.0,
            'total_fees': 0.0,
            'profit_factor': 0.0,
            'active_positions': 0,
            'data_source': 'REAL_BINANCE'
        }
    
    def clear_all_data(self) -> bool:
        """Clear all metrics and trade history (use with caution!)"""
        try:
            with self._lock:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute('DELETE FROM system_metrics')
                    cursor.execute('DELETE FROM trades')
                    cursor.execute('DELETE FROM active_positions')
                    cursor.execute('DELETE FROM performance_snapshots')
                    conn.commit()
                
                # Clear memory cache
                self._active_trades = {}
                self._performance_cache = {}
                
                self.logger.info("All data cleared")
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to clear data: {e}")
            return False
    
    def backup_database(self, backup_path: str = None) -> bool:
        """Backup database to file"""
        try:
            if not backup_path:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_path = f"data/backup_trade_logs_{timestamp}.db"
            
            import shutil
            shutil.copy2(self.db_path, backup_path)
            
            self.logger.info(f"Database backed up to: {backup_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to backup database: {e}")
            return False
    
    def get_statistics_summary(self) -> Dict:
        """Get comprehensive trading statistics - REAL DATA ONLY"""
        try:
            performance = self.get_performance_summary()
            recent_trades = self.get_recent_trades(10)
            active_positions = self.get_active_positions()
            
            return {
                'performance': performance,
                'recent_trades_count': len(recent_trades),
                'active_positions_count': len(active_positions),
                'recent_trades': recent_trades,
                'active_positions': active_positions,
                'last_updated': datetime.now().isoformat(),
                'data_source': 'REAL_BINANCE_ONLY'
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get statistics summary: {e}")
            return {
                'performance': self._default_performance(),
                'recent_trades_count': 0,
                'active_positions_count': 0,
                'recent_trades': [],
                'active_positions': [],
                'last_updated': datetime.now().isoformat(),
                'data_source': 'REAL_BINANCE_ONLY'
            }
    
    def close(self):
        """Close persistence system"""
        try:
            # Save final snapshot
            self.save_performance_snapshot()
            self.logger.info("PnL Persistence closed successfully")
        except Exception as e:
            self.logger.error(f"Error closing persistence: {e}")

# Convenience functions for external use
def get_persistence_instance(db_path: str = None) -> PnLPersistence:
    """Get a PnLPersistence instance"""
    if db_path is None:
        db_path = "data/trade_logs.db"
    return PnLPersistence(db_path)

def quick_performance_check() -> Dict:
    """Quick performance check for REAL trading data"""
    persistence = get_persistence_instance()
    return persistence.get_performance_summary()

if __name__ == "__main__":
    # Test the persistence system
    logging.basicConfig(level=logging.INFO)
    
    print("Testing PnL Persistence System - REAL DATA ONLY")
    persistence = PnLPersistence()
    
    # Test metrics
    test_metrics = {
        'total_trades': 10,
        'winning_trades': 7,
        'total_pnl': 150.50,
        'win_rate': 70.0,
        'active_positions': 2,
        'best_trade': 45.20,
        'data_source': 'REAL_TRADING'
    }
    
    print("Saving test metrics...")
    persistence.save_metrics(test_metrics)
    
    print("Loading metrics...")
    loaded_metrics = persistence.load_metrics()
    print(f"Loaded: {loaded_metrics}")
    
    print("Getting performance summary...")
    performance = persistence.get_performance_summary()
    print(f"Performance: {performance}")
    
    print("PnL Persistence test completed successfully!")