#!/usr/bin/env python3
"""
V3 PnL Persistence Manager - REAL DATA ONLY
Enhanced trade logging and performance tracking with real data enforcement
NO SIMULATION - PRODUCTION GRADE SYSTEM
"""

import os
import sys
import logging
import sqlite3
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
import threading
from contextlib import contextmanager

# Enforce real data mode
REAL_DATA_ONLY = True
MOCK_DATA_DISABLED = True

@dataclass
class TradeRecord:
    """Structure for trade records"""
    trade_id: str
    symbol: str
    side: str  # 'buy' or 'sell'
    entry_price: float
    exit_price: float
    quantity: float
    entry_time: datetime
    exit_time: datetime
    pnl: float
    pnl_percent: float
    strategy: str
    timeframe: str
    win: bool
    fees: float = 0.0
    notes: str = ""
    trade_type: str = "real"  # 'real', 'paper', 'backtest'

@dataclass
class PerformanceMetrics:
    """Structure for performance metrics"""
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    total_pnl: float
    total_pnl_percent: float
    average_win: float
    average_loss: float
    profit_factor: float
    sharpe_ratio: float
    max_drawdown: float
    max_consecutive_wins: int
    max_consecutive_losses: int
    total_fees: float
    active_positions: int

class PnLPersistence:
    """
    V3 PnL Persistence Manager
    - REAL DATA ONLY
    - Enhanced trade logging
    - Real-time performance tracking
    - Production-grade reliability
    """
    
    def __init__(self):
        """Initialize PnL Persistence with real data enforcement"""
        self.logger = logging.getLogger('pnl_persistence')
        
        # Enforce real data mode
        if not REAL_DATA_ONLY:
            raise ValueError("CRITICAL: System must use REAL DATA ONLY")
        
        # Database configuration
        self.db_path = os.getenv('DB_PATH', 'data/trade_logs.db')
        self.backup_frequency = int(os.getenv('PERSISTENCE_BACKUP_FREQUENCY', 3600))  # 1 hour
        
        # Ensure data directory exists
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        # Threading lock for database operations
        self._db_lock = threading.Lock()
        
        # Performance cache
        self._performance_cache = {}
        self._cache_timestamp = 0
        self._cache_duration = 300  # 5 minutes
        
        # Active trades tracking
        self._active_trades = {}
        
        # Initialize database
        self._initialize_database()
        
        # Load active trades
        self._load_active_trades()
        
        self.logger.info("PnL Persistence initialized - REAL DATA ONLY")
    
    def _initialize_database(self):
        """Initialize SQLite database with required tables"""
        try:
            with self._get_db_connection() as conn:
                cursor = conn.cursor()
                
                # Create trades table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS trades (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        trade_id TEXT UNIQUE NOT NULL,
                        symbol TEXT NOT NULL,
                        side TEXT NOT NULL,
                        entry_price REAL NOT NULL,
                        exit_price REAL,
                        quantity REAL NOT NULL,
                        entry_time TEXT NOT NULL,
                        exit_time TEXT,
                        pnl REAL,
                        pnl_percent REAL,
                        strategy TEXT NOT NULL,
                        timeframe TEXT NOT NULL,
                        win INTEGER,
                        fees REAL DEFAULT 0.0,
                        notes TEXT DEFAULT '',
                        trade_type TEXT DEFAULT 'real',
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Create performance snapshots table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS performance_snapshots (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        snapshot_date TEXT NOT NULL,
                        total_trades INTEGER NOT NULL,
                        winning_trades INTEGER NOT NULL,
                        losing_trades INTEGER NOT NULL,
                        win_rate REAL NOT NULL,
                        total_pnl REAL NOT NULL,
                        total_pnl_percent REAL NOT NULL,
                        average_win REAL NOT NULL,
                        average_loss REAL NOT NULL,
                        profit_factor REAL NOT NULL,
                        sharpe_ratio REAL NOT NULL,
                        max_drawdown REAL NOT NULL,
                        max_consecutive_wins INTEGER NOT NULL,
                        max_consecutive_losses INTEGER NOT NULL,
                        total_fees REAL NOT NULL,
                        active_positions INTEGER NOT NULL,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Create active positions table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS active_positions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        position_id TEXT UNIQUE NOT NULL,
                        symbol TEXT NOT NULL,
                        side TEXT NOT NULL,
                        entry_price REAL NOT NULL,
                        quantity REAL NOT NULL,
                        entry_time TEXT NOT NULL,
                        strategy TEXT NOT NULL,
                        timeframe TEXT NOT NULL,
                        unrealized_pnl REAL DEFAULT 0.0,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Create indices for better performance
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_trades_strategy ON trades(strategy)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_trades_entry_time ON trades(entry_time)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_active_positions_symbol ON active_positions(symbol)')
                
                conn.commit()
                self.logger.info("Database initialized successfully")
                
        except Exception as e:
            self.logger.error(f"Database initialization failed: {e}")
            raise
    
    @contextmanager
    def _get_db_connection(self):
        """Get database connection with proper error handling"""
        conn = None
        try:
            with self._db_lock:
                conn = sqlite3.connect(self.db_path, timeout=30)
                conn.execute('PRAGMA journal_mode=WAL')
                conn.execute('PRAGMA synchronous=NORMAL')
                conn.execute('PRAGMA cache_size=10000')
                yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            self.logger.error(f"Database connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def _load_active_trades(self):
        """Load active trades from database"""
        try:
            with self._get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT position_id, symbol, side, entry_price, quantity, 
                           entry_time, strategy, timeframe, unrealized_pnl
                    FROM active_positions
                ''')
                
                rows = cursor.fetchall()
                for row in rows:
                    position_id, symbol, side, entry_price, quantity, entry_time, strategy, timeframe, unrealized_pnl = row
                    
                    self._active_trades[position_id] = {
                        'symbol': symbol,
                        'side': side,
                        'entry_price': entry_price,
                        'quantity': quantity,
                        'entry_time': datetime.fromisoformat(entry_time),
                        'strategy': strategy,
                        'timeframe': timeframe,
                        'unrealized_pnl': unrealized_pnl
                    }
                
                self.logger.info(f"Loaded {len(self._active_trades)} active positions")
                
        except Exception as e:
            self.logger.error(f"Error loading active trades: {e}")
    
    def log_trade_entry(self, trade_data: Dict[str, Any]) -> str:
        """Log a trade entry"""
        try:
            trade_id = trade_data.get('trade_id', f"trade_{int(time.time())}")
            
            # Add to active trades
            self._active_trades[trade_id] = {
                'symbol': trade_data['symbol'],
                'side': trade_data['side'],
                'entry_price': trade_data['entry_price'],
                'quantity': trade_data['quantity'],
                'entry_time': trade_data.get('entry_time', datetime.now()),
                'strategy': trade_data['strategy'],
                'timeframe': trade_data['timeframe'],
                'unrealized_pnl': 0.0
            }
            
            # Save to database
            with self._get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO active_positions 
                    (position_id, symbol, side, entry_price, quantity, entry_time, strategy, timeframe, unrealized_pnl)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    trade_id,
                    trade_data['symbol'],
                    trade_data['side'],
                    trade_data['entry_price'],
                    trade_data['quantity'],
                    trade_data.get('entry_time', datetime.now()).isoformat(),
                    trade_data['strategy'],
                    trade_data['timeframe'],
                    0.0
                ))
                conn.commit()
            
            self.logger.info(f"Trade entry logged: {trade_id} - {trade_data['symbol']} {trade_data['side']}")
            return trade_id
            
        except Exception as e:
            self.logger.error(f"Error logging trade entry: {e}")
            return None
    
    def log_trade_exit(self, trade_id: str, exit_data: Dict[str, Any]) -> bool:
        """Log a trade exit and calculate PnL"""
        try:
            if trade_id not in self._active_trades:
                self.logger.warning(f"Trade {trade_id} not found in active trades")
                return False
            
            trade = self._active_trades[trade_id]
            
            # Calculate PnL
            entry_price = trade['entry_price']
            exit_price = exit_data['exit_price']
            quantity = trade['quantity']
            side = trade['side']
            
            if side.lower() == 'buy':
                pnl = (exit_price - entry_price) * quantity
            else:  # sell
                pnl = (entry_price - exit_price) * quantity
            
            pnl_percent = (pnl / (entry_price * quantity)) * 100
            
            # Calculate fees
            fees = exit_data.get('fees', 0.0)
            pnl_after_fees = pnl - fees
            
            # Create trade record
            trade_record = TradeRecord(
                trade_id=trade_id,
                symbol=trade['symbol'],
                side=side,
                entry_price=entry_price,
                exit_price=exit_price,
                quantity=quantity,
                entry_time=trade['entry_time'],
                exit_time=exit_data.get('exit_time', datetime.now()),
                pnl=pnl_after_fees,
                pnl_percent=pnl_percent,
                strategy=trade['strategy'],
                timeframe=trade['timeframe'],
                win=pnl_after_fees > 0,
                fees=fees,
                notes=exit_data.get('notes', ''),
                trade_type='real'
            )
            
            # Save completed trade
            with self._get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO trades 
                    (trade_id, symbol, side, entry_price, exit_price, quantity, entry_time, exit_time, 
                     pnl, pnl_percent, strategy, timeframe, win, fees, notes, trade_type)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    trade_record.trade_id,
                    trade_record.symbol,
                    trade_record.side,
                    trade_record.entry_price,
                    trade_record.exit_price,
                    trade_record.quantity,
                    trade_record.entry_time.isoformat(),
                    trade_record.exit_time.isoformat(),
                    trade_record.pnl,
                    trade_record.pnl_percent,
                    trade_record.strategy,
                    trade_record.timeframe,
                    1 if trade_record.win else 0,
                    trade_record.fees,
                    trade_record.notes,
                    trade_record.trade_type
                ))
                
                # Remove from active positions
                cursor.execute('DELETE FROM active_positions WHERE position_id = ?', (trade_id,))
                
                conn.commit()
            
            # Remove from active trades
            del self._active_trades[trade_id]
            
            # Clear performance cache
            self._performance_cache = {}
            
            self.logger.info(f"Trade exit logged: {trade_id} - PnL: ${pnl_after_fees:.2f} ({pnl_percent:.2f}%)")
            return True
            
        except Exception as e:
            self.logger.error(f"Error logging trade exit: {e}")
            return False
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get comprehensive performance summary"""
        try:
            # Check cache
            current_time = time.time()
            if (self._performance_cache and 
                current_time - self._cache_timestamp < self._cache_duration):
                return self._performance_cache
            
            with self._get_db_connection() as conn:
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
                        SUM(fees) as total_fees
                    FROM trades
                    WHERE trade_type = 'real'
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
                        'total_pnl_percent': 0.0,
                        'average_win': 0.0,
                        'average_loss': 0.0,
                        'profit_factor': 0.0,
                        'sharpe_ratio': 0.0,
                        'max_drawdown': 0.0,
                        'max_consecutive_wins': 0,
                        'max_consecutive_losses': 0,
                        'total_fees': 0.0,
                        'active_positions': len(self._active_trades)
                    }
                else:
                    total_trades, winning_trades, losing_trades, avg_win, avg_loss, total_pnl, total_pnl_percent, total_fees = row
                    
                    # Calculate derived metrics
                    win_rate = (winning_trades / total_trades) * 100 if total_trades > 0 else 0
                    avg_win = avg_win or 0.0
                    avg_loss = abs(avg_loss) if avg_loss else 0.0
                    profit_factor = avg_win / avg_loss if avg_loss > 0 else float('inf') if avg_win > 0 else 0
                    
                    # Calculate Sharpe ratio (simplified)
                    cursor.execute('SELECT pnl FROM trades WHERE trade_type = "real" ORDER BY entry_time')
                    pnl_values = [row[0] for row in cursor.fetchall()]
                    
                    if len(pnl_values) > 1:
                        import numpy as np
                        sharpe_ratio = np.mean(pnl_values) / np.std(pnl_values) if np.std(pnl_values) > 0 else 0
                    else:
                        sharpe_ratio = 0
                    
                    # Calculate max drawdown
                    cumulative_pnl = []
                    running_total = 0
                    for pnl in pnl_values:
                        running_total += pnl
                        cumulative_pnl.append(running_total)
                    
                    if cumulative_pnl:
                        peak = cumulative_pnl[0]
                        max_drawdown = 0
                        for value in cumulative_pnl:
                            if value > peak:
                                peak = value
                            drawdown = peak - value
                            if drawdown > max_drawdown:
                                max_drawdown = drawdown
                    else:
                        max_drawdown = 0
                    
                    # Calculate consecutive wins/losses
                    consecutive_wins = 0
                    consecutive_losses = 0
                    max_consecutive_wins = 0
                    max_consecutive_losses = 0
                    
                    cursor.execute('SELECT win FROM trades WHERE trade_type = "real" ORDER BY entry_time')
                    win_sequence = [row[0] for row in cursor.fetchall()]
                    
                    for win in win_sequence:
                        if win:
                            consecutive_wins += 1
                            consecutive_losses = 0
                            max_consecutive_wins = max(max_consecutive_wins, consecutive_wins)
                        else:
                            consecutive_losses += 1
                            consecutive_wins = 0
                            max_consecutive_losses = max(max_consecutive_losses, consecutive_losses)
                    
                    performance = {
                        'total_trades': total_trades,
                        'winning_trades': winning_trades,
                        'losing_trades': losing_trades,
                        'win_rate': win_rate,
                        'total_pnl': total_pnl,
                        'total_pnl_percent': total_pnl_percent,
                        'average_win': avg_win,
                        'average_loss': avg_loss,
                        'profit_factor': profit_factor,
                        'sharpe_ratio': sharpe_ratio,
                        'max_drawdown': max_drawdown,
                        'max_consecutive_wins': max_consecutive_wins,
                        'max_consecutive_losses': max_consecutive_losses,
                        'total_fees': total_fees or 0.0,
                        'active_positions': len(self._active_trades)
                    }
                
                # Cache the result
                self._performance_cache = performance
                self._cache_timestamp = current_time
                
                return performance
                
        except Exception as e:
            self.logger.error(f"Error getting performance summary: {e}")
            # Return empty performance data
            return {
                'total_trades': 0,
                'winning_trades': 0,
                'losing_trades': 0,
                'win_rate': 0.0,
                'total_pnl': 0.0,
                'total_pnl_percent': 0.0,
                'average_win': 0.0,
                'average_loss': 0.0,
                'profit_factor': 0.0,
                'sharpe_ratio': 0.0,
                'max_drawdown': 0.0,
                'max_consecutive_wins': 0,
                'max_consecutive_losses': 0,
                'total_fees': 0.0,
                'active_positions': 0
            }
    
    def get_recent_trades(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent completed trades"""
        try:
            with self._get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT trade_id, symbol, side, entry_price, exit_price, quantity,
                           entry_time, exit_time, pnl, pnl_percent, strategy, timeframe, win, fees
                    FROM trades
                    WHERE trade_type = 'real'
                    ORDER BY exit_time DESC
                    LIMIT ?
                ''', (limit,))
                
                trades = []
                for row in cursor.fetchall():
                    trade_id, symbol, side, entry_price, exit_price, quantity, entry_time, exit_time, pnl, pnl_percent, strategy, timeframe, win, fees = row
                    
                    trades.append({
                        'trade_id': trade_id,
                        'symbol': symbol,
                        'side': side,
                        'entry_price': entry_price,
                        'exit_price': exit_price,
                        'quantity': quantity,
                        'entry_time': entry_time,
                        'exit_time': exit_time,
                        'pnl': pnl,
                        'pnl_percent': pnl_percent,
                        'strategy': strategy,
                        'timeframe': timeframe,
                        'win': bool(win),
                        'fees': fees
                    })
                
                return trades
                
        except Exception as e:
            self.logger.error(f"Error getting recent trades: {e}")
            return []
    
    def get_active_positions(self) -> List[Dict[str, Any]]:
        """Get current active positions"""
        try:
            positions = []
            for position_id, trade in self._active_trades.items():
                positions.append({
                    'position_id': position_id,
                    'symbol': trade['symbol'],
                    'side': trade['side'],
                    'entry_price': trade['entry_price'],
                    'quantity': trade['quantity'],
                    'entry_time': trade['entry_time'].isoformat(),
                    'strategy': trade['strategy'],
                    'timeframe': trade['timeframe'],
                    'unrealized_pnl': trade['unrealized_pnl']
                })
            
            return positions
            
        except Exception as e:
            self.logger.error(f"Error getting active positions: {e}")
            return []
    
    def update_unrealized_pnl(self, position_id: str, current_price: float):
        """Update unrealized PnL for an active position"""
        try:
            if position_id not in self._active_trades:
                return
            
            trade = self._active_trades[position_id]
            entry_price = trade['entry_price']
            quantity = trade['quantity']
            side = trade['side']
            
            if side.lower() == 'buy':
                unrealized_pnl = (current_price - entry_price) * quantity
            else:  # sell
                unrealized_pnl = (entry_price - current_price) * quantity
            
            trade['unrealized_pnl'] = unrealized_pnl
            
            # Update in database
            with self._get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE active_positions 
                    SET unrealized_pnl = ?, updated_at = CURRENT_TIMESTAMP 
                    WHERE position_id = ?
                ''', (unrealized_pnl, position_id))
                conn.commit()
            
        except Exception as e:
            self.logger.error(f"Error updating unrealized PnL: {e}")
    
    def save_performance_snapshot(self):
        """Save current performance snapshot"""
        try:
            performance = self.get_performance_summary()
            
            with self._get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO performance_snapshots 
                    (snapshot_date, total_trades, winning_trades, losing_trades, win_rate,
                     total_pnl, total_pnl_percent, average_win, average_loss, profit_factor,
                     sharpe_ratio, max_drawdown, max_consecutive_wins, max_consecutive_losses,
                     total_fees, active_positions)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    datetime.now().isoformat(),
                    performance['total_trades'],
                    performance['winning_trades'],
                    performance['losing_trades'],
                    performance['win_rate'],
                    performance['total_pnl'],
                    performance['total_pnl_percent'],
                    performance['average_win'],
                    performance['average_loss'],
                    performance['profit_factor'],
                    performance['sharpe_ratio'],
                    performance['max_drawdown'],
                    performance['max_consecutive_wins'],
                    performance['max_consecutive_losses'],
                    performance['total_fees'],
                    performance['active_positions']
                ))
                conn.commit()
            
            self.logger.info("Performance snapshot saved")
            
        except Exception as e:
            self.logger.error(f"Error saving performance snapshot: {e}")
    
    def close(self):
        """Close database connections and cleanup"""
        try:
            # Save final performance snapshot
            self.save_performance_snapshot()
            
            # Clear cache
            self._performance_cache = {}
            
            self.logger.info("PnL Persistence closed")
            
        except Exception as e:
            self.logger.error(f"Error closing PnL Persistence: {e}")

# Global instance management
_pnl_persistence_instance = None

def get_pnl_persistence() -> PnLPersistence:
    """Get or create the PnL persistence instance"""
    global _pnl_persistence_instance
    if _pnl_persistence_instance is None:
        _pnl_persistence_instance = PnLPersistence()
    return _pnl_persistence_instance

if __name__ == "__main__":
    # Test the PnL persistence
    pnl = PnLPersistence()
    performance = pnl.get_performance_summary()
    print(f"Performance: {performance}")