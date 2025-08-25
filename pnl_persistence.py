#!/usr/bin/env python3
"""
P&L PERSISTENCE SYSTEM
=====================
Saves and loads trading performance data between system restarts
"""

import json
import sqlite3
import os
import logging
from datetime import datetime
from typing import Dict, Optional

class PnLPersistence:
    """Handles saving and loading P&L data between system restarts"""
    
    def __init__(self, db_path: str = "data/trading_metrics.db"):
        self.db_path = db_path
        self.ensure_data_dir()
        self.init_database()
        
    def ensure_data_dir(self):
        """Ensure data directory exists"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
    
    def init_database(self):
        """Initialize the metrics database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Create tables
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS system_metrics (
                        id INTEGER PRIMARY KEY,
                        total_trades INTEGER DEFAULT 0,
                        winning_trades INTEGER DEFAULT 0,
                        total_pnl REAL DEFAULT 0.0,
                        win_rate REAL DEFAULT 0.0,
                        active_positions INTEGER DEFAULT 0,
                        last_updated TEXT,
                        session_start TEXT,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS trade_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        trade_id INTEGER,
                        symbol TEXT,
                        side TEXT,
                        quantity REAL,
                        entry_price REAL,
                        exit_price REAL,
                        profit_loss REAL,
                        profit_pct REAL,
                        is_win BOOLEAN,
                        confidence REAL,
                        timestamp TEXT,
                        source TEXT,
                        session_id TEXT
                    )
                ''')
                
                conn.commit()
                logging.info("[PnL] Database initialized successfully")
                
        except Exception as e:
            logging.error(f"[PnL] Database initialization failed: {e}")
    
    def save_metrics(self, metrics: Dict) -> bool:
        """Save current metrics to database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Update or insert current metrics
                cursor.execute('''
                    INSERT OR REPLACE INTO system_metrics 
                    (id, total_trades, winning_trades, total_pnl, win_rate, 
                     active_positions, last_updated, session_start)
                    VALUES (1, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    metrics.get('total_trades', 0),
                    metrics.get('winning_trades', 0),
                    metrics.get('total_pnl', 0.0),
                    metrics.get('win_rate', 0.0),
                    metrics.get('active_positions', 0),
                    datetime.now().isoformat(),
                    metrics.get('session_start', datetime.now().isoformat())
                ))
                
                conn.commit()
                logging.info(f"[PnL] Metrics saved: {metrics.get('total_trades', 0)} trades, ${metrics.get('total_pnl', 0):.2f} P&L")
                return True
                
        except Exception as e:
            logging.error(f"[PnL] Failed to save metrics: {e}")
            return False
    
    def load_metrics(self) -> Dict:
        """Load metrics from database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT total_trades, winning_trades, total_pnl, win_rate, 
                           active_positions, last_updated, session_start
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
                        'last_updated': result[5],
                        'session_start': result[6],
                        'daily_trades': 0  # Reset daily counter
                    }
                    
                    logging.info(f"[PnL] Metrics loaded: {metrics['total_trades']} trades, ${metrics['total_pnl']:.2f} P&L")
                    return metrics
                else:
                    logging.info("[PnL] No previous metrics found, starting fresh")
                    return self._default_metrics()
                    
        except Exception as e:
            logging.error(f"[PnL] Failed to load metrics: {e}")
            return self._default_metrics()
    
    def save_trade(self, trade_data: Dict) -> bool:
        """Save individual trade to history"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT INTO trade_history 
                    (trade_id, symbol, side, quantity, entry_price, exit_price,
                     profit_loss, profit_pct, is_win, confidence, timestamp, source, session_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    trade_data.get('trade_id'),
                    trade_data.get('symbol'),
                    trade_data.get('side'),
                    trade_data.get('quantity', 0),
                    trade_data.get('entry_price', 0),
                    trade_data.get('exit_price', 0),
                    trade_data.get('profit_loss', 0),
                    trade_data.get('profit_pct', 0),
                    trade_data.get('win', False),
                    trade_data.get('confidence', 0),
                    trade_data.get('timestamp', datetime.now().isoformat()),
                    trade_data.get('source', 'UNKNOWN'),
                    trade_data.get('session_id', 'default')
                ))
                
                conn.commit()
                return True
                
        except Exception as e:
            logging.error(f"[PnL] Failed to save trade: {e}")
            return False
    
    def get_trade_history(self, limit: int = 100) -> list:
        """Get recent trade history"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT * FROM trade_history 
                    ORDER BY timestamp DESC 
                    LIMIT ?
                ''', (limit,))
                
                columns = [desc[0] for desc in cursor.description]
                results = cursor.fetchall()
                
                return [dict(zip(columns, row)) for row in results]
                
        except Exception as e:
            logging.error(f"[PnL] Failed to get trade history: {e}")
            return []
    
    def get_summary_stats(self) -> Dict:
        """Get summary statistics"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Get basic stats
                cursor.execute('''
                    SELECT 
                        COUNT(*) as total_trades,
                        SUM(CASE WHEN is_win = 1 THEN 1 ELSE 0 END) as wins,
                        SUM(profit_loss) as total_pnl,
                        AVG(profit_loss) as avg_trade,
                        MAX(profit_loss) as best_trade,
                        MIN(profit_loss) as worst_trade
                    FROM trade_history
                ''')
                
                result = cursor.fetchone()
                
                if result and result[0] > 0:
                    total_trades, wins, total_pnl, avg_trade, best_trade, worst_trade = result
                    win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
                    
                    return {
                        'total_trades': total_trades,
                        'winning_trades': wins,
                        'losing_trades': total_trades - wins,
                        'win_rate': win_rate,
                        'total_pnl': total_pnl or 0,
                        'avg_trade': avg_trade or 0,
                        'best_trade': best_trade or 0,
                        'worst_trade': worst_trade or 0
                    }
                
            return self._default_summary()
            
        except Exception as e:
            logging.error(f"[PnL] Failed to get summary stats: {e}")
            return self._default_summary()
    
    def _default_metrics(self) -> Dict:
        """Return default metrics for new systems"""
        return {
            'total_trades': 0,
            'winning_trades': 0,
            'total_pnl': 0.0,
            'win_rate': 0.0,
            'active_positions': 0,
            'daily_trades': 0,
            'last_updated': None,
            'session_start': datetime.now().isoformat()
        }
    
    def _default_summary(self) -> Dict:
        """Return default summary stats"""
        return {
            'total_trades': 0,
            'winning_trades': 0,
            'losing_trades': 0,
            'win_rate': 0.0,
            'total_pnl': 0.0,
            'avg_trade': 0.0,
            'best_trade': 0.0,
            'worst_trade': 0.0
        }
    
    def clear_all_data(self) -> bool:
        """Clear all metrics and trade history (use with caution!)"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('DELETE FROM system_metrics')
                cursor.execute('DELETE FROM trade_history')
                conn.commit()
                
            logging.info("[PnL] All data cleared")
            return True
            
        except Exception as e:
            logging.error(f"[PnL] Failed to clear data: {e}")
            return False
