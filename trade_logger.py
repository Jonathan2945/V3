#!/usr/bin/env python3
"""
COMPREHENSIVE TRADE LOGGER
=========================
Complete trade logging system with detailed entry/exit tracking,
P&L calculations, and trade analysis
"""

import sqlite3
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import pandas as pd

class TradeLogger:
    """Comprehensive trade logging and analysis system"""
    
    def __init__(self, db_path: str = "data/trade_logs.db"):
        self.db_path = db_path
        self.ensure_data_dir()
        self.init_database()
        
        logging.info("[TRADE_LOG] Comprehensive trade logger initialized")
    
    def ensure_data_dir(self):
        """Ensure data directory exists"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
    
    def init_database(self):
        """Initialize comprehensive trade logging database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Main trades table with complete lifecycle
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS trades (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        trade_id TEXT UNIQUE NOT NULL,
                        symbol TEXT NOT NULL,
                        side TEXT NOT NULL,
                        status TEXT DEFAULT 'OPEN',
                        
                        -- Entry Details
                        entry_time TEXT NOT NULL,
                        entry_price REAL NOT NULL,
                        entry_reason TEXT,
                        entry_pattern TEXT,
                        entry_confidence REAL,
                        quantity REAL NOT NULL,
                        
                        -- Exit Details
                        exit_time TEXT,
                        exit_price REAL,
                        exit_reason TEXT,
                        exit_type TEXT,
                        
                        -- Performance
                        profit_loss REAL DEFAULT 0,
                        profit_pct REAL DEFAULT 0,
                        is_win BOOLEAN,
                        
                        -- Duration
                        hold_duration_seconds INTEGER,
                        hold_duration_human TEXT,
                        
                        -- Risk Management
                        stop_loss REAL,
                        take_profit REAL,
                        risk_reward_ratio REAL,
                        max_favorable_excursion REAL DEFAULT 0,
                        max_adverse_excursion REAL DEFAULT 0,
                        
                        -- Market Context
                        market_conditions TEXT,
                        volatility_at_entry REAL,
                        volume_at_entry REAL,
                        
                        -- Strategy Info
                        strategy_used TEXT,
                        ml_enhanced BOOLEAN DEFAULT FALSE,
                        algorithm_version TEXT,
                        
                        -- Metadata
                        session_id TEXT,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Trade updates table for tracking price movements during trade
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS trade_updates (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        trade_id TEXT NOT NULL,
                        timestamp TEXT NOT NULL,
                        current_price REAL NOT NULL,
                        unrealized_pnl REAL NOT NULL,
                        unrealized_pct REAL NOT NULL,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (trade_id) REFERENCES trades (trade_id)
                    )
                ''')
                
                # Portfolio snapshots
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS portfolio_snapshots (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT NOT NULL,
                        total_balance REAL NOT NULL,
                        available_cash REAL NOT NULL,
                        total_pnl REAL NOT NULL,
                        open_positions INTEGER NOT NULL,
                        daily_pnl REAL NOT NULL,
                        assets_json TEXT,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Trade analysis cache
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS trade_analysis (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        analysis_date TEXT NOT NULL,
                        total_trades INTEGER,
                        winning_trades INTEGER,
                        losing_trades INTEGER,
                        win_rate REAL,
                        total_pnl REAL,
                        avg_win REAL,
                        avg_loss REAL,
                        profit_factor REAL,
                        sharpe_ratio REAL,
                        max_drawdown REAL,
                        avg_hold_time_seconds INTEGER,
                        best_trade REAL,
                        worst_trade REAL,
                        analysis_json TEXT,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Create indexes for performance
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_trades_entry_time ON trades(entry_time)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_trade_updates_trade_id ON trade_updates(trade_id)')
                
                conn.commit()
                logging.info("[TRADE_LOG] Database initialized successfully")
                
        except Exception as e:
            logging.error(f"[TRADE_LOG] Database initialization failed: {e}")
    
    def log_trade_entry(self, trade_data: Dict) -> str:
        """Log a new trade entry with comprehensive details"""
        try:
            trade_id = trade_data.get('trade_id') or f"trade_{int(datetime.now().timestamp() * 1000)}"
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT INTO trades (
                        trade_id, symbol, side, entry_time, entry_price, entry_reason,
                        entry_pattern, entry_confidence, quantity, stop_loss, take_profit,
                        risk_reward_ratio, market_conditions, volatility_at_entry,
                        volume_at_entry, strategy_used, ml_enhanced, algorithm_version,
                        session_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    trade_id,
                    trade_data.get('symbol'),
                    trade_data.get('side'),
                    trade_data.get('entry_time', datetime.now().isoformat()),
                    trade_data.get('entry_price'),
                    trade_data.get('entry_reason', 'Algorithm signal'),
                    trade_data.get('entry_pattern', 'Price action'),
                    trade_data.get('entry_confidence', 0),
                    trade_data.get('quantity'),
                    trade_data.get('stop_loss'),
                    trade_data.get('take_profit'),
                    trade_data.get('risk_reward_ratio'),
                    json.dumps(trade_data.get('market_conditions', {})),
                    trade_data.get('volatility_at_entry'),
                    trade_data.get('volume_at_entry'),
                    trade_data.get('strategy_used', 'Default'),
                    trade_data.get('ml_enhanced', False),
                    trade_data.get('algorithm_version', '1.0'),
                    trade_data.get('session_id', datetime.now().strftime('%Y%m%d'))
                ))
                
                conn.commit()
                
            logging.info(f"[TRADE_LOG] Trade entry logged: {trade_id} - {trade_data.get('side')} {trade_data.get('symbol')}")
            return trade_id
            
        except Exception as e:
            logging.error(f"[TRADE_LOG] Failed to log trade entry: {e}")
            return None
    
    def update_trade_position(self, trade_id: str, current_price: float) -> bool:
        """Update trade with current price and unrealized P&L"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Get trade details
                cursor.execute('''
                    SELECT entry_price, quantity, side FROM trades 
                    WHERE trade_id = ? AND status = 'OPEN'
                ''', (trade_id,))
                
                result = cursor.fetchone()
                if not result:
                    return False
                
                entry_price, quantity, side = result
                
                # Calculate unrealized P&L
                if side.upper() == 'BUY':
                    unrealized_pnl = (current_price - entry_price) * quantity
                else:  # SELL
                    unrealized_pnl = (entry_price - current_price) * quantity
                
                unrealized_pct = (unrealized_pnl / (entry_price * quantity)) * 100
                
                # Insert trade update
                cursor.execute('''
                    INSERT INTO trade_updates (trade_id, timestamp, current_price, unrealized_pnl, unrealized_pct)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    trade_id,
                    datetime.now().isoformat(),
                    current_price,
                    unrealized_pnl,
                    unrealized_pct
                ))
                
                # Update max favorable/adverse excursion
                cursor.execute('''
                    UPDATE trades SET 
                        max_favorable_excursion = MAX(max_favorable_excursion, ?),
                        max_adverse_excursion = MIN(max_adverse_excursion, ?),
                        updated_at = ?
                    WHERE trade_id = ?
                ''', (
                    max(0, unrealized_pnl),
                    min(0, unrealized_pnl),
                    datetime.now().isoformat(),
                    trade_id
                ))
                
                conn.commit()
                return True
                
        except Exception as e:
            logging.error(f"[TRADE_LOG] Failed to update trade position: {e}")
            return False
    
    def log_trade_exit(self, trade_id: str, exit_data: Dict) -> bool:
        """Log trade exit with comprehensive analysis"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Get trade entry details
                cursor.execute('''
                    SELECT entry_time, entry_price, quantity, side FROM trades 
                    WHERE trade_id = ?
                ''', (trade_id,))
                
                result = cursor.fetchone()
                if not result:
                    logging.error(f"[TRADE_LOG] Trade {trade_id} not found for exit")
                    return False
                
                entry_time, entry_price, quantity, side = result
                
                # Calculate trade performance
                exit_price = exit_data.get('exit_price')
                exit_time = exit_data.get('exit_time', datetime.now().isoformat())
                
                if side.upper() == 'BUY':
                    profit_loss = (exit_price - entry_price) * quantity
                else:  # SELL
                    profit_loss = (entry_price - exit_price) * quantity
                
                profit_pct = (profit_loss / (entry_price * quantity)) * 100
                is_win = profit_loss > 0
                
                # Calculate hold duration
                entry_dt = datetime.fromisoformat(entry_time.replace('Z', '+00:00'))
                exit_dt = datetime.fromisoformat(exit_time.replace('Z', '+00:00')) if 'T' in exit_time else datetime.now()
                duration = exit_dt - entry_dt
                duration_seconds = int(duration.total_seconds())
                
                # Human readable duration
                hours = duration_seconds // 3600
                minutes = (duration_seconds % 3600) // 60
                duration_human = f"{hours}h {minutes}m"
                
                # Update trade record
                cursor.execute('''
                    UPDATE trades SET 
                        status = 'CLOSED',
                        exit_time = ?,
                        exit_price = ?,
                        exit_reason = ?,
                        exit_type = ?,
                        profit_loss = ?,
                        profit_pct = ?,
                        is_win = ?,
                        hold_duration_seconds = ?,
                        hold_duration_human = ?,
                        updated_at = ?
                    WHERE trade_id = ?
                ''', (
                    exit_time,
                    exit_price,
                    exit_data.get('exit_reason', 'Strategy exit'),
                    exit_data.get('exit_type', 'AUTO'),
                    profit_loss,
                    profit_pct,
                    is_win,
                    duration_seconds,
                    duration_human,
                    datetime.now().isoformat(),
                    trade_id
                ))
                
                conn.commit()
                
            logging.info(f"[TRADE_LOG] Trade exit logged: {trade_id} - {'WIN' if is_win else 'LOSS'} ${profit_loss:.2f} ({duration_human})")
            return True
            
        except Exception as e:
            logging.error(f"[TRADE_LOG] Failed to log trade exit: {e}")
            return False
    
    def log_portfolio_snapshot(self, portfolio_data: Dict) -> bool:
        """Log current portfolio state"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT INTO portfolio_snapshots (
                        timestamp, total_balance, available_cash, total_pnl,
                        open_positions, daily_pnl, assets_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    datetime.now().isoformat(),
                    portfolio_data.get('total_balance', 0),
                    portfolio_data.get('available_cash', 0),
                    portfolio_data.get('total_pnl', 0),
                    portfolio_data.get('open_positions', 0),
                    portfolio_data.get('daily_pnl', 0),
                    json.dumps(portfolio_data.get('assets', []))
                ))
                
                conn.commit()
                return True
                
        except Exception as e:
            logging.error(f"[TRADE_LOG] Failed to log portfolio snapshot: {e}")
            return False
    
    def get_open_trades(self) -> List[Dict]:
        """Get all currently open trades"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT * FROM trades WHERE status = 'OPEN'
                    ORDER BY entry_time DESC
                ''')
                
                columns = [desc[0] for desc in cursor.description]
                results = cursor.fetchall()
                
                trades = []
                for row in results:
                    trade = dict(zip(columns, row))
                    
                    # Get latest price update
                    cursor.execute('''
                        SELECT current_price, unrealized_pnl, unrealized_pct 
                        FROM trade_updates 
                        WHERE trade_id = ? 
                        ORDER BY timestamp DESC LIMIT 1
                    ''', (trade['trade_id'],))
                    
                    update = cursor.fetchone()
                    if update:
                        trade['current_price'] = update[0]
                        trade['unrealized_pnl'] = update[1]
                        trade['unrealized_pct'] = update[2]
                    
                    trades.append(trade)
                
                return trades
                
        except Exception as e:
            logging.error(f"[TRADE_LOG] Failed to get open trades: {e}")
            return []
    
    def get_trade_history(self, limit: int = 50, symbol: str = None) -> List[Dict]:
        """Get trade history with optional filtering"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                query = '''
                    SELECT * FROM trades WHERE status = 'CLOSED'
                '''
                params = []
                
                if symbol:
                    query += ' AND symbol = ?'
                    params.append(symbol)
                
                query += ' ORDER BY exit_time DESC LIMIT ?'
                params.append(limit)
                
                cursor.execute(query, params)
                
                columns = [desc[0] for desc in cursor.description]
                results = cursor.fetchall()
                
                return [dict(zip(columns, row)) for row in results]
                
        except Exception as e:
            logging.error(f"[TRADE_LOG] Failed to get trade history: {e}")
            return []
    
    def get_performance_summary(self, days: int = 30) -> Dict:
        """Get comprehensive performance summary"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Get trades from last N days
                since_date = (datetime.now() - timedelta(days=days)).isoformat()
                
                cursor.execute('''
                    SELECT 
                        COUNT(*) as total_trades,
                        SUM(CASE WHEN is_win = 1 THEN 1 ELSE 0 END) as winning_trades,
                        SUM(profit_loss) as total_pnl,
                        AVG(CASE WHEN is_win = 1 THEN profit_loss ELSE NULL END) as avg_win,
                        AVG(CASE WHEN is_win = 0 THEN profit_loss ELSE NULL END) as avg_loss,
                        MAX(profit_loss) as best_trade,
                        MIN(profit_loss) as worst_trade,
                        AVG(hold_duration_seconds) as avg_hold_time,
                        MAX(max_favorable_excursion) as max_favorable,
                        MIN(max_adverse_excursion) as max_adverse
                    FROM trades 
                    WHERE status = 'CLOSED' AND exit_time >= ?
                ''', (since_date,))
                
                result = cursor.fetchone()
                
                if result and result[0] > 0:  # If we have trades
                    total_trades = result[0]
                    winning_trades = result[1] or 0
                    losing_trades = total_trades - winning_trades
                    win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
                    
                    avg_win = result[3] or 0
                    avg_loss = result[4] or 0
                    profit_factor = abs(avg_win / avg_loss) if avg_loss != 0 else 0
                    
                    return {
                        'total_trades': total_trades,
                        'winning_trades': winning_trades,
                        'losing_trades': losing_trades,
                        'win_rate': win_rate,
                        'total_pnl': result[2] or 0,
                        'avg_win': avg_win,
                        'avg_loss': avg_loss,
                        'profit_factor': profit_factor,
                        'best_trade': result[5] or 0,
                        'worst_trade': result[6] or 0,
                        'avg_hold_time_seconds': result[7] or 0,
                        'max_favorable_excursion': result[8] or 0,
                        'max_adverse_excursion': result[9] or 0,
                        'period_days': days
                    }
                else:
                    return self._empty_performance_summary(days)
                    
        except Exception as e:
            logging.error(f"[TRADE_LOG] Failed to get performance summary: {e}")
            return self._empty_performance_summary(days)
    
    def _empty_performance_summary(self, days: int) -> Dict:
        """Return empty performance summary"""
        return {
            'total_trades': 0,
            'winning_trades': 0,
            'losing_trades': 0,
            'win_rate': 0,
            'total_pnl': 0,
            'avg_win': 0,
            'avg_loss': 0,
            'profit_factor': 0,
            'best_trade': 0,
            'worst_trade': 0,
            'avg_hold_time_seconds': 0,
            'max_favorable_excursion': 0,
            'max_adverse_excursion': 0,
            'period_days': days
        }
    
    def get_portfolio_history(self, days: int = 7) -> List[Dict]:
        """Get portfolio balance history"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                since_date = (datetime.now() - timedelta(days=days)).isoformat()
                
                cursor.execute('''
                    SELECT * FROM portfolio_snapshots 
                    WHERE timestamp >= ?
                    ORDER BY timestamp ASC
                ''', (since_date,))
                
                columns = [desc[0] for desc in cursor.description]
                results = cursor.fetchall()
                
                return [dict(zip(columns, row)) for row in results]
                
        except Exception as e:
            logging.error(f"[TRADE_LOG] Failed to get portfolio history: {e}")
            return []
    
    def cleanup_old_data(self, days_to_keep: int = 90):
        """Clean up old trade data to manage database size"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cutoff_date = (datetime.now() - timedelta(days=days_to_keep)).isoformat()
                
                # Clean old trade updates (keep recent ones for active analysis)
                cursor.execute('''
                    DELETE FROM trade_updates 
                    WHERE timestamp < ? AND trade_id NOT IN (
                        SELECT trade_id FROM trades WHERE status = 'OPEN'
                    )
                ''', (cutoff_date,))
                
                # Clean old portfolio snapshots (keep one per day)
                cursor.execute('''
                    DELETE FROM portfolio_snapshots 
                    WHERE timestamp < ? AND id NOT IN (
                        SELECT MIN(id) FROM portfolio_snapshots 
                        WHERE timestamp < ?
                        GROUP BY DATE(timestamp)
                    )
                ''', (cutoff_date, cutoff_date))
                
                deleted_updates = cursor.rowcount
                conn.commit()
                
                logging.info(f"[TRADE_LOG] Cleaned up {deleted_updates} old records")
                
        except Exception as e:
            logging.error(f"[TRADE_LOG] Failed to cleanup old data: {e}")
    
    def export_trades_to_csv(self, filepath: str, days: int = 30) -> bool:
        """Export trade data to CSV for external analysis"""
        try:
            trades = self.get_trade_history(limit=1000)
            
            if not trades:
                logging.warning("[TRADE_LOG] No trades to export")
                return False
            
            df = pd.DataFrame(trades)
            df.to_csv(filepath, index=False)
            
            logging.info(f"[TRADE_LOG] Exported {len(trades)} trades to {filepath}")
            return True
            
        except Exception as e:
            logging.error(f"[TRADE_LOG] Failed to export trades: {e}")
            return False
    
    def get_trade_statistics(self) -> Dict:
        """Get comprehensive trade statistics"""
        try:
            summary = self.get_performance_summary(days=365)  # Last year
            
            # Add additional statistics
            summary['avg_hold_time_human'] = self._seconds_to_human(summary['avg_hold_time_seconds'])
            summary['profit_factor_rating'] = self._rate_profit_factor(summary['profit_factor'])
            summary['win_rate_rating'] = self._rate_win_rate(summary['win_rate'])
            
            return summary
            
        except Exception as e:
            logging.error(f"[TRADE_LOG] Failed to get trade statistics: {e}")
            return {}
    
    def _seconds_to_human(self, seconds: float) -> str:
        """Convert seconds to human readable format"""
        if not seconds:
            return "0m"
        
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        
        if hours > 0:
            return f"{hours}h {minutes}m"
        else:
            return f"{minutes}m"
    
    def _rate_profit_factor(self, pf: float) -> str:
        """Rate profit factor performance"""
        if pf >= 2.0:
            return "Excellent"
        elif pf >= 1.5:
            return "Good"
        elif pf >= 1.0:
            return "Break Even"
        else:
            return "Needs Improvement"
    
    def _rate_win_rate(self, wr: float) -> str:
        """Rate win rate performance"""
        if wr >= 70:
            return "Excellent"
        elif wr >= 60:
            return "Good"
        elif wr >= 50:
            return "Average"
        else:
            return "Needs Improvement"