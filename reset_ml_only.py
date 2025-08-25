#!/usr/bin/env python3
"""Clear only ML learning data, preserve real trades"""

import sqlite3
from datetime import datetime

def clear_ml_learning_data():
    print("?? Clearing ML learning data from trading_metrics.db...")
    
    conn = sqlite3.connect('data/trading_metrics.db')
    cursor = conn.cursor()
    
    try:
        # Show current state
        print("\n?? BEFORE CLEARING:")
        cursor.execute("SELECT COUNT(*) FROM trade_history")
        total_before = cursor.fetchone()[0]
        print(f"   Total trades: {total_before}")
        
        cursor.execute("SELECT source, COUNT(*) FROM trade_history GROUP BY source")
        sources = cursor.fetchall()
        for source, count in sources:
            print(f"   {source}: {count} trades")
        
        # Clear ML/simulation trades (keep real trades if any)
        ml_sources = [
            '%SIMULATION%', '%V1_PROVEN%', '%V3_HYBRID%', 
            '%BACKTEST%', '%TESTNET%', '%MOCK%', '%PAPER%'
        ]
        
        deleted_total = 0
        for source_pattern in ml_sources:
            cursor.execute("SELECT COUNT(*) FROM trade_history WHERE source LIKE ?", (source_pattern,))
            count = cursor.fetchone()[0]
            if count > 0:
                cursor.execute("DELETE FROM trade_history WHERE source LIKE ?", (source_pattern,))
                deleted_total += count
                print(f"   ? Deleted {count} trades from {source_pattern}")
        
        # Reset system metrics to start fresh
        cursor.execute("""
            UPDATE system_metrics SET 
                total_trades = 0,
                winning_trades = 0,
                total_pnl = 0.0,
                win_rate = 0.0,
                active_positions = 0,
                last_updated = ?
        """, (datetime.now().isoformat(),))
        
        conn.commit()
        
        # Show final state
        print("\n?? AFTER CLEARING:")
        cursor.execute("SELECT COUNT(*) FROM trade_history")
        total_after = cursor.fetchone()[0]
        print(f"   Total trades remaining: {total_after}")
        print(f"   Deleted {deleted_total} ML training trades")
        
        cursor.execute("SELECT * FROM system_metrics")
        metrics = cursor.fetchone()
        if metrics:
            print(f"   System metrics reset: {metrics}")
        
        print("? ML learning data cleared successfully!")
        
    except Exception as e:
        print(f"? Error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    clear_ml_learning_data()