#!/usr/bin/env python3
import sqlite3

print("Checking strategy data and scanner logic...\n")

# Check strategies
conn = sqlite3.connect('data/comprehensive_backtest.db')
cursor = conn.cursor()

cursor.execute("""
    SELECT symbol, strategy_type, win_rate, sharpe_ratio 
    FROM historical_backtests 
    WHERE total_trades >= 20 AND sharpe_ratio > 0.3
    ORDER BY sharpe_ratio DESC LIMIT 5
""")

print("Top 5 strategies in database:")
for row in cursor.fetchall():
    win_rate_pct = row[2] * 100 if row[2] < 1 else row[2]
    print(f"  {row[0]:10s} {row[1]:12s} WR: {win_rate_pct:5.1f}% Sharpe: {row[3]:.2f}")

conn.close()

# Check MIN_CONFIDENCE
import os
min_conf = os.getenv('MIN_CONFIDENCE', '45.0')
print(f"\nMIN_CONFIDENCE from .env: {min_conf}")

# Show what confidence would be needed
print(f"\nFor a trade to execute:")
print(f"  - Calculated confidence must be > {min_conf}%")
print(f"  - Scanner must find opportunities > 0")
print(f"  - _should_execute_trades() must return True")

print("\nThe scanner is running but returning 0 opportunities.")
print("This means _analyze_pair_with_strategies() is returning confidence < 45%")
print("\nPossible causes:")
print("  1. ml_trained_strategies has strategies but confidence calc is wrong")
print("  2. The strategies are loaded but not being used properly")
print("  3. Market conditions don't meet any strategy criteria")
