#!/usr/bin/env python3
"""
Trace exactly what happens when main.py runs
"""

import logging
import sys
import os
from dotenv import load_dotenv

# Set up detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('trading_trace.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Monitor environment
load_dotenv()
print("=== ENVIRONMENT CHECK ===")
print(f"TESTNET: {os.getenv('TESTNET')}")
print(f"DEFAULT_TRADING_MODE: {os.getenv('DEFAULT_TRADING_MODE')}")
print(f"TRADE_AMOUNT_USDT: {os.getenv('TRADE_AMOUNT_USDT')}")
print(f"LIVE_TRADING_ENABLED: {os.getenv('LIVE_TRADING_ENABLED')}")

# Monkey patch to trace trade executions
original_place_order = None

def trace_trade_execution():
    """Patch all possible trade execution points"""
    try:
        from binance_exchange_manager import exchange_manager
        global original_place_order
        original_place_order = exchange_manager.place_market_order
        
        def traced_place_order(symbol, side, quantity):
            print(f"\n🚨 REAL TRADE ATTEMPT DETECTED 🚨")
            print(f"Symbol: {symbol}, Side: {side}, Quantity: {quantity}")
            print(f"Estimated value: ${quantity * 100000:.2f}")  # Rough estimate
            
            # Call original function
            result = original_place_order(symbol, side, quantity)
            
            if result:
                print(f"✅ TRADE EXECUTED: {result.get('orderId', 'Unknown')}")
            else:
                print(f"❌ TRADE FAILED")
            
            return result
        
        exchange_manager.place_market_order = traced_place_order
        print("✅ Trade execution tracing enabled")
        
    except Exception as e:
        print(f"❌ Failed to set up trade tracing: {e}")

# Start tracing
trace_trade_execution()

print("\n=== STARTING MAIN SYSTEM WITH TRACING ===")
print("This will show EVERY trade attempt...")

# Import and run main
try:
    import main
    main.main()
except KeyboardInterrupt:
    print("\n=== SYSTEM INTERRUPTED ===")
except Exception as e:
    print(f"\n=== SYSTEM ERROR: {e} ===")
    import traceback
    traceback.print_exc()
