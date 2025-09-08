#!/usr/bin/env python3
"""
Diagnose what's actually happening when trading system runs
"""

import os
import logging
from dotenv import load_dotenv

load_dotenv()

print("=== TRADING SYSTEM DIAGNOSIS ===")
print(f"TESTNET setting: {os.getenv('TESTNET')}")
print(f"LIVE_TRADING_ENABLED: {os.getenv('LIVE_TRADING_ENABLED')}")
print(f"ALLOW_LIVE_TRADING: {os.getenv('ALLOW_LIVE_TRADING')}")
print(f"REQUIRE_CONFIRMATION_LIVE: {os.getenv('REQUIRE_CONFIRMATION_LIVE')}")

print("\n=== TEST 1: Exchange Manager Import ===")
try:
    from binance_exchange_manager import exchange_manager
    balance = exchange_manager.get_account_balance('USDT')
    print(f"SUCCESS: Real balance ${balance:.2f}")
except Exception as e:
    print(f"ERROR: {e}")

print("\n=== TEST 2: Direct Trade Test ===")
try:
    from binance_exchange_manager import exchange_manager
    print("Attempting REAL $0.50 BTC buy test...")
    
    # Calculate tiny position for test
    from binance_exchange_manager import calculate_position_size
    position_size = calculate_position_size('BTCUSDT', 0.5)  # $0.50 test
    
    print(f"Position size: {position_size:.8f} BTC")
    print("READY TO PLACE REAL TRADE - Type 'YES' to proceed:")
    
    # Don't actually place trade without confirmation
    print("(Trade not placed - manual confirmation required)")
    
except Exception as e:
    print(f"ERROR: {e}")

print("\n=== TEST 3: Check for Mock/Simulation Modes ===")

# Search for simulation flags
simulation_flags = [
    'MOCK_DATA_DISABLED',
    'TEST_MODE_DISABLED', 
    'ENABLE_MOCK_APIS',
    'REALISTIC_SIMULATION',
    'USE_REAL_DATA_ONLY',
    'SIMULATION_MODE',
    'PAPER_TRADING',
    'DRY_RUN'
]

for flag in simulation_flags:
    value = os.getenv(flag)
    if value is not None:
        print(f"{flag}: {value}")

print("\n=== TEST 4: Check Main System Components ===")

# Check what main.py actually imports and runs
print("Checking main.py imports...")
try:
    import sys
    sys.path.append('.')
    
    # Try importing main components
    import main
    print("main.py imported successfully")
    
    # Check if main has any simulation modes
    if hasattr(main, 'V3SystemManager'):
        print("Found V3SystemManager")
    
except Exception as e:
    print(f"Main import error: {e}")

print("\n=== TEST 5: Grep for Simulation Code ===")
import subprocess
import os

# Search for simulation-related code
search_terms = ['simulation', 'mock', 'fake', 'paper_trading', 'dry_run', 'test_mode']

for term in search_terms:
    try:
        result = subprocess.run(['grep', '-r', '-i', term, '.', '--include=*.py'], 
                              capture_output=True, text=True, cwd='/root/2')
        if result.stdout:
            lines = result.stdout.split('\n')[:5]  # First 5 matches
            print(f"\nFound '{term}' in:")
            for line in lines:
                if line.strip():
                    print(f"  {line}")
    except:
        pass

print("\n=== DIAGNOSIS COMPLETE ===")
print("Run this diagnosis to find where simulation is happening.")
