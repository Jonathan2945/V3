#!/usr/bin/env python3
import requests
import time

print("Triggering backtest...")
try:
    response = requests.post('http://localhost:8102/api/backtest/start')
    print(f"Backtest triggered: {response.json()}")
except Exception as e:
    print(f"Note: API endpoint may not be running yet: {e}")
    print("The system will auto-backtest on startup")

time.sleep(2)
print("\nMonitor logs with: tail -f logs/v3_complete_system.log")
