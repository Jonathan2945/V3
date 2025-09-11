#!/usr/bin/env python3
"""
Test buttons with proper JSON headers
"""
import requests
import json

def test_buttons_with_headers():
    base_url = "http://localhost:8102"
    headers = {'Content-Type': 'application/json'}
    
    print("Testing buttons with proper headers...")
    
    # Test start backtest
    print("\n1. Testing START BACKTEST with headers...")
    try:
        response = requests.post(f"{base_url}/api/control/start_backtest", 
                               headers=headers, 
                               json={})
        print(f"   Response code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"   Response: {json.dumps(data, indent=2)}")
        else:
            print(f"   Error: {response.text}")
    except Exception as e:
        print(f"   Error: {e}")
    
    # Test start trading
    print("\n2. Testing START TRADING with headers...")
    try:
        response = requests.post(f"{base_url}/api/control/start_trading", 
                               headers=headers, 
                               json={})
        print(f"   Response code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"   Response: {json.dumps(data, indent=2)}")
        else:
            print(f"   Error: {response.text}")
    except Exception as e:
        print(f"   Error: {e}")

if __name__ == "__main__":
    test_buttons_with_headers()
