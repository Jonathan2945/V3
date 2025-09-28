#!/usr/bin/env python3
import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from config import settings
    
    def test_api_connections():
        print("Testing API connections...")
        configs = settings.get_api_configs()
        for api_name, config in configs.items():
            print(f"{api_name}: {config['status']}")
    
    if __name__ == "__main__":
        test_api_connections()
        
except ImportError as e:
    print(f"Import error: {e}")
    print("Make sure you're in the correct directory with config.py")
except Exception as e:
    print(f"Error: {e}")
