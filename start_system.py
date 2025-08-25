#!/usr/bin/env python3
"""
Enhanced Trading System Startup Script
====================================
Safely starts the trading system with proper error handling
Uses configuration from .env file
"""

import os
import sys
import time
import signal
import subprocess
from pathlib import Path

def check_requirements():
    """Check if system requirements are met"""
    print("Checking system requirements...")
    
    # Check Python version
    if sys.version_info < (3, 8):
        print(f"✗ Python 3.8+ required (current: {sys.version})")
        return False
    
    # Check .env file
    if not Path('.env').exists():
        print("✗ .env file not found")
        print("  Please copy .env.template to .env and configure")
        return False
    
    # Check required directories
    required_dirs = ['data', 'logs', 'backups', 'models']
    for dir_name in required_dirs:
        Path(dir_name).mkdir(exist_ok=True)
    
    # Load and display configuration
    try:
        from config_reader import get_config
        port = get_config('port', 8107)
        trading_mode = get_config('trading_mode', 'PAPER_TRADING')
        testnet_enabled = get_config('testnet_enabled', True)
        
        print("✓ System requirements met")
        print(f"✓ Configuration loaded from .env")
        print(f"  - Port: {port}")
        print(f"  - Trading Mode: {trading_mode}")
        print(f"  - Testnet: {'Enabled' if testnet_enabled else 'Disabled'}")
    except ImportError:
        # Fallback if config_reader not available
        print("✓ System requirements met")
        print("✓ Using fallback configuration")
    
    return True

def start_system():
    """Start the trading system"""
    if not check_requirements():
        return False
    
    # Get port from configuration
    try:
        from config_reader import get_config
        port = get_config('port', 8107)
    except ImportError:
        import os
        port = int(os.getenv('MAIN_SYSTEM_PORT', os.getenv('FLASK_PORT', '8107')))
    
    print("\nStarting Price Action Trading System...")
    print(f"Dashboard will be available at: http://localhost:{port}")
    print("\nPress Ctrl+C to shutdown gracefully\n")
    
    try:
        # Start main.py
        process = subprocess.Popen([sys.executable, 'main.py'])
        
        # Wait for process
        process.wait()
        
    except KeyboardInterrupt:
        print("\n\nShutdown requested...")
        if process.poll() is None:
            print("Stopping system gracefully...")
            process.terminate()
            
            # Wait for graceful shutdown
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                print("Force stopping system...")
                process.kill()
        
        print("System stopped")
        return True
        
    except Exception as e:
        print(f"\nError starting system: {e}")
        return False

if __name__ == "__main__":
    start_system()
