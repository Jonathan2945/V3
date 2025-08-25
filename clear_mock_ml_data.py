#!/usr/bin/env python3
"""
CLEAR MOCK ML DATA SCRIPT
========================
Clear all ML training data from mock/random runs
"""

import os
import json
import logging
from datetime import datetime

def clear_mock_ml_data():
    """Clear all ML training data from mock runs"""
    print("🧹 CLEARING MOCK ML TRAINING DATA")
    print("=" * 50)
    
    files_to_clear = []
    directories_to_check = ['.', 'data', 'ml_data', 'models']
    
    # 1. Clear ML model files
    ml_files = [
        'ml_models.json',
        'ml_training_data.json', 
        'model_cache.pkl',
        'trading_models.pkl',
        'backtest_training_data.json',
        'testnet_training_data.json',
        'prediction_history.json',
        'model_performance_history.json'
    ]
    
    for directory in directories_to_check:
        if os.path.exists(directory):
            for filename in ml_files:
                filepath = os.path.join(directory, filename)
                if os.path.exists(filepath):
                    try:
                        os.remove(filepath)
                        files_to_clear.append(filepath)
                        print(f"  ✅ Cleared: {filepath}")
                    except Exception as e:
                        print(f"  ❌ Failed to clear {filepath}: {e}")
    
    # 2. Clear log files with mock data
    log_files = [
        'auto_backtest.log',
        'test_debug.log',
        'ultimate_test_results.log',
        'ml_training.log',
        'trading.log'
    ]
    
    for log_file in log_files:
        if os.path.exists(log_file):
            try:
                # Archive old logs instead of deleting
                archive_name = f"{log_file}.archive_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                os.rename(log_file, archive_name)
                print(f"  📁 Archived: {log_file} → {archive_name}")
                
                # Create fresh log file
                with open(log_file, 'w') as f:
                    f.write(f"# CLEARED MOCK DATA - {datetime.now().isoformat()}\n")
                    f.write("# Starting fresh with REAL Binance testnet data\n\n")
                
            except Exception as e:
                print(f"  ❌ Failed to archive {log_file}: {e}")
    
    # 3. Clear any SQLite databases with mock data
    db_files = [
        'trading_bot.db',
        'ml_data.db',
        'backtest_results.db'
    ]
    
    for db_file in db_files:
        for directory in directories_to_check:
            db_path = os.path.join(directory, db_file)
            if os.path.exists(db_path):
                try:
                    os.remove(db_path)
                    print(f"  🗄️ Cleared database: {db_path}")
                except Exception as e:
                    print(f"  ❌ Failed to clear {db_path}: {e}")
    
    # 4. Reset ML engine cache if the file exists
    try:
        from advanced_ml_engine import AdvancedMLEngine
        
        # Create fresh ML engine and clear cache
        ml_engine = AdvancedMLEngine()
        if hasattr(ml_engine, 'model_cache'):
            ml_engine.model_cache = {}
        if hasattr(ml_engine, 'training_data'):
            ml_engine.training_data = []
        if hasattr(ml_engine, 'prediction_history'):
            ml_engine.prediction_history = []
        if hasattr(ml_engine, 'backtest_training_data'):
            ml_engine.backtest_training_data = []
        if hasattr(ml_engine, 'testnet_training_data'):
            ml_engine.testnet_training_data = []
            
        print("  🧠 ML Engine cache cleared")
        
    except Exception as e:
        print(f"  ⚠️ Could not clear ML engine cache: {e}")
    
    # 5. Create clean state marker
    clean_state = {
        'cleaned_at': datetime.now().isoformat(),
        'reason': 'Cleared mock training data for real testnet connection',
        'files_cleared': files_to_clear,
        'ready_for_real_data': True
    }
    
    try:
        with open('ml_clean_state.json', 'w') as f:
            json.dump(clean_state, f, indent=2)
        print("  📝 Created clean state marker")
    except Exception as e:
        print(f"  ❌ Failed to create clean state marker: {e}")
    
    print("\n✅ MOCK ML DATA CLEANUP COMPLETE")
    print("🚀 Ready for REAL Binance testnet training!")
    print(f"📊 Cleared {len(files_to_clear)} files")
    print("🧠 ML engine will now learn from REAL market data")

def verify_real_connection():
    """Verify we can connect to real Binance testnet"""
    print("\n🔍 VERIFYING REAL BINANCE TESTNET CONNECTION")
    print("=" * 50)
    
    try:
        from binance.client import Client
        import os
        
        api_key = os.getenv('BINANCE_API_KEY')
        api_secret = os.getenv('BINANCE_API_SECRET')
        
        if not api_key or not api_secret:
            print("❌ No Binance API credentials found in environment")
            return False
        
        # Test connection
        client = Client(api_key, api_secret, testnet=True)
        account = client.get_account()
        
        print("✅ Successfully connected to Binance testnet!")
        print(f"   Can Trade: {account.get('canTrade', False)}")
        print(f"   Account Type: {account.get('accountType', 'UNKNOWN')}")
        
        # Test market data
        ticker = client.get_symbol_ticker(symbol="BTCUSDT")
        print(f"📈 Current BTC Price: ${float(ticker['price']):,.2f}")
        
        # Check balances
        balances = [b for b in account['balances'] if float(b['free']) > 0]
        print(f"💰 Available balances: {len(balances)} assets")
        
        return True
        
    except ImportError:
        print("❌ python-binance not installed!")
        print("   Run: pip install python-binance")
        return False
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

if __name__ == "__main__":
    # Load environment
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except:
        pass
    
    # Clear mock data
    clear_mock_ml_data()
    
    # Verify real connection
    verify_real_connection()
    
    print("\n🎯 NEXT STEPS:")
    print("1. Update your .env file: ENABLE_MOCK_APIS=false")
    print("2. Install real client: pip install python-binance")
    print("3. Run your system - it will now use REAL testnet data!")
    print("4. ML will learn from actual market conditions 📊")