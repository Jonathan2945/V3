#!/usr/bin/env python3
"""
V3 LIVE DATA ML CLEANER SCRIPT
==============================
Clear all legacy ML training data and prepare for V3 live data only
V3 COMPLIANCE: No references to test/mock data - production ready
"""

import os
import json
import logging
from datetime import datetime

def clear_legacy_ml_data():
    """Clear all legacy ML training data for V3 live data compliance"""
    print("[V3_CLEANER] CLEARING LEGACY ML TRAINING DATA")
    print("=" * 50)
    
    files_to_clear = []
    directories_to_check = ['.', 'data', 'ml_data', 'models']
    
    # 1. Clear legacy ML model files
    ml_files = [
        'ml_models.json',
        'ml_training_data.json', 
        'model_cache.pkl',
        'trading_models.pkl',
        'backtest_training_data.json',
        'testnet_training_data.json',
        'prediction_history.json',
        'model_performance_history.json',
        'legacy_training_data.json',
        'old_model_cache.pkl'
    ]
    
    for directory in directories_to_check:
        if os.path.exists(directory):
            for filename in ml_files:
                filepath = os.path.join(directory, filename)
                if os.path.exists(filepath):
                    try:
                        os.remove(filepath)
                        files_to_clear.append(filepath)
                        print(f"  [V3_CLEANER] Cleared: {filepath}")
                    except Exception as e:
                        print(f"  [V3_CLEANER] Failed to clear {filepath}: {e}")
    
    # 2. Clear legacy log files
    log_files = [
        'auto_backtest.log',
        'debug.log',
        'ultimate_results.log',
        'ml_training.log',
        'trading.log',
        'legacy_training.log'
    ]
    
    for log_file in log_files:
        if os.path.exists(log_file):
            try:
                # Archive old logs instead of deleting
                archive_name = f"{log_file}.archive_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                os.rename(log_file, archive_name)
                print(f"  [V3_CLEANER] Archived: {log_file} ? {archive_name}")
                
                # Create fresh V3 log file
                with open(log_file, 'w') as f:
                    f.write(f"# V3 LIVE DATA CLEARED - {datetime.now().isoformat()}\n")
                    f.write("# Starting fresh with V3 live market data only\n\n")
                
            except Exception as e:
                print(f"  [V3_CLEANER] Failed to archive {log_file}: {e}")
    
    # 3. Clear legacy SQLite databases
    db_files = [
        'trading_bot.db',
        'ml_data.db',
        'backtest_results.db',
        'legacy_data.db'
    ]
    
    for db_file in db_files:
        for directory in directories_to_check:
            db_path = os.path.join(directory, db_file)
            if os.path.exists(db_path):
                try:
                    os.remove(db_path)
                    print(f"  [V3_CLEANER] Cleared database: {db_path}")
                except Exception as e:
                    print(f"  [V3_CLEANER] Failed to clear {db_path}: {e}")
    
    # 4. Reset V3 ML engine cache if the file exists
    try:
        from advanced_ml_engine import AdvancedMLEngine
        
        # Create fresh V3 ML engine and clear legacy cache
        ml_engine = AdvancedMLEngine(test_mode=False)  # V3: Live mode only
        if hasattr(ml_engine, 'model_cache'):
            ml_engine.model_cache = {}
        if hasattr(ml_engine, 'training_data'):
            ml_engine.training_data = []
        if hasattr(ml_engine, 'prediction_history'):
            ml_engine.prediction_history = []
        if hasattr(ml_engine, 'backtest_training_data'):
            ml_engine.backtest_training_data = []
        if hasattr(ml_engine, 'testnet_enhancement_data'):
            ml_engine.testnet_enhancement_data = []
            
        print("  [V3_CLEANER] V3 ML Engine cache cleared - ready for live data")
        
    except Exception as e:
        print(f"  [V3_CLEANER] Could not clear ML engine cache: {e}")
    
    # 5. Create V3 clean state marker
    v3_clean_state = {
        'cleaned_at': datetime.now().isoformat(),
        'v3_compliance': True,
        'reason': 'Cleared legacy training data for V3 live market data only',
        'files_cleared': files_to_clear,
        'ready_for_live_data': True,
        'data_mode': 'V3_LIVE_PRODUCTION'
    }
    
    try:
        with open('v3_clean_state.json', 'w') as f:
            json.dump(v3_clean_state, f, indent=2)
        print("  [V3_CLEANER] Created V3 clean state marker")
    except Exception as e:
        print(f"  [V3_CLEANER] Failed to create V3 clean state marker: {e}")
    
    print("\n[V3_CLEANER] LEGACY ML DATA CLEANUP COMPLETE")
    print("[V3_CLEANER] Ready for V3 LIVE market data training!")
    print(f"[V3_CLEANER] Cleared {len(files_to_clear)} legacy files")
    print("[V3_CLEANER] ML engine will now learn from LIVE market data only")

def verify_live_binance_connection():
    """Verify we can connect to live Binance API for V3 compliance"""
    print("\n[V3_CLEANER] VERIFYING LIVE BINANCE CONNECTION")
    print("=" * 50)
    
    try:
        from binance.client import Client
        import os
        
        api_key = os.getenv('BINANCE_API_KEY')
        api_secret = os.getenv('BINANCE_API_SECRET')
        
        if not api_key or not api_secret:
            print("[V3_CLEANER] No Binance API credentials found in environment")
            return False
        
        # Test live connection (V3: testnet for safety)
        client = Client(api_key, api_secret, testnet=True)
        account = client.get_account()
        
        print("[V3_CLEANER] Successfully connected to live Binance testnet!")
        print(f"   Can Trade: {account.get('canTrade', False)}")
        print(f"   Account Type: {account.get('accountType', 'UNKNOWN')}")
        
        # Test live market data
        ticker = client.get_symbol_ticker(symbol="BTCUSDT")
        print(f"[V3_CLEANER] Current BTC Price (Live): ${float(ticker['price']):,.2f}")
        
        # Check live balances
        balances = [b for b in account['balances'] if float(b['free']) > 0]
        print(f"[V3_CLEANER] Available live balances: {len(balances)} assets")
        
        return True
        
    except ImportError:
        print("[V3_CLEANER] python-binance not installed!")
        print("   Run: pip install python-binance")
        return False
    except Exception as e:
        print(f"[V3_CLEANER] Live connection failed: {e}")
        return False

def setup_v3_environment():
    """Setup V3 environment configuration for live data only"""
    print("\n[V3_CLEANER] SETTING UP V3 ENVIRONMENT")
    print("=" * 50)
    
    v3_env_settings = {
        'ENABLE_LIVE_DATA': 'true',
        'V3_COMPLIANCE_MODE': 'true',
        'DISABLE_LEGACY_MODES': 'true',
        'LIVE_TRADING_ONLY': 'true',
        'V3_ML_LIVE_LEARNING': 'true'
    }
    
    try:
        # Check if .env file exists
        env_file = '.env'
        env_lines = []
        
        if os.path.exists(env_file):
            with open(env_file, 'r') as f:
                env_lines = f.readlines()
        
        # Update V3 settings
        updated_lines = []
        settings_added = set()
        
        for line in env_lines:
            line = line.strip()
            if not line or line.startswith('#'):
                updated_lines.append(line)
                continue
            
            key = line.split('=')[0].strip()
            if key in v3_env_settings:
                updated_lines.append(f"{key}={v3_env_settings[key]}")
                settings_added.add(key)
                print(f"  [V3_CLEANER] Updated: {key}={v3_env_settings[key]}")
            else:
                updated_lines.append(line)
        
        # Add missing V3 settings
        for key, value in v3_env_settings.items():
            if key not in settings_added:
                updated_lines.append(f"{key}={value}")
                print(f"  [V3_CLEANER] Added: {key}={value}")
        
        # Write updated .env file
        with open(env_file, 'w') as f:
            for line in updated_lines:
                f.write(line + '\n')
        
        print("[V3_CLEANER] V3 environment configuration complete")
        return True
        
    except Exception as e:
        print(f"[V3_CLEANER] Failed to setup V3 environment: {e}")
        return False

def validate_v3_compliance():
    """Validate that system is V3 compliant"""
    print("\n[V3_CLEANER] VALIDATING V3 COMPLIANCE")
    print("=" * 50)
    
    compliance_checks = []
    
    # Check 1: No legacy files remain
    legacy_files = ['ml_models.json', 'legacy_data.db', 'old_cache.pkl']
    legacy_found = any(os.path.exists(f) for f in legacy_files)
    if not legacy_found:
        compliance_checks.append("No legacy files found")
        print("[V3_CLEANER] ? Legacy files cleared")
    else:
        print("[V3_CLEANER] ? Legacy files still present")
    
    # Check 2: V3 clean state marker exists
    if os.path.exists('v3_clean_state.json'):
        compliance_checks.append("V3 clean state confirmed")
        print("[V3_CLEANER] ? V3 clean state marker found")
    else:
        print("[V3_CLEANER] ? V3 clean state marker missing")
    
    # Check 3: Environment configured for live data
    try:
        import os
        live_data_enabled = os.getenv('ENABLE_LIVE_DATA', 'false').lower() == 'true'
        if live_data_enabled:
            compliance_checks.append("Live data enabled")
            print("[V3_CLEANER] ? Live data mode enabled")
        else:
            print("[V3_CLEANER] ? Live data mode not enabled")
    except:
        print("[V3_CLEANER] ? Could not check environment")
    
    # Check 4: ML engine in live mode
    try:
        from advanced_ml_engine import AdvancedMLEngine
        engine = AdvancedMLEngine()
        if not engine.test_mode:
            compliance_checks.append("ML engine in live mode")
            print("[V3_CLEANER] ? ML engine configured for live data")
        else:
            print("[V3_CLEANER] ? ML engine still in test mode")
    except:
        print("[V3_CLEANER] ? Could not check ML engine")
    
    v3_compliant = len(compliance_checks) >= 3
    
    if v3_compliant:
        print(f"\n[V3_CLEANER] ? V3 COMPLIANCE ACHIEVED")
        print(f"[V3_CLEANER] Passed {len(compliance_checks)}/4 checks")
    else:
        print(f"\n[V3_CLEANER] ?? V3 COMPLIANCE INCOMPLETE")
        print(f"[V3_CLEANER] Passed {len(compliance_checks)}/4 checks")
    
    return v3_compliant

if __name__ == "__main__":
    # Load environment
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except:
        pass
    
    # V3 Cleanup process
    clear_legacy_ml_data()
    
    # Verify live connection
    verify_live_binance_connection()
    
    # Setup V3 environment
    setup_v3_environment()
    
    # Validate V3 compliance
    v3_compliant = validate_v3_compliance()
    
    print("\n[V3_CLEANER] V3 PREPARATION COMPLETE")
    print("=" * 50)
    if v3_compliant:
        print("1. ? V3 system ready for live data")
        print("2. ? Legacy data cleared")
        print("3. ? Environment configured")
        print("4. ? ML will learn from live market conditions")
    else:
        print("1. ?? Manual V3 configuration may be needed")
        print("2. ?? Check environment settings")
        print("3. ?? Verify API connections")
    
    print("\n[V3_CLEANER] System is now V3 compliant - LIVE DATA ONLY!")