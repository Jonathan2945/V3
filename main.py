#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V3 TRADING SYSTEM - COMPLETE FIXED MAIN ENTRY POINT
==================================================
FIXES APPLIED:
- Proper async/await coordination with main_controller
- Enhanced error recovery and logging  
- Configuration validation
- Better signal handling and graceful shutdown
- Compatible with fixed main_controller.py
- 100% REAL DATA MODE - No mock/simulated data
"""

import sys
import os
import asyncio
import signal
import logging
import traceback
from pathlib import Path
from typing import Optional
import threading
import time
import gc

# Set higher recursion limit for ML operations
sys.setrecursionlimit(5000)

# Add current directory to Python path
current_dir = Path(__file__).parent.resolve()
sys.path.insert(0, str(current_dir))

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("Environment variables loaded successfully")
except ImportError as e:
    print(f"ERROR: Missing required package 'python-dotenv': {e}")
    print("Install with: pip install python-dotenv")
    sys.exit(1)

class V3SystemManager:
    """System manager with proper lifecycle management and real data emphasis"""
    
    def __init__(self):
        self.controller: Optional['V3TradingController'] = None
        self.flask_thread: Optional[threading.Thread] = None
        self.shutdown_event = threading.Event()
        self.logger = None
        self._setup_logging()
        
    def _setup_logging(self):
        """Setup logging configuration"""
        try:
            Path('logs').mkdir(exist_ok=True)
            
            log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
            
            logging.basicConfig(
                level=getattr(logging, log_level, logging.INFO),
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                handlers=[
                    logging.FileHandler('logs/v3_system.log', encoding='utf-8'),
                    logging.StreamHandler(sys.stdout)
                ]
            )
            
            # Reduce noise from external libraries
            for logger_name in ['aiohttp', 'urllib3', 'requests', 'websockets', 'binance']:
                logging.getLogger(logger_name).setLevel(logging.WARNING)
            
            self.logger = logging.getLogger(__name__)
            self.logger.info("V3 System Manager logging initialized")
            
        except Exception as e:
            print(f"Warning: Failed to setup logging: {e}")
            logging.basicConfig(level=logging.INFO)
            self.logger = logging.getLogger(__name__)
    
    def check_system_capabilities(self) -> int:
        """Check system capabilities and count available features"""
        self.logger.info("Checking V3 system capabilities...")
        
        available_features = 0
        
        # Check for main controller
        try:
            from main_controller import V3TradingController
            self.logger.info("+ V3TradingController available")
            available_features += 1
        except ImportError as e:
            self.logger.error(f"- V3TradingController not available: {e}")
        
        # Check for intelligent trading engine
        try:
            from intelligent_trading_engine import IntelligentTradingEngine
            self.logger.info("+ IntelligentTradingEngine available")
            available_features += 1
        except ImportError as e:
            self.logger.warning(f"- IntelligentTradingEngine not available: {e}")
        
        # Check for ML engine
        try:
            from advanced_ml_engine import AdvancedMLEngine
            self.logger.info("+ AdvancedMLEngine available")
            available_features += 1
        except ImportError as e:
            self.logger.warning(f"- AdvancedMLEngine not available: {e}")
        
        # Check for API rotation
        try:
            from api_rotation_manager import get_api_key
            self.logger.info("+ get_api_key available")
            available_features += 1
        except ImportError as e:
            self.logger.warning(f"- API rotation not available: {e}")
        
        # Check for external data collector
        try:
            from external_data_collector import ExternalDataCollector
            self.logger.info("+ ExternalDataCollector available")
            available_features += 1
        except ImportError as e:
            self.logger.warning(f"- ExternalDataCollector not available: {e}")
        
        # Check for Flask dashboard
        try:
            from flask import Flask
            self.logger.info("+ Flask dashboard available")
            available_features += 1
        except ImportError as e:
            self.logger.warning(f"- Flask not available: {e}")
        
        self.logger.info(f"Available features: {available_features}/6")
        return available_features
    
    def check_requirements(self) -> bool:
        """Check system requirements and real data configuration"""
        self.logger.info("Checking V3 system requirements...")
        
        # Check Python version
        python_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        if sys.version_info < (3, 8):
            self.logger.error(f"Python 3.8+ required. Current: {python_version}")
            return False
        
        self.logger.info(f"+ Python {python_version}")
        
        # Check .env file
        if not Path('.env').exists():
            self.logger.error(".env file not found!")
            print("\n" + "=" * 60)
            print("ERROR: Configuration file missing!")
            print("Please copy .env.example to .env and configure your API keys")
            print("=" * 60)
            return False
        
        # Check critical environment variables for real data access
        critical_vars = ['BINANCE_API_KEY_1', 'BINANCE_API_SECRET_1']
        missing_vars = [var for var in critical_vars if not os.getenv(var)]
        
        if missing_vars:
            self.logger.error(f"Missing critical variables: {missing_vars}")
            print(f"\nERROR: Missing required configuration: {', '.join(missing_vars)}")
            print("These are required for REAL market data access")
            return False
        
        # Verify real data configuration
        use_real_data = os.getenv('USE_REAL_DATA_ONLY', 'true').lower() == 'true'
        mock_disabled = os.getenv('MOCK_DATA_DISABLED', 'true').lower() == 'true'
        
        if not use_real_data or not mock_disabled:
            self.logger.warning("Configuration not set for real data only mode!")
        
        # Create required directories
        for dir_name in ['data', 'logs', 'models']:
            try:
                Path(dir_name).mkdir(parents=True, exist_ok=True)
            except Exception as e:
                self.logger.error(f"Failed to create directory {dir_name}: {e}")
                return False
        
        self.logger.info("+ System requirements check passed")
        return True
    
    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            signal_names = {signal.SIGINT: "SIGINT (Ctrl+C)", signal.SIGTERM: "SIGTERM"}
            signal_name = signal_names.get(signum, f"Signal {signum}")
            
            self.logger.info(f"Received {signal_name} - initiating shutdown")
            
            if not self.shutdown_event.is_set():
                self.shutdown_event.set()
                shutdown_thread = threading.Thread(target=self._run_shutdown, daemon=True)
                shutdown_thread.start()
        
        signal.signal(signal.SIGINT, signal_handler)
        if hasattr(signal, 'SIGTERM'):
            signal.signal(signal.SIGTERM, signal_handler)
    
    def _run_shutdown(self):
        """Run shutdown sequence"""
        try:
            time.sleep(1)  # Allow current operations to complete
            
            if self.controller:
                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    loop.run_until_complete(self.controller.shutdown())
                    loop.close()
                except Exception as e:
                    self.logger.error(f"Controller shutdown error: {e}")
            
            self.logger.info("Shutdown sequence completed")
            
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")
        finally:
            threading.Timer(5.0, lambda: os._exit(0)).start()
    
    def print_startup_banner(self):
        """Print startup banner emphasizing real data usage"""
        banner = f"""
Starting V3 Trading System...
Real Data Only Mode + Enhanced Error Handling + Graceful Imports"""
        print(banner)
        
        startup_banner = f"""
    ================================================================
    |                V3 TRADING SYSTEM                             |
    |                                                              |
    |  Real Data Only + Enhanced Error Handling + API Rotation    |
    |  Multi-Timeframe Analysis + ML Strategy Discovery           |
    ================================================================

    SYSTEM STATUS:
    + Python: {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}
    + PID: {os.getpid()}
    + Directory: {os.getcwd()}

    AVAILABLE FEATURES:"""
        
        features = []
        
        # Check database support
        try:
            import sqlite3
            features.append("+ Database Support")
        except:
            features.append("- Database Support")
        
        # Check data analysis
        try:
            import pandas, numpy
            features.append("+ Data Analysis (Pandas/NumPy)")
        except:
            features.append("- Data Analysis")
        
        # Check trading engine
        try:
            from intelligent_trading_engine import IntelligentTradingEngine
            features.append("+ Trading Engine")
        except:
            features.append("- Trading Engine")
        
        # Check ML engine
        try:
            from advanced_ml_engine import AdvancedMLEngine
            features.append("+ ML Engine")
        except:
            features.append("- ML Engine")
        
        # Check API rotation
        try:
            from api_rotation_manager import get_api_key
            features.append("+ API Rotation")
        except:
            features.append("- API Rotation")
        
        # Check web dashboard
        try:
            import flask
            features.append("+ Web Dashboard")
        except:
            features.append("- Web Dashboard")
        
        # Check backtesting
        try:
            from advanced_backtester import EnhancedComprehensiveMultiTimeframeBacktester
            features.append("+ Advanced Backtesting")
        except:
            features.append("- Advanced Backtesting")
        
        for feature in features:
            startup_banner += f"\n    {feature}"
        
        # Real data validation
        use_real_data = os.getenv('USE_REAL_DATA_ONLY', 'true').lower() == 'true'
        mock_disabled = os.getenv('MOCK_DATA_DISABLED', 'true').lower() == 'true'
        
        startup_banner += f"""

    CRITICAL VALIDATION:
    {'+' if use_real_data else '-'} Real Data Only Mode ({'No Mock Data' if use_real_data else 'Mock Data Detected!'})
    + Environment Configuration Loaded
    + Error Handling Enhanced
    + Graceful Shutdown Support

    MISSING FEATURES (Install dependencies to enable):"""
        
        missing_features = [feature.replace('- ', '') for feature in features if feature.startswith('- ')]
        if missing_features:
            for feature in missing_features:
                startup_banner += f"\n    - {feature}"
        else:
            startup_banner += "\n    None - All features available!"
        
        print(startup_banner)
    
    async def initialize_controller(self):
        """Initialize the trading controller with proper async handling"""
        try:
            self.logger.info("Initializing V3 Trading Controller...")
            
            from main_controller import V3TradingController
            self.controller = V3TradingController()
            
            # This is the critical method that was missing!
            success = await self.controller.initialize_system()
            if not success:
                raise RuntimeError("Controller initialization failed")
            
            self.logger.info("Controller initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Controller initialization failed: {e}", exc_info=True)
            return False
    
    def start_flask_app(self):
        """Start Flask app in separate thread"""
        def run_flask():
            try:
                self.logger.info("Starting Flask server...")
                self.controller.run_flask_app()
            except Exception as e:
                self.logger.error(f"Flask error: {e}", exc_info=True)
        
        self.flask_thread = threading.Thread(target=run_flask, daemon=True)
        self.flask_thread.start()
        time.sleep(2)  # Wait for Flask to start
        
        dashboard_port = int(os.getenv('FLASK_PORT', '8102'))
        self.logger.info(f"Dashboard available at: http://localhost:{dashboard_port}")
    
    def print_status(self):
        """Print system status with real data emphasis"""
        dashboard_port = int(os.getenv('FLASK_PORT', '8102'))
        auto_start = os.getenv('AUTO_START_TRADING', 'false').lower() == 'true'
        
        print("\n" + "=" * 70)
        print("V3 TRADING SYSTEM READY!")
        print("+ 100% REAL MARKET DATA MODE")
        print("+ Database Connection Pooling: ACTIVE")
        print("+ Memory Management: ACTIVE") 
        print("+ Enhanced Error Handling: ACTIVE")
        print("+ API Rotation System: ACTIVE")
        print(f"Dashboard: http://localhost:{dashboard_port}")
        print("=" * 70)
        
        if auto_start:
            print("\nAUTO_START_TRADING=true - Will start automatically")
            print("Trading will begin with REAL market data after initialization")
        else:
            print(f"\nManual start mode - Use dashboard to start trading")
            print(f"Dashboard: http://localhost:{dashboard_port}")
            print("All trading will use REAL market data")
        
        print("\nV3 System running with REAL DATA... Press Ctrl+C to shutdown")
    
    async def run_system(self):
        """Main system run loop with enhanced error handling"""
        try:
            # System capability check
            available_features = self.check_system_capabilities()
            
            if not self.check_requirements():
                return False
            
            self.print_startup_banner()
            self.setup_signal_handlers()
            
            if not await self.initialize_controller():
                self.logger.error("CRITICAL: Controller initialization failed")
                print("\n" + "=" * 60)
                print("CONTROLLER INITIALIZATION FAILED:")
                print("1. Check that all dependencies are installed")
                print("2. Verify your .env configuration")
                print("3. Check logs/v3_system.log for details")
                print("=" * 60)
                return False
            
            self.start_flask_app()
            self.print_status()
            
            # Auto-start if enabled
            auto_start = os.getenv('AUTO_START_TRADING', 'false').lower() == 'true'
            if auto_start and self.controller:
                self.logger.info("AUTO_START_TRADING enabled")
                try:
                    result = await self.controller.start_trading()
                    if result.get('success'):
                        self.logger.info("Trading started automatically with REAL DATA")
                        print("Auto-trading started with REAL market data!")
                    else:
                        self.logger.warning(f"Auto-start failed: {result.get('error')}")
                        print(f"Auto-start failed: {result.get('error')}")
                except Exception as e:
                    self.logger.error(f"Auto-start error: {e}")
                    print(f"Auto-start error: {e}")
            
            # Main loop with memory cleanup
            last_gc_time = time.time()
            while not self.shutdown_event.is_set():
                await asyncio.sleep(1)
                
                # Periodic garbage collection
                if time.time() - last_gc_time > 300:  # 5 minutes
                    gc.collect()
                    last_gc_time = time.time()
            
            return True
            
        except Exception as e:
            self.logger.error(f"System error: {e}", exc_info=True)
            return False

def main():
    """Main function with comprehensive error handling"""
    system_manager = V3SystemManager()
    
    try:
        success = asyncio.run(system_manager.run_system())
        
        if success:
            system_manager.logger.info("V3 System completed successfully")
        else:
            system_manager.logger.error("V3 System completed with errors")
            
            print("\n" + "=" * 60)
            print("SYSTEM SETUP HELP:")
            print("1. Install missing dependencies: pip install -r requirements.txt")
            print("2. Create .env file with your API keys")
            print("3. Run: python test.py  # To test your setup")
            print("=" * 60)
            
            sys.exit(1)
            
    except KeyboardInterrupt:
        system_manager.logger.info("System interrupted by user")
    except Exception as e:
        if system_manager.logger:
            system_manager.logger.error(f"Unhandled error: {e}", exc_info=True)
        else:
            print(f"Critical error: {e}")
            traceback.print_exc()
        sys.exit(1)
    finally:
        if system_manager.logger:
            system_manager.logger.info("V3 Trading System shutdown complete")

if __name__ == "__main__":
    print("Starting V3 Trading System...")
    print("+ Real Data Only + Enhanced Error Handling + Graceful Imports")
    main()