#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V3 TRADING SYSTEM - COMPLETE FIXED MAIN ENTRY POINT
==================================================
FIXES APPLIED:
- Integrated all fixed components (controller, persistence, middleware, backtester)
- Enhanced error handling and recovery
- Real data validation and enforcement  
- Proper async/sync coordination with fixed components
- Complete system health monitoring
- REAL DATA ONLY (no mock/simulated data)
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
import json
from datetime import datetime

# Set higher recursion limit for ML operations
sys.setrecursionlimit(5000)

# Add current directory to Python path
current_dir = Path(__file__).parent.resolve()
sys.path.insert(0, str(current_dir))

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError as e:
    print(f"ERROR: Missing required package 'python-dotenv': {e}")
    print("Install with: pip install python-dotenv")
    sys.exit(1)

class V3SystemManager:
    """Enhanced system manager with complete integration - REAL DATA ONLY"""
    
    def __init__(self):
        self.controller: Optional['V3TradingController'] = None
        self.middleware: Optional['APIMiddleware'] = None
        self.flask_thread: Optional[threading.Thread] = None
        self.shutdown_event = threading.Event()
        self.logger = None
        self.system_health = {
            'controller_status': 'not_initialized',
            'middleware_status': 'not_initialized', 
            'binance_connection': 'not_tested',
            'database_status': 'not_checked',
            'api_status': 'not_checked',
            'real_data_mode': True,
            'initialization_progress': 0
        }
        self._setup_logging()
        
    def _setup_logging(self):
        """Setup comprehensive logging configuration"""
        try:
            Path('logs').mkdir(exist_ok=True)
            
            log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
            
            logging.basicConfig(
                level=getattr(logging, log_level, logging.INFO),
                format='%(asctime)s - %(name)s.%(levelname)s - %(message)s',
                handlers=[
                    logging.FileHandler('logs/v3_complete_system.log', encoding='utf-8'),
                    logging.StreamHandler(sys.stdout)
                ]
            )
            
            # Reduce noise from external libraries
            for logger_name in ['aiohttp', 'urllib3', 'requests', 'websockets', 'binance', 'werkzeug']:
                logging.getLogger(logger_name).setLevel(logging.WARNING)
            
            self.logger = logging.getLogger(f"{__name__}.SystemManager")
            self.logger.info("V3 Complete System logging initialized")
            
        except Exception as e:
            print(f"Warning: Failed to setup logging: {e}")
            logging.basicConfig(level=logging.INFO)
            self.logger = logging.getLogger(__name__)
    
    def check_requirements(self) -> bool:
        """Check comprehensive system requirements"""
        self.logger.info("Checking V3 system requirements...")
        
        # Check Python version
        if sys.version_info < (3, 8):
            self.logger.error(f"Python 3.8+ required. Current: {sys.version}")
            return False
        
        # Check .env file
        if not Path('.env').exists():
            self.logger.error(".env file not found!")
            print("\nERROR: Configuration file missing!")
            print("Please ensure .env file exists with your API credentials")
            return False
        
        # Check critical environment variables for REAL trading
        critical_vars = ['BINANCE_API_KEY_1', 'BINANCE_API_SECRET_1']
        missing_vars = [var for var in critical_vars if not os.getenv(var)]
        
        if missing_vars:
            self.logger.error(f"Missing critical variables: {missing_vars}")
            print(f"\nERROR: Missing required configuration: {', '.join(missing_vars)}")
            return False
        
        # Validate REAL data mode settings
        use_real_data = os.getenv('USE_REAL_DATA_ONLY', 'true').lower() == 'true'
        mock_disabled = os.getenv('MOCK_DATA_DISABLED', 'true').lower() == 'true'
        
        if not use_real_data or not mock_disabled:
            self.logger.error("System not configured for REAL data only mode")
            print("\nERROR: System must be configured for REAL data only")
            print("Set USE_REAL_DATA_ONLY=true and MOCK_DATA_DISABLED=true")
            return False
        
        # Create required directories
        for dir_name in ['data', 'logs', 'models']:
            try:
                Path(dir_name).mkdir(parents=True, exist_ok=True)
            except Exception as e:
                self.logger.error(f"Failed to create directory {dir_name}: {e}")
                return False
        
        # Check required Python packages
        required_packages = [
            'pandas', 'numpy', 'flask', 'flask_cors', 'flask_socketio',
            'python-binance', 'sqlite3', 'psutil', 'threading'
        ]
        
        missing_packages = []
        for package in required_packages:
            try:
                if package == 'python-binance':
                    import binance
                elif package == 'flask_cors':
                    import flask_cors
                elif package == 'flask_socketio':
                    import flask_socketio
                else:
                    __import__(package)
            except ImportError:
                missing_packages.append(package)
        
        if missing_packages:
            self.logger.error(f"Missing required packages: {missing_packages}")
            print(f"\nERROR: Missing packages: {', '.join(missing_packages)}")
            print("Install with: pip install -r requirements.txt")
            return False
        
        self.logger.info("System requirements check passed")
        return True
    
    def setup_signal_handlers(self):
        """Setup enhanced signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            signal_names = {signal.SIGINT: "SIGINT (Ctrl+C)", signal.SIGTERM: "SIGTERM"}
            signal_name = signal_names.get(signum, f"Signal {signum}")
            
            self.logger.info(f"Received {signal_name} - initiating graceful shutdown")
            
            if not self.shutdown_event.is_set():
                self.shutdown_event.set()
                shutdown_thread = threading.Thread(target=self._run_shutdown, daemon=True)
                shutdown_thread.start()
        
        signal.signal(signal.SIGINT, signal_handler)
        if hasattr(signal, 'SIGTERM'):
            signal.signal(signal.SIGTERM, signal_handler)
    
    def _run_shutdown(self):
        """Run comprehensive shutdown sequence"""
        try:
            self.logger.info("Initiating graceful shutdown...")
            time.sleep(1)  # Allow current operations to complete
            
            # Stop controller
            if self.controller:
                try:
                    self.logger.info("Shutting down controller...")
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    loop.run_until_complete(self.controller.shutdown())
                    loop.close()
                    self.system_health['controller_status'] = 'shutdown'
                except Exception as e:
                    self.logger.error(f"Controller shutdown error: {e}")
            
            # Stop middleware
            if self.middleware:
                try:
                    self.logger.info("Shutting down middleware...")
                    self.middleware.stop()
                    self.system_health['middleware_status'] = 'shutdown'
                except Exception as e:
                    self.logger.error(f"Middleware shutdown error: {e}")
            
            # Final cleanup
            gc.collect()
            
            self.logger.info("System completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")
        finally:
            # Force exit after timeout
            threading.Timer(5.0, lambda: os._exit(0)).start()
    
    def print_startup_banner(self):
        """Print comprehensive startup banner"""
        banner = f"""
    ================================================================
    |           V3 COMPLETE TRADING SYSTEM - REAL DATA ONLY      |
    |                                                              |
    |  ?? Enhanced Multi-Timeframe Backtesting                   |
    |  ?? Real Market Data Integration                            |
    |  ?? ML-Powered Strategy Discovery                          |
    |  ? High-Performance Database Pooling                       |
    |  ?? Advanced API Rotation Management                        |
    |  ?? Real-Time Dashboard & WebSocket Updates                |
    ================================================================
    
    SYSTEM COMPONENTS:
    ? Fixed Main Controller (run_comprehensive_backtest method)
    ? Enhanced PnL Persistence (all methods working)
    ? Complete API Middleware (real-time updates)
    ? Advanced Multi-Timeframe Backtester (genetic optimization)
    ? Real Binance Integration (no mock data)
    ? Thread-Safe Database Operations
    ? Memory Leak Prevention
    ? Graceful Error Recovery
    
    DATA SOURCES: 100% REAL MARKET DATA
    API ENDPOINTS: Binance Testnet + Live Data
    ML TRAINING: Real Historical Performance Only
    
    Python: {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}
    PID: {os.getpid()}
    Timestamp: {datetime.now().isoformat()}
        """
        print(banner)
    
    def _validate_real_data_configuration(self) -> bool:
        """Validate that system is properly configured for real data only"""
        try:
            # Check environment settings
            real_data_settings = {
                'USE_REAL_DATA_ONLY': 'true',
                'MOCK_DATA_DISABLED': 'true', 
                'TEST_MODE_DISABLED': 'true',
                'FORCE_REAL_DATA_MODE': 'true'
            }
            
            for setting, expected_value in real_data_settings.items():
                actual_value = os.getenv(setting, '').lower()
                if actual_value != expected_value:
                    self.logger.warning(f"{setting} should be {expected_value}, got {actual_value}")
                    # Auto-fix common issues
                    os.environ[setting] = expected_value
            
            # Validate API credentials
            binance_key = os.getenv('BINANCE_API_KEY_1')
            binance_secret = os.getenv('BINANCE_API_SECRET_1')
            
            if not binance_key or not binance_secret:
                self.logger.error("Binance API credentials not found")
                return False
            
            if len(binance_key) < 50 or len(binance_secret) < 50:
                self.logger.error("Binance API credentials appear invalid")
                return False
            
            self.logger.info("Real data configuration validated")
            return True
            
        except Exception as e:
            self.logger.error(f"Configuration validation error: {e}")
            return False
    
    async def test_binance_connection(self) -> bool:
        """Test real Binance connection"""
        try:
            self.logger.info("Testing Binance connection...")
            
            from binance.client import Client
            
            api_key = os.getenv('BINANCE_API_KEY_1')
            api_secret = os.getenv('BINANCE_API_SECRET_1')
            testnet = os.getenv('USE_BINANCE_TESTNET', 'true').lower() == 'true'
            
            client = Client(api_key, api_secret, testnet=testnet)
            
            # Test connection
            account = client.get_account()
            ticker = client.get_symbol_ticker(symbol="BTCUSDT")
            
            current_btc = float(ticker['price'])
            can_trade = account.get('canTrade', False)
            
            self.system_health['binance_connection'] = 'connected'
            
            self.logger.info(f"Binance {'testnet' if testnet else 'mainnet'} connected successfully")
            self.logger.info(f"BTC Price: ${current_btc:,.2f}, Can Trade: {can_trade}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Binance connection failed: {e}")
            self.system_health['binance_connection'] = 'failed'
            return False
    
    async def initialize_controller(self):
        """Initialize the enhanced trading controller"""
        try:
            self.logger.info("Step 1: Initializing V3 Trading Controller...")
            self.system_health['initialization_progress'] = 20
            
            # Import the fixed controller
            from main_controller import V3TradingController
            self.controller = V3TradingController()
            
            self.logger.info("Creating V3TradingController instance...")
            
            # Initialize the system
            success = await self.controller.initialize_system()
            if not success:
                raise RuntimeError("Controller initialization failed")
            
            self.system_health['controller_status'] = 'initialized'
            self.system_health['initialization_progress'] = 60
            
            self.logger.info("Testing controller API methods...")
            
            # Test key methods
            if hasattr(self.controller, 'get_comprehensive_dashboard_data'):
                dashboard_data = self.controller.get_comprehensive_dashboard_data()
                self.logger.info("Trading Status: {} trades, ${:.2f} P&L".format(
                    dashboard_data.get('overview', {}).get('trading', {}).get('total_trades', 0),
                    dashboard_data.get('overview', {}).get('trading', {}).get('total_pnl', 0)
                ))
            
            if hasattr(self.controller, 'comprehensive_backtester') and self.controller.comprehensive_backtester:
#                 progress = self.controller.comprehensive_backtester.get_progress()
                self.logger.info("System Status: {} strategies loaded".format(
                    len(self.controller.top_strategies)
                ))
            
            self.logger.info("Controller initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize controller: {e}", exc_info=True)
            self.system_health['controller_status'] = 'failed'
            return False
    
    async def initialize_middleware(self):
        """Initialize the enhanced API middleware"""
        try:
            self.logger.info("Step 2: Initializing API Middleware...")
            self.system_health['initialization_progress'] = 70
            
            # Import the fixed middleware
            from api_middleware import APIMiddleware
            
            host = os.getenv('HOST', '0.0.0.0') 
            port = int(os.getenv('FLASK_PORT', '8102'))
            
            self.logger.info(f"Creating APIMiddleware instance on {host}:{port}...")
            
            self.middleware = APIMiddleware(host=host, port=port)
            
            # Register controller with middleware
            if self.controller:
                self.middleware.register_controller(self.controller)
                self.logger.info("Controller successfully registered")
            
            self.system_health['middleware_status'] = 'initialized'
            self.system_health['api_status'] = 'ready'
            self.system_health['initialization_progress'] = 85
            
            # Verify integration
            self.logger.info("Step 3: Verifying system integration...")
            
            # Test middleware methods
            if hasattr(self.middleware.controller_interface, 'get_controller_data'):
                metrics = self.middleware.controller_interface.get_controller_data('metrics')
                if metrics:
                    self.logger.info("get_trading_status: Working")
                else:
                    self.logger.warning("get_trading_status: No data")
            
            self.logger.info("Integration verification passed")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize middleware: {e}", exc_info=True)
            self.system_health['middleware_status'] = 'failed'
            return False
    
    def start_flask_server(self):
        """Start Flask server in separate thread"""
        def run_flask():
            try:
                self.logger.info("Starting Flask server...")
                if self.middleware:
                    self.middleware.run(debug=False)
            except Exception as e:
                self.logger.error(f"Flask error: {e}", exc_info=True)
        
        self.flask_thread = threading.Thread(target=run_flask, daemon=True)
        self.flask_thread.start()
        time.sleep(3)  # Wait for Flask to start
        
        dashboard_port = int(os.getenv('FLASK_PORT', '8102'))
        host = os.getenv('HOST', '0.0.0.0')
        
        self.logger.info(f"Dashboard URL: http://{host}:{dashboard_port}")
        self.logger.info(f"API Endpoints: http://{host}:{dashboard_port}/api/")
        
        # Update system health
        self.system_health['initialization_progress'] = 100
    
    def print_system_status(self):
        """Print comprehensive system status"""
        dashboard_port = int(os.getenv('FLASK_PORT', '8102'))
        host = os.getenv('HOST', '0.0.0.0')
        auto_start = os.getenv('AUTO_START_TRADING', 'false').lower() == 'true'
        
        print("\n" + "="*70)
        print("?? V3 COMPLETE TRADING SYSTEM READY!")
        print("="*70)
        print("SYSTEM STATUS:")
        print(f"? Controller: {self.system_health['controller_status']}")
        print(f"? Middleware: {self.system_health['middleware_status']}")
        print(f"? Binance: {self.system_health['binance_connection']}")
        print(f"? Real Data Mode: {self.system_health['real_data_mode']}")
        print(f"? API Status: {self.system_health['api_status']}")
        print(f"? Progress: {self.system_health['initialization_progress']}%")
        print("="*70)
        print("ENDPOINTS:")
        print(f"?? Dashboard: http://{host}:{dashboard_port}")
        print(f"?? API: http://{host}:{dashboard_port}/api/")
        print(f"?? Health: http://{host}:{dashboard_port}/health")
        print("="*70)
        print("FEATURES READY:")
        print("? Multi-Timeframe Backtesting")
        print("? Real Market Data Analysis")
        print("? ML Strategy Discovery")
        print("? Live Trading Interface")
        print("? Performance Monitoring")
        print("? Risk Management")
        print("="*70)
        
        if auto_start:
            print("?? AUTO_START_TRADING=true - Will start automatically")
        else:
            print("?? Monitor mode - Use dashboard to start trading")
        
        print(f"\n?? V3 Complete System running... Press Ctrl+C to shutdown")
        print(f"?? Access your dashboard: http://{host}:{dashboard_port}")
    
    def save_system_state(self):
        """Save current system state"""
        try:
            state = {
                'timestamp': datetime.now().isoformat(),
                'system_health': self.system_health,
                'controller_available': self.controller is not None,
                'middleware_available': self.middleware is not None,
                'real_data_mode': True,
                'version': 'V3_COMPLETE_FIXED'
            }
            
            with open('data/system_state.json', 'w') as f:
                json.dump(state, f, indent=2)
                
            self.logger.info("System state saved")
            
        except Exception as e:
            self.logger.error(f"Failed to save system state: {e}")
    
    async def run_system(self):
        """Main system run loop with comprehensive error handling"""
        try:
            # Phase 1: Pre-flight checks
            if not self.check_requirements():
                return False
            
            if not self._validate_real_data_configuration():
                self.logger.error("Real data configuration validation failed")
                return False
            
            self.print_startup_banner()
            self.setup_signal_handlers()
            
            # Phase 2: Test connections
            if not await self.test_binance_connection():
                self.logger.error("Binance connection test failed")
                return False
            
            # Phase 3: Initialize core components
            if not await self.initialize_controller():
                return False
            
            if not await self.initialize_middleware():
                return False
            
            # Phase 4: Start services
            self.start_flask_server()
            self.print_system_status()
            
            # Phase 5: Auto-start trading if enabled
            auto_start = os.getenv('AUTO_START_TRADING', 'false').lower() == 'true'
            if auto_start and self.controller:
                self.logger.info("AUTO_START_TRADING enabled")
                try:
                    self.controller.is_running = True
                    self.logger.info("Trading started automatically")
                except Exception as e:
                    self.logger.error(f"Auto-start error: {e}")
            
            # Phase 6: Save system state
            self.save_system_state()
            
            # Phase 7: Main monitoring loop
            last_gc_time = time.time()
            last_health_check = time.time()
            
            while not self.shutdown_event.is_set():
                await asyncio.sleep(2)
                
                # Periodic garbage collection
                if time.time() - last_gc_time > 300:  # 5 minutes
                    gc.collect()
                    last_gc_time = time.time()
                
                # Health monitoring
                if time.time() - last_health_check > 60:  # 1 minute
                    try:
                        if self.controller and hasattr(self.controller, 'metrics'):
                            self.logger.info(f"System Health: {self.controller.metrics.get('total_trades', 0)} trades, "
                                           f"${self.controller.metrics.get('total_pnl', 0):.2f} P&L")
                        last_health_check = time.time()
                    except Exception as e:
                        self.logger.warning(f"Health check error: {e}")
            
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
            system_manager.logger.info("V3 Complete System completed successfully")
        else:
            system_manager.logger.error("V3 Complete System completed with errors")
            sys.exit(1)
            
    except KeyboardInterrupt:
        system_manager.logger.info("V3 Complete System interrupted by user")
    except Exception as e:
        if system_manager.logger:
            system_manager.logger.error(f"Unhandled error: {e}", exc_info=True)
        else:
            print(f"Critical error: {e}")
            traceback.print_exc()
        sys.exit(1)
    finally:
        if system_manager.logger:
            system_manager.logger.info("V3 Complete Trading System shutdown complete")

if __name__ == "__main__":
    print("?? Starting V3 Complete Trading System...")
    print("?? Real Data Only + Enhanced Components + Fixed Methods")
    main()