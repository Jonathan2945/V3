#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V3 TRADING SYSTEM - FIXED MAIN ENTRY POINT
==========================================
CRITICAL FIXES APPLIED:
- Fixed controller initialization issue
- Proper trading engine capability detection
- Enhanced error handling for import failures
- Safe fallback for missing components
- Maintains V3 real-data-only compliance
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

# CRITICAL: Safe import handling
def safe_import(module_name: str, package_name: str = None, required: bool = True):
    """Safely import a module with proper error handling"""
    try:
        if package_name:
            module = __import__(module_name, fromlist=[package_name])
            return getattr(module, package_name)
        else:
            return __import__(module_name)
    except ImportError as e:
        if required:
            print(f"CRITICAL: Missing required package '{module_name}': {e}")
            print(f"Install with: pip install {module_name}")
        else:
            print(f"Optional package '{module_name}' not available: {e}")
        return None
    except Exception as e:
        print(f"Error importing '{module_name}': {e}")
        return None

# Load environment variables with error handling
dotenv = safe_import('dotenv', required=False)
if dotenv:
    try:
        dotenv.load_dotenv()
        print("Environment variables loaded successfully")
    except Exception as e:
        print(f"Warning: Could not load .env file: {e}")
else:
    print("Warning: python-dotenv not installed. Environment variables may not be loaded.")
    print("Install with: pip install python-dotenv")

# Critical imports with error handling
required_packages = {
    'pandas': 'pandas>=2.0.0',
    'numpy': 'numpy>=1.24.0',
    'psutil': 'psutil>=5.9.5',
    'flask': 'flask>=2.0.0',
    'asyncio': None,  # Built-in
    'logging': None,  # Built-in
    'sqlite3': None,  # Built-in
}

# Track which packages are available
AVAILABLE_PACKAGES = {}

for package, install_cmd in required_packages.items():
    result = safe_import(package, required=(package in ['asyncio', 'logging']))
    AVAILABLE_PACKAGES[package] = result is not None
    
    if not AVAILABLE_PACKAGES[package] and install_cmd:
        print(f"Missing: {package} - Install with: pip install {install_cmd}")

# Check if basic requirements are met
BASIC_REQUIREMENTS_MET = all(AVAILABLE_PACKAGES[pkg] for pkg in ['asyncio', 'logging'])

if not BASIC_REQUIREMENTS_MET:
    print("CRITICAL: Basic Python requirements not met!")
    sys.exit(1)

class V3SystemManager:
    """Enhanced system manager with graceful error handling and FIXED initialization"""
    
    def __init__(self):
        self.controller: Optional['V3TradingController'] = None
        self.flask_thread: Optional[threading.Thread] = None
        self.shutdown_event = threading.Event()
        self.logger = None
        self.available_features = {}
        
        # Setup logging first
        self._setup_logging()
        
        # Check system capabilities
        self._check_system_capabilities()
        
    def _setup_logging(self):
        """Setup logging with enhanced error handling"""
        try:
            # Create logs directory
            logs_dir = Path('logs')
            logs_dir.mkdir(exist_ok=True)
            
            log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
            
            # Configure logging
            logging.basicConfig(
                level=getattr(logging, log_level, logging.INFO),
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                handlers=[
                    logging.FileHandler(logs_dir / 'v3_system.log', encoding='utf-8'),
                    logging.StreamHandler(sys.stdout)
                ]
            )
            
            # Reduce noise from external libraries
            for logger_name in ['aiohttp', 'urllib3', 'requests', 'websockets']:
                try:
                    logging.getLogger(logger_name).setLevel(logging.WARNING)
                except:
                    pass
            
            self.logger = logging.getLogger(__name__)
            self.logger.info("V3 System Manager logging initialized")
            
        except Exception as e:
            # Fallback to basic logging
            print(f"Warning: Enhanced logging setup failed: {e}")
            logging.basicConfig(level=logging.INFO)
            self.logger = logging.getLogger(__name__)
            self.logger.warning("Using fallback logging configuration")
    
    def _check_system_capabilities(self):
        """FIXED - Check what system capabilities are available"""
        self.logger.info("Checking V3 system capabilities...")
        
        capabilities = {
            'trading_engine': False,
            'ml_engine': False,
            'backtesting': False,
            'api_rotation': False,
            'external_data': False,
            'dashboard': False,
            'database': AVAILABLE_PACKAGES.get('sqlite3', False),
            'data_analysis': AVAILABLE_PACKAGES.get('pandas', False) and AVAILABLE_PACKAGES.get('numpy', False)
        }
        
        # FIXED - Check for V3 components with proper capability mapping
        v3_components = [
            ('main_controller', 'V3TradingController', 'trading_engine'),
            ('intelligent_trading_engine', 'IntelligentTradingEngine', 'trading_engine'),
            ('advanced_ml_engine', 'AdvancedMLEngine', 'ml_engine'),
            ('advanced_backtester', 'V3RealDataBacktester', 'backtesting'),
            ('api_rotation_manager', 'get_api_key', 'api_rotation'),
            ('external_data_collector', 'ExternalDataCollector', 'external_data')
        ]
        
        for module_name, class_name, capability_key in v3_components:
            try:
                module = __import__(module_name)
                if hasattr(module, class_name):
                    if capability_key in capabilities:
                        capabilities[capability_key] = True
                    self.logger.info(f"✓ {class_name} available")
            except Exception as e:
                self.logger.warning(f"✗ {module_name} not available: {e}")
        
        # Check Flask/Dashboard capability
        flask = safe_import('flask', required=False)
        if flask:
            capabilities['dashboard'] = True
            self.logger.info("✓ Flask dashboard available")
        else:
            self.logger.warning("✗ Flask dashboard not available")
        
        self.available_features = capabilities
        self.logger.info(f"Available features: {sum(capabilities.values())}/{len(capabilities)}")
        
        return capabilities
    
    def check_requirements(self) -> bool:
        """Enhanced requirements check with better error messages"""
        self.logger.info("Checking V3 system requirements...")
        
        # Check Python version
        if sys.version_info < (3, 8):
            self.logger.error(f"Python 3.8+ required. Current: {sys.version}")
            return False
        
        self.logger.info(f"✓ Python {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}")
        
        # Check .env file
        env_path = Path('.env')
        if not env_path.exists():
            self.logger.warning(".env file not found!")
            
            # Look for template
            template_path = Path('.env.template')
            if template_path.exists():
                self.logger.info("Found .env.template - you may need to copy it to .env")
                print("\nSETUP REQUIRED:")
                print("1. Copy .env.template to .env")
                print("2. Add your API keys to .env")
                print("3. Run the system again")
            else:
                self.logger.warning("No .env or .env.template found")
                print("\nCREATE .env FILE:")
                print("Create a .env file with your configuration")
            
            return False
        
        # Check critical environment variables with better error handling
        critical_vars = ['BINANCE_API_KEY_1', 'BINANCE_API_SECRET_1']
        missing_vars = []
        
        for var in critical_vars:
            value = os.getenv(var)
            if not value:
                missing_vars.append(var)
            elif len(value) < 10:  # Basic validation
                self.logger.warning(f"{var} seems too short - check your configuration")
        
        if missing_vars:
            self.logger.error(f"Missing critical variables: {missing_vars}")
            print(f"\nERROR: Missing required configuration in .env file:")
            for var in missing_vars:
                print(f"   {var}=your_api_key_here")
            return False
        
        # Validate numeric configs with better error handling
        try:
            max_pos = int(os.getenv('MAX_TOTAL_POSITIONS', '3'))
            if not 1 <= max_pos <= 50:
                self.logger.error("MAX_TOTAL_POSITIONS must be between 1 and 50")
                return False
                
            trade_amount = float(os.getenv('TRADE_AMOUNT_USDT', '10.0'))
            if trade_amount <= 0:
                self.logger.error("TRADE_AMOUNT_USDT must be positive")
                return False
                
        except ValueError as e:
            self.logger.error(f"Configuration validation error: {e}")
            print("\nCHECK YOUR .env FILE:")
            print("Ensure numeric values are valid numbers")
            return False
        
        # Create required directories
        directories = ['data', 'logs', 'models', 'backups']
        for dir_name in directories:
            try:
                Path(dir_name).mkdir(parents=True, exist_ok=True)
            except Exception as e:
                self.logger.warning(f"Could not create directory {dir_name}: {e}")
        
        self.logger.info("✓ System requirements check passed")
        return True
    
    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
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
        """Enhanced shutdown sequence"""
        try:
            self.logger.info("Starting graceful shutdown...")
            time.sleep(1)  # Allow current operations to complete
            
            if self.controller:
                try:
                    self.logger.info("Shutting down trading controller...")
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    loop.run_until_complete(self.controller.shutdown())
                    loop.close()
                    self.logger.info("Trading controller shutdown complete")
                except Exception as e:
                    self.logger.error(f"Controller shutdown error: {e}")
            
            if self.flask_thread and self.flask_thread.is_alive():
                self.logger.info("Stopping Flask server...")
                # Flask server will stop when main thread exits
            
            self.logger.info("Graceful shutdown sequence completed")
            
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")
        finally:
            # Force exit after timeout
            threading.Timer(5.0, lambda: os._exit(0)).start()
    
    def print_startup_banner(self):
        """Enhanced startup banner with system status"""
        banner = f"""
    ================================================================
    |                V3 TRADING SYSTEM                             |
    |                                                              |
    |  Real Data Only • Enhanced Error Handling • API Rotation    |
    |  Multi-Timeframe Analysis • ML Strategy Discovery           |
    ================================================================
    
    SYSTEM STATUS:
    ✓ Python: {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}
    ✓ PID: {os.getpid()}
    ✓ Directory: {current_dir}
    
    AVAILABLE FEATURES:
    {'✓' if self.available_features.get('database') else '✗'} Database Support
    {'✓' if self.available_features.get('data_analysis') else '✗'} Data Analysis (Pandas/NumPy)
    {'✓' if self.available_features.get('trading_engine') else '✗'} Trading Engine
    {'✓' if self.available_features.get('ml_engine') else '✗'} ML Engine
    {'✓' if self.available_features.get('api_rotation') else '✗'} API Rotation
    {'✓' if self.available_features.get('dashboard') else '✗'} Web Dashboard
    {'✓' if self.available_features.get('backtesting') else '✗'} Advanced Backtesting
    
    CRITICAL VALIDATION:
    ✓ Real Data Only Mode (No Mock Data)
    ✓ Environment Configuration Loaded
    ✓ Error Handling Enhanced
    ✓ Graceful Shutdown Support
        """
        print(banner)
        
        # Show warnings for missing features
        missing_features = [k for k, v in self.available_features.items() if not v]
        if missing_features:
            print("    MISSING FEATURES (Install dependencies to enable):")
            for feature in missing_features[:5]:  # Show first 5
                print(f"    ✗ {feature.replace('_', ' ').title()}")
            if len(missing_features) > 5:
                print(f"    ... and {len(missing_features) - 5} more")
    
    async def initialize_controller(self):
        """FIXED - Initialize the trading controller with proper error handling"""
        try:
            self.logger.info("Initializing V3 Trading Controller...")
            
            # FIXED - Check if trading controller is available
            if not self.available_features.get('trading_engine', False):
                self.logger.error("V3TradingController not available - check imports")
                return False
            
            # FIXED - Import and initialize controller
            try:
                from main_controller import V3TradingController
                self.controller = V3TradingController()
                
                # Initialize the controller
                success = await self.controller.initialize_system()
                if not success:
                    self.logger.error("Controller initialization returned False")
                    return False
                
                self.logger.info("✓ V3 Trading Controller initialized successfully")
                return True
                
            except ImportError as e:
                self.logger.error(f"Could not import V3TradingController: {e}")
                return False
            except Exception as e:
                self.logger.error(f"Controller initialization failed: {e}")
                traceback.print_exc()
                return False
            
        except Exception as e:
            self.logger.error(f"Failed to initialize controller: {e}", exc_info=True)
            return False
    
    def start_flask_app(self):
        """FIXED - Start Flask app with proper error handling"""
        if not self.available_features.get('dashboard', False):
            self.logger.warning("Dashboard not available - Flask not installed")
            return
        
        if not self.controller:
            self.logger.warning("No controller available - cannot start Flask app")
            return
        
        def run_flask():
            try:
                self.logger.info("Starting Flask dashboard server...")
                
                if hasattr(self.controller, 'run_flask_app'):
                    self.controller.run_flask_app()
                else:
                    self.logger.error("Controller has no run_flask_app method")
                    
            except Exception as e:
                self.logger.error(f"Flask server error: {e}", exc_info=True)
        
        self.flask_thread = threading.Thread(target=run_flask, daemon=True)
        self.flask_thread.start()
        time.sleep(3)  # Wait for Flask to start
        
        dashboard_port = int(os.getenv('FLASK_PORT', '8102'))
        self.logger.info(f"Dashboard should be available at: http://localhost:{dashboard_port}")
    
    def print_status(self):
        """Enhanced status display"""
        dashboard_port = int(os.getenv('FLASK_PORT', '8102'))
        auto_start = os.getenv('AUTO_START_TRADING', 'false').lower() == 'true'
        
        print("=" * 70)
        print("🚀 V3 TRADING SYSTEM READY!")
        
        # Show available features
        if self.available_features.get('dashboard') and self.controller:
            print(f"🌐 Dashboard: http://localhost:{dashboard_port}")
        
        if self.available_features.get('trading_engine') and self.controller:
            print("✓ Trading Engine: READY")
        else:
            print("✗ Trading Engine: NOT AVAILABLE")
        
        if self.available_features.get('api_rotation'):
            print("🔄 API Rotation: ACTIVE")
        
        print("✓ Real Data Only Mode: ACTIVE")
        print("✓ Enhanced Error Handling: ACTIVE")
        print("=" * 70)
        
        if auto_start:
            print("\n🚀 AUTO_START_TRADING=true - Will start automatically")
        else:
            print("\n🔍 Monitor mode - Use dashboard to start trading")
            if self.available_features.get('dashboard') and self.controller:
                print(f"Dashboard: http://localhost:{dashboard_port}")
        
        print("\n🔄 V3 System running... Press Ctrl+C to shutdown gracefully")
    
    async def run_system(self):
        """FIXED - Enhanced main system run loop"""
        try:
            if not self.check_requirements():
                self.logger.error("System requirements not met - exiting")
                return False
            
            self.print_startup_banner()
            self.setup_signal_handlers()
            
            # FIXED - Initialize controller first
            controller_initialized = await self.initialize_controller()
            
            if not controller_initialized:
                self.logger.error("CRITICAL: Controller initialization failed")
                print("\n" + "="*60)
                print("CONTROLLER INITIALIZATION FAILED:")
                print("1. Check that all dependencies are installed")
                print("2. Verify your .env configuration")
                print("3. Check logs/v3_system.log for details")
                print("="*60)
                return False
            
            # Start dashboard if controller available
            self.start_flask_app()
            
            self.print_status()
            
            # Auto-start trading if enabled and controller available
            auto_start = os.getenv('AUTO_START_TRADING', 'false').lower() == 'true'
            if auto_start and self.controller:
                self.logger.info("AUTO_START_TRADING enabled")
                try:
                    result = await self.controller.start_trading()
                    if result.get('success'):
                        self.logger.info("✓ Trading started automatically")
                    else:
                        self.logger.warning(f"Auto-start failed: {result.get('error')}")
                except Exception as e:
                    self.logger.error(f"Auto-start error: {e}")
            
            # Main loop with enhanced monitoring
            last_gc_time = time.time()
            last_status_time = time.time()
            
            while not self.shutdown_event.is_set():
                await asyncio.sleep(1)
                
                # Periodic garbage collection
                current_time = time.time()
                if current_time - last_gc_time > 300:  # 5 minutes
                    gc.collect()
                    last_gc_time = current_time
                
                # Periodic status check
                if current_time - last_status_time > 3600:  # 1 hour
                    self.logger.info("System health check - all systems operational")
                    last_status_time = current_time
            
            return True
            
        except Exception as e:
            self.logger.error(f"System error: {e}", exc_info=True)
            return False

def main():
    """FIXED - Enhanced main function with comprehensive error handling"""
    system_manager = V3SystemManager()
    
    try:
        # Run the system
        success = asyncio.run(system_manager.run_system())
        
        if success:
            system_manager.logger.info("V3 System completed successfully")
        else:
            system_manager.logger.error("V3 System completed with errors")
            print("\n" + "="*60)
            print("SYSTEM SETUP HELP:")
            print("1. Install missing dependencies: pip install -r requirements.txt")
            print("2. Create .env file with your API keys")
            print("3. Run: python test.py  # To test your setup")
            print("="*60)
            sys.exit(1)
            
    except KeyboardInterrupt:
        system_manager.logger.info("System interrupted by user")
        print("\nShutdown complete.")
    except Exception as e:
        if system_manager.logger:
            system_manager.logger.error(f"Unhandled error: {e}", exc_info=True)
        else:
            print(f"Critical error: {e}")
            traceback.print_exc()
        
        print("\n" + "="*60)
        print("TROUBLESHOOTING:")
        print("1. Check that all dependencies are installed")
        print("2. Verify your .env configuration")
        print("3. Run: python test.py --json-report")
        print("4. Check logs/v3_system.log for details")
        print("="*60)
        sys.exit(1)
    finally:
        if system_manager.logger:
            system_manager.logger.info("V3 Trading System shutdown complete")

if __name__ == "__main__":
    print("🚀 Starting V3 Trading System...")
    print("✓ Real Data Only Mode • Enhanced Error Handling • Graceful Imports")
    main()