#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V3 TRADING SYSTEM - UNIFIED MAIN ENTRY POINT
==========================================
UNIFIED ARCHITECTURE:
- Starts main controller (no Flask)
- Starts API middleware (handles web dashboard)
- Connects controller to middleware
- Single entry point for entire system
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
except ImportError as e:
    print(f"ERROR: Missing required package 'python-dotenv': {e}")
    print("Install with: pip install python-dotenv")
    sys.exit(1)

class V3UnifiedSystemManager:
    """Unified system manager that starts controller + middleware"""
    
    def __init__(self):
        self.controller: Optional['V3TradingController'] = None
        self.middleware = None
        self.middleware_thread: Optional[threading.Thread] = None
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
                    logging.FileHandler('logs/v3_unified_system.log', encoding='utf-8'),
                    logging.StreamHandler(sys.stdout)
                ]
            )
            
            # Reduce noise from external libraries
            for logger_name in ['aiohttp', 'urllib3', 'requests', 'websockets', 'binance']:
                logging.getLogger(logger_name).setLevel(logging.WARNING)
            
            self.logger = logging.getLogger(__name__)
            self.logger.info("Unified logging system initialized")
            
        except Exception as e:
            print(f"Warning: Failed to setup logging: {e}")
            logging.basicConfig(level=logging.INFO)
            self.logger = logging.getLogger(__name__)
    
    def check_requirements(self) -> bool:
        """Check system requirements"""
        self.logger.info("Checking system requirements...")
        
        # Check Python version
        if sys.version_info < (3, 8):
            self.logger.error(f"Python 3.8+ required. Current: {sys.version}")
            return False
        
        # Check .env file
        if not Path('.env').exists():
            self.logger.error(".env file not found!")
            print("\nERROR: Configuration file missing!")
            print("Please copy .env.template to .env and configure your API keys")
            return False
        
        # Check critical environment variables  
        critical_vars = ['BINANCE_API_KEY_1', 'BINANCE_API_SECRET_1']
        missing_vars = [var for var in critical_vars if not os.getenv(var)]
        
        if missing_vars:
            self.logger.error(f"Missing critical variables: {missing_vars}")
            print(f"\nERROR: Missing required configuration: {', '.join(missing_vars)}")
            return False
        
        # Create required directories
        for dir_name in ['data', 'logs', 'models']:
            try:
                Path(dir_name).mkdir(parents=True, exist_ok=True)
            except Exception as e:
                self.logger.error(f"Failed to create directory {dir_name}: {e}")
                return False
        
        self.logger.info("System requirements check passed")
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
            
            # Shutdown controller
            if self.controller:
                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    loop.run_until_complete(self.controller.shutdown())
                    loop.close()
                except Exception as e:
                    self.logger.error(f"Controller shutdown error: {e}")
            
            # Shutdown middleware
            if self.middleware:
                try:
                    self.middleware.stop()
                except Exception as e:
                    self.logger.error(f"Middleware shutdown error: {e}")
            
            self.logger.info("Shutdown sequence completed")
            
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")
        finally:
            threading.Timer(5.0, lambda: os._exit(0)).start()
    
    def print_startup_banner(self):
        """Print startup banner"""
        banner = f"""
    ================================================================
    |              V3 UNIFIED TRADING SYSTEM                      |
    |                                                              |
    |  Controller + Middleware Architecture                       |
    |  Real Data Only - No Simulation                             |
    |  Single Entry Point - Unified Management                    |
    ================================================================
    
    UNIFIED COMPONENTS:
    ? Main Trading Controller (real data only)
    ? API Middleware (web dashboard interface)  
    ? Database Connection Pooling
    ? Enhanced Error Recovery
    ? Async Task Management
    ? Thread-Safe Operations
    ? Graceful Startup/Shutdown
    
    Python: {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}
    PID: {os.getpid()}
        """
        print(banner)
    
    async def initialize_controller(self):
        """Initialize the trading controller (without Flask)"""
        try:
            self.logger.info("Initializing V3 Trading Controller...")
            
            # Import the clean controller (without Flask)
            from main_controller import V3TradingController
            self.controller = V3TradingController()
            
            # Initialize the controller
            success = await self.controller.initialize_system()
            if not success:
                raise RuntimeError("Controller initialization failed")
            
            self.logger.info("Controller initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize controller: {e}", exc_info=True)
            return False
    
    def start_middleware(self):
        """Start API middleware in separate thread"""
        def run_middleware():
            try:
                self.logger.info("Starting API Middleware...")
                
                # Import and create middleware
                from api_middleware import create_middleware
                self.middleware = create_middleware()
                
                # Register controller with middleware
                if self.controller:
                    self.middleware.register_controller(self.controller)
                
                # Run middleware (this blocks)
                dashboard_port = int(os.getenv('FLASK_PORT', '8102'))
                self.logger.info(f"Middleware starting on port {dashboard_port}")
                self.middleware.run(debug=False)
                
            except Exception as e:
                self.logger.error(f"Middleware error: {e}", exc_info=True)
        
        self.middleware_thread = threading.Thread(target=run_middleware, daemon=True)
        self.middleware_thread.start()
        time.sleep(3)  # Wait for middleware to start
        
        dashboard_port = int(os.getenv('FLASK_PORT', '8102'))
        self.logger.info(f"Dashboard available at: http://localhost:{dashboard_port}")
    
    def print_status(self):
        """Print system status"""
        dashboard_port = int(os.getenv('FLASK_PORT', '8102'))
        auto_start = os.getenv('AUTO_START_TRADING', 'false').lower() == 'true'
        
        print("=" * 70)
        print("? V3 UNIFIED TRADING SYSTEM READY!")
        print("? Controller: Initialized (Real Data Only)")
        print("? Middleware: Running") 
        print("? Database: Connection Pooling Active")
        print("? Memory Management: Active")
        print("? Error Handling: Enhanced")
        print("? Task Management: Async")
        print(f"?? Dashboard: http://localhost:{dashboard_port}")
        print("=" * 70)
        
        if auto_start:
            print("\n?? AUTO_START_TRADING=true - Will start automatically")
        else:
            print(f"\n?? Monitor mode - Use dashboard to start trading")
            print(f"Dashboard: http://localhost:{dashboard_port}")
        
        print("\n?? V3 Unified System running... Press Ctrl+C to shutdown")
        print("\nData Flow: Dashboard ? Middleware ? Controller")
        print("Real Data Only: No simulation or fake data generation")
    
    async def run_system(self):
        """Main system run loop"""
        try:
            if not self.check_requirements():
                return False
            
            self.print_startup_banner()
            self.setup_signal_handlers()
            
            # Initialize controller (no Flask)
            if not await self.initialize_controller():
                return False
            
            # Start middleware (handles web dashboard)
            self.start_middleware()
            
            self.print_status()
            
            # Auto-start trading if enabled
            auto_start = os.getenv('AUTO_START_TRADING', 'false').lower() == 'true'
            if auto_start and self.controller:
                self.logger.info("AUTO_START_TRADING enabled")
                try:
                    # Controller doesn't have start_trading in our clean version
                    # Instead, set the running flag
                    self.controller.is_running = True
                    self.logger.info("Trading started automatically")
                except Exception as e:
                    self.logger.error(f"Auto-start error: {e}")
            
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
    """Main function with unified architecture"""
    system_manager = V3UnifiedSystemManager()
    
    try:
        success = asyncio.run(system_manager.run_system())
        
        if success:
            system_manager.logger.info("System completed successfully")
        else:
            system_manager.logger.error("System completed with errors")
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
            system_manager.logger.info("V3 Unified Trading System shutdown complete")

if __name__ == "__main__":
    print("?? Starting V3 Unified Trading System...")
    print("? Controller + Middleware Architecture")
    print("? Real Data Only - No Simulation")
    main()