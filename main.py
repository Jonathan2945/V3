#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V3 TRADING SYSTEM - COMPLETE FIXED MAIN ENTRY POINT
===================================================
REAL DATA ONLY - NO SIMULATION OR FALLBACKS
All components initialized with real market data
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

class V3SystemManager:
    """Real data only system manager with complete lifecycle management"""
    
    def __init__(self):
        self.controller: Optional['V3TradingController'] = None
        self.middleware: Optional['APIMiddleware'] = None
        self.middleware_thread: Optional[threading.Thread] = None
        self.shutdown_event = threading.Event()
        self.logger = None
        self._setup_logging()
        
    def _setup_logging(self):
        """Setup logging configuration for real data only"""
        try:
            Path('logs').mkdir(exist_ok=True)
            
            log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
            
            logging.basicConfig(
                level=getattr(logging, log_level, logging.INFO),
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                handlers=[
                    logging.FileHandler('logs/v3_real_system.log', encoding='utf-8'),
                    logging.StreamHandler(sys.stdout)
                ]
            )
            
            # Reduce noise from external libraries
            for logger_name in ['aiohttp', 'urllib3', 'requests', 'websockets', 'binance']:
                logging.getLogger(logger_name).setLevel(logging.WARNING)
            
            self.logger = logging.getLogger(__name__)
            self.logger.info("REAL trading system logging initialized")
            
        except Exception as e:
            print(f"Warning: Failed to setup logging: {e}")
            logging.basicConfig(level=logging.INFO)
            self.logger = logging.getLogger(__name__)
    
    def check_requirements(self) -> bool:
        """Check system requirements for REAL trading"""
        self.logger.info("Checking system requirements for REAL trading...")
        
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
        
        # Check critical environment variables for REAL trading
        critical_vars = [
            'BINANCE_API_KEY_1', 'BINANCE_API_SECRET_1',
            'USE_REAL_DATA_ONLY', 'MOCK_DATA_DISABLED'
        ]
        missing_vars = [var for var in critical_vars if not os.getenv(var)]
        
        if missing_vars:
            self.logger.error(f"Missing critical variables for REAL trading: {missing_vars}")
            print(f"\nERROR: Missing required configuration for REAL trading: {', '.join(missing_vars)}")
            return False
        
        # Verify REAL data only mode
        if os.getenv('USE_REAL_DATA_ONLY', 'false').lower() != 'true':
            self.logger.error("USE_REAL_DATA_ONLY must be set to 'true'")
            return False
            
        if os.getenv('MOCK_DATA_DISABLED', 'false').lower() != 'true':
            self.logger.error("MOCK_DATA_DISABLED must be set to 'true'")
            return False
        
        # Create required directories for REAL data
        for dir_name in ['data', 'logs', 'models']:
            try:
                Path(dir_name).mkdir(parents=True, exist_ok=True)
                self.logger.info(f"Created directory for REAL data: {dir_name}")
            except Exception as e:
                self.logger.error(f"Failed to create directory {dir_name}: {e}")
                return False
        
        self.logger.info("REAL system requirements check passed")
        return True
    
    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            signal_names = {signal.SIGINT: "SIGINT (Ctrl+C)", signal.SIGTERM: "SIGTERM"}
            signal_name = signal_names.get(signum, f"Signal {signum}")
            
            self.logger.info(f"Received {signal_name} - initiating REAL system shutdown")
            
            if not self.shutdown_event.is_set():
                self.shutdown_event.set()
                shutdown_thread = threading.Thread(target=self._run_shutdown, daemon=True)
                shutdown_thread.start()
        
        signal.signal(signal.SIGINT, signal_handler)
        if hasattr(signal, 'SIGTERM'):
            signal.signal(signal.SIGTERM, signal_handler)
    
    def _run_shutdown(self):
        """Run shutdown sequence for REAL system"""
        try:
            time.sleep(1)  # Allow current operations to complete
            
            # Shutdown middleware first
            if self.middleware:
                try:
                    self.middleware.stop()
                    self.logger.info("API middleware stopped")
                except Exception as e:
                    self.logger.error(f"Middleware shutdown error: {e}")
            
            # Shutdown controller
            if self.controller:
                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    loop.run_until_complete(self.controller.shutdown())
                    loop.close()
                    self.logger.info("REAL controller shutdown completed")
                except Exception as e:
                    self.logger.error(f"Controller shutdown error: {e}")
            
            self.logger.info("REAL system shutdown sequence completed")
            
        except Exception as e:
            self.logger.error(f"Error during REAL system shutdown: {e}")
        finally:
            threading.Timer(5.0, lambda: os._exit(0)).start()
    
    def print_startup_banner(self):
        """Print startup banner for REAL data system"""
        banner = f"""
    ================================================================
    |              V3 UNIFIED TRADING SYSTEM                      |
    |                                                              |
    |  Controller + Middleware Architecture                       |
    |  Real Market Data Only - NO Simulation                      |
    |  Single Entry Point - Unified Management                    |
    ================================================================

    UNIFIED COMPONENTS (REAL DATA ONLY):
    ? Main Trading Controller (real market data only)
    ? API Middleware (web dashboard interface)
    ? Database Connection Pooling (real data storage)
    ? Enhanced Error Recovery
    ?? Async Task Management
    ?? Thread-Safe Operations
    ? Graceful Startup/Shutdown

    ?? NO MOCK DATA: All data comes from real market sources
    ?? NO SIMULATION: All trading operations are real or paper trading only

    Python: {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}
    PID: {os.getpid()}
        """
        print(banner)
    
    async def initialize_controller(self):
        """Initialize the REAL trading controller"""
        try:
            self.logger.info("Initializing V3 Trading Controller - REAL DATA ONLY...")
            
            from main_controller import V3TradingController
            self.controller = V3TradingController()
            
            success = await self.controller.initialize_system()
            if not success:
                raise RuntimeError("REAL controller initialization failed")
            
            self.logger.info("REAL controller initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize REAL controller: {e}", exc_info=True)
            return False
    
    def initialize_middleware(self):
        """Initialize API middleware for REAL data"""
        try:
            self.logger.info("Starting API Middleware for REAL data...")
            
            from api_middleware import APIMiddleware
            
            # Get port from environment
            port = int(os.getenv('FLASK_PORT', os.getenv('MAIN_SYSTEM_PORT', '8102')))
            host = os.getenv('HOST', '0.0.0.0')
            
            self.middleware = APIMiddleware(host=host, port=port)
            
            # Register controller with middleware
            if self.controller:
                self.middleware.register_controller(self.controller)
            
            # Start middleware in separate thread
            def run_middleware():
                try:
                    self.logger.info(f"Starting REAL data middleware on {host}:{port}")
                    self.middleware.run(debug=False)
                except Exception as e:
                    self.logger.error(f"Middleware error: {e}", exc_info=True)
            
            self.middleware_thread = threading.Thread(target=run_middleware, daemon=True)
            self.middleware_thread.start()
            time.sleep(3)  # Wait for middleware to start
            
            dashboard_port = port
            self.logger.info(f"REAL data dashboard available at: http://localhost:{dashboard_port}")
            return True
            
        except Exception as e:
            self.logger.error(f"API middleware initialization failed: {e}", exc_info=True)
            return False
    
    def print_status(self):
        """Print REAL system status"""
        dashboard_port = int(os.getenv('FLASK_PORT', '8102'))
        auto_start = os.getenv('AUTO_START_TRADING', 'false').lower() == 'true'
        
        print("=" * 70)
        print("? V3 UNIFIED TRADING SYSTEM READY!")
        print("? Controller + Middleware Architecture: ACTIVE")
        print("? Real Market Data Only: ACTIVE") 
        print("? Database Connection Pooling: ACTIVE")
        print("? Enhanced Error Handling: ACTIVE")
        print("? Async Task Management: ACTIVE")
        print(f"?? Dashboard: http://localhost:{dashboard_port}")
        print("=" * 70)
        
        if auto_start:
            print("\n?? AUTO_START_TRADING=true - Will start automatically")
        else:
            print(f"\n?? Monitor mode - Use dashboard to start trading")
            print(f"Dashboard: http://localhost:{dashboard_port}")
        
        print("\n?? V3 Real Data System running... Press Ctrl+C to shutdown")
    
    async def run_system(self):
        """Main REAL system run loop"""
        try:
            if not self.check_requirements():
                return False
            
            self.print_startup_banner()
            self.setup_signal_handlers()
            
            if not await self.initialize_controller():
                return False
            
            if not self.initialize_middleware():
                return False
            
            self.print_status()
            
            # Auto-start if enabled
            auto_start = os.getenv('AUTO_START_TRADING', 'false').lower() == 'true'
            if auto_start and self.controller:
                self.logger.info("AUTO_START_TRADING enabled")
                try:
                    result = await self.controller.start_trading()
                    if result.get('success'):
                        self.logger.info("Trading started automatically")
                    else:
                        self.logger.warning(f"Auto-start failed: {result.get('error')}")
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
            self.logger.error(f"REAL system error: {e}", exc_info=True)
            return False

def main():
    """Main function with error handling for REAL data only"""
    system_manager = V3SystemManager()
    
    try:
        success = asyncio.run(system_manager.run_system())
        
        if success:
            system_manager.logger.info("REAL system completed successfully")
        else:
            system_manager.logger.error("REAL system completed with errors")
            sys.exit(1)
            
    except KeyboardInterrupt:
        system_manager.logger.info("REAL system interrupted by user")
    except Exception as e:
        if system_manager.logger:
            system_manager.logger.error(f"Unhandled REAL system error: {e}", exc_info=True)
        else:
            print(f"Critical REAL system error: {e}")
            traceback.print_exc()
        sys.exit(1)
    finally:
        if system_manager.logger:
            system_manager.logger.info("REAL controller shutdown completed")

if __name__ == "__main__":
    print("?? Starting V3 Unified Trading System - REAL DATA ONLY...")
    print("?? Controller + Middleware Architecture")
    print("?? Real Market Data Only - NO Simulation or Mock Data")
    main()