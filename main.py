#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V3 TRADING SYSTEM - FIXED MAIN ENTRY POINT
==========================================
FIXES APPLIED:
- Proper signal handling and graceful shutdown
- Enhanced error recovery and logging  
- Configuration validation
- Better async/sync coordination
- Keeps existing API rotation system
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
    """System manager with proper lifecycle management"""
    
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
                    logging.FileHandler('logs/v3_fixed_system.log', encoding='utf-8'),
                    logging.StreamHandler(sys.stdout)
                ]
            )
            
            # Reduce noise from external libraries
            for logger_name in ['aiohttp', 'urllib3', 'requests', 'websockets', 'binance']:
                logging.getLogger(logger_name).setLevel(logging.WARNING)
            
            self.logger = logging.getLogger(__name__)
            self.logger.info("Fixed logging system initialized")
            
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
        """Print startup banner"""
        banner = f"""
    ================================================================
    |              V3 FIXED TRADING SYSTEM                        |
    |                                                              |
    |  Database Connection Pooling + Memory Management            |
    |  Enhanced Error Handling + Async Task Management            |
    |  Keeps Your Existing API Rotation System                    |
    ================================================================
    
    FIXED COMPONENTS:
    ? Database Connection Pooling (SQLite locking fixed)
    ? Memory Leak Prevention (bounded collections)  
    ? Enhanced Error Recovery (proper exception handling)
    ? Async Task Management (no hanging tasks)
    ? Thread-Safe Operations (proper locking)
    ? Graceful Startup/Shutdown (signal handling)
    ? Your Excellent API Rotation System (unchanged)
    
    Python: {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}
    PID: {os.getpid()}
        """
        print(banner)
    
    async def initialize_controller(self):
        """Initialize the trading controller"""
        try:
            self.logger.info("Initializing V3 Trading Controller...")
            
            from main_controller import V3TradingController
            self.controller = V3TradingController()
            
            success = await self.controller.initialize_system()
            if not success:
                raise RuntimeError("Controller initialization failed")
            
            self.logger.info("Controller initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize controller: {e}", exc_info=True)
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
        """Print system status"""
        dashboard_port = int(os.getenv('FLASK_PORT', '8102'))
        auto_start = os.getenv('AUTO_START_TRADING', 'false').lower() == 'true'
        
        print("=" * 70)
        print("?? V3 FIXED TRADING SYSTEM READY!")
        print("? Database Connection Pooling: ACTIVE")
        print("? Memory Management: ACTIVE") 
        print("? Enhanced Error Handling: ACTIVE")
        print("? Async Task Management: ACTIVE")
        print("? Your API Rotation System: ACTIVE")
        print(f"?? Dashboard: http://localhost:{dashboard_port}")
        print("=" * 70)
        
        if auto_start:
            print("\n?? AUTO_START_TRADING=true - Will start automatically")
        else:
            print(f"\n? Monitor mode - Use dashboard to start trading")
            print(f"Dashboard: http://localhost:{dashboard_port}")
        
        print("\n?? V3 Fixed System running... Press Ctrl+C to shutdown")
    
    async def run_system(self):
        """Main system run loop"""
        try:
            if not self.check_requirements():
                return False
            
            self.print_startup_banner()
            self.setup_signal_handlers()
            
            if not await self.initialize_controller():
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
            self.logger.error(f"System error: {e}", exc_info=True)
            return False

def main():
    """Main function with error handling"""
    system_manager = V3SystemManager()
    
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
            system_manager.logger.info("V3 Fixed Trading System shutdown complete")

if __name__ == "__main__":
    print("?? Starting V3 Fixed Trading System...")
    print("? Database Pooling + Memory Management + Enhanced Error Handling")
    main()