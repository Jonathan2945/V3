#!/usr/bin/env python3
"""
V3 TRADING SYSTEM MAIN - FIXED
===============================
Fixed imports and database initialization issues
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
    """Enhanced System Manager with proper error handling"""
    
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
            
            self.logger = logging.getLogger('V3_MAIN')
            self.logger.info("V3 System Manager initialized")
            
        except Exception as e:
            print(f"Warning: Failed to setup logging: {e}")
            logging.basicConfig(level=logging.INFO)
            self.logger = logging.getLogger('V3_MAIN')
    
    def validate_environment(self) -> bool:
        """Validate system environment"""
        self.logger.info("Validating V3 system environment...")
        
        # Check Python version
        if sys.version_info < (3, 8):
            self.logger.error(f"Python 3.8+ required. Current: {sys.version}")
            return False
        
        # Check .env file
        if not Path('.env').exists():
            self.logger.error(".env file not found!")
            return False
        
        # Check critical environment variables  
        critical_vars = ['BINANCE_API_KEY_1', 'BINANCE_API_SECRET_1']
        missing_vars = [var for var in critical_vars if not os.getenv(var)]
        
        if missing_vars:
            self.logger.error(f"Missing critical variables: {missing_vars}")
            return False
        
        # Create required directories
        for dir_name in ['data', 'logs', 'models']:
            try:
                Path(dir_name).mkdir(parents=True, exist_ok=True)
            except Exception as e:
                self.logger.error(f"Failed to create directory {dir_name}: {e}")
                return False
        
        self.logger.info("Environment validation passed")
        return True
    
    def check_port_availability(self) -> bool:
        """Check if required port is available"""
        import socket
        
        port = int(os.getenv('MAIN_SYSTEM_PORT', '8102'))
        
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            result = sock.bind(('localhost', port))
            sock.close()
            self.logger.info(f"Port {port} is available")
            return True
        except Exception as e:
            self.logger.error(f"Port {port} is not available: {e}")
            return False
    
    async def initialize_controller(self):
        """Initialize the trading controller with proper error handling"""
        try:
            self.logger.info("Initializing V3 Controller...")
            
            # Import and initialize the correct controller
            from main_controller import V3TradingController
            self.controller = V3TradingController()
            
            # Initialize the system
            success = await self.controller.initialize_system()
            if not success:
                raise RuntimeError("Controller initialization returned False")
            
            self.logger.info("Controller initialized successfully")
            return True
            
        except ImportError as e:
            self.logger.error(f"Import error: {e}")
            self.logger.info("Falling back to basic controller...")
            return self._initialize_fallback_controller()
            
        except Exception as e:
            self.logger.error(f"Controller initialization failed: {e}")
            traceback.print_exc()
            return False
    
    def _initialize_fallback_controller(self):
        """Initialize a minimal fallback controller"""
        try:
            self.logger.info("Setting up minimal controller...")
            
            class MinimalController:
                def __init__(self):
                    self.is_initialized = True
                    self.logger = logging.getLogger('MinimalController')
                
                async def initialize_system(self):
                    return True
                
                def run_flask_app(self):
                    from flask import Flask, render_template_string, jsonify
                    
                    app = Flask(__name__)
                    
                    @app.route('/')
                    def dashboard():
                        # Use the existing dashboard content
                        try:
                            with open('dashbored.html', 'r', encoding='utf-8') as f:
                                dashboard_content = f.read()
                            return dashboard_content
                        except FileNotFoundError:
                            return "<h1>V3 Trading System</h1><p>Dashboard file not found</p>"
                    
                    @app.route('/api/status')
                    def api_status():
                        return jsonify({
                            'status': 'running',
                            'mode': 'minimal',
                            'message': 'System running in minimal mode'
                        })
                    
                    port = int(os.getenv('FLASK_PORT', '8102'))
                    host = os.getenv('HOST', '0.0.0.0')
                    
                    self.logger.info(f"Starting minimal Flask server on {host}:{port}")
                    app.run(host=host, port=port, debug=False, threaded=True, use_reloader=False)
                
                async def shutdown(self):
                    self.logger.info("Minimal controller shutdown")
            
            self.controller = MinimalController()
            return True
            
        except Exception as e:
            self.logger.error(f"Fallback controller failed: {e}")
            return False
    
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
            
            if self.controller and hasattr(self.controller, 'shutdown'):
                try:
                    if asyncio.iscoroutinefunction(self.controller.shutdown):
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        loop.run_until_complete(self.controller.shutdown())
                        loop.close()
                    else:
                        self.controller.shutdown()
                except Exception as e:
                    self.logger.error(f"Controller shutdown error: {e}")
            
            self.logger.info("Shutdown sequence completed")
            
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")
        finally:
            threading.Timer(3.0, lambda: os._exit(0)).start()
    
    def start_flask_app(self):
        """Start Flask app in separate thread"""
        def run_flask():
            try:
                self.logger.info("Starting Flask server...")
                self.controller.run_flask_app()
            except Exception as e:
                self.logger.error(f"Flask error: {e}")
        
        self.flask_thread = threading.Thread(target=run_flask, daemon=True)
        self.flask_thread.start()
        time.sleep(2)  # Wait for Flask to start
        
        dashboard_port = int(os.getenv('FLASK_PORT', '8102'))
        self.logger.info(f"Dashboard available at: http://localhost:{dashboard_port}")
    
    def print_startup_banner(self):
        """Print startup banner"""
        port = int(os.getenv('MAIN_SYSTEM_PORT', '8102'))
        external_ip = os.getenv('EXTERNAL_IP', '185.202.239.125')
        
        print("=" * 80)
        print("V3 TRADING SYSTEM - COMPREHENSIVE ANALYSIS ENGINE")
        print("=" * 80)
        print("Version: V3.0-FIXED")
        print("Architecture: V1 Performance + V2 Infrastructure + V3 ML Enhancement")
        print(f"Dashboard: http://localhost:{port}")
        print(f"External Access: http://{external_ip}:{port}")
        print(f"Started: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("FEATURES:")
        print("  - Fixed backtesting progress tracking")
        print("  - Enhanced cross-component communication")
        print("  - Improved state management")
        print("  - Real-time dashboard updates")
        print("  - Comprehensive error handling")
        print("=" * 80)
    
    async def run_startup_checks(self):
        """Run comprehensive startup checks"""
        checks = [
            ("Environment validation", self.validate_environment),
            ("Port availability", self.check_port_availability),
            ("Controller initialization", self.initialize_controller),
        ]
        
        self.logger.info("Running V3 system startup checks...")
        
        for i, (check_name, check_func) in enumerate(checks, 1):
            try:
                if asyncio.iscoroutinefunction(check_func):
                    result = await check_func()
                else:
                    result = check_func()
                
                if result:
                    self.logger.info(f"Check {i}/{len(checks)}: {check_name} PASSED")
                else:
                    self.logger.error(f"Check {i}/{len(checks)}: {check_name} FAILED")
                    return False
                    
            except Exception as e:
                self.logger.error(f"Check {i}/{len(checks)}: {check_name} FAILED")
                self.logger.error(f"Error: {e}")
                return False
        
        return True
    
    async def run_system(self):
        """Main system run loop"""
        try:
            self.print_startup_banner()
            
            # Run startup checks
            if not await self.run_startup_checks():
                self.logger.error("System startup aborted due to failed checks")
                return False
            
            self.logger.info("All startup checks passed!")
            self.setup_signal_handlers()
            
            # Start Flask app
            self.start_flask_app()
            
            # Print status
            port = int(os.getenv('FLASK_PORT', '8102'))
            auto_start = os.getenv('AUTO_START_TRADING', 'false').lower() == 'true'
            
            print("\n" + "=" * 70)
            print("?? V3 TRADING SYSTEM READY!")
            print(f"?? Dashboard: http://localhost:{port}")
            print("=" * 70)
            
            if auto_start:
                print("\n?? AUTO_START_TRADING=true - Will start automatically")
                if hasattr(self.controller, 'start_trading'):
                    try:
                        result = await self.controller.start_trading()
                        if result and result.get('success'):
                            self.logger.info("Trading started automatically")
                        else:
                            self.logger.warning(f"Auto-start failed: {result.get('error') if result else 'Unknown error'}")
                    except Exception as e:
                        self.logger.error(f"Auto-start error: {e}")
            else:
                print(f"\n?? Monitor mode - Use dashboard to start trading")
                print(f"Dashboard: http://localhost:{port}")
            
            print("\n?? V3 System running... Press Ctrl+C to shutdown")
            
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
            self.logger.error(f"System error: {e}")
            traceback.print_exc()
            return False

def main():
    """Main function with comprehensive error handling"""
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
            system_manager.logger.error(f"Unhandled error: {e}")
            traceback.print_exc()
        else:
            print(f"Critical error: {e}")
            traceback.print_exc()
        sys.exit(1)
    finally:
        if system_manager.logger:
            system_manager.logger.info("V3 Trading System shutdown complete")

if __name__ == "__main__":
    print("?? Starting V3 Trading System...")
    print("?? Comprehensive Analysis Engine with Enhanced Error Handling")
    main()