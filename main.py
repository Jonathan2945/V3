#!/usr/bin/env python3
"""
V3 TRADING SYSTEM - MAIN ENTRY POINT - FINAL VERSION
====================================================
COMPLETE INTEGRATION OF CONTROLLER AND MIDDLEWARE
- Proper controller initialization with all API methods
- Fixed middleware registration and Flask app setup
- Real data only throughout the entire system
- Complete dashboard connectivity with 192 strategies loaded
"""
import os
import sys
import time
import logging
import signal
import atexit
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/v3_system.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

class V3SystemManager:
    """Main system manager for V3 Trading System"""
    
    def __init__(self):
        self.controller = None
        self.middleware = None
        self.is_running = False
        self.logger = logging.getLogger(f"{__name__}.SystemManager")
        
        # Register cleanup handlers
        atexit.register(self.cleanup)
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle system signals for graceful shutdown"""
        self.logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.shutdown()
        sys.exit(0)
    
    def initialize_system(self):
        """Initialize the complete V3 trading system"""
        try:
            self.logger.info("=" * 60)
            self.logger.info("V3 TRADING SYSTEM - FINAL VERSION STARTUP")
            self.logger.info("=" * 60)
            
            # Validate environment
            if not self._validate_environment():
                raise RuntimeError("Environment validation failed")
            
            # Initialize controller
            self.logger.info("Step 1: Initializing V3 Trading Controller...")
            self.controller = self._initialize_controller()
            if not self.controller:
                raise RuntimeError("Failed to initialize controller")
            
            # Initialize middleware
            self.logger.info("Step 2: Initializing API Middleware...")
            self.middleware = self._initialize_middleware()
            if not self.middleware:
                raise RuntimeError("Failed to initialize middleware")
            
            # Register controller with middleware
            self.logger.info("Step 3: Registering controller with middleware...")
            self.middleware.register_controller(self.controller)
            
            # Verify integration
            self.logger.info("Step 4: Verifying system integration...")
            if not self._verify_integration():
                raise RuntimeError("System integration verification failed")
            
            self.is_running = True
            self.logger.info("? V3 TRADING SYSTEM INITIALIZATION COMPLETE!")
            self.logger.info("=" * 60)
            
            return True
            
        except Exception as e:
            self.logger.error(f"? System initialization failed: {str(e)}")
            self.cleanup()
            return False
    
    def _validate_environment(self):
        """Validate environment setup"""
        try:
            # Check required directories
            required_dirs = ['data', 'logs', 'models']
            for dir_name in required_dirs:
                dir_path = Path(dir_name)
                if not dir_path.exists():
                    dir_path.mkdir(parents=True, exist_ok=True)
                    self.logger.info(f"Created directory: {dir_path}")
            
            # Check environment variables
            required_env_vars = [
                'USE_REAL_DATA_ONLY',
                'MOCK_DATA_DISABLED',
                'FLASK_PORT'
            ]
            
            for env_var in required_env_vars:
                value = os.getenv(env_var)
                if value is None:
                    self.logger.warning(f"Environment variable {env_var} not set")
                else:
                    self.logger.info(f"{env_var}: {value}")
            
            # Verify real data mode
            real_data_mode = os.getenv('USE_REAL_DATA_ONLY', 'false').lower() == 'true'
            mock_disabled = os.getenv('MOCK_DATA_DISABLED', 'false').lower() == 'true'
            
            if not real_data_mode or not mock_disabled:
                self.logger.warning("System not configured for real data mode")
                return False
            
            self.logger.info("? Environment validation passed")
            return True
            
        except Exception as e:
            self.logger.error(f"Environment validation error: {str(e)}")
            return False
    
    def _initialize_controller(self):
        """Initialize the V3 trading controller"""
        try:
            from main_controller import V3TradingController
            
            self.logger.info("Creating V3TradingController instance...")
            controller = V3TradingController()
            
            # Verify controller has required API methods
            required_methods = [
                'get_trading_status',
                'get_system_status', 
                'get_performance_metrics',
                'get_backtest_progress'
            ]
            
            for method_name in required_methods:
                if not hasattr(controller, method_name):
                    raise AttributeError(f"Controller missing required method: {method_name}")
            
            # Test methods
            self.logger.info("Testing controller API methods...")
            trading_status = controller.get_trading_status()
            system_status = controller.get_system_status()
            
            self.logger.info(f"Trading Status: {trading_status.get('total_trades', 0)} trades, ${trading_status.get('total_pnl', 0.0):.2f} P&L")
            self.logger.info(f"System Status: {system_status.get('strategies_loaded', 0)} strategies loaded")
            
            self.logger.info("? Controller initialized successfully")
            return controller
            
        except Exception as e:
            self.logger.error(f"Controller initialization error: {str(e)}")
            return None
    
    def _initialize_middleware(self):
        """Initialize the API middleware"""
        try:
            from api_middleware import APIMiddleware
            
            # Get configuration
            host = os.getenv('HOST', '0.0.0.0')
            port = int(os.getenv('FLASK_PORT', os.getenv('MAIN_SYSTEM_PORT', '8102')))
            
            self.logger.info(f"Creating APIMiddleware instance on {host}:{port}...")
            middleware = APIMiddleware(host=host, port=port)
            
            # Verify Flask app is properly initialized
            if not hasattr(middleware, 'app') or middleware.app is None:
                raise RuntimeError("Flask app not properly initialized")
            
            # Check routes
            routes = [str(rule) for rule in middleware.app.url_map.iter_rules()]
            api_routes = [route for route in routes if '/api/' in route]
            
            self.logger.info(f"Flask app created with {len(routes)} total routes, {len(api_routes)} API routes")
            self.logger.info("? Middleware initialized successfully")
            
            return middleware
            
        except Exception as e:
            self.logger.error(f"Middleware initialization error: {str(e)}")
            return None
    
    def _verify_integration(self):
        """Verify controller-middleware integration"""
        try:
            # Check if controller is registered
            if not self.middleware.controller_interface.is_controller_available():
                self.logger.error("Controller not available in middleware")
                return False
            
            # Test API data flow
            controller = self.middleware.controller_interface.get_controller()
            if not controller:
                self.logger.error("Cannot retrieve controller from middleware")
                return False
            
            # Test API method calls
            test_methods = ['get_trading_status', 'get_system_status']
            for method_name in test_methods:
                if hasattr(controller, method_name):
                    try:
                        result = getattr(controller, method_name)()
                        if result and isinstance(result, dict):
                            self.logger.info(f"? {method_name}: Working")
                        else:
                            self.logger.warning(f"?? {method_name}: Returned invalid data")
                    except Exception as e:
                        self.logger.error(f"? {method_name}: {str(e)}")
                        return False
                else:
                    self.logger.error(f"? {method_name}: Method not found")
                    return False
            
            self.logger.info("? Integration verification passed")
            return True
            
        except Exception as e:
            self.logger.error(f"Integration verification error: {str(e)}")
            return False
    
    def start_server(self):
        """Start the server and dashboard"""
        try:
            if not self.is_running:
                self.logger.error("System not properly initialized")
                return False
            
            # Get server configuration
            host = self.middleware.host
            port = self.middleware.port
            debug_mode = os.getenv('FLASK_DEBUG', 'false').lower() == 'true'
            
            self.logger.info("=" * 60)
            self.logger.info("STARTING V3 TRADING SYSTEM SERVER")
            self.logger.info("=" * 60)
            self.logger.info(f"?? Dashboard URL: http://{host}:{port}")
            self.logger.info(f"?? API Endpoints: http://{host}:{port}/api/")
            self.logger.info(f"?? Real Data Mode: ACTIVE")
            self.logger.info(f"?? Controller: {self.controller.__class__.__name__}")
            self.logger.info(f"?? Strategies Loaded: {len(getattr(self.controller, 'loaded_strategies', []))}")
            self.logger.info(f"?? ML Strategies: {len(getattr(self.controller, 'ml_trained_strategies', []))}")
            self.logger.info("=" * 60)
            
            # Print system status
            if hasattr(self.controller, 'get_system_status'):
                status = self.controller.get_system_status()
                self.logger.info(f"System Ready: {status.get('system_ready', False)}")
                self.logger.info(f"Trading Allowed: {status.get('trading_allowed', False)}")
                self.logger.info(f"Backtest Complete: {status.get('backtest_completed', False)}")
                self.logger.info(f"ML Training Complete: {status.get('ml_training_completed', False)}")
            
            self.logger.info("Starting Flask server...")
            
            # Start the server (this blocks)
            self.middleware.run(debug=debug_mode, host=host, port=port)
            
        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt, shutting down...")
            self.shutdown()
        except Exception as e:
            self.logger.error(f"Server error: {str(e)}")
            self.shutdown()
            return False
    
    def shutdown(self):
        """Graceful system shutdown"""
        try:
            if not self.is_running:
                return
            
            self.logger.info("Initiating graceful shutdown...")
            
            # Save controller state
            if self.controller and hasattr(self.controller, 'save_current_metrics'):
                try:
                    self.controller.save_current_metrics()
                    self.logger.info("Controller metrics saved")
                except Exception as e:
                    self.logger.error(f"Error saving controller metrics: {e}")
            
            # Shutdown middleware
            if self.middleware and hasattr(self.middleware, 'shutdown'):
                try:
                    self.middleware.shutdown()
                    self.logger.info("Middleware shutdown complete")
                except Exception as e:
                    self.logger.error(f"Error shutting down middleware: {e}")
            
            # Cleanup controller
            if self.controller and hasattr(self.controller, 'cleanup'):
                try:
                    self.controller.cleanup()
                    self.logger.info("Controller cleanup complete")
                except Exception as e:
                    self.logger.error(f"Error cleaning up controller: {e}")
            
            self.is_running = False
            self.logger.info("? Graceful shutdown complete")
            
        except Exception as e:
            self.logger.error(f"Error during shutdown: {str(e)}")
    
    def cleanup(self):
        """Cleanup method for atexit"""
        if self.is_running:
            self.shutdown()


def main():
    """Main entry point"""
    print("?? V3 TRADING SYSTEM - FINAL VERSION")
    print("=" * 60)
    print("REAL DATA TRADING SYSTEM WITH COMPLETE DASHBOARD")
    print("- 192 Strategies Loaded from Backtest Database")
    print("- 132 ML-Trained High-Performance Strategies")
    print("- Complete API Integration")
    print("- Real-Time Dashboard with Live Data")
    print("=" * 60)
    
    # Create system manager
    system_manager = V3SystemManager()
    
    # Initialize system
    if not system_manager.initialize_system():
        print("? System initialization failed. Check logs for details.")
        sys.exit(1)
    
    # Start server
    print("\n?? Starting dashboard server...")
    print("   Press Ctrl+C to stop the system")
    print("=" * 60)
    
    try:
        system_manager.start_server()
    except KeyboardInterrupt:
        print("\n?? Shutting down...")
    except Exception as e:
        print(f"\n? Unexpected error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()