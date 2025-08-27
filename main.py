#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V3 TRADING SYSTEM - MAIN ENTRY POINT (COMPLETE FIXED VERSION)
==============================================================
This is the complete fixed main.py that integrates all the fixes for:
- Backtesting progress tracking
- Cross-communication issues  
- State management problems
- Dashboard integration
- Character encoding issues resolved

USAGE:
    python main.py                    # Normal startup
    python main.py --test-mode        # Test mode (limited backtesting)
    python main.py --auto-start       # Auto-start trading after backtest
    python main.py --clean-state      # Clean previous state on startup
"""

import os
import sys
import asyncio
import signal
import time
import argparse
from pathlib import Path
from datetime import datetime
import logging
import traceback

# Add current directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/v3_system.log', mode='a') if Path('logs').exists() or Path('logs').mkdir(exist_ok=True) else logging.NullHandler()
    ]
)

logger = logging.getLogger('V3_MAIN')

class V3SystemManager:
    """V3 Trading System Manager with integrated fixes"""
    
    def __init__(self, test_mode: bool = False, auto_start: bool = False, clean_state: bool = False):
        self.test_mode = test_mode
        self.auto_start = auto_start
        self.clean_state = clean_state
        
        # Configuration
        self.port = int(os.getenv('FLASK_PORT', 8102))
        self.host = os.getenv('HOST', '0.0.0.0')
        
        # System components
        self.controller = None
        self.system_running = False
        self.startup_time = datetime.now()
        
        logger.info(f"V3 System Manager initialized")
        logger.info(f"Configuration: Port={self.port}, TestMode={test_mode}, AutoStart={auto_start}")
    
    def validate_environment(self) -> bool:
        """Validate system environment and requirements"""
        logger.info("Validating V3 system environment...")
        
        errors = []
        warnings = []
        
        # Check .env file
        if not Path('.env').exists():
            errors.append(".env file not found")
        
        # Check required directories
        required_dirs = ['data', 'logs', 'backups']
        for dir_name in required_dirs:
            dir_path = Path(dir_name)
            if not dir_path.exists():
                dir_path.mkdir(exist_ok=True)
                warnings.append(f"Created missing directory: {dir_name}")
        
        # Check Python version
        if sys.version_info < (3, 8):
            errors.append(f"Python 3.8+ required, found {sys.version}")
        
        # Check critical environment variables
        critical_vars = [
            'BINANCE_API_KEY_1', 'BINANCE_API_SECRET_1',
            'COMPREHENSIVE_ANALYSIS_ENABLED', 'CLEAR_MOCK_ML_DATA'
        ]
        
        missing_vars = []
        for var in critical_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            warnings.append(f"Missing environment variables: {', '.join(missing_vars[:3])}")
        
        # Check V3 specific settings
        if os.getenv('USE_REAL_DATA_ONLY', '').lower() != 'true':
            warnings.append("USE_REAL_DATA_ONLY should be 'true' for V3")
        
        if os.getenv('ENABLE_MOCK_APIS', '').lower() != 'false':
            warnings.append("ENABLE_MOCK_APIS should be 'false' for V3")
        
        # Report validation results
        if errors:
            logger.error("Environment validation failed:")
            for error in errors:
                logger.error(f"  - {error}")
            return False
        
        if warnings:
            logger.warning("Environment validation warnings:")
            for warning in warnings:
                logger.warning(f"  - {warning}")
        
        logger.info("Environment validation passed")
        return True
    
    def clean_previous_state(self):
        """Clean previous system state if requested"""
        if not self.clean_state:
            return
        
        logger.info("Cleaning previous system state...")
        
        try:
            # Import and use the state cleanup utility
            from state_cleanup import V3StateCleanup
            
            cleanup = V3StateCleanup()
            success = cleanup.clean_backtest_progress()
            
            if success:
                logger.info("Previous state cleaned successfully")
            else:
                logger.warning("State cleanup had issues, continuing anyway")
                
        except ImportError:
            logger.warning("State cleanup utility not available, manual cleanup recommended")
        except Exception as e:
            logger.error(f"State cleanup failed: {e}")
    
    def check_port_availability(self) -> bool:
        """Check if the configured port is available"""
        try:
            import psutil
            
            for conn in psutil.net_connections():
                if conn.laddr.port == self.port and conn.status == 'LISTEN':
                    logger.error(f"Port {self.port} is already in use")
                    
                    # Try to identify the process
                    try:
                        proc = psutil.Process(conn.pid)
                        logger.error(f"   Process: {proc.name()} (PID: {conn.pid})")
                        logger.error(f"   Command: {' '.join(proc.cmdline())}")
                        logger.error(f"   Kill it with: kill {conn.pid}")
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        logger.error(f"   Process PID: {conn.pid}")
                    
                    return False
            
            logger.info(f"Port {self.port} is available")
            return True
            
        except ImportError:
            logger.warning("psutil not available, cannot check port availability")
            return True  # Assume it's available
        except Exception as e:
            logger.warning(f"Port check failed: {e}")
            return True  # Assume it's available
    
    def initialize_controller(self) -> bool:
        """Initialize the main V3 controller"""
        try:
            logger.info("Initializing V3 Controller...")
            
            # Import the controller (try both versions)
            try:
                from main_controller import FixedV3Controller
                self.controller = FixedV3Controller()
                logger.info("Using Fixed V3 Controller")
            except ImportError:
                logger.warning("Fixed controller not available, falling back to original")
                from main_controller import V3TradingController
                self.controller = V3TradingController()
            
            return True
            
        except Exception as e:
            logger.error(f"Controller initialization failed: {e}")
            traceback.print_exc()
            return False
    
    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating graceful shutdown...")
            self.shutdown()
            sys.exit(0)
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
        signal.signal(signal.SIGTERM, signal_handler)  # Termination request
        
        if hasattr(signal, 'SIGQUIT'):  # Unix only
            signal.signal(signal.SIGQUIT, signal_handler)  # Quit signal
    
    def display_startup_banner(self):
        """Display the V3 system startup banner"""
        print("\n" + "="*80)
        print("V3 TRADING SYSTEM - COMPREHENSIVE ANALYSIS ENGINE")
        print("="*80)
        print(f"Version: V3.0-FIXED")
        print(f"Architecture: V1 Performance + V2 Infrastructure + V3 ML Enhancement")
        print(f"Dashboard: http://localhost:{self.port}")
        print(f"External Access: http://185.202.239.125:{self.port}")
        print(f"Started: {self.startup_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        if self.test_mode:
            print("TEST MODE: Limited backtesting for faster testing")
        
        if self.auto_start:
            print("AUTO-START: Trading will begin after backtesting completes")
        
        print("\nFEATURES:")
        print("  - Fixed backtesting progress tracking")
        print("  - Enhanced cross-component communication")
        print("  - Improved state management")
        print("  - Real-time dashboard updates")
        print("  - Comprehensive error handling")
        print("="*80)
    
    def run_system_startup_checks(self) -> bool:
        """Run comprehensive system startup checks"""
        logger.info("Running V3 system startup checks...")
        
        checks_passed = 0
        total_checks = 6
        
        # Check 1: Environment validation
        if self.validate_environment():
            checks_passed += 1
            logger.info("Check 1/6: Environment validation PASSED")
        else:
            logger.error("Check 1/6: Environment validation FAILED")
            return False
        
        # Check 2: Port availability
        if self.check_port_availability():
            checks_passed += 1
            logger.info("Check 2/6: Port availability PASSED")
        else:
            logger.error("Check 2/6: Port availability FAILED")
            return False
        
        # Check 3: Controller initialization
        if self.initialize_controller():
            checks_passed += 1
            logger.info("Check 3/6: Controller initialization PASSED")
        else:
            logger.error("Check 3/6: Controller initialization FAILED")
            return False
        
        # Check 4: Database connectivity
        try:
            import sqlite3
            test_db = Path('data/test_connection.db')
            conn = sqlite3.connect(test_db)
            conn.execute("SELECT 1")
            conn.close()
            test_db.unlink(missing_ok=True)
            checks_passed += 1
            logger.info("Check 4/6: Database connectivity PASSED")
        except Exception as e:
            logger.error(f"Check 4/6: Database connectivity FAILED: {e}")
        
        # Check 5: Component imports
        try:
            component_count = 0
            components = [
                ('advanced_backtester', 'Advanced Backtester'),
                ('intelligent_trading_engine', 'Trading Engine'),
                ('price_action_core', 'Price Action Core'),
                ('api_rotation_manager', 'API Rotation Manager')
            ]
            
            for module_name, display_name in components:
                try:
                    __import__(module_name)
                    component_count += 1
                except ImportError:
                    logger.warning(f"Component {display_name} not available")
            
            if component_count >= 2:  # At least 2 core components
                checks_passed += 1
                logger.info(f"Check 5/6: Component imports PASSED ({component_count}/{len(components)})")
            else:
                logger.error(f"Check 5/6: Component imports FAILED ({component_count}/{len(components)})")
        except Exception as e:
            logger.error(f"Check 5/6: Component imports FAILED: {e}")
        
        # Check 6: Previous state cleanup
        try:
            self.clean_previous_state()
            checks_passed += 1
            logger.info("Check 6/6: State cleanup PASSED")
        except Exception as e:
            logger.warning(f"Check 6/6: State cleanup had issues: {e}")
            checks_passed += 1  # Don't fail startup for this
        
        # Summary
        logger.info(f"System checks: {checks_passed}/{total_checks} passed")
        
        if checks_passed >= total_checks - 1:  # Allow 1 failure
            logger.info("System startup checks passed")
            return True
        else:
            logger.error("System startup checks failed")
            return False
    
    def run_system(self):
        """Main system execution"""
        try:
            # Setup signal handlers
            self.setup_signal_handlers()
            
            # Display banner
            self.display_startup_banner()
            
            # Run startup checks
            if not self.run_system_startup_checks():
                logger.error("System startup aborted due to failed checks")
                return 1
            
            # Mark system as running
            self.system_running = True
            
            logger.info("Starting V3 Flask application...")
            
            # Set test mode environment variable if needed
            if self.test_mode:
                os.environ['TEST_MODE'] = 'true'
            
            if self.auto_start:
                os.environ['AUTO_START_TRADING'] = 'true'
            
            # Run the Flask application
            self.controller.run_flask_app()
            
        except KeyboardInterrupt:
            logger.info("Shutdown requested by user (Ctrl+C)")
            return 0
        except Exception as e:
            logger.error(f"System execution failed: {e}")
            traceback.print_exc()
            return 1
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Graceful system shutdown"""
        if not self.system_running:
            return
        
        logger.info("Initiating V3 system shutdown...")
        self.system_running = False
        
        try:
            if self.controller:
                self.controller.shutdown()
                logger.info("Controller shutdown complete")
            
            # Final cleanup
            uptime = datetime.now() - self.startup_time
            logger.info(f"System uptime: {str(uptime).split('.')[0]}")
            logger.info("V3 Trading System shutdown complete")
            
        except Exception as e:
            logger.error(f"Shutdown error: {e}")

def main():
    """Main entry point with argument parsing"""
    parser = argparse.ArgumentParser(
        description="V3 Trading System - Advanced Algorithmic Trading Platform",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                      # Normal startup
  python main.py --test-mode          # Test mode (faster, limited scope)
  python main.py --auto-start         # Auto-start trading after analysis
  python main.py --clean-state        # Clean previous state on startup
  python main.py --port 8105          # Use custom port
  
Environment:
  Set FLASK_PORT in .env to configure default port
  Set AUTO_START_TRADING=true to enable auto-start by default
  Set LOG_LEVEL=DEBUG for verbose logging
        """
    )
    
    parser.add_argument(
        '--test-mode', 
        action='store_true',
        help='Run in test mode (limited backtesting for faster testing)'
    )
    
    parser.add_argument(
        '--auto-start',
        action='store_true',
        help='Automatically start trading after backtesting completes'
    )
    
    parser.add_argument(
        '--clean-state',
        action='store_true',
        help='Clean previous system state on startup (fixes "already started" issues)'
    )
    
    parser.add_argument(
        '--port',
        type=int,
        help='Override the Flask port (default from .env FLASK_PORT)'
    )
    
    parser.add_argument(
        '--host',
        type=str,
        default='0.0.0.0',
        help='Flask host address (default: 0.0.0.0)'
    )
    
    parser.add_argument(
        '--version',
        action='version',
        version='V3.0-FIXED (Enhanced Multi-Timeframe Trading System)'
    )
    
    args = parser.parse_args()
    
    # Override environment variables if specified
    if args.port:
        os.environ['FLASK_PORT'] = str(args.port)
    
    if args.host:
        os.environ['HOST'] = args.host
    
    # Handle special arguments
    if args.auto_start:
        os.environ['AUTO_START_TRADING'] = 'true'
    
    try:
        # Create and run system manager
        system_manager = V3SystemManager(
            test_mode=args.test_mode,
            auto_start=args.auto_start,
            clean_state=args.clean_state
        )
        
        exit_code = system_manager.run_system()
        sys.exit(exit_code)
        
    except Exception as e:
        print(f"\nCRITICAL ERROR: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()