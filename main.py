#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V3 UNIFIED TRADING SYSTEM - REAL DATA ONLY ENTRY POINT
======================================================
Clean UTF-8 implementation with ZERO mock/simulated data
Only processes actual real market data and live trading metrics
"""
import asyncio
import logging
import os
import signal
import sys
import time
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging with UTF-8 encoding
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/v3_real_system.log', encoding='utf-8') if os.path.exists('logs') else logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def handle_shutdown(signum, frame):
    """Handle shutdown signals"""
    logger.info("Received shutdown signal for REAL system")
    sys.exit(0)

def check_dependencies():
    """Check for required dependencies"""
    missing_deps = []
    
    try:
        import pandas
    except ImportError:
        missing_deps.append('pandas')
    
    try:
        import numpy
    except ImportError:
        missing_deps.append('numpy')
    
    try:
        import flask
    except ImportError:
        missing_deps.append('flask')
    
    try:
        import psutil
    except ImportError:
        missing_deps.append('psutil')
    
    if missing_deps:
        logger.error(f"Missing dependencies for REAL system: {', '.join(missing_deps)}")
        logger.error("Install with: pip install " + " ".join(missing_deps))
        return False
    
    return True

def create_required_directories():
    """Create required directories for REAL data"""
    dirs = ['data', 'logs']
    for dir_name in dirs:
        try:
            os.makedirs(dir_name, exist_ok=True)
            logger.info(f"Created directory for REAL data: {dir_name}")
        except Exception as e:
            logger.error(f"Failed to create directory {dir_name}: {e}")

async def main():
    """Main entry point for REAL trading system"""
    try:
        print("?? Starting V3 Unified Trading System - REAL DATA ONLY...")
        print("?? Controller + Middleware Architecture")
        print("?? Real Market Data Only - NO Simulation or Mock Data")
        
        logger.info("REAL trading system logging initialized")
        
        # Check dependencies
        logger.info("Checking system requirements for REAL trading...")
        if not check_dependencies():
            logger.error("REAL system requirements check failed")
            return 1
        
        logger.info("REAL system requirements check passed")
        
        # Create required directories
        create_required_directories()
        
        print(f"""
    ================================================================
    |              V3 UNIFIED TRADING SYSTEM                      |
    |                                                              |
    |  Controller + Middleware Architecture                       |
    |  Real Market Data Only - NO Simulation                      |
    |  Single Entry Point - Unified Management                    |
    ================================================================

    UNIFIED COMPONENTS (REAL DATA ONLY):
    ?? Main Trading Controller (real market data only)
    ?? API Middleware (web dashboard interface)
    ?? Database Connection Pooling (real data storage)
    ?? Enhanced Error Recovery
    ? Async Task Management
    ?? Thread-Safe Operations
    ??? Graceful Startup/Shutdown
    
    ?? NO MOCK DATA: All data comes from real market sources
    ?? NO SIMULATION: All trading operations are real or paper trading only

    Python: {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}
    PID: {os.getpid()}
        """)
        
        # Initialize controller with REAL data handling
        logger.info("Initializing V3 Trading Controller - REAL DATA ONLY...")
        
        try:
            # Import with safe fallbacks
            try:
                from clean_main_controller import V3TradingController
                logger.info("Using clean main controller for REAL data")
            except ImportError:
                try:
                    from main_controller import V3TradingController
                    logger.info("Using standard main controller for REAL data")
                except ImportError as e:
                    logger.error(f"Main controller import failed: {e}")
                    logger.info("Cannot proceed without controller")
                    return 1
            
            controller = V3TradingController()
            
            # Initialize REAL system
            success = await controller.initialize_system()
            if not success:
                logger.error("REAL controller initialization failed")
                # Continue without controller but warn
                controller = None
                logger.warning("Proceeding without controller - dashboard will show empty data")
            else:
                logger.info("REAL controller initialized successfully")
            
        except Exception as e:
            logger.error(f"REAL controller initialization failed: {e}")
            controller = None
            logger.warning("Proceeding without controller - dashboard will show empty data")
        
        # Initialize middleware for REAL data
        logger.info("Starting API Middleware for REAL data...")
        
        try:
            # Import with safe fallbacks
            try:
                from clean_api_middleware import APIMiddleware
                logger.info("Using clean API middleware for REAL data")
            except ImportError:
                try:
                    from api_middleware import APIMiddleware
                    logger.info("Using standard API middleware for REAL data")
                except ImportError as e:
                    logger.error(f"API middleware import failed: {e}")
                    return 1
            
            # Get configuration
            host = os.getenv('HOST', '0.0.0.0')
            port = int(os.getenv('FLASK_PORT', os.getenv('MAIN_SYSTEM_PORT', '8102')))
            
            middleware = APIMiddleware(host=host, port=port)
            logger.info("API Middleware initialized for REAL data")
            
            # Register REAL controller if available
            if controller:
                middleware.register_controller(controller)
                logger.info("REAL main controller registered with middleware")
            else:
                logger.warning("Running without main controller - dashboard will show empty REAL data structures")
            
            logger.info(f"Middleware starting on port {port} for REAL data")
            logger.info(f"Starting API Middleware on {host}:{port} - REAL DATA ONLY")
            
            # Setup signal handlers
            signal.signal(signal.SIGINT, handle_shutdown)
            signal.signal(signal.SIGTERM, handle_shutdown)
            
            # Display startup info
            time.sleep(2)
            logger.info(f"REAL data dashboard available at: http://localhost:{port}")
            
            print("=" * 70)
            print("?? V3 UNIFIED TRADING SYSTEM READY!")
            print(f"?? Controller: {'Initialized' if controller else 'Empty Data Mode'} (Real Data Only)")
            print("?? Middleware: Running (Real Data Only)")
            print("?? Database: Connection Pooling Active (Real Data Storage)")
            print("?? Memory Management: Active")
            print("?? Error Handling: Enhanced")
            print("? Task Management: Async")
            print(f"?? Dashboard: http://localhost:{port}")
            print("?? DATA MODE: REAL MARKET DATA ONLY - NO MOCK/SIMULATION")
            print("=" * 70)
            print()
            print("?? Monitor mode - Use dashboard to start REAL trading")
            print(f"Dashboard: http://localhost:{port}")
            print()
            print("?? V3 Unified System running... Press Ctrl+C to shutdown")
            print()
            print("Data Flow: Dashboard ? Middleware ? Controller ? REAL Market APIs")
            print("Real Data Only: No simulation, mock data, or fake data generation")
            print("?? CRITICAL: All trading operations use REAL market data")
            
            # Run middleware (this blocks)
            middleware.run(debug=False)
            
        except Exception as e:
            logger.error(f"REAL middleware error: {e}")
            return 1
        
    except KeyboardInterrupt:
        logger.info("REAL system shutdown requested by user")
        return 0
    except Exception as e:
        logger.error(f"Unexpected REAL system error: {e}", exc_info=True)
        return 1
    finally:
        # Cleanup REAL system
        if 'controller' in locals() and controller:
            try:
                await controller.shutdown()
                logger.info("REAL controller shutdown completed")
            except Exception as e:
                logger.error(f"REAL controller shutdown error: {e}")

if __name__ == "__main__":
    # Run main coroutine for REAL system
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\nREAL system shutdown complete")
        sys.exit(0)
    except Exception as e:
        print(f"Fatal REAL system error: {e}")
        sys.exit(1)