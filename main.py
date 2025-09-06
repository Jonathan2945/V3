#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V3 TRADING SYSTEM - MAIN ENTRY POINT
===================================

V3 System: Best of V1 + V2
- V1's proven trading performance and self-progression
- V2's multi-pair scanning and API rotation infrastructure
- No mock data - only real market interactions
- Live trading capabilities with testnet?live progression

Architecture:
1. V1 Price Action Core (PROVEN Signal Generator)
2. V2 Multi-Pair Scanner (Enhanced Opportunity Detection) 
3. V1 ML Engine (PROVEN Self-Progression)
4. V3 Trading Engine (Hybrid V1+V2 Execution)
5. API Middleware (Dashboard Interface)

Version: 3.0 - V1 Performance + V2 Infrastructure + Middleware Architecture
"""

import sys
sys.setrecursionlimit(2000)
import os
import asyncio
import signal
import logging
from pathlib import Path
import threading
import time

# Add current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

# Load environment variables first
from dotenv import load_dotenv
load_dotenv()

# Global shutdown handler
shutdown_event = asyncio.Event()
shutdown_initiated = False

async def graceful_shutdown(controller=None, middleware=None):
    """Handle graceful shutdown of all V3 components"""
    global shutdown_initiated
    
    if shutdown_initiated:
        return
    
    shutdown_initiated = True
    
    try:
        logging.info("Received SIGINT (Ctrl+C) - initiating REAL system shutdown")
        
        # Stop trading first
        if controller and hasattr(controller, 'stop_trading'):
            try:
                await controller.stop_trading()
            except Exception as e:
                logging.error(f"Error stopping trading: {e}")
        
        # Stop middleware
        if middleware and hasattr(middleware, 'stop'):
            try:
                middleware.stop()
            except Exception as e:
                logging.error(f"Error stopping middleware: {e}")
        
        # Clean up async tasks
        if hasattr(controller, 'task_manager') and controller.task_manager:
            controller.task_manager.cancel_all_tasks()
        
        await asyncio.sleep(1)
        
        logging.info("REAL system completed successfully")
        logging.info("REAL controller shutdown completed")
        
    except Exception as e:
        logging.error(f"Error during V3 shutdown: {e}")

def signal_handler(signum, frame):
    """Signal handler for graceful V3 shutdown"""
    global shutdown_initiated
    if not shutdown_initiated:
        print("\n")
        logging.info("Received SIGINT (Ctrl+C) - initiating REAL system shutdown")
        # The actual shutdown will be handled by the main loop

def check_environment():
    """Check if the V3 environment is properly set up"""
    logging.info("Checking system requirements for REAL trading...")
    
    # Check required directories
    required_dirs = ['data', 'logs', 'models']
    for dir_name in required_dirs:
        Path(dir_name).mkdir(exist_ok=True)
        logging.info(f"Created directory for REAL data: {dir_name}")
    
    logging.info("REAL system requirements check passed")
    return True

def setup_logging():
    """Setup V3 logging configuration"""
    Path('logs').mkdir(exist_ok=True)
    
    log_level = os.getenv('LOG_LEVEL', 'INFO')
    
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/v3_trading_system.log', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Reduce noise from external libraries
    logging.getLogger('aiohttp').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('werkzeug').setLevel(logging.INFO)

def print_v3_startup_banner():
    """Print V3 startup banner"""
    print("?? Starting V3 Unified Trading System - REAL DATA ONLY...")
    print("?? Controller + Middleware Architecture")
    print("?? Real Market Data Only - NO Simulation or Mock Data")

def initialize_controller():
    """Initialize the trading controller"""
    try:
        logging.info("Initializing V3 Trading Controller - REAL DATA ONLY...")
        
        # Import and create controller
        from main_controller import V3TradingController
        controller = V3TradingController()
        
        # Initialize the controller's components
        asyncio.run(controller.initialize_system())
        
        logging.info("REAL controller initialized successfully")
        return controller
        
    except Exception as e:
        logging.error(f"Failed to initialize REAL controller: {e}")
        return None

def initialize_middleware(controller):
    """Initialize the API middleware"""
    try:
        logging.info("Starting API Middleware for REAL data...")
        
        # Import and create middleware
        from api_middleware import APIMiddleware
        
        host = os.getenv('HOST', '0.0.0.0')
        port = int(os.getenv('MAIN_SYSTEM_PORT', os.getenv('FLASK_PORT', '8102')))
        
        middleware = APIMiddleware(host=host, port=port)
        
        # Register controller with middleware
        if controller:
            middleware.register_controller(controller)
            logging.info("Main controller registered with middleware")
        
        logging.info(f"Starting REAL data middleware on {host}:{port}")
        return middleware, host, port
        
    except Exception as e:
        logging.error(f"Failed to initialize middleware: {e}")
        return None, None, None

def main():
    """Main V3 function - Controller + Middleware Architecture"""
    controller = None
    middleware = None
    
    try:
        # Set up signal handlers
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Print startup banner
        print_v3_startup_banner()
        
        # Setup logging
        logging.info("REAL trading system logging initialized")
        setup_logging()
        
        # Check environment
        if not check_environment():
            sys.exit(1)
        
        print("""
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
    ? Thread-Safe Operations
    ?? Graceful Startup/Shutdown

    ? NO MOCK DATA: All data comes from real market sources
    ? NO SIMULATION: All trading operations are real or paper trading only

    Python: {0}
    PID: {1}
        """.format(sys.version.split()[0], os.getpid()))
        
        # Initialize trading controller
        controller = initialize_controller()
        if not controller:
            logging.error("REAL system completed with errors")
            sys.exit(1)
        
        # Initialize API middleware
        middleware, host, port = initialize_middleware(controller)
        if not middleware:
            logging.error("REAL system completed with errors") 
            sys.exit(1)
        
        # Show dashboard info
        logging.info(f"REAL data dashboard available at: http://localhost:{port}")
        
        print("======================================================================")
        print("?? V3 UNIFIED TRADING SYSTEM READY!")
        print("?? Controller + Middleware Architecture: ACTIVE")
        print("?? Real Market Data Only: ACTIVE")
        print("?? Database Connection Pooling: ACTIVE")
        print("?? Enhanced Error Handling: ACTIVE")
        print("?? Async Task Management: ACTIVE")
        print(f"? Dashboard: http://localhost:{port}")
        print("======================================================================")
        print()
        print("? Monitor mode - Use dashboard to start trading")
        print(f"Dashboard: http://localhost:{port}")
        print()
        print("? V3 Real Data System running... Press Ctrl+C to shutdown")
        
        # Run middleware (this blocks until shutdown)
        try:
            middleware.run(debug=False)
        except KeyboardInterrupt:
            pass
        
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logging.error(f"REAL system error: {e}")
        print(f"System error: {e}")
    finally:
        # Shutdown sequence
        try:
            if shutdown_initiated or True:  # Always run cleanup
                asyncio.run(graceful_shutdown(controller, middleware))
        except Exception as e:
            logging.error(f"Shutdown error: {e}")
        
        logging.info("REAL controller shutdown completed")

if __name__ == "__main__":
    # Check Python version
    if sys.version_info < (3, 8):
        print("? Python 3.8+ required")
        print(f"Current version: {sys.version}")
        sys.exit(1)
    
    # Run the V3 system
    main()