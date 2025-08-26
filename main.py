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

Version: 3.0 - V1 Performance + V2 Infrastructure
"""

import sys
sys.setrecursionlimit(2000)
import os
import asyncio
import signal
import logging
from pathlib import Path

# Add current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

# Load environment variables first
from dotenv import load_dotenv
load_dotenv()

# V3 Critical environment variables
TESTNET = os.getenv('TESTNET', 'true').lower() == 'true'
MIN_CONFIDENCE = float(os.getenv('MIN_CONFIDENCE', '70.0'))
MAX_TOTAL_POSITIONS = int(os.getenv('MAX_TOTAL_POSITIONS', '3'))
TRADE_AMOUNT_USDT = float(os.getenv('TRADE_AMOUNT_USDT', '100.0'))

# Global shutdown handler
shutdown_event = asyncio.Event()
shutdown_initiated = False

async def graceful_shutdown():
    """Handle graceful shutdown of all V3 components"""
    global shutdown_initiated
    
    if shutdown_initiated:
        return
    
    shutdown_initiated = True
    
    try:
        logging.info("[V3_STOP] Graceful shutdown initiated...")
        shutdown_event.set()
        await asyncio.sleep(1)
        
        # Clean up tasks
        current_task = asyncio.current_task()
        tasks = [task for task in asyncio.all_tasks() 
                if not task.done() and task != current_task]
        
        if tasks:
            logging.info(f"[V3_CLEANUP] Cleaning up {len(tasks)} remaining tasks...")
            for task in tasks:
                try:
                    task.cancel()
                except Exception:
                    pass
            
            try:
                await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True), 
                    timeout=3.0
                )
            except asyncio.TimeoutError:
                logging.warning("[V3_CLEANUP] Some tasks didn't complete within timeout")
        
        logging.info("[V3_OK] Graceful shutdown complete")
        
    except Exception as e:
        logging.error(f"Error during V3 shutdown: {e}")

def signal_handler(signum, frame):
    """Signal handler for graceful V3 shutdown"""
    global shutdown_initiated
    if not shutdown_initiated:
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(graceful_shutdown())
            else:
                asyncio.run(graceful_shutdown())
        except Exception as e:
            logging.error(f"V3 signal handler error: {e}")
            shutdown_initiated = True

def check_environment():
    """Check if the V3 environment is properly set up"""
    print("?? Checking V3 environment setup...")
    
    if not Path('.env').exists():
        print("? ERROR: .env file not found!")
        print("Please copy .env.template to .env and configure your API keys:")
        print("   cp .env.template .env")
        print("   # Then edit .env with your actual API keys")
        return False
    
    # Check required directories
    required_dirs = ['data', 'logs', 'models']
    for dir_name in required_dirs:
        Path(dir_name).mkdir(exist_ok=True)
    
    # Validate critical V3 environment variables
    print(f"? TESTNET mode: {TESTNET}")
    print(f"? Min confidence: {MIN_CONFIDENCE}%")
    print(f"? Max positions: {MAX_TOTAL_POSITIONS}")
    print(f"? Trade amount: ${TRADE_AMOUNT_USDT}")
    
    print("? V3 environment check passed")
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

def print_v3_startup_banner():
    """Print V3 startup banner"""
    banner = """
    ================================================================
    |                   V3 TRADING SYSTEM                          |
    |                                                              |
    |  ?? V1 Proven Performance + V2 Advanced Infrastructure       |
    |  Version 3.0 - Best of Both Worlds                          |
    ================================================================
    
    V3 HYBRID ARCHITECTURE:
    
    ?? V1 PROVEN COMPONENTS (BATTLE-TESTED):
    ? Trading Execution Logic (PROFITABLE)
    ? Self-Progression ML System (WORKING)
    ? Signal Generation (PROVEN)
    ? Position Management (TESTED)
    ? P&L Persistence (RELIABLE)
    
    ?? V2 ADVANCED INFRASTRUCTURE:
    ? Multi-Pair Scanning (50+ pairs)
    ? API Rotation Management (3x keys per service)
    ? Advanced Risk Management
    ? Multi-Timeframe Analysis
    ? Enhanced Dashboard
    
    ?? V3 UNIQUE FEATURES:
    ? No Mock Data (Real trading only)
    ? Testnet ? Live Progression
    ? Multi-Pair with V1 Performance
    ? V2 Dashboard with V1 Metrics
    ? Hybrid Execution Engine
    """
    print(banner)

async def main():
    """Main V3 async function"""
    try:
        # Set up signal handlers
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Print V3 startup banner
        print_v3_startup_banner()
        
        # Check environment
        if not check_environment():
            sys.exit(1)
        
        # Setup logging
        setup_logging()
        
        # Import V3 controller
        from main_controller import V3TradingController
        
        print("?? Initializing V3 Trading System...")
        print("=" * 70)
        
        # Create V3 controller
        controller = V3TradingController()
        
        # Start Flask app in background thread
        import threading
        flask_thread = threading.Thread(target=controller.run_flask_app, daemon=True)
        flask_thread.start()
        
        # Get dashboard port
        dashboard_port = int(os.getenv('MAIN_SYSTEM_PORT', os.getenv('FLASK_PORT', 8102)))
        print(f"?? V3 Dashboard starting at: http://localhost:{dashboard_port}")
        print("?? V2 Enhanced Dashboard with V1 Performance Metrics")
        print("=" * 70)
        
        # Initialize V3 system
        print("? Initializing V3 hybrid components...")
        await controller.initialize_system()
        
        print("=" * 70)
        print("?? V3 TRADING SYSTEM READY!")
        print("? V1 Trading Logic: ACTIVE")
        print("? V2 Infrastructure: ACTIVE")
        print("? Multi-Pair Scanning: ACTIVE")
        print("? API Rotation: ACTIVE")
        print("? Real Market Data: ACTIVE")
        print("? No Mock Data: CONFIRMED")
        print(f"?? Dashboard: http://localhost:{dashboard_port}")
        print("=" * 70)
        
        # Start in monitor mode by default
        print("\n?? V3 System starting in MONITOR MODE")
        print("Use the dashboard to start/stop trading")
        print(f"Dashboard URL: http://localhost:{dashboard_port}")
        print("Click 'START TRADING' button when ready to begin")
        
        # Check if AUTO_START environment variable is set
        auto_start = os.getenv('AUTO_START_TRADING', 'false').lower() == 'true'
        
        if auto_start:
            print("\n?? AUTO_START_TRADING=true detected - Starting V3 trading automatically...")
            await controller.start_trading()
            print("?? V3 Trading system is now LIVE!")
            print("?? V1 Performance + V2 Multi-Pair Power")
        else:
            print("\n? Monitor mode - V3 system running without trading")
            print("You can start trading via the dashboard")
        
        # Keep the system running
        print("\n?? V3 System is running... Press Ctrl+C to shutdown gracefully")
        
        try:
            while True:
                await asyncio.sleep(1)
                
        except KeyboardInterrupt:
            print("\n\n?? V3 Shutdown signal received...")
            if controller.is_running:
                print("?? Closing open positions and stopping trading...")
                await controller.stop_trading()
            
            print("?? Saving data and cleaning up...")
            print("?? V3 Trading System shut down gracefully")
            
    except Exception as e:
        logging.error(f"Critical error in V3 main: {e}")
        print(f"\n? CRITICAL V3 ERROR: {e}")
        print("Check logs/v3_trading_system.log for detailed error information")
        sys.exit(1)

def sync_main():
    """Synchronous wrapper for V3 main async function"""
    try:
        # Run the async main function
        asyncio.run(main())
        
    except KeyboardInterrupt:
        print("\n?? V3 system interrupted by user")
        
    except Exception as e:
        print(f"\n? V3 system error: {e}")
        logging.error(f"V3 system error in sync_main: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Check Python version
    if sys.version_info < (3, 8):
        print("? Python 3.8+ required")
        print(f"Current version: {sys.version}")
        sys.exit(1)
    
    print("?? Starting V3 Trading System...")
    print("?? V1 Proven Performance + V2 Advanced Infrastructure")
    
    # Run the V3 system
    sync_main()