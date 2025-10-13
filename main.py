#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V3 TRADING SYSTEM - COMPLETE FIXED VERSION
==========================================
Main entry point with full integration
"""
import asyncio
import logging
import signal
import sys
import os
from pathlib import Path
from datetime import datetime
import psutil
import gc
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s.%(funcName)s.%(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / 'v3_complete_system.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class V3SystemManager:
    """V3 System Manager - Complete Integration"""
    
    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.SystemManager")
        self.controller = None
        self.is_running = False
        self._shutdown_requested = False
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self.logger.info("=" * 80)
        self.logger.info("V3 COMPLETE TRADING SYSTEM - STARTING")
        self.logger.info("=" * 80)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info(f"Received signal {signum}, initiating shutdown...")
        self._shutdown_requested = True
        
        if self.controller:
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(self.shutdown())
                else:
                    loop.run_until_complete(self.shutdown())
            except Exception as e:
                self.logger.error(f"Error during signal shutdown: {e}")
                sys.exit(1)
    
    async def initialize(self):
        """Initialize the system"""
        try:
            self.logger.info("Initializing V3 Trading Controller...")
            
            # Import and initialize controller
            from main_controller import V3TradingController
            self.controller = V3TradingController()
            
            self.logger.info("Initializing system components...")
            success = await self.controller.initialize_system()
            
            if not success:
                self.logger.error("System initialization failed")
                return False
            
            self.logger.info("? System initialization complete")
            return True
            
        except Exception as e:
            self.logger.error(f"Initialization error: {e}", exc_info=True)
            return False
    
    async def run(self):
        """Main run loop"""
        try:
            # Initialize system
            if not await self.initialize():
                self.logger.error("Failed to initialize system")
                return False
            
            self.is_running = True
            
            # Check auto-start trading
            auto_start = os.getenv('AUTO_START_TRADING', 'false').lower() == 'true'
            
            if auto_start:
                self.logger.info("AUTO_START_TRADING enabled")
                result = self.controller.start_trading()
                if result.get('success'):
                    self.logger.info("Trading started automatically")
                else:
                    self.logger.warning(f"Failed to auto-start trading: {result.get('error')}")
            
            # Main monitoring loop
            last_health_check = 0
            health_check_interval = 60  # seconds
            
            while self.is_running and not self._shutdown_requested:
                try:
                    current_time = asyncio.get_event_loop().time()
                    
                    # Periodic health check
                    if current_time - last_health_check >= health_check_interval:
                        await self._health_check()
                        last_health_check = current_time
                        
                        # Save state periodically
                        self.controller.save_current_metrics()
                        self.logger.info("System state saved")
                    
                    # Memory management
                    if psutil.virtual_memory().percent > 85:
                        self.logger.warning("High memory usage detected, running garbage collection")
                        gc.collect()
                    
                    await asyncio.sleep(10)
                    
                except asyncio.CancelledError:
                    self.logger.info("Main loop cancelled")
                    break
                except Exception as e:
                    self.logger.error(f"Error in main loop: {e}", exc_info=True)
                    await asyncio.sleep(5)
            
            self.logger.info("Main loop exited")
            return True
            
        except Exception as e:
            self.logger.error(f"Fatal error in run loop: {e}", exc_info=True)
            return False
    
    async def _health_check(self):
        """Perform system health check"""
        try:
            if not self.controller:
                return
            
            metrics = self.controller.metrics
            
            total_trades = metrics.get('total_trades', 0)
            total_pnl = metrics.get('total_pnl', 0.0)
            win_rate = metrics.get('win_rate', 0.0)
            
            status = "TRADING" if self.controller.is_trading else "PAUSED"
            
            self.logger.info(
                f"System Health: {total_trades} trades, "
                f"${total_pnl:.2f} P&L, "
                f"{win_rate:.1f}% win rate, "
                f"Status: {status}"
            )
            
        except Exception as e:
            self.logger.error(f"Health check error: {e}")
    
    async def shutdown(self):
        """Graceful shutdown"""
        try:
            self.logger.info("Starting graceful shutdown...")
            self.is_running = False
            
            if self.controller:
                # Stop trading first
                if self.controller.is_trading:
                    self.logger.info("Stopping trading...")
                    self.controller.stop_trading()
                
                # Save final state
                self.logger.info("Saving final state...")
                self.controller.save_current_metrics()
                
                # Shutdown controller
                self.logger.info("Shutting down controller...")
                await self.controller.shutdown()
            
            self.logger.info("? Shutdown complete")
            
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}", exc_info=True)

async def main():
    """Main entry point"""
    manager = V3SystemManager()
    
    try:
        success = await manager.run()
        
        if not success:
            logger.error("System exited with errors")
            return 1
        
        logger.info("System exited successfully")
        return 0
        
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
        await manager.shutdown()
        return 0
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1
    finally:
        # Final cleanup
        try:
            await manager.shutdown()
        except:
            pass

if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except Exception as e:
        logger.error(f"Fatal startup error: {e}", exc_info=True)
        sys.exit(1)