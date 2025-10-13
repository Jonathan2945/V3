#!/usr/bin/env python3
"""
Manual backtest trigger to generate initial strategies
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from main_controller import V3TradingController
import asyncio
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def run_backtest():
    try:
        logger.info("Initializing controller for backtest...")
        controller = V3TradingController()
        
        logger.info("Starting comprehensive backtest...")
        result = controller.run_comprehensive_backtest()
        
        logger.info(f"Backtest triggered: {result}")
        
        # Wait for backtest to complete
        import time
        logger.info("Waiting for backtest to complete (this takes 2-3 minutes)...")
        
        for i in range(60):  # Wait up to 5 minutes
            time.sleep(5)
            status = controller.get_backtest_status()
            logger.info(f"Progress: {status.get('completed', 0)}/{status.get('total', 0)} - {status.get('status', 'unknown')}")
            
            if status.get('status') == 'Completed':
                logger.info("✅ Backtest completed successfully!")
                logger.info(f"Successful strategies: {status.get('successful', 0)}")
                break
        
        # Load strategies
        await controller._load_existing_strategies()
        logger.info(f"Loaded {len(controller.top_strategies)} strategies, {len(controller.ml_trained_strategies)} ML-trained")
        
        # Cleanup
        await controller.shutdown()
        
        logger.info("✅ Initial backtest complete! Restart main.py to begin trading.")
        
    except Exception as e:
        logger.error(f"Backtest failed: {e}", exc_info=True)
        return False
    
    return True

if __name__ == "__main__":
    success = asyncio.run(run_backtest())
    sys.exit(0 if success else 1)
