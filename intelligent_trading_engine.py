#!/usr/bin/env python3
"""
Intelligent Trading Engine - Bulletproof Version
No f-strings, no format errors, guaranteed to work
"""
import logging
from binance.client import Client
import os
from dotenv import load_dotenv

load_dotenv()

class IntelligentTradingEngine:
    """Minimal bulletproof trading engine"""
    
    def __init__(self, data_manager=None, data_collector=None, market_analyzer=None, ml_engine=None):
        """Initialize - guaranteed no crashes"""
        self.logger = logging.getLogger(__name__)
        self.data_manager = data_manager
        self.data_collector = data_collector
        self.market_analyzer = market_analyzer
        self.ml_engine = ml_engine
        self.client = None
        
        # Safe initialization
        self._init_binance_safe()
        
        self.logger.info("Trading engine initialized")
    
    def _init_binance_safe(self):
        """100% safe Binance initialization"""
        try:
            testnet = os.getenv('TESTNET', 'true').lower() == 'true'
            
            if testnet:
                key = os.getenv('BINANCE_API_KEY_1')
                secret = os.getenv('BINANCE_API_SECRET_1')
            else:
                key = os.getenv('BINANCE_LIVE_API_KEY_1')
                secret = os.getenv('BINANCE_LIVE_API_SECRET_1')
            
            if key and secret:
                self.client = Client(key, secret, testnet=testnet)
                self.logger.info("Binance client connected successfully")
            else:
                self.logger.warning("No Binance credentials found")
                
        except Exception as e:
            self.logger.error("Binance init failed: " + str(e)[:50])
            self.client = None
