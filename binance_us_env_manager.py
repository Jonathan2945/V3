#!/usr/bin/env python3
"""
Environment-aware Binance US manager - Switches between testnet/live via .env
"""

import logging
import os
from dotenv import load_dotenv
from ccxt_binance_wrapper import CCXTBinanceUSWrapper

load_dotenv()

class BinanceUSEnvManager:
    """Binance US manager that respects TESTNET environment variable"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Read from environment like your original system
        self.testnet_mode = os.getenv('TESTNET', 'false').lower() == 'true'
        
        if self.testnet_mode:
            # Use regular keys for testnet
            api_key = os.getenv('BINANCE_API_KEY_1', '')
            api_secret = os.getenv('BINANCE_API_SECRET_1', '')
            mode_name = "TESTNET"
        else:
            # Use live keys for real trading
            api_key = os.getenv('BINANCE_LIVE_API_KEY_1', '')
            api_secret = os.getenv('BINANCE_LIVE_API_SECRET_1', '')
            mode_name = "LIVE"
        
        if not api_key or not api_secret:
            raise Exception(f"No {mode_name} API keys configured")
        
        self.client = CCXTBinanceUSWrapper(api_key, api_secret, self.testnet_mode)
        self.logger.info(f"{mode_name} TRADING - Binance US connected")
    
    def get_account_balance(self, asset=None):
        """Get account balance"""
        try:
            account = self.client.get_account()
            
            if asset:
                for balance in account['balances']:
                    if balance['asset'] == asset:
                        return float(balance['free'])
                return 0.0
            else:
                balances = {}
                for balance in account['balances']:
                    free = float(balance['free'])
                    locked = float(balance['locked'])
                    if free > 0 or locked > 0:
                        balances[balance['asset']] = {
                            'free': free,
                            'locked': locked,
                            'total': free + locked
                        }
                return balances
        except Exception as e:
            self.logger.error(f"Failed to get balance: {e}")
            return {} if asset is None else 0.0
    
    def place_market_order(self, symbol, side, quantity):
        """Place market order"""
        try:
            mode = "TESTNET" if self.testnet_mode else "REAL"
            self.logger.warning(f"{mode} TRADE: {side} {quantity} {symbol}")
            
            if side.upper() == 'BUY':
                order = self.client.order_market_buy(symbol, quantity)
            else:
                order = self.client.order_market_sell(symbol, quantity)
            
            self.logger.info(f"{mode} TRADE EXECUTED: {order['orderId']}")
            return order
        except Exception as e:
            self.logger.error(f"Trade failed: {e}")
            return None

# Test with current environment
if __name__ == "__main__":
    try:
        manager = BinanceUSEnvManager()
        
        testnet = os.getenv('TESTNET', 'false').lower() == 'true'
        mode = "TESTNET" if testnet else "LIVE"
        
        print(f"SUCCESS - {mode} manager ready!")
        
        usdt_balance = manager.get_account_balance('USDT')
        print(f"USDT Balance: ${usdt_balance:.2f}")
        
        print(f"\nTo switch modes:")
        print(f"TESTNET=true   -> Uses virtual money")
        print(f"TESTNET=false  -> Uses real money (current)")
        
    except Exception as e:
        print(f"Error: {e}")
