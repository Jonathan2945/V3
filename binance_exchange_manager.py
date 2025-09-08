#!/usr/bin/env python3
"""
Compatible Binance US Exchange Manager - Maintains original interface
"""

import logging
import os
from dotenv import load_dotenv
from ccxt_binance_wrapper import CCXTBinanceUSWrapper

load_dotenv()

class BinanceUSEnvManager:
    """Environment-aware Binance US manager with original interface"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.testnet_mode = os.getenv('TESTNET', 'false').lower() == 'true'
        
        if self.testnet_mode:
            api_key = os.getenv('BINANCE_API_KEY_1', '')
            api_secret = os.getenv('BINANCE_API_SECRET_1', '')
            mode_name = "TESTNET"
        else:
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

# Create the global exchange_manager instance that other files expect
exchange_manager = BinanceUSEnvManager()

# Provide the missing functions that other files import
def get_tradeable_pairs():
    """Get tradeable pairs from Binance US"""
    try:
        exchange_info = exchange_manager.client.get_exchange_info()
        pairs = []
        for symbol_info in exchange_info['symbols']:
            if symbol_info['status'] == 'TRADING':
                pairs.append(symbol_info['symbol'])
        return pairs
    except Exception as e:
        logging.error(f"Failed to get tradeable pairs: {e}")
        return ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']  # Fallback pairs

def calculate_position_size(symbol, usdt_amount, current_price=None):
    """Calculate position size"""
    try:
        if current_price is None:
            ticker = exchange_manager.client.get_symbol_ticker(symbol)
            current_price = float(ticker['price'])
        
        quantity = usdt_amount / current_price
        return quantity
    except Exception as e:
        logging.error(f"Failed to calculate position size: {e}")
        return 0.0

def validate_order(symbol, side, quantity):
    """Validate order parameters"""
    try:
        if quantity <= 0:
            return False, "Invalid quantity"
        if side.upper() not in ['BUY', 'SELL']:
            return False, "Invalid side"
        return True, "Valid"
    except Exception as e:
        return False, f"Validation error: {e}"

# Test the compatible interface
if __name__ == "__main__":
    try:
        print("Testing compatible interface...")
        
        # Test exchange_manager
        balance = exchange_manager.get_account_balance('USDT')
        print(f"USDT Balance: ${balance:.2f}")
        
        # Test get_tradeable_pairs
        pairs = get_tradeable_pairs()
        print(f"Available pairs: {len(pairs)}")
        
        # Test calculate_position_size
        size = calculate_position_size('BTCUSDT', 2.0, 50000)
        print(f"Position size for $2: {size:.6f} BTC")
        
        # Test validate_order
        valid, msg = validate_order('BTCUSDT', 'BUY', 0.001)
        print(f"Order validation: {valid} - {msg}")
        
        testnet = os.getenv('TESTNET', 'false').lower() == 'true'
        mode = "TESTNET" if testnet else "LIVE"
        print(f"SUCCESS - Compatible interface ready for {mode} trading!")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
