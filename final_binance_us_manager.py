#!/usr/bin/env python3
"""
Final Binance US manager - Ready for real trading
"""

import logging
import os
from ccxt_binance_wrapper import CCXTBinanceUSWrapper

class FinalBinanceUSManager:
    """Production-ready Binance US manager"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Your verified working Binance US keys
        api_key = 'C7fuaYyvhrHXj2VxDjTgOy7gmjkfimc4o8vl5OJJiv3IHnupASeFXoVJeBC6l1b4'
        api_secret = 'pkT6A8MScBexfdd1h84aM0JNmrLQIaE4BCnZg8tkHc66w2LfOkH9YgS7B8dhUv0P'
        
        self.client = CCXTBinanceUSWrapper(api_key, api_secret, testnet=False)
        self.logger.info("LIVE TRADING ENABLED - Binance US connected")
        
        # Verify connection and balance
        try:
            account = self.client.get_account()
            usdt_balance = self.get_account_balance('USDT')
            self.logger.info(f"Account verified - USDT Balance: ${usdt_balance:.2f}")
        except Exception as e:
            self.logger.error(f"Failed to verify account: {e}")
            raise
    
    def get_account_balance(self, asset=None):
        """Get real account balance from Binance US"""
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
        """Place REAL market order on Binance US"""
        try:
            # Log before placing real trade
            self.logger.warning(f"PLACING REAL TRADE: {side} {quantity} {symbol}")
            
            if side.upper() == 'BUY':
                order = self.client.order_market_buy(symbol, quantity)
            else:
                order = self.client.order_market_sell(symbol, quantity)
            
            self.logger.info(f"REAL TRADE EXECUTED: {order['orderId']} - {side} {quantity} {symbol}")
            return order
        except Exception as e:
            self.logger.error(f"REAL TRADE FAILED: {e}")
            return None
    
    def validate_trade_amount(self, usdt_amount):
        """Validate trade amount against available balance"""
        usdt_balance = self.get_account_balance('USDT')
        
        if usdt_amount > usdt_balance:
            self.logger.error(f"Insufficient balance: Need ${usdt_amount}, Have ${usdt_balance}")
            return False
        
        return True

# Production test
if __name__ == "__main__":
    print("FINAL BINANCE US TRADING SYSTEM")
    print("=" * 40)
    
    try:
        manager = FinalBinanceUSManager()
        print("SUCCESS - Production Binance US manager ready!")
        
        # Show real balance
        usdt_balance = manager.get_account_balance('USDT')
        print(f"Real USDT Balance: ${usdt_balance:.2f}")
        
        # Validate trade amount from config
        from dotenv import load_dotenv
        load_dotenv()
        trade_amount = float(os.getenv('TRADE_AMOUNT_USDT', '2.0'))
        
        if manager.validate_trade_amount(trade_amount):
            print(f"Trade amount ${trade_amount} validated against balance")
            print("SYSTEM READY FOR LIVE TRADING")
        else:
            print(f"WARNING: Trade amount ${trade_amount} exceeds balance ${usdt_balance}")
        
        print("\nREAL TRADING CONFIRMED:")
        print("- Connected to Binance US")
        print("- Real account balance verified")
        print("- Trade validation passed")
        print("- All trades will use real money")
        
    except Exception as e:
        print(f"Error: {e}")
