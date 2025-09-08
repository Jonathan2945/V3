#!/usr/bin/env python3
"""
CCXT to python-binance compatibility wrapper for Binance US
"""

import ccxt
import os
from datetime import datetime

class CCXTBinanceUSWrapper:
    """Wrapper to make CCXT look like python-binance Client"""
    
    def __init__(self, api_key, api_secret, testnet=False):
        self.exchange = ccxt.binanceus({
            'apiKey': api_key,
            'secret': api_secret,
            'sandbox': testnet,  # CCXT uses 'sandbox' for testnet
            'enableRateLimit': True,
        })
        
        print(f"[CCXT_WRAPPER] Binance US client initialized - Real trading")
    
    def get_account(self):
        """Get account info in python-binance format"""
        balance = self.exchange.fetch_balance()
        
        # Convert to python-binance format
        binance_format = {
            'accountType': 'SPOT',
            'canTrade': True,
            'canWithdraw': True,
            'canDeposit': True,
            'balances': []
        }
        
        for asset, amounts in balance['total'].items():
            if asset in balance['free'] and asset in balance['used']:
                binance_format['balances'].append({
                    'asset': asset,
                    'free': str(balance['free'][asset]),
                    'locked': str(balance['used'][asset])
                })
        
        return binance_format
    
    def get_symbol_ticker(self, symbol):
        """Get ticker for symbol"""
        ticker = self.exchange.fetch_ticker(symbol)
        return {'price': str(ticker['last'])}
    
    def order_market_buy(self, symbol, quantity):
        """Place market buy order"""
        order = self.exchange.create_market_buy_order(symbol, quantity)
        return self._convert_order_format(order)
    
    def order_market_sell(self, symbol, quantity):
        """Place market sell order"""  
        order = self.exchange.create_market_sell_order(symbol, quantity)
        return self._convert_order_format(order)
    
    def order_limit(self, symbol, side, quantity, price):
        """Place limit order"""
        if side.upper() == 'BUY':
            order = self.exchange.create_limit_buy_order(symbol, quantity, price)
        else:
            order = self.exchange.create_limit_sell_order(symbol, quantity, price)
        return self._convert_order_format(order)
    
    def get_order(self, symbol, orderId):
        """Get order status"""
        order = self.exchange.fetch_order(orderId, symbol)
        return self._convert_order_format(order)
    
    def cancel_order(self, symbol, orderId):
        """Cancel order"""
        return self.exchange.cancel_order(orderId, symbol)
    
    def get_exchange_info(self):
        """Get exchange info"""
        markets = self.exchange.load_markets()
        
        symbols = []
        for symbol, market in markets.items():
            symbols.append({
                'symbol': symbol,
                'status': 'TRADING',
                'baseAsset': market['base'],
                'quoteAsset': market['quote'],
                'filters': [
                    {
                        'filterType': 'MIN_NOTIONAL',
                        'minNotional': str(market.get('limits', {}).get('cost', {}).get('min', 10.0))
                    },
                    {
                        'filterType': 'LOT_SIZE', 
                        'stepSize': str(market.get('precision', {}).get('amount', 0.001))
                    },
                    {
                        'filterType': 'PRICE_FILTER',
                        'minPrice': str(market.get('limits', {}).get('price', {}).get('min', 0.01)),
                        'maxPrice': str(market.get('limits', {}).get('price', {}).get('max', 1000000)),
                        'tickSize': str(market.get('precision', {}).get('price', 0.01))
                    }
                ]
            })
        
        return {'symbols': symbols}
    
    def _convert_order_format(self, ccxt_order):
        """Convert CCXT order format to python-binance format"""
        return {
            'orderId': ccxt_order['id'],
            'clientOrderId': ccxt_order.get('clientOrderId', ''),
            'symbol': ccxt_order['symbol'],
            'side': ccxt_order['side'].upper(),
            'type': ccxt_order['type'].upper(),
            'status': ccxt_order['status'].upper(),
            'origQty': str(ccxt_order['amount']),
            'price': str(ccxt_order['price'] or 0),
            'executedQty': str(ccxt_order['filled']),
            'cummulativeQuoteQty': str(ccxt_order['cost']),
            'transactTime': int(ccxt_order['timestamp']),
            'fills': ccxt_order.get('trades', [])
        }

# Test the wrapper
if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    
    api_key = os.getenv('BINANCE_LIVE_API_KEY_1')
    api_secret = os.getenv('BINANCE_LIVE_API_SECRET_1')
    
    try:
        client = CCXTBinanceUSWrapper(api_key, api_secret, testnet=False)
        account = client.get_account()
        print("SUCCESS - CCXT Wrapper works!")
        print(f"Can Trade: {account.get('canTrade')}")
        
        # Show balances
        balances = [b for b in account['balances'] if float(b['free']) > 0]
        print(f"Real balances: {len(balances)}")
        for b in balances:
            print(f"  {b['asset']}: {b['free']}")
            
    except Exception as e:
        print(f"Wrapper Error: {e}")
