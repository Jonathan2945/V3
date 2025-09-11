"""
CCXT to Python-Binance Compatibility Wrapper
Translates python-binance method calls to CCXT equivalents
"""
import pandas as pd
from datetime import datetime, timedelta
import time
import re

class CCXTBinanceWrapper:
    """Wrapper to make CCXT client compatible with python-binance methods"""
    
    def __init__(self, ccxt_client):
        self.ccxt_client = ccxt_client
        
        # Map timeframe formats
        self.timeframe_map = {
            '1m': '1m', '3m': '3m', '5m': '5m', '15m': '15m', '30m': '30m',
            '1h': '1h', '2h': '2h', '4h': '4h', '6h': '6h', '8h': '8h', '12h': '12h',
            '1d': '1d', '3d': '3d', '1w': '1w', '1M': '1M'
        }
    
    def _parse_time_string(self, time_str):
        """Parse various time string formats to millisecond timestamp"""
        try:
            # If it's already a number, return it
            if isinstance(time_str, (int, float)):
                return int(time_str)
            
            # If it's a string number, convert directly
            if time_str.isdigit():
                return int(time_str)
            
            # Parse relative time strings like "1000 hours ago UTC"
            if 'ago' in time_str.lower():
                # Extract number and unit
                match = re.search(r'(\d+)\s*(hour|day|week|month)s?\s+ago', time_str.lower())
                if match:
                    amount = int(match.group(1))
                    unit = match.group(2)
                    
                    # Calculate timedelta
                    if unit == 'hour':
                        delta = timedelta(hours=amount)
                    elif unit == 'day':
                        delta = timedelta(days=amount)
                    elif unit == 'week':
                        delta = timedelta(weeks=amount)
                    elif unit == 'month':
                        delta = timedelta(days=amount * 30)  # Approximate
                    else:
                        delta = timedelta(hours=amount)  # Default to hours
                    
                    # Calculate timestamp
                    past_time = datetime.now() - delta
                    return int(past_time.timestamp() * 1000)
            
            # Try to parse as ISO format or other common formats
            try:
                dt = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
                return int(dt.timestamp() * 1000)
            except:
                pass
            
            # Default fallback - use current time minus some hours
            fallback_hours = 24  # Default to 24 hours ago
            past_time = datetime.now() - timedelta(hours=fallback_hours)
            return int(past_time.timestamp() * 1000)
            
        except Exception as e:
            # If all parsing fails, return timestamp for 24 hours ago
            past_time = datetime.now() - timedelta(hours=24)
            return int(past_time.timestamp() * 1000)
    
    def get_historical_klines(self, symbol, interval, start_str=None, end_str=None, limit=500):
        """
        Emulate python-binance get_historical_klines using CCXT fetch_ohlcv
        Returns data in python-binance format
        """
        try:
            # Convert interval to CCXT format
            timeframe = self.timeframe_map.get(interval, interval)
            
            # Convert timestamps if provided
            since = None
            if start_str:
                since = self._parse_time_string(start_str)
            
            # Fetch OHLCV data using CCXT
            ohlcv = self.ccxt_client.fetch_ohlcv(
                symbol=symbol,
                timeframe=timeframe,
                since=since,
                limit=limit
            )
            
            if not ohlcv:
                return []
            
            # Convert CCXT format to python-binance format
            # CCXT: [timestamp, open, high, low, close, volume]
            # python-binance: [timestamp, open, high, low, close, volume, close_time, ...]
            klines = []
            for candle in ohlcv:
                timestamp, open_price, high, low, close, volume = candle
                
                # Calculate close time based on timeframe
                timeframe_ms = {
                    '1m': 60000, '3m': 180000, '5m': 300000, '15m': 900000, '30m': 1800000,
                    '1h': 3600000, '2h': 7200000, '4h': 14400000, '6h': 21600000, 
                    '8h': 28800000, '12h': 43200000, '1d': 86400000, '3d': 259200000,
                    '1w': 604800000, '1M': 2592000000
                }.get(timeframe, 60000)
                
                close_time = int(timestamp + timeframe_ms - 1)
                
                klines.append([
                    int(timestamp),          # Open time
                    str(open_price),         # Open
                    str(high),               # High  
                    str(low),                # Low
                    str(close),              # Close
                    str(volume),             # Volume
                    close_time,              # Close time
                    str(volume * close),     # Quote asset volume (estimated)
                    100,                     # Number of trades (estimated)
                    str(volume * 0.5),       # Taker buy base asset volume (estimated)
                    str(volume * close * 0.5), # Taker buy quote asset volume (estimated)
                    str(0)                   # Ignore
                ])
            
            return klines
            
        except Exception as e:
            print(f"Error in get_historical_klines wrapper: {e}")
            return []
    
    def get_symbol_ticker(self, symbol):
        """Get current price ticker"""
        try:
            ticker = self.ccxt_client.fetch_ticker(symbol)
            return {'price': str(ticker['last'])}
        except Exception as e:
            print(f"Error getting ticker: {e}")
            return {'price': '0'}
    
    def __getattr__(self, name):
        """Delegate other method calls to the original CCXT client"""
        return getattr(self.ccxt_client, name)
