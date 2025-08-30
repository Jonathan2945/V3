#!/usr/bin/env python3
"""
Advanced Multi-Timeframe Backtesting with Strategy Discovery
Real market data implementation for V3 Trading System
"""

import pandas as pd
import numpy as np
import asyncio
import ccxt
from typing import Dict, List, Optional
from datetime import datetime

class AdvancedMultiTimeframeBacktester:
    def __init__(self):
        # Binance timeframes for real historical data
        self.timeframes = [
            '1m', '3m', '5m', '15m', '30m',
            '1h', '2h', '4h', '6h', '8h', '12h', 
            '1d', '3d', '1w', '1M'
        ]
        
        # Multi-timeframe combinations for confluence analysis
        self.mtf_combinations = [
            (['1m', '5m', '15m'], 'scalping'),
            (['5m', '15m', '30m'], 'short_term'),
            (['15m', '1h', '4h'], 'intraday'),
            (['1h', '4h', '1d'], 'swing'),
            (['4h', '1d', '1w'], 'position'),
            (['1d', '1w', '1M'], 'long_term')
        ]
        
        # Strategy discovery parameters
        self.strategy_discovery = True
        self.genetic_optimization = True
        
        # Initialize exchange for real market data
        self.exchange = ccxt.binance({
            'sandbox': True,
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'}
        })
        
    async def get_comprehensive_data(self, symbol: str, timeframe: str, limit: int = 1000) -> Optional[pd.DataFrame]:
        """Fetch real market data from Binance API"""
        try:
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            
            if not ohlcv or len(ohlcv) == 0:
                return None
            
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            
            return df if len(df) >= 100 else None
            
        except Exception as e:
            print(f"Error fetching real market data for {symbol} {timeframe}: {e}")
            return None
        
    async def run_multi_timeframe_analysis(self, symbol: str):
        """Run multi-timeframe confluence analysis using real market data"""
        
        print(f"Multi-timeframe analysis for {symbol}:")
        
        # Get real data for all timeframes
        timeframe_data = {}
        for tf in self.timeframes:
            try:
                data = await self.get_comprehensive_data(symbol, tf)
                if data is not None and len(data) >= 100:
                    timeframe_data[tf] = data
                    print(f"  {tf}: {len(data)} real market candles")
                else:
                    print(f"  {tf}: Insufficient real data")
            except Exception as e:
                print(f"  {tf}: Error fetching real data - {e}")
        
        # Run confluence analysis on real data
        confluence_results = {}
        
        for timeframes, strategy_type in self.mtf_combinations:
            available_tfs = [tf for tf in timeframes if tf in timeframe_data]
            
            if len(available_tfs) >= 2:
                print(f"\nAnalyzing {strategy_type} confluence: {available_tfs}")
                
                result = await self.analyze_timeframe_confluence(
                    symbol, available_tfs, timeframe_data, strategy_type
                )
                
                if result:
                    confluence_results[strategy_type] = result
        
        return confluence_results
    
    async def analyze_timeframe_confluence(self, symbol: str, timeframes: list, 
                                         data_dict: dict, strategy_type: str):
        """Analyze confluence across multiple timeframes using real market data"""
        
        primary_tf = timeframes[0]
        primary_data = data_dict[primary_tf]
        
        # Calculate indicators for all timeframes using real data
        tf_indicators = {}
        for tf in timeframes:
            tf_indicators[tf] = self.calculate_all_indicators(data_dict[tf])
        
        # Run confluence backtest on real data
        trades = []
        balance = 10000
        position = None
        
        for i in range(200, len(primary_data)):
            
            confluence_signals = self.get_confluence_signals(
                tf_indicators, timeframes, i, primary_tf
            )
            
            # Entry logic - require confluence from multiple timeframes
            if not position and confluence_signals['strength'] >= 2:
                
                signal_direction = confluence_signals['direction']
                confidence = confluence_signals['confidence']
                
                position = {
                    'entry_price': primary_data.iloc[i]['close'],
                    'entry_time': primary_data.iloc[i]['timestamp'],
                    'side': signal_direction,
                    'confidence': confidence,
                    'timeframes_confirming': confluence_signals['confirming_tfs']
                }
                
            # Exit logic
            elif position:
                exit_signal = self.get_exit_signal(
                    tf_indicators, timeframes, i, position, primary_tf
                )
                
                if exit_signal:
                    current_price = primary_data.iloc[i]['close']
                    
                    if position['side'] == 'BUY':
                        return_pct = (current_price - position['entry_price']) / position['entry_price']
                    else:
                        return_pct = (position['entry_price'] - current_price) / position['entry_price']
                    
                    return_pct -= 0.002  # Trading costs
                    balance *= (1 + return_pct)
                    
                    trade = {
                        **position,
                        'exit_price': current_price,
                        'exit_time': primary_data.iloc[i]['timestamp'],
                        'return_pct': return_pct,
                        'exit_reason': exit_signal['reason']
                    }
                    
                    trades.append(trade)
                    position = None
        
        if not trades:
            return None
        
        returns = [t['return_pct'] for t in trades]
        winning_trades = sum(1 for r in returns if r > 0)
        
        return {
            'symbol': symbol,
            'strategy_type': strategy_type,
            'timeframes': timeframes,
            'total_trades': len(trades),
            'winning_trades': winning_trades,
            'win_rate': (winning_trades / len(trades)) * 100,
            'total_return': ((balance - 10000) / 10000) * 100,
            'avg_return_per_trade': np.mean(returns) * 100,
            'sharpe_ratio': self.calculate_sharpe_ratio(returns),
            'trades': trades
        }
    
    def get_confluence_signals(self, tf_indicators: dict, timeframes: list, 
                              index: int, primary_tf: str) -> dict:
        """Get confluence signals from multiple timeframes"""
        
        signals = {'BUY': 0, 'SELL': 0, 'NEUTRAL': 0}
        confirming_tfs = []
        
        for tf in timeframes:
            indicators = tf_indicators[tf]
            tf_index = min(index, len(indicators) - 1)
            
            if tf_index < 50:
                continue
            
            tf_signals = []
            
            # RSI signals
            rsi = indicators.iloc[tf_index]['rsi']
            if rsi < 30:
                tf_signals.append('BUY')
            elif rsi > 70:
                tf_signals.append('SELL')
            
            # MACD signals  
            macd = indicators.iloc[tf_index]['macd']
            macd_signal = indicators.iloc[tf_index]['macd_signal']
            if tf_index > 0:
                macd_prev = indicators.iloc[tf_index-1]['macd']
                signal_prev = indicators.iloc[tf_index-1]['macd_signal']
                
                if macd > macd_signal and macd_prev <= signal_prev:
                    tf_signals.append('BUY')
                elif macd < macd_signal and macd_prev >= signal_prev:
                    tf_signals.append('SELL')
            
            # Moving average signals
            price = indicators.iloc[tf_index]['close']
            sma20 = indicators.iloc[tf_index]['sma_20']
            sma50 = indicators.iloc[tf_index]['sma_50']
            
            if price > sma20 > sma50:
                tf_signals.append('BUY')
            elif price < sma20 < sma50:
                tf_signals.append('SELL')
            
            # Bollinger Band signals
            bb_upper = indicators.iloc[tf_index]['bb_upper']
            bb_lower = indicators.iloc[tf_index]['bb_lower']
            
            if price <= bb_lower:
                tf_signals.append('BUY')
            elif price >= bb_upper:
                tf_signals.append('SELL')
            
            # Count signals for this timeframe
            buy_signals = tf_signals.count('BUY')
            sell_signals = tf_signals.count('SELL')
            
            if buy_signals > sell_signals:
                signals['BUY'] += 1
                confirming_tfs.append(f"{tf}_BUY")
            elif sell_signals > buy_signals:
                signals['SELL'] += 1
                confirming_tfs.append(f"{tf}_SELL")
            else:
                signals['NEUTRAL'] += 1
        
        max_direction = max(signals, key=signals.get)
        strength = signals[max_direction]
        confidence = (strength / len(timeframes)) * 100
        
        return {
            'direction': max_direction,
            'strength': strength,
            'confidence': confidence,
            'confirming_tfs': confirming_tfs,
            'signal_breakdown': signals
        }
    
    def get_exit_signal(self, tf_indicators: dict, timeframes: list, 
                       index: int, position: dict, primary_tf: str) -> Optional[dict]:
        """Generate exit signals based on multiple timeframe analysis"""
        
        primary_indicators = tf_indicators[primary_tf]
        tf_index = min(index, len(primary_indicators) - 1)
        
        if tf_index < 1:
            return None
        
        current_price = primary_indicators.iloc[tf_index]['close']
        atr = primary_indicators.iloc[tf_index]['atr']
        entry_price = position['entry_price']
        
        # ATR-based stop loss (2x ATR)
        if position['side'] == 'BUY':
            stop_loss = entry_price - (2 * atr)
            take_profit = entry_price + (4 * atr)
            
            if current_price <= stop_loss:
                return {'reason': 'stop_loss', 'type': 'risk_management'}
            elif current_price >= take_profit:
                return {'reason': 'take_profit', 'type': 'profit_taking'}
                
        else:  # SELL position
            stop_loss = entry_price + (2 * atr)
            take_profit = entry_price - (4 * atr)
            
            if current_price >= stop_loss:
                return {'reason': 'stop_loss', 'type': 'risk_management'}
            elif current_price <= take_profit:
                return {'reason': 'take_profit', 'type': 'profit_taking'}
        
        # Signal reversal exit
        confluence_signals = self.get_confluence_signals(
            tf_indicators, timeframes, index, primary_tf
        )
        
        if (position['side'] == 'BUY' and confluence_signals['direction'] == 'SELL' and 
            confluence_signals['strength'] >= 2):
            return {'reason': 'signal_reversal', 'type': 'signal_based'}
        elif (position['side'] == 'SELL' and confluence_signals['direction'] == 'BUY' and 
              confluence_signals['strength'] >= 2):
            return {'reason': 'signal_reversal', 'type': 'signal_based'}
        
        # Time-based exit (max 100 bars)
        bars_in_trade = index - position.get('entry_index', index - 100)
        if bars_in_trade > 100:
            return {'reason': 'time_limit', 'type': 'time_based'}
        
        return None
    
    def calculate_all_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate comprehensive technical indicators from real market data"""
        
        df = df.copy()
        
        # Moving averages
        df['sma_10'] = df['close'].rolling(10).mean()
        df['sma_20'] = df['close'].rolling(20).mean()
        df['sma_50'] = df['close'].rolling(50).mean()
        df['sma_200'] = df['close'].rolling(200).mean()
        
        df['ema_12'] = df['close'].ewm(span=12).mean()
        df['ema_26'] = df['close'].ewm(span=26).mean()
        
        # RSI
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # MACD
        df['macd'] = df['ema_12'] - df['ema_26']
        df['macd_signal'] = df['macd'].ewm(span=9).mean()
        df['macd_histogram'] = df['macd'] - df['macd_signal']
        
        # Bollinger Bands
        df['bb_middle'] = df['close'].rolling(20).mean()
        bb_std = df['close'].rolling(20).std()
        df['bb_upper'] = df['bb_middle'] + (bb_std * 2)
        df['bb_lower'] = df['bb_middle'] - (bb_std * 2)
        
        # Stochastic
        low_14 = df['low'].rolling(14).min()
        high_14 = df['high'].rolling(14).max()
        df['stoch_k'] = 100 * ((df['close'] - low_14) / (high_14 - low_14))
        df['stoch_d'] = df['stoch_k'].rolling(3).mean()
        
        # ATR
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())
        ranges = np.maximum(high_low, np.maximum(high_close, low_close))
        df['atr'] = ranges.rolling(14).mean()
        
        # Volume indicators
        df['volume_sma'] = df['volume'].rolling(20).mean()
        df['volume_ratio'] = df['volume'] / df['volume_sma']
        
        return df
    
    def calculate_sharpe_ratio(self, returns: List[float]) -> float:
        """Calculate Sharpe ratio from trade returns"""
        if not returns or len(returns) < 2:
            return 0.0
        
        returns_array = np.array(returns)
        if np.std(returns_array) == 0:
            return 0.0
        
        return np.mean(returns_array) / np.std(returns_array) * np.sqrt(252)

if __name__ == "__main__":
    backtester = AdvancedMultiTimeframeBacktester()
    
    async def main():
        results = await backtester.run_multi_timeframe_analysis('BTCUSDT')
        if results:
            for strategy, result in results.items():
                print(f"{strategy}: {result['total_trades']} trades, "
                      f"{result['win_rate']:.1f}% win rate, "
                      f"{result['total_return']:.2f}% return")
    
    asyncio.run(main())