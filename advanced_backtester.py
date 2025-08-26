#!/usr/bin/env python3
"""
Advanced Multi-Timeframe Backtesting with Strategy Discovery
"""

import pandas as pd

class AdvancedMultiTimeframeBacktester:
    def __init__(self):
        # All available Binance timeframes for historical data
        self.timeframes = [
            '1m', '3m', '5m', '15m', '30m',
            '1h', '2h', '4h', '6h', '8h', '12h', 
            '1d', '3d', '1w', '1M'
        ]
        
        # Multi-timeframe combinations for confluence analysis
        self.mtf_combinations = [
            # Short-term combinations
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
        
    async def run_multi_timeframe_analysis(self, symbol: str):
        """Run multi-timeframe confluence analysis"""
        
        print(f"Multi-timeframe analysis for {symbol}:")
        
        # Get data for all timeframes
        timeframe_data = {}
        for tf in self.timeframes:
            try:
                data = await self.get_comprehensive_data(symbol, tf)
                if data is not None and len(data) >= 100:
                    timeframe_data[tf] = data
                    print(f"  {tf}: {len(data)} candles")
                else:
                    print(f"  {tf}: Insufficient data")
            except Exception as e:
                print(f"  {tf}: Error - {e}")
        
        # Run confluence analysis
        confluence_results = {}
        
        for timeframes, strategy_type in self.mtf_combinations:
            # Check if we have data for all required timeframes
            available_tfs = [tf for tf in timeframes if tf in timeframe_data]
            
            if len(available_tfs) >= 2:  # Need at least 2 timeframes
                print(f"\nAnalyzing {strategy_type} confluence: {available_tfs}")
                
                result = await self.analyze_timeframe_confluence(
                    symbol, available_tfs, timeframe_data, strategy_type
                )
                
                if result:
                    confluence_results[strategy_type] = result
        
        return confluence_results
    
    async def analyze_timeframe_confluence(self, symbol: str, timeframes: list, 
                                         data_dict: dict, strategy_type: str):
        """Analyze confluence across multiple timeframes"""
        
        # Get the shortest timeframe as primary for trade execution
        primary_tf = timeframes[0]  # Shortest timeframe
        confirmation_tfs = timeframes[1:]  # Higher timeframes for confirmation
        
        primary_data = data_dict[primary_tf]
        
        # Calculate indicators for all timeframes
        tf_indicators = {}
        for tf in timeframes:
            tf_indicators[tf] = self.calculate_all_indicators(data_dict[tf])
        
        # Run confluence backtest
        trades = []
        balance = 10000
        position = None
        
        # Use primary timeframe for iteration
        for i in range(200, len(primary_data)):  # Allow indicator warmup
            
            # Get confluence signals from all timeframes
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
                    
                    # Trading costs
                    return_pct -= 0.002  # 0.2% total fees
                    
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
        
        # Calculate performance
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
            
            # Adjust index for different timeframe lengths
            tf_index = min(index, len(indicators) - 1)
            
            if tf_index < 50:  # Need enough data for indicators
                continue
            
            # Multiple signal checks
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
        
        # Determine overall direction and strength
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
    
    def calculate_all_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate comprehensive technical indicators"""
        
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

# Strategy Discovery System
class StrategyDiscoveryEngine:
    def __init__(self):
        self.strategy_templates = [
            self.momentum_strategy,
            self.mean_reversion_strategy, 
            self.breakout_strategy,
            self.trend_following_strategy,
            self.volatility_strategy
        ]
        
        # Genetic algorithm parameters
        self.population_size = 50
        self.generations = 100
        
    def discover_optimal_strategies(self, symbol: str, timeframe_data: dict):
        """Use genetic algorithm to discover optimal strategies"""
        
        print(f"Strategy discovery for {symbol}...")
        
        discovered_strategies = {}
        
        for tf, data in timeframe_data.items():
            if len(data) < 1000:  # Need sufficient data
                continue
                
            print(f"  Optimizing strategies for {tf}...")
            
            # Test each strategy template
            for strategy_func in self.strategy_templates:
                
                # Genetic optimization for this strategy
                best_params = self.genetic_optimize_strategy(
                    strategy_func, data, tf
                )
                
                if best_params['fitness'] > 1.5:  # Minimum performance threshold
                    strategy_name = f"{strategy_func.__name__}_{tf}"
                    discovered_strategies[strategy_name] = best_params
        
        return discovered_strategies
    
    def genetic_optimize_strategy(self, strategy_func, data: pd.DataFrame, timeframe: str):
        """Genetic algorithm optimization for strategy parameters"""
        
        # Initialize population with random parameters
        population = []
        for _ in range(self.population_size):
            params = self.generate_random_params(strategy_func)
            fitness = self.evaluate_strategy(strategy_func, params, data)
            population.append({'params': params, 'fitness': fitness})
        
        # Evolution loop
        for generation in range(self.generations):
            # Selection, crossover, mutation
            population = self.evolve_population(population, strategy_func, data)
            
            # Track best fitness
            best = max(population, key=lambda x: x['fitness'])
            
            if generation % 20 == 0:
                print(f"    Gen {generation}: Best fitness = {best['fitness']:.3f}")
        
        return max(population, key=lambda x: x['fitness'])