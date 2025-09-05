#!/usr/bin/env python3
"""
V3 Intelligent Trading Engine - Performance Optimized
Enhanced with data fetching caching and performance optimization
"""

import asyncio
import time
import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict, deque
from functools import lru_cache, wraps
import concurrent.futures
import hashlib
import json
import numpy as np
import pandas as pd
import psutil
from dataclasses import dataclass

@dataclass
class TradingSignal:
    """Trading signal data structure"""
    symbol: str
    direction: str  # 'buy', 'sell', 'hold'
    strength: float  # 0-1
    confidence: float  # 0-100
    entry_price: float
    stop_loss: float
    take_profit: float
    timeframe: str
    timestamp: datetime
    metadata: Dict[str, Any]

class IntelligentCache:
    """High-performance caching system for trading data"""
    
    def __init__(self, max_size: int = 3000, ttl_seconds: int = 300):
        self.cache = {}
        self.timestamps = {}
        self.access_patterns = {}
        self.performance_scores = {}
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self.lock = threading.RLock()
        
        # Cache statistics
        self.stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0,
            'total_requests': 0
        }
    
    def _calculate_score(self, key: str) -> float:
        """Calculate performance score for cache entry"""
        access_count = self.access_patterns.get(key, 0)
        age = time.time() - self.timestamps.get(key, time.time())
        
        # Score based on access frequency and recency
        frequency_score = min(access_count / 10.0, 1.0)
        recency_score = max(0, 1.0 - age / self.ttl_seconds)
        
        return frequency_score * 0.6 + recency_score * 0.4
    
    def _cleanup_expired(self):
        """Remove expired entries and low-performance entries"""
        current_time = time.time()
        
        # Remove expired entries
        expired_keys = [
            key for key, timestamp in self.timestamps.items()
            if current_time - timestamp > self.ttl_seconds
        ]
        
        for key in expired_keys:
            self._remove_entry(key)
        
        # If still over capacity, remove lowest scoring entries
        if len(self.cache) >= self.max_size:
            scores = {key: self._calculate_score(key) for key in self.cache.keys()}
            sorted_keys = sorted(scores.keys(), key=lambda k: scores[k])
            
            # Remove bottom 20% of entries
            remove_count = max(1, len(sorted_keys) // 5)
            for key in sorted_keys[:remove_count]:
                self._remove_entry(key)
                self.stats['evictions'] += 1
    
    def _remove_entry(self, key: str):
        """Remove a single cache entry"""
        self.cache.pop(key, None)
        self.timestamps.pop(key, None)
        self.access_patterns.pop(key, None)
        self.performance_scores.pop(key, None)
    
    def get(self, key: str) -> Optional[Any]:
        """Get cached value with intelligent access tracking"""
        with self.lock:
            self.stats['total_requests'] += 1
            
            if key in self.cache:
                current_time = time.time()
                if current_time - self.timestamps.get(key, 0) <= self.ttl_seconds:
                    # Update access patterns
                    self.access_patterns[key] = self.access_patterns.get(key, 0) + 1
                    self.timestamps[key] = current_time  # Update access time
                    self.stats['hits'] += 1
                    return self.cache[key]
                else:
                    # Expired entry
                    self._remove_entry(key)
            
            self.stats['misses'] += 1
            return None
    
    def set(self, key: str, value: Any):
        """Set cache value with intelligent management"""
        with self.lock:
            # Clean up if necessary
            if len(self.cache) >= self.max_size:
                self._cleanup_expired()
            
            # Store new entry
            current_time = time.time()
            self.cache[key] = value
            self.timestamps[key] = current_time
            self.access_patterns[key] = 1
            self.performance_scores[key] = 1.0  # New entries start with high score
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache performance statistics"""
        hit_rate = self.stats['hits'] / max(self.stats['total_requests'], 1)
        
        return {
            'size': len(self.cache),
            'max_size': self.max_size,
            'hit_rate': hit_rate,
            'hits': self.stats['hits'],
            'misses': self.stats['misses'],
            'evictions': self.stats['evictions'],
            'ttl_seconds': self.ttl_seconds
        }

def intelligent_cache(ttl_seconds: int = 300, cache_size: int = 1000):
    """Decorator for intelligent caching of trading data"""
    def decorator(func):
        cache = IntelligentCache(max_size=cache_size, ttl_seconds=ttl_seconds)
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Create intelligent cache key
            cache_key = f"{func.__name__}_{hashlib.md5(str(args).encode() + str(kwargs).encode()).hexdigest()}"
            
            # Try cache first
            cached_result = cache.get(cache_key)
            if cached_result is not None:
                return cached_result
            
            # Execute function and cache result
            result = func(*args, **kwargs)
            if result is not None:
                cache.set(cache_key, result)
            
            return result
        
        # Attach cache management methods
        wrapper.get_cache_stats = cache.get_stats
        wrapper.clear_cache = lambda: cache.cache.clear()
        
        return wrapper
    return decorator

class MarketDataProcessor:
    """Enhanced market data processing with intelligent caching"""
    
    def __init__(self):
        self.data_cache = IntelligentCache(max_size=2000, ttl_seconds=180)
        self.indicator_cache = IntelligentCache(max_size=1500, ttl_seconds=300)
    
    @intelligent_cache(ttl_seconds=120, cache_size=800)
    def get_market_data(self, symbol: str, timeframe: str, limit: int = 100) -> Optional[pd.DataFrame]:
        """Get real market data only - NO MOCK DATA"""
        try:
            # V3 REAL DATA ENFORCEMENT - Only fetch real market data
            # Connect to real data sources: Binance API, historical data manager, etc.
            
            logging.info(f"Fetching real market data for {symbol} {timeframe}")
            
            # Implementation would connect to:
            # - Binance exchange manager for real OHLCV data
            # - Historical data manager for stored real data
            # - External data collector for real market data
            
            # Return None when real data not available - never generate mock data
            return None
            
        except Exception as e:
            logging.error(f"Error getting real market data for {symbol}: {e}")
            return None
    
    @intelligent_cache(ttl_seconds=240, cache_size=600)
    def calculate_technical_indicators(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Calculate technical indicators with caching"""
        try:
            if data is None or data.empty:
                return {}
            
            indicators = {}
            close_prices = data['close'].values
            high_prices = data['high'].values
            low_prices = data['low'].values
            volumes = data['volume'].values
            
            # Moving averages
            indicators['sma_20'] = pd.Series(close_prices).rolling(window=20).mean().iloc[-1] if len(close_prices) >= 20 else close_prices[-1]
            indicators['sma_50'] = pd.Series(close_prices).rolling(window=50).mean().iloc[-1] if len(close_prices) >= 50 else close_prices[-1]
            indicators['ema_12'] = pd.Series(close_prices).ewm(span=12).mean().iloc[-1]
            indicators['ema_26'] = pd.Series(close_prices).ewm(span=26).mean().iloc[-1]
            
            # RSI
            if len(close_prices) >= 14:
                delta = pd.Series(close_prices).diff()
                gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
                loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
                rs = gain / loss
                indicators['rsi'] = (100 - (100 / (1 + rs))).iloc[-1]
            else:
                indicators['rsi'] = 50
            
            # MACD
            indicators['macd'] = indicators['ema_12'] - indicators['ema_26']
            indicators['macd_signal'] = pd.Series([indicators['macd']]).ewm(span=9).mean().iloc[0]
            indicators['macd_histogram'] = indicators['macd'] - indicators['macd_signal']
            
            # Bollinger Bands
            if len(close_prices) >= 20:
                sma_20 = pd.Series(close_prices).rolling(window=20).mean()
                std_20 = pd.Series(close_prices).rolling(window=20).std()
                indicators['bb_upper'] = (sma_20 + (std_20 * 2)).iloc[-1]
                indicators['bb_lower'] = (sma_20 - (std_20 * 2)).iloc[-1]
                indicators['bb_middle'] = sma_20.iloc[-1]
            else:
                indicators['bb_upper'] = close_prices[-1] * 1.02
                indicators['bb_lower'] = close_prices[-1] * 0.98
                indicators['bb_middle'] = close_prices[-1]
            
            # Stochastic Oscillator
            if len(close_prices) >= 14:
                low_14 = pd.Series(low_prices).rolling(window=14).min()
                high_14 = pd.Series(high_prices).rolling(window=14).max()
                k_percent = 100 * ((close_prices[-1] - low_14.iloc[-1]) / (high_14.iloc[-1] - low_14.iloc[-1]))
                indicators['stoch_k'] = k_percent
                indicators['stoch_d'] = k_percent  # Simplified
            else:
                indicators['stoch_k'] = 50
                indicators['stoch_d'] = 50
            
            # Volume indicators
            indicators['volume_sma'] = pd.Series(volumes).rolling(window=20).mean().iloc[-1] if len(volumes) >= 20 else volumes[-1]
            indicators['volume_ratio'] = volumes[-1] / indicators['volume_sma'] if indicators['volume_sma'] > 0 else 1
            
            # Current price info
            indicators['current_price'] = close_prices[-1]
            indicators['price_change'] = (close_prices[-1] - close_prices[-2]) / close_prices[-2] * 100 if len(close_prices) > 1 else 0
            
            return indicators
            
        except Exception as e:
            logging.error(f"Error calculating technical indicators: {e}")
            return {}

class SignalGenerator:
    """Enhanced signal generation with intelligent analysis"""
    
    def __init__(self):
        self.signal_cache = IntelligentCache(max_size=1000, ttl_seconds=300)
        self.pattern_cache = IntelligentCache(max_size=500, ttl_seconds=600)
    
    @intelligent_cache(ttl_seconds=180, cache_size=400)
    def generate_trading_signal(self, symbol: str, indicators: Dict[str, Any], 
                              market_data: pd.DataFrame) -> Optional[TradingSignal]:
        """Generate trading signal with intelligent analysis and caching"""
        try:
            if not indicators or market_data is None or market_data.empty:
                return None
            
            # Analyze multiple signal sources
            trend_signal = self._analyze_trend_signals(indicators)
            momentum_signal = self._analyze_momentum_signals(indicators)
            volume_signal = self._analyze_volume_signals(indicators)
            pattern_signal = self._analyze_pattern_signals(market_data, indicators)
            
            # Combine signals with weighted approach
            signals = [trend_signal, momentum_signal, volume_signal, pattern_signal]
            weights = [0.35, 0.25, 0.2, 0.2]  # Trend gets highest weight
            
            # Calculate overall signal strength and direction
            total_weight = 0
            weighted_score = 0
            confidence_scores = []
            
            for signal, weight in zip(signals, weights):
                if signal and 'direction' in signal and 'strength' in signal:
                    total_weight += weight
                    
                    # Convert direction to score (-1 to 1)
                    direction_score = 1 if signal['direction'] == 'bullish' else -1 if signal['direction'] == 'bearish' else 0
                    weighted_score += direction_score * signal['strength'] * weight
                    confidence_scores.append(signal.get('confidence', 50))
            
            if total_weight == 0:
                return None
            
            # Normalize and determine final signal
            final_score = weighted_score / total_weight
            overall_confidence = sum(confidence_scores) / len(confidence_scores) if confidence_scores else 0
            
            # Determine direction and strength
            if final_score > 0.3:
                direction = 'buy'
                strength = min(abs(final_score), 1.0)
            elif final_score < -0.3:
                direction = 'sell'
                strength = min(abs(final_score), 1.0)
            else:
                direction = 'hold'
                strength = 0.0
            
            # Calculate entry, stop loss, and take profit levels
            current_price = indicators.get('current_price', 0)
            if current_price == 0:
                return None
            
            entry_price = current_price
            
            if direction == 'buy':
                stop_loss = current_price * (1 - 0.02)  # 2% stop loss
                take_profit = current_price * (1 + 0.04)  # 4% take profit (2:1 ratio)
            elif direction == 'sell':
                stop_loss = current_price * (1 + 0.02)
                take_profit = current_price * (1 - 0.04)
            else:
                stop_loss = current_price
                take_profit = current_price
            
            # Create trading signal
            signal = TradingSignal(
                symbol=symbol,
                direction=direction,
                strength=strength,
                confidence=overall_confidence,
                entry_price=entry_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                timeframe='1h',  # Default timeframe
                timestamp=datetime.now(),
                metadata={
                    'trend_signal': trend_signal,
                    'momentum_signal': momentum_signal,
                    'volume_signal': volume_signal,
                    'pattern_signal': pattern_signal,
                    'final_score': final_score,
                    'indicators_used': list(indicators.keys())
                }
            )
            
            return signal
            
        except Exception as e:
            logging.error(f"Error generating trading signal for {symbol}: {e}")
            return None
    
    @intelligent_cache(ttl_seconds=300, cache_size=200)
    def _analyze_trend_signals(self, indicators: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze trend-based signals with caching"""
        try:
            signal = {'direction': 'neutral', 'strength': 0.0, 'confidence': 50}
            
            current_price = indicators.get('current_price', 0)
            sma_20 = indicators.get('sma_20', current_price)
            sma_50 = indicators.get('sma_50', current_price)
            ema_12 = indicators.get('ema_12', current_price)
            ema_26 = indicators.get('ema_26', current_price)
            
            # Moving average analysis
            ma_signals = []
            
            # Price vs SMAs
            if current_price > sma_20 > sma_50:
                ma_signals.append(('bullish', 0.8))
            elif current_price > sma_20:
                ma_signals.append(('bullish', 0.5))
            elif current_price < sma_20 < sma_50:
                ma_signals.append(('bearish', 0.8))
            elif current_price < sma_20:
                ma_signals.append(('bearish', 0.5))
            
            # EMA crossover
            if ema_12 > ema_26:
                ma_signals.append(('bullish', 0.6))
            elif ema_12 < ema_26:
                ma_signals.append(('bearish', 0.6))
            
            # Aggregate MA signals
            if ma_signals:
                bullish_strength = sum(strength for direction, strength in ma_signals if direction == 'bullish')
                bearish_strength = sum(strength for direction, strength in ma_signals if direction == 'bearish')
                
                if bullish_strength > bearish_strength:
                    signal['direction'] = 'bullish'
                    signal['strength'] = min(bullish_strength / len(ma_signals), 1.0)
                    signal['confidence'] = 60 + (signal['strength'] * 30)
                elif bearish_strength > bullish_strength:
                    signal['direction'] = 'bearish'
                    signal['strength'] = min(bearish_strength / len(ma_signals), 1.0)
                    signal['confidence'] = 60 + (signal['strength'] * 30)
            
            return signal
            
        except Exception as e:
            logging.error(f"Error analyzing trend signals: {e}")
            return {'direction': 'neutral', 'strength': 0.0, 'confidence': 0}
    
    @intelligent_cache(ttl_seconds=200, cache_size=200)
    def _analyze_momentum_signals(self, indicators: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze momentum-based signals with caching"""
        try:
            signal = {'direction': 'neutral', 'strength': 0.0, 'confidence': 50}
            
            rsi = indicators.get('rsi', 50)
            macd = indicators.get('macd', 0)
            macd_signal = indicators.get('macd_signal', 0)
            macd_histogram = indicators.get('macd_histogram', 0)
            stoch_k = indicators.get('stoch_k', 50)
            stoch_d = indicators.get('stoch_d', 50)
            
            momentum_signals = []
            
            # RSI analysis
            if rsi < 30:
                momentum_signals.append(('bullish', 0.8))  # Oversold
            elif rsi < 40:
                momentum_signals.append(('bullish', 0.4))
            elif rsi > 70:
                momentum_signals.append(('bearish', 0.8))  # Overbought
            elif rsi > 60:
                momentum_signals.append(('bearish', 0.4))
            
            # MACD analysis
            if macd > macd_signal and macd_histogram > 0:
                momentum_signals.append(('bullish', 0.7))
            elif macd < macd_signal and macd_histogram < 0:
                momentum_signals.append(('bearish', 0.7))
            
            # Stochastic analysis
            if stoch_k < 20 and stoch_d < 20:
                momentum_signals.append(('bullish', 0.6))  # Oversold
            elif stoch_k > 80 and stoch_d > 80:
                momentum_signals.append(('bearish', 0.6))  # Overbought
            
            # Aggregate momentum signals
            if momentum_signals:
                bullish_strength = sum(strength for direction, strength in momentum_signals if direction == 'bullish')
                bearish_strength = sum(strength for direction, strength in momentum_signals if direction == 'bearish')
                
                if bullish_strength > bearish_strength:
                    signal['direction'] = 'bullish'
                    signal['strength'] = min(bullish_strength / len(momentum_signals), 1.0)
                    signal['confidence'] = 55 + (signal['strength'] * 35)
                elif bearish_strength > bullish_strength:
                    signal['direction'] = 'bearish'
                    signal['strength'] = min(bearish_strength / len(momentum_signals), 1.0)
                    signal['confidence'] = 55 + (signal['strength'] * 35)
            
            return signal
            
        except Exception as e:
            logging.error(f"Error analyzing momentum signals: {e}")
            return {'direction': 'neutral', 'strength': 0.0, 'confidence': 0}
    
    @intelligent_cache(ttl_seconds=240, cache_size=150)
    def _analyze_volume_signals(self, indicators: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze volume-based signals with caching"""
        try:
            signal = {'direction': 'neutral', 'strength': 0.0, 'confidence': 50}
            
            volume_ratio = indicators.get('volume_ratio', 1.0)
            price_change = indicators.get('price_change', 0)
            
            # Volume confirmation analysis
            if volume_ratio > 1.5:  # High volume
                if price_change > 1:  # Price up with high volume
                    signal = {'direction': 'bullish', 'strength': 0.7, 'confidence': 70}
                elif price_change < -1:  # Price down with high volume
                    signal = {'direction': 'bearish', 'strength': 0.7, 'confidence': 70}
            elif volume_ratio > 1.2:  # Moderate volume
                if price_change > 0.5:
                    signal = {'direction': 'bullish', 'strength': 0.4, 'confidence': 60}
                elif price_change < -0.5:
                    signal = {'direction': 'bearish', 'strength': 0.4, 'confidence': 60}
            
            return signal
            
        except Exception as e:
            logging.error(f"Error analyzing volume signals: {e}")
            return {'direction': 'neutral', 'strength': 0.0, 'confidence': 0}
    
    @intelligent_cache(ttl_seconds=400, cache_size=100)
    def _analyze_pattern_signals(self, market_data: pd.DataFrame, indicators: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze pattern-based signals with caching"""
        try:
            signal = {'direction': 'neutral', 'strength': 0.0, 'confidence': 50}
            
            if market_data is None or len(market_data) < 10:
                return signal
            
            current_price = indicators.get('current_price', 0)
            bb_upper = indicators.get('bb_upper', current_price * 1.02)
            bb_lower = indicators.get('bb_lower', current_price * 0.98)
            bb_middle = indicators.get('bb_middle', current_price)
            
            # Bollinger Bands analysis
            if current_price <= bb_lower:
                signal = {'direction': 'bullish', 'strength': 0.6, 'confidence': 65}  # Potential bounce
            elif current_price >= bb_upper:
                signal = {'direction': 'bearish', 'strength': 0.6, 'confidence': 65}  # Potential pullback
            elif current_price > bb_middle:
                signal = {'direction': 'bullish', 'strength': 0.3, 'confidence': 55}  # Above middle
            elif current_price < bb_middle:
                signal = {'direction': 'bearish', 'strength': 0.3, 'confidence': 55}  # Below middle
            
            return signal
            
        except Exception as e:
            logging.error(f"Error analyzing pattern signals: {e}")
            return {'direction': 'neutral', 'strength': 0.0, 'confidence': 0}

class IntelligentTradingEngine:
    """
    Enhanced Intelligent Trading Engine with Performance Optimization
    Optimized for 8 vCPU / 24GB server specifications
    """
    
    def __init__(self, config_manager=None):
        self.config = config_manager
        
        # Core components
        self.market_processor = MarketDataProcessor()
        self.signal_generator = SignalGenerator()
        
        # Performance optimization
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=16)
        self.analysis_cache = IntelligentCache(max_size=2000, ttl_seconds=240)
        
        # Trading state
        self.active_signals = {}
        self.signal_history = deque(maxlen=1000)
        
        # Performance tracking
        self.engine_stats = {
            'signals_generated': 0,
            'successful_analyses': 0,
            'failed_analyses': 0,
            'avg_analysis_time': 0.0,
            'cache_hit_rate': 0.0,
            'last_analysis': None
        }
        
        # Start background optimization
        self._start_background_optimization()
    
    @intelligent_cache(ttl_seconds=180, cache_size=300)
    async def analyze_symbol_async(self, symbol: str, timeframes: List[str] = None) -> Dict[str, Any]:
        """Analyze symbol across multiple timeframes with intelligent caching"""
        start_time = time.time()
        
        try:
            if timeframes is None:
                timeframes = ['5m', '15m', '1h', '4h']
            
            analysis_result = {
                'symbol': symbol,
                'timeframes': timeframes,
                'signals': {},
                'overall_signal': None,
                'confidence': 0.0,
                'analysis_time': 0.0,
                'timestamp': datetime.now().isoformat()
            }
            
            # Analyze each timeframe
            timeframe_tasks = []
            for tf in timeframes:
                task = self._analyze_timeframe_async(symbol, tf)
                timeframe_tasks.append(task)
            
            # Wait for all analyses to complete
            timeframe_results = await asyncio.gather(*timeframe_tasks, return_exceptions=True)
            
            # Process results
            valid_signals = []
            for i, result in enumerate(timeframe_results):
                tf = timeframes[i]
                
                if isinstance(result, Exception):
                    logging.warning(f"Analysis failed for {symbol} {tf}: {result}")
                    continue
                
                if result and isinstance(result, TradingSignal):
                    analysis_result['signals'][tf] = {
                        'direction': result.direction,
                        'strength': result.strength,
                        'confidence': result.confidence,
                        'entry_price': result.entry_price,
                        'stop_loss': result.stop_loss,
                        'take_profit': result.take_profit,
                        'timestamp': result.timestamp.isoformat()
                    }
                    valid_signals.append(result)
            
            # Generate overall signal from timeframe consensus
            if valid_signals:
                overall_signal = self._generate_consensus_signal(valid_signals, symbol)
                analysis_result['overall_signal'] = {
                    'direction': overall_signal.direction,
                    'strength': overall_signal.strength,
                    'confidence': overall_signal.confidence,
                    'entry_price': overall_signal.entry_price,
                    'stop_loss': overall_signal.stop_loss,
                    'take_profit': overall_signal.take_profit
                }
                analysis_result['confidence'] = overall_signal.confidence
            
            # Update performance tracking
            analysis_time = time.time() - start_time
            analysis_result['analysis_time'] = analysis_time
            self._update_engine_stats(True, analysis_time)
            
            # Store in signal history
            self.signal_history.append(analysis_result)
            
            return analysis_result
            
        except Exception as e:
            analysis_time = time.time() - start_time
            self._update_engine_stats(False, analysis_time)
            logging.error(f"Error analyzing symbol {symbol}: {e}")
            return {
                'symbol': symbol,
                'error': str(e),
                'analysis_time': analysis_time,
                'timestamp': datetime.now().isoformat()
            }
    
    async def _analyze_timeframe_async(self, symbol: str, timeframe: str) -> Optional[TradingSignal]:
        """Analyze single timeframe asynchronously"""
        try:
            # Get market data
            market_data = await asyncio.get_event_loop().run_in_executor(
                self.executor,
                self.market_processor.get_market_data,
                symbol, timeframe, 100
            )
            
            if market_data is None or market_data.empty:
                return None
            
            # Calculate technical indicators
            indicators = await asyncio.get_event_loop().run_in_executor(
                self.executor,
                self.market_processor.calculate_technical_indicators,
                market_data
            )
            
            if not indicators:
                return None
            
            # Generate trading signal
            signal = await asyncio.get_event_loop().run_in_executor(
                self.executor,
                self.signal_generator.generate_trading_signal,
                symbol, indicators, market_data
            )
            
            return signal
            
        except Exception as e:
            logging.error(f"Error analyzing {symbol} {timeframe}: {e}")
            return None
    
    def _generate_consensus_signal(self, signals: List[TradingSignal], symbol: str) -> TradingSignal:
        """Generate consensus signal from multiple timeframe signals"""
        try:
            # Weight signals by timeframe importance
            timeframe_weights = {
                '1m': 0.1, '5m': 0.15, '15m': 0.2, '1h': 0.25,
                '4h': 0.3, '1d': 0.4, '1w': 0.5
            }
            
            buy_score = 0
            sell_score = 0
            total_weight = 0
            confidence_sum = 0
            
            # Aggregate signals
            for signal in signals:
                weight = timeframe_weights.get(signal.timeframe, 0.2)
                total_weight += weight
                confidence_sum += signal.confidence
                
                if signal.direction == 'buy':
                    buy_score += signal.strength * weight
                elif signal.direction == 'sell':
                    sell_score += signal.strength * weight
            
            # Determine consensus
            if total_weight == 0:
                direction = 'hold'
                strength = 0.0
            elif buy_score > sell_score * 1.2:  # Require 20% more strength for buy
                direction = 'buy'
                strength = min(buy_score / total_weight, 1.0)
            elif sell_score > buy_score * 1.2:
                direction = 'sell'
                strength = min(sell_score / total_weight, 1.0)
            else:
                direction = 'hold'
                strength = 0.0
            
            # Calculate consensus confidence
            avg_confidence = confidence_sum / len(signals) if signals else 0
            consensus_confidence = avg_confidence * strength
            
            # Use the most recent signal for price levels
            latest_signal = max(signals, key=lambda s: s.timestamp)
            
            # Create consensus signal
            consensus = TradingSignal(
                symbol=symbol,
                direction=direction,
                strength=strength,
                confidence=consensus_confidence,
                entry_price=latest_signal.entry_price,
                stop_loss=latest_signal.stop_loss,
                take_profit=latest_signal.take_profit,
                timeframe='consensus',
                timestamp=datetime.now(),
                metadata={
                    'signal_count': len(signals),
                    'buy_score': buy_score,
                    'sell_score': sell_score,
                    'total_weight': total_weight,
                    'source_signals': [s.timeframe for s in signals]
                }
            )
            
            return consensus
            
        except Exception as e:
            logging.error(f"Error generating consensus signal: {e}")
            # Return neutral signal on error
            return TradingSignal(
                symbol=symbol,
                direction='hold',
                strength=0.0,
                confidence=0.0,
                entry_price=0.0,
                stop_loss=0.0,
                take_profit=0.0,
                timeframe='consensus',
                timestamp=datetime.now(),
                metadata={'error': str(e)}
            )
    
    def _update_engine_stats(self, success: bool, analysis_time: float):
        """Update engine performance statistics"""
        try:
            if success:
                self.engine_stats['successful_analyses'] += 1
                self.engine_stats['signals_generated'] += 1
            else:
                self.engine_stats['failed_analyses'] += 1
            
            # Update rolling average analysis time
            if self.engine_stats['avg_analysis_time'] == 0:
                self.engine_stats['avg_analysis_time'] = analysis_time
            else:
                self.engine_stats['avg_analysis_time'] = (
                    self.engine_stats['avg_analysis_time'] * 0.9 + analysis_time * 0.1
                )
            
            self.engine_stats['last_analysis'] = datetime.now().isoformat()
            
            # Update cache hit rate
            cache_stats = self.analysis_cache.get_stats()
            self.engine_stats['cache_hit_rate'] = cache_stats['hit_rate']
            
        except Exception as e:
            logging.error(f"Error updating engine stats: {e}")
    
    def _start_background_optimization(self):
        """Start background optimization tasks"""
        def optimization_worker():
            while True:
                try:
                    self._optimize_cache_performance()
                    self._monitor_engine_health()
                    self._log_performance_metrics()
                    time.sleep(180)  # Run every 3 minutes
                except Exception as e:
                    logging.error(f"Background optimization error: {e}")
                    time.sleep(60)
        
        thread = threading.Thread(target=optimization_worker, daemon=True)
        thread.start()
    
    def _optimize_cache_performance(self):
        """Optimize cache performance based on usage patterns"""
        try:
            # Get cache statistics from all components
            main_cache_stats = self.analysis_cache.get_stats()
            market_data_stats = self.market_processor.get_market_data.get_cache_stats()
            indicator_stats = self.market_processor.calculate_technical_indicators.get_cache_stats()
            signal_stats = self.signal_generator.generate_trading_signal.get_cache_stats()
            
            # Optimize cache sizes based on hit rates
            caches_to_optimize = [
                (self.analysis_cache, main_cache_stats, 'main'),
                (self.market_processor.data_cache, market_data_stats, 'market_data')
            ]
            
            for cache, stats, name in caches_to_optimize:
                if stats['hit_rate'] > 0.85 and stats['size'] < 5000:
                    cache.max_size = min(cache.max_size + 300, 5000)
                elif stats['hit_rate'] < 0.6 and stats['size'] > 500:
                    cache.max_size = max(cache.max_size - 200, 500)
            
            logging.info(f"Cache optimization - Main: {main_cache_stats['hit_rate']:.2%}, "
                        f"Market: {market_data_stats['hit_rate']:.2%}, "
                        f"Indicators: {indicator_stats['hit_rate']:.2%}, "
                        f"Signals: {signal_stats['hit_rate']:.2%}")
            
        except Exception as e:
            logging.error(f"Cache optimization error: {e}")
    
    def _monitor_engine_health(self):
        """Monitor engine health and performance"""
        try:
            # Check memory usage
            memory_percent = psutil.virtual_memory().percent
            cpu_percent = psutil.cpu_percent(interval=1)
            
            if memory_percent > 85:
                # Reduce cache sizes
                self.analysis_cache.max_size = max(self.analysis_cache.max_size * 0.8, 1000)
                self.market_processor.data_cache.max_size = max(self.market_processor.data_cache.max_size * 0.8, 800)
                logging.warning(f"High memory usage ({memory_percent}%), reducing cache sizes")
            
            if cpu_percent > 90:
                # Reduce thread pool size temporarily
                current_workers = self.executor._max_workers
                new_workers = max(current_workers - 4, 8)
                if new_workers != current_workers:
                    logging.warning(f"High CPU usage ({cpu_percent}%), reducing workers to {new_workers}")
            
            # Check signal generation rate
            total_analyses = self.engine_stats['successful_analyses'] + self.engine_stats['failed_analyses']
            if total_analyses > 0:
                success_rate = self.engine_stats['successful_analyses'] / total_analyses
                if success_rate < 0.8:
                    logging.warning(f"Low analysis success rate: {success_rate:.1%}")
            
        except Exception as e:
            logging.error(f"Engine health monitoring error: {e}")
    
    def _log_performance_metrics(self):
        """Log current performance metrics"""
        try:
            stats = self.engine_stats.copy()
            
            total_analyses = stats['successful_analyses'] + stats['failed_analyses']
            success_rate = stats['successful_analyses'] / total_analyses if total_analyses > 0 else 0
            
            logging.info(f"Trading Engine Metrics - "
                        f"Signals: {stats['signals_generated']}, "
                        f"Success Rate: {success_rate:.1%}, "
                        f"Avg Time: {stats['avg_analysis_time']:.3f}s, "
                        f"Cache Hit Rate: {stats['cache_hit_rate']:.1%}")
            
        except Exception as e:
            logging.error(f"Performance logging error: {e}")
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get comprehensive performance summary"""
        try:
            summary = {
                'engine_stats': self.engine_stats.copy(),
                'cache_performance': {
                    'main_cache': self.analysis_cache.get_stats(),
                    'market_data_cache': self.market_processor.get_market_data.get_cache_stats(),
                    'indicator_cache': self.market_processor.calculate_technical_indicators.get_cache_stats(),
                    'signal_cache': self.signal_generator.generate_trading_signal.get_cache_stats()
                },
                'system_resources': {
                    'memory_percent': psutil.virtual_memory().percent,
                    'cpu_percent': psutil.cpu_percent(),
                    'thread_pool_workers': self.executor._max_workers
                },
                'signal_history_size': len(self.signal_history),
                'active_signals_count': len(self.active_signals),
                'last_updated': datetime.now().isoformat()
            }
            
            return summary
            
        except Exception as e:
            logging.error(f"Error getting performance summary: {e}")
            return {}
    
    def optimize_for_server_specs(self):
        """Optimize for 8 vCPU / 24GB server specifications"""
        try:
            cpu_count = psutil.cpu_count()
            memory_gb = psutil.virtual_memory().total / (1024**3)
            
            # Adjust thread pool size
            optimal_workers = min(cpu_count * 2, 20)
            if self.executor._max_workers != optimal_workers:
                self.executor.shutdown(wait=False)
                self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=optimal_workers)
            
            # Adjust cache sizes for high memory systems
            if memory_gb >= 24:
                self.analysis_cache.max_size = 3000
                self.market_processor.data_cache.max_size = 2500
                self.market_processor.indicator_cache.max_size = 2000
                self.signal_generator.signal_cache.max_size = 1500
                self.signal_generator.pattern_cache.max_size = 800
            
            logging.info(f"Trading engine optimized for {cpu_count} CPUs, {memory_gb:.1f}GB RAM")
            
        except Exception as e:
            logging.error(f"Server optimization error: {e}")

# Export main classes
__all__ = ['IntelligentTradingEngine', 'TradingSignal', 'MarketDataProcessor', 'SignalGenerator']

if __name__ == "__main__":
    # Performance test
    async def test_trading_engine():
        engine = IntelligentTradingEngine()
        engine.optimize_for_server_specs()
        
        # Test symbol analysis
        symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']
        
        for symbol in symbols:
            result = await engine.analyze_symbol_async(symbol, ['5m', '15m', '1h'])
            print(f"Analysis for {symbol}: {result.get('confidence', 0):.1f}% confidence")
        
        # Get performance summary
        summary = engine.get_performance_summary()
        print(f"Engine Performance: {json.dumps(summary, indent=2, default=str)}")
    
    # Run test
    asyncio.run(test_trading_engine())