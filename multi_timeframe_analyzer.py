#!/usr/bin/env python3
"""
MULTI-TIMEFRAME ANALYZER
========================
Advanced multi-timeframe analysis for trading decisions
Features:
- Cross-timeframe trend analysis
- Timeframe-specific signal generation
- Confluence analysis across timeframes
- Risk assessment by timeframe alignment
- Dynamic timeframe weighting
"""

import os
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
import numpy as np
import pandas as pd
from enum import Enum
from dotenv import load_dotenv
from binance_exchange_manager import exchange_manager
from multi_pair_scanner import multi_pair_scanner
from api_rotation_manager import get_api_key, report_api_result

# Load environment variables
load_dotenv()

class TrendDirection(Enum):
    STRONG_BULLISH = "STRONG_BULLISH"
    BULLISH = "BULLISH"
    NEUTRAL = "NEUTRAL"
    BEARISH = "BEARISH"
    STRONG_BEARISH = "STRONG_BEARISH"

class SignalStrength(Enum):
    VERY_WEAK = 1
    WEAK = 2
    MODERATE = 3
    STRONG = 4
    VERY_STRONG = 5

@dataclass
class TimeframeAnalysis:
    """Analysis result for a specific timeframe"""
    timeframe: str
    trend_direction: TrendDirection
    signal_strength: SignalStrength
    confidence: float
    support_levels: List[float]
    resistance_levels: List[float]
    key_indicators: Dict[str, float]
    volume_profile: Dict[str, float]
    momentum_score: float
    volatility_score: float
    timestamp: datetime

@dataclass
class MultiTimeframeSignal:
    """Combined signal from multiple timeframes"""
    symbol: str
    overall_direction: TrendDirection
    overall_confidence: float
    consensus_score: float
    timeframe_analyses: Dict[str, TimeframeAnalysis]
    dominant_timeframe: str
    conflicting_timeframes: List[str]
    recommended_action: str
    entry_price: float
    stop_loss: float
    take_profit: float
    risk_reward_ratio: float
    position_size_recommendation: float
    reasons: List[str]
    timestamp: datetime

class MultiTimeframeAnalyzer:
    """Advanced multi-timeframe analysis engine"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Configuration from .env
        self.timeframes = os.getenv('TIMEFRAMES', '1m,5m,15m,30m,1h,4h,1d').split(',')
        self.timeframes = [tf.strip() for tf in self.timeframes if tf.strip()]
        self.primary_timeframe = os.getenv('PRIMARY_TIMEFRAME', '15m')
        self.confirm_timeframes = os.getenv('CONFIRM_TIMEFRAMES', '5m,1h').split(',')
        self.confirm_timeframes = [tf.strip() for tf in self.confirm_timeframes if tf.strip()]
        
        # Analysis parameters
        self.multi_timeframe_confirmation = os.getenv('MULTI_TIMEFRAME_CONFIRMATION', 'true').lower() == 'true'
        self.min_timeframe_agreement = int(os.getenv('MIN_TIMEFRAME_AGREEMENT', '2'))
        self.consensus_threshold = float(os.getenv('CONSENSUS_THRESHOLD', '70.0'))
        
        # Timeframe weights (higher timeframes have more weight)
        self.timeframe_weights = {
            '1m': 0.5,
            '5m': 1.0,
            '15m': 1.5,
            '30m': 2.0,
            '1h': 2.5,
            '4h': 3.0,
            '1d': 4.0,
            '1w': 5.0
        }
        
        # Analysis cache
        self.analysis_cache: Dict[str, Dict[str, TimeframeAnalysis]] = {}
        self.signal_cache: Dict[str, MultiTimeframeSignal] = {}
        
        # Performance tracking
        self.analysis_stats = {
            'total_analyses': 0,
            'successful_analyses': 0,
            'cache_hits': 0,
            'avg_analysis_time': 0.0
        }
        
        self.logger.info(f"[MTF_ANALYZER] Multi-timeframe analyzer initialized")
        self.logger.info(f"[MTF_ANALYZER] Timeframes: {self.timeframes}")
        self.logger.info(f"[MTF_ANALYZER] Primary: {self.primary_timeframe}")
    
    async def analyze_symbol_multi_timeframe(self, symbol: str, 
                                           force_refresh: bool = False) -> Optional[MultiTimeframeSignal]:
        """Perform comprehensive multi-timeframe analysis for a symbol"""
        try:
            start_time = datetime.now()
            
            # Check cache first
            if not force_refresh and symbol in self.signal_cache:
                cached_signal = self.signal_cache[symbol]
                if (datetime.now() - cached_signal.timestamp).total_seconds() < 300:  # 5-minute cache
                    self.analysis_stats['cache_hits'] += 1
                    return cached_signal
            
            # Perform analysis for each timeframe
            timeframe_analyses = {}
            
            for timeframe in self.timeframes:
                try:
                    analysis = await self._analyze_single_timeframe(symbol, timeframe)
                    if analysis:
                        timeframe_analyses[timeframe] = analysis
                except Exception as e:
                    self.logger.debug(f"[MTF_ANALYZER] Error analyzing {symbol} {timeframe}: {e}")
                    continue
            
            if not timeframe_analyses:
                self.logger.warning(f"[MTF_ANALYZER] No timeframe analyses available for {symbol}")
                return None
            
            # Combine analyses into multi-timeframe signal
            combined_signal = self._combine_timeframe_analyses(symbol, timeframe_analyses)
            
            # Cache the result
            self.signal_cache[symbol] = combined_signal
            self.analysis_cache[symbol] = timeframe_analyses
            
            # Update statistics
            analysis_time = (datetime.now() - start_time).total_seconds()
            self.analysis_stats['total_analyses'] += 1
            self.analysis_stats['successful_analyses'] += 1
            self.analysis_stats['avg_analysis_time'] = (
                (self.analysis_stats['avg_analysis_time'] * (self.analysis_stats['total_analyses'] - 1) + analysis_time)
                / self.analysis_stats['total_analyses']
            )
            
            return combined_signal
        
        except Exception as e:
            self.logger.error(f"[MTF_ANALYZER] Error in multi-timeframe analysis for {symbol}: {e}")
            return None
    
    async def _analyze_single_timeframe(self, symbol: str, timeframe: str) -> Optional[TimeframeAnalysis]:
        """Analyze a single timeframe for a symbol"""
        try:
            # Get historical data
            historical_data = await multi_pair_scanner._get_historical_data(symbol, timeframe, limit=200)
            if historical_data is None or len(historical_data) < 50:
                return None
            
            # Calculate comprehensive indicators
            indicators = self._calculate_advanced_indicators(historical_data)
            
            # Determine trend direction
            trend_direction = self._determine_trend_direction(historical_data, indicators)
            
            # Calculate signal strength
            signal_strength = self._calculate_signal_strength(indicators, trend_direction)
            
            # Calculate confidence
            confidence = self._calculate_confidence(indicators, historical_data)
            
            # Find support and resistance levels
            support_levels, resistance_levels = self._find_support_resistance(historical_data)
            
            # Calculate volume profile
            volume_profile = self._calculate_volume_profile(historical_data)
            
            # Calculate momentum and volatility scores
            momentum_score = self._calculate_momentum_score(historical_data)
            volatility_score = self._calculate_volatility_score(historical_data)
            
            return TimeframeAnalysis(
                timeframe=timeframe,
                trend_direction=trend_direction,
                signal_strength=signal_strength,
                confidence=confidence,
                support_levels=support_levels,
                resistance_levels=resistance_levels,
                key_indicators=indicators,
                volume_profile=volume_profile,
                momentum_score=momentum_score,
                volatility_score=volatility_score,
                timestamp=datetime.now()
            )
        
        except Exception as e:
            self.logger.debug(f"[MTF_ANALYZER] Error analyzing {symbol} {timeframe}: {e}")
            return None
    
    def _calculate_advanced_indicators(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calculate comprehensive technical indicators"""
        try:
            indicators = {}
            
            # Basic price indicators
            indicators['current_price'] = float(df['close'].iloc[-1])
            indicators['price_change_pct'] = float(((df['close'].iloc[-1] / df['close'].iloc[-2]) - 1) * 100)
            
            # Moving averages
            indicators['sma_10'] = float(df['close'].rolling(10).mean().iloc[-1])
            indicators['sma_20'] = float(df['close'].rolling(20).mean().iloc[-1])
            indicators['sma_50'] = float(df['close'].rolling(50).mean().iloc[-1])
            indicators['ema_12'] = float(df['close'].ewm(span=12).mean().iloc[-1])
            indicators['ema_26'] = float(df['close'].ewm(span=26).mean().iloc[-1])
            
            # RSI
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            indicators['rsi'] = float(rsi.iloc[-1]) if not np.isnan(rsi.iloc[-1]) else 50.0
            
            # MACD
            macd_line = indicators['ema_12'] - indicators['ema_26']
            macd_signal = df['close'].ewm(span=9).mean().iloc[-1]
            indicators['macd'] = macd_line
            indicators['macd_signal'] = float(macd_signal)
            indicators['macd_histogram'] = macd_line - indicators['macd_signal']
            
            # Bollinger Bands
            sma_20 = df['close'].rolling(20).mean()
            std_20 = df['close'].rolling(20).std()
            indicators['bb_upper'] = float((sma_20 + (std_20 * 2)).iloc[-1])
            indicators['bb_lower'] = float((sma_20 - (std_20 * 2)).iloc[-1])
            indicators['bb_width'] = (indicators['bb_upper'] - indicators['bb_lower']) / indicators['sma_20'] * 100
            
            # Stochastic Oscillator
            low_min = df['low'].rolling(14).min()
            high_max = df['high'].rolling(14).max()
            stoch_k = 100 * ((df['close'] - low_min) / (high_max - low_min))
            stoch_d = stoch_k.rolling(3).mean()
            indicators['stoch_k'] = float(stoch_k.iloc[-1]) if not np.isnan(stoch_k.iloc[-1]) else 50.0
            indicators['stoch_d'] = float(stoch_d.iloc[-1]) if not np.isnan(stoch_d.iloc[-1]) else 50.0
            
            # Williams %R
            high_14 = df['high'].rolling(14).max()
            low_14 = df['low'].rolling(14).min()
            williams_r = -100 * ((high_14 - df['close']) / (high_14 - low_14))
            indicators['williams_r'] = float(williams_r.iloc[-1]) if not np.isnan(williams_r.iloc[-1]) else -50.0
            
            # Average True Range (ATR)
            high_low = df['high'] - df['low']
            high_close = np.abs(df['high'] - df['close'].shift())
            low_close = np.abs(df['low'] - df['close'].shift())
            tr = np.maximum(high_low, np.maximum(high_close, low_close))
            atr = tr.rolling(14).mean()
            indicators['atr'] = float(atr.iloc[-1]) if not np.isnan(atr.iloc[-1]) else 0.0
            
            # Commodity Channel Index (CCI)
            typical_price = (df['high'] + df['low'] + df['close']) / 3
            sma_tp = typical_price.rolling(20).mean()
            mad = typical_price.rolling(20).apply(lambda x: np.mean(np.abs(x - np.mean(x))))
            cci = (typical_price - sma_tp) / (0.015 * mad)
            indicators['cci'] = float(cci.iloc[-1]) if not np.isnan(cci.iloc[-1]) else 0.0
            
            # Volume indicators
            indicators['volume_sma'] = float(df['volume'].rolling(20).mean().iloc[-1])
            indicators['volume_ratio'] = float(df['volume'].iloc[-1] / indicators['volume_sma'])
            
            # On-Balance Volume (OBV)
            obv = (df['volume'] * np.where(df['close'] > df['close'].shift(1), 1, 
                   np.where(df['close'] < df['close'].shift(1), -1, 0))).cumsum()
            indicators['obv'] = float(obv.iloc[-1])
            indicators['obv_sma'] = float(obv.rolling(10).mean().iloc[-1])
            
            return indicators
        
        except Exception as e:
            self.logger.debug(f"[MTF_ANALYZER] Error calculating indicators: {e}")
            return {'current_price': float(df['close'].iloc[-1])}
    
    def _determine_trend_direction(self, df: pd.DataFrame, indicators: Dict[str, float]) -> TrendDirection:
        """Determine trend direction based on multiple factors"""
        try:
            bullish_factors = 0
            bearish_factors = 0
            total_factors = 0
            
            current_price = indicators['current_price']
            
            # Moving average analysis
            if 'sma_10' in indicators and 'sma_20' in indicators:
                if indicators['sma_10'] > indicators['sma_20']:
                    bullish_factors += 1
                else:
                    bearish_factors += 1
                total_factors += 1
            
            if 'sma_20' in indicators and 'sma_50' in indicators:
                if indicators['sma_20'] > indicators['sma_50']:
                    bullish_factors += 1
                else:
                    bearish_factors += 1
                total_factors += 1
            
            # Price vs MA analysis
            if 'sma_20' in indicators:
                if current_price > indicators['sma_20']:
                    bullish_factors += 1
                else:
                    bearish_factors += 1
                total_factors += 1
            
            # MACD analysis
            if 'macd' in indicators and 'macd_signal' in indicators:
                if indicators['macd'] > indicators['macd_signal']:
                    bullish_factors += 1
                else:
                    bearish_factors += 1
                total_factors += 1
            
            # RSI analysis
            if 'rsi' in indicators:
                if indicators['rsi'] > 50:
                    bullish_factors += 1
                else:
                    bearish_factors += 1
                total_factors += 1
            
            # Stochastic analysis
            if 'stoch_k' in indicators and 'stoch_d' in indicators:
                if indicators['stoch_k'] > indicators['stoch_d'] and indicators['stoch_k'] > 20:
                    bullish_factors += 1
                elif indicators['stoch_k'] < indicators['stoch_d'] and indicators['stoch_k'] < 80:
                    bearish_factors += 1
                total_factors += 1
            
            # Volume confirmation
            if 'volume_ratio' in indicators:
                if indicators['volume_ratio'] > 1.2:  # High volume
                    # Volume confirms the price direction
                    if indicators.get('price_change_pct', 0) > 0:
                        bullish_factors += 1
                    else:
                        bearish_factors += 1
                total_factors += 1
            
            # Calculate trend strength
            if total_factors == 0:
                return TrendDirection.NEUTRAL
            
            bullish_ratio = bullish_factors / total_factors
            bearish_ratio = bearish_factors / total_factors
            
            if bullish_ratio >= 0.8:
                return TrendDirection.STRONG_BULLISH
            elif bullish_ratio >= 0.6:
                return TrendDirection.BULLISH
            elif bearish_ratio >= 0.8:
                return TrendDirection.STRONG_BEARISH
            elif bearish_ratio >= 0.6:
                return TrendDirection.BEARISH
            else:
                return TrendDirection.NEUTRAL
        
        except Exception as e:
            self.logger.debug(f"[MTF_ANALYZER] Error determining trend: {e}")
            return TrendDirection.NEUTRAL
    
    def _calculate_signal_strength(self, indicators: Dict[str, float], trend: TrendDirection) -> SignalStrength:
        """Calculate signal strength based on indicator alignment"""
        try:
            strength_score = 0
            
            # RSI strength
            rsi = indicators.get('rsi', 50)
            if trend in [TrendDirection.BULLISH, TrendDirection.STRONG_BULLISH]:
                if rsi < 30:
                    strength_score += 2  # Oversold in bullish trend
                elif 40 < rsi < 60:
                    strength_score += 1  # Neutral RSI
            elif trend in [TrendDirection.BEARISH, TrendDirection.STRONG_BEARISH]:
                if rsi > 70:
                    strength_score += 2  # Overbought in bearish trend
                elif 40 < rsi < 60:
                    strength_score += 1  # Neutral RSI
            
            # MACD strength
            macd = indicators.get('macd', 0)
            macd_signal = indicators.get('macd_signal', 0)
            macd_histogram = indicators.get('macd_histogram', 0)
            
            if abs(macd_histogram) > abs(macd * 0.1):  # Strong MACD signal
                strength_score += 2
            elif abs(macd_histogram) > 0:
                strength_score += 1
            
            # Volume confirmation
            volume_ratio = indicators.get('volume_ratio', 1.0)
            if volume_ratio > 1.5:
                strength_score += 2
            elif volume_ratio > 1.2:
                strength_score += 1
            
            # Bollinger Band position
            current_price = indicators.get('current_price', 0)
            bb_upper = indicators.get('bb_upper', 0)
            bb_lower = indicators.get('bb_lower', 0)
            
            if bb_upper > bb_lower:
                bb_position = (current_price - bb_lower) / (bb_upper - bb_lower)
                if trend in [TrendDirection.BULLISH, TrendDirection.STRONG_BULLISH] and bb_position < 0.2:
                    strength_score += 1  # Near lower band in bullish trend
                elif trend in [TrendDirection.BEARISH, TrendDirection.STRONG_BEARISH] and bb_position > 0.8:
                    strength_score += 1  # Near upper band in bearish trend
            
            # Convert to enum
            if strength_score >= 7:
                return SignalStrength.VERY_STRONG
            elif strength_score >= 5:
                return SignalStrength.STRONG
            elif strength_score >= 3:
                return SignalStrength.MODERATE
            elif strength_score >= 1:
                return SignalStrength.WEAK
            else:
                return SignalStrength.VERY_WEAK
        
        except Exception as e:
            self.logger.debug(f"[MTF_ANALYZER] Error calculating signal strength: {e}")
            return SignalStrength.WEAK
    
    def _calculate_confidence(self, indicators: Dict[str, float], df: pd.DataFrame) -> float:
        """Calculate confidence in the analysis"""
        try:
            confidence_factors = []
            
            # Data quality
            data_quality = min(len(df) / 100.0, 1.0)  # Full confidence with 100+ data points
            confidence_factors.append(data_quality)
            
            # Indicator consistency
            rsi = indicators.get('rsi', 50)
            if 20 < rsi < 80:  # RSI in reasonable range
                confidence_factors.append(0.8)
            else:
                confidence_factors.append(0.6)
            
            # Volume consistency
            volume_ratio = indicators.get('volume_ratio', 1.0)
            if 0.5 < volume_ratio < 3.0:  # Reasonable volume
                confidence_factors.append(0.8)
            else:
                confidence_factors.append(0.4)
            
            # Volatility check (stable data is more reliable)
            price_changes = df['close'].pct_change().dropna()
            volatility = price_changes.std()
            if volatility < 0.05:  # Low volatility
                confidence_factors.append(0.9)
            elif volatility < 0.1:
                confidence_factors.append(0.7)
            else:
                confidence_factors.append(0.5)
            
            return min(np.mean(confidence_factors) * 100, 95.0)  # Cap at 95%
        
        except Exception as e:
            self.logger.debug(f"[MTF_ANALYZER] Error calculating confidence: {e}")
            return 50.0
    
    def _find_support_resistance(self, df: pd.DataFrame) -> Tuple[List[float], List[float]]:
        """Find support and resistance levels"""
        try:
            # Simple method using recent highs and lows
            recent_data = df.tail(50)  # Last 50 periods
            
            # Find local highs and lows
            highs = []
            lows = []
            
            for i in range(2, len(recent_data) - 2):
                current_high = recent_data['high'].iloc[i]
                current_low = recent_data['low'].iloc[i]
                
                # Check if it's a local high
                if (current_high > recent_data['high'].iloc[i-2:i].max() and 
                    current_high > recent_data['high'].iloc[i+1:i+3].max()):
                    highs.append(float(current_high))
                
                # Check if it's a local low
                if (current_low < recent_data['low'].iloc[i-2:i].min() and 
                    current_low < recent_data['low'].iloc[i+1:i+3].min()):
                    lows.append(float(current_low))
            
            # Remove duplicates and sort
            resistance_levels = sorted(list(set(highs)), reverse=True)[:5]  # Top 5 resistance
            support_levels = sorted(list(set(lows)))[:5]  # Top 5 support
            
            return support_levels, resistance_levels
        
        except Exception as e:
            self.logger.debug(f"[MTF_ANALYZER] Error finding support/resistance: {e}")
            return [], []
    
    def _calculate_volume_profile(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calculate volume profile metrics"""
        try:
            recent_volume = df['volume'].tail(20)
            
            return {
                'avg_volume': float(recent_volume.mean()),
                'volume_trend': float((recent_volume.iloc[-1] / recent_volume.iloc[0] - 1) * 100),
                'volume_volatility': float(recent_volume.std()),
                'max_volume': float(recent_volume.max()),
                'min_volume': float(recent_volume.min())
            }
        
        except Exception as e:
            self.logger.debug(f"[MTF_ANALYZER] Error calculating volume profile: {e}")
            return {'avg_volume': 0.0, 'volume_trend': 0.0, 'volume_volatility': 0.0,
                   'max_volume': 0.0, 'min_volume': 0.0}
    
    def _calculate_momentum_score(self, df: pd.DataFrame) -> float:
        """Calculate momentum score"""
        try:
            # Rate of change over different periods
            roc_5 = (df['close'].iloc[-1] / df['close'].iloc[-6] - 1) * 100
            roc_10 = (df['close'].iloc[-1] / df['close'].iloc[-11] - 1) * 100
            roc_20 = (df['close'].iloc[-1] / df['close'].iloc[-21] - 1) * 100
            
            # Weighted momentum score
            momentum = (roc_5 * 0.5 + roc_10 * 0.3 + roc_20 * 0.2)
            
            # Normalize to 0-100 scale
            return float(max(-100, min(100, momentum)))
        
        except Exception as e:
            self.logger.debug(f"[MTF_ANALYZER] Error calculating momentum: {e}")
            return 0.0
    
    def _calculate_volatility_score(self, df: pd.DataFrame) -> float:
        """Calculate volatility score"""
        try:
            # Calculate price volatility
            returns = df['close'].pct_change().dropna()
            volatility = returns.std()
            
            # Normalize to 0-100 scale (typical crypto volatility 0-20%)
            volatility_score = min(volatility * 500, 100)  # Scale factor
            
            return float(volatility_score)
        
        except Exception as e:
            self.logger.debug(f"[MTF_ANALYZER] Error calculating volatility: {e}")
            return 50.0
    
    def _combine_timeframe_analyses(self, symbol: str, 
                                  timeframe_analyses: Dict[str, TimeframeAnalysis]) -> MultiTimeframeSignal:
        """Combine multiple timeframe analyses into a single signal"""
        try:
            # Calculate weighted consensus
            total_weight = 0
            weighted_bullish = 0
            weighted_bearish = 0
            
            reasons = []
            conflicting_timeframes = []
            
            # Find dominant timeframe (highest weight with strong signal)
            dominant_timeframe = self.primary_timeframe
            max_weighted_strength = 0
            
            for timeframe, analysis in timeframe_analyses.items():
                weight = self.timeframe_weights.get(timeframe, 1.0)
                confidence_factor = analysis.confidence / 100.0
                weighted_contribution = weight * confidence_factor
                
                total_weight += weighted_contribution
                
                # Add to consensus based on trend direction
                if analysis.trend_direction in [TrendDirection.BULLISH, TrendDirection.STRONG_BULLISH]:
                    weighted_bullish += weighted_contribution
                elif analysis.trend_direction in [TrendDirection.BEARISH, TrendDirection.STRONG_BEARISH]:
                    weighted_bearish += weighted_contribution
                
                # Check for dominant timeframe
                strength_score = analysis.signal_strength.value * weighted_contribution
                if strength_score > max_weighted_strength:
                    max_weighted_strength = strength_score
                    dominant_timeframe = timeframe
                
                # Collect reasons
                if analysis.signal_strength.value >= 3:  # Moderate or stronger
                    reasons.append(f"{timeframe}: {analysis.trend_direction.value}")
            
            # Determine overall direction and confidence
            if total_weight == 0:
                overall_direction = TrendDirection.NEUTRAL
                overall_confidence = 50.0
                consensus_score = 0.0
            else:
                bullish_ratio = weighted_bullish / total_weight
                bearish_ratio = weighted_bearish / total_weight
                
                if bullish_ratio > 0.7:
                    overall_direction = TrendDirection.STRONG_BULLISH
                    overall_confidence = min(95, bullish_ratio * 100)
                elif bullish_ratio > 0.55:
                    overall_direction = TrendDirection.BULLISH
                    overall_confidence = min(85, bullish_ratio * 100)
                elif bearish_ratio > 0.7:
                    overall_direction = TrendDirection.STRONG_BEARISH
                    overall_confidence = min(95, bearish_ratio * 100)
                elif bearish_ratio > 0.55:
                    overall_direction = TrendDirection.BEARISH
                    overall_confidence = min(85, bearish_ratio * 100)
                else:
                    overall_direction = TrendDirection.NEUTRAL
                    overall_confidence = 50.0
                
                consensus_score = max(bullish_ratio, bearish_ratio) * 100
            
            # Find conflicting timeframes
            for timeframe, analysis in timeframe_analyses.items():
                if overall_direction in [TrendDirection.BULLISH, TrendDirection.STRONG_BULLISH]:
                    if analysis.trend_direction in [TrendDirection.BEARISH, TrendDirection.STRONG_BEARISH]:
                        conflicting_timeframes.append(timeframe)
                elif overall_direction in [TrendDirection.BEARISH, TrendDirection.STRONG_BEARISH]:
                    if analysis.trend_direction in [TrendDirection.BULLISH, TrendDirection.STRONG_BULLISH]:
                        conflicting_timeframes.append(timeframe)
            
            # Calculate entry, stop loss, and take profit
            dominant_analysis = timeframe_analyses[dominant_timeframe]
            current_price = dominant_analysis.key_indicators.get('current_price', 0.0)
            
            entry_price = current_price
            atr = dominant_analysis.key_indicators.get('atr', current_price * 0.02)
            
            if overall_direction in [TrendDirection.BULLISH, TrendDirection.STRONG_BULLISH]:
                recommended_action = 'BUY'
                stop_loss = current_price - (atr * 2)
                take_profit = current_price + (atr * 3)
            elif overall_direction in [TrendDirection.BEARISH, TrendDirection.STRONG_BEARISH]:
                recommended_action = 'SELL'
                stop_loss = current_price + (atr * 2)
                take_profit = current_price - (atr * 3)
            else:
                recommended_action = 'HOLD'
                stop_loss = current_price
                take_profit = current_price
            
            # Calculate risk-reward ratio
            if stop_loss != entry_price:
                risk_reward_ratio = abs(take_profit - entry_price) / abs(entry_price - stop_loss)
            else:
                risk_reward_ratio = 1.0
            
            # Position size recommendation based on confidence and risk
            base_position_size = 1.0
            confidence_factor = overall_confidence / 100.0
            risk_factor = min(risk_reward_ratio / 2.0, 1.0)  # Cap risk factor
            
            position_size_recommendation = base_position_size * confidence_factor * risk_factor
            
            return MultiTimeframeSignal(
                symbol=symbol,
                overall_direction=overall_direction,
                overall_confidence=overall_confidence,
                consensus_score=consensus_score,
                timeframe_analyses=timeframe_analyses,
                dominant_timeframe=dominant_timeframe,
                conflicting_timeframes=conflicting_timeframes,
                recommended_action=recommended_action,
                entry_price=entry_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                risk_reward_ratio=risk_reward_ratio,
                position_size_recommendation=position_size_recommendation,
                reasons=reasons,
                timestamp=datetime.now()
            )
        
        except Exception as e:
            self.logger.error(f"[MTF_ANALYZER] Error combining analyses for {symbol}: {e}")
            # Return neutral signal
            return MultiTimeframeSignal(
                symbol=symbol,
                overall_direction=TrendDirection.NEUTRAL,
                overall_confidence=50.0,
                consensus_score=0.0,
                timeframe_analyses=timeframe_analyses,
                dominant_timeframe=self.primary_timeframe,
                conflicting_timeframes=[],
                recommended_action='HOLD',
                entry_price=0.0,
                stop_loss=0.0,
                take_profit=0.0,
                risk_reward_ratio=1.0,
                position_size_recommendation=0.0,
                reasons=['Error in analysis'],
                timestamp=datetime.now()
            )
    
    def get_analyzer_status(self) -> Dict[str, Any]:
        """Get analyzer status and performance metrics"""
        return {
            'timeframes': self.timeframes,
            'primary_timeframe': self.primary_timeframe,
            'confirm_timeframes': self.confirm_timeframes,
            'multi_timeframe_confirmation': self.multi_timeframe_confirmation,
            'min_timeframe_agreement': self.min_timeframe_agreement,
            'consensus_threshold': self.consensus_threshold,
            'cached_analyses': len(self.analysis_cache),
            'cached_signals': len(self.signal_cache),
            'analysis_stats': self.analysis_stats
        }
    
    def get_symbol_analysis(self, symbol: str) -> Optional[MultiTimeframeSignal]:
        """Get cached analysis for a symbol"""
        return self.signal_cache.get(symbol)
    
    def clear_cache(self, symbol: Optional[str] = None):
        """Clear analysis cache"""
        if symbol:
            self.signal_cache.pop(symbol, None)
            self.analysis_cache.pop(symbol, None)
        else:
            self.signal_cache.clear()
            self.analysis_cache.clear()
        
        self.logger.info(f"[MTF_ANALYZER] Cache cleared for {'all symbols' if not symbol else symbol}")

# Global instance
multi_timeframe_analyzer = MultiTimeframeAnalyzer()

# Convenience functions
async def analyze_symbol(symbol: str, force_refresh: bool = False) -> Optional[MultiTimeframeSignal]:
    """Analyze symbol across multiple timeframes"""
    return await multi_timeframe_analyzer.analyze_symbol_multi_timeframe(symbol, force_refresh)

def get_analyzer_status() -> Dict[str, Any]:
    """Get analyzer status"""
    return multi_timeframe_analyzer.get_analyzer_status()

def get_cached_analysis(symbol: str) -> Optional[MultiTimeframeSignal]:
    """Get cached analysis for symbol"""
    return multi_timeframe_analyzer.get_symbol_analysis(symbol)

if __name__ == "__main__":
    # Test the analyzer
    import asyncio
    
    async def test_analyzer():
        print("Testing Multi-Timeframe Analyzer")
        print("=" * 40)
        
        # Test symbols
        test_symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']
        
        for symbol in test_symbols:
            print(f"\nAnalyzing {symbol}...")
            
            # Perform multi-timeframe analysis
            signal = await analyze_symbol(symbol)
            
            if signal:
                print(f"Overall Direction: {signal.overall_direction.value}")
                print(f"Confidence: {signal.overall_confidence:.1f}%")
                print(f"Consensus Score: {signal.consensus_score:.1f}%")
                print(f"Recommended Action: {signal.recommended_action}")
                print(f"Dominant Timeframe: {signal.dominant_timeframe}")
                print(f"Risk/Reward Ratio: {signal.risk_reward_ratio:.2f}")
                print(f"Entry: ${signal.entry_price:.2f}")
                print(f"Stop Loss: ${signal.stop_loss:.2f}")
                print(f"Take Profit: ${signal.take_profit:.2f}")
                
                if signal.reasons:
                    print(f"Reasons: {', '.join(signal.reasons[:3])}")
                
                if signal.conflicting_timeframes:
                    print(f"Conflicting TFs: {', '.join(signal.conflicting_timeframes)}")
            else:
                print("No analysis available")
        
        # Get status
        status = get_analyzer_status()
        print(f"\nAnalyzer Status:")
        print(f"Cached Analyses: {status['cached_analyses']}")
        print(f"Analysis Stats: {status['analysis_stats']}")
    
    asyncio.run(test_analyzer())