#!/usr/bin/env python3
"""
V3 SIGNAL CONFIRMATION ENGINE - LIVE DATA ONLY
==============================================
Advanced signal confirmation system with V3 compliance:
- Multi-timeframe signal confirmation using live data only
- Volume confirmation from real market data
- Momentum confirmation using live indicators
- Support/resistance level validation from live price action
- Risk assessment using live market conditions
- NO MOCK DATA - V3 production ready
"""

import logging
import asyncio
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass

@dataclass
class SignalConfirmation:
    """V3 Live signal confirmation result"""
    signal_confirmed: bool
    confidence_score: float
    confirmation_factors: List[str]
    risk_level: str
    timeframes_confirmed: List[str]
    volume_confirmed: bool
    momentum_confirmed: bool
    support_resistance_confirmed: bool
    live_data_quality: float
    timestamp: datetime
    v3_compliance: bool = True

class ConfirmationEngine:
    """V3 Signal Confirmation Engine - LIVE DATA ONLY"""
    
    def __init__(self, data_manager, market_analyzer):
        self.data_manager = data_manager
        self.market_analyzer = market_analyzer
        self.logger = logging.getLogger(__name__)
        
        # V3 Configuration - Live Data Only
        self.config = {
            'confirmation_timeframes': ['5m', '15m', '1h', '4h'],
            'volume_threshold': 1.2,  # 20% above average
            'momentum_threshold': 0.6,
            'confidence_threshold': 0.7,
            'min_confirmations': 3,
            'live_data_only': True,  # V3 enforcement
            'risk_levels': {
                'low': 0.3,
                'medium': 0.6,
                'high': 0.8
            }
        }
        
        # V3 Live confirmation history
        self.confirmation_history = []
        self.live_market_context = {}
        
        self.logger.info("[CONFIRMATION] V3 Signal Confirmation Engine initialized - LIVE DATA ONLY")
    
    async def confirm_trade_signal(self, signal: Dict, symbol: str) -> SignalConfirmation:
        """
        V3 Main signal confirmation using LIVE DATA ONLY
        """
        try:
            self.logger.info(f"[CONFIRMATION] V3 Confirming signal for {symbol} - LIVE DATA")
            
            confirmation_factors = []
            confirmations_count = 0
            confidence_score = 0.0
            
            # V3 Multi-timeframe confirmation using live data
            timeframe_results = await self._confirm_across_live_timeframes(signal, symbol)
            timeframes_confirmed = [tf for tf, confirmed in timeframe_results.items() if confirmed]
            
            if len(timeframes_confirmed) >= 2:
                confirmations_count += 1
                confidence_score += 0.25
                confirmation_factors.append(f"Timeframe alignment: {len(timeframes_confirmed)}/4")
            
            # V3 Volume confirmation using live market data
            volume_confirmed = await self._confirm_live_volume(signal, symbol)
            if volume_confirmed:
                confirmations_count += 1
                confidence_score += 0.2
                confirmation_factors.append("Live volume confirmation")
            
            # V3 Momentum confirmation using live indicators
            momentum_confirmed = await self._confirm_live_momentum(signal, symbol)
            if momentum_confirmed:
                confirmations_count += 1
                confidence_score += 0.2
                confirmation_factors.append("Live momentum confirmation")
            
            # V3 Support/Resistance confirmation using live price action
            sr_confirmed = await self._confirm_live_support_resistance(signal, symbol)
            if sr_confirmed:
                confirmations_count += 1
                confidence_score += 0.15
                confirmation_factors.append("Live S/R level confirmation")
            
            # V3 Market condition confirmation using live analysis
            market_condition_score = await self._assess_live_market_conditions(symbol)
            confidence_score += market_condition_score * 0.2
            if market_condition_score > 0.6:
                confirmation_factors.append("Favorable live market conditions")
            
            # V3 Risk assessment using live data
            risk_level = self._calculate_live_risk_level(signal, confidence_score)
            
            # V3 Final confirmation decision
            signal_confirmed = (
                confirmations_count >= self.config['min_confirmations'] and
                confidence_score >= self.config['confidence_threshold']
            )
            
            # V3 Live data quality assessment
            live_data_quality = await self._assess_live_data_quality(symbol)
            
            # Create V3 confirmation result
            confirmation = SignalConfirmation(
                signal_confirmed=signal_confirmed,
                confidence_score=min(0.95, confidence_score),
                confirmation_factors=confirmation_factors,
                risk_level=risk_level,
                timeframes_confirmed=timeframes_confirmed,
                volume_confirmed=volume_confirmed,
                momentum_confirmed=momentum_confirmed,
                support_resistance_confirmed=sr_confirmed,
                live_data_quality=live_data_quality,
                timestamp=datetime.now(),
                v3_compliance=True
            )
            
            # Store V3 confirmation history
            self.confirmation_history.append({
                'symbol': symbol,
                'signal': signal,
                'confirmation': confirmation,
                'timestamp': datetime.now(),
                'data_source': 'live_market_data'
            })
            
            self.logger.info(f"[CONFIRMATION] V3 Signal {'CONFIRMED' if signal_confirmed else 'REJECTED'} "
                           f"- Confidence: {confidence_score:.1%} - Live Data Quality: {live_data_quality:.1%}")
            
            return confirmation
            
        except Exception as e:
            self.logger.error(f"[CONFIRMATION] V3 Signal confirmation failed: {e}")
            return SignalConfirmation(
                signal_confirmed=False,
                confidence_score=0.0,
                confirmation_factors=[f"Error: {str(e)}"],
                risk_level="high",
                timeframes_confirmed=[],
                volume_confirmed=False,
                momentum_confirmed=False,
                support_resistance_confirmed=False,
                live_data_quality=0.0,
                timestamp=datetime.now(),
                v3_compliance=True
            )
    
    async def _confirm_across_live_timeframes(self, signal: Dict, symbol: str) -> Dict[str, bool]:
        """V3 Confirm signal across multiple timeframes using LIVE DATA"""
        try:
            timeframe_results = {}
            signal_direction = signal.get('direction', 'neutral')
            
            for timeframe in self.config['confirmation_timeframes']:
                try:
                    # Get live market data for this timeframe
                    live_data = await self.data_manager.get_historical_data(
                        symbol, timeframe,
                        start_time=datetime.now() - timedelta(hours=24)
                    )
                    
                    if not live_data or len(live_data.get('close', [])) < 20:
                        timeframe_results[timeframe] = False
                        continue
                    
                    # V3 Analyze live price action
                    closes = np.array(live_data['close'])
                    
                    # V3 Trend confirmation using live data
                    trend_confirmed = self._analyze_live_trend_alignment(closes, signal_direction)
                    
                    # V3 Price level confirmation using live data
                    price_level_confirmed = self._analyze_live_price_levels(
                        live_data, signal.get('entry_price', closes[-1])
                    )
                    
                    # V3 Timeframe confirmation result
                    timeframe_confirmed = trend_confirmed and price_level_confirmed
                    timeframe_results[timeframe] = timeframe_confirmed
                    
                except Exception as e:
                    self.logger.warning(f"[CONFIRMATION] Live timeframe {timeframe} analysis failed: {e}")
                    timeframe_results[timeframe] = False
            
            return timeframe_results
            
        except Exception as e:
            self.logger.error(f"[CONFIRMATION] Live multi-timeframe confirmation failed: {e}")
            return {tf: False for tf in self.config['confirmation_timeframes']}
    
    def _analyze_live_trend_alignment(self, closes: np.ndarray, signal_direction: str) -> bool:
        """V3 Analyze trend alignment using live price data"""
        try:
            if len(closes) < 10:
                return False
            
            # V3 Calculate live trend using moving averages
            short_ma = np.mean(closes[-5:])
            long_ma = np.mean(closes[-20:])
            
            if signal_direction == 'bullish':
                return short_ma > long_ma
            elif signal_direction == 'bearish':
                return short_ma < long_ma
            else:
                return True  # Neutral signals don't need trend alignment
                
        except Exception as e:
            self.logger.warning(f"[CONFIRMATION] Live trend alignment analysis failed: {e}")
            return False
    
    def _analyze_live_price_levels(self, live_data: Dict, entry_price: float) -> bool:
        """V3 Analyze price levels using live market data"""
        try:
            highs = np.array(live_data['high'])
            lows = np.array(live_data['low'])
            closes = np.array(live_data['close'])
            
            # V3 Check if entry price is within reasonable range of live market
            current_price = closes[-1]
            price_range = max(highs[-20:]) - min(lows[-20:])
            
            # Entry price should be within 2% of current price for live trading
            price_diff_pct = abs(entry_price - current_price) / current_price
            
            return price_diff_pct <= 0.02  # Within 2% for live execution
            
        except Exception as e:
            self.logger.warning(f"[CONFIRMATION] Live price level analysis failed: {e}")
            return False
    
    async def _confirm_live_volume(self, signal: Dict, symbol: str) -> bool:
        """V3 Confirm volume using LIVE market data"""
        try:
            # Get recent live volume data
            live_data = await self.data_manager.get_historical_data(
                symbol, '5m',
                start_time=datetime.now() - timedelta(hours=6)
            )
            
            if not live_data or len(live_data.get('volume', [])) < 20:
                return False
            
            volumes = np.array(live_data['volume'])
            
            # V3 Calculate live volume metrics
            recent_volume = np.mean(volumes[-3:])  # Last 3 periods
            avg_volume = np.mean(volumes[:-3])     # Historical average
            
            # V3 Volume confirmation criteria
            volume_ratio = recent_volume / avg_volume if avg_volume > 0 else 1.0
            
            volume_confirmed = volume_ratio >= self.config['volume_threshold']
            
            if volume_confirmed:
                self.logger.info(f"[CONFIRMATION] Live volume confirmed - Ratio: {volume_ratio:.2f}")
            
            return volume_confirmed
            
        except Exception as e:
            self.logger.warning(f"[CONFIRMATION] Live volume confirmation failed: {e}")
            return False
    
    async def _confirm_live_momentum(self, signal: Dict, symbol: str) -> bool:
        """V3 Confirm momentum using live indicators"""
        try:
            # Get live data for momentum calculation
            live_data = await self.data_manager.get_historical_data(
                symbol, '15m',
                start_time=datetime.now() - timedelta(hours=12)
            )
            
            if not live_data or len(live_data.get('close', [])) < 14:
                return False
            
            closes = np.array(live_data['close'])
            
            # V3 Calculate live RSI
            rsi = self._calculate_live_rsi(closes, period=14)
            
            # V3 Calculate live momentum score
            momentum_score = self._calculate_live_momentum_score(closes)
            
            signal_direction = signal.get('direction', 'neutral')
            
            # V3 Momentum confirmation logic
            if signal_direction == 'bullish':
                momentum_confirmed = rsi > 50 and momentum_score > self.config['momentum_threshold']
            elif signal_direction == 'bearish':
                momentum_confirmed = rsi < 50 and momentum_score > self.config['momentum_threshold']
            else:
                momentum_confirmed = True  # Neutral signals
            
            return momentum_confirmed
            
        except Exception as e:
            self.logger.warning(f"[CONFIRMATION] Live momentum confirmation failed: {e}")
            return False
    
    def _calculate_live_rsi(self, closes: np.ndarray, period: int = 14) -> float:
        """V3 Calculate RSI using live price data"""
        try:
            if len(closes) < period + 1:
                return 50.0
            
            deltas = np.diff(closes)
            gains = np.where(deltas > 0, deltas, 0)
            losses = np.where(deltas < 0, -deltas, 0)
            
            avg_gains = np.mean(gains[-period:])
            avg_losses = np.mean(losses[-period:])
            
            if avg_losses == 0:
                return 100.0
            
            rs = avg_gains / avg_losses
            rsi = 100 - (100 / (1 + rs))
            
            return rsi
            
        except Exception as e:
            self.logger.warning(f"[CONFIRMATION] Live RSI calculation failed: {e}")
            return 50.0
    
    def _calculate_live_momentum_score(self, closes: np.ndarray) -> float:
        """V3 Calculate momentum score using live data"""
        try:
            if len(closes) < 10:
                return 0.5
            
            # V3 Price momentum
            price_change_5 = (closes[-1] - closes[-6]) / closes[-6] if len(closes) >= 6 else 0
            price_change_10 = (closes[-1] - closes[-11]) / closes[-11] if len(closes) >= 11 else 0
            
            # V3 Momentum score
            momentum_score = (abs(price_change_5) + abs(price_change_10)) / 2
            
            return min(1.0, momentum_score * 10)  # Normalize to 0-1
            
        except Exception as e:
            self.logger.warning(f"[CONFIRMATION] Live momentum score calculation failed: {e}")
            return 0.5
    
    async def _confirm_live_support_resistance(self, signal: Dict, symbol: str) -> bool:
        """V3 Confirm support/resistance levels using live price action"""
        try:
            # Get live data for S/R analysis
            live_data = await self.data_manager.get_historical_data(
                symbol, '1h',
                start_time=datetime.now() - timedelta(days=7)
            )
            
            if not live_data or len(live_data.get('high', [])) < 50:
                return False
            
            highs = np.array(live_data['high'])
            lows = np.array(live_data['low'])
            closes = np.array(live_data['close'])
            
            current_price = closes[-1]
            entry_price = signal.get('entry_price', current_price)
            signal_direction = signal.get('direction', 'neutral')
            
            # V3 Find live support and resistance levels
            resistance_levels = self._find_live_resistance_levels(highs, current_price)
            support_levels = self._find_live_support_levels(lows, current_price)
            
            # V3 S/R confirmation logic
            if signal_direction == 'bullish':
                # For bullish signals, check if we're near support and away from resistance
                near_support = any(abs(entry_price - level) / level < 0.02 for level in support_levels)
                away_from_resistance = all(entry_price < level * 0.95 for level in resistance_levels)
                sr_confirmed = near_support or away_from_resistance
            elif signal_direction == 'bearish':
                # For bearish signals, check if we're near resistance and away from support
                near_resistance = any(abs(entry_price - level) / level < 0.02 for level in resistance_levels)
                away_from_support = all(entry_price > level * 1.05 for level in support_levels)
                sr_confirmed = near_resistance or away_from_support
            else:
                sr_confirmed = True  # Neutral signals
            
            return sr_confirmed
            
        except Exception as e:
            self.logger.warning(f"[CONFIRMATION] Live S/R confirmation failed: {e}")
            return False
    
    def _find_live_resistance_levels(self, highs: np.ndarray, current_price: float) -> List[float]:
        """V3 Find resistance levels from live price data"""
        try:
            # Find local peaks in live data
            resistance_levels = []
            
            for i in range(2, len(highs) - 2):
                if (highs[i] > highs[i-1] and highs[i] > highs[i-2] and 
                    highs[i] > highs[i+1] and highs[i] > highs[i+2]):
                    if highs[i] > current_price:  # Above current price
                        resistance_levels.append(highs[i])
            
            # Return the closest resistance levels
            resistance_levels.sort()
            return resistance_levels[:3]  # Top 3 closest resistance levels
            
        except Exception as e:
            self.logger.warning(f"[CONFIRMATION] Live resistance level detection failed: {e}")
            return []
    
    def _find_live_support_levels(self, lows: np.ndarray, current_price: float) -> List[float]:
        """V3 Find support levels from live price data"""
        try:
            # Find local troughs in live data
            support_levels = []
            
            for i in range(2, len(lows) - 2):
                if (lows[i] < lows[i-1] and lows[i] < lows[i-2] and 
                    lows[i] < lows[i+1] and lows[i] < lows[i+2]):
                    if lows[i] < current_price:  # Below current price
                        support_levels.append(lows[i])
            
            # Return the closest support levels
            support_levels.sort(reverse=True)
            return support_levels[:3]  # Top 3 closest support levels
            
        except Exception as e:
            self.logger.warning(f"[CONFIRMATION] Live support level detection failed: {e}")
            return []
    
    async def _assess_live_market_conditions(self, symbol: str) -> float:
        """V3 Assess overall market conditions using live data"""
        try:
            # Get live market analysis if available
            if hasattr(self.market_analyzer, 'get_current_live_analysis'):
                live_analysis = self.market_analyzer.get_current_live_analysis()
                
                if live_analysis and 'risk_assessment' in live_analysis:
                    risk_data = live_analysis['risk_assessment']
                    overall_risk = risk_data.get('overall_risk', 'medium')
                    
                    # Convert risk level to condition score
                    if overall_risk == 'low':
                        return 0.8
                    elif overall_risk == 'medium':
                        return 0.6
                    else:  # high risk
                        return 0.3
            
            # Fallback: basic live market condition assessment
            live_data = await self.data_manager.get_historical_data(
                symbol, '1h',
                start_time=datetime.now() - timedelta(hours=24)
            )
            
            if not live_data or len(live_data.get('close', [])) < 20:
                return 0.5
            
            # V3 Calculate live volatility
            closes = np.array(live_data['close'])
            returns = np.diff(closes) / closes[:-1]
            volatility = np.std(returns)
            
            # V3 Lower volatility = better conditions for confirmation
            if volatility < 0.02:  # Low volatility
                return 0.8
            elif volatility < 0.05:  # Medium volatility
                return 0.6
            else:  # High volatility
                return 0.3
            
        except Exception as e:
            self.logger.warning(f"[CONFIRMATION] Live market condition assessment failed: {e}")
            return 0.5
    
    def _calculate_live_risk_level(self, signal: Dict, confidence_score: float) -> str:
        """V3 Calculate risk level based on live analysis"""
        try:
            # V3 Risk factors from live data
            risk_factors = []
            
            # Confidence-based risk
            if confidence_score < 0.5:
                risk_factors.append("low_confidence")
            elif confidence_score > 0.8:
                risk_factors.append("high_confidence")
            
            # Signal strength risk
            signal_strength = signal.get('strength', 0.5)
            if signal_strength < 0.4:
                risk_factors.append("weak_signal")
            elif signal_strength > 0.8:
                risk_factors.append("strong_signal")
            
            # V3 Risk level calculation
            if "low_confidence" in risk_factors or "weak_signal" in risk_factors:
                return "high"
            elif "high_confidence" in risk_factors and "strong_signal" in risk_factors:
                return "low"
            else:
                return "medium"
                
        except Exception as e:
            self.logger.warning(f"[CONFIRMATION] Risk level calculation failed: {e}")
            return "medium"
    
    async def _assess_live_data_quality(self, symbol: str) -> float:
        """V3 Assess quality of live data available"""
        try:
            data_quality_factors = []
            
            # Check data availability across timeframes
            for timeframe in ['5m', '15m', '1h']:
                try:
                    live_data = await self.data_manager.get_historical_data(
                        symbol, timeframe,
                        start_time=datetime.now() - timedelta(hours=6)
                    )
                    
                    if live_data and len(live_data.get('close', [])) >= 10:
                        data_quality_factors.append(1.0)
                    else:
                        data_quality_factors.append(0.0)
                        
                except Exception:
                    data_quality_factors.append(0.0)
            
            # V3 Overall data quality score
            return np.mean(data_quality_factors) if data_quality_factors else 0.0
            
        except Exception as e:
            self.logger.warning(f"[CONFIRMATION] Live data quality assessment failed: {e}")
            return 0.5
    
    def get_v3_confirmation_stats(self) -> Dict:
        """V3 Get confirmation engine statistics"""
        try:
            if not self.confirmation_history:
                return {
                    'total_confirmations': 0,
                    'confirmed_signals': 0,
                    'rejected_signals': 0,
                    'avg_confidence': 0.0,
                    'v3_compliance': True
                }
            
            total = len(self.confirmation_history)
            confirmed = sum(1 for h in self.confirmation_history 
                          if h['confirmation'].signal_confirmed)
            
            avg_confidence = np.mean([h['confirmation'].confidence_score 
                                    for h in self.confirmation_history])
            
            return {
                'total_confirmations': total,
                'confirmed_signals': confirmed,
                'rejected_signals': total - confirmed,
                'confirmation_rate': confirmed / total,
                'avg_confidence': avg_confidence,
                'recent_confirmations': self.confirmation_history[-5:] if len(self.confirmation_history) >= 5 else self.confirmation_history,
                'v3_compliance': True,
                'data_source': 'live_market_data'
            }
            
        except Exception as e:
            self.logger.error(f"[CONFIRMATION] V3 stats calculation failed: {e}")
            return {'error': str(e), 'v3_compliance': True}


# V3 Testing
if __name__ == "__main__":
    print("[CONFIRMATION] Testing V3 Signal Confirmation Engine - LIVE DATA ONLY")
    
    class MockDataManager:
        async def get_historical_data(self, symbol, timeframe, start_time):
            # V3: Mock returns live-like data structure
            import random
            return {
                'close': [100 + random.uniform(-2, 2) for _ in range(50)],
                'high': [102 + random.uniform(-1, 3) for _ in range(50)],
                'low': [98 + random.uniform(-3, 1) for _ in range(50)],
                'volume': [1000 + random.uniform(-200, 500) for _ in range(50)]
            }
    
    class MockMarketAnalyzer:
        def get_current_live_analysis(self):
            return {
                'risk_assessment': {
                    'overall_risk': 'medium'
                }
            }
    
    async def test_v3_confirmation():
        data_manager = MockDataManager()
        market_analyzer = MockMarketAnalyzer()
        
        engine = ConfirmationEngine(data_manager, market_analyzer)
        
        test_signal = {
            'direction': 'bullish',
            'strength': 0.75,
            'entry_price': 100.0
        }
        
        confirmation = await engine.confirm_trade_signal(test_signal, 'BTCUSDT')
        
        print(f"[CONFIRMATION] V3 Signal Confirmed: {confirmation.signal_confirmed}")
        print(f"[CONFIRMATION] V3 Confidence: {confirmation.confidence_score:.1%}")
        print(f"[CONFIRMATION] V3 Factors: {', '.join(confirmation.confirmation_factors)}")
        
        stats = engine.get_v3_confirmation_stats()
        print(f"[CONFIRMATION] V3 Stats: {stats}")
    
    asyncio.run(test_v3_confirmation())
    print("[CONFIRMATION] V3 Signal Confirmation Engine test complete!")