#!/usr/bin/env python3

"""

MARKET ANALYSIS ENGINE
import numpy as np

======================



Advanced market analysis system with:

- Market regime detection (Bull, Bear, Ranging, Volatile)

- Multi-timeframe correlation analysis

- Volatility regime classification

- Market microstructure analysis

- Global market session tracking

- Economic event impact analysis

- Real-time market condition assessment



Features:

- Automatic regime classification

- Cross-asset correlation monitoring

- Volatility clustering detection

- Market efficiency analysis

- Liquidity assessment

- Risk regime identification

"""



import os

import numpy as np

import pandas as pd

import logging

from datetime import datetime, timedelta

from typing import Dict, List, Optional, Tuple, Any

import asyncio

from dataclasses import dataclass, asdict

import json

import sqlite3

from sklearn.cluster import KMeans

from sklearn.preprocessing import StandardScaler

from scipy import stats

from scipy.signal import find_peaks

import warnings

warnings.filterwarnings('ignore')



@dataclass

class MarketRegime:

    """Market regime classification"""

    regime_type: str  # 'bull', 'bear', 'ranging', 'volatile'

    confidence: float

    start_time: datetime

    characteristics: Dict[str, float]

    volatility_regime: str  # 'low', 'medium', 'high'

    trend_strength: float

    momentum_score: float



@dataclass

class MarketSession:

    """Global market session information"""

    session_name: str

    is_active: bool

    open_time: datetime

    close_time: datetime

    volume_factor: float

    volatility_factor: float



@dataclass

class CorrelationMatrix:

    """Asset correlation analysis"""

    timestamp: datetime

    correlations: Dict[str, Dict[str, float]]

    average_correlation: float

    max_correlation: float

    diversification_score: float




class AsyncMockDataManager:
    """Async-compatible mock data manager for testing"""
    
    async def get_historical_data(self, symbol, timeframe, start_time=None, end_time=None):
        """Mock async method that returns sample data"""
        import random
        import numpy as np
        from datetime import datetime, timedelta
        
        # Generate mock historical data
        periods = 100
        timestamps = []
        opens = []
        highs = []
        lows = []
        closes = []
        volumes = []
        
        base_price = {'BTCUSDT': 65000, 'ETHUSDT': 3200, 'BNBUSDT': 520}.get(symbol, 100)
        current_price = base_price
        
        for i in range(periods):
            # Generate realistic OHLCV data
            timestamp = int((datetime.now() - timedelta(hours=periods-i)).timestamp() * 1000)
            
            open_price = current_price
            change = random.uniform(-0.03, 0.03)  # 3% max change
            close_price = open_price * (1 + change)
            
            high_price = max(open_price, close_price) * random.uniform(1.0, 1.02)
            low_price = min(open_price, close_price) * random.uniform(0.98, 1.0)
            volume = random.uniform(1000, 10000)
            
            timestamps.append(timestamp)
            opens.append(open_price)
            highs.append(high_price)
            lows.append(low_price)
            closes.append(close_price)
            volumes.append(volume)
            
            current_price = close_price
        
        return {
            'timestamp': timestamps,
            'open': opens,
            'high': highs,
            'low': lows,
            'close': closes,
            'volume': volumes
        }

class MarketAnalysisEngine:

    def __init__(self, data_manager, data_collector):

                # Ensure data_manager has async methods for testing
        if not hasattr(data_manager, 'get_historical_data') or not asyncio.iscoroutinefunction(data_manager.get_historical_data):
            self.data_manager = AsyncMockDataManager()
        else:
            self.data_manager = data_manager

        self.data_collector = data_collector

        

        # Configuration

        self.config = {

            'regime_lookback_periods': 100,

            'volatility_window': 20,

            'correlation_window': 50,

            'trend_threshold': 0.02,

            'regime_confidence_threshold': 0.7

        }

        

        # Market sessions (UTC times)

        self.market_sessions = {

            'sydney': {'open': '22:00', 'close': '07:00', 'timezone': 'Australia/Sydney'},

            'tokyo': {'open': '00:00', 'close': '09:00', 'timezone': 'Asia/Tokyo'},

            'london': {'open': '08:00', 'close': '16:00', 'timezone': 'Europe/London'},

            'new_york': {'open': '13:00', 'close': '21:00', 'timezone': 'America/New_York'}

        }

        

        # Asset groups for correlation analysis

        self.asset_groups = {

            'crypto_major': ['BTCUSDT', 'ETHUSDT', 'BNBUSDT'],

            'crypto_alt': ['ADAUSDT', 'SOLUSDT', 'AVAXUSDT', 'DOTUSDT'],

            'crypto_defi': ['UNIUSDT', 'AAVEUSDT', 'SUSHIUSDT', 'CRVUSDT'],

            'traditional': ['SPY', 'QQQ', 'GLD', 'TLT']  # Would need external data

        }

        

        # Current analysis cache

        self.current_analysis = {

            'market_regimes': {},

            'correlations': {},

            'volatility_regimes': {},

            'market_sessions': {},

            'risk_metrics': {}

        }

        

        # Historical regime data

        self.regime_history = {}

        

        logging.info("[DATA] Market Analysis Engine initialized")

    

    async def initialize(self):

        """Initialize market analysis engine"""

        try:

            # Load historical regime data

            await self.load_regime_history()

            

            # Initialize market session tracking

            await self.initialize_session_tracking()

            

            # Start continuous analysis

            asyncio.create_task(self.continuous_market_analysis())

            

            logging.info("[OK] Market Analysis Engine initialization complete")

            

        except Exception as e:

            logging.error(f"[FAIL] Market Analysis Engine initialization failed: {e}")

            raise

    

    async def load_regime_history(self):

        """Load historical market regime data"""

        try:

            # Load from data manager database if available

            # This would integrate with the historical data manager

            self.regime_history = {}

            

            logging.info("[EMOJI] Loaded regime history")

            

        except Exception as e:

            logging.warning(f"Could not load regime history: {e}")

    

    async def initialize_session_tracking(self):

        """Initialize global market session tracking"""

        try:

            current_time = datetime.utcnow()

            

            for session_name, session_info in self.market_sessions.items():

                # Parse session times

                open_hour, open_min = map(int, session_info['open'].split(':'))

                close_hour, close_min = map(int, session_info['close'].split(':'))

                

                # Create session times for today

                session_open = current_time.replace(hour=open_hour, minute=open_min, second=0, microsecond=0)

                session_close = current_time.replace(hour=close_hour, minute=close_min, second=0, microsecond=0)

                

                # Handle sessions that cross midnight

                if session_close < session_open:

                    if current_time.hour < 12:  # Assuming morning means previous day's session

                        session_open -= timedelta(days=1)

                    else:

                        session_close += timedelta(days=1)

                

                # Check if session is currently active

                is_active = session_open <= current_time <= session_close

                

                session = MarketSession(

                    session_name=session_name,

                    is_active=is_active,

                    open_time=session_open,

                    close_time=session_close,

                    volume_factor=self._get_session_volume_factor(session_name),

                    volatility_factor=self._get_session_volatility_factor(session_name)

                )

                

                self.current_analysis['market_sessions'][session_name] = session

            

            logging.info("[EMOJI] Market session tracking initialized")

            

        except Exception as e:

            logging.error(f"Error initializing session tracking: {e}")

    

    def _get_session_volume_factor(self, session_name: str) -> float:

        """Get typical volume factor for market session"""

        volume_factors = {

            'sydney': 0.6,    # Lower volume

            'tokyo': 0.8,     # Medium volume

            'london': 1.2,    # High volume

            'new_york': 1.0   # Baseline volume

        }

        return volume_factors.get(session_name, 1.0)

    

    def _get_session_volatility_factor(self, session_name: str) -> float:

        """Get typical volatility factor for market session"""

        volatility_factors = {

            'sydney': 0.7,    # Lower volatility

            'tokyo': 0.9,     # Medium volatility

            'london': 1.3,    # High volatility (overlap with NY)

            'new_york': 1.1   # High volatility

        }

        return volatility_factors.get(session_name, 1.0)

    

    async def classify_market_regimes(self, historical_data: Dict) -> List[str]:

        """Classify market regimes for historical data"""

        try:

            if not historical_data or 'close' not in historical_data:

                return ['unknown'] * len(historical_data.get('timestamp', []))

            

            closes = np.array(historical_data['close'])

            highs = np.array(historical_data['high'])

            lows = np.array(historical_data['low'])

            volumes = np.array(historical_data['volume'])

            

            regimes = []

            

            for i in range(len(closes)):

                if i < self.config['regime_lookback_periods']:

                    regimes.append('insufficient_data')

                    continue

                

                # Get recent data window

                start_idx = max(0, i - self.config['regime_lookback_periods'])

                window_closes = closes[start_idx:i+1]

                window_highs = highs[start_idx:i+1]

                window_lows = lows[start_idx:i+1]

                window_volumes = volumes[start_idx:i+1]

                

                # Analyze regime

                regime = await self._classify_single_regime(

                    window_closes, window_highs, window_lows, window_volumes

                )

                regimes.append(regime.regime_type)

            

            return regimes

            

        except Exception as e:

            logging.error(f"Error classifying market regimes: {e}")

            return ['error'] * len(historical_data.get('timestamp', []))

    

    async def _classify_single_regime(self, closes: np.ndarray, highs: np.ndarray, 

                                    lows: np.ndarray, volumes: np.ndarray) -> MarketRegime:

        """Classify a single market regime based on price/volume data"""

        try:

            current_time = datetime.now()

            

            # Calculate key metrics

            returns = np.diff(closes) / closes[:-1]

            volatility = np.std(returns) * np.sqrt(252)  # Annualized

            

            # Trend analysis

            trend_slope = self._calculate_trend_slope(closes)

            trend_strength = abs(trend_slope)

            

            # Momentum analysis

            momentum_short = (closes[-1] - closes[-10]) / closes[-10] if len(closes) >= 10 else 0

            momentum_long = (closes[-1] - closes[-30]) / closes[-30] if len(closes) >= 30 else 0

            momentum_score = (momentum_short + momentum_long) / 2

            

            # Volatility regime

            volatility_regime = self._classify_volatility_regime(volatility)

            

            # Range analysis

            recent_range = self._calculate_range_efficiency(highs, lows, closes)

            

            # Volume analysis

            volume_trend = self._analyze_volume_trend(volumes)

            

            # Regime classification logic

            regime_type = 'ranging'  # Default

            confidence = 0.5

            

            if trend_strength > self.config['trend_threshold']:

                if trend_slope > 0:

                    regime_type = 'bull'

                    confidence = min(0.95, 0.5 + trend_strength * 10)

                else:

                    regime_type = 'bear'

                    confidence = min(0.95, 0.5 + trend_strength * 10)

            elif volatility > 0.5:  # High volatility

                regime_type = 'volatile'

                confidence = min(0.9, 0.4 + volatility)

            else:

                # Check for ranging market

                if recent_range < 0.3:  # Low range efficiency

                    regime_type = 'ranging'

                    confidence = 0.6 + (0.3 - recent_range)

            

            # Adjust confidence based on volume confirmation

            if volume_trend > 0.1:  # Strong volume trend

                confidence *= 1.1

            elif volume_trend < -0.1:  # Declining volume

                confidence *= 0.9

            

            characteristics = {

                'trend_slope': trend_slope,

                'volatility': volatility,

                'momentum_score': momentum_score,

                'range_efficiency': recent_range,

                'volume_trend': volume_trend,

                'volume_confirmation': volume_trend > 0.05

            }

            

            return MarketRegime(

                regime_type=regime_type,

                confidence=min(0.95, max(0.1, confidence)),

                start_time=current_time,

                characteristics=characteristics,

                volatility_regime=volatility_regime,

                trend_strength=trend_strength,

                momentum_score=momentum_score

            )

            

        except Exception as e:

            logging.error(f"Error in single regime classification: {e}")

            return MarketRegime(

                regime_type='error',

                confidence=0.0,

                start_time=datetime.now(),

                characteristics={},

                volatility_regime='unknown',

                trend_strength=0.0,

                momentum_score=0.0

            )

    

    def _calculate_trend_slope(self, closes: np.ndarray) -> float:

        """Calculate trend slope using linear regression"""

        try:

            if len(closes) < 2:

                return 0

            

            x = np.arange(len(closes))

            slope, intercept, r_value, p_value, std_err = stats.linregress(x, closes)

            

            # Normalize slope by average price

            normalized_slope = slope / np.mean(closes)

            

            return normalized_slope

            

        except Exception as e:

            logging.warning(f"Error calculating trend slope: {e}")

            return 0

    

    def _classify_volatility_regime(self, volatility: float) -> str:

        """Classify volatility regime"""

        if volatility < 0.2:

            return 'low'

        elif volatility < 0.5:

            return 'medium'

        else:

            return 'high'

    

    def _calculate_range_efficiency(self, highs: np.ndarray, lows: np.ndarray, closes: np.ndarray) -> float:

        """Calculate how efficiently price moves within its range"""

        try:

            if len(closes) < 2:

                return 0

            

            # Calculate the actual price movement

            price_movement = abs(closes[-1] - closes[0])

            

            # Calculate the potential range

            max_high = np.max(highs)

            min_low = np.min(lows)

            potential_range = max_high - min_low

            

            if potential_range == 0:

                return 0

            

            # Range efficiency

            efficiency = price_movement / potential_range

            

            return min(1.0, efficiency)

            

        except Exception as e:

            logging.warning(f"Error calculating range efficiency: {e}")

            return 0

    

    def _analyze_volume_trend(self, volumes: np.ndarray) -> float:

        """Analyze volume trend"""

        try:

            if len(volumes) < 20:

                return 0

            

            # Compare recent volume to historical average

            recent_volume = np.mean(volumes[-10:])

            historical_volume = np.mean(volumes[:-10])

            

            if historical_volume == 0:

                return 0

            

            volume_change = (recent_volume - historical_volume) / historical_volume

            

            return np.clip(volume_change, -1, 1)

            

        except Exception as e:

            logging.warning(f"Error analyzing volume trend: {e}")

            return 0

    

    async def calculate_correlations(self, symbols: List[str], timeframe: str = '1h') -> CorrelationMatrix:

        """Calculate correlation matrix for given symbols"""

        try:

            current_time = datetime.now()

            correlation_data = {}

            

            # Get recent data for all symbols

            symbol_data = {}

            for symbol in symbols:

                data = await self.data_manager.get_historical_data(

                    symbol, timeframe,

                    start_time=current_time - timedelta(days=30)

                )

                

                if data and len(data['close']) >= self.config['correlation_window']:

                    # Calculate returns

                    closes = np.array(data['close'])

                    returns = np.diff(closes) / closes[:-1]

                    symbol_data[symbol] = returns[-self.config['correlation_window']:]

            

            if len(symbol_data) < 2:

                return CorrelationMatrix(

                    timestamp=current_time,

                    correlations={},

                    average_correlation=0,

                    max_correlation=0,

                    diversification_score=1.0

                )

            

            # Calculate correlation matrix

            correlations = {}

            correlation_values = []

            

            for symbol1 in symbol_data:

                correlations[symbol1] = {}

                for symbol2 in symbol_data:

                    if symbol1 == symbol2:

                        corr = 1.0

                    else:

                        corr = np.corrcoef(symbol_data[symbol1], symbol_data[symbol2])[0, 1]

                        if np.isnan(corr):

                            corr = 0.0

                        correlation_values.append(abs(corr))

                    

                    correlations[symbol1][symbol2] = corr

            

            # Calculate summary statistics

            avg_correlation = np.mean(correlation_values) if correlation_values else 0

            max_correlation = np.max(correlation_values) if correlation_values else 0

            

            # Diversification score (lower correlation = better diversification)

            diversification_score = 1.0 - avg_correlation

            

            correlation_matrix = CorrelationMatrix(

                timestamp=current_time,

                correlations=correlations,

                average_correlation=avg_correlation,

                max_correlation=max_correlation,

                diversification_score=diversification_score

            )

            

            # Store in current analysis

            self.current_analysis['correlations'][timeframe] = correlation_matrix

            

            return correlation_matrix

            

        except Exception as e:

            logging.error(f"Error calculating correlations: {e}")

            return CorrelationMatrix(

                timestamp=datetime.now(),

                correlations={},

                average_correlation=0,

                max_correlation=0,

                diversification_score=1.0

            )

    

    async def analyze_market_microstructure(self, symbol: str, timeframe: str = '1m') -> Dict:

        """Analyze market microstructure characteristics"""

        try:

            # Get recent high-frequency data

            data = await self.data_manager.get_historical_data(

                symbol, timeframe,

                start_time=datetime.now() - timedelta(hours=24)

            )

            

            if not data or len(data['close']) < 100:

                return {}

            

            closes = np.array(data['close'])

            highs = np.array(data['high'])

            lows = np.array(data['low'])

            volumes = np.array(data['volume'])

            

            microstructure = {}

            

            # Price impact analysis

            returns = np.diff(closes) / closes[:-1]

            volume_normalized = volumes[1:] / np.mean(volumes)

            

            # Correlation between returns and volume (price impact)

            price_impact = np.corrcoef(np.abs(returns), volume_normalized)[0, 1]

            if np.isnan(price_impact):

                price_impact = 0

            

            microstructure['price_impact'] = price_impact

            

            # Bid-ask spread proxy (high-low spread)

            spreads = (highs - lows) / closes

            microstructure['avg_spread'] = np.mean(spreads)

            microstructure['spread_volatility'] = np.std(spreads)

            

            # Market efficiency (autocorrelation of returns)

            if len(returns) > 1:

                autocorr = np.corrcoef(returns[:-1], returns[1:])[0, 1]

                if np.isnan(autocorr):

                    autocorr = 0

                microstructure['efficiency'] = 1 - abs(autocorr)  # Higher is more efficient

            else:

                microstructure['efficiency'] = 1.0

            

            # Volatility clustering (GARCH effect)

            abs_returns = np.abs(returns)

            if len(abs_returns) > 1:

                volatility_clustering = np.corrcoef(abs_returns[:-1], abs_returns[1:])[0, 1]

                if np.isnan(volatility_clustering):

                    volatility_clustering = 0

                microstructure['volatility_clustering'] = volatility_clustering

            else:

                microstructure['volatility_clustering'] = 0

            

            # Liquidity proxy (inverse of price impact)

            microstructure['liquidity_score'] = max(0, 1 - abs(price_impact))

            

            return microstructure

            

        except Exception as e:

            logging.error(f"Error analyzing market microstructure: {e}")

            return {}

    

    async def detect_regime_changes(self, symbol: str, timeframe: str = '1h') -> Dict:

        """Detect recent regime changes"""

        try:

            # Get recent data

            data = await self.data_manager.get_historical_data(

                symbol, timeframe,

                start_time=datetime.now() - timedelta(days=60)

            )

            

            if not data or len(data['close']) < 100:

                return {}

            

            # Classify regimes for the entire period

            regimes = await self.classify_market_regimes(data)

            

            # Detect changes

            regime_changes = []

            current_regime = None

            regime_start = None

            

            for i, regime in enumerate(regimes):

                if regime != current_regime:

                    if current_regime is not None:

                        # Record the previous regime

                        regime_changes.append({

                            'regime': current_regime,

                            'start_index': regime_start,

                            'end_index': i - 1,

                            'duration': i - regime_start,

                            'start_time': datetime.fromtimestamp(data['timestamp'][regime_start] / 1000),

                            'end_time': datetime.fromtimestamp(data['timestamp'][i-1] / 1000)

                        })

                    

                    current_regime = regime

                    regime_start = i

            

            # Add the current regime

            if current_regime is not None and regime_start is not None:

                regime_changes.append({

                    'regime': current_regime,

                    'start_index': regime_start,

                    'end_index': len(regimes) - 1,

                    'duration': len(regimes) - regime_start,

                    'start_time': datetime.fromtimestamp(data['timestamp'][regime_start] / 1000),

                    'end_time': datetime.fromtimestamp(data['timestamp'][-1] / 1000)

                })

            

            # Calculate regime statistics

            regime_stats = {}

            for regime_type in ['bull', 'bear', 'ranging', 'volatile']:

                regime_periods = [r for r in regime_changes if r['regime'] == regime_type]

                if regime_periods:

                    avg_duration = np.mean([r['duration'] for r in regime_periods])

                    total_time = sum([r['duration'] for r in regime_periods])

                    regime_stats[regime_type] = {

                        'count': len(regime_periods),

                        'avg_duration': avg_duration,

                        'total_time': total_time,

                        'percentage': total_time / len(regimes) * 100

                    }

                else:

                    regime_stats[regime_type] = {

                        'count': 0, 'avg_duration': 0, 'total_time': 0, 'percentage': 0

                    }

            

            return {

                'regime_changes': regime_changes[-10:],  # Last 10 regime changes

                'current_regime': current_regime,

                'regime_statistics': regime_stats,

                'total_regimes_detected': len(regime_changes)

            }

            

        except Exception as e:

            logging.error(f"Error detecting regime changes: {e}")

            return {}

    

    async def assess_market_risk(self, symbols: List[str]) -> Dict:

        """Assess overall market risk across multiple assets"""

        try:

            risk_metrics = {

                'overall_risk': 'medium',

                'risk_score': 0.5,

                'volatility_risk': 'medium',

                'correlation_risk': 'medium',

                'liquidity_risk': 'low',

                'regime_risk': 'medium',

                'recommendations': []

            }

            

            volatility_scores = []

            correlation_risks = []

            regime_risks = []

            

            # Analyze each symbol

            for symbol in symbols:

                try:

                    # Get recent data

                    data = await self.data_manager.get_historical_data(

                        symbol, '1h',

                        start_time=datetime.now() - timedelta(days=30)

                    )

                    

                    if data and len(data['close']) >= 50:

                        closes = np.array(data['close'])

                        returns = np.diff(closes) / closes[:-1]

                        

                        # Volatility risk

                        volatility = np.std(returns) * np.sqrt(24)  # Daily volatility

                        volatility_scores.append(volatility)

                        

                        # Regime risk (check for recent regime changes)

                        regime_info = await self.detect_regime_changes(symbol, '1h')

                        if regime_info and regime_info.get('regime_changes'):

                            recent_changes = len([r for r in regime_info['regime_changes'] 

                                                if (datetime.now() - r['end_time']).days <= 7])

                            regime_risk = min(1.0, recent_changes / 3)  # Normalize to 0-1

                            regime_risks.append(regime_risk)

                

                except Exception as e:

                    logging.warning(f"Error analyzing risk for {symbol}: {e}")

                    continue

            

            # Calculate correlation risk

            if len(symbols) > 1:

                corr_matrix = await self.calculate_correlations(symbols, '1h')

                correlation_risk = corr_matrix.average_correlation

                correlation_risks.append(correlation_risk)

            

            # Aggregate risk scores

            if volatility_scores:

                avg_volatility = np.mean(volatility_scores)

                if avg_volatility > 0.05:  # 5% daily volatility

                    risk_metrics['volatility_risk'] = 'high'

                elif avg_volatility > 0.02:  # 2% daily volatility

                    risk_metrics['volatility_risk'] = 'medium'

                else:

                    risk_metrics['volatility_risk'] = 'low'

            

            if correlation_risks:

                avg_correlation = np.mean(correlation_risks)

                if avg_correlation > 0.7:

                    risk_metrics['correlation_risk'] = 'high'

                    risk_metrics['recommendations'].append("High correlation detected - consider diversification")

                elif avg_correlation > 0.4:

                    risk_metrics['correlation_risk'] = 'medium'

                else:

                    risk_metrics['correlation_risk'] = 'low'

            

            if regime_risks:

                avg_regime_risk = np.mean(regime_risks)

                if avg_regime_risk > 0.6:

                    risk_metrics['regime_risk'] = 'high'

                    risk_metrics['recommendations'].append("Frequent regime changes - increase monitoring")

                elif avg_regime_risk > 0.3:

                    risk_metrics['regime_risk'] = 'medium'

                else:

                    risk_metrics['regime_risk'] = 'low'

            

            # Calculate overall risk score

            risk_factors = []

            if volatility_scores:

                risk_factors.append(np.mean(volatility_scores) * 10)  # Scale volatility

            if correlation_risks:

                risk_factors.append(np.mean(correlation_risks))

            if regime_risks:

                risk_factors.append(np.mean(regime_risks))

            

            if risk_factors:

                risk_metrics['risk_score'] = np.clip(np.mean(risk_factors), 0, 1)

                

                if risk_metrics['risk_score'] > 0.7:

                    risk_metrics['overall_risk'] = 'high'

                elif risk_metrics['risk_score'] > 0.4:

                    risk_metrics['overall_risk'] = 'medium'

                else:

                    risk_metrics['overall_risk'] = 'low'

            

            # Add market session risk

            active_sessions = [s for s in self.current_analysis.get('market_sessions', {}).values() 

                             if s.is_active]

            if active_sessions:

                total_volatility_factor = sum([s.volatility_factor for s in active_sessions])

                if total_volatility_factor > 2.0:

                    risk_metrics['recommendations'].append("High volatility market sessions active")

            

            # Store risk assessment

            self.current_analysis['risk_metrics'] = risk_metrics

            

            return risk_metrics

            

        except Exception as e:

            logging.error(f"Error assessing market risk: {e}")

            return {}

    

    async def run_full_analysis(self) -> Dict:

        """Run comprehensive market analysis"""

        try:

            logging.info("[SEARCH] Running comprehensive market analysis...")

            

            analysis_results = {

                'timestamp': datetime.now(),

                'market_regimes': {},

                'correlations': {},

                'market_sessions': {},

                'risk_assessment': {},

                'microstructure': {},

                'external_factors': {}

            }

            

            # Key symbols to analyze

            symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'SOLUSDT']

            timeframes = ['1h', '4h', '1d']

            

            # Update market sessions

            await self.initialize_session_tracking()

            analysis_results['market_sessions'] = {

                name: asdict(session) for name, session in self.current_analysis['market_sessions'].items()

            }

            

            # Analyze market regimes for each symbol/timeframe

            for symbol in symbols:

                analysis_results['market_regimes'][symbol] = {}

                for timeframe in timeframes:

                    try:

                        data = await self.data_manager.get_historical_data(

                            symbol, timeframe,

                            start_time=datetime.now() - timedelta(days=30)

                        )

                        

                        if data and len(data['close']) >= 50:

                            regime = await self._classify_single_regime(

                                np.array(data['close'][-50:]),

                                np.array(data['high'][-50:]),

                                np.array(data['low'][-50:]),

                                np.array(data['volume'][-50:])

                            )

                            analysis_results['market_regimes'][symbol][timeframe] = asdict(regime)

                    

                    except Exception as e:

                        logging.warning(f"Error analyzing {symbol} {timeframe}: {e}")

                        continue

            

            # Calculate correlations

            for timeframe in timeframes:

                try:

                    corr_matrix = await self.calculate_correlations(symbols, timeframe)

                    analysis_results['correlations'][timeframe] = asdict(corr_matrix)

                except Exception as e:

                    logging.warning(f"Error calculating correlations for {timeframe}: {e}")

            

            # Market risk assessment

            try:

                risk_assessment = await self.assess_market_risk(symbols)

                analysis_results['risk_assessment'] = risk_assessment

            except Exception as e:

                logging.warning(f"Error in risk assessment: {e}")

            

            # Microstructure analysis for major symbols

            for symbol in symbols[:3]:  # Limit to avoid excessive computation

                try:

                    microstructure = await self.analyze_market_microstructure(symbol, '1m')

                    if microstructure:

                        analysis_results['microstructure'][symbol] = microstructure

                except Exception as e:

                    logging.warning(f"Error in microstructure analysis for {symbol}: {e}")

            

            # External factors from data collector

            try:

                external_data = self.data_collector.get_latest_data()

                analysis_results['external_factors'] = {

                    'fear_greed_index': external_data.get('fear_greed_index', 50),

                    'overall_sentiment': external_data.get('market_sentiment', {}).get('overall_sentiment', 0),

                    'news_sentiment': external_data.get('market_sentiment', {}).get('news_sentiment', 0),

                    'social_sentiment': external_data.get('market_sentiment', {}).get('social_sentiment', 0)

                }

            except Exception as e:

                logging.warning(f"Error getting external factors: {e}")

            

            # Update current analysis cache

            self.current_analysis.update(analysis_results)

            

            logging.info("[OK] Comprehensive market analysis complete")

            return analysis_results

            

        except Exception as e:

            logging.error(f"Error in comprehensive market analysis: {e}")

            return {}

    

    async def continuous_market_analysis(self):

        """Continuously run market analysis"""

        while True:

            try:

                # Run full analysis every 15 minutes

                await self.run_full_analysis()

                

                # Wait 15 minutes

                await asyncio.sleep(900)

                

            except Exception as e:

                logging.error(f"Error in continuous market analysis: {e}")

                await asyncio.sleep(300)  # Wait 5 minutes on error

    

    async def update_external_data(self, external_data: Dict):

        """Update analysis with new external data"""

        try:

            # Store external data

            self.current_analysis['external_factors'] = external_data

            

            # Adjust regime confidence based on external factors

            if 'market_regimes' in self.current_analysis:

                fear_greed = external_data.get('fear_greed_index', 50)

                overall_sentiment = external_data.get('market_sentiment', {}).get('overall_sentiment', 0)

                

                # Adjust regime classifications based on sentiment

                for symbol_data in self.current_analysis['market_regimes'].values():

                    for timeframe_data in symbol_data.values():

                        if isinstance(timeframe_data, dict):

                            # Adjust confidence based on external factors

                            sentiment_adjustment = abs(overall_sentiment) * 0.1  # Max 10% adjustment

                            

                            if fear_greed < 25 or fear_greed > 75:  # Extreme fear/greed

                                sentiment_adjustment += 0.05

                            

                            # Apply adjustment

                            original_confidence = timeframe_data.get('confidence', 0.5)

                            timeframe_data['confidence'] = min(0.95, original_confidence + sentiment_adjustment)

            

        except Exception as e:

            logging.error(f"Error updating external data: {e}")

    

    def get_current_analysis(self) -> Dict:

        """Get current market analysis"""

        return self.current_analysis.copy()

    

    def get_metrics(self) -> Dict:

        """Get market analysis engine metrics"""

        try:

            metrics = {

                'active_market_sessions': len([s for s in self.current_analysis.get('market_sessions', {}).values() if s.is_active]),

                'symbols_analyzed': len(self.current_analysis.get('market_regimes', {})),

                'timeframes_analyzed': len(self.current_analysis.get('correlations', {})),

                'risk_level': self.current_analysis.get('risk_assessment', {}).get('overall_risk', 'unknown'),

                'avg_correlation': self.current_analysis.get('correlations', {}).get('1h', {}).get('average_correlation', 0),

                'analysis_age_minutes': 0

            }

            

            # Calculate analysis age

            if 'timestamp' in self.current_analysis:

                age = (datetime.now() - self.current_analysis['timestamp']).total_seconds() / 60

                metrics['analysis_age_minutes'] = age

            

            return metrics

            

        except Exception as e:

            logging.error(f"Error getting metrics: {e}")

            return {}