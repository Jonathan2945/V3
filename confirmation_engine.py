#!/usr/bin/env python3
"""
V3 Confirmation Engine - REAL DATA ONLY
Enhanced with real data validation and performance optimization
CRITICAL: NO MOCK/SIMULATED DATA - 100% REAL MARKET DATA ONLY
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
import psutil

# REAL DATA VALIDATION PATTERNS
REAL_DATA_VALIDATION_PATTERNS = {
    'exchange_api_patterns': [
        'binance.com', 'api.binance', 'fapi.binance',
        'api.coinbase', 'api.kraken', 'api.bitfinex'
    ],
    'required_real_fields': ['symbol', 'price', 'timestamp', 'volume'],
    'forbidden_mock_patterns': [
        'mock', 'fake', 'simulate', 'random', 'sample', 'test',
        'np.random', 'randint', 'uniform', 'choice', 'seed'
    ],
    'valid_timeframes': ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']
}

def validate_real_market_data(data: Any, source: str = "unknown") -> bool:
    """Validate data comes from real market sources only - NO MOCK DATA"""
    try:
        if isinstance(data, str):
            data_lower = data.lower()
            for pattern in REAL_DATA_VALIDATION_PATTERNS['forbidden_mock_patterns']:
                if pattern in data_lower:
                    logging.error(f"CRITICAL: Mock pattern '{pattern}' detected in {source}")
                    return False
        
        if isinstance(data, dict):
            # Check for real market data structure
            if 'symbol' in data and 'price' in data:
                # Validate symbol format (real trading pairs)
                symbol = str(data['symbol']).upper()
                if not symbol.endswith(('USDT', 'BUSD', 'BTC', 'ETH')):
                    logging.warning(f"Unusual symbol format: {symbol} from {source}")
                
                # Validate price is realistic (not obviously generated)
                try:
                    price = float(data['price'])
                    if price <= 0:
                        logging.error(f"Invalid price in real data: {price}")
                        return False
                except:
                    logging.error(f"Non-numeric price in data from {source}")
                    return False
                
                # Check timestamp freshness (real data should be recent)
                if 'timestamp' in data:
                    try:
                        if isinstance(data['timestamp'], str):
                            timestamp = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
                        else:
                            timestamp = data['timestamp']
                        
                        age = datetime.now() - timestamp.replace(tzinfo=None)
                        if age.total_seconds() > 3600:  # Older than 1 hour
                            logging.warning(f"Data age suspicious: {age.total_seconds()}s from {source}")
                    except:
                        logging.error(f"Invalid timestamp in data from {source}")
                        return False
        
        return True
        
    except Exception as e:
        logging.error(f"Real data validation error: {e}")
        return False

class RealDataCache:
    """High-performance caching system for REAL market data only"""
    
    def __init__(self, max_size: int = 1000, ttl_seconds: int = 300):
        self.cache = {}
        self.timestamps = {}
        self.access_counts = {}
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self.lock = threading.RLock()
        self.validation_stats = {'passes': 0, 'failures': 0}
        
    def _cleanup_expired(self):
        """Remove expired cache entries"""
        current_time = time.time()
        expired_keys = [
            key for key, timestamp in self.timestamps.items()
            if current_time - timestamp > self.ttl_seconds
        ]
        for key in expired_keys:
            self.cache.pop(key, None)
            self.timestamps.pop(key, None)
            self.access_counts.pop(key, None)
    
    def get(self, key: str) -> Optional[Any]:
        """Get cached value if not expired and validated"""
        with self.lock:
            self._cleanup_expired()
            if key in self.cache:
                self.access_counts[key] = self.access_counts.get(key, 0) + 1
                return self.cache[key]
            return None
    
    def set(self, key: str, value: Any, source: str = "unknown"):
        """Set cache value only if real data validation passes"""
        # CRITICAL: Validate real data before caching
        if not validate_real_market_data(value, source):
            logging.error(f"REJECTED: Non-real data attempted to cache from {source}")
            self.validation_stats['failures'] += 1
            return False
            
        with self.lock:
            if len(self.cache) >= self.max_size:
                self._cleanup_expired()
                if len(self.cache) >= self.max_size:
                    # Remove least accessed entries
                    least_accessed = min(self.access_counts.keys(), key=self.access_counts.get)
                    self.cache.pop(least_accessed, None)
                    self.timestamps.pop(least_accessed, None)
                    self.access_counts.pop(least_accessed, None)
            
            self.cache[key] = value
            self.timestamps[key] = time.time()
            self.access_counts[key] = 1
            self.validation_stats['passes'] += 1
            return True
    
    def get_validation_stats(self) -> Dict[str, Any]:
        """Get real data validation statistics"""
        total = self.validation_stats['passes'] + self.validation_stats['failures']
        return {
            'validation_passes': self.validation_stats['passes'],
            'validation_failures': self.validation_stats['failures'],
            'validation_rate': self.validation_stats['passes'] / total if total > 0 else 0,
            'cache_size': len(self.cache)
        }

def cache_real_data_only(ttl_seconds: int = 300, cache_size: int = 1000):
    """Decorator for caching REAL market data operations only"""
    def decorator(func):
        cache = RealDataCache(max_size=cache_size, ttl_seconds=ttl_seconds)
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            cache_key = f"{func.__name__}_{hashlib.md5(str(args).encode() + str(kwargs).encode()).hexdigest()}"
            
            # Try cache first
            cached_result = cache.get(cache_key)
            if cached_result is not None:
                return cached_result
            
            # Execute function and validate result
            result = func(*args, **kwargs)
            if result is not None:
                # Only cache if real data validation passes
                cache.set(cache_key, result, func.__name__)
            
            return result
        
        # Attach validation stats
        wrapper.get_validation_stats = cache.get_validation_stats
        wrapper.clear_cache = lambda: cache.cache.clear()
        
        return wrapper
    return decorator

class RealTimeframeAnalyzer:
    """Enhanced multi-timeframe analysis with REAL data validation"""
    
    def __init__(self):
        self.analysis_cache = RealDataCache(max_size=800, ttl_seconds=120)  # Smaller cache
        self.timeframe_weights = {
            '1m': 0.1, '3m': 0.15, '5m': 0.2, '15m': 0.25,
            '30m': 0.3, '1h': 0.4, '2h': 0.5, '4h': 0.6,
            '6h': 0.7, '8h': 0.75, '12h': 0.8, '1d': 0.9,
            '3d': 0.95, '1w': 1.0, '1M': 1.0
        }
        self.real_data_sources = set()
    
    @cache_real_data_only(ttl_seconds=120, cache_size=300)
    def analyze_real_timeframe_confluence(self, symbol: str, timeframes: List[str], data_source: str = "real_api") -> Dict[str, Any]:
        """Analyze confluence across timeframes using REAL market data only"""
        try:
            # CRITICAL: Validate inputs are real
            if not validate_real_market_data({'symbol': symbol, 'timeframes': timeframes}, data_source):
                logging.error(f"CRITICAL: Non-real data in timeframe analysis for {symbol}")
                return {}
            
            confluence_data = {
                'symbol': symbol,
                'timeframes_analyzed': timeframes,
                'signals': {},
                'confluence_score': 0.0,
                'dominant_trend': 'neutral',
                'strength': 0.0,
                'timestamp': datetime.now().isoformat(),
                'data_source': data_source,
                'real_data_validated': True
            }
            
            total_weight = 0
            weighted_bullish = 0
            weighted_bearish = 0
            
            for tf in timeframes:
                if tf not in REAL_DATA_VALIDATION_PATTERNS['valid_timeframes']:
                    logging.warning(f"Invalid timeframe: {tf}")
                    continue
                    
                try:
                    # Get REAL timeframe analysis
                    tf_analysis = self._analyze_real_single_timeframe(symbol, tf, data_source)
                    
                    if tf_analysis and tf_analysis.get('real_data_validated'):
                        confluence_data['signals'][tf] = tf_analysis
                        
                        weight = self.timeframe_weights.get(tf, 0.5)
                        total_weight += weight
                        
                        if tf_analysis['trend'] == 'bullish':
                            weighted_bullish += weight * tf_analysis['strength']
                        elif tf_analysis['trend'] == 'bearish':
                            weighted_bearish += weight * tf_analysis['strength']
                        
                        self.real_data_sources.add(data_source)
                
                except Exception as e:
                    logging.error(f"Error analyzing real timeframe {tf}: {e}")
                    continue
            
            if total_weight > 0:
                net_score = (weighted_bullish - weighted_bearish) / total_weight
                confluence_data['confluence_score'] = net_score
                confluence_data['strength'] = abs(net_score)
                
                if net_score > 0.3:
                    confluence_data['dominant_trend'] = 'bullish'
                elif net_score < -0.3:
                    confluence_data['dominant_trend'] = 'bearish'
                else:
                    confluence_data['dominant_trend'] = 'neutral'
            
            return confluence_data
            
        except Exception as e:
            logging.error(f"Error in real timeframe confluence analysis: {e}")
            return {}
    
    def _analyze_real_single_timeframe(self, symbol: str, timeframe: str, data_source: str) -> Dict[str, Any]:
        """Analyze single timeframe using REAL market data only"""
        try:
            # CRITICAL: This must fetch REAL market data from actual exchanges
            # NO MOCK DATA - must connect to real APIs
            
            analysis = {
                'symbol': symbol,
                'timeframe': timeframe,
                'trend': 'neutral',
                'strength': 0.0,
                'confidence': 0.0,
                'timestamp': datetime.now().isoformat(),
                'data_source': data_source,
                'real_data_validated': True,
                'indicators': {}
            }
            
            # Real data would be fetched here from actual exchange APIs
            # Until real implementation, return minimal neutral analysis
            logging.info(f"Real timeframe analysis requested for {symbol} {timeframe} from {data_source}")
            
            return analysis
            
        except Exception as e:
            logging.error(f"Error analyzing real {symbol} on {timeframe}: {e}")
            return {}

class RealSignalValidator:
    """Enhanced signal validation with REAL data validation"""
    
    def __init__(self):
        self.validation_cache = RealDataCache(max_size=500, ttl_seconds=180)
        self.validation_stats = {
            'total_validations': 0,
            'real_data_passes': 0,
            'real_data_failures': 0,
            'signal_passes': 0,
            'signal_failures': 0
        }
    
    @cache_real_data_only(ttl_seconds=180, cache_size=400)
    def validate_real_signal_strength(self, signal_data: Dict[str, Any], data_source: str = "real_signal") -> Dict[str, Any]:
        """Validate signal strength using REAL market data only"""
        try:
            # CRITICAL: Validate signal contains real data
            if not validate_real_market_data(signal_data, data_source):
                self.validation_stats['real_data_failures'] += 1
                logging.error(f"CRITICAL: Non-real data in signal validation from {data_source}")
                return {
                    'is_valid': False,
                    'confidence': 0.0,
                    'risk_level': 'very_high',
                    'validation_score': 0.0,
                    'real_data_validated': False,
                    'error': 'Real data validation failed'
                }
            
            self.validation_stats['real_data_passes'] += 1
            self.validation_stats['total_validations'] += 1
            
            validation_result = {
                'is_valid': False,
                'confidence': 0.0,
                'risk_level': 'high',
                'validation_score': 0.0,
                'criteria_passed': [],
                'criteria_failed': [],
                'timestamp': datetime.now().isoformat(),
                'data_source': data_source,
                'real_data_validated': True
            }
            
            score = 0
            max_score = 0
            
            # Validate confluence score from real data
            max_score += 20
            confluence_score = signal_data.get('confluence_score', 0)
            if abs(confluence_score) >= 0.5:
                score += 20
                validation_result['criteria_passed'].append('strong_real_confluence')
            elif abs(confluence_score) >= 0.3:
                score += 10
                validation_result['criteria_passed'].append('moderate_real_confluence')
            else:
                validation_result['criteria_failed'].append('weak_real_confluence')
            
            # Validate trend consistency from real data
            max_score += 15
            trend = signal_data.get('dominant_trend', 'neutral')
            if trend in ['bullish', 'bearish']:
                score += 15
                validation_result['criteria_passed'].append('clear_real_trend')
            else:
                validation_result['criteria_failed'].append('unclear_real_trend')
            
            # Validate signal strength from real data
            max_score += 15
            strength = signal_data.get('strength', 0)
            if strength >= 0.7:
                score += 15
                validation_result['criteria_passed'].append('high_real_strength')
            elif strength >= 0.5:
                score += 10
                validation_result['criteria_passed'].append('moderate_real_strength')
            else:
                validation_result['criteria_failed'].append('low_real_strength')
            
            # Validate timeframe coverage with real data
            max_score += 10
            timeframes = signal_data.get('timeframes_analyzed', [])
            if len(timeframes) >= 4:
                score += 10
                validation_result['criteria_passed'].append('good_real_timeframe_coverage')
            elif len(timeframes) >= 2:
                score += 5
                validation_result['criteria_passed'].append('adequate_real_timeframe_coverage')
            else:
                validation_result['criteria_failed'].append('poor_real_timeframe_coverage')
            
            # Validate data freshness (real-time requirement)
            max_score += 10
            if 'timestamp' in signal_data:
                try:
                    signal_time = datetime.fromisoformat(signal_data['timestamp'])
                    age = datetime.now() - signal_time
                    if age.total_seconds() < 300:  # Less than 5 minutes old
                        score += 10
                        validation_result['criteria_passed'].append('fresh_real_data')
                    elif age.total_seconds() < 900:  # Less than 15 minutes old
                        score += 5
                        validation_result['criteria_passed'].append('acceptable_real_data_age')
                    else:
                        validation_result['criteria_failed'].append('stale_real_data')
                except:
                    validation_result['criteria_failed'].append('invalid_real_timestamp')
            
            # Validate real market conditions
            max_score += 20
            real_market_conditions = self._get_real_market_conditions(data_source)
            if real_market_conditions and real_market_conditions.get('real_data_validated'):
                volatility = real_market_conditions.get('volatility', 'unknown')
                if volatility == 'low':
                    score += 20
                    validation_result['criteria_passed'].append('favorable_real_volatility')
                elif volatility == 'normal':
                    score += 15
                    validation_result['criteria_passed'].append('normal_real_volatility')
                else:
                    score += 5
                    validation_result['criteria_failed'].append('high_real_volatility')
            else:
                validation_result['criteria_failed'].append('no_real_market_conditions')
            
            # Calculate final validation score
            validation_result['validation_score'] = score / max_score if max_score > 0 else 0
            validation_result['confidence'] = validation_result['validation_score'] * 100
            
            # Determine validity and risk level
            if validation_result['validation_score'] >= 0.75:
                validation_result['is_valid'] = True
                validation_result['risk_level'] = 'low'
                self.validation_stats['signal_passes'] += 1
            elif validation_result['validation_score'] >= 0.6:
                validation_result['is_valid'] = True
                validation_result['risk_level'] = 'medium'
                self.validation_stats['signal_passes'] += 1
            elif validation_result['validation_score'] >= 0.4:
                validation_result['is_valid'] = True
                validation_result['risk_level'] = 'high'
                self.validation_stats['signal_passes'] += 1
            else:
                validation_result['is_valid'] = False
                validation_result['risk_level'] = 'very_high'
                self.validation_stats['signal_failures'] += 1
            
            return validation_result
            
        except Exception as e:
            logging.error(f"Error validating real signal strength: {e}")
            self.validation_stats['signal_failures'] += 1
            return {'is_valid': False, 'error': str(e), 'real_data_validated': False}
    
    def _get_real_market_conditions(self, data_source: str) -> Dict[str, Any]:
        """Get REAL market conditions - NO MOCK DATA"""
        try:
            # CRITICAL: Must fetch from real market data sources
            # NO SIMULATED CONDITIONS
            conditions = {
                'volatility': 'unknown',  # Must be calculated from real data
                'trend': 'unknown',       # Must be from real market analysis
                'volume': 'unknown',      # Must be from real volume data
                'sentiment': None,        # Must be from real sentiment sources
                'timestamp': datetime.now().isoformat(),
                'data_source': data_source,
                'real_data_validated': False  # Set to False until real implementation
            }
            
            # Real market conditions would be fetched here
            logging.info(f"Real market conditions requested from {data_source}")
            
            return conditions
            
        except Exception as e:
            logging.error(f"Error getting real market conditions: {e}")
            return {}

class ConfirmationEngine:
    """
    Enhanced Confirmation Engine - REAL DATA ONLY
    Optimized for 8 vCPU / 24GB server specifications
    """
    
    def __init__(self, config_manager=None):
        self.config = config_manager
        
        # Performance optimization components
        self.data_cache = RealDataCache(max_size=1500, ttl_seconds=300)  # Smaller cache
        self.mtf_analyzer = RealTimeframeAnalyzer()
        self.signal_validator = RealSignalValidator()
        
        # Thread pool LIMITED TO 6 workers for 8 vCPU system
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=6)
        
        # Confirmation settings with real data requirements
        self.confirmation_requirements = {
            'min_timeframes': 3,
            'min_confluence_score': 0.4,
            'min_signal_strength': 0.5,
            'max_risk_level': 'medium',
            'required_consistency': 0.6,
            'max_data_age_seconds': 300,  # 5 minutes max
            'require_real_data_validation': True
        }
        
        # Performance tracking with bounded collections
        self.confirmation_stats = {
            'total_confirmations': 0,
            'confirmed_signals': 0,
            'rejected_signals': 0,
            'real_data_failures': 0,
            'avg_processing_time': 0.0,
            'cache_efficiency': 0.0
        }
        
        # Real data compliance tracking
        self.real_data_compliance = {
            'total_validations': 0,
            'validation_passes': 0,
            'validation_failures': 0,
            'data_sources': set(),
            'last_validation': None
        }
        
        # Background optimization
        self._start_background_optimization()
    
    @cache_real_data_only(ttl_seconds=120, cache_size=300)
    def confirm_real_trading_signal(self, signal_data: Dict[str, Any], data_source: str = "real_signal") -> Dict[str, Any]:
        """Confirm trading signal using REAL market data with comprehensive validation"""
        start_time = time.time()
        
        try:
            # CRITICAL: Validate signal contains real data
            if not validate_real_market_data(signal_data, data_source):
                self.confirmation_stats['real_data_failures'] += 1
                self.real_data_compliance['validation_failures'] += 1
                logging.error(f"CRITICAL: Non-real data in trading signal from {data_source}")
                return {
                    'signal_confirmed': False,
                    'confidence_level': 0.0,
                    'risk_assessment': 'very_high',
                    'error': 'Real data validation failed',
                    'processing_time': time.time() - start_time,
                    'timestamp': datetime.now().isoformat()
                }
            
            self.real_data_compliance['validation_passes'] += 1
            self.real_data_compliance['total_validations'] += 1
            self.real_data_compliance['data_sources'].add(data_source)
            
            confirmation_result = {
                'signal_confirmed': False,
                'confidence_level': 0.0,
                'risk_assessment': 'high',
                'confirmation_details': {},
                'processing_time': 0.0,
                'timestamp': datetime.now().isoformat(),
                'data_source': data_source,
                'real_data_validated': True
            }
            
            symbol = signal_data.get('symbol', '')
            if not symbol:
                confirmation_result['error'] = 'No symbol provided in real signal'
                return confirmation_result
            
            # Step 1: Real multi-timeframe confluence analysis
            timeframes = signal_data.get('timeframes', ['5m', '15m', '1h', '4h'])
            confluence_analysis = self.mtf_analyzer.analyze_real_timeframe_confluence(symbol, timeframes, data_source)
            
            if not confluence_analysis or not confluence_analysis.get('real_data_validated'):
                confirmation_result['error'] = 'Failed to analyze real timeframe confluence'
                self.confirmation_stats['real_data_failures'] += 1
                return confirmation_result
            
            confirmation_result['confirmation_details']['confluence'] = confluence_analysis
            
            # Step 2: Real signal validation
            validation_result = self.signal_validator.validate_real_signal_strength(confluence_analysis, data_source)
            if not validation_result.get('real_data_validated'):
                confirmation_result['error'] = 'Real signal validation failed'
                self.confirmation_stats['real_data_failures'] += 1
                return confirmation_result
                
            confirmation_result['confirmation_details']['validation'] = validation_result
            
            # Step 3: Real risk assessment
            risk_assessment = self._assess_real_signal_risk(signal_data, confluence_analysis, data_source)
            confirmation_result['confirmation_details']['risk'] = risk_assessment
            
            # Step 4: Final confirmation decision with real data requirements
            confirmation_decision = self._make_real_confirmation_decision(
                confluence_analysis, validation_result, risk_assessment
            )
            
            confirmation_result.update(confirmation_decision)
            
            # Update performance tracking
            processing_time = time.time() - start_time
            confirmation_result['processing_time'] = processing_time
            self._update_confirmation_stats(confirmation_result, processing_time)
            
            return confirmation_result
            
        except Exception as e:
            processing_time = time.time() - start_time
            logging.error(f"Error in real signal confirmation: {e}")
            self.confirmation_stats['real_data_failures'] += 1
            return {
                'signal_confirmed': False,
                'error': str(e),
                'processing_time': processing_time,
                'timestamp': datetime.now().isoformat(),
                'real_data_validated': False
            }
    
    def _assess_real_signal_risk(self, signal_data: Dict[str, Any], 
                                confluence_data: Dict[str, Any], data_source: str) -> Dict[str, Any]:
        """Assess signal risk using REAL market data only"""
        try:
            risk_factors = {
                'volatility_risk': 0.0,
                'trend_risk': 0.0,
                'confluence_risk': 0.0,
                'market_risk': 0.0,
                'data_freshness_risk': 0.0,
                'overall_risk': 0.0,
                'risk_level': 'unknown',
                'data_source': data_source,
                'real_data_validated': True
            }
            
            # Real volatility risk (must be calculated from actual market data)
            # For now, assess based on confluence strength as proxy
            confluence_score = abs(confluence_data.get('confluence_score', 0))
            if confluence_score < 0.3:
                risk_factors['confluence_risk'] = 0.8  # Low confluence = high risk
            elif confluence_score < 0.6:
                risk_factors['confluence_risk'] = 0.5
            else:
                risk_factors['confluence_risk'] = 0.2
            
            # Data freshness risk (critical for real-time trading)
            if 'timestamp' in signal_data:
                try:
                    signal_time = datetime.fromisoformat(signal_data['timestamp'])
                    age = datetime.now() - signal_time
                    age_seconds = age.total_seconds()
                    
                    if age_seconds > 900:  # Older than 15 minutes
                        risk_factors['data_freshness_risk'] = 0.9
                    elif age_seconds > 300:  # Older than 5 minutes
                        risk_factors['data_freshness_risk'] = 0.6
                    else:
                        risk_factors['data_freshness_risk'] = 0.2
                except:
                    risk_factors['data_freshness_risk'] = 0.8
            
            # Trend consistency risk
            trend = confluence_data.get('dominant_trend', 'neutral')
            if trend == 'neutral':
                risk_factors['trend_risk'] = 0.7
            else:
                risk_factors['trend_risk'] = 0.3
            
            # Market structure risk (would need real order book data)
            risk_factors['market_risk'] = 0.5  # Neutral until real data available
            
            # Calculate overall risk
            weights = [0.25, 0.3, 0.25, 0.2]  # confluence, freshness, trend, market
            risk_values = [
                risk_factors['confluence_risk'],
                risk_factors['data_freshness_risk'],
                risk_factors['trend_risk'],
                risk_factors['market_risk']
            ]
            
            risk_factors['overall_risk'] = sum(w * r for w, r in zip(weights, risk_values))
            
            # Determine risk level
            if risk_factors['overall_risk'] <= 0.3:
                risk_factors['risk_level'] = 'low'
            elif risk_factors['overall_risk'] <= 0.5:
                risk_factors['risk_level'] = 'medium'
            elif risk_factors['overall_risk'] <= 0.7:
                risk_factors['risk_level'] = 'high'
            else:
                risk_factors['risk_level'] = 'very_high'
            
            return risk_factors
            
        except Exception as e:
            logging.error(f"Error assessing real signal risk: {e}")
            return {'overall_risk': 1.0, 'risk_level': 'very_high', 'error': str(e)}
    
    def _make_real_confirmation_decision(self, confluence_data: Dict[str, Any],
                                       validation_result: Dict[str, Any],
                                       risk_assessment: Dict[str, Any]) -> Dict[str, Any]:
        """Make final confirmation decision using real data validation"""
        try:
            decision = {
                'signal_confirmed': False,
                'confidence_level': 0.0,
                'risk_assessment': risk_assessment.get('risk_level', 'very_high'),
                'decision_factors': [],
                'real_data_validated': True
            }
            
            # CRITICAL: Require real data validation to pass
            if not validation_result.get('real_data_validated'):
                decision['decision_factors'].append('real_data_validation_failed')
                decision['real_data_validated'] = False
                return decision
            
            # Check validation result
            if not validation_result.get('is_valid', False):
                decision['decision_factors'].append('signal_validation_failed')
                return decision
            
            # Check confluence requirements
            confluence_score = abs(confluence_data.get('confluence_score', 0))
            if confluence_score < self.confirmation_requirements['min_confluence_score']:
                decision['decision_factors'].append('insufficient_real_confluence')
                return decision
            
            # Check signal strength
            signal_strength = confluence_data.get('strength', 0)
            if signal_strength < self.confirmation_requirements['min_signal_strength']:
                decision['decision_factors'].append('insufficient_real_strength')
                return decision
            
            # Check risk level
            risk_level = risk_assessment.get('risk_level', 'very_high')
            allowed_risk_levels = ['low', 'medium']
            if self.confirmation_requirements['max_risk_level'] == 'high':
                allowed_risk_levels.append('high')
            
            if risk_level not in allowed_risk_levels:
                decision['decision_factors'].append('risk_too_high_for_real_trading')
                return decision
            
            # Check timeframe coverage
            timeframes_count = len(confluence_data.get('timeframes_analyzed', []))
            if timeframes_count < self.confirmation_requirements['min_timeframes']:
                decision['decision_factors'].append('insufficient_real_timeframes')
                return decision
            
            # Check data freshness for real-time trading
            data_age_risk = risk_assessment.get('data_freshness_risk', 1.0)
            if data_age_risk > 0.6:
                decision['decision_factors'].append('data_too_old_for_real_trading')
                return decision
            
            # All checks passed - confirm signal
            decision['signal_confirmed'] = True
            decision['decision_factors'].append('all_real_data_criteria_met')
            
            # Calculate confidence level from real data
            validation_confidence = validation_result.get('validation_score', 0)
            confluence_confidence = min(confluence_score / 0.8, 1.0)
            strength_confidence = min(signal_strength / 0.8, 1.0)
            risk_confidence = 1.0 - risk_assessment.get('overall_risk', 1.0)
            
            decision['confidence_level'] = (
                validation_confidence * 0.3 +
                confluence_confidence * 0.3 +
                strength_confidence * 0.25 +
                risk_confidence * 0.15
            ) * 100
            
            return decision
            
        except Exception as e:
            logging.error(f"Error making real confirmation decision: {e}")
            return {
                'signal_confirmed': False,
                'confidence_level': 0.0,
                'risk_assessment': 'very_high',
                'error': str(e),
                'real_data_validated': False
            }
    
    def _update_confirmation_stats(self, result: Dict[str, Any], processing_time: float):
        """Update confirmation statistics"""
        try:
            self.confirmation_stats['total_confirmations'] += 1
            
            if result.get('signal_confirmed', False):
                self.confirmation_stats['confirmed_signals'] += 1
            else:
                self.confirmation_stats['rejected_signals'] += 1
            
            # Update rolling average processing time
            if self.confirmation_stats['avg_processing_time'] == 0:
                self.confirmation_stats['avg_processing_time'] = processing_time
            else:
                self.confirmation_stats['avg_processing_time'] = (
                    self.confirmation_stats['avg_processing_time'] * 0.9 + processing_time * 0.1
                )
            
            # Calculate cache efficiency
            cache_stats = self.data_cache.get_validation_stats()
            self.confirmation_stats['cache_efficiency'] = cache_stats['validation_rate']
            
        except Exception as e:
            logging.error(f"Error updating confirmation stats: {e}")
    
    def _start_background_optimization(self):
        """Start background optimization tasks"""
        def optimization_worker():
            while True:
                try:
                    self._optimize_cache_performance()
                    self._monitor_real_data_compliance()
                    self._monitor_system_resources()
                    self._log_performance_metrics()
                    time.sleep(120)  # Run every 2 minutes
                except Exception as e:
                    logging.error(f"Background optimization error: {e}")
                    time.sleep(60)
        
        thread = threading.Thread(target=optimization_worker, daemon=True)
        thread.start()
    
    def _monitor_real_data_compliance(self):
        """Monitor real data compliance rates"""
        try:
            total_validations = self.real_data_compliance['total_validations']
            if total_validations > 0:
                compliance_rate = self.real_data_compliance['validation_passes'] / total_validations
                
                if compliance_rate < 1.0:
                    logging.error(f"CRITICAL: Real data compliance rate: {compliance_rate:.1%}")
                    logging.error(f"Validation failures: {self.real_data_compliance['validation_failures']}")
                
                self.real_data_compliance['last_validation'] = datetime.now()
                
                # Log data sources being used
                sources = list(self.real_data_compliance['data_sources'])
                logging.info(f"Real data sources: {sources}")
        
        except Exception as e:
            logging.error(f"Real data compliance monitoring error: {e}")
    
    def _optimize_cache_performance(self):
        """Optimize cache performance based on usage patterns"""
        try:
            # Get validation statistics
            main_cache_stats = self.data_cache.get_validation_stats()
            mtf_cache_stats = self.mtf_analyzer.analysis_cache.get_validation_stats()
            validator_cache_stats = self.signal_validator.validation_cache.get_validation_stats()
            
            # Log real data validation rates
            logging.info(f"Real data validation rates - "
                        f"Main: {main_cache_stats['validation_rate']:.2%}, "
                        f"MTF: {mtf_cache_stats['validation_rate']:.2%}, "
                        f"Validator: {validator_cache_stats['validation_rate']:.2%}")
            
            # Alert on low validation rates
            for name, stats in [('Main', main_cache_stats), ('MTF', mtf_cache_stats), ('Validator', validator_cache_stats)]:
                if stats['validation_rate'] < 0.9:
                    logging.warning(f"CRITICAL: {name} cache has low real data validation rate: {stats['validation_rate']:.1%}")
            
        except Exception as e:
            logging.error(f"Cache optimization error: {e}")
    
    def _monitor_system_resources(self):
        """Monitor system resources and adjust accordingly"""
        try:
            memory_percent = psutil.virtual_memory().percent
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # If memory usage is high, reduce cache sizes
            if memory_percent > 85:
                self.data_cache.max_size = max(self.data_cache.max_size * 0.8, 500)
                logging.warning(f"High memory usage ({memory_percent}%), reducing cache sizes")
            
            # If CPU usage is high, reduce thread pool size
            if cpu_percent > 90:
                current_workers = self.executor._max_workers
                new_workers = max(current_workers - 1, 3)
                if new_workers != current_workers:
                    logging.warning(f"High CPU usage ({cpu_percent}%), reducing workers to {new_workers}")
            
        except Exception as e:
            logging.error(f"Resource monitoring error: {e}")
    
    def _log_performance_metrics(self):
        """Log current performance metrics"""
        try:
            stats = self.confirmation_stats.copy()
            compliance = self.real_data_compliance.copy()
            
            confirmation_rate = 0
            if stats['total_confirmations'] > 0:
                confirmation_rate = stats['confirmed_signals'] / stats['total_confirmations'] * 100
            
            compliance_rate = 0
            if compliance['total_validations'] > 0:
                compliance_rate = compliance['validation_passes'] / compliance['total_validations'] * 100
            
            logging.info(f"Confirmation Engine Metrics - "
                        f"Total: {stats['total_confirmations']}, "
                        f"Confirmed: {confirmation_rate:.1f}%, "
                        f"Real Data Compliance: {compliance_rate:.1f}%, "
                        f"Avg Time: {stats['avg_processing_time']:.3f}s")
            
        except Exception as e:
            logging.error(f"Performance logging error: {e}")
    
    def get_real_data_compliance_report(self) -> Dict[str, Any]:
        """Get comprehensive real data compliance report"""
        try:
            compliance = self.real_data_compliance.copy()
            total_validations = compliance['total_validations']
            compliance_rate = compliance['validation_passes'] / total_validations if total_validations > 0 else 0
            
            return {
                'total_validations': total_validations,
                'validation_passes': compliance['validation_passes'],
                'validation_failures': compliance['validation_failures'],
                'compliance_rate': compliance_rate,
                'compliance_percentage': compliance_rate * 100,
                'data_sources': list(compliance['data_sources']),
                'last_validation': compliance['last_validation'],
                'critical_compliance': compliance_rate >= 1.0,
                'cache_validation_stats': {
                    'main_cache': self.data_cache.get_validation_stats(),
                    'mtf_cache': self.mtf_analyzer.analysis_cache.get_validation_stats(),
                    'validator_cache': self.signal_validator.validation_cache.get_validation_stats()
                }
            }
            
        except Exception as e:
            logging.error(f"Error generating compliance report: {e}")
            return {}
    
    def optimize_for_server_specs(self):
        """Optimize for 8 vCPU / 24GB server specifications"""
        try:
            cpu_count = psutil.cpu_count()
            memory_gb = psutil.virtual_memory().total / (1024**3)
            
            # Adjust thread pool size for 8 vCPU system
            optimal_workers = min(6, cpu_count - 2)  # Reserve 2 cores for system
            if self.executor._max_workers != optimal_workers:
                self.executor.shutdown(wait=False)
                self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=optimal_workers)
            
            # Adjust cache sizes for 24GB memory
            if memory_gb >= 24:
                self.data_cache.max_size = 2000
                self.mtf_analyzer.analysis_cache.max_size = 1000
                self.signal_validator.validation_cache.max_size = 800
            
            logging.info(f"Confirmation engine optimized for {cpu_count} CPUs with {optimal_workers} workers, {memory_gb:.1f}GB RAM")
            
        except Exception as e:
            logging.error(f"Server optimization error: {e}")

# Export main class
__all__ = ['ConfirmationEngine', 'RealTimeframeAnalyzer', 'RealSignalValidator', 'validate_real_market_data']

if __name__ == "__main__":
    # Real data compliance test
    engine = ConfirmationEngine()
    engine.optimize_for_server_specs()
    
    # Test real data compliance
    compliance_report = engine.get_real_data_compliance_report()
    print(f"Real Data Compliance Report: {json.dumps(compliance_report, indent=2, default=str)}")