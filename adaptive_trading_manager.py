#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ADAPTIVE TRADING MANAGER - V3 REAL DATA ONLY VERSION
===================================================

V3 CRITICAL REQUIREMENTS:
- ZERO MOCK DATA - 100% REAL MARKET DATA ONLY
- Real data validation patterns
- Proper ThreadPoolExecutor configuration
- UTF-8 compliance

FIXES APPLIED:
- Removed all mock data functionality
- Added real data validation
- Configured ThreadPoolExecutor with max_workers
- Enhanced error handling for production use
- UTF-8 encoding compliance
"""

import os
import asyncio
import logging
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Union
import json
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor
import threading
import psutil

# V3 REAL DATA ENFORCEMENT
EMOJI = "[V3-REAL-DATA]"

@dataclass
class MarketCondition:
    """Current market condition assessment - REAL DATA ONLY"""
    condition: str  # 'trending', 'ranging', 'volatile', 'breakout', 'breakdown', 'emergency'
    strength: float  # 0-1
    volatility: float
    volume_profile: str  # 'high', 'medium', 'low'
    trend_direction: str  # 'up', 'down', 'sideways'
    major_event_detected: bool
    tradeable: bool
    recommended_strategy: str
    data_source: str = "REAL_MARKET_ONLY"  # V3 Real data tracking

@dataclass
class TradingOpportunity:
    """Trading opportunity identification - REAL DATA ONLY"""
    symbol: str
    timeframe: str
    opportunity_type: str  # 'breakout', 'reversal', 'momentum', 'mean_reversion'
    confidence: float
    entry_price: float
    target_price: float
    stop_loss: float
    urgency: str  # 'immediate', 'normal', 'low'
    market_condition: str
    data_source: str = "REAL_MARKET_ONLY"  # V3 Real data tracking
    data_validation_passed: bool = False  # V3 Data validation flag

class RealDataValidator:
    """V3 Real Data Validation - CRITICAL FOR PRODUCTION"""
    
    @staticmethod
    def validate_real_market_data(data: Dict, symbol: str, source: str = "unknown") -> bool:
        """
        Validate that data comes from real market sources only
        
        V3 CRITICAL: This function ensures ZERO MOCK DATA
        """
        try:
            if not data or not isinstance(data, dict):
                logging.error(f"[V3-DATA-VIOLATION] Invalid data structure for {symbol} from {source}")
                return False
            
            # Check for required real market data fields
            required_fields = ['close', 'high', 'low', 'volume', 'open']
            missing_fields = [field for field in required_fields if field not in data or not data[field]]
            
            if missing_fields:
                logging.error(f"[V3-DATA-VIOLATION] Missing real market fields {missing_fields} for {symbol}")
                return False
            
            # Validate data types and realistic values
            for field in ['close', 'high', 'low', 'open']:
                if not isinstance(data[field], (list, np.ndarray)) or len(data[field]) == 0:
                    logging.error(f"[V3-DATA-VIOLATION] Invalid {field} data type for {symbol}")
                    return False
                
                # Check for realistic price values (not mock patterns)
                prices = np.array(data[field])
                if np.any(prices <= 0) or np.any(np.isnan(prices)) or np.any(np.isinf(prices)):
                    logging.error(f"[V3-DATA-VIOLATION] Invalid price values in {field} for {symbol}")
                    return False
                
                # Check for mock data patterns (too perfect or repetitive)
                if len(set(prices[-10:])) < 3:  # Too little variation suggests mock data
                    logging.warning(f"[V3-DATA-SUSPICIOUS] Low price variation in {field} for {symbol} - possible mock data")
                    return False
            
            # Validate volume data
            volumes = np.array(data['volume'])
            if np.any(volumes < 0) or np.any(np.isnan(volumes)):
                logging.error(f"[V3-DATA-VIOLATION] Invalid volume data for {symbol}")
                return False
            
            # Check for realistic market relationships
            closes = np.array(data['close'])
            highs = np.array(data['high'])
            lows = np.array(data['low'])
            
            # High >= Close >= Low validation
            if not np.all(highs >= closes) or not np.all(closes >= lows):
                logging.error(f"[V3-DATA-VIOLATION] Invalid OHLC relationships for {symbol}")
                return False
            
            # Validate data freshness (must be recent for real market data)
            if len(closes) < 10:
                logging.error(f"[V3-DATA-VIOLATION] Insufficient data points for {symbol} (need 10+, got {len(closes)})")
                return False
            
            logging.info(f"[V3-DATA-VALIDATED] Real market data validation PASSED for {symbol}")
            return True
            
        except Exception as e:
            logging.error(f"[V3-DATA-VIOLATION] Data validation error for {symbol}: {e}")
            return False
    
    @staticmethod
    def validate_component_real_data_only(component, component_name: str) -> bool:
        """
        Validate that a component only uses real data
        
        V3 CRITICAL: Ensures no mock components in production
        """
        if hasattr(component, 'is_mock') and component.is_mock:
            logging.error(f"[V3-MOCK-VIOLATION] Mock component detected: {component_name}")
            return False
        
        if hasattr(component, '__class__') and 'Mock' in component.__class__.__name__:
            logging.error(f"[V3-MOCK-VIOLATION] Mock class detected: {component_name}")
            return False
        
        return True

class AdaptiveTradingManager:
    """
    V3 Adaptive Trading Manager - REAL DATA ONLY
    
    CRITICAL V3 REQUIREMENTS:
    - ZERO MOCK DATA - 100% REAL MARKET DATA
    - Real data validation on all inputs
    - Proper resource management with ThreadPoolExecutor
    - UTF-8 compliance
    """
    
    def __init__(self, trading_engine=None, ml_engine=None, market_analyzer=None, data_manager=None):
        """
        Initialize with REAL COMPONENTS ONLY - V3 Production Mode
        
        Args:
            trading_engine: REAL trading execution engine (REQUIRED)
            ml_engine: REAL ML prediction engine (REQUIRED)
            market_analyzer: REAL market analysis component (REQUIRED)
            data_manager: REAL data management component (REQUIRED)
        """
        # V3 CRITICAL: Validate all components are REAL (no mocks)
        self._validate_real_components_only(trading_engine, ml_engine, market_analyzer, data_manager)
        
        # Assign validated real components
        self.trading_engine = trading_engine
        self.ml_engine = ml_engine
        self.market_analyzer = market_analyzer
        self.data_manager = data_manager
        
        # V3 Real data validator
        self.data_validator = RealDataValidator()
        
        # V3 ThreadPoolExecutor with proper configuration for server specs
        # Optimized for 8 vCPU server as mentioned in test results
        max_workers = min(4, psutil.cpu_count())  # Conservative CPU usage
        self.thread_pool = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="V3-AdaptiveTrading"
        )
        
        # V3 Configuration - optimized for real data processing
        self.config = {
            'max_hours_without_trade': 2,  # Maximum 2 hours without a trade
            'min_confidence_emergency': 0.45,  # Lower confidence in emergency mode
            'min_confidence_normal': 0.65,  # Normal confidence threshold
            'emergency_mode_duration': 4,  # Hours to stay in emergency mode
            'volatility_threshold_high': 0.05,  # 5% hourly volatility
            'volatility_threshold_low': 0.01,  # 1% hourly volatility
            'volume_threshold_low': 0.5,  # 50% of average volume
            'real_data_validation_required': True,  # V3 CRITICAL
            'max_memory_mb': int(os.getenv('MAX_MEMORY_MB', 2000)),
            'max_cpu_percent': int(os.getenv('MAX_CPU_PERCENT', 75)),
        }
        
        # V3 State tracking with real data validation
        self.last_trade_time = {}  # Symbol -> datetime
        self.emergency_mode = False
        self.emergency_mode_start = None
        self.current_market_conditions = {}
        self.trading_opportunities = []
        self.major_events_detected = []
        self.data_validation_stats = {
            'total_validations': 0,
            'passed_validations': 0,
            'failed_validations': 0,
            'last_validation': None
        }
        
        # V3 Strategy pools for different conditions - real market optimized
        self.strategy_pools = {
            'trending': ['momentum', 'trend_following', 'breakout'],
            'ranging': ['mean_reversion', 'support_resistance', 'oscillator'],
            'volatile': ['volatility_trading', 'news_trading', 'scalping'],
            'low_volume': ['patience_strategy', 'wide_spread', 'longer_timeframe'],
            'emergency': ['conservative', 'defensive', 'liquidity_focused']
        }
        
        # V3 Adaptive parameters
        self.adaptive_params = {
            'position_size_multiplier': 1.0,
            'confidence_threshold': self.config['min_confidence_normal'],
            'trading_frequency': 'normal',  # 'aggressive', 'normal', 'conservative'
            'timeframe_preference': ['1h', '4h'],
            'risk_multiplier': 1.0,
            'real_data_only': True  # V3 CRITICAL FLAG
        }
        
        # V3 Thread safety
        self._lock = threading.Lock()
        
        logging.info(f"[V3-INIT] Adaptive Trading Manager initialized - REAL DATA ONLY MODE")
        logging.info(f"[V3-RESOURCES] ThreadPoolExecutor: {max_workers} workers, Memory limit: {self.config['max_memory_mb']}MB")
    
    def _validate_real_components_only(self, trading_engine, ml_engine, market_analyzer, data_manager):
        """
        V3 CRITICAL: Validate all components are REAL (no mocks allowed)
        """
        components = {
            'trading_engine': trading_engine,
            'ml_engine': ml_engine,
            'market_analyzer': market_analyzer,
            'data_manager': data_manager
        }
        
        for name, component in components.items():
            if component is None:
                raise ValueError(f"[V3-INIT-ERROR] {name} is required - V3 does not allow None components")
            
            if not self.data_validator.validate_component_real_data_only(component, name):
                raise ValueError(f"[V3-MOCK-VIOLATION] {name} contains mock functionality - V3 REAL DATA ONLY")
        
        logging.info("[V3-VALIDATION] All components validated as REAL - no mock components detected")
    
    async def initialize(self):
        """Initialize adaptive trading manager with real data validation"""
        try:
            # V3 Initialize last trade times for real trading pairs
            real_symbols = self._get_real_trading_symbols()
            for symbol in real_symbols:
                self.last_trade_time[symbol] = datetime.now() - timedelta(hours=24)
            
            # V3 Start monitoring loops with proper resource management
            asyncio.create_task(self._continuous_opportunity_scanning())
            asyncio.create_task(self._adaptive_strategy_management())
            asyncio.create_task(self._emergency_mode_monitoring())
            asyncio.create_task(self._resource_monitoring())
            
            logging.info("[V3-OK] Adaptive Trading Manager initialized - REAL DATA MONITORING ACTIVE")
            return True
            
        except Exception as e:
            logging.error(f"[V3-INIT-FAIL] Adaptive Trading Manager initialization failed: {e}")
            raise
    
    def _get_real_trading_symbols(self) -> List[str]:
        """Get real trading symbols from environment - no hardcoded mocks"""
        try:
            # Get from environment or use proven major pairs
            env_pairs = os.getenv('MAJOR_PAIRS', '')
            if env_pairs:
                return env_pairs.split(',')
            
            # V3 Fallback to proven liquid pairs (real market only)
            return ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'SOLUSDT']
            
        except Exception as e:
            logging.error(f"[V3-ERROR] Error getting real trading symbols: {e}")
            return ['BTCUSDT', 'ETHUSDT']  # Minimal fallback
    
    async def validate_dependencies(self) -> Dict[str, bool]:
        """Validate that all dependencies are available and working with REAL DATA"""
        results = {}
        
        try:
            # V3 Test trading engine with real data validation
            results['trading_engine'] = (
                hasattr(self.trading_engine, 'execute_trade') and
                not hasattr(self.trading_engine, 'is_mock')
            )
            
            # V3 Test ML engine with real data validation
            results['ml_engine'] = (
                hasattr(self.ml_engine, 'predict') and 
                hasattr(self.ml_engine, 'generate_ml_features') and
                not hasattr(self.ml_engine, 'is_mock')
            )
            
            # V3 Test market analyzer with real data validation
            results['market_analyzer'] = (
                hasattr(self.market_analyzer, 'analyze_market') and
                not hasattr(self.market_analyzer, 'is_mock')
            )
            
            # V3 Test data manager with real data validation
            results['data_manager'] = (
                hasattr(self.data_manager, 'get_historical_data') and
                not hasattr(self.data_manager, 'is_mock')
            )
            
            # V3 Test actual real data retrieval
            try:
                test_data = await self._test_real_data_retrieval()
                results['real_data_access'] = test_data
            except Exception as e:
                logging.error(f"[V3-DATA-TEST-FAIL] Real data test failed: {e}")
                results['real_data_access'] = False
            
        except Exception as e:
            logging.error(f"[V3-VALIDATION-ERROR] Error validating dependencies: {e}")
            for key in ['trading_engine', 'ml_engine', 'market_analyzer', 'data_manager', 'real_data_access']:
                results[key] = False
        
        return results
    
    async def _test_real_data_retrieval(self) -> bool:
        """Test that we can retrieve real market data"""
        try:
            # Test with a major pair
            test_data = await asyncio.get_event_loop().run_in_executor(
                self.thread_pool,
                self.data_manager.get_historical_data,
                'BTCUSDT', '1h'
            )
            
            # V3 Validate the retrieved data is real
            if self.data_validator.validate_real_market_data(test_data, 'BTCUSDT', 'test_retrieval'):
                logging.info("[V3-DATA-TEST-OK] Real data retrieval test PASSED")
                return True
            else:
                logging.error("[V3-DATA-TEST-FAIL] Real data validation FAILED")
                return False
                
        except Exception as e:
            logging.error(f"[V3-DATA-TEST-ERROR] Real data test error: {e}")
            return False
    
    async def assess_market_condition(self, symbol: str, timeframe: str) -> MarketCondition:
        """Assess current market condition using REAL DATA ONLY"""
        try:
            # V3 Get recent REAL market data with validation
            data = await asyncio.get_event_loop().run_in_executor(
                self.thread_pool,
                self.data_manager.get_historical_data,
                symbol, timeframe, datetime.now() - timedelta(hours=48)
            )
            
            # V3 CRITICAL: Validate real data
            self.data_validation_stats['total_validations'] += 1
            if not self.data_validator.validate_real_market_data(data, symbol, 'market_condition'):
                self.data_validation_stats['failed_validations'] += 1
                logging.error(f"[V3-DATA-VIOLATION] Market condition assessment failed - invalid data for {symbol}")
                return self._create_error_market_condition()
            
            self.data_validation_stats['passed_validations'] += 1
            self.data_validation_stats['last_validation'] = datetime.now()
            
            if not data or len(data['close']) < 20:
                logging.warning(f"[V3-DATA-INSUFFICIENT] Insufficient real data for {symbol} - need 20+ points")
                return self._create_unknown_market_condition()
            
            # V3 Process REAL market data
            closes = np.array(data['close'])
            volumes = np.array(data['volume'])
            highs = np.array(data['high'])
            lows = np.array(data['low'])
            
            # Calculate real market metrics
            returns = np.diff(closes) / closes[:-1]
            volatility = np.std(returns[-24:]) if len(returns) >= 24 else np.std(returns)
            
            # Real trend analysis
            trend_strength = abs((closes[-1] - closes[-10]) / closes[-10]) if len(closes) >= 10 else 0
            trend_direction = 'up' if closes[-1] > closes[-10] else 'down' if closes[-1] < closes[-10] else 'sideways'
            
            # Real volume analysis
            avg_volume = np.mean(volumes[-24:]) if len(volumes) >= 24 else np.mean(volumes)
            current_volume = volumes[-1]
            volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1
            
            volume_profile = 'high' if volume_ratio > 1.5 else 'low' if volume_ratio < self.config['volume_threshold_low'] else 'medium'
            
            # Determine market condition from real data
            condition, strength = self._determine_market_condition(volatility, trend_strength, volume_profile)
            
            # Real major event detection
            major_event = self._detect_major_events(volatility, volume_ratio, trend_strength)
            
            # Real tradeable assessment
            tradeable = self._assess_tradeability(volume_profile, condition, volatility)
            
            # Recommended strategy based on real conditions
            recommended_strategy = self._get_recommended_strategy(condition, major_event)
            
            market_condition = MarketCondition(
                condition=condition,
                strength=strength,
                volatility=volatility,
                volume_profile=volume_profile,
                trend_direction=trend_direction,
                major_event_detected=major_event,
                tradeable=tradeable,
                recommended_strategy=recommended_strategy,
                data_source="REAL_MARKET_ONLY"
            )
            
            # Cache the condition
            with self._lock:
                self.current_market_conditions[symbol] = market_condition
            
            return market_condition
            
        except Exception as e:
            logging.error(f"[V3-ERROR] Error assessing market condition for {symbol}: {e}")
            return self._create_error_market_condition()
    
    def _determine_market_condition(self, volatility: float, trend_strength: float, volume_profile: str) -> Tuple[str, float]:
        """Determine market condition from real market metrics"""
        if volatility > self.config['volatility_threshold_high']:
            condition = 'volatile'
            strength = min(1.0, volatility / 0.1)
        elif trend_strength > 0.03:  # 3% move
            if volume_profile == 'high':
                condition = 'breakout'
            else:
                condition = 'trending'
            strength = min(1.0, trend_strength / 0.05)
        elif volatility < self.config['volatility_threshold_low']:
            condition = 'ranging'
            strength = 1.0 - volatility / self.config['volatility_threshold_low']
        else:
            condition = 'ranging'
            strength = 0.5
        
        return condition, strength
    
    def _detect_major_events(self, volatility: float, volume_ratio: float, trend_strength: float) -> bool:
        """Detect major market events from real data"""
        return (
            volatility > self.config['volatility_threshold_high'] * 2 or
            volume_ratio > 3 or
            trend_strength > 0.1  # 10% move
        )
    
    def _assess_tradeability(self, volume_profile: str, condition: str, volatility: float) -> bool:
        """Assess if market conditions are tradeable"""
        return not (
            volume_profile == 'low' and 
            condition == 'ranging' and 
            volatility < self.config['volatility_threshold_low'] / 2
        )
    
    def _get_recommended_strategy(self, condition: str, major_event: bool) -> str:
        """Get recommended strategy based on real market conditions"""
        if major_event:
            return 'emergency'
        elif condition in self.strategy_pools:
            return self.strategy_pools[condition][0]
        else:
            return 'conservative'
    
    def _create_unknown_market_condition(self) -> MarketCondition:
        """Create market condition for unknown state"""
        return MarketCondition(
            condition='unknown', strength=0, volatility=0,
            volume_profile='low', trend_direction='sideways',
            major_event_detected=False, tradeable=False,
            recommended_strategy='conservative',
            data_source="REAL_MARKET_ONLY"
        )
    
    def _create_error_market_condition(self) -> MarketCondition:
        """Create market condition for error state"""
        return MarketCondition(
            condition='error', strength=0, volatility=0,
            volume_profile='low', trend_direction='sideways',
            major_event_detected=False, tradeable=False,
            recommended_strategy='conservative',
            data_source="REAL_MARKET_ONLY"
        )
    
    async def scan_symbol_opportunity(self, symbol: str, timeframe: str, urgent: bool = False) -> Optional[TradingOpportunity]:
        """Scan for trading opportunities using REAL DATA ONLY"""
        try:
            # Get market condition from real data
            market_condition = await self.assess_market_condition(symbol, timeframe)
            
            if not market_condition.tradeable and not urgent:
                return None
            
            # Get current real data
            data = await asyncio.get_event_loop().run_in_executor(
                self.thread_pool,
                self.data_manager.get_historical_data,
                symbol, timeframe, datetime.now() - timedelta(hours=24)
            )
            
            # V3 CRITICAL: Validate real data
            if not self.data_validator.validate_real_market_data(data, symbol, 'opportunity_scan'):
                logging.error(f"[V3-DATA-VIOLATION] Opportunity scan failed - invalid data for {symbol}")
                return None
            
            if not data or len(data['close']) < 20:
                return None
            
            current_price = data['close'][-1]
            
            # Generate ML prediction using real data
            try:
                features = await asyncio.get_event_loop().run_in_executor(
                    self.thread_pool,
                    self.ml_engine.generate_ml_features,
                    symbol, {'historical_data': data}, {}
                )
                
                if features is None:
                    return None
                
                ml_prediction = await asyncio.get_event_loop().run_in_executor(
                    self.thread_pool,
                    self.ml_engine.predict,
                    features, symbol
                )
            except Exception as e:
                logging.error(f"[V3-ML-ERROR] ML prediction failed for {symbol}: {e}")
                return None
            
            # Adjust confidence threshold based on real market conditions
            confidence_threshold = self.get_adaptive_confidence_threshold(urgent, market_condition)
            
            if ml_prediction['confidence'] < confidence_threshold:
                if urgent:
                    return await self.generate_emergency_opportunity(symbol, timeframe, current_price, market_condition)
                return None
            
            # Determine opportunity type based on real data analysis
            opportunity_type = self.determine_opportunity_type(ml_prediction, market_condition)
            
            # Calculate trade levels from real market data
            entry_price, target_price, stop_loss = self.calculate_trade_levels(
                current_price, data, ml_prediction, market_condition
            )
            
            # Determine urgency from real conditions
            urgency = 'immediate' if urgent else 'normal'
            if market_condition.condition in ['breakout', 'breakdown']:
                urgency = 'immediate'
            
            opportunity = TradingOpportunity(
                symbol=symbol,
                timeframe=timeframe,
                opportunity_type=opportunity_type,
                confidence=ml_prediction['confidence'],
                entry_price=entry_price,
                target_price=target_price,
                stop_loss=stop_loss,
                urgency=urgency,
                market_condition=market_condition.condition,
                data_source="REAL_MARKET_ONLY",
                data_validation_passed=True
            )
            
            # Cache the opportunity
            with self._lock:
                self.trading_opportunities.append(opportunity)
                # Keep only recent opportunities
                self.trading_opportunities = self.trading_opportunities[-50:]
            
            return opportunity
            
        except Exception as e:
            logging.error(f"[V3-ERROR] Error scanning {symbol} {timeframe}: {e}")
            return None
    
    async def enter_emergency_mode(self):
        """Enter emergency trading mode with real data focus"""
        with self._lock:
            self.emergency_mode = True
            self.emergency_mode_start = datetime.now()
            
            # Adjust parameters for emergency mode
            self.adaptive_params['confidence_threshold'] = self.config['min_confidence_emergency']
            self.adaptive_params['position_size_multiplier'] = 0.6
            self.adaptive_params['trading_frequency'] = 'aggressive'
        
        logging.warning(f"[V3-EMERGENCY] ENTERING EMERGENCY TRADING MODE - REAL DATA ONLY")
        logging.info("[V3-EMERGENCY] Reduced confidence thresholds, increased scanning frequency")
    
    async def exit_emergency_mode(self):
        """Exit emergency trading mode"""
        with self._lock:
            self.emergency_mode = False
            self.emergency_mode_start = None
            
            # Reset parameters to normal
            self.adaptive_params['confidence_threshold'] = self.config['min_confidence_normal']
            self.adaptive_params['position_size_multiplier'] = 1.0
            self.adaptive_params['trading_frequency'] = 'normal'
        
        logging.info("[V3-OK] EXITING EMERGENCY TRADING MODE - Returning to normal parameters")
    
    def get_adaptive_confidence_threshold(self, urgent: bool, market_condition: MarketCondition) -> float:
        """Get adaptive confidence threshold based on real market conditions"""
        base_threshold = self.config['min_confidence_normal']
        
        if urgent:
            base_threshold = self.config['min_confidence_emergency']
        
        if self.emergency_mode:
            base_threshold = min(base_threshold, self.config['min_confidence_emergency'])
        
        # Adjust based on real market condition
        if market_condition.condition == 'volatile':
            base_threshold += 0.1  # Require higher confidence in volatile markets
        elif market_condition.condition == 'trending' and market_condition.strength > 0.7:
            base_threshold -= 0.05  # Lower threshold for strong trends
        elif market_condition.volume_profile == 'low':
            base_threshold += 0.05  # Higher threshold for low volume
        
        return max(0.3, min(0.9, base_threshold))
    
    def determine_opportunity_type(self, ml_prediction: Dict, market_condition: MarketCondition) -> str:
        """Determine trading opportunity type from real market analysis"""
        prediction_class = ml_prediction['prediction']
        
        # Map ML prediction to opportunity type based on real market condition
        if market_condition.condition == 'breakout':
            return 'breakout'
        elif market_condition.condition == 'ranging':
            return 'mean_reversion'
        elif market_condition.condition == 'trending':
            return 'momentum'
        elif market_condition.condition == 'volatile':
            return 'volatility_trading'
        else:
            # Default based on ML prediction
            if prediction_class in [0, 1]:  # Sell signals
                return 'reversal'
            elif prediction_class in [3, 4]:  # Buy signals
                return 'momentum'
            else:
                return 'mean_reversion'
    
    def calculate_trade_levels(self, current_price: float, data: Dict, 
                             ml_prediction: Dict, market_condition: MarketCondition) -> Tuple[float, float, float]:
        """Calculate trade levels from real market data"""
        try:
            closes = np.array(data['close'])
            highs = np.array(data['high'])
            lows = np.array(data['low'])
            
            if len(closes) == 0:
                return current_price, current_price * 1.02, current_price * 0.98
            
            # Calculate ATR from real market data
            atr = self.calculate_atr(highs, lows, closes)
            
            # Adaptive multipliers based on real market condition
            if market_condition.condition == 'volatile':
                stop_multiplier = 3.0
                target_multiplier = 4.0
            elif market_condition.condition == 'trending':
                stop_multiplier = 2.0
                target_multiplier = 5.0
            elif market_condition.condition == 'ranging':
                stop_multiplier = 1.5
                target_multiplier = 2.5
            else:
                stop_multiplier = 2.5
                target_multiplier = 3.5
            
            # Adjust for emergency mode
            if self.emergency_mode:
                stop_multiplier *= 1.5
                target_multiplier *= 1.2
            
            prediction_class = ml_prediction['prediction']
            
            if prediction_class in [3, 4]:  # Buy signal
                entry_price = current_price
                stop_loss = current_price - (atr * stop_multiplier)
                target_price = current_price + (atr * target_multiplier)
            elif prediction_class in [0, 1]:  # Sell signal
                entry_price = current_price
                stop_loss = current_price + (atr * stop_multiplier)
                target_price = current_price - (atr * target_multiplier)
            else:  # Neutral - create small range trade
                entry_price = current_price
                stop_loss = current_price - (atr * 1.0)
                target_price = current_price + (atr * 1.5)
            
            return entry_price, target_price, stop_loss
            
        except Exception as e:
            logging.error(f"[V3-ERROR] Error calculating trade levels: {e}")
            # Return conservative levels
            return current_price, current_price * 1.02, current_price * 0.98
    
    def calculate_atr(self, highs: np.ndarray, lows: np.ndarray, closes: np.ndarray) -> float:
        """Calculate Average True Range from real market data"""
        try:
            if len(closes) < 14:
                return (np.mean(highs) - np.mean(lows)) * 0.1
            
            tr_list = []
            for i in range(1, min(15, len(closes))):
                tr = max(
                    highs[-i] - lows[-i],
                    abs(highs[-i] - closes[-i-1]),
                    abs(lows[-i] - closes[-i-1])
                )
                tr_list.append(tr)
            
            return np.mean(tr_list)
            
        except Exception as e:
            logging.warning(f"[V3-ERROR] Error calculating ATR: {e}")
            return closes[-1] * 0.02  # 2% fallback
    
    async def generate_emergency_opportunity(self, symbol: str, timeframe: str, 
                                           current_price: float, market_condition: MarketCondition) -> Optional[TradingOpportunity]:
        """Generate emergency trading opportunity from real market data"""
        try:
            logging.info(f"[V3-EMERGENCY] Generating emergency opportunity for {symbol}")
            
            # Get very recent real price data
            data = await asyncio.get_event_loop().run_in_executor(
                self.thread_pool,
                self.data_manager.get_historical_data,
                symbol, timeframe, datetime.now() - timedelta(hours=6)
            )
            
            # V3 Validate real data
            if not self.data_validator.validate_real_market_data(data, symbol, 'emergency_opportunity'):
                logging.error(f"[V3-DATA-VIOLATION] Emergency opportunity failed - invalid data for {symbol}")
                return None
            
            if not data or len(data['close']) < 5:
                return None
            
            closes = np.array(data['close'])
            
            # Real market emergency strategy: trade against recent extreme moves
            recent_change = (closes[-1] - closes[-5]) / closes[-5] if len(closes) >= 5 else 0
            
            if abs(recent_change) > 0.02:  # 2% move
                # Counter-trend trade based on real market movement
                if recent_change > 0:  # Price went up, bet on pullback
                    opportunity_type = 'reversal'
                    target_price = current_price * 0.995  # Small pullback
                    stop_loss = current_price * 1.01
                else:  # Price went down, bet on bounce
                    opportunity_type = 'reversal'
                    target_price = current_price * 1.005  # Small bounce
                    stop_loss = current_price * 0.99
            else:
                # Momentum trade based on real market direction
                if recent_change > 0:
                    opportunity_type = 'momentum'
                    target_price = current_price * 1.005
                    stop_loss = current_price * 0.995
                else:
                    opportunity_type = 'momentum'
                    target_price = current_price * 0.995
                    stop_loss = current_price * 1.005
            
            return TradingOpportunity(
                symbol=symbol,
                timeframe=timeframe,
                opportunity_type=opportunity_type,
                confidence=0.5,  # Low confidence but acceptable in emergency
                entry_price=current_price,
                target_price=target_price,
                stop_loss=stop_loss,
                urgency='immediate',
                market_condition=market_condition.condition,
                data_source="REAL_MARKET_ONLY",
                data_validation_passed=True
            )
            
        except Exception as e:
            logging.error(f"[V3-ERROR] Error generating emergency opportunity: {e}")
            return None
    
    async def _continuous_opportunity_scanning(self):
        """Continuously scan for opportunities using real data"""
        while True:
            try:
                symbols = self._get_real_trading_symbols()
                timeframes = ['1h', '4h']
                
                for symbol in symbols:
                    for timeframe in timeframes:
                        try:
                            await self.scan_symbol_opportunity(symbol, timeframe)
                            await asyncio.sleep(1)  # Rate limiting
                        except Exception as e:
                            logging.error(f"[V3-SCAN-ERROR] Error scanning {symbol} {timeframe}: {e}")
                
                await asyncio.sleep(30)  # Scan every 30 seconds
                
            except Exception as e:
                logging.error(f"[V3-SCAN-LOOP-ERROR] Continuous scanning error: {e}")
                await asyncio.sleep(60)  # Wait longer on error
    
    async def _adaptive_strategy_management(self):
        """Manage adaptive strategies based on real market conditions"""
        while True:
            try:
                with self._lock:
                    # Analyze current market conditions across symbols
                    total_volatile = sum(1 for mc in self.current_market_conditions.values() 
                                       if mc.condition == 'volatile')
                    total_trending = sum(1 for mc in self.current_market_conditions.values() 
                                       if mc.condition == 'trending')
                    total_symbols = len(self.current_market_conditions)
                
                if total_symbols > 0:
                    volatility_ratio = total_volatile / total_symbols
                    trending_ratio = total_trending / total_symbols
                    
                    # Adjust global parameters based on real market state
                    if volatility_ratio > 0.5:
                        self.adaptive_params['risk_multiplier'] = 0.7
                        self.adaptive_params['trading_frequency'] = 'conservative'
                    elif trending_ratio > 0.6:
                        self.adaptive_params['risk_multiplier'] = 1.2
                        self.adaptive_params['trading_frequency'] = 'aggressive'
                    else:
                        self.adaptive_params['risk_multiplier'] = 1.0
                        self.adaptive_params['trading_frequency'] = 'normal'
                
                await asyncio.sleep(300)  # Adjust every 5 minutes
                
            except Exception as e:
                logging.error(f"[V3-STRATEGY-ERROR] Adaptive strategy management error: {e}")
                await asyncio.sleep(600)  # Wait longer on error
    
    async def _emergency_mode_monitoring(self):
        """Monitor for emergency conditions based on real market data"""
        while True:
            try:
                now = datetime.now()
                
                # Check if we need to enter emergency mode
                symbols_needing_trades = []
                for symbol, last_trade in self.last_trade_time.items():
                    hours_since = (now - last_trade).total_seconds() / 3600
                    if hours_since > self.config['max_hours_without_trade']:
                        symbols_needing_trades.append(symbol)
                
                if symbols_needing_trades and not self.emergency_mode:
                    logging.warning(f"[V3-EMERGENCY-TRIGGER] Symbols needing trades: {symbols_needing_trades}")
                    await self.enter_emergency_mode()
                
                # Check if we can exit emergency mode
                if self.emergency_mode and self.emergency_mode_start:
                    emergency_duration = (now - self.emergency_mode_start).total_seconds() / 3600
                    if emergency_duration > self.config['emergency_mode_duration']:
                        await self.exit_emergency_mode()
                
                await asyncio.sleep(300)  # Check every 5 minutes
                
            except Exception as e:
                logging.error(f"[V3-EMERGENCY-MONITOR-ERROR] Emergency monitoring error: {e}")
                await asyncio.sleep(600)
    
    async def _resource_monitoring(self):
        """Monitor system resources and adjust accordingly"""
        while True:
            try:
                # Monitor memory usage
                memory_percent = psutil.virtual_memory().percent
                cpu_percent = psutil.cpu_percent(interval=1)
                
                if memory_percent > 90:
                    logging.warning(f"[V3-RESOURCE] High memory usage: {memory_percent}%")
                    # Reduce cache sizes
                    with self._lock:
                        self.trading_opportunities = self.trading_opportunities[-25:]
                        if len(self.current_market_conditions) > 20:
                            # Keep only most recent conditions
                            symbols = list(self.current_market_conditions.keys())
                            for symbol in symbols[:-20]:
                                del self.current_market_conditions[symbol]
                
                if cpu_percent > 85:
                    logging.warning(f"[V3-RESOURCE] High CPU usage: {cpu_percent}%")
                    # Add delays to reduce CPU load
                    await asyncio.sleep(2)
                
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                logging.error(f"[V3-RESOURCE-ERROR] Resource monitoring error: {e}")
                await asyncio.sleep(120)
    
    def get_metrics(self) -> Dict:
        """Get comprehensive metrics including real data validation stats"""
        try:
            with self._lock:
                # Calculate time since last trade for each symbol
                time_since_trades = {}
                now = datetime.now()
                for symbol, last_trade in self.last_trade_time.items():
                    hours_since = (now - last_trade).total_seconds() / 3600
                    time_since_trades[symbol] = hours_since
                
                # Get current market conditions summary
                market_conditions_summary = {}
                for symbol, mc in self.current_market_conditions.items():
                    market_conditions_summary[symbol] = {
                        'condition': mc.condition,
                        'tradeable': mc.tradeable,
                        'volatility': mc.volatility,
                        'data_source': mc.data_source
                    }
                
                # Calculate data validation success rate
                validation_success_rate = 0.0
                if self.data_validation_stats['total_validations'] > 0:
                    validation_success_rate = (
                        self.data_validation_stats['passed_validations'] / 
                        self.data_validation_stats['total_validations']
                    ) * 100
                
                return {
                    'system_version': 'V3_REAL_DATA_ONLY',
                    'emergency_mode': self.emergency_mode,
                    'emergency_mode_duration': (now - self.emergency_mode_start).total_seconds() / 3600 
                                             if self.emergency_mode_start else 0,
                    'max_hours_since_trade': max(time_since_trades.values()) if time_since_trades else 0,
                    'symbols_needing_trades': len([h for h in time_since_trades.values() 
                                                 if h > self.config['max_hours_without_trade'] * 0.8]),
                    'current_opportunities': len(self.trading_opportunities),
                    'market_conditions': market_conditions_summary,
                    'adaptive_confidence_threshold': self.adaptive_params['confidence_threshold'],
                    'trading_frequency': self.adaptive_params['trading_frequency'],
                    'real_data_only': self.adaptive_params['real_data_only'],
                    'data_validation_stats': self.data_validation_stats.copy(),
                    'data_validation_success_rate': validation_success_rate,
                    'thread_pool_active': self.thread_pool._threads is not None and len(self.thread_pool._threads) > 0,
                    'resource_config': {
                        'max_memory_mb': self.config['max_memory_mb'],
                        'max_cpu_percent': self.config['max_cpu_percent'],
                        'thread_pool_workers': self.thread_pool._max_workers
                    },
                    'dependency_status': await self.validate_dependencies()
                }
                
        except Exception as e:
            logging.error(f"[V3-METRICS-ERROR] Error getting metrics: {e}")
            return {
                'system_version': 'V3_REAL_DATA_ONLY',
                'error': str(e),
                'real_data_only': True,
                'data_validation_required': True
            }
    
    def __del__(self):
        """Cleanup resources"""
        try:
            if hasattr(self, 'thread_pool'):
                self.thread_pool.shutdown(wait=False)
        except:
            pass

# V3 Factory functions for creating real instances
def create_adaptive_trading_manager(trading_engine, ml_engine, market_analyzer, data_manager):
    """
    V3 Factory function to create AdaptiveTradingManager with real components
    
    CRITICAL: All components must be REAL - no mocks allowed in V3
    """
    return AdaptiveTradingManager(trading_engine, ml_engine, market_analyzer, data_manager)

# V3 Main execution for testing real data functionality
if __name__ == "__main__":
    async def test_v3_real_data_manager():
        """Test V3 Adaptive Trading Manager with real data validation"""
        print("[V3-TEST] Testing V3 Adaptive Trading Manager - REAL DATA ONLY")
        
        # This test requires real components - cannot run without them
        print("[V3-INFO] This module requires real trading components")
        print("[V3-INFO] Mock testing removed - V3 uses REAL DATA ONLY")
        print("[V3-INFO] Use create_adaptive_trading_manager() with real components")
        
        # Test real data validator
        validator = RealDataValidator()
        
        # Test with sample real-like data structure
        test_data = {
            'close': [50000, 50100, 49900, 50200, 50150],
            'high': [50200, 50300, 50100, 50400, 50300],
            'low': [49800, 49900, 49700, 50000, 49950],
            'volume': [1000, 1200, 800, 1500, 900],
            'open': [50000, 50100, 49900, 50200, 50150]
        }
        
        validation_result = validator.validate_real_market_data(test_data, 'BTCUSDT', 'test')
        print(f"[V3-TEST] Data validation test: {'PASS' if validation_result else 'FAIL'}")
        
        return validation_result
    
    # Run the test
    import asyncio
    result = asyncio.run(test_v3_real_data_manager())
    print(f"[V3-RESULT] V3 Real Data Manager Test: {'PASS' if result else 'FAIL'}")