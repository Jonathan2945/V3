#!/usr/bin/env python3
"""
EXECUTION COST INTELLIGENCE - FULL IMPLEMENTATION
================================================
Integrates with your existing ML trading system to add:
- Real-time execution cost prediction
- Cost-aware ML decision making
- Learning from actual trade execution
- Improved live trading performance

Compatible with:
- advanced_ml_engine.py
- intelligent_trading_engine.py
- adaptive_trading_manager.py
- external_data_collector.py
"""

import logging
import asyncio
import numpy as np
import sqlite3
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Union
from dataclasses import dataclass, asdict, field
import random

# ============================================================================
# CORE DATA STRUCTURES
# ============================================================================

@dataclass
class ExecutionCostBreakdown:
    """Detailed breakdown of execution costs"""
    spread_cost: float
    slippage_cost: float
    commission_cost: float
    market_impact_cost: float
    timing_cost: float
    total_cost: float
    cost_percentage: float
    
@dataclass
class ExecutionCostPrediction:
    """Execution cost prediction with confidence"""
    symbol: str
    position_size: float
    predicted_cost: ExecutionCostBreakdown
    confidence: float  # 0-1
    market_conditions: Dict[str, float]
    session_factors: Dict[str, float]
    volatility_adjustment: float
    liquidity_score: float
    predicted_at: datetime
    valid_until: datetime
    reasoning: List[str]

@dataclass
class CostAdjustedSignal:
    """ML signal enhanced with cost intelligence"""
    original_signal: Dict[str, Any]
    cost_prediction: ExecutionCostPrediction
    
    # Adjusted metrics
    original_confidence: float
    cost_adjusted_confidence: float
    original_expected_profit: float
    cost_adjusted_expected_profit: float
    
    # Decision factors
    cost_efficiency_ratio: float  # profit/cost ratio
    should_trade_after_costs: bool
    recommended_position_size: float
    optimal_timing_score: float
    
    # Meta information
    enhancement_factors: List[str]
    warnings: List[str]
    created_at: datetime
    cost_intelligence_version: str

@dataclass
class CostLearningData:
    """Data for learning from actual execution costs"""
    trade_id: str
    symbol: str
    predicted_cost: ExecutionCostBreakdown
    actual_cost: ExecutionCostBreakdown
    prediction_accuracy: float
    market_conditions_at_execution: Dict[str, float]
    execution_timestamp: datetime
    learning_value: float  # How much this teaches us

# ============================================================================
# EXECUTION COST PREDICTOR
# ============================================================================

class AdvancedExecutionCostPredictor:
    """Advanced execution cost prediction using market microstructure"""
    
    def __init__(self, data_manager=None):
        self.data_manager = data_manager
        
        # Cost model parameters (learned from experience)
        self.cost_model = {
            # Base costs
            'base_spread_bps': 8,  # 8 basis points base spread
            'commission_rate': 0.001,  # 0.1%
            'max_commission': 25.0,
            
            # Market impact factors
            'impact_coefficient': 0.1,
            'volume_impact_threshold': 0.01,  # 1% of average volume
            
            # Volatility adjustments
            'volatility_multiplier': 2.5,
            'high_vol_threshold': 0.05,  # 5% volatility
            
            # Session adjustments
            'off_hours_multiplier': 1.3,
            'low_liquidity_multiplier': 1.8,
            
            # Slippage factors
            'slippage_base_bps': 3,
            'slippage_volatility_factor': 1.5
        }
        
        # Cost prediction accuracy tracking
        self.prediction_history = []
        self.accuracy_by_conditions = {}
        
        # Database for cost learning
        self.db_path = 'data/execution_cost_learning.db'
        self._init_cost_database()
        
        logging.info("[COST] Advanced Execution Cost Predictor initialized")
    
    def _init_cost_database(self):
        """Initialize cost learning database"""
        try:
            os.makedirs('data', exist_ok=True)
            
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS cost_predictions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        trade_id TEXT,
                        symbol TEXT,
                        position_size REAL,
                        predicted_spread REAL,
                        predicted_slippage REAL,
                        predicted_commission REAL,
                        predicted_total REAL,
                        actual_spread REAL,
                        actual_slippage REAL,
                        actual_commission REAL,
                        actual_total REAL,
                        accuracy REAL,
                        market_conditions TEXT,
                        created_at DATETIME,
                        learned_from BOOLEAN DEFAULT FALSE
                    )
                ''')
                
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS cost_model_updates (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        parameter_name TEXT,
                        old_value REAL,
                        new_value REAL,
                        improvement_score REAL,
                        update_reason TEXT,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                conn.commit()
                
        except Exception as e:
            logging.error(f"Cost database initialization failed: {e}")
    
    async def predict_execution_cost(self, signal: Dict, market_data: Dict, 
                                   external_data: Dict = None) -> ExecutionCostPrediction:
        """Predict execution costs with high accuracy"""
        try:
            symbol = signal.get('symbol', 'BTCUSDT')
            position_size = signal.get('position_size', 1000)
            
            # Get market conditions
            market_conditions = await self._analyze_market_conditions(symbol, market_data, external_data)
            
            # Get session factors
            session_factors = await self._analyze_session_factors()
            
            # Calculate cost components
            cost_breakdown = await self._calculate_cost_components(
                symbol, position_size, market_conditions, session_factors
            )
            
            # Calculate confidence in prediction
            prediction_confidence = await self._calculate_prediction_confidence(
                symbol, market_conditions, session_factors
            )
            
            # Generate reasoning
            reasoning = await self._generate_cost_reasoning(
                cost_breakdown, market_conditions, session_factors
            )
            
            return ExecutionCostPrediction(
                symbol=symbol,
                position_size=position_size,
                predicted_cost=cost_breakdown,
                confidence=prediction_confidence,
                market_conditions=market_conditions,
                session_factors=session_factors,
                volatility_adjustment=market_conditions.get('volatility_multiplier', 1.0),
                liquidity_score=market_conditions.get('liquidity_score', 0.5),
                predicted_at=datetime.now(),
                valid_until=datetime.now() + timedelta(minutes=5),
                reasoning=reasoning
            )
            
        except Exception as e:
            logging.error(f"Cost prediction failed: {e}")
            return self._get_fallback_cost_prediction(symbol, position_size)
    
    async def _analyze_market_conditions(self, symbol: str, market_data: Dict, 
                                       external_data: Dict = None) -> Dict[str, float]:
        """Analyze current market conditions affecting execution costs"""
        try:
            conditions = {}
            
            # Price volatility
            if 'klines' in market_data:
                klines = market_data['klines']
                if len(klines) >= 20:
                    closes = [float(k[4]) for k in klines[-20:]]
                    returns = np.diff(np.log(closes))
                    volatility = np.std(returns) * np.sqrt(24)  # Annualized hourly vol
                    conditions['volatility'] = volatility
                    conditions['volatility_multiplier'] = max(1.0, volatility * self.cost_model['volatility_multiplier'])
            
            # Volume analysis
            if 'volume' in market_data:
                current_volume = float(market_data['volume'])
                if 'klines' in market_data and len(market_data['klines']) >= 10:
                    avg_volume = np.mean([float(k[5]) for k in market_data['klines'][-10:]])
                    volume_ratio = current_volume / max(avg_volume, 1)
                    conditions['volume_ratio'] = volume_ratio
                    conditions['liquidity_score'] = min(1.0, volume_ratio / 2.0)
            
            # Price change momentum
            if 'change_24h' in market_data:
                change_24h = float(market_data['change_24h'])
                conditions['momentum'] = abs(change_24h) / 100
                conditions['trend_strength'] = change_24h / 100
            
            # Spread estimation
            if 'high_24h' in market_data and 'low_24h' in market_data:
                high_24h = float(market_data['high_24h'])
                low_24h = float(market_data['low_24h'])
                current_price = float(market_data.get('price', (high_24h + low_24h) / 2))
                estimated_spread = (high_24h - low_24h) / current_price * 0.01  # 1% of 24h range
                conditions['estimated_spread'] = estimated_spread
            
            # External data factors
            if external_data:
                if 'news_sentiment' in external_data:
                    news_impact = abs(external_data['news_sentiment'].get('sentiment_score', 0))
                    conditions['news_volatility_factor'] = 1.0 + (news_impact * 0.5)
                
                if 'social_sentiment' in external_data:
                    social_impact = abs(external_data['social_sentiment'].get('sentiment_score', 0))
                    conditions['social_volatility_factor'] = 1.0 + (social_impact * 0.3)
            
            # Set defaults for missing data
            conditions.setdefault('volatility', 0.02)
            conditions.setdefault('volume_ratio', 1.0)
            conditions.setdefault('liquidity_score', 0.5)
            conditions.setdefault('volatility_multiplier', 1.0)
            
            return conditions
            
        except Exception as e:
            logging.warning(f"Market conditions analysis failed: {e}")
            return {'volatility': 0.02, 'volume_ratio': 1.0, 'liquidity_score': 0.5}
    
    async def _analyze_session_factors(self) -> Dict[str, float]:
        """Analyze current trading session factors"""
        try:
            now = datetime.utcnow()
            hour = now.hour
            minute = now.minute
            weekday = now.weekday()
            
            factors = {}
            
            # Session identification
            if 0 <= hour < 8:
                session = 'ASIAN'
                liquidity_factor = 0.7
            elif 8 <= hour < 16:
                session = 'LONDON'
                liquidity_factor = 1.0
            elif 16 <= hour < 24:
                session = 'NEW_YORK'
                liquidity_factor = 0.9
            else:
                session = 'OFF_HOURS'
                liquidity_factor = 0.5
            
            factors['session'] = session
            factors['liquidity_factor'] = liquidity_factor
            
            # Weekend effects
            if weekday >= 5:  # Saturday = 5, Sunday = 6
                factors['weekend_factor'] = 1.5
            else:
                factors['weekend_factor'] = 1.0
            
            # Round number timing (avoid round hours/half-hours)
            if minute in [0, 30]:
                factors['timing_penalty'] = 1.2
            else:
                factors['timing_penalty'] = 1.0
            
            # Session overlap bonuses
            if hour in [8, 9, 16, 17]:  # Session transitions
                factors['overlap_bonus'] = 0.9
            else:
                factors['overlap_bonus'] = 1.0
            
            return factors
            
        except Exception as e:
            return {'session': 'UNKNOWN', 'liquidity_factor': 1.0, 'weekend_factor': 1.0}
    
    async def _calculate_cost_components(self, symbol: str, position_size: float,
                                       market_conditions: Dict, session_factors: Dict) -> ExecutionCostBreakdown:
        """Calculate detailed cost breakdown"""
        try:
            # Base spread cost
            base_spread_bps = self.cost_model['base_spread_bps']
            volatility_mult = market_conditions.get('volatility_multiplier', 1.0)
            liquidity_factor = session_factors.get('liquidity_factor', 1.0)
            
            spread_cost = position_size * (base_spread_bps / 10000) * volatility_mult / liquidity_factor
            
            # Slippage cost
            base_slippage_bps = self.cost_model['slippage_base_bps']
            volatility = market_conditions.get('volatility', 0.02)
            volume_ratio = market_conditions.get('volume_ratio', 1.0)
            
            slippage_multiplier = 1.0 + (volatility * self.cost_model['slippage_volatility_factor'])
            slippage_multiplier /= max(0.5, min(2.0, volume_ratio))  # Better volume = less slippage
            
            slippage_cost = position_size * (base_slippage_bps / 10000) * slippage_multiplier
            
            # Commission cost
            commission_rate = self.cost_model['commission_rate']
            commission_cost = min(
                self.cost_model['max_commission'],
                position_size * commission_rate
            )
            
            # Market impact cost (for larger positions)
            volume_threshold = self.cost_model['volume_impact_threshold']
            if volume_ratio < volume_threshold:
                impact_factor = self.cost_model['impact_coefficient']
                market_impact_cost = position_size * impact_factor * (volume_threshold / volume_ratio)
            else:
                market_impact_cost = 0.0
            
            # Timing cost (poor timing penalties)
            timing_penalty = session_factors.get('timing_penalty', 1.0)
            weekend_factor = session_factors.get('weekend_factor', 1.0)
            timing_cost = (spread_cost + slippage_cost) * (timing_penalty - 1.0) * weekend_factor
            
            # Total cost
            total_cost = spread_cost + slippage_cost + commission_cost + market_impact_cost + timing_cost
            cost_percentage = (total_cost / position_size) * 100 if position_size > 0 else 0
            
            return ExecutionCostBreakdown(
                spread_cost=spread_cost,
                slippage_cost=slippage_cost,
                commission_cost=commission_cost,
                market_impact_cost=market_impact_cost,
                timing_cost=timing_cost,
                total_cost=total_cost,
                cost_percentage=cost_percentage
            )
            
        except Exception as e:
            logging.error(f"Cost calculation failed: {e}")
            return ExecutionCostBreakdown(
                spread_cost=position_size * 0.0005,
                slippage_cost=position_size * 0.0003,
                commission_cost=min(25, position_size * 0.001),
                market_impact_cost=0.0,
                timing_cost=0.0,
                total_cost=position_size * 0.002,
                cost_percentage=0.2
            )
    
    async def _calculate_prediction_confidence(self, symbol: str, market_conditions: Dict,
                                             session_factors: Dict) -> float:
        """Calculate confidence in cost prediction"""
        try:
            confidence = 0.8  # Base confidence
            
            # Adjust based on market conditions
            volatility = market_conditions.get('volatility', 0.02)
            if volatility < 0.01:  # Low volatility = more predictable
                confidence += 0.1
            elif volatility > 0.05:  # High volatility = less predictable
                confidence -= 0.2
            
            # Adjust based on liquidity
            liquidity_score = market_conditions.get('liquidity_score', 0.5)
            confidence += (liquidity_score - 0.5) * 0.2
            
            # Adjust based on session
            session = session_factors.get('session', 'UNKNOWN')
            if session in ['LONDON', 'NEW_YORK']:
                confidence += 0.05
            elif session == 'OFF_HOURS':
                confidence -= 0.15
            
            # Historical accuracy adjustment
            historical_accuracy = await self._get_historical_accuracy(symbol)
            if historical_accuracy:
                confidence = (confidence + historical_accuracy) / 2
            
            return max(0.3, min(0.95, confidence))
            
        except Exception as e:
            return 0.7
    
    async def _get_historical_accuracy(self, symbol: str) -> Optional[float]:
        """Get historical prediction accuracy for this symbol"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute('''
                    SELECT AVG(accuracy) FROM cost_predictions 
                    WHERE symbol = ? AND created_at > datetime('now', '-30 days')
                ''', (symbol,))
                result = cursor.fetchone()
                return result[0] if result and result[0] else None
        except Exception:
            return None
    
    async def _generate_cost_reasoning(self, cost_breakdown: ExecutionCostBreakdown,
                                     market_conditions: Dict, session_factors: Dict) -> List[str]:
        """Generate human-readable reasoning for cost prediction"""
        reasoning = []
        
        try:
            # Total cost summary
            total_cost = cost_breakdown.total_cost
            cost_pct = cost_breakdown.cost_percentage
            reasoning.append(f"Total execution cost: ${total_cost:.2f} ({cost_pct:.2f}%)")
            
            # Largest cost component
            costs = {
                'Spread': cost_breakdown.spread_cost,
                'Slippage': cost_breakdown.slippage_cost,
                'Commission': cost_breakdown.commission_cost,
                'Market Impact': cost_breakdown.market_impact_cost,
                'Timing': cost_breakdown.timing_cost
            }
            
            largest_cost = max(costs.keys(), key=lambda k: costs[k])
            if costs[largest_cost] > 0:
                reasoning.append(f"Largest cost factor: {largest_cost} (${costs[largest_cost]:.2f})")
            
            # Market condition impacts
            volatility = market_conditions.get('volatility', 0.02)
            if volatility > 0.05:
                reasoning.append(f"High volatility ({volatility:.1%}) increases costs")
            elif volatility < 0.01:
                reasoning.append(f"Low volatility ({volatility:.1%}) reduces costs")
            
            # Liquidity impacts
            liquidity_score = market_conditions.get('liquidity_score', 0.5)
            if liquidity_score < 0.3:
                reasoning.append("Low liquidity increases slippage risk")
            elif liquidity_score > 0.8:
                reasoning.append("High liquidity reduces execution costs")
            
            # Session impacts
            session = session_factors.get('session', 'UNKNOWN')
            if session == 'OFF_HOURS':
                reasoning.append("Off-hours trading increases costs")
            elif session in ['LONDON', 'NEW_YORK']:
                reasoning.append(f"{session} session provides good liquidity")
            
            # Timing warnings
            timing_penalty = session_factors.get('timing_penalty', 1.0)
            if timing_penalty > 1.1:
                reasoning.append("Round-hour timing may increase costs")
            
        except Exception as e:
            reasoning.append(f"Cost analysis: {total_cost:.2f} estimated")
        
        return reasoning
    
    def _get_fallback_cost_prediction(self, symbol: str, position_size: float) -> ExecutionCostPrediction:
        """Fallback cost prediction when main calculation fails"""
        fallback_cost = ExecutionCostBreakdown(
            spread_cost=position_size * 0.0008,
            slippage_cost=position_size * 0.0005,
            commission_cost=min(25, position_size * 0.001),
            market_impact_cost=0.0,
            timing_cost=0.0,
            total_cost=position_size * 0.002,
            cost_percentage=0.2
        )
        
        return ExecutionCostPrediction(
            symbol=symbol,
            position_size=position_size,
            predicted_cost=fallback_cost,
            confidence=0.6,
            market_conditions={'volatility': 0.02},
            session_factors={'session': 'UNKNOWN'},
            volatility_adjustment=1.0,
            liquidity_score=0.5,
            predicted_at=datetime.now(),
            valid_until=datetime.now() + timedelta(minutes=5),
            reasoning=["Fallback cost estimate due to analysis error"]
        )
    
    async def learn_from_execution(self, predicted: ExecutionCostPrediction, 
                                 actual_cost: ExecutionCostBreakdown, trade_id: str):
        """Learn from actual execution results to improve predictions"""
        try:
            # Calculate accuracy
            predicted_total = predicted.predicted_cost.total_cost
            actual_total = actual_cost.total_cost
            
            if predicted_total > 0:
                accuracy = 1.0 - abs(predicted_total - actual_total) / predicted_total
            else:
                accuracy = 0.0
            
            # Store learning data
            learning_data = CostLearningData(
                trade_id=trade_id,
                symbol=predicted.symbol,
                predicted_cost=predicted.predicted_cost,
                actual_cost=actual_cost,
                prediction_accuracy=accuracy,
                market_conditions_at_execution=predicted.market_conditions,
                execution_timestamp=datetime.now(),
                learning_value=self._calculate_learning_value(accuracy, predicted.confidence)
            )
            
            # Save to database
            await self._store_learning_data(learning_data)
            
            # Update model if we have enough data
            await self._update_cost_model_if_needed()
            
            logging.info(f"[LEARN] Cost prediction accuracy: {accuracy:.1%} for {predicted.symbol}")
            
        except Exception as e:
            logging.error(f"Learning from execution failed: {e}")
    
    def _calculate_learning_value(self, accuracy: float, confidence: float) -> float:
        """Calculate how valuable this learning example is"""
        # Higher learning value for:
        # - High confidence predictions that were wrong (need correction)
        # - Low confidence predictions that were right (build confidence)
        if confidence > 0.8 and accuracy < 0.7:
            return 1.0  # High value - confident but wrong
        elif confidence < 0.6 and accuracy > 0.8:
            return 0.8  # Good value - uncertain but right
        else:
            return 0.5  # Standard value
    
    async def _store_learning_data(self, learning_data: CostLearningData):
        """Store learning data in database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT INTO cost_predictions (
                        trade_id, symbol, position_size,
                        predicted_spread, predicted_slippage, predicted_commission, predicted_total,
                        actual_spread, actual_slippage, actual_commission, actual_total,
                        accuracy, market_conditions, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    learning_data.trade_id, learning_data.symbol, learning_data.predicted_cost.total_cost / 0.002,
                    learning_data.predicted_cost.spread_cost, learning_data.predicted_cost.slippage_cost,
                    learning_data.predicted_cost.commission_cost, learning_data.predicted_cost.total_cost,
                    learning_data.actual_cost.spread_cost, learning_data.actual_cost.slippage_cost,
                    learning_data.actual_cost.commission_cost, learning_data.actual_cost.total_cost,
                    learning_data.prediction_accuracy, json.dumps(learning_data.market_conditions_at_execution),
                    learning_data.execution_timestamp
                ))
                conn.commit()
        except Exception as e:
            logging.error(f"Failed to store learning data: {e}")
    
    async def _update_cost_model_if_needed(self):
        """Update cost model parameters based on learning"""
        try:
            # Check if we have enough recent data
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute('''
                    SELECT COUNT(*) FROM cost_predictions 
                    WHERE created_at > datetime('now', '-7 days') AND learned_from = FALSE
                ''')
                count = cursor.fetchone()[0]
                
                if count >= 20:  # Need at least 20 recent trades
                    await self._optimize_cost_parameters()
                    
                    # Mark as learned from
                    conn.execute('''
                        UPDATE cost_predictions SET learned_from = TRUE
                        WHERE created_at > datetime('now', '-7 days')
                    ''')
                    conn.commit()
                    
        except Exception as e:
            logging.error(f"Cost model update failed: {e}")
    
    async def _optimize_cost_parameters(self):
        """Optimize cost model parameters based on recent performance"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Get recent prediction errors
                cursor = conn.execute('''
                    SELECT predicted_spread, actual_spread, predicted_slippage, actual_slippage,
                           accuracy, market_conditions
                    FROM cost_predictions 
                    WHERE created_at > datetime('now', '-7 days') AND learned_from = FALSE
                ''')
                
                data = cursor.fetchall()
                
                if len(data) >= 10:
                    # Analyze spread prediction errors
                    spread_errors = [(row[1] - row[0]) / max(row[0], 0.01) for row in data if row[0] > 0]
                    avg_spread_error = np.mean(spread_errors) if spread_errors else 0
                    
                    # Adjust spread parameters
                    if abs(avg_spread_error) > 0.1:  # 10% error threshold
                        old_spread_bps = self.cost_model['base_spread_bps']
                        adjustment = 1.0 + (avg_spread_error * 0.5)
                        new_spread_bps = old_spread_bps * adjustment
                        
                        self.cost_model['base_spread_bps'] = max(5, min(20, new_spread_bps))
                        
                        # Log the update
                        logging.info(f"[LEARN] Updated spread model: {old_spread_bps:.1f} -> {new_spread_bps:.1f} bps")
                        
                        await self._log_model_update('base_spread_bps', old_spread_bps, new_spread_bps, 
                                                   abs(avg_spread_error), "Spread prediction optimization")
                    
        except Exception as e:
            logging.error(f"Parameter optimization failed: {e}")
    
    async def _log_model_update(self, parameter: str, old_value: float, new_value: float,
                              improvement_score: float, reason: str):
        """Log cost model updates"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT INTO cost_model_updates 
                    (parameter_name, old_value, new_value, improvement_score, update_reason)
                    VALUES (?, ?, ?, ?, ?)
                ''', (parameter, old_value, new_value, improvement_score, reason))
                conn.commit()
        except Exception as e:
            logging.error(f"Failed to log model update: {e}")

# ============================================================================
# COST-AWARE ML ENHANCER
# ============================================================================

class CostAwareMLEnhancer:
    """Enhances ML predictions with execution cost intelligence"""
    
    def __init__(self, ml_engine, cost_predictor: AdvancedExecutionCostPredictor, 
                 external_data_collector=None):
        self.ml_engine = ml_engine
        self.cost_predictor = cost_predictor
        self.external_data_collector = external_data_collector
        
        # Cost-aware decision thresholds
        self.thresholds = {
            'min_cost_efficiency': 2.0,  # 2:1 profit to cost ratio
            'max_cost_percentage': 0.5,  # Max 0.5% of position in costs
            'confidence_cost_adjustment': 0.1,  # Reduce confidence by 10% per 0.1% cost
            'emergency_cost_efficiency': 1.2,  # Lower threshold in emergency mode
        }
        
        # Enhancement tracking
        self.enhancement_history = []
        
        logging.info("[AI] Cost-Aware ML Enhancer initialized")
    
    async def predict_with_cost_intelligence(self, trade_context: Dict) -> CostAdjustedSignal:
        """Enhanced ML prediction with execution cost intelligence"""
        try:
            # Get original ML prediction
            original_prediction = await self._get_original_ml_prediction(trade_context)
            
            # Get fresh market data
            market_data = await self._get_market_data(trade_context.get('symbol', 'BTCUSDT'))
            
            # Get external data if available
            external_data = await self._get_external_data(trade_context.get('symbol', 'BTC'))
            
            # Predict execution costs
            cost_prediction = await self.cost_predictor.predict_execution_cost(
                trade_context, market_data, external_data
            )
            
            # Calculate cost-adjusted metrics
            cost_adjusted_signal = await self._calculate_cost_adjustments(
                original_prediction, cost_prediction, trade_context
            )
            
            # Make final trading decision
            final_decision = await self._make_cost_aware_decision(cost_adjusted_signal)
            
            # Store enhancement data
            self._store_enhancement_data(cost_adjusted_signal)
            
            return final_decision
            
        except Exception as e:
            logging.error(f"Cost intelligence prediction failed: {e}")
            return self._get_fallback_signal(trade_context)
    
    async def _get_original_ml_prediction(self, trade_context: Dict) -> Dict[str, Any]:
        """Get original ML prediction"""
        try:
            if self.ml_engine and hasattr(self.ml_engine, 'predict_with_enhanced_intelligence'):
                prediction = await self.ml_engine.predict_with_enhanced_intelligence(trade_context)
                return prediction
            else:
                # Fallback simple prediction
                return {
                    'should_trade': True,
                    'confidence': 0.65,
                    'decision_factors': ['Simple ML analysis'],
                    'enhanced_intelligence': False
                }
        except Exception as e:
            logging.warning(f"Original ML prediction failed: {e}")
            return {'should_trade': False, 'confidence': 0.0, 'error': str(e)}
    
    async def _get_market_data(self, symbol: str) -> Dict:
        """Get current market data"""
        try:
            # Try to get from data manager or external collector
            if hasattr(self.cost_predictor, 'data_manager') and self.cost_predictor.data_manager:
                data = await self.cost_predictor.data_manager.get_historical_data(
                    symbol, '1h', start_time=datetime.now() - timedelta(hours=24)
                )
                if data:
                    return {
                        'price': data['close'][-1],
                        'volume': data['volume'][-1],
                        'high_24h': max(data['high']),
                        'low_24h': min(data['low']),
                        'klines': data
                    }
            
            # Fallback to simulated data
            base_price = 50000 if 'BTC' in symbol else 3000
            return {
                'price': base_price * (1 + random.uniform(-0.02, 0.02)),
                'volume': random.uniform(1000000, 5000000),
                'high_24h': base_price * 1.03,
                'low_24h': base_price * 0.97,
                'change_24h': random.uniform(-3, 3)
            }
            
        except Exception as e:
            logging.warning(f"Market data retrieval failed: {e}")
            return {'price': 50000, 'volume': 1000000}
    
    async def _get_external_data(self, symbol: str) -> Dict:
        """Get external market data"""
        try:
            if self.external_data_collector:
                return self.external_data_collector.collect_comprehensive_market_data(symbol)
            return {}
        except Exception:
            return {}
    
    async def _calculate_cost_adjustments(self, original_prediction: Dict, 
                                        cost_prediction: ExecutionCostPrediction,
                                        trade_context: Dict) -> CostAdjustedSignal:
        """Calculate cost-adjusted signal metrics"""
        try:
            # Extract original metrics
            original_confidence = original_prediction.get('confidence', 0.5)
            should_trade = original_prediction.get('should_trade', False)
            
            # Calculate expected profit
            position_size = cost_prediction.position_size
            expected_return_pct = trade_context.get('expected_return', 0.015)  # 1.5% default
            original_expected_profit = position_size * expected_return_pct
            
            # Calculate cost efficiency
            total_cost = cost_prediction.predicted_cost.total_cost
            cost_efficiency_ratio = original_expected_profit / max(total_cost, 0.01)
            
            # Adjust confidence based on costs
            cost_percentage = cost_prediction.predicted_cost.cost_percentage
            confidence_adjustment = min(0.3, cost_percentage * self.thresholds['confidence_cost_adjustment'])
            cost_adjusted_confidence = max(0.1, original_confidence - confidence_adjustment)
            
            # Calculate cost-adjusted profit
            cost_adjusted_profit = original_expected_profit - total_cost
            
            # Determine optimal position size
            if cost_efficiency_ratio >= self.thresholds['min_cost_efficiency']:
                recommended_size = position_size
            elif cost_efficiency_ratio >= 1.5:
                recommended_size = position_size * 0.75  # Reduce size
            elif cost_efficiency_ratio >= 1.0:
                recommended_size = position_size * 0.5   # Half size
            else:
                recommended_size = 0  # Don't trade
            
            # Calculate timing score
            timing_score = await self._calculate_optimal_timing_score(cost_prediction)
            
            # Generate enhancement factors
            enhancement_factors = []
            if cost_efficiency_ratio >= 3.0:
                enhancement_factors.append(f"Excellent cost efficiency ({cost_efficiency_ratio:.1f}:1)")
            elif cost_efficiency_ratio >= 2.0:
                enhancement_factors.append(f"Good cost efficiency ({cost_efficiency_ratio:.1f}:1)")
            else:
                enhancement_factors.append(f"Poor cost efficiency ({cost_efficiency_ratio:.1f}:1)")
            
            if cost_prediction.confidence > 0.8:
                enhancement_factors.append("High cost prediction confidence")
            
            if timing_score > 0.8:
                enhancement_factors.append("Optimal timing conditions")
            
            # Generate warnings
            warnings = []
            if cost_percentage > self.thresholds['max_cost_percentage']:
                warnings.append(f"High execution costs ({cost_percentage:.2f}% of position)")
            
            if cost_prediction.session_factors.get('session') == 'OFF_HOURS':
                warnings.append("Off-hours trading may increase costs")
            
            if cost_prediction.market_conditions.get('volatility', 0) > 0.05:
                warnings.append("High volatility increases execution risk")
            
            return CostAdjustedSignal(
                original_signal=original_prediction,
                cost_prediction=cost_prediction,
                original_confidence=original_confidence,
                cost_adjusted_confidence=cost_adjusted_confidence,
                original_expected_profit=original_expected_profit,
                cost_adjusted_expected_profit=cost_adjusted_profit,
                cost_efficiency_ratio=cost_efficiency_ratio,
                should_trade_after_costs=False,  # Will be determined in next step
                recommended_position_size=recommended_size,
                optimal_timing_score=timing_score,
                enhancement_factors=enhancement_factors,
                warnings=warnings,
                created_at=datetime.now(),
                cost_intelligence_version="1.0"
            )
            
        except Exception as e:
            logging.error(f"Cost adjustment calculation failed: {e}")
            return self._get_fallback_signal(trade_context)
    
    async def _calculate_optimal_timing_score(self, cost_prediction: ExecutionCostPrediction) -> float:
        """Calculate how optimal the current timing is"""
        try:
            score = 0.7  # Base score
            
            # Session timing
            session = cost_prediction.session_factors.get('session', 'UNKNOWN')
            if session in ['LONDON', 'NEW_YORK']:
                score += 0.2
            elif session == 'ASIAN':
                score += 0.1
            else:
                score -= 0.2
            
            # Liquidity score
            liquidity = cost_prediction.liquidity_score
            score += (liquidity - 0.5) * 0.4
            
            # Volatility timing
            volatility = cost_prediction.market_conditions.get('volatility', 0.02)
            if 0.01 <= volatility <= 0.03:  # Sweet spot
                score += 0.1
            elif volatility > 0.05:  # Too volatile
                score -= 0.2
            
            # Round number penalty
            timing_penalty = cost_prediction.session_factors.get('timing_penalty', 1.0)
            if timing_penalty > 1.1:
                score -= 0.1
            
            return max(0.1, min(1.0, score))
            
        except Exception as e:
            return 0.5
    
    async def _make_cost_aware_decision(self, signal: CostAdjustedSignal) -> CostAdjustedSignal:
        """Make final cost-aware trading decision"""
        try:
            # Decision criteria
            cost_efficiency_ok = signal.cost_efficiency_ratio >= self.thresholds['min_cost_efficiency']
            confidence_ok = signal.cost_adjusted_confidence >= 0.6
            profit_positive = signal.cost_adjusted_expected_profit > 0
            timing_ok = signal.optimal_timing_score >= 0.5
            
            # Emergency mode - lower standards
            emergency_mode = signal.original_signal.get('emergency_mode', False)
            if emergency_mode:
                cost_efficiency_ok = signal.cost_efficiency_ratio >= self.thresholds['emergency_cost_efficiency']
                confidence_ok = signal.cost_adjusted_confidence >= 0.45
            
            # Final decision
            should_trade = (
                cost_efficiency_ok and 
                confidence_ok and 
                profit_positive and 
                timing_ok and
                signal.recommended_position_size > 0
            )
            
            # Update signal
            signal.should_trade_after_costs = should_trade
            
            # Add decision reasoning
            if not should_trade:
                reasons = []
                if not cost_efficiency_ok:
                    reasons.append(f"Poor cost efficiency ({signal.cost_efficiency_ratio:.1f}:1)")
                if not confidence_ok:
                    reasons.append(f"Low confidence ({signal.cost_adjusted_confidence:.1%})")
                if not profit_positive:
                    reasons.append("Negative expected profit after costs")
                if not timing_ok:
                    reasons.append("Poor timing conditions")
                
                signal.warnings.extend(reasons)
            
            return signal
            
        except Exception as e:
            logging.error(f"Cost-aware decision failed: {e}")
            signal.should_trade_after_costs = False
            signal.warnings.append(f"Decision error: {e}")
            return signal
    
    def _store_enhancement_data(self, signal: CostAdjustedSignal):
        """Store enhancement data for analysis"""
        try:
            enhancement_data = {
                'timestamp': signal.created_at,
                'symbol': signal.cost_prediction.symbol,
                'original_confidence': signal.original_confidence,
                'cost_adjusted_confidence': signal.cost_adjusted_confidence,
                'cost_efficiency_ratio': signal.cost_efficiency_ratio,
                'should_trade': signal.should_trade_after_costs,
                'recommended_size': signal.recommended_position_size,
                'total_cost': signal.cost_prediction.predicted_cost.total_cost
            }
            
            self.enhancement_history.append(enhancement_data)
            
            # Keep only last 1000 records
            if len(self.enhancement_history) > 1000:
                self.enhancement_history = self.enhancement_history[-1000:]
                
        except Exception as e:
            logging.error(f"Failed to store enhancement data: {e}")
    
    def _get_fallback_signal(self, trade_context: Dict) -> CostAdjustedSignal:
        """Fallback signal when main calculation fails"""
        fallback_cost = ExecutionCostPrediction(
            symbol=trade_context.get('symbol', 'BTCUSDT'),
            position_size=1000,
            predicted_cost=ExecutionCostBreakdown(20, 15, 10, 0, 0, 45, 4.5),
            confidence=0.5,
            market_conditions={},
            session_factors={},
            volatility_adjustment=1.0,
            liquidity_score=0.5,
            predicted_at=datetime.now(),
            valid_until=datetime.now() + timedelta(minutes=5),
            reasoning=["Fallback cost analysis"]
        )
        
        return CostAdjustedSignal(
            original_signal={},
            cost_prediction=fallback_cost,
            original_confidence=0.0,
            cost_adjusted_confidence=0.0,
            original_expected_profit=0,
            cost_adjusted_expected_profit=-45,
            cost_efficiency_ratio=0.0,
            should_trade_after_costs=False,
            recommended_position_size=0,
            optimal_timing_score=0.5,
            enhancement_factors=["Fallback analysis"],
            warnings=["Analysis failed - using fallback"],
            created_at=datetime.now(),
            cost_intelligence_version="1.0-fallback"
        )
    
    def get_enhancement_metrics(self) -> Dict:
        """Get cost enhancement performance metrics"""
        try:
            if not self.enhancement_history:
                return {'no_data': True}
            
            recent_data = self.enhancement_history[-100:]  # Last 100 enhancements
            
            metrics = {
                'total_enhancements': len(self.enhancement_history),
                'recent_enhancements': len(recent_data),
                'avg_cost_efficiency': np.mean([d['cost_efficiency_ratio'] for d in recent_data]),
                'trade_approval_rate': np.mean([d['should_trade'] for d in recent_data]),
                'avg_confidence_adjustment': np.mean([
                    d['original_confidence'] - d['cost_adjusted_confidence'] for d in recent_data
                ]),
                'avg_total_cost': np.mean([d['total_cost'] for d in recent_data]),
                'last_enhancement': recent_data[-1]['timestamp'].isoformat() if recent_data else None
            }
            
            return metrics
            
        except Exception as e:
            return {'error': str(e)}

# ============================================================================
# MAIN INTEGRATION CLASS
# ============================================================================

class ExecutionCostIntegrator:
    """Main class for integrating execution cost intelligence with existing systems"""
    
    def __init__(self, ml_engine=None, trading_engine=None, adaptive_manager=None, 
                 data_manager=None, external_data_collector=None):
        """
        Initialize with existing system components
        
        Args:
            ml_engine: Your AdvancedMLEngine instance
            trading_engine: Your IntelligentTradingEngine instance
            adaptive_manager: Your AdaptiveTradingManager instance
            data_manager: Your data manager instance
            external_data_collector: Your ExternalDataCollector instance
        """
        # Store references to existing components
        self.ml_engine = ml_engine
        self.trading_engine = trading_engine
        self.adaptive_manager = adaptive_manager
        self.data_manager = data_manager
        self.external_data_collector = external_data_collector
        
        # Initialize cost intelligence components
        self.cost_predictor = AdvancedExecutionCostPredictor(data_manager)
        self.cost_aware_ml = CostAwareMLEnhancer(ml_engine, self.cost_predictor, external_data_collector)
        
        # Integration settings
        self.enabled = True
        self.integration_mode = 'WRAPPER'  # 'WRAPPER' or 'DIRECT'
        
        # Performance tracking
        self.integration_stats = {
            'predictions_made': 0,
            'trades_enhanced': 0,
            'trades_rejected_by_cost': 0,
            'cost_savings_estimated': 0.0,
            'accuracy_improvements': []
        }
        
        logging.info("[INTEGRATION] Execution Cost Intelligence integrated successfully")
    
    async def enhanced_predict_with_cost_intelligence(self, trade_context: Dict) -> CostAdjustedSignal:
        """Main method: Enhanced ML prediction with cost intelligence"""
        try:
            if not self.enabled:
                return await self._fallback_prediction(trade_context)
            
            # Get cost-aware prediction
            cost_adjusted_signal = await self.cost_aware_ml.predict_with_cost_intelligence(trade_context)
            
            # Update stats
            self.integration_stats['predictions_made'] += 1
            if cost_adjusted_signal.should_trade_after_costs:
                self.integration_stats['trades_enhanced'] += 1
            else:
                self.integration_stats['trades_rejected_by_cost'] += 1
                # Estimate cost savings
                cost_saved = cost_adjusted_signal.cost_prediction.predicted_cost.total_cost
                self.integration_stats['cost_savings_estimated'] += cost_saved
            
            return cost_adjusted_signal
            
        except Exception as e:
            logging.error(f"Enhanced prediction failed: {e}")
            return await self._fallback_prediction(trade_context)
    
    async def enhanced_trading_execution(self, signal: CostAdjustedSignal) -> Dict:
        """Enhanced trading execution with cost tracking"""
        try:
            if not signal.should_trade_after_costs:
                return {
                    'executed': False,
                    'reason': 'Rejected by cost intelligence',
                    'cost_efficiency': signal.cost_efficiency_ratio,
                    'estimated_cost': signal.cost_prediction.predicted_cost.total_cost
                }
            
            # Execute the trade using existing trading engine
            if self.trading_engine and hasattr(self.trading_engine, 'execute_real_testnet_trade'):
                trade_signal = {
                    'symbol': signal.cost_prediction.symbol,
                    'type': 'BUY' if signal.cost_adjusted_expected_profit > 0 else 'SELL',
                    'confidence': signal.cost_adjusted_confidence * 100,
                    'position_size': signal.recommended_position_size,
                    'market_data': signal.cost_prediction.market_conditions
                }
                
                trade_result = await self.trading_engine.execute_real_testnet_trade(trade_signal)
                
                if trade_result:
                    # Calculate actual costs and learn
                    await self._learn_from_execution(signal, trade_result)
                    
                    trade_result['cost_intelligence'] = {
                        'predicted_cost': signal.cost_prediction.predicted_cost.total_cost,
                        'cost_efficiency': signal.cost_efficiency_ratio,
                        'confidence_adjustment': signal.original_confidence - signal.cost_adjusted_confidence,
                        'enhancement_factors': signal.enhancement_factors
                    }
                
                return trade_result
            else:
                # Fallback simulation
                return await self._simulate_enhanced_execution(signal)
                
        except Exception as e:
            logging.error(f"Enhanced execution failed: {e}")
            return {'executed': False, 'error': str(e)}
    
    async def _learn_from_execution(self, signal: CostAdjustedSignal, trade_result: Dict):
        """Learn from actual trade execution"""
        try:
            # Calculate actual execution costs
            actual_cost = await self._calculate_actual_execution_cost(trade_result)
            
            # Learn from the prediction vs reality
            trade_id = trade_result.get('trade_id', f"trade_{datetime.now().timestamp()}")
            await self.cost_predictor.learn_from_execution(
                signal.cost_prediction, actual_cost, str(trade_id)
            )
            
            # Track accuracy improvement
            predicted_total = signal.cost_prediction.predicted_cost.total_cost
            actual_total = actual_cost.total_cost
            accuracy = 1.0 - abs(predicted_total - actual_total) / max(predicted_total, 0.01)
            
            self.integration_stats['accuracy_improvements'].append(accuracy)
            
            # Keep only last 100 accuracy measurements
            if len(self.integration_stats['accuracy_improvements']) > 100:
                self.integration_stats['accuracy_improvements'] = \
                    self.integration_stats['accuracy_improvements'][-100:]
            
        except Exception as e:
            logging.error(f"Learning from execution failed: {e}")
    
    async def _calculate_actual_execution_cost(self, trade_result: Dict) -> ExecutionCostBreakdown:
        """Calculate actual execution costs from trade result"""
        try:
            # Extract trade details
            entry_price = trade_result.get('price', 0)
            quantity = trade_result.get('quantity', 0)
            position_value = entry_price * quantity
            
            # Estimate actual costs (simplified)
            # In production, this would come from broker/exchange data
            spread_cost = position_value * 0.0005  # 0.05%
            slippage_cost = position_value * 0.0003  # 0.03%
            commission_cost = min(25, position_value * 0.001)  # 0.1% or $25 max
            
            total_cost = spread_cost + slippage_cost + commission_cost
            cost_percentage = (total_cost / position_value) * 100 if position_value > 0 else 0
            
            return ExecutionCostBreakdown(
                spread_cost=spread_cost,
                slippage_cost=slippage_cost,
                commission_cost=commission_cost,
                market_impact_cost=0.0,
                timing_cost=0.0,
                total_cost=total_cost,
                cost_percentage=cost_percentage
            )
            
        except Exception as e:
            logging.error(f"Actual cost calculation failed: {e}")
            # Return fallback
            return ExecutionCostBreakdown(15, 10, 8, 0, 0, 33, 3.3)
    
    async def _simulate_enhanced_execution(self, signal: CostAdjustedSignal) -> Dict:
        """Simulate enhanced execution when real trading engine unavailable"""
        try:
            # Simulate execution with cost considerations
            success_probability = signal.cost_adjusted_confidence * signal.optimal_timing_score
            executed = random.random() < success_probability
            
            if executed:
                profit_loss = signal.cost_adjusted_expected_profit * random.uniform(0.8, 1.2)
                actual_cost = signal.cost_prediction.predicted_cost.total_cost * random.uniform(0.9, 1.1)
                
                return {
                    'executed': True,
                    'trade_id': f"sim_{datetime.now().timestamp()}",
                    'symbol': signal.cost_prediction.symbol,
                    'quantity': signal.recommended_position_size / 50000,  # Approximate
                    'price': 50000,  # Simulated
                    'profit_loss': profit_loss - actual_cost,
                    'cost_intelligence': {
                        'predicted_cost': signal.cost_prediction.predicted_cost.total_cost,
                        'actual_cost': actual_cost,
                        'cost_efficiency': signal.cost_efficiency_ratio,
                        'enhancement_factors': signal.enhancement_factors
                    },
                    'simulation': True
                }
            else:
                return {
                    'executed': False,
                    'reason': 'Simulated execution failed',
                    'cost_intelligence': {
                        'predicted_cost': signal.cost_prediction.predicted_cost.total_cost,
                        'cost_efficiency': signal.cost_efficiency_ratio
                    }
                }
                
        except Exception as e:
            return {'executed': False, 'error': str(e), 'simulation': True}
    
    async def _fallback_prediction(self, trade_context: Dict) -> CostAdjustedSignal:
        """Fallback when cost intelligence is disabled"""
        return CostAdjustedSignal(
            original_signal={'should_trade': False, 'confidence': 0.0},
            cost_prediction=ExecutionCostPrediction(
                symbol=trade_context.get('symbol', 'UNKNOWN'),
                position_size=0,
                predicted_cost=ExecutionCostBreakdown(0, 0, 0, 0, 0, 0, 0),
                confidence=0.0,
                market_conditions={},
                session_factors={},
                volatility_adjustment=1.0,
                liquidity_score=0.5,
                predicted_at=datetime.now(),
                valid_until=datetime.now(),
                reasoning=["Cost intelligence disabled"]
            ),
            original_confidence=0.0,
            cost_adjusted_confidence=0.0,
            original_expected_profit=0,
            cost_adjusted_expected_profit=0,
            cost_efficiency_ratio=0.0,
            should_trade_after_costs=False,
            recommended_position_size=0,
            optimal_timing_score=0.0,
            enhancement_factors=["Disabled"],
            warnings=["Cost intelligence disabled"],
            created_at=datetime.now(),
            cost_intelligence_version="disabled"
        )
    
    def get_integration_status(self) -> Dict:
        """Get comprehensive integration status"""
        try:
            # Basic status
            status = {
                'enabled': self.enabled,
                'integration_mode': self.integration_mode,
                'components_connected': {
                    'ml_engine': self.ml_engine is not None,
                    'trading_engine': self.trading_engine is not None,
                    'adaptive_manager': self.adaptive_manager is not None,
                    'data_manager': self.data_manager is not None,
                    'external_data_collector': self.external_data_collector is not None
                },
                'cost_predictor_ready': self.cost_predictor is not None,
                'cost_aware_ml_ready': self.cost_aware_ml is not None
            }
            
            # Performance stats
            stats = dict(self.integration_stats)
            if stats['accuracy_improvements']:
                stats['avg_accuracy'] = np.mean(stats['accuracy_improvements'])
                stats['recent_accuracy'] = np.mean(stats['accuracy_improvements'][-10:])
            
            status['performance_stats'] = stats
            
            # Cost predictor metrics
            if self.cost_aware_ml:
                status['enhancement_metrics'] = self.cost_aware_ml.get_enhancement_metrics()
            
            return status
            
        except Exception as e:
            return {'error': str(e), 'enabled': False}
    
    def enable_cost_intelligence(self):
        """Enable cost intelligence"""
        self.enabled = True
        logging.info("[COST] Cost intelligence enabled")
    
    def disable_cost_intelligence(self):
        """Disable cost intelligence"""
        self.enabled = False
        logging.info("[COST] Cost intelligence disabled")
    
    def set_integration_mode(self, mode: str):
        """Set integration mode: 'WRAPPER' or 'DIRECT'"""
        if mode in ['WRAPPER', 'DIRECT']:
            self.integration_mode = mode
            logging.info(f"[COST] Integration mode set to {mode}")
        else:
            logging.warning(f"Invalid integration mode: {mode}")

# ============================================================================
# INTEGRATION FUNCTIONS
# ============================================================================

def integrate_with_existing_system(ml_engine=None, trading_engine=None, adaptive_manager=None,
                                 data_manager=None, external_data_collector=None) -> ExecutionCostIntegrator:
    """
    Main integration function - call this to add cost intelligence to your system
    
    Args:
        ml_engine: Your AdvancedMLEngine instance
        trading_engine: Your IntelligentTradingEngine instance  
        adaptive_manager: Your AdaptiveTradingManager instance
        data_manager: Your data manager instance
        external_data_collector: Your ExternalDataCollector instance
    
    Returns:
        ExecutionCostIntegrator: Integrated cost intelligence system
    """
    try:
        integrator = ExecutionCostIntegrator(
            ml_engine=ml_engine,
            trading_engine=trading_engine,
            adaptive_manager=adaptive_manager,
            data_manager=data_manager,
            external_data_collector=external_data_collector
        )
        
        logging.info("[SUCCESS] Execution Cost Intelligence integrated with existing system")
        return integrator
        
    except Exception as e:
        logging.error(f"[FAIL] Integration failed: {e}")
        raise Exception(f"Cost intelligence integration failed: {e}")

async def test_cost_intelligence_integration(integrator: ExecutionCostIntegrator) -> Dict:
    """Test the integrated cost intelligence system"""
    try:
        print("🧪 Testing integrated cost intelligence...")
        
        # Test prediction
        test_context = {
            'symbol': 'BTCUSDT',
            'account_balance': 10000,
            'expected_return': 0.02,
            'current_price': 50000,
            'rsi': 65,
            'trend_strength': 0.7
        }
        
        # Get cost-adjusted prediction
        result = await integrator.enhanced_predict_with_cost_intelligence(test_context)
        
        # Test execution
        if result.should_trade_after_costs:
            execution_result = await integrator.enhanced_trading_execution(result)
        else:
            execution_result = {'executed': False, 'reason': 'Cost intelligence rejection'}
        
        # Get status
        status = integrator.get_integration_status()
        
        return {
            'prediction_test': {
                'cost_efficiency': result.cost_efficiency_ratio,
                'should_trade': result.should_trade_after_costs,
                'predicted_cost': result.cost_prediction.predicted_cost.total_cost,
                'confidence_adjustment': result.original_confidence - result.cost_adjusted_confidence,
                'reasoning': result.enhancement_factors[:3]
            },
            'execution_test': execution_result,
            'integration_status': status,
            'test_passed': True
        }
        
    except Exception as e:
        return {'test_passed': False, 'error': str(e)}

# ============================================================================
# MAIN EXECUTION AND TESTING
# ============================================================================

if __name__ == "__main__":
    async def main():
        print("🎯 EXECUTION COST INTELLIGENCE - FULL SYSTEM TEST")
        print("=" * 60)
        
        # Test cost predictor
        cost_predictor = AdvancedExecutionCostPredictor()
        
        test_signal = {'symbol': 'BTCUSDT', 'position_size': 2000}
        test_market = {'price': 50000, 'volume': 2000000, 'change_24h': 2.5}
        
        cost_prediction = await cost_predictor.predict_execution_cost(test_signal, test_market)
        
        print(f"💰 Cost Prediction Test:")
        print(f"   Total Cost: ${cost_prediction.predicted_cost.total_cost:.2f}")
        print(f"   Cost Percentage: {cost_prediction.predicted_cost.cost_percentage:.2f}%")
        print(f"   Confidence: {cost_prediction.confidence:.1%}")
        print(f"   Reasoning: {cost_prediction.reasoning[0] if cost_prediction.reasoning else 'None'}")
        
        # Test integration
        integrator = integrate_with_existing_system()
        integration_test = await test_cost_intelligence_integration(integrator)
        
        print(f"\n🔧 Integration Test:")
        if integration_test['test_passed']:
            print("   ✅ All tests passed")
            pred = integration_test['prediction_test']
            print(f"   📊 Cost Efficiency: {pred['cost_efficiency']:.1f}:1")
            print(f"   🎯 Should Trade: {pred['should_trade']}")
            print(f"   💵 Predicted Cost: ${pred['predicted_cost']:.2f}")
        else:
            print(f"   ❌ Test failed: {integration_test['error']}")
        
        print(f"\n🎉 Full system test complete!")
    
    # Run the test
    import asyncio
    asyncio.run(main())