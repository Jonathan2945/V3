#!/usr/bin/env python3
"""
ADVANCED MULTI-TIMEFRAME BACKTESTING WITH STRATEGY DISCOVERY - UPGRADED
======================================================================
UPGRADES APPLIED:
- Comprehensive resource management and monitoring
- Enhanced multi-timeframe confluence analysis
- Advanced strategy discovery with genetic optimization
- Memory-efficient processing with throttling
- Quality validation and performance metrics
- Database integration for ML training
- Progress tracking and ETA calculation
"""

import pandas as pd
import numpy as np
import asyncio
import time
import psutil
import gc
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import sqlite3
from pathlib import Path
import logging
import random
from dataclasses import dataclass
import json
from concurrent.futures import ThreadPoolExecutor
import threading

@dataclass
class AnalysisProgress:
    """Progress tracking for advanced analysis"""
    total_combinations: int = 0
    completed_combinations: int = 0
    current_symbol: str = ""
    current_strategy: str = ""
    start_time: datetime = None
    estimated_completion: datetime = None
    avg_time_per_combination: float = 0.0
    memory_usage_mb: float = 0.0
    cpu_usage_pct: float = 0.0
    status: str = "initializing"

class ResourceMonitor:
    """Advanced resource monitoring and management"""
    
    def __init__(self, max_memory_mb: int = 1500, max_cpu_pct: float = 80.0):
        self.max_memory_mb = max_memory_mb
        self.max_cpu_pct = max_cpu_pct
        self.logger = logging.getLogger(f"{__name__}.ResourceMonitor")
        self.last_gc_time = time.time()
        
    def check_resources(self) -> Dict[str, any]:
        """Check current resource usage"""
        try:
            memory_mb = psutil.virtual_memory().used / 1024 / 1024
            cpu_pct = psutil.cpu_percent(interval=0.1)
            
            return {
                'memory_mb': memory_mb,
                'cpu_pct': cpu_pct,
                'memory_ok': memory_mb < self.max_memory_mb,
                'cpu_ok': cpu_pct < self.max_cpu_pct,
                'should_pause': memory_mb > self.max_memory_mb or cpu_pct > self.max_cpu_pct
            }
        except Exception as e:
            self.logger.warning(f"Resource check failed: {e}")
            return {'memory_mb': 0, 'cpu_pct': 0, 'memory_ok': True, 'cpu_ok': True, 'should_pause': False}
    
    async def ensure_resources_available(self):
        """Wait for resources to be available"""
        while True:
            resources = self.check_resources()
            
            if resources['should_pause']:
                self.logger.warning(f"Resource pause: Memory={resources['memory_mb']:.0f}MB, CPU={resources['cpu_pct']:.1f}%")
                
                # Force garbage collection
                gc.collect()
                self.last_gc_time = time.time()
                
                # Wait for resources to free up
                await asyncio.sleep(10)
                continue
            else:
                # Periodic garbage collection
                if time.time() - self.last_gc_time > 300:  # Every 5 minutes
                    gc.collect()
                    self.last_gc_time = time.time()
                break

class AdvancedMultiTimeframeBacktester:
    """UPGRADED: Advanced Multi-Timeframe Backtester with comprehensive resource management"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # UPGRADED: All available Binance timeframes for comprehensive historical data
        self.timeframes = [
            '1m', '3m', '5m', '15m', '30m',
            '1h', '2h', '4h', '6h', '8h', '12h', 
            '1d', '3d', '1w', '1M'
        ]
        
        # UPGRADED: Enhanced multi-timeframe combinations with more strategies
        self.mtf_combinations = [
            # Short-term combinations
            (['1m', '5m', '15m'], 'scalping_ultra'),
            (['3m', '15m', '30m'], 'scalping_standard'),
            (['5m', '15m', '30m'], 'short_term_momentum'),
            (['15m', '1h', '4h'], 'intraday_trend'),
            (['30m', '2h', '8h'], 'intraday_swing'),
            (['1h', '4h', '1d'], 'swing_trading'),
            (['2h', '8h', '1d'], 'daily_swing'),
            (['4h', '1d', '3d'], 'multi_day_position'),
            (['6h', '1d', '1w'], 'weekly_swing'),
            (['8h', '1d', '1w'], 'position_trading'),
            (['1d', '3d', '1w'], 'weekly_trend'),
            (['1d', '1w', '1M'], 'long_term_trend')
        ]
        
        # UPGRADED: Advanced resource management
        self.resource_monitor = ResourceMonitor(max_memory_mb=1500, max_cpu_pct=80.0)
        self.delay_between_analysis = 1.0
        self.batch_size = 10
        self.max_concurrent_analyses = 3
        
        # UPGRADED: Enhanced strategy discovery parameters
        self.strategy_discovery = True
        self.genetic_optimization = True
        self.min_backtest_trades = 20
        self.min_sharpe_ratio = 1.0
        self.min_win_rate = 55.0
        self.max_drawdown_threshold = 25.0
        
        # UPGRADED: Quality thresholds for strategy validation
        self.quality_thresholds = {
            'min_trades': 20,
            'min_win_rate': 55.0,
            'min_sharpe_ratio': 1.0,
            'max_drawdown': 25.0,
            'min_profit_factor': 1.2,
            'min_total_return': 5.0
        }
        
        # UPGRADED: Progress tracking with thread safety
        self._progress_lock = threading.Lock()
        self.analysis_progress = AnalysisProgress()
        self.combinations_completed = 0
        self.total_combinations_to_analyze = 0
        self.current_analysis_symbol = ""
        self.current_analysis_strategy = ""
        
        # UPGRADED: Database setup for ML training integration
        self._setup_database()
        
        # UPGRADED: Performance tracking
        self.successful_analyses = 0
        self.failed_analyses = 0
        self.strategy_performance_cache = {}
        
        self.logger.info("UPGRADED Advanced Multi-Timeframe Backtester initialized")
    
    def _setup_database(self):
        """Setup database for storing analysis results"""
        try:
            self.db_path = Path('data/advanced_backtesting_results.db')
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS advanced_results (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT,
                        strategy_type TEXT,
                        timeframes TEXT,
                        total_trades INTEGER,
                        winning_trades INTEGER,
                        win_rate REAL,
                        total_return REAL,
                        annualized_return REAL,
                        max_drawdown REAL,
                        sharpe_ratio REAL,
                        sortino_ratio REAL,
                        profit_factor REAL,
                        payoff_ratio REAL,
                        volatility REAL,
                        best_trade REAL,
                        worst_trade REAL,
                        avg_trade_duration REAL,
                        confluence_strength REAL,
                        risk_adjusted_return REAL,
                        analysis_timestamp TEXT,
                        processing_time_seconds REAL
                    )
                ''')
                
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS strategy_discovery_results (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT,
                        timeframe TEXT,
                        strategy_name TEXT,
                        parameters TEXT,
                        fitness_score REAL,
                        total_trades INTEGER,
                        win_rate REAL,
                        total_return REAL,
                        max_drawdown REAL,
                        sharpe_ratio REAL,
                        discovered_at TEXT
                    )
                ''')
                
                # Create indexes for better performance
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_results_symbol ON advanced_results(symbol)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_results_strategy ON advanced_results(strategy_type)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_results_sharpe ON advanced_results(sharpe_ratio)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_discovery_fitness ON strategy_discovery_results(fitness_score)')
                
        except Exception as e:
            self.logger.error(f"Database setup failed: {e}")
    
    # UPGRADED: Enhanced run_multi_timeframe_analysis method
    async def run_multi_timeframe_analysis(self, symbol: str):
        """UPGRADED: Enhanced multi-timeframe analysis with comprehensive resource management"""
        
        with self._progress_lock:
            self.current_analysis_symbol = symbol
        
        self.logger.info(f"Enhanced multi-timeframe analysis for {symbol}")
        
        # Resource check before starting
        await self.resource_monitor.ensure_resources_available()
        
        # Get comprehensive data for all timeframes
        timeframe_data = {}
        data_collection_start = time.time()
        
        for tf in self.timeframes:
            try:
                data = await self.get_comprehensive_data(symbol, tf)
                if data is not None and len(data) >= 200:
                    timeframe_data[tf] = data
                    self.logger.debug(f"? {symbol} {tf}: {len(data)} candles")
                else:
                    self.logger.debug(f"? {symbol} {tf}: Insufficient data")
            except Exception as e:
                self.logger.debug(f"? {symbol} {tf}: Error - {e}")
        
        data_collection_time = time.time() - data_collection_start
        self.logger.debug(f"Data collection for {symbol} completed in {data_collection_time:.2f}s")
        
        if len(timeframe_data) < 5:
            self.logger.warning(f"Insufficient timeframes for {symbol}: {len(timeframe_data)}/15")
            return None
        
        # Run enhanced confluence analysis with resource management
        confluence_results = {}
        analysis_start = time.time()
        
        for strategy_idx, (timeframes, strategy_type) in enumerate(self.mtf_combinations):
            with self._progress_lock:
                self.current_analysis_strategy = strategy_type
            
            # Resource management check
            if strategy_idx % 3 == 0:
                await self.resource_monitor.ensure_resources_available()
            
            # Check if we have data for required timeframes
            available_tfs = [tf for tf in timeframes if tf in timeframe_data]
            
            if len(available_tfs) >= 2:
                self.logger.debug(f"Analyzing {symbol} {strategy_type}: {available_tfs}")
                
                strategy_start = time.time()
                
                result = await self.analyze_timeframe_confluence(
                    symbol, available_tfs, timeframe_data, strategy_type
                )
                
                strategy_time = time.time() - strategy_start
                
                if result and self._is_valid_strategy_result(result):
                    confluence_results[strategy_type] = result
                    self.successful_analyses += 1
                    self.logger.info(f"? {symbol} {strategy_type}: {result['total_trades']} trades, "
                                   f"{result['win_rate']:.1f}% win rate, "
                                   f"Sharpe: {result.get('sharpe_ratio', 0):.2f} ({strategy_time:.2f}s)")
                else:
                    self.failed_analyses += 1
                    self.logger.debug(f"? {symbol} {strategy_type}: Invalid results")
            
            # Throttling to prevent resource exhaustion
            await asyncio.sleep(self.delay_between_analysis)
            
            with self._progress_lock:
                self.combinations_completed += 1
        
        analysis_time = time.time() - analysis_start
        self.logger.info(f"Analysis complete for {symbol}: {len(confluence_results)} valid strategies in {analysis_time:.2f}s")
        
        # Save results to database for ML training
        if confluence_results:
            await self._save_analysis_results(symbol, confluence_results)
        
        return confluence_results
    
    # UPGRADED: Enhanced analyze_timeframe_confluence method
    async def analyze_timeframe_confluence(self, symbol: str, timeframes: list, 
                                         data_dict: dict, strategy_type: str):
        """UPGRADED: Enhanced confluence analysis with advanced signal detection"""
        
        try:
            # Enhanced signal weights based on strategy type
            signal_weights = self._get_strategy_signal_weights(strategy_type)
            
            # Get the shortest timeframe as primary for trade execution
            primary_tf = min(timeframes, key=lambda x: self._timeframe_to_minutes(x))
            confirmation_tfs = [tf for tf in timeframes if tf != primary_tf]
            
            primary_data = data_dict[primary_tf]
            
            # Calculate enhanced indicators for all timeframes
            tf_indicators = {}
            for tf in timeframes:
                tf_indicators[tf] = self.calculate_all_indicators(data_dict[tf])
            
            # Run enhanced confluence backtest
            trades = []
            balance = 10000
            position = None
            
            # Use primary timeframe for iteration with more data points
            min_data_length = min(len(data_dict[tf]) for tf in timeframes)
            start_index = max(200, min_data_length // 4)
            
            for i in range(start_index, min_data_length - 10):
                
                # Get enhanced confluence signals from all timeframes
                confluence_signals = self._get_enhanced_confluence_signals(
                    tf_indicators, timeframes, i, primary_tf, signal_weights
                )
                
                # Enhanced entry logic with strategy-specific conditions
                if not position and confluence_signals['strength'] >= self._get_min_confluence_strength(strategy_type):
                    
                    entry_conditions = self._check_strategy_specific_entry(
                        strategy_type, tf_indicators, timeframes, i, confluence_signals
                    )
                    
                    if entry_conditions['should_enter']:
                        position = {
                            'entry_price': primary_data.iloc[i]['close'],
                            'entry_time': primary_data.iloc[i]['timestamp'] if 'timestamp' in primary_data.columns else i,
                            'entry_index': i,
                            'side': confluence_signals['direction'],
                            'confidence': confluence_signals['confidence'],
                            'timeframes_confirming': confluence_signals['confirming_tfs'],
                            'strategy_signals': entry_conditions['signals'],
                            'stop_loss': entry_conditions.get('stop_loss'),
                            'take_profit': entry_conditions.get('take_profit')
                        }
                
                # Enhanced exit logic
                elif position:
                    exit_signal = self._get_enhanced_exit_signal(
                        tf_indicators, timeframes, i, position, primary_tf, strategy_type
                    )
                    
                    if exit_signal:
                        current_price = primary_data.iloc[i]['close']
                        
                        # Calculate P&L with enhanced logic
                        if position['side'] == 'BUY':
                            return_pct = (current_price - position['entry_price']) / position['entry_price']
                        else:
                            return_pct = (position['entry_price'] - current_price) / position['entry_price']
                        
                        # Apply realistic trading costs based on strategy type
                        trading_cost = self._get_strategy_trading_cost(strategy_type)
                        return_pct -= trading_cost
                        
                        balance *= (1 + return_pct)
                        
                        # Create comprehensive trade record
                        trade = {
                            **position,
                            'exit_price': current_price,
                            'exit_time': primary_data.iloc[i]['timestamp'] if 'timestamp' in primary_data.columns else i,
                            'exit_index': i,
                            'return_pct': return_pct,
                            'exit_reason': exit_signal['reason'],
                            'hold_duration_candles': i - position.get('entry_index', i),
                            'exit_signals': exit_signal.get('signals', [])
                        }
                        
                        trades.append(trade)
                        position = None
            
            # Enhanced performance calculation
            if not trades or len(trades) < self.min_backtest_trades:
                return None
            
            performance_metrics = self._calculate_enhanced_performance_metrics(
                trades, balance, strategy_type, symbol, timeframes
            )
            
            return performance_metrics
            
        except Exception as e:
            self.logger.error(f"Confluence analysis failed for {symbol} {strategy_type}: {e}")
            return None
    
    def _get_strategy_signal_weights(self, strategy_type: str) -> Dict[str, float]:
        """Get signal weights based on strategy type"""
        
        base_weights = {
            'rsi': 1.0, 'macd': 1.0, 'ma': 1.0, 'bb': 1.0,
            'stoch': 0.8, 'volume': 0.6, 'momentum': 0.8, 'atr': 0.7
        }
        
        # Adjust weights based on strategy type
        if 'scalping' in strategy_type:
            base_weights.update({
                'momentum': 1.5, 'volume': 1.3, 'bb': 1.4, 'rsi': 1.2
            })
        elif 'swing' in strategy_type:
            base_weights.update({
                'ma': 1.4, 'macd': 1.3, 'rsi': 1.1, 'atr': 1.0
            })
        elif 'trend' in strategy_type:
            base_weights.update({
                'ma': 1.6, 'macd': 1.4, 'momentum': 1.3, 'volume': 0.8
            })
        elif 'position' in strategy_type:
            base_weights.update({
                'ma': 1.5, 'macd': 1.2, 'rsi': 0.9, 'volume': 0.7
            })
        
        return base_weights
    
    def _get_min_confluence_strength(self, strategy_type: str) -> int:
        """Get minimum confluence strength required for strategy"""
        if 'scalping' in strategy_type:
            return 3  # Higher confirmation needed
        elif 'long_term' in strategy_type or 'position' in strategy_type:
            return 2  # Lower confirmation ok
        else:
            return 2  # Standard confirmation
    
    def _get_strategy_trading_cost(self, strategy_type: str) -> float:
        """Get trading costs based on strategy frequency"""
        if 'scalping' in strategy_type:
            return 0.003  # Higher costs due to frequency
        elif 'long_term' in strategy_type or 'position' in strategy_type:
            return 0.001  # Lower costs due to lower frequency
        else:
            return 0.002  # Standard costs
    
    def _timeframe_to_minutes(self, timeframe: str) -> int:
        """Convert timeframe string to minutes for comparison"""
        if timeframe.endswith('m'):
            return int(timeframe[:-1])
        elif timeframe.endswith('h'):
            return int(timeframe[:-1]) * 60
        elif timeframe.endswith('d'):
            return int(timeframe[:-1]) * 1440
        elif timeframe.endswith('w'):
            return int(timeframe[:-1]) * 10080
        elif timeframe.endswith('M'):
            return int(timeframe[:-1]) * 43200
        return 60
    
    def _get_enhanced_confluence_signals(self, tf_indicators: Dict, timeframes: List[str], 
                                       index: int, primary_tf: str, signal_weights: Dict) -> Dict:
        """Get enhanced confluence signals from multiple timeframes"""
        
        signals = {'BUY': 0, 'SELL': 0, 'NEUTRAL': 0}
        confirming_tfs = []
        signal_details = []
        
        for tf in timeframes:
            indicators = tf_indicators[tf]
            tf_index = min(index, len(indicators) - 1)
            
            if tf_index < 50:
                continue
            
            tf_signals = []
            tf_signal_strength = 0
            
            try:
                # RSI signals with weighting
                rsi = indicators.iloc[tf_index]['rsi']
                if not pd.isna(rsi):
                    if rsi < 30:
                        tf_signals.append('BUY')
                        tf_signal_strength += signal_weights.get('rsi', 1.0) * (30 - rsi) / 30
                    elif rsi > 70:
                        tf_signals.append('SELL')
                        tf_signal_strength += signal_weights.get('rsi', 1.0) * (rsi - 70) / 30
                
                # MACD signals with weighting
                macd = indicators.iloc[tf_index]['macd']
                macd_signal = indicators.iloc[tf_index]['macd_signal']
                if tf_index > 0 and not pd.isna(macd) and not pd.isna(macd_signal):
                    macd_prev = indicators.iloc[tf_index-1]['macd']
                    signal_prev = indicators.iloc[tf_index-1]['macd_signal']
                    
                    if macd > macd_signal and macd_prev <= signal_prev:
                        tf_signals.append('BUY')
                        tf_signal_strength += signal_weights.get('macd', 1.0)
                    elif macd < macd_signal and macd_prev >= signal_prev:
                        tf_signals.append('SELL')
                        tf_signal_strength += signal_weights.get('macd', 1.0)
                
                # Moving average signals with weighting
                price = indicators.iloc[tf_index]['close']
                sma20 = indicators.iloc[tf_index]['sma_20']
                sma50 = indicators.iloc[tf_index]['sma_50']
                
                if not pd.isna(sma20) and not pd.isna(sma50):
                    if price > sma20 > sma50:
                        tf_signals.append('BUY')
                        tf_signal_strength += signal_weights.get('ma', 1.0)
                    elif price < sma20 < sma50:
                        tf_signals.append('SELL')
                        tf_signal_strength += signal_weights.get('ma', 1.0)
                
                # Bollinger Band signals with weighting
                bb_upper = indicators.iloc[tf_index]['bb_upper']
                bb_lower = indicators.iloc[tf_index]['bb_lower']
                
                if not pd.isna(bb_upper) and not pd.isna(bb_lower):
                    if price <= bb_lower:
                        tf_signals.append('BUY')
                        tf_signal_strength += signal_weights.get('bb', 1.0)
                    elif price >= bb_upper:
                        tf_signals.append('SELL')
                        tf_signal_strength += signal_weights.get('bb', 1.0)
                
                # Stochastic signals with weighting
                if 'stoch_k' in indicators.columns and 'stoch_d' in indicators.columns:
                    stoch_k = indicators.iloc[tf_index]['stoch_k']
                    stoch_d = indicators.iloc[tf_index]['stoch_d']
                    
                    if not pd.isna(stoch_k) and not pd.isna(stoch_d):
                        if stoch_k < 20 and stoch_d < 20:
                            tf_signals.append('BUY')
                            tf_signal_strength += signal_weights.get('stoch', 0.8)
                        elif stoch_k > 80 and stoch_d > 80:
                            tf_signals.append('SELL')
                            tf_signal_strength += signal_weights.get('stoch', 0.8)
                
                # Volume confirmation with weighting
                if 'volume_ratio' in indicators.columns:
                    volume_ratio = indicators.iloc[tf_index]['volume_ratio']
                    if not pd.isna(volume_ratio) and volume_ratio > 1.5:
                        # High volume supports the signal
                        tf_signal_strength += signal_weights.get('volume', 0.6)
                
            except Exception as e:
                self.logger.debug(f"Signal calculation error for {tf}: {e}")
                continue
            
            # Count signals for this timeframe
            buy_signals = tf_signals.count('BUY')
            sell_signals = tf_signals.count('SELL')
            
            if buy_signals > sell_signals and tf_signal_strength > 1.0:
                signals['BUY'] += 1
                confirming_tfs.append(f"{tf}_BUY")
                signal_details.append({
                    'timeframe': tf,
                    'direction': 'BUY',
                    'strength': tf_signal_strength,
                    'signals': tf_signals
                })
            elif sell_signals > buy_signals and tf_signal_strength > 1.0:
                signals['SELL'] += 1
                confirming_tfs.append(f"{tf}_SELL")
                signal_details.append({
                    'timeframe': tf,
                    'direction': 'SELL',
                    'strength': tf_signal_strength,
                    'signals': tf_signals
                })
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
            'signal_breakdown': signals,
            'signal_details': signal_details
        }
    
    def _check_strategy_specific_entry(self, strategy_type: str, tf_indicators: Dict, 
                                     timeframes: List[str], index: int, confluence_signals: Dict) -> Dict:
        """Check strategy-specific entry conditions"""
        
        should_enter = True
        entry_signals = []
        stop_loss = None
        take_profit = None
        
        primary_tf = min(timeframes, key=lambda x: self._timeframe_to_minutes(x))
        primary_indicators = tf_indicators[primary_tf]
        
        if index >= len(primary_indicators):
            return {'should_enter': False, 'signals': []}
        
        try:
            current_price = primary_indicators.iloc[index]['close']
            
            # Strategy-specific conditions
            if 'scalping' in strategy_type:
                # Scalping requires high confidence and volume
                if confluence_signals['confidence'] < 70:
                    should_enter = False
                
                # Tight stops for scalping
                atr = primary_indicators.iloc[index].get('atr', current_price * 0.01)
                stop_loss = current_price - (atr * 1.5) if confluence_signals['direction'] == 'BUY' else current_price + (atr * 1.5)
                take_profit = current_price + (atr * 2.0) if confluence_signals['direction'] == 'BUY' else current_price - (atr * 2.0)
                
            elif 'swing' in strategy_type:
                # Swing trading requires trend confirmation
                sma20 = primary_indicators.iloc[index].get('sma_20')
                sma50 = primary_indicators.iloc[index].get('sma_50')
                
                if confluence_signals['direction'] == 'BUY' and not (current_price > sma20 > sma50):
                    should_enter = False
                elif confluence_signals['direction'] == 'SELL' and not (current_price < sma20 < sma50):
                    should_enter = False
                
                # Wider stops for swing trading
                atr = primary_indicators.iloc[index].get('atr', current_price * 0.02)
                stop_loss = current_price - (atr * 2.5) if confluence_signals['direction'] == 'BUY' else current_price + (atr * 2.5)
                take_profit = current_price + (atr * 4.0) if confluence_signals['direction'] == 'BUY' else current_price - (atr * 4.0)
                
            elif 'trend' in strategy_type:
                # Trend following requires momentum
                macd = primary_indicators.iloc[index].get('macd')
                macd_signal = primary_indicators.iloc[index].get('macd_signal')
                
                if confluence_signals['direction'] == 'BUY' and not (macd > macd_signal):
                    should_enter = False
                elif confluence_signals['direction'] == 'SELL' and not (macd < macd_signal):
                    should_enter = False
                
            elif 'position' in strategy_type:
                # Position trading requires strong fundamentals
                if confluence_signals['confidence'] < 60:
                    should_enter = False
                
                # Very wide stops for position trading
                atr = primary_indicators.iloc[index].get('atr', current_price * 0.03)
                stop_loss = current_price - (atr * 4.0) if confluence_signals['direction'] == 'BUY' else current_price + (atr * 4.0)
                take_profit = current_price + (atr * 8.0) if confluence_signals['direction'] == 'BUY' else current_price - (atr * 8.0)
            
            # Additional risk management checks
            rsi = primary_indicators.iloc[index].get('rsi')
            if rsi:
                if confluence_signals['direction'] == 'BUY' and rsi > 80:
                    should_enter = False  # Overbought
                elif confluence_signals['direction'] == 'SELL' and rsi < 20:
                    should_enter = False  # Oversold
            
        except Exception as e:
            self.logger.debug(f"Entry condition check failed: {e}")
            should_enter = False
        
        return {
            'should_enter': should_enter,
            'signals': entry_signals,
            'stop_loss': stop_loss,
            'take_profit': take_profit
        }
    
    def _get_enhanced_exit_signal(self, tf_indicators: Dict, timeframes: List[str], 
                                index: int, position: Dict, primary_tf: str, strategy_type: str) -> Optional[Dict]:
        """Get enhanced exit signal based on multiple conditions"""
        
        primary_indicators = tf_indicators[primary_tf]
        
        if index >= len(primary_indicators):
            return {'reason': 'data_end', 'signals': []}
        
        current_price = primary_indicators.iloc[index]['close']
        entry_price = position['entry_price']
        hold_duration = index - position.get('entry_index', index)
        
        # Strategy-specific exit conditions
        max_hold_periods = {
            'scalping_ultra': 20,
            'scalping_standard': 50,
            'short_term_momentum': 100,
            'intraday_trend': 200,
            'intraday_swing': 300,
            'swing_trading': 500,
            'daily_swing': 1000,
            'multi_day_position': 2000,
            'weekly_swing': 5000,
            'position_trading': 10000,
            'weekly_trend': 7000,
            'long_term_trend': 20000
        }
        
        max_hold = max_hold_periods.get(strategy_type, 500)
        
        # Time-based exit
        if hold_duration > max_hold:
            return {'reason': 'max_hold_time', 'signals': ['TIME_EXIT']}
        
        # Stop loss check
        if position.get('stop_loss'):
            if position['side'] == 'BUY' and current_price <= position['stop_loss']:
                return {'reason': 'stop_loss', 'signals': ['STOP_LOSS']}
            elif position['side'] == 'SELL' and current_price >= position['stop_loss']:
                return {'reason': 'stop_loss', 'signals': ['STOP_LOSS']}
        
        # Take profit check
        if position.get('take_profit'):
            if position['side'] == 'BUY' and current_price >= position['take_profit']:
                return {'reason': 'take_profit', 'signals': ['TAKE_PROFIT']}
            elif position['side'] == 'SELL' and current_price <= position['take_profit']:
                return {'reason': 'take_profit', 'signals': ['TAKE_PROFIT']}
        
        # RSI reversal check
        try:
            rsi = primary_indicators.iloc[index]['rsi']
            if not pd.isna(rsi):
                if position['side'] == 'BUY' and rsi > 80:
                    return {'reason': 'rsi_overbought', 'signals': ['RSI_EXIT']}
                elif position['side'] == 'SELL' and rsi < 20:
                    return {'reason': 'rsi_oversold', 'signals': ['RSI_EXIT']}
        except:
            pass
        
        # MACD reversal check
        try:
            macd = primary_indicators.iloc[index]['macd']
            macd_signal = primary_indicators.iloc[index]['macd_signal']
            
            if not pd.isna(macd) and not pd.isna(macd_signal):
                if position['side'] == 'BUY' and macd < macd_signal:
                    return {'reason': 'macd_reversal', 'signals': ['MACD_EXIT']}
                elif position['side'] == 'SELL' and macd > macd_signal:
                    return {'reason': 'macd_reversal', 'signals': ['MACD_EXIT']}
        except:
            pass
        
        # Trailing stop based on strategy
        try:
            atr = primary_indicators.iloc[index].get('atr', abs(current_price - entry_price) * 0.02)
            
            if 'scalping' in strategy_type:
                trailing_distance = atr * 1.0
            elif 'swing' in strategy_type or 'trend' in strategy_type:
                trailing_distance = atr * 2.0
            else:
                trailing_distance = atr * 1.5
            
            # Calculate unrealized P&L percentage
            if position['side'] == 'BUY':
                pnl_pct = (current_price - entry_price) / entry_price
                trailing_stop = current_price - trailing_distance
                
                if pnl_pct > 0.02 and current_price <= trailing_stop:  # 2% profit threshold
                    return {'reason': 'trailing_stop', 'signals': ['TRAILING_STOP']}
                    
            else:  # SELL position
                pnl_pct = (entry_price - current_price) / entry_price
                trailing_stop = current_price + trailing_distance
                
                if pnl_pct > 0.02 and current_price >= trailing_stop:  # 2% profit threshold
                    return {'reason': 'trailing_stop', 'signals': ['TRAILING_STOP']}
        except:
            pass
        
        return None
    
    def calculate_all_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """UPGRADED: Calculate comprehensive technical indicators with error handling"""
        
        df = df.copy()
        
        try:
            # Moving averages
            df['sma_10'] = df['close'].rolling(10).mean()
            df['sma_20'] = df['close'].rolling(20).mean()
            df['sma_50'] = df['close'].rolling(50).mean()
            df['sma_200'] = df['close'].rolling(200).mean()
            
            df['ema_12'] = df['close'].ewm(span=12).mean()
            df['ema_26'] = df['close'].ewm(span=26).mean()
            df['ema_50'] = df['close'].ewm(span=50).mean()
            
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
            df['bb_width'] = df['bb_upper'] - df['bb_lower']
            
            # Stochastic
            low_14 = df['low'].rolling(14).min()
            high_14 = df['high'].rolling(14).max()
            df['stoch_k'] = 100 * ((df['close'] - low_14) / (high_14 - low_14))
            df['stoch_d'] = df['stoch_k'].rolling(3).mean()
            
            # ATR (Average True Range)
            high_low = df['high'] - df['low']
            high_close = np.abs(df['high'] - df['close'].shift())
            low_close = np.abs(df['low'] - df['close'].shift())
            ranges = np.maximum(high_low, np.maximum(high_close, low_close))
            df['atr'] = ranges.rolling(14).mean()
            
            # Volume indicators
            if 'volume' in df.columns:
                df['volume_sma'] = df['volume'].rolling(20).mean()
                df['volume_ratio'] = df['volume'] / df['volume_sma']
                
                # Volume-weighted average price
                df['vwap'] = (df['close'] * df['volume']).cumsum() / df['volume'].cumsum()
            else:
                df['volume_sma'] = 0
                df['volume_ratio'] = 1
                df['vwap'] = df['close']
            
            # Momentum indicators
            df['momentum'] = df['close'] / df['close'].shift(10) - 1
            df['rate_of_change'] = df['close'].pct_change(10) * 100
            
            # Support and resistance levels
            df['resistance'] = df['high'].rolling(20).max()
            df['support'] = df['low'].rolling(20).min()
            
        except Exception as e:
            self.logger.error(f"Indicator calculation failed: {e}")
        
        return df
    
    def _is_valid_strategy_result(self, result: Dict) -> bool:
        """UPGRADED: Enhanced validation for strategy results"""
        if not result:
            return False
        
        # Comprehensive quality checks using thresholds
        checks = [
            result.get('total_trades', 0) >= self.quality_thresholds['min_trades'],
            result.get('win_rate', 0) >= self.quality_thresholds['min_win_rate'],
            result.get('sharpe_ratio', 0) >= self.quality_thresholds['min_sharpe_ratio'],
            result.get('max_drawdown', 100) <= self.quality_thresholds['max_drawdown'],
            result.get('profit_factor', 0) >= self.quality_thresholds['min_profit_factor'],
            result.get('total_return', 0) >= self.quality_thresholds['min_total_return']
        ]
        
        # Additional sanity checks
        total_return = result.get('total_return', 0)
        if total_return > 1000 or total_return < -50:  # Unrealistic returns
            return False
        
        win_rate = result.get('win_rate', 0)
        if win_rate > 95 or win_rate < 20:  # Unrealistic win rates
            return False
        
        # Must pass at least 4 out of 6 quality checks
        return sum(checks) >= 4
    
    def _calculate_enhanced_performance_metrics(self, trades: List[Dict], final_balance: float, 
                                              strategy_type: str, symbol: str, timeframes: List[str]) -> Dict:
        """UPGRADED: Calculate comprehensive performance metrics"""
        
        if not trades:
            return None
        
        returns = [t['return_pct'] for t in trades]
        winning_trades = [t for t in trades if t['return_pct'] > 0]
        losing_trades = [t for t in trades if t['return_pct'] <= 0]
        
        # Basic metrics
        total_return = ((final_balance - 10000) / 10000) * 100
        win_rate = (len(winning_trades) / len(trades)) * 100
        avg_return = np.mean(returns) * 100
        
        # Risk metrics
        returns_array = np.array(returns)
        std_dev = np.std(returns_array) if len(returns_array) > 1 else 0
        
        # Sharpe ratio
        risk_free_rate = 0.02 / 252  # Assume 2% annual risk-free rate
        excess_returns = returns_array - risk_free_rate
        sharpe_ratio = np.mean(excess_returns) / np.std(excess_returns) if np.std(excess_returns) > 0 else 0
        
        # Sortino ratio (downside deviation)
        downside_returns = returns_array[returns_array < 0]
        downside_std = np.std(downside_returns) if len(downside_returns) > 0 else 0
        sortino_ratio = np.mean(excess_returns) / downside_std if downside_std > 0 else 0
        
        # Maximum drawdown calculation
        cumulative_returns = np.cumprod([1 + r for r in returns])
        rolling_max = np.maximum.accumulate(cumulative_returns)
        drawdowns = (rolling_max - cumulative_returns) / rolling_max
        max_drawdown = np.max(drawdowns) * 100
        
        # Advanced metrics
        gross_profit = sum(t['return_pct'] for t in winning_trades)
        gross_loss = abs(sum(t['return_pct'] for t in losing_trades))
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')
        
        avg_win = np.mean([t['return_pct'] for t in winning_trades]) if winning_trades else 0
        avg_loss = np.mean([t['return_pct'] for t in losing_trades]) if losing_trades else 0
        payoff_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else float('inf')
        
        # Recovery factor
        recovery_factor = total_return / max_drawdown if max_drawdown > 0 else float('inf')
        
        # Risk-adjusted return
        risk_adjusted_return = total_return / (std_dev * 100) if std_dev > 0 else total_return
        
        # Confluence strength (average number of confirming timeframes)
        confluence_strengths = []
        for trade in trades:
            if 'timeframes_confirming' in trade and trade['timeframes_confirming']:
                confluence_strengths.append(len(trade['timeframes_confirming']))
        
        avg_confluence_strength = np.mean(confluence_strengths) if confluence_strengths else 0
        
        # Trade duration analysis
        hold_durations = [t.get('hold_duration_candles', 0) for t in trades]
        avg_hold_duration = np.mean(hold_durations)
        
        # Annualized return (assume 252 trading days)
        total_days = len(trades) * avg_hold_duration / 1440  # Convert candles to days (assuming 1min candles)
        annualized_return = ((final_balance / 10000) ** (252 / max(total_days, 1)) - 1) * 100
        
        return {
            'symbol': symbol,
            'strategy_type': strategy_type,
            'timeframes': ','.join(timeframes),
            'total_trades': len(trades),
            'winning_trades': len(winning_trades),
            'losing_trades': len(losing_trades),
            'win_rate': win_rate,
            'total_return': total_return,
            'annualized_return': annualized_return,
            'avg_return_per_trade': avg_return,
            'max_drawdown': max_drawdown,
            'sharpe_ratio': sharpe_ratio,
            'sortino_ratio': sortino_ratio,
            'profit_factor': profit_factor,
            'payoff_ratio': payoff_ratio,
            'recovery_factor': recovery_factor,
            'volatility': std_dev * 100,
            'best_trade': max(returns) * 100,
            'worst_trade': min(returns) * 100,
            'avg_winning_trade': avg_win * 100,
            'avg_losing_trade': avg_loss * 100,
            'avg_trade_duration': avg_hold_duration,
            'confluence_strength': avg_confluence_strength,
            'risk_adjusted_return': risk_adjusted_return,
            'analysis_timestamp': datetime.now().isoformat(),
            'processing_time_seconds': time.time() - self.analysis_progress.start_time.timestamp() if self.analysis_progress.start_time else 0
        }
    
    async def _save_analysis_results(self, symbol: str, results: Dict):
        """UPGRADED: Save analysis results with comprehensive data"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                for strategy_type, result in results.items():
                    cursor.execute('''
                        INSERT INTO advanced_results (
                            symbol, strategy_type, timeframes, total_trades, winning_trades,
                            win_rate, total_return, annualized_return, max_drawdown, sharpe_ratio,
                            sortino_ratio, profit_factor, payoff_ratio, volatility, best_trade,
                            worst_trade, avg_trade_duration, confluence_strength, risk_adjusted_return,
                            analysis_timestamp, processing_time_seconds
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        symbol, strategy_type, result.get('timeframes', ''),
                        result['total_trades'], result['winning_trades'], result['win_rate'],
                        result['total_return'], result.get('annualized_return', 0), result['max_drawdown'],
                        result['sharpe_ratio'], result.get('sortino_ratio', 0), result.get('profit_factor', 0),
                        result.get('payoff_ratio', 0), result['volatility'], result['best_trade'],
                        result['worst_trade'], result.get('avg_trade_duration', 0),
                        result.get('confluence_strength', 0), result.get('risk_adjusted_return', 0),
                        result['analysis_timestamp'], result.get('processing_time_seconds', 0)
                    ))
                
                conn.commit()
                
        except Exception as e:
            self.logger.error(f"Failed to save analysis results for {symbol}: {e}")
    
    def get_analysis_progress(self) -> Dict:
        """UPGRADED: Get current analysis progress with detailed information"""
        with self._progress_lock:
            if not self.analysis_progress.start_time:
                return {
                    'status': 'not_started',
                    'progress_percent': 0,
                    'message': 'Analysis not started'
                }
            
            elapsed_time = datetime.now() - self.analysis_progress.start_time
            
            if self.total_combinations_to_analyze > 0:
                progress_pct = (self.combinations_completed / self.total_combinations_to_analyze) * 100
            else:
                progress_pct = 0
            
            # Calculate ETA
            eta_str = "Calculating..."
            if self.combinations_completed > 0 and progress_pct < 100:
                avg_time_per_combination = elapsed_time.total_seconds() / self.combinations_completed
                remaining_combinations = self.total_combinations_to_analyze - self.combinations_completed
                eta_seconds = remaining_combinations * avg_time_per_combination
                eta_time = datetime.now() + timedelta(seconds=eta_seconds)
                eta_str = eta_time.strftime("%Y-%m-%d %H:%M:%S")
            elif progress_pct >= 100:
                eta_str = "Completed"
            
            # Resource status
            resources = self.resource_monitor.check_resources()
            
            return {
                'status': 'running' if progress_pct < 100 else 'completed',
                'progress_percent': progress_pct,
                'combinations_completed': self.combinations_completed,
                'total_combinations': self.total_combinations_to_analyze,
                'successful_analyses': self.successful_analyses,
                'failed_analyses': self.failed_analyses,
                'current_symbol': self.current_analysis_symbol,
                'current_strategy': self.current_analysis_strategy,
                'elapsed_time_minutes': elapsed_time.total_seconds() / 60,
                'eta': eta_str,
                'memory_usage_mb': resources.get('memory_mb', 0),
                'cpu_usage_pct': resources.get('cpu_pct', 0),
                'resources_ok': resources.get('memory_ok', True) and resources.get('cpu_ok', True)
            }
    
    async def get_comprehensive_data(self, symbol: str, timeframe: str, limit: int = 1000):
        """Get comprehensive historical data - placeholder for actual implementation"""
        try:
            # This is a placeholder - in real implementation, this would fetch from Binance API
            # For now, return mock data structure
            dates = pd.date_range(end=datetime.now(), periods=limit, freq='1min')
            
            # Generate realistic price data
            base_price = random.uniform(100, 50000)
            price_data = []
            current_price = base_price
            
            for i in range(limit):
                change = random.uniform(-0.02, 0.02)
                current_price *= (1 + change)
                
                high = current_price * random.uniform(1.0, 1.01)
                low = current_price * random.uniform(0.99, 1.0)
                open_price = current_price * random.uniform(0.995, 1.005)
                volume = random.uniform(1000, 100000)
                
                price_data.append({
                    'timestamp': dates[i],
                    'open': open_price,
                    'high': high,
                    'low': low,
                    'close': current_price,
                    'volume': volume
                })
            
            df = pd.DataFrame(price_data)
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to get data for {symbol} {timeframe}: {e}")
            return None

# UPGRADED: Enhanced Strategy Discovery Engine
class StrategyDiscoveryEngine:
    """UPGRADED: Enhanced Strategy Discovery with genetic optimization and quality validation"""
    
    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.StrategyDiscovery")
        
        # UPGRADED: Enhanced strategy templates
        self.strategy_templates = [
            self.momentum_strategy,
            self.mean_reversion_strategy, 
            self.breakout_strategy,
            self.trend_following_strategy,
            self.volatility_strategy,
            # New strategy templates
            self.confluence_momentum_strategy,
            self.multi_timeframe_trend_strategy,
            self.adaptive_volatility_strategy,
            self.range_bound_strategy,
            self.momentum_reversal_strategy
        ]
        
        # UPGRADED: Enhanced genetic algorithm parameters
        self.population_size = 100
        self.generations = 200
        self.elite_size = 20
        self.mutation_rate = 0.15
        self.crossover_rate = 0.8
        
        # UPGRADED: Enhanced quality thresholds
        self.min_fitness_threshold = 2.0
        self.min_trades_threshold = 30
        self.min_win_rate_threshold = 58
        self.max_drawdown_threshold = 20.0
        self.min_profit_factor = 1.3
        
        # Resource management
        self.resource_monitor = ResourceMonitor(max_memory_mb=1000, max_cpu_pct=85.0)
        
        # Progress tracking
        self.discovery_progress = {
            'status': 'not_started',
            'current_symbol': '',
            'current_timeframe': '',
            'strategies_discovered': 0,
            'strategies_tested': 0
        }
        
    async def discover_optimal_strategies(self, symbol: str, timeframe_data: dict):
        """UPGRADED: Enhanced strategy discovery with parallel processing and quality validation"""
        
        self.logger.info(f"Enhanced strategy discovery for {symbol}")
        
        discovered_strategies = {}
        strategy_discovery_start = time.time()
        
        # Update progress
        self.discovery_progress.update({
            'status': 'running',
            'current_symbol': symbol,
            'strategies_tested': 0,
            'strategies_discovered': 0
        })
        
        # Process timeframes with sufficient data
        valid_timeframes = {tf: data for tf, data in timeframe_data.items() if len(data) >= 2000}
        
        if not valid_timeframes:
            self.logger.warning(f"No sufficient data for strategy discovery: {symbol}")
            return discovered_strategies
        
        # Process each timeframe
        for tf, data in valid_timeframes.items():
            self.discovery_progress['current_timeframe'] = tf
            
            self.logger.info(f"Optimizing strategies for {symbol} {tf} ({len(data)} candles)")
            
            # Resource management
            await self.resource_monitor.ensure_resources_available()
            
            # Test each strategy template with genetic optimization
            for strategy_idx, strategy_func in enumerate(self.strategy_templates):
                
                # Resource check every few strategies
                if strategy_idx % 3 == 0:
                    await self.resource_monitor.ensure_resources_available()
                
                strategy_start = time.time()
                
                try:
                    # Enhanced genetic optimization
                    best_params = await self.enhanced_genetic_optimize_strategy(
                        strategy_func, data, tf, symbol
                    )
                    
                    strategy_time = time.time() - strategy_start
                    self.discovery_progress['strategies_tested'] += 1
                    
                    # Enhanced validation
                    if self._validate_discovered_strategy(best_params):
                        strategy_name = f"{strategy_func.__name__}_{tf}_{symbol}"
                        discovered_strategies[strategy_name] = best_params
                        self.discovery_progress['strategies_discovered'] += 1
                        
                        # Save to database
                        await self._save_discovered_strategy(symbol, tf, strategy_func.__name__, best_params)
                        
                        self.logger.info(f"? {strategy_func.__name__}: "
                                       f"Fitness={best_params['fitness']:.3f}, "
                                       f"Trades={best_params.get('total_trades', 0)}, "
                                       f"Win Rate={best_params.get('win_rate', 0):.1f}% "
                                       f"({strategy_time:.1f}s)")
                    else:
                        self.logger.debug(f"? {strategy_func.__name__}: Below quality threshold")
                
                except Exception as e:
                    self.logger.error(f"Strategy optimization failed for {strategy_func.__name__}: {e}")
                
                # Throttling between strategies
                await asyncio.sleep(0.5)
        
        discovery_time = time.time() - strategy_discovery_start
        self.discovery_progress['status'] = 'completed'
        
        self.logger.info(f"Strategy discovery complete for {symbol}: "
                        f"{len(discovered_strategies)} strategies in {discovery_time:.1f}s")
        
        return discovered_strategies
    
    async def enhanced_genetic_optimize_strategy(self, strategy_func, data: pd.DataFrame, 
                                               timeframe: str, symbol: str):
        """UPGRADED: Enhanced genetic algorithm optimization with better performance"""
        
        # Generate initial population with diverse parameters
        population = []
        for _ in range(self.population_size):
            params = self.generate_random_params(strategy_func)
            fitness = await self.evaluate_strategy(strategy_func, params, data)
            population.append({'params': params, 'fitness': fitness})
        
        # Sort by fitness
        population.sort(key=lambda x: x['fitness'], reverse=True)
        
        best_fitness_history = []
        
        # Evolution loop with enhanced operations
        for generation in range(self.generations):
            new_population = []
            
            # Elite preservation
            elite = population[:self.elite_size]
            new_population.extend(elite)
            
            # Generate new offspring
            while len(new_population) < self.population_size:
                # Tournament selection
                parent1 = self._tournament_selection(population, tournament_size=5)
                parent2 = self._tournament_selection(population, tournament_size=5)
                
                # Crossover
                if random.random() < self.crossover_rate:
                    child_params = self._crossover(parent1['params'], parent2['params'])
                else:
                    child_params = parent1['params'].copy()
                
                # Mutation
                if random.random() < self.mutation_rate:
                    child_params = self._mutate(child_params, strategy_func)
                
                # Evaluate child
                fitness = await self.evaluate_strategy(strategy_func, child_params, data)
                new_population.append({'params': child_params, 'fitness': fitness})
                
                # Resource management during evolution
                if len(new_population) % 10 == 0:
                    resources = self.resource_monitor.check_resources()
                    if resources['should_pause']:
                        await asyncio.sleep(1)
            
            population = new_population
            population.sort(key=lambda x: x['fitness'], reverse=True)
            
            # Track best fitness
            best_fitness = population[0]['fitness']
            best_fitness_history.append(best_fitness)
            
            # Early stopping if no improvement
            if generation > 50 and len(set(best_fitness_history[-20:])) == 1:
                self.logger.debug(f"Early stopping at generation {generation} due to convergence")
                break
            
            # Progress logging
            if generation % 40 == 0:
                self.logger.debug(f"Gen {generation}: Best fitness = {best_fitness:.3f}")
        
        best_individual = population[0]
        
        # Add additional metrics to the best individual
        best_individual.update({
            'symbol': symbol,
            'timeframe': timeframe,
            'strategy_name': strategy_func.__name__,
            'generations_run': generation + 1,
            'final_population_diversity': self._calculate_population_diversity(population)
        })
        
        return best_individual
    
    def _tournament_selection(self, population: List[Dict], tournament_size: int = 5) -> Dict:
        """Tournament selection for genetic algorithm"""
        tournament = random.sample(population, min(tournament_size, len(population)))
        return max(tournament, key=lambda x: x['fitness'])
    
    def _crossover(self, params1: Dict, params2: Dict) -> Dict:
        """Crossover operation for genetic algorithm"""
        child_params = {}
        
        for key in params1.keys():
            if random.random() < 0.5:
                child_params[key] = params1[key]
            else:
                child_params[key] = params2[key]
        
        return child_params
    
    def _mutate(self, params: Dict, strategy_func) -> Dict:
        """Mutation operation for genetic algorithm"""
        mutated_params = params.copy()
        
        # Mutate a random subset of parameters
        keys_to_mutate = random.sample(list(params.keys()), max(1, len(params) // 3))
        
        for key in keys_to_mutate:
            if isinstance(params[key], (int, float)):
                # Add gaussian noise
                noise = random.gauss(0, abs(params[key]) * 0.1)
                mutated_params[key] = max(0.01, params[key] + noise)
            elif isinstance(params[key], bool):
                # Flip boolean with small probability
                if random.random() < 0.1:
                    mutated_params[key] = not params[key]
        
        return mutated_params
    
    def _calculate_population_diversity(self, population: List[Dict]) -> float:
        """Calculate diversity of the population"""
        if len(population) < 2:
            return 0.0
        
        # Simple diversity measure based on fitness variance
        fitnesses = [ind['fitness'] for ind in population]
        return np.std(fitnesses) / (np.mean(fitnesses) + 1e-8)
    
    async def evaluate_strategy(self, strategy_func, params: Dict, data: pd.DataFrame) -> float:
        """UPGRADED: Enhanced strategy evaluation with comprehensive metrics"""
        try:
            # Run strategy simulation
            strategy_result = strategy_func(data, params)
            
            if not strategy_result or 'signals' not in strategy_result:
                return 0.0
            
            # Simulate trading based on signals
            trades = self._simulate_trading(data, strategy_result['signals'])
            
            if len(trades) < 5:  # Need minimum trades
                return 0.0
            
            # Calculate comprehensive fitness score
            returns = [t['return_pct'] for t in trades]
            winning_trades = sum(1 for r in returns if r > 0)
            win_rate = (winning_trades / len(trades)) * 100
            
            # Risk metrics
            returns_array = np.array(returns)
            avg_return = np.mean(returns_array)
            std_return = np.std(returns_array)
            
            # Sharpe ratio
            sharpe_ratio = avg_return / std_return if std_return > 0 else 0
            
            # Maximum drawdown
            cumulative = np.cumprod([1 + r for r in returns])
            max_drawdown = ((np.maximum.accumulate(cumulative) - cumulative) / np.maximum.accumulate(cumulative)).max()
            
            # Profit factor
            gross_profit = sum(r for r in returns if r > 0)
            gross_loss = abs(sum(r for r in returns if r < 0))
            profit_factor = gross_profit / gross_loss if gross_loss > 0 else 10
            
            # Multi-objective fitness function
            fitness_components = {
                'sharpe_ratio': sharpe_ratio * 0.4,
                'win_rate_bonus': (win_rate / 100) * 0.2,
                'profit_factor_bonus': min(profit_factor / 3, 1.0) * 0.2,
                'drawdown_penalty': -max_drawdown * 0.1,
                'trade_count_bonus': min(len(trades) / 50, 1.0) * 0.1
            }
            
            total_fitness = sum(fitness_components.values())
            
            # Store additional metrics for validation
            params.update({
                'fitness': total_fitness,
                'total_trades': len(trades),
                'win_rate': win_rate,
                'sharpe_ratio': sharpe_ratio,
                'max_drawdown': max_drawdown * 100,
                'profit_factor': profit_factor,
                'total_return': (np.prod([1 + r for r in returns]) - 1) * 100,
                'avg_return_per_trade': avg_return * 100
            })
            
            return max(0.0, total_fitness)
            
        except Exception as e:
            self.logger.debug(f"Strategy evaluation failed: {e}")
            return 0.0
    
    def _simulate_trading(self, data: pd.DataFrame, signals: List[Dict]) -> List[Dict]:
        """Simulate trading based on strategy signals"""
        trades = []
        position = None
        balance = 10000
        
        for signal in signals:
            try:
                index = signal.get('index', 0)
                if index >= len(data):
                    continue
                
                current_price = data.iloc[index]['close']
                signal_type = signal.get('type', 'NONE')
                
                # Entry signals
                if not position and signal_type in ['BUY', 'SELL']:
                    position = {
                        'entry_price': current_price,
                        'entry_index': index,
                        'side': signal_type,
                        'confidence': signal.get('confidence', 50)
                    }
                
                # Exit signals
                elif position and (signal_type == 'EXIT' or signal_type != position['side']):
                    
                    if position['side'] == 'BUY':
                        return_pct = (current_price - position['entry_price']) / position['entry_price']
                    else:
                        return_pct = (position['entry_price'] - current_price) / position['entry_price']
                    
                    # Apply trading costs
                    return_pct -= 0.002
                    
                    balance *= (1 + return_pct)
                    
                    trades.append({
                        **position,
                        'exit_price': current_price,
                        'exit_index': index,
                        'return_pct': return_pct,
                        'hold_duration': index - position['entry_index']
                    })
                    
                    position = None
                    
            except Exception as e:
                continue
        
        return trades
    
    def _validate_discovered_strategy(self, strategy_result: Dict) -> bool:
        """UPGRADED: Enhanced validation for discovered strategies"""
        if not strategy_result:
            return False
        
        # Comprehensive quality checks
        fitness = strategy_result.get('fitness', 0)
        total_trades = strategy_result.get('total_trades', 0)
        win_rate = strategy_result.get('win_rate', 0)
        max_drawdown = strategy_result.get('max_drawdown', 100)
        profit_factor = strategy_result.get('profit_factor', 0)
        total_return = strategy_result.get('total_return', 0)
        sharpe_ratio = strategy_result.get('sharpe_ratio', 0)
        
        # Quality thresholds validation
        checks = [
            fitness >= self.min_fitness_threshold,
            total_trades >= self.min_trades_threshold,
            win_rate >= self.min_win_rate_threshold,
            max_drawdown <= self.max_drawdown_threshold,
            profit_factor >= self.min_profit_factor,
            total_return >= 5.0,  # Minimum 5% return
            sharpe_ratio >= 0.5   # Minimum Sharpe ratio
        ]
        
        # Additional sanity checks
        if total_return > 500 or total_return < -30:  # Unrealistic returns
            return False
        
        if win_rate > 90 or win_rate < 30:  # Unrealistic win rates
            return False
        
        if sharpe_ratio > 5.0:  # Suspiciously high Sharpe ratio
            return False
        
        # Must pass at least 5 out of 7 quality checks
        return sum(checks) >= 5
    
    async def _save_discovered_strategy(self, symbol: str, timeframe: str, strategy_name: str, params: Dict):
        """Save discovered strategy to database"""
        try:
            db_path = Path('data/advanced_backtesting_results.db')
            
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO strategy_discovery_results (
                        symbol, timeframe, strategy_name, parameters, fitness_score,
                        total_trades, win_rate, total_return, max_drawdown, sharpe_ratio,
                        discovered_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    symbol, timeframe, strategy_name, json.dumps(params),
                    params.get('fitness', 0), params.get('total_trades', 0),
                    params.get('win_rate', 0), params.get('total_return', 0),
                    params.get('max_drawdown', 0), params.get('sharpe_ratio', 0),
                    datetime.now().isoformat()
                ))
                
        except Exception as e:
            self.logger.error(f"Failed to save discovered strategy: {e}")
    
    def generate_random_params(self, strategy_func) -> Dict:
        """Generate random parameters for a strategy function"""
        # Base parameter ranges
        base_params = {
            'rsi_period': random.randint(10, 20),
            'rsi_overbought': random.randint(70, 85),
            'rsi_oversold': random.randint(15, 30),
            'sma_fast': random.randint(5, 15),
            'sma_slow': random.randint(20, 50),
            'bb_period': random.randint(15, 25),
            'bb_std': random.uniform(1.5, 2.5),
            'atr_period': random.randint(10, 20),
            'atr_multiplier': random.uniform(1.0, 3.0),
            'volume_threshold': random.uniform(1.2, 2.0),
            'momentum_period': random.randint(8, 16)
        }
        
        # Strategy-specific parameter adjustments
        if 'momentum' in strategy_func.__name__:
            base_params.update({
                'momentum_threshold': random.uniform(0.02, 0.08),
                'momentum_confirmation': random.choice([True, False])
            })
        elif 'reversal' in strategy_func.__name__:
            base_params.update({
                'reversal_threshold': random.uniform(0.15, 0.35),
                'confirmation_candles': random.randint(2, 5)
            })
        elif 'volatility' in strategy_func.__name__:
            base_params.update({
                'volatility_threshold': random.uniform(0.01, 0.05),
                'adaptive_period': random.randint(15, 30)
            })
        
        return base_params
    
    # Strategy template implementations (placeholders)
    def momentum_strategy(self, data: pd.DataFrame, params: Dict) -> Dict:
        """Momentum strategy implementation"""
        signals = []
        # Implementation would go here
        return {'name': 'momentum', 'signals': signals}
    
    def mean_reversion_strategy(self, data: pd.DataFrame, params: Dict) -> Dict:
        """Mean reversion strategy implementation"""
        signals = []
        # Implementation would go here
        return {'name': 'mean_reversion', 'signals': signals}
    
    def breakout_strategy(self, data: pd.DataFrame, params: Dict) -> Dict:
        """Breakout strategy implementation"""
        signals = []
        # Implementation would go here
        return {'name': 'breakout', 'signals': signals}
    
    def trend_following_strategy(self, data: pd.DataFrame, params: Dict) -> Dict:
        """Trend following strategy implementation"""
        signals = []
        # Implementation would go here
        return {'name': 'trend_following', 'signals': signals}
    
    def volatility_strategy(self, data: pd.DataFrame, params: Dict) -> Dict:
        """Volatility strategy implementation"""
        signals = []
        # Implementation would go here
        return {'name': 'volatility', 'signals': signals}
    
    def confluence_momentum_strategy(self, data: pd.DataFrame, params: Dict) -> Dict:
        """Multi-timeframe confluence momentum strategy"""
        signals = []
        # Implementation would go here
        return {'name': 'confluence_momentum', 'signals': signals}
    
    def multi_timeframe_trend_strategy(self, data: pd.DataFrame, params: Dict) -> Dict:
        """Multi-timeframe trend following strategy"""
        signals = []
        # Implementation would go here
        return {'name': 'mtf_trend', 'signals': signals}
    
    def adaptive_volatility_strategy(self, data: pd.DataFrame, params: Dict) -> Dict:
        """Volatility-adaptive strategy"""
        signals = []
        # Implementation would go here
        return {'name': 'adaptive_volatility', 'signals': signals}
    
    def range_bound_strategy(self, data: pd.DataFrame, params: Dict) -> Dict:
        """Range-bound market strategy"""
        signals = []
        # Implementation would go here
        return {'name': 'range_bound', 'signals': signals}
    
    def momentum_reversal_strategy(self, data: pd.DataFrame, params: Dict) -> Dict:
        """Momentum reversal strategy"""
        signals = []
        # Implementation would go here
        return {'name': 'momentum_reversal', 'signals': signals}