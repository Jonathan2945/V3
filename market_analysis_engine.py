#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V3 Market Analysis Engine - PERFORMANCE OPTIMIZED & REAL DATA ONLY
================================================================

V3 CRITICAL REQUIREMENTS:
- ThreadPoolExecutor optimized for 8 vCPU server specs
- Bounded collections to prevent memory leaks
- ZERO MOCK DATA - 100% REAL MARKET DATA ONLY
- Real data validation patterns
- UTF-8 compliance

PERFORMANCE FIXES APPLIED:
- ThreadPoolExecutor: max_workers optimized for 8 vCPU (was 16, now 4-6)
- Bounded deque: maxlen limits added to prevent memory leaks
- Memory management: Automatic cleanup and limits
- Resource monitoring: CPU and memory usage tracking
- Real data validation: V3 compliance enforcement
"""

import asyncio
import time
import logging
import threading
import sqlite3
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict, deque
from functools import lru_cache, wraps
import concurrent.futures
import hashlib
import psutil
import queue
from dataclasses import dataclass
import pickle
import os

# V3 Real Data Enforcement
EMOJI = "[V3-MARKET-ANALYSIS]"

class V3RealDataValidator:
    """V3 Real Data Validation for Market Analysis - CRITICAL FOR PRODUCTION"""
    
    @staticmethod
    def validate_market_analysis_data(data: Any, data_type: str, source: str = "unknown") -> bool:
        """
        Validate that market analysis data is REAL ONLY
        
        V3 CRITICAL: Ensures no mock data in market analysis
        """
        try:
            if data is None:
                logging.error(f"[V3-DATA-VIOLATION] Null data for {data_type} from {source}")
                return False
            
            # Validate DataFrame structure for market data
            if isinstance(data, pd.DataFrame):
                required_columns = ['close', 'high', 'low', 'volume']
                missing_columns = [col for col in required_columns if col not in data.columns]
                
                if missing_columns:
                    logging.error(f"[V3-DATA-VIOLATION] Missing columns {missing_columns} in {data_type}")
                    return False
                
                # Check for realistic market data patterns
                if len(data) == 0:
                    logging.error(f"[V3-DATA-VIOLATION] Empty dataset for {data_type}")
                    return False
                
                # Validate price relationships (High >= Close >= Low)
                if not all(data['high'] >= data['close']) or not all(data['close'] >= data['low']):
                    logging.error(f"[V3-DATA-VIOLATION] Invalid OHLC relationships in {data_type}")
                    return False
                
                # Check for mock data patterns (too perfect or repetitive)
                if len(data) >= 10:
                    recent_closes = data['close'].tail(10).values
                    if len(set(recent_closes)) < 3:  # Too little variation
                        logging.warning(f"[V3-DATA-SUSPICIOUS] Low variation in {data_type} - possible mock data")
                        return False
            
            # Validate numeric arrays
            elif isinstance(data, (list, np.ndarray)):
                data_array = np.array(data)
                if np.any(np.isnan(data_array)) or np.any(np.isinf(data_array)):
                    logging.error(f"[V3-DATA-VIOLATION] Invalid numeric values in {data_type}")
                    return False
                
                if len(data_array) > 0 and np.all(data_array <= 0):
                    logging.error(f"[V3-DATA-VIOLATION] Non-positive values in {data_type}")
                    return False
            
            logging.debug(f"[V3-DATA-VALIDATED] Real market data validation PASSED for {data_type}")
            return True
            
        except Exception as e:
            logging.error(f"[V3-DATA-VIOLATION] Validation error for {data_type}: {e}")
            return False
    
    @staticmethod
    def validate_analysis_components(component, component_name: str) -> bool:
        """Validate that analysis components are real (no mocks)"""
        if hasattr(component, 'is_mock') and component.is_mock:
            logging.error(f"[V3-MOCK-VIOLATION] Mock component detected in analysis: {component_name}")
            return False
        
        if 'Mock' in str(type(component)):
            logging.error(f"[V3-MOCK-VIOLATION] Mock class detected in analysis: {component_name}")
            return False
        
        return True

class OptimizedDatabaseConnectionPool:
    """
    V3 Optimized Database Connection Pool for 8 vCPU / 24GB Server
    
    PERFORMANCE OPTIMIZATIONS:
    - Connection count optimized for server specs
    - Bounded queues to prevent memory leaks
    - Automatic resource cleanup
    """
    
    def __init__(self, db_path: str, max_connections: int = None):
        # V3 AUTO-OPTIMIZE: Calculate optimal connections for 8 vCPU server
        if max_connections is None:
            cpu_count = psutil.cpu_count()
            # Conservative: 2-3 connections per CPU for database operations
            max_connections = min(cpu_count * 3, 20)  # Max 20 for safety
        
        self.db_path = db_path
        self.max_connections = max_connections
        # V3 BOUNDED QUEUE: Prevent memory leaks with maxsize
        self.connections = queue.Queue(maxsize=max_connections)
        self.total_connections = 0
        self.lock = threading.Lock()
        
        # V3 BOUNDED STATS: Limit memory usage
        self.pool_stats = {
            'connections_created': 0,
            'connections_reused': 0,
            'connections_closed': 0,
            'active_connections': 0,
            'wait_time_total': 0.0,
            'avg_wait_time': 0.0,
            'max_connections': max_connections,
            'memory_usage_mb': 0.0
        }
        
        self._initialize_pool()
        logging.info(f"[V3-DB-POOL] Initialized with {max_connections} max connections for 8 vCPU server")
    
    def _initialize_pool(self):
        """Initialize pool with conservative connection count"""
        try:
            # V3 CONSERVATIVE START: Start with fewer connections, scale as needed
            initial_connections = min(3, self.max_connections)
            
            for _ in range(initial_connections):
                conn = self._create_optimized_connection()
                if conn:
                    self.connections.put(conn)
                    self.total_connections += 1
                    self.pool_stats['connections_created'] += 1
            
            logging.info(f"[V3-DB-INIT] Database pool initialized with {self.total_connections} connections")
            
        except Exception as e:
            logging.error(f"[V3-DB-ERROR] Database pool initialization error: {e}")
    
    def _create_optimized_connection(self) -> Optional[sqlite3.Connection]:
        """Create V3 optimized database connection"""
        try:
            conn = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=30.0,  # Reduced timeout for faster failure
                isolation_level=None  # Enable autocommit mode
            )
            
            # V3 OPTIMIZATIONS: Tuned for market analysis workloads on 8 vCPU
            optimizations = [
                'PRAGMA journal_mode=WAL',
                'PRAGMA synchronous=NORMAL',
                'PRAGMA cache_size=15000',  # Optimized for 24GB RAM
                'PRAGMA temp_store=MEMORY',
                'PRAGMA mmap_size=268435456',  # 256MB mmap (conservative)
                'PRAGMA page_size=4096',  # Standard page size for stability
                'PRAGMA optimize'
            ]
            
            for pragma in optimizations:
                conn.execute(pragma)
            
            self._create_analysis_tables(conn)
            return conn
            
        except Exception as e:
            logging.error(f"[V3-DB-ERROR] Connection creation error: {e}")
            return None
    
    def _create_analysis_tables(self, conn: sqlite3.Connection):
        """Create optimized analysis tables with proper indexes"""
        try:
            # V3 OPTIMIZED TABLES: Minimal but efficient schema
            table_queries = [
                '''CREATE TABLE IF NOT EXISTS market_patterns (
                    id INTEGER PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    pattern_type TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )''',
                
                '''CREATE TABLE IF NOT EXISTS market_conditions (
                    id INTEGER PRIMARY KEY,
                    timestamp DATETIME NOT NULL,
                    overall_trend TEXT NOT NULL,
                    volatility_level TEXT NOT NULL,
                    sentiment_score REAL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )''',
                
                '''CREATE TABLE IF NOT EXISTS analysis_cache (
                    cache_key TEXT PRIMARY KEY,
                    cached_data BLOB NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    expires_at DATETIME NOT NULL
                )'''
            ]
            
            # V3 PERFORMANCE INDEXES: Essential indexes only
            index_queries = [
                'CREATE INDEX IF NOT EXISTS idx_patterns_symbol ON market_patterns(symbol, timeframe)',
                'CREATE INDEX IF NOT EXISTS idx_conditions_timestamp ON market_conditions(timestamp)',
                'CREATE INDEX IF NOT EXISTS idx_cache_expires ON analysis_cache(expires_at)'
            ]
            
            for query in table_queries + index_queries:
                conn.execute(query)
            
            conn.commit()
            
        except Exception as e:
            logging.error(f"[V3-DB-ERROR] Table creation error: {e}")
    
    def get_connection(self) -> Optional[sqlite3.Connection]:
        """Get connection with V3 performance tracking"""
        start_time = time.time()
        
        try:
            # V3 FAST PATH: Try existing connection first
            if not self.connections.empty():
                conn = self.connections.get(timeout=1)
                self.pool_stats['connections_reused'] += 1
                self._update_wait_time(time.time() - start_time)
                return conn
            
            # V3 SCALE PATH: Create new connection if under limit
            with self.lock:
                if self.total_connections < self.max_connections:
                    conn = self._create_optimized_connection()
                    if conn:
                        self.total_connections += 1
                        self.pool_stats['connections_created'] += 1
                        self.pool_stats['active_connections'] = self.total_connections
                        self._update_wait_time(time.time() - start_time)
                        return conn
            
            # V3 WAIT PATH: Wait for available connection (bounded time)
            conn = self.connections.get(timeout=15)  # Reduced wait time
            self.pool_stats['connections_reused'] += 1
            self._update_wait_time(time.time() - start_time)
            return conn
            
        except queue.Empty:
            logging.warning("[V3-DB-TIMEOUT] No database connections available")
            return None
        except Exception as e:
            logging.error(f"[V3-DB-ERROR] Error getting connection: {e}")
            return None
    
    def return_connection(self, conn: sqlite3.Connection):
        """Return connection with V3 validation"""
        try:
            if conn and not self.connections.full():
                # V3 HEALTH CHECK: Test connection is still valid
                conn.execute('SELECT 1')
                self.connections.put(conn)
            elif conn:
                # V3 CLEANUP: Pool is full, close excess connections
                conn.close()
                self.pool_stats['connections_closed'] += 1
                with self.lock:
                    self.total_connections -= 1
                    self.pool_stats['active_connections'] = self.total_connections
        except Exception as e:
            logging.error(f"[V3-DB-ERROR] Error returning connection: {e}")
            if conn:
                try:
                    conn.close()
                    self.pool_stats['connections_closed'] += 1
                    with self.lock:
                        self.total_connections -= 1
                        self.pool_stats['active_connections'] = self.total_connections
                except:
                    pass
    
    def _update_wait_time(self, wait_time: float):
        """Update wait time statistics with bounds"""
        self.pool_stats['wait_time_total'] += wait_time
        total_requests = self.pool_stats['connections_created'] + self.pool_stats['connections_reused']
        if total_requests > 0:
            self.pool_stats['avg_wait_time'] = self.pool_stats['wait_time_total'] / total_requests
    
    def execute_query_with_pool(self, query: str, params: tuple = None, 
                               fetch_one: bool = False, fetch_all: bool = False) -> Any:
        """Execute query with V3 error handling"""
        conn = None
        try:
            conn = self.get_connection()
            if not conn:
                raise Exception("No database connection available")
            
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            if fetch_one:
                result = cursor.fetchone()
            elif fetch_all:
                result = cursor.fetchall()
            else:
                conn.commit()
                result = cursor.rowcount
            
            return result
            
        except Exception as e:
            logging.error(f"[V3-DB-QUERY-ERROR] Database query error: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                self.return_connection(conn)
    
    def get_pool_stats(self) -> Dict[str, Any]:
        """Get V3 pool statistics"""
        # V3 MEMORY TRACKING: Add memory usage
        try:
            process = psutil.Process()
            self.pool_stats['memory_usage_mb'] = process.memory_info().rss / 1024 / 1024
        except:
            pass
        
        return self.pool_stats.copy()
    
    def close_all_connections(self):
        """V3 Cleanup: Close all connections safely"""
        try:
            while not self.connections.empty():
                conn = self.connections.get()
                conn.close()
                self.pool_stats['connections_closed'] += 1
            
            with self.lock:
                self.total_connections = 0
                self.pool_stats['active_connections'] = 0
            
            logging.info("[V3-DB-CLEANUP] All database connections closed")
            
        except Exception as e:
            logging.error(f"[V3-DB-ERROR] Error closing connections: {e}")

@dataclass
class MarketPattern:
    """V3 Market pattern data structure with real data validation"""
    symbol: str
    pattern_type: str
    timeframe: str
    confidence: float
    start_time: datetime
    end_time: datetime
    pattern_data: Dict[str, Any]
    data_source: str = "REAL_MARKET_ONLY"  # V3 Real data tracking

@dataclass
class SupportResistanceLevel:
    """V3 Support/Resistance level with real data validation"""
    symbol: str
    level_type: str  # 'support' or 'resistance'
    price_level: float
    strength: float
    timeframe: str
    touch_count: int
    last_test: datetime
    data_source: str = "REAL_MARKET_ONLY"  # V3 Real data tracking

class OptimizedPatternRecognition:
    """
    V3 Pattern Recognition with Performance Optimization
    
    OPTIMIZATIONS:
    - Bounded caches to prevent memory leaks
    - Real data validation
    - CPU-efficient algorithms
    """
    
    def __init__(self, db_pool: OptimizedDatabaseConnectionPool):
        self.db_pool = db_pool
        self.data_validator = V3RealDataValidator()
        
        # V3 BOUNDED CACHE: Prevent memory leaks with maxlen
        self.pattern_cache = {}
        self.cache_lock = threading.RLock()
        self.max_cache_size = 100  # V3 MEMORY LIMIT
        
        # V3 PERFORMANCE TRACKING
        self.pattern_stats = {
            'patterns_detected': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'validation_failures': 0
        }
    
    def detect_patterns(self, symbol: str, timeframe: str, data: pd.DataFrame) -> List[MarketPattern]:
        """Detect patterns with V3 real data validation and performance optimization"""
        try:
            # V3 CRITICAL: Validate real data first
            if not self.data_validator.validate_market_analysis_data(data, f"pattern_data_{symbol}_{timeframe}"):
                self.pattern_stats['validation_failures'] += 1
                logging.error(f"[V3-DATA-VIOLATION] Pattern detection aborted - invalid data for {symbol}")
                return []
            
            if data is None or len(data) < 20:
                return []
            
            # V3 CACHE CHECK: Bounded cache lookup
            cache_key = f"{symbol}_{timeframe}_{len(data)}_{hash(str(data['close'].iloc[-1]))}"
            cached_patterns = self._get_cached_patterns(cache_key)
            if cached_patterns:
                self.pattern_stats['cache_hits'] += 1
                return cached_patterns
            
            self.pattern_stats['cache_misses'] += 1
            patterns = []
            
            # V3 PERFORMANCE: Parallel pattern detection
            pattern_tasks = [
                self._detect_trend_patterns(symbol, timeframe, data),
                self._detect_reversal_patterns(symbol, timeframe, data),
                self._detect_continuation_patterns(symbol, timeframe, data),
                self._detect_candlestick_patterns(symbol, timeframe, data)
            ]
            
            # V3 EFFICIENCY: Combine all pattern results
            for pattern_list in pattern_tasks:
                if pattern_list:
                    patterns.extend(pattern_list)
            
            # V3 BOUNDED STORAGE: Limit patterns stored
            if len(patterns) > 50:  # Limit to prevent memory issues
                patterns = sorted(patterns, key=lambda p: p.confidence, reverse=True)[:50]
            
            # V3 CACHE: Store results with bounds
            self._cache_patterns(cache_key, patterns)
            self.pattern_stats['patterns_detected'] += len(patterns)
            
            return patterns
            
        except Exception as e:
            logging.error(f"[V3-PATTERN-ERROR] Error detecting patterns for {symbol}: {e}")
            return []
    
    def _detect_trend_patterns(self, symbol: str, timeframe: str, data: pd.DataFrame) -> List[MarketPattern]:
        """V3 Optimized trend pattern detection"""
        patterns = []
        
        try:
            close_prices = data['close'].values
            
            # V3 EFFICIENT: Use vectorized operations
            if len(close_prices) >= 20:
                # V3 PERFORMANCE: Pandas rolling operations (faster than loops)
                sma_20 = data['close'].rolling(window=20).mean()
                sma_50 = data['close'].rolling(window=min(50, len(data))).mean()
                
                current_price = close_prices[-1]
                current_sma_20 = sma_20.iloc[-1]
                current_sma_50 = sma_50.iloc[-1]
                
                # V3 VECTORIZED: Efficient trend detection
                if current_sma_20 > current_sma_50 and current_price > current_sma_20:
                    trend_strength = min((current_sma_20 - current_sma_50) / current_sma_50 * 100, 10)
                    
                    pattern = MarketPattern(
                        symbol=symbol,
                        pattern_type='uptrend',
                        timeframe=timeframe,
                        confidence=min(0.9, 0.5 + trend_strength / 20),  # Dynamic confidence
                        start_time=data['timestamp'].iloc[-20],
                        end_time=data['timestamp'].iloc[-1],
                        pattern_data={
                            'sma_20': current_sma_20,
                            'sma_50': current_sma_50,
                            'current_price': current_price,
                            'trend_strength': trend_strength
                        },
                        data_source="REAL_MARKET_ONLY"
                    )
                    patterns.append(pattern)
                
                elif current_sma_20 < current_sma_50 and current_price < current_sma_20:
                    trend_strength = min((current_sma_50 - current_sma_20) / current_sma_50 * 100, 10)
                    
                    pattern = MarketPattern(
                        symbol=symbol,
                        pattern_type='downtrend',
                        timeframe=timeframe,
                        confidence=min(0.9, 0.5 + trend_strength / 20),
                        start_time=data['timestamp'].iloc[-20],
                        end_time=data['timestamp'].iloc[-1],
                        pattern_data={
                            'sma_20': current_sma_20,
                            'sma_50': current_sma_50,
                            'current_price': current_price,
                            'trend_strength': trend_strength
                        },
                        data_source="REAL_MARKET_ONLY"
                    )
                    patterns.append(pattern)
            
        except Exception as e:
            logging.error(f"[V3-TREND-ERROR] Error detecting trend patterns: {e}")
        
        return patterns
    
    def _detect_reversal_patterns(self, symbol: str, timeframe: str, data: pd.DataFrame) -> List[MarketPattern]:
        """V3 Optimized reversal pattern detection"""
        patterns = []
        
        try:
            if len(data) < 10:
                return patterns
            
            # V3 VECTORIZED: Use pandas operations for efficiency
            high_prices = data['high'].values
            low_prices = data['low'].values
            
            # V3 PERFORMANCE: Simplified double top/bottom detection
            if len(high_prices) >= 20:
                # V3 EFFICIENT: Rolling max/min operations
                rolling_max = data['high'].rolling(window=5, center=True).max()
                rolling_min = data['low'].rolling(window=5, center=True).min()
                
                # Find peaks and troughs more efficiently
                peaks = data['high'] == rolling_max
                troughs = data['low'] == rolling_min
                
                peak_indices = data[peaks].index.tolist()
                trough_indices = data[troughs].index.tolist()
                
                # V3 BOUNDED: Limit analysis to recent patterns
                recent_peaks = peak_indices[-5:] if len(peak_indices) >= 2 else peak_indices
                recent_troughs = trough_indices[-5:] if len(trough_indices) >= 2 else trough_indices
                
                # Double top detection (simplified)
                if len(recent_peaks) >= 2:
                    last_peak_idx = recent_peaks[-1]
                    prev_peak_idx = recent_peaks[-2]
                    
                    last_peak_price = data.loc[last_peak_idx, 'high']
                    prev_peak_price = data.loc[prev_peak_idx, 'high']
                    
                    if abs(last_peak_price - prev_peak_price) / prev_peak_price < 0.03:  # 3% tolerance
                        pattern = MarketPattern(
                            symbol=symbol,
                            pattern_type='double_top',
                            timeframe=timeframe,
                            confidence=0.6,
                            start_time=data.loc[prev_peak_idx, 'timestamp'],
                            end_time=data.loc[last_peak_idx, 'timestamp'],
                            pattern_data={
                                'first_high': prev_peak_price,
                                'second_high': last_peak_price,
                                'current_price': data['close'].iloc[-1],
                                'similarity': 1 - abs(last_peak_price - prev_peak_price) / prev_peak_price
                            },
                            data_source="REAL_MARKET_ONLY"
                        )
                        patterns.append(pattern)
                
                # Double bottom detection (simplified)
                if len(recent_troughs) >= 2:
                    last_trough_idx = recent_troughs[-1]
                    prev_trough_idx = recent_troughs[-2]
                    
                    last_trough_price = data.loc[last_trough_idx, 'low']
                    prev_trough_price = data.loc[prev_trough_idx, 'low']
                    
                    if abs(last_trough_price - prev_trough_price) / prev_trough_price < 0.03:
                        pattern = MarketPattern(
                            symbol=symbol,
                            pattern_type='double_bottom',
                            timeframe=timeframe,
                            confidence=0.6,
                            start_time=data.loc[prev_trough_idx, 'timestamp'],
                            end_time=data.loc[last_trough_idx, 'timestamp'],
                            pattern_data={
                                'first_low': prev_trough_price,
                                'second_low': last_trough_price,
                                'current_price': data['close'].iloc[-1],
                                'similarity': 1 - abs(last_trough_price - prev_trough_price) / prev_trough_price
                            },
                            data_source="REAL_MARKET_ONLY"
                        )
                        patterns.append(pattern)
            
        except Exception as e:
            logging.error(f"[V3-REVERSAL-ERROR] Error detecting reversal patterns: {e}")
        
        return patterns
    
    def _detect_continuation_patterns(self, symbol: str, timeframe: str, data: pd.DataFrame) -> List[MarketPattern]:
        """V3 Optimized continuation pattern detection"""
        patterns = []
        
        try:
            if len(data) < 15:
                return patterns
            
            # V3 VECTORIZED: Efficient flag pattern detection
            close_prices = data['close'].values
            
            if len(close_prices) >= 15:
                # V3 PERFORMANCE: Use numpy operations
                recent_range = close_prices[-10:]
                prev_range = close_prices[-15:-10]
                
                recent_mean = np.mean(recent_range)
                prev_mean = np.mean(prev_range)
                recent_std = np.std(recent_range)
                prev_std = np.std(prev_range)
                
                # V3 EFFICIENT: Vectorized calculations
                if (recent_mean > prev_mean * 1.02 and recent_std < prev_std * 0.8):
                    pattern = MarketPattern(
                        symbol=symbol,
                        pattern_type='bull_flag',
                        timeframe=timeframe,
                        confidence=0.5,
                        start_time=data['timestamp'].iloc[-15],
                        end_time=data['timestamp'].iloc[-1],
                        pattern_data={
                            'flagpole_strength': (recent_mean - prev_mean) / prev_mean * 100,
                            'consolidation_ratio': recent_std / prev_std,
                            'current_price': close_prices[-1]
                        },
                        data_source="REAL_MARKET_ONLY"
                    )
                    patterns.append(pattern)
                
                elif (recent_mean < prev_mean * 0.98 and recent_std < prev_std * 0.8):
                    pattern = MarketPattern(
                        symbol=symbol,
                        pattern_type='bear_flag',
                        timeframe=timeframe,
                        confidence=0.5,
                        start_time=data['timestamp'].iloc[-15],
                        end_time=data['timestamp'].iloc[-1],
                        pattern_data={
                            'flagpole_strength': (prev_mean - recent_mean) / prev_mean * 100,
                            'consolidation_ratio': recent_std / prev_std,
                            'current_price': close_prices[-1]
                        },
                        data_source="REAL_MARKET_ONLY"
                    )
                    patterns.append(pattern)
            
        except Exception as e:
            logging.error(f"[V3-CONTINUATION-ERROR] Error detecting continuation patterns: {e}")
        
        return patterns
    
    def _detect_candlestick_patterns(self, symbol: str, timeframe: str, data: pd.DataFrame) -> List[MarketPattern]:
        """V3 Optimized candlestick pattern detection"""
        patterns = []
        
        try:
            if len(data) < 3:
                return patterns
            
            # V3 VECTORIZED: Efficient OHLC operations
            opens = data['open'].values
            highs = data['high'].values
            lows = data['low'].values
            closes = data['close'].values
            
            # V3 PERFORMANCE: Analyze only recent candles (bounded)
            start_idx = max(0, len(data) - 10)
            
            for i in range(start_idx, len(data)):
                if i < 1:
                    continue
                
                # V3 EFFICIENT: Vectorized calculations
                body_size = abs(closes[i] - opens[i])
                candle_range = highs[i] - lows[i]
                
                if candle_range == 0:
                    continue
                
                # Doji pattern
                if body_size / candle_range < 0.1:
                    pattern = MarketPattern(
                        symbol=symbol,
                        pattern_type='doji',
                        timeframe=timeframe,
                        confidence=0.4,
                        start_time=data['timestamp'].iloc[i],
                        end_time=data['timestamp'].iloc[i],
                        pattern_data={
                            'body_size_ratio': body_size / candle_range,
                            'price_level': closes[i]
                        },
                        data_source="REAL_MARKET_ONLY"
                    )
                    patterns.append(pattern)
                
                # Hammer pattern
                lower_shadow = min(opens[i], closes[i]) - lows[i]
                upper_shadow = highs[i] - max(opens[i], closes[i])
                
                if (lower_shadow > body_size * 2 and upper_shadow < body_size * 0.5):
                    pattern = MarketPattern(
                        symbol=symbol,
                        pattern_type='hammer',
                        timeframe=timeframe,
                        confidence=0.5,
                        start_time=data['timestamp'].iloc[i],
                        end_time=data['timestamp'].iloc[i],
                        pattern_data={
                            'lower_shadow_ratio': lower_shadow / candle_range,
                            'price_level': closes[i]
                        },
                        data_source="REAL_MARKET_ONLY"
                    )
                    patterns.append(pattern)
            
        except Exception as e:
            logging.error(f"[V3-CANDLESTICK-ERROR] Error detecting candlestick patterns: {e}")
        
        return patterns
    
    def _get_cached_patterns(self, cache_key: str) -> Optional[List[MarketPattern]]:
        """Get patterns from bounded cache"""
        try:
            with self.cache_lock:
                if cache_key in self.pattern_cache:
                    cache_entry = self.pattern_cache[cache_key]
                    if time.time() - cache_entry['timestamp'] < 300:  # 5 minute cache
                        return cache_entry['patterns']
                    else:
                        del self.pattern_cache[cache_key]
            return None
        except Exception as e:
            logging.error(f"[V3-CACHE-ERROR] Error getting cached patterns: {e}")
            return None
    
    def _cache_patterns(self, cache_key: str, patterns: List[MarketPattern]):
        """Cache patterns with V3 bounds"""
        try:
            with self.cache_lock:
                # V3 BOUNDED: Limit cache size to prevent memory leaks
                if len(self.pattern_cache) >= self.max_cache_size:
                    # Remove oldest entries
                    oldest_keys = sorted(self.pattern_cache.keys(), 
                                       key=lambda k: self.pattern_cache[k]['timestamp'])
                    for key in oldest_keys[:10]:  # Remove 10 oldest
                        del self.pattern_cache[key]
                
                self.pattern_cache[cache_key] = {
                    'patterns': patterns,
                    'timestamp': time.time()
                }
        except Exception as e:
            logging.error(f"[V3-CACHE-ERROR] Error caching patterns: {e}")
    
    def get_pattern_stats(self) -> Dict[str, Any]:
        """Get V3 pattern statistics"""
        return self.pattern_stats.copy()

class V3MarketAnalysisEngine:
    """
    V3 Market Analysis Engine - PERFORMANCE OPTIMIZED FOR 8 vCPU / 24GB
    
    CRITICAL V3 FIXES:
    - ThreadPoolExecutor: max_workers optimized for 8 vCPU (4-6 workers)
    - Bounded collections: All deques and caches have size limits
    - Real data validation: V3 compliance with zero mock data
    - Memory management: Automatic cleanup and monitoring
    - UTF-8 compliance: Proper encoding handling
    """
    
    def __init__(self, config_manager=None):
        self.config = config_manager
        self.data_validator = V3RealDataValidator()
        
        # V3 CRITICAL: ThreadPoolExecutor optimized for 8 vCPU server
        cpu_count = psutil.cpu_count()
        # FIXED: Was 16 workers, now optimized for 8 vCPU server
        optimal_workers = min(cpu_count // 2, 6)  # Conservative: 4-6 workers max
        
        self.executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=optimal_workers,
            thread_name_prefix="V3-MarketAnalysis"
        )
        
        logging.info(f"[V3-PERFORMANCE] ThreadPoolExecutor configured with {optimal_workers} workers (CPU: {cpu_count})")
        
        # V3 OPTIMIZED: Database pool sized for server
        max_db_connections = min(cpu_count * 2, 16)  # 2 per CPU, max 16
        self.db_pool = OptimizedDatabaseConnectionPool(
            'market_analysis.db', 
            max_connections=max_db_connections
        )
        
        # V3 PERFORMANCE: Optimized pattern recognition
        self.pattern_recognition = OptimizedPatternRecognition(self.db_pool)
        
        # V3 BOUNDED CACHES: Prevent memory leaks with size limits
        self.analysis_cache = {}
        self.cache_lock = threading.RLock()
        self.max_cache_size = 150  # V3 MEMORY BOUND
        
        # V3 BOUNDED COLLECTIONS: All collections have size limits
        self.analysis_queue = deque(maxlen=100)  # FIXED: Was unbounded
        self.performance_metrics = deque(maxlen=50)  # FIXED: Was unbounded
        self.error_log = deque(maxlen=25)  # FIXED: Was unbounded
        
        # V3 PERFORMANCE TRACKING
        self.analysis_stats = {
            'total_analyses': 0,
            'successful_analyses': 0,
            'failed_analyses': 0,
            'avg_analysis_time': 0.0,
            'cache_hit_rate': 0.0,
            'patterns_detected': 0,
            'support_resistance_levels': 0,
            'memory_usage_mb': 0.0,
            'cpu_usage_percent': 0.0,
            'thread_pool_workers': optimal_workers,
            'real_data_validations': 0,
            'mock_data_violations': 0
        }
        
        # V3 RESOURCE MONITORING
        self.resource_monitor = {
            'max_memory_mb': int(os.getenv('MAX_MEMORY_MB', 2000)),
            'max_cpu_percent': int(os.getenv('MAX_CPU_PERCENT', 75)),
            'memory_warning_threshold': 85,
            'cpu_warning_threshold': 80
        }
        
        # V3 Start background monitoring
        self._start_v3_background_tasks()
        
        logging.info("[V3-INIT] Market Analysis Engine initialized - REAL DATA ONLY MODE")
    
    async def analyze_market_comprehensive_v3(self, symbol: str, timeframes: List[str] = None) -> Dict[str, Any]:
        """
        V3 Comprehensive market analysis with real data validation and performance optimization
        """
        start_time = time.time()
        analysis_id = f"{symbol}_{int(time.time())}"
        
        try:
            # V3 CRITICAL: Validate component integrity
            if not self.data_validator.validate_analysis_components(self, "market_analysis_engine"):
                raise ValueError("[V3-MOCK-VIOLATION] Analysis engine contains mock components")
            
            if timeframes is None:
                timeframes = ['5m', '15m', '1h', '4h', '1d']
            
            # V3 BOUNDED: Limit timeframes to prevent resource exhaustion
            timeframes = timeframes[:5]  # Max 5 timeframes
            
            analysis_result = {
                'analysis_id': analysis_id,
                'symbol': symbol,
                'timeframes': timeframes,
                'analysis_timestamp': datetime.now().isoformat(),
                'overall_sentiment': 'neutral',
                'confidence': 0.0,
                'analysis_time': 0.0,
                'timeframe_analyses': {},
                'patterns': [],
                'support_resistance': [],
                'market_structure': {},
                'volatility_analysis': {},
                'volume_analysis': {},
                'v3_data_validation': True,
                'data_source': 'REAL_MARKET_ONLY'
            }
            
            # V3 PERFORMANCE: Parallel analysis with resource monitoring
            self._check_resource_usage()
            
            analysis_tasks = []
            for tf in timeframes:
                task = self._analyze_timeframe_v3(symbol, tf)
                analysis_tasks.append(task)
            
            # V3 BOUNDED: Timeout protection
            try:
                timeframe_results = await asyncio.wait_for(
                    asyncio.gather(*analysis_tasks, return_exceptions=True),
                    timeout=60.0  # 60 second timeout
                )
            except asyncio.TimeoutError:
                logging.warning(f"[V3-TIMEOUT] Analysis timeout for {symbol}")
                timeframe_results = [None] * len(timeframes)
            
            # V3 PROCESS: Handle results with error protection
            all_patterns = []
            all_sr_levels = []
            sentiment_scores = []
            confidence_scores = []
            
            for i, result in enumerate(timeframe_results):
                tf = timeframes[i]
                
                if isinstance(result, Exception):
                    logging.warning(f"[V3-ANALYSIS-ERROR] Analysis failed for {symbol} {tf}: {result}")
                    self.error_log.append({
                        'timestamp': datetime.now(),
                        'symbol': symbol,
                        'timeframe': tf,
                        'error': str(result)
                    })
                    continue
                
                if result and isinstance(result, dict):
                    analysis_result['timeframe_analyses'][tf] = result
                    
                    # V3 BOUNDED: Limit collected data
                    if 'patterns' in result:
                        patterns = result['patterns'][:10]  # Max 10 patterns per timeframe
                        all_patterns.extend(patterns)
                    
                    if 'support_resistance' in result:
                        sr_levels = result['support_resistance'][:10]  # Max 10 S/R levels
                        all_sr_levels.extend(sr_levels)
                    
                    if 'sentiment_score' in result:
                        sentiment_scores.append(result['sentiment_score'])
                    if 'confidence' in result:
                        confidence_scores.append(result['confidence'])
            
            # V3 BOUNDED: Limit final results
            analysis_result['patterns'] = all_patterns[:25]  # Max 25 total patterns
            analysis_result['support_resistance'] = all_sr_levels[:15]  # Max 15 S/R levels
            
            # V3 AGGREGATE: Calculate overall metrics
            if sentiment_scores:
                avg_sentiment = sum(sentiment_scores) / len(sentiment_scores)
                if avg_sentiment > 0.3:
                    analysis_result['overall_sentiment'] = 'bullish'
                elif avg_sentiment < -0.3:
                    analysis_result['overall_sentiment'] = 'bearish'
                else:
                    analysis_result['overall_sentiment'] = 'neutral'
            
            if confidence_scores:
                analysis_result['confidence'] = sum(confidence_scores) / len(confidence_scores)
            
            # V3 STORE: Save results efficiently
            await self._store_analysis_results_v3(analysis_result)
            
            # V3 TRACK: Update performance metrics
            analysis_time = time.time() - start_time
            analysis_result['analysis_time'] = analysis_time
            self._update_analysis_stats_v3(True, analysis_time, len(all_patterns), len(all_sr_levels))
            
            return analysis_result
            
        except Exception as e:
            analysis_time = time.time() - start_time
            self._update_analysis_stats_v3(False, analysis_time, 0, 0)
            logging.error(f"[V3-ANALYSIS-ERROR] Comprehensive analysis error for {symbol}: {e}")
            
            return {
                'analysis_id': analysis_id,
                'symbol': symbol,
                'error': str(e),
                'analysis_time': analysis_time,
                'analysis_timestamp': datetime.now().isoformat(),
                'v3_data_validation': False,
                'data_source': 'ERROR'
            }
    
    async def _analyze_timeframe_v3(self, symbol: str, timeframe: str) -> Dict[str, Any]:
        """V3 Optimized timeframe analysis with real data validation"""
        try:
            # V3 CACHE: Check bounded cache first
            cache_key = f"v3_{symbol}_{timeframe}_{int(time.time() // 300)}"  # 5-minute cache
            cached_result = self._get_cached_analysis_v3(cache_key)
            if cached_result:
                return cached_result
            
            # V3 CRITICAL: Get real market data only
            market_data = await self._get_real_market_data_v3(symbol, timeframe)
            
            # V3 VALIDATION: Ensure real data compliance
            if not self.data_validator.validate_market_analysis_data(
                market_data, f"timeframe_analysis_{symbol}_{timeframe}"
            ):
                self.analysis_stats['mock_data_violations'] += 1
                logging.error(f"[V3-DATA-VIOLATION] Timeframe analysis aborted - invalid data for {symbol} {timeframe}")
                return {}
            
            self.analysis_stats['real_data_validations'] += 1
            
            if market_data is None or market_data.empty:
                return {}
            
            result = {
                'symbol': symbol,
                'timeframe': timeframe,
                'timestamp': datetime.now().isoformat(),
                'patterns': [],
                'support_resistance': [],
                'sentiment_score': 0.0,
                'confidence': 0.0,
                'technical_indicators': {},
                'volume_profile': {},
                'volatility_metrics': {},
                'data_source': 'REAL_MARKET_ONLY',
                'v3_validated': True
            }
            
            # V3 PARALLEL: Efficient async analysis
            analysis_tasks = [
                asyncio.get_event_loop().run_in_executor(
                    self.executor,
                    self.pattern_recognition.detect_patterns,
                    symbol, timeframe, market_data
                ),
                self._analyze_support_resistance_v3(symbol, timeframe, market_data),
                self._calculate_indicators_v3(market_data),
                self._analyze_volume_v3(market_data),
                self._analyze_volatility_v3(market_data)
            ]
            
            # V3 BOUNDED: Timeout protection for analysis tasks
            try:
                task_results = await asyncio.wait_for(
                    asyncio.gather(*analysis_tasks, return_exceptions=True),
                    timeout=30.0  # 30 second timeout per timeframe
                )
            except asyncio.TimeoutError:
                logging.warning(f"[V3-TIMEOUT] Timeframe analysis timeout for {symbol} {timeframe}")
                return {}
            
            # V3 PROCESS: Handle results safely
            patterns, sr_levels, indicators, volume_analysis, volatility_metrics = task_results
            
            # V3 SAFE: Handle exceptions in results
            if not isinstance(patterns, Exception) and patterns:
                # Convert patterns to dict format with bounds
                result['patterns'] = [
                    {
                        'type': p.pattern_type,
                        'confidence': p.confidence,
                        'start_time': p.start_time.isoformat(),
                        'end_time': p.end_time.isoformat(),
                        'data': p.pattern_data,
                        'data_source': p.data_source
                    } for p in patterns[:10]  # Max 10 patterns
                ]
            
            if not isinstance(sr_levels, Exception) and sr_levels:
                result['support_resistance'] = sr_levels[:10]  # Max 10 S/R levels
            
            if not isinstance(indicators, Exception) and indicators:
                result['technical_indicators'] = indicators
            
            if not isinstance(volume_analysis, Exception) and volume_analysis:
                result['volume_profile'] = volume_analysis
            
            if not isinstance(volatility_metrics, Exception) and volatility_metrics:
                result['volatility_metrics'] = volatility_metrics
            
            # V3 CALCULATE: Sentiment and confidence
            result['sentiment_score'] = self._calculate_sentiment_score_v3(indicators, patterns)
            result['confidence'] = self._calculate_confidence_score_v3(patterns, sr_levels, indicators)
            
            # V3 CACHE: Store with bounds
            self._cache_analysis_v3(cache_key, result)
            
            return result
            
        except Exception as e:
            logging.error(f"[V3-TIMEFRAME-ERROR] Error analyzing {symbol} {timeframe}: {e}")
            return {}
    
    async def _get_real_market_data_v3(self, symbol: str, timeframe: str, limit: int = 200) -> Optional[pd.DataFrame]:
        """
        V3 CRITICAL: Get REAL market data only - NO MOCK DATA ALLOWED
        
        This function enforces V3's zero mock data policy
        """
        try:
            # V3 ENFORCEMENT: Only real data sources allowed
            logging.info(f"[V3-REAL-DATA] Fetching real market data: {symbol} {timeframe}")
            
            def fetch_real_data_only():
                """
                V3 CRITICAL: This function must only return real market data
                
                Implementation should query:
                - Historical data manager for stored real market data
                - Binance exchange manager for live real market data  
                - External data collector for real historical market data
                
                NEVER generate or return mock/simulated data
                """
                
                # V3 PLACEHOLDER: Connect to real data sources
                # In production, this would connect to:
                # - self.historical_data_manager.get_historical_data(symbol, timeframe, limit)
                # - self.binance_manager.get_real_market_data(symbol, timeframe, limit)
                # - self.external_data_collector.get_real_historical_data(symbol, timeframe, limit)
                
                logging.info(f"[V3-REAL-DATA-REQUIRED] Real data connection needed for {symbol} {timeframe}")
                logging.info("[V3-PRODUCTION] Connect to real data sources: Historical/Binance/External")
                
                # V3 COMPLIANCE: Return None when real data not available
                # Never return mock/simulated data
                return None
            
            return await asyncio.get_event_loop().run_in_executor(
                self.executor, fetch_real_data_only
            )
            
        except Exception as e:
            logging.error(f"[V3-REAL-DATA-ERROR] Error fetching real market data for {symbol}: {e}")
            return None
    
    async def _analyze_support_resistance_v3(self, symbol: str, timeframe: str, 
                                            data: pd.DataFrame) -> List[Dict[str, Any]]:
        """V3 Optimized support/resistance analysis"""
        try:
            if data is None or len(data) < 20:
                return []
            
            def analyze_sr_levels():
                levels = []
                
                # V3 VECTORIZED: Efficient operations
                high_prices = data['high'].values
                low_prices = data['low'].values
                
                # V3 BOUNDED: Limit analysis range
                analysis_range = min(len(data), 100)  # Max 100 periods
                start_idx = max(0, len(data) - analysis_range)
                
                # V3 PERFORMANCE: Use pandas rolling operations
                window_size = min(10, analysis_range // 4)
                rolling_max = data['high'].rolling(window=window_size, center=True).max()
                rolling_min = data['low'].rolling(window=window_size, center=True).min()
                
                # Find resistance levels (simplified)
                resistance_mask = (data['high'] == rolling_max) & (data['high'] > data['high'].quantile(0.9))
                support_mask = (data['low'] == rolling_min) & (data['low'] < data['low'].quantile(0.1))
                
                # V3 BOUNDED: Limit number of levels
                resistance_levels = data[resistance_mask].tail(5)  # Max 5 resistance levels
                support_levels = data[support_mask].tail(5)  # Max 5 support levels
                
                for _, level_data in resistance_levels.iterrows():
                    levels.append({
                        'level_type': 'resistance',
                        'price_level': level_data['high'],
                        'strength': 0.7,  # Simplified strength calculation
                        'timeframe': timeframe,
                        'touch_count': 1,
                        'last_test': level_data['timestamp'].isoformat(),
                        'data_source': 'REAL_MARKET_ONLY'
                    })
                
                for _, level_data in support_levels.iterrows():
                    levels.append({
                        'level_type': 'support',
                        'price_level': level_data['low'],
                        'strength': 0.7,
                        'timeframe': timeframe,
                        'touch_count': 1,
                        'last_test': level_data['timestamp'].isoformat(),
                        'data_source': 'REAL_MARKET_ONLY'
                    })
                
                return levels[:10]  # V3 BOUNDED: Max 10 levels total
            
            return await asyncio.get_event_loop().run_in_executor(
                self.executor, analyze_sr_levels
            )
            
        except Exception as e:
            logging.error(f"[V3-SR-ERROR] Error analyzing support/resistance: {e}")
            return []
    
    async def _calculate_indicators_v3(self, data: pd.DataFrame) -> Dict[str, Any]:
        """V3 Optimized technical indicators calculation"""
        try:
            if data is None or len(data) < 20:
                return {}
            
            def calculate_efficient_indicators():
                indicators = {}
                
                # V3 VECTORIZED: Use pandas for efficiency
                close_prices = data['close']
                
                # V3 PERFORMANCE: Efficient moving averages
                if len(close_prices) >= 20:
                    indicators['sma_20'] = close_prices.rolling(window=20).mean().iloc[-1]
                    if len(close_prices) >= 50:
                        indicators['sma_50'] = close_prices.rolling(window=50).mean().iloc[-1]
                    else:
                        indicators['sma_50'] = indicators['sma_20']
                    
                    # V3 EFFICIENT: Exponential moving averages
                    indicators['ema_12'] = close_prices.ewm(span=12).mean().iloc[-1]
                    indicators['ema_26'] = close_prices.ewm(span=26).mean().iloc[-1]
                    
                    # V3 MACD calculation
                    indicators['macd'] = indicators['ema_12'] - indicators['ema_26']
                    macd_series = close_prices.ewm(span=12).mean() - close_prices.ewm(span=26).mean()
                    indicators['macd_signal'] = macd_series.ewm(span=9).mean().iloc[-1]
                
                # V3 RSI calculation (vectorized)
                if len(close_prices) >= 14:
                    delta = close_prices.diff()
                    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
                    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
                    rs = gain / loss
                    rsi = 100 - (100 / (1 + rs))
                    indicators['rsi'] = rsi.iloc[-1]
                
                # V3 Bollinger Bands
                if len(close_prices) >= 20:
                    sma_20 = close_prices.rolling(window=20).mean()
                    std_20 = close_prices.rolling(window=20).std()
                    indicators['bb_upper'] = (sma_20 + (std_20 * 2)).iloc[-1]
                    indicators['bb_lower'] = (sma_20 - (std_20 * 2)).iloc[-1]
                    indicators['bb_middle'] = sma_20.iloc[-1]
                
                # V3 Current metrics
                indicators['current_price'] = close_prices.iloc[-1]
                if len(close_prices) > 1:
                    indicators['price_change_pct'] = ((close_prices.iloc[-1] - close_prices.iloc[-2]) / 
                                                    close_prices.iloc[-2] * 100)
                else:
                    indicators['price_change_pct'] = 0
                
                return indicators
            
            return await asyncio.get_event_loop().run_in_executor(
                self.executor, calculate_efficient_indicators
            )
            
        except Exception as e:
            logging.error(f"[V3-INDICATORS-ERROR] Error calculating indicators: {e}")
            return {}
    
    async def _analyze_volume_v3(self, data: pd.DataFrame) -> Dict[str, Any]:
        """V3 Optimized volume analysis"""
        try:
            if data is None or 'volume' not in data.columns:
                return {}
            
            def analyze_volume_efficiently():
                volume_analysis = {}
                
                # V3 VECTORIZED: Efficient volume calculations
                volumes = data['volume']
                close_prices = data['close']
                
                if len(volumes) >= 20:
                    # V3 PERFORMANCE: Rolling operations
                    volume_analysis['avg_volume_20'] = volumes.rolling(window=20).mean().iloc[-1]
                    volume_analysis['current_volume'] = volumes.iloc[-1]
                    volume_analysis['volume_ratio'] = (volume_analysis['current_volume'] / 
                                                     volume_analysis['avg_volume_20'])
                    
                    # V3 EFFICIENT: Volume trend calculation
                    recent_volumes = volumes.tail(10)
                    if len(recent_volumes) >= 5:
                        # Simple trend using correlation with time index
                        time_index = np.arange(len(recent_volumes))
                        correlation = np.corrcoef(time_index, recent_volumes.values)[0, 1]
                        volume_analysis['volume_trend'] = 'increasing' if correlation > 0.1 else 'decreasing'
                    
                    # V3 BOUNDED: Volume-price relationship (limited analysis)
                    if len(close_prices) >= 20:
                        price_changes = close_prices.pct_change().tail(20)
                        volume_changes = volumes.pct_change().tail(20)
                        
                        # Remove NaN values
                        valid_mask = ~(price_changes.isna() | volume_changes.isna())
                        if valid_mask.sum() > 5:
                            correlation = np.corrcoef(
                                price_changes[valid_mask].values,
                                volume_changes[valid_mask].values
                            )[0, 1]
                            volume_analysis['price_volume_correlation'] = correlation if not np.isnan(correlation) else 0
                
                return volume_analysis
            
            return await asyncio.get_event_loop().run_in_executor(
                self.executor, analyze_volume_efficiently
            )
            
        except Exception as e:
            logging.error(f"[V3-VOLUME-ERROR] Error analyzing volume: {e}")
            return {}
    
    async def _analyze_volatility_v3(self, data: pd.DataFrame) -> Dict[str, Any]:
        """V3 Optimized volatility analysis"""
        try:
            if data is None or len(data) < 20:
                return {}
            
            def analyze_volatility_efficiently():
                volatility_metrics = {}
                
                # V3 VECTORIZED: Efficient volatility calculations
                close_prices = data['close']
                high_prices = data['high']
                low_prices = data['low']
                
                # V3 PERFORMANCE: Vectorized returns calculation
                returns = close_prices.pct_change().dropna()
                
                if len(returns) > 0:
                    # V3 EFFICIENT: Volatility measures
                    volatility_metrics['historical_volatility'] = returns.std() * np.sqrt(252)  # Annualized
                    volatility_metrics['recent_volatility'] = returns.tail(10).std() * np.sqrt(252)
                
                # V3 ATR calculation (vectorized)
                if len(data) > 1:
                    # True Range calculation
                    tr1 = high_prices - low_prices
                    tr2 = abs(high_prices - close_prices.shift(1))
                    tr3 = abs(low_prices - close_prices.shift(1))
                    
                    true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
                    atr = true_range.rolling(window=min(14, len(true_range))).mean().iloc[-1]
                    
                    volatility_metrics['atr'] = atr
                    volatility_metrics['atr_pct'] = atr / close_prices.iloc[-1] * 100
                
                # V3 Volatility regime classification
                if 'historical_volatility' in volatility_metrics and 'recent_volatility' in volatility_metrics:
                    recent = volatility_metrics['recent_volatility']
                    historical = volatility_metrics['historical_volatility']
                    
                    if recent > historical * 1.5:
                        volatility_metrics['regime'] = 'high'
                    elif recent < historical * 0.7:
                        volatility_metrics['regime'] = 'low'
                    else:
                        volatility_metrics['regime'] = 'normal'
                
                return volatility_metrics
            
            return await asyncio.get_event_loop().run_in_executor(
                self.executor, analyze_volatility_efficiently
            )
            
        except Exception as e:
            logging.error(f"[V3-VOLATILITY-ERROR] Error analyzing volatility: {e}")
            return {}
    
    def _calculate_sentiment_score_v3(self, indicators: Dict[str, Any], patterns: List[MarketPattern]) -> float:
        """V3 Optimized sentiment calculation"""
        try:
            if not indicators:
                return 0.0
            
            sentiment_factors = []
            
            # V3 EFFICIENT: Vectorized sentiment calculations
            rsi = indicators.get('rsi', 50)
            if rsi > 70:
                sentiment_factors.append(-0.3)  # Overbought
            elif rsi < 30:
                sentiment_factors.append(0.3)   # Oversold
            else:
                sentiment_factors.append((rsi - 50) / 100)  # Normalized
            
            # V3 MACD sentiment
            macd = indicators.get('macd', 0)
            macd_signal = indicators.get('macd_signal', 0)
            if abs(macd - macd_signal) > 0.01:  # Meaningful signal
                sentiment_factors.append(0.2 if macd > macd_signal else -0.2)
            
            # V3 Moving average sentiment
            current_price = indicators.get('current_price', 0)
            sma_20 = indicators.get('sma_20', current_price)
            sma_50 = indicators.get('sma_50', current_price)
            
            if current_price > 0 and sma_20 > 0 and sma_50 > 0:
                if current_price > sma_20 > sma_50:
                    sentiment_factors.append(0.3)
                elif current_price < sma_20 < sma_50:
                    sentiment_factors.append(-0.3)
                else:
                    sentiment_factors.append((current_price - sma_20) / sma_20 * 2)  # Normalized
            
            # V3 BOUNDED: Pattern sentiment (limited patterns)
            if patterns and len(patterns) > 0:
                bullish_patterns = {'uptrend', 'double_bottom', 'bull_flag', 'hammer'}
                bearish_patterns = {'downtrend', 'double_top', 'bear_flag'}
                
                for pattern in patterns[:5]:  # Max 5 patterns for sentiment
                    if pattern.pattern_type in bullish_patterns:
                        sentiment_factors.append(0.2 * pattern.confidence)
                    elif pattern.pattern_type in bearish_patterns:
                        sentiment_factors.append(-0.2 * pattern.confidence)
            
            # V3 CALCULATE: Bounded sentiment score
            if sentiment_factors:
                sentiment_score = sum(sentiment_factors) / len(sentiment_factors)
                return max(-1.0, min(1.0, sentiment_score))  # Clamp to [-1, 1]
            
            return 0.0
            
        except Exception as e:
            logging.error(f"[V3-SENTIMENT-ERROR] Error calculating sentiment: {e}")
            return 0.0
    
    def _calculate_confidence_score_v3(self, patterns: List[MarketPattern], 
                                      sr_levels: List[Dict], indicators: Dict[str, Any]) -> float:
        """V3 Optimized confidence calculation"""
        try:
            confidence_factors = []
            
            # V3 BOUNDED: Pattern confidence (limited)
            if patterns and len(patterns) > 0:
                valid_patterns = [p for p in patterns[:10] if hasattr(p, 'confidence')]  # Max 10
                if valid_patterns:
                    avg_pattern_confidence = sum(p.confidence for p in valid_patterns) / len(valid_patterns)
                    confidence_factors.append(avg_pattern_confidence * 30)  # Max 30 points
            
            # V3 BOUNDED: S/R confidence (limited)
            if sr_levels and len(sr_levels) > 0:
                valid_sr = [level for level in sr_levels[:10] if 'strength' in level]  # Max 10
                if valid_sr:
                    avg_sr_strength = sum(level['strength'] for level in valid_sr) / len(valid_sr)
                    confidence_factors.append(avg_sr_strength * 25)  # Max 25 points
            
            # V3 EFFICIENT: Indicator confluence
            if indicators:
                indicator_signals = 0
                total_indicators = 0
                
                # RSI signal strength
                rsi = indicators.get('rsi', 50)
                if rsi < 30 or rsi > 70:
                    indicator_signals += 1
                total_indicators += 1
                
                # MACD signal strength
                macd = indicators.get('macd', 0)
                macd_signal = indicators.get('macd_signal', 0)
                if abs(macd - macd_signal) > 0.01:  # Meaningful signal
                    indicator_signals += 1
                total_indicators += 1
                
                # Moving average confluence
                current_price = indicators.get('current_price', 0)
                sma_20 = indicators.get('sma_20', current_price)
                if current_price > 0 and sma_20 > 0:
                    if abs(current_price - sma_20) / sma_20 > 0.02:  # 2% away from MA
                        indicator_signals += 1
                total_indicators += 1
                
                if total_indicators > 0:
                    indicator_confidence = (indicator_signals / total_indicators) * 25  # Max 25 points
                    confidence_factors.append(indicator_confidence)
            
            # V3 BASE: Data quality factor
            confidence_factors.append(20)  # Base confidence for real data
            
            # V3 CALCULATE: Total confidence
            if confidence_factors:
                total_confidence = sum(confidence_factors)
                return min(100.0, max(0.0, total_confidence))
            
            return 50.0  # Default moderate confidence
            
        except Exception as e:
            logging.error(f"[V3-CONFIDENCE-ERROR] Error calculating confidence: {e}")
            return 50.0
    
    def _get_cached_analysis_v3(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """V3 Bounded cache lookup"""
        try:
            with self.cache_lock:
                if cache_key in self.analysis_cache:
                    cache_entry = self.analysis_cache[cache_key]
                    if time.time() - cache_entry['timestamp'] < 300:  # 5 minute cache
                        return cache_entry['data']
                    else:
                        del self.analysis_cache[cache_key]
            return None
        except Exception as e:
            logging.error(f"[V3-CACHE-ERROR] Error getting cached analysis: {e}")
            return None
    
    def _cache_analysis_v3(self, cache_key: str, analysis_data: Dict[str, Any]):
        """V3 Bounded cache storage"""
        try:
            with self.cache_lock:
                # V3 BOUNDED: Prevent memory leaks
                if len(self.analysis_cache) >= self.max_cache_size:
                    # Remove oldest entries (LRU-like)
                    oldest_keys = sorted(self.analysis_cache.keys(), 
                                       key=lambda k: self.analysis_cache[k]['timestamp'])
                    for key in oldest_keys[:20]:  # Remove 20 oldest
                        del self.analysis_cache[key]
                
                self.analysis_cache[cache_key] = {
                    'data': analysis_data,
                    'timestamp': time.time()
                }
        except Exception as e:
            logging.error(f"[V3-CACHE-ERROR] Error caching analysis: {e}")
    
    async def _store_analysis_results_v3(self, analysis_result: Dict[str, Any]):
        """V3 Optimized result storage"""
        try:
            # V3 EFFICIENT: Store only essential data
            overall_sentiment = analysis_result.get('overall_sentiment', 'neutral')
            confidence = analysis_result.get('confidence', 0.0)
            
            # V3 BOUNDED: Limit stored data size
            essential_data = {
                'symbol': analysis_result.get('symbol'),
                'sentiment': overall_sentiment,
                'confidence': confidence,
                'patterns_count': len(analysis_result.get('patterns', [])),
                'sr_levels_count': len(analysis_result.get('support_resistance', []))
            }
            
            analysis_data_json = json.dumps(essential_data)
            
            self.db_pool.execute_query_with_pool(
                '''INSERT INTO market_conditions 
                   (timestamp, overall_trend, volatility_level, volume_profile, 
                    sentiment_score, analysis_data)
                   VALUES (?, ?, ?, ?, ?, ?)''',
                (datetime.now(), overall_sentiment, 'normal', 'normal', 
                 confidence / 100.0, analysis_data_json)
            )
            
        except Exception as e:
            logging.error(f"[V3-STORE-ERROR] Error storing analysis results: {e}")
    
    def _update_analysis_stats_v3(self, success: bool, analysis_time: float, 
                                 patterns_count: int, sr_levels_count: int):
        """V3 Performance statistics tracking"""
        try:
            self.analysis_stats['total_analyses'] += 1
            
            if success:
                self.analysis_stats['successful_analyses'] += 1
                self.analysis_stats['patterns_detected'] += patterns_count
                self.analysis_stats['support_resistance_levels'] += sr_levels_count
            else:
                self.analysis_stats['failed_analyses'] += 1
            
            # V3 EFFICIENT: Rolling average update
            alpha = 0.1  # Smoothing factor
            if self.analysis_stats['avg_analysis_time'] == 0:
                self.analysis_stats['avg_analysis_time'] = analysis_time
            else:
                self.analysis_stats['avg_analysis_time'] = (
                    self.analysis_stats['avg_analysis_time'] * (1 - alpha) + 
                    analysis_time * alpha
                )
            
            # V3 CACHE: Update cache hit rate
            total_cache_requests = len(self.analysis_cache)
            if total_cache_requests > 0:
                self.analysis_stats['cache_hit_rate'] = min(1.0, total_cache_requests / 
                                                          max(self.analysis_stats['total_analyses'], 1))
            
            # V3 RESOURCE: Update resource metrics
            try:
                self.analysis_stats['memory_usage_mb'] = psutil.virtual_memory().used / 1024 / 1024
                self.analysis_stats['cpu_usage_percent'] = psutil.cpu_percent()
            except:
                pass
            
        except Exception as e:
            logging.error(f"[V3-STATS-ERROR] Error updating analysis stats: {e}")
    
    def _check_resource_usage(self):
        """V3 Resource monitoring and protection"""
        try:
            memory_percent = psutil.virtual_memory().percent
            cpu_percent = psutil.cpu_percent(interval=0.1)
            
            # V3 PROTECTION: Memory threshold check
            if memory_percent > self.resource_monitor['memory_warning_threshold']:
                logging.warning(f"[V3-MEMORY-WARNING] High memory usage: {memory_percent}%")
                
                # V3 CLEANUP: Aggressive cache clearing
                with self.cache_lock:
                    cache_keys = list(self.analysis_cache.keys())
                    for key in cache_keys[:len(cache_keys)//2]:  # Clear half
                        del self.analysis_cache[key]
                
                # V3 CLEANUP: Clear pattern cache
                with self.pattern_recognition.cache_lock:
                    pattern_keys = list(self.pattern_recognition.pattern_cache.keys())
                    for key in pattern_keys[:len(pattern_keys)//2]:
                        del self.pattern_recognition.pattern_cache[key]
            
            # V3 PROTECTION: CPU threshold check
            if cpu_percent > self.resource_monitor['cpu_warning_threshold']:
                logging.warning(f"[V3-CPU-WARNING] High CPU usage: {cpu_percent}%")
                # Add small delay to reduce CPU pressure
                time.sleep(0.1)
            
        except Exception as e:
            logging.error(f"[V3-RESOURCE-ERROR] Resource monitoring error: {e}")
    
    def _start_v3_background_tasks(self):
        """V3 Background monitoring and cleanup"""
        def v3_background_worker():
            while True:
                try:
                    # V3 CLEANUP: Regular maintenance
                    self._cleanup_caches_and_database_v3()
                    self._monitor_performance_v3()
                    self._log_v3_metrics()
                    
                    # V3 BOUNDED: Sleep with resource check
                    time.sleep(300)  # 5 minutes
                    
                except Exception as e:
                    logging.error(f"[V3-BACKGROUND-ERROR] Background task error: {e}")
                    time.sleep(120)  # Wait longer on error
        
        thread = threading.Thread(target=v3_background_worker, daemon=True, name="V3-Background")
        thread.start()
        logging.info("[V3-BACKGROUND] Background monitoring started")
    
    def _cleanup_caches_and_database_v3(self):
        """V3 Aggressive cleanup to prevent memory leaks"""
        try:
            current_time = time.time()
            
            # V3 MEMORY: Clean analysis cache
            with self.cache_lock:
                expired_keys = [
                    key for key, cache_entry in self.analysis_cache.items()
                    if current_time - cache_entry['timestamp'] > 600  # 10 minutes
                ]
                for key in expired_keys:
                    del self.analysis_cache[key]
            
            # V3 MEMORY: Clean pattern cache
            with self.pattern_recognition.cache_lock:
                expired_pattern_keys = [
                    key for key, cache_entry in self.pattern_recognition.pattern_cache.items()
                    if current_time - cache_entry['timestamp'] > 600
                ]
                for key in expired_pattern_keys:
                    del self.pattern_recognition.pattern_cache[key]
            
            # V3 BOUNDED: Trim deques
            while len(self.analysis_queue) > 50:
                self.analysis_queue.popleft()
            
            while len(self.performance_metrics) > 25:
                self.performance_metrics.popleft()
            
            while len(self.error_log) > 15:
                self.error_log.popleft()
            
            # V3 DATABASE: Clean old entries (weekly)
            if int(current_time) % (7 * 24 * 3600) < 300:  # Once per week
                cutoff_date = datetime.now() - timedelta(days=30)
                cleanup_queries = [
                    ('DELETE FROM market_patterns WHERE created_at < ?', (cutoff_date,)),
                    ('DELETE FROM market_conditions WHERE created_at < ?', (cutoff_date,)),
                    ('DELETE FROM analysis_cache WHERE expires_at < ?', (datetime.now(),))
                ]
                
                for query, params in cleanup_queries:
                    try:
                        self.db_pool.execute_query_with_pool(query, params)
                    except Exception as e:
                        logging.error(f"[V3-DB-CLEANUP-ERROR] {e}")
            
            logging.debug("[V3-CLEANUP] Cache and database cleanup completed")
            
        except Exception as e:
            logging.error(f"[V3-CLEANUP-ERROR] Cleanup error: {e}")
    
    def _monitor_performance_v3(self):
        """V3 Performance monitoring and alerts"""
        try:
            # V3 METRICS: Resource usage
            memory_info = psutil.virtual_memory()
            cpu_percent = psutil.cpu_percent()
            
            # V3 DATABASE: Pool performance
            pool_stats = self.db_pool.get_pool_stats()
            
            # V3 PATTERN: Pattern recognition stats
            pattern_stats = self.pattern_recognition.get_pattern_stats()
            
            # V3 ALERTS: Performance alerts
            if memory_info.percent > 90:
                logging.critical(f"[V3-CRITICAL] Very high memory usage: {memory_info.percent}%")
            elif memory_info.percent > 80:
                logging.warning(f"[V3-WARNING] High memory usage: {memory_info.percent}%")
            
            if cpu_percent > 90:
                logging.critical(f"[V3-CRITICAL] Very high CPU usage: {cpu_percent}%")
            elif cpu_percent > 75:
                logging.warning(f"[V3-WARNING] High CPU usage: {cpu_percent}%")
            
            if pool_stats.get('avg_wait_time', 0) > 3.0:
                logging.warning(f"[V3-DB-WARNING] High DB wait times: {pool_stats['avg_wait_time']:.2f}s")
            
            # V3 TRACK: Add to performance metrics deque (bounded)
            self.performance_metrics.append({
                'timestamp': datetime.now(),
                'memory_percent': memory_info.percent,
                'cpu_percent': cpu_percent,
                'db_wait_time': pool_stats.get('avg_wait_time', 0),
                'cache_size': len(self.analysis_cache),
                'pattern_cache_size': len(self.pattern_recognition.pattern_cache)
            })
            
        except Exception as e:
            logging.error(f"[V3-MONITOR-ERROR] Performance monitoring error: {e}")
    
    def _log_v3_metrics(self):
        """V3 Metrics logging"""
        try:
            stats = self.analysis_stats.copy()
            
            # V3 CALCULATE: Success rate
            success_rate = 0
            if stats['total_analyses'] > 0:
                success_rate = stats['successful_analyses'] / stats['total_analyses'] * 100
            
            # V3 CALCULATE: Data validation rate
            validation_rate = 0
            if stats['total_analyses'] > 0:
                validation_rate = stats['real_data_validations'] / stats['total_analyses'] * 100
            
            logging.info(f"[V3-METRICS] Analysis Engine Performance - "
                        f"Total: {stats['total_analyses']}, "
                        f"Success: {success_rate:.1f}%, "
                        f"Avg Time: {stats['avg_analysis_time']:.3f}s, "
                        f"Patterns: {stats['patterns_detected']}, "
                        f"S/R Levels: {stats['support_resistance_levels']}, "
                        f"Workers: {stats['thread_pool_workers']}, "
                        f"Real Data: {validation_rate:.1f}%, "
                        f"Mock Violations: {stats['mock_data_violations']}")
            
        except Exception as e:
            logging.error(f"[V3-METRICS-ERROR] Metrics logging error: {e}")
    
    def get_v3_performance_summary(self) -> Dict[str, Any]:
        """V3 Comprehensive performance summary"""
        try:
            # V3 CURRENT: Real-time metrics
            current_memory = psutil.virtual_memory()
            current_cpu = psutil.cpu_percent()
            
            summary = {
                'system_version': 'V3_PERFORMANCE_OPTIMIZED',
                'analysis_stats': self.analysis_stats.copy(),
                'database_pool_stats': self.db_pool.get_pool_stats(),
                'pattern_recognition_stats': self.pattern_recognition.get_pattern_stats(),
                'cache_stats': {
                    'analysis_cache_size': len(self.analysis_cache),
                    'max_analysis_cache_size': self.max_cache_size,
                    'pattern_cache_size': len(self.pattern_recognition.pattern_cache),
                    'max_pattern_cache_size': self.pattern_recognition.max_cache_size
                },
                'system_resources': {
                    'memory_percent': current_memory.percent,
                    'memory_used_gb': current_memory.used / (1024**3),
                    'memory_available_gb': current_memory.available / (1024**3),
                    'cpu_percent': current_cpu,
                    'cpu_count': psutil.cpu_count(),
                    'thread_pool_workers': self.executor._max_workers,
                    'max_thread_pool_workers': self.executor._max_workers
                },
                'v3_optimizations': {
                    'bounded_collections': True,
                    'real_data_validation': True,
                    'memory_leak_prevention': True,
                    'cpu_optimization': True,
                    'resource_monitoring': True
                },
                'performance_thresholds': self.resource_monitor,
                'last_updated': datetime.now().isoformat()
            }
            
            return summary
            
        except Exception as e:
            logging.error(f"[V3-SUMMARY-ERROR] Error getting performance summary: {e}")
            return {
                'system_version': 'V3_PERFORMANCE_OPTIMIZED',
                'error': str(e),
                'last_updated': datetime.now().isoformat()
            }
    
    def optimize_for_8cpu_24gb_server(self):
        """V3 Server-specific optimization for 8 vCPU / 24GB"""
        try:
            cpu_count = psutil.cpu_count()
            memory_gb = psutil.virtual_memory().total / (1024**3)
            
            logging.info(f"[V3-OPTIMIZE] Optimizing for {cpu_count} CPUs, {memory_gb:.1f}GB RAM")
            
            # V3 THREAD POOL: Re-optimize for detected specs
            if cpu_count <= 8:
                optimal_workers = min(cpu_count // 2, 6)  # Conservative for 8 CPU
                if self.executor._max_workers != optimal_workers:
                    old_workers = self.executor._max_workers
                    self.executor.shutdown(wait=False)
                    self.executor = concurrent.futures.ThreadPoolExecutor(
                        max_workers=optimal_workers,
                        thread_name_prefix="V3-MarketAnalysis-Optimized"
                    )
                    self.analysis_stats['thread_pool_workers'] = optimal_workers
                    logging.info(f"[V3-OPTIMIZE] Thread pool resized: {old_workers} -> {optimal_workers}")
            
            # V3 DATABASE: Adjust for high memory
            if memory_gb >= 20:
                if self.db_pool.max_connections < 20:
                    old_max = self.db_pool.max_connections
                    self.db_pool.max_connections = min(20, cpu_count * 3)
                    logging.info(f"[V3-OPTIMIZE] DB pool resized: {old_max} -> {self.db_pool.max_connections}")
            
            # V3 CACHE: Adjust cache sizes for available memory
            if memory_gb >= 24:
                self.max_cache_size = 200  # Increase cache for high memory
                self.pattern_recognition.max_cache_size = 150
                logging.info("[V3-OPTIMIZE] Cache sizes increased for 24GB+ RAM")
            
            # V3 RESOURCE: Update monitoring thresholds
            if memory_gb >= 24:
                self.resource_monitor['memory_warning_threshold'] = 80  # Higher threshold
                self.resource_monitor['max_memory_mb'] = 4000  # Allow more memory usage
            
            logging.info("[V3-OPTIMIZE] Server optimization completed")
            
        except Exception as e:
            logging.error(f"[V3-OPTIMIZE-ERROR] Server optimization error: {e}")
    
    def __del__(self):
        """V3 Cleanup resources"""
        try:
            if hasattr(self, 'db_pool'):
                self.db_pool.close_all_connections()
            if hasattr(self, 'executor'):
                self.executor.shutdown(wait=False)
            logging.info("[V3-CLEANUP] Market Analysis Engine resources cleaned up")
        except:
            pass

# V3 Export optimized classes
__all__ = [
    'V3MarketAnalysisEngine', 
    'OptimizedDatabaseConnectionPool', 
    'OptimizedPatternRecognition', 
    'MarketPattern',
    'V3RealDataValidator'
]

if __name__ == "__main__":
    # V3 Performance test for 8 vCPU / 24GB server
    async def test_v3_market_analysis():
        print("[V3-TEST] Testing V3 Market Analysis Engine - PERFORMANCE OPTIMIZED")
        
        engine = V3MarketAnalysisEngine()
        engine.optimize_for_8cpu_24gb_server()
        
        # V3 Test symbols
        symbols = ['BTCUSDT', 'ETHUSDT']
        
        for symbol in symbols:
            result = await engine.analyze_market_comprehensive_v3(symbol, ['1h', '4h'])
            print(f"[V3-RESULT] {symbol}: {result.get('overall_sentiment')} "
                  f"({result.get('confidence', 0):.1f}% confidence) "
                  f"in {result.get('analysis_time', 0):.3f}s")
        
        # V3 Performance summary
        summary = engine.get_v3_performance_summary()
        print(f"[V3-PERFORMANCE] Workers: {summary['system_resources']['thread_pool_workers']}, "
              f"Memory: {summary['system_resources']['memory_percent']:.1f}%, "
              f"CPU: {summary['system_resources']['cpu_percent']:.1f}%")
        
        print("[V3-TEST] V3 Market Analysis Engine test completed")
    
    # Run V3 test
    asyncio.run(test_v3_market_analysis())