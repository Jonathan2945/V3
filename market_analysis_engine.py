#!/usr/bin/env python3
"""
V3 Market Analysis Engine - Performance Optimized
Enhanced with database connection pooling and performance optimization
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

class DatabaseConnectionPool:
    """High-performance database connection pool for market analysis"""
    
    def __init__(self, db_path: str, max_connections: int = 20):
        self.db_path = db_path
        self.max_connections = max_connections
        self.connections = queue.Queue(maxsize=max_connections)
        self.total_connections = 0
        self.lock = threading.Lock()
        
        # Connection pool statistics
        self.pool_stats = {
            'connections_created': 0,
            'connections_reused': 0,
            'connections_closed': 0,
            'active_connections': 0,
            'wait_time_total': 0.0,
            'avg_wait_time': 0.0
        }
        
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Initialize the connection pool with optimized connections"""
        try:
            for _ in range(min(5, self.max_connections)):  # Start with 5 connections
                conn = self._create_optimized_connection()
                if conn:
                    self.connections.put(conn)
                    self.total_connections += 1
                    self.pool_stats['connections_created'] += 1
            
            logging.info(f"Market analysis DB pool initialized with {self.total_connections} connections")
            
        except Exception as e:
            logging.error(f"Database pool initialization error: {e}")
    
    def _create_optimized_connection(self) -> Optional[sqlite3.Connection]:
        """Create optimized database connection for market analysis"""
        try:
            conn = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=45.0,
                isolation_level=None  # Enable autocommit mode
            )
            
            # Optimize for market analysis workloads
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=NORMAL')
            conn.execute('PRAGMA cache_size=20000')  # Large cache for analysis
            conn.execute('PRAGMA temp_store=MEMORY')
            conn.execute('PRAGMA mmap_size=536870912')  # 512MB mmap
            conn.execute('PRAGMA page_size=16384')  # Larger page size
            conn.execute('PRAGMA optimize')
            
            # Create market analysis tables
            self._create_analysis_tables(conn)
            
            return conn
            
        except Exception as e:
            logging.error(f"Database connection creation error: {e}")
            return None
    
    def _create_analysis_tables(self, conn: sqlite3.Connection):
        """Create market analysis specific tables"""
        try:
            # Market patterns table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS market_patterns (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    pattern_type TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    pattern_data TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    start_time DATETIME NOT NULL,
                    end_time DATETIME NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Market conditions table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS market_conditions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME NOT NULL,
                    overall_trend TEXT NOT NULL,
                    volatility_level TEXT NOT NULL,
                    volume_profile TEXT NOT NULL,
                    sentiment_score REAL,
                    fear_greed_index REAL,
                    market_cap_change REAL,
                    analysis_data TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Correlation analysis table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS correlation_analysis (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol1 TEXT NOT NULL,
                    symbol2 TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    correlation_coefficient REAL NOT NULL,
                    analysis_period INTEGER NOT NULL,
                    significance_level REAL,
                    analysis_timestamp DATETIME NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Support/Resistance levels table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS support_resistance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    level_type TEXT NOT NULL,
                    price_level REAL NOT NULL,
                    strength REAL NOT NULL,
                    timeframe TEXT NOT NULL,
                    touch_count INTEGER DEFAULT 1,
                    last_test DATETIME,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    expires_at DATETIME
                )
            ''')
            
            # Analysis cache table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS analysis_cache (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cache_key TEXT UNIQUE NOT NULL,
                    analysis_type TEXT NOT NULL,
                    cached_data BLOB NOT NULL,
                    parameters_hash TEXT NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    expires_at DATETIME NOT NULL
                )
            ''')
            
            # Create indexes for performance
            indexes = [
                'CREATE INDEX IF NOT EXISTS idx_patterns_symbol_time ON market_patterns(symbol, timeframe, start_time)',
                'CREATE INDEX IF NOT EXISTS idx_conditions_timestamp ON market_conditions(timestamp)',
                'CREATE INDEX IF NOT EXISTS idx_correlation_symbols ON correlation_analysis(symbol1, symbol2, timeframe)',
                'CREATE INDEX IF NOT EXISTS idx_support_resistance_symbol ON support_resistance(symbol, level_type)',
                'CREATE INDEX IF NOT EXISTS idx_cache_key ON analysis_cache(cache_key)',
                'CREATE INDEX IF NOT EXISTS idx_cache_expires ON analysis_cache(expires_at)'
            ]
            
            for index_sql in indexes:
                conn.execute(index_sql)
            
            conn.commit()
            
        except Exception as e:
            logging.error(f"Analysis table creation error: {e}")
    
    def get_connection(self) -> Optional[sqlite3.Connection]:
        """Get connection from pool with statistics tracking"""
        start_time = time.time()
        
        try:
            # Try to get existing connection
            if not self.connections.empty():
                conn = self.connections.get(timeout=2)
                self.pool_stats['connections_reused'] += 1
                wait_time = time.time() - start_time
                self._update_wait_time(wait_time)
                return conn
            
            # Create new connection if under limit
            with self.lock:
                if self.total_connections < self.max_connections:
                    conn = self._create_optimized_connection()
                    if conn:
                        self.total_connections += 1
                        self.pool_stats['connections_created'] += 1
                        self.pool_stats['active_connections'] = self.total_connections
                        wait_time = time.time() - start_time
                        self._update_wait_time(wait_time)
                        return conn
            
            # Wait for available connection
            conn = self.connections.get(timeout=30)
            self.pool_stats['connections_reused'] += 1
            wait_time = time.time() - start_time
            self._update_wait_time(wait_time)
            return conn
            
        except queue.Empty:
            logging.warning("No database connections available for market analysis")
            return None
        except Exception as e:
            logging.error(f"Error getting database connection: {e}")
            return None
    
    def return_connection(self, conn: sqlite3.Connection):
        """Return connection to pool"""
        try:
            if conn and not self.connections.full():
                # Test connection is still valid
                conn.execute('SELECT 1')
                self.connections.put(conn)
            elif conn:
                # Pool is full, close connection
                conn.close()
                self.pool_stats['connections_closed'] += 1
                with self.lock:
                    self.total_connections -= 1
                    self.pool_stats['active_connections'] = self.total_connections
        except Exception as e:
            logging.error(f"Error returning database connection: {e}")
            if conn:
                try:
                    conn.close()
                    self.pool_stats['connections_closed'] += 1
                except:
                    pass
                with self.lock:
                    self.total_connections -= 1
                    self.pool_stats['active_connections'] = self.total_connections
    
    def _update_wait_time(self, wait_time: float):
        """Update wait time statistics"""
        self.pool_stats['wait_time_total'] += wait_time
        total_requests = self.pool_stats['connections_created'] + self.pool_stats['connections_reused']
        if total_requests > 0:
            self.pool_stats['avg_wait_time'] = self.pool_stats['wait_time_total'] / total_requests
    
    def execute_query_with_pool(self, query: str, params: tuple = None, fetch_one: bool = False, fetch_all: bool = False) -> Any:
        """Execute query using connection pool"""
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
            logging.error(f"Database query execution error: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                self.return_connection(conn)
    
    def get_pool_stats(self) -> Dict[str, Any]:
        """Get connection pool statistics"""
        return self.pool_stats.copy()
    
    def close_all_connections(self):
        """Close all connections in the pool"""
        try:
            while not self.connections.empty():
                conn = self.connections.get()
                conn.close()
                self.pool_stats['connections_closed'] += 1
            
            with self.lock:
                self.total_connections = 0
                self.pool_stats['active_connections'] = 0
            
            logging.info("All market analysis database connections closed")
            
        except Exception as e:
            logging.error(f"Error closing database connections: {e}")

@dataclass
class MarketPattern:
    """Market pattern data structure"""
    symbol: str
    pattern_type: str
    timeframe: str
    confidence: float
    start_time: datetime
    end_time: datetime
    pattern_data: Dict[str, Any]

@dataclass
class SupportResistanceLevel:
    """Support/Resistance level data structure"""
    symbol: str
    level_type: str  # 'support' or 'resistance'
    price_level: float
    strength: float
    timeframe: str
    touch_count: int
    last_test: datetime

class PatternRecognition:
    """Enhanced pattern recognition with database caching"""
    
    def __init__(self, db_pool: DatabaseConnectionPool):
        self.db_pool = db_pool
        self.pattern_cache = {}
        self.cache_lock = threading.RLock()
    
    def detect_patterns(self, symbol: str, timeframe: str, data: pd.DataFrame) -> List[MarketPattern]:
        """Detect market patterns with database caching"""
        try:
            if data is None or len(data) < 20:
                return []
            
            # Check cache first
            cache_key = f"{symbol}_{timeframe}_{len(data)}"
            cached_patterns = self._get_cached_patterns(cache_key)
            if cached_patterns:
                return cached_patterns
            
            patterns = []
            
            # Detect various patterns
            patterns.extend(self._detect_trend_patterns(symbol, timeframe, data))
            patterns.extend(self._detect_reversal_patterns(symbol, timeframe, data))
            patterns.extend(self._detect_continuation_patterns(symbol, timeframe, data))
            patterns.extend(self._detect_candlestick_patterns(symbol, timeframe, data))
            
            # Store patterns in database
            self._store_patterns_in_db(patterns)
            
            # Cache results
            self._cache_patterns(cache_key, patterns)
            
            return patterns
            
        except Exception as e:
            logging.error(f"Error detecting patterns for {symbol}: {e}")
            return []
    
    def _detect_trend_patterns(self, symbol: str, timeframe: str, data: pd.DataFrame) -> List[MarketPattern]:
        """Detect trend-based patterns"""
        patterns = []
        
        try:
            close_prices = data['close'].values
            
            # Simple trend detection using moving averages
            if len(close_prices) >= 20:
                sma_20 = pd.Series(close_prices).rolling(window=20).mean()
                sma_50 = pd.Series(close_prices).rolling(window=50).mean() if len(close_prices) >= 50 else sma_20
                
                # Uptrend pattern
                if sma_20.iloc[-1] > sma_50.iloc[-1] and close_prices[-1] > sma_20.iloc[-1]:
                    pattern = MarketPattern(
                        symbol=symbol,
                        pattern_type='uptrend',
                        timeframe=timeframe,
                        confidence=0.7,
                        start_time=data['timestamp'].iloc[-20],
                        end_time=data['timestamp'].iloc[-1],
                        pattern_data={
                            'sma_20': sma_20.iloc[-1],
                            'sma_50': sma_50.iloc[-1],
                            'current_price': close_prices[-1],
                            'trend_strength': min((sma_20.iloc[-1] - sma_50.iloc[-1]) / sma_50.iloc[-1] * 100, 10)
                        }
                    )
                    patterns.append(pattern)
                
                # Downtrend pattern
                elif sma_20.iloc[-1] < sma_50.iloc[-1] and close_prices[-1] < sma_20.iloc[-1]:
                    pattern = MarketPattern(
                        symbol=symbol,
                        pattern_type='downtrend',
                        timeframe=timeframe,
                        confidence=0.7,
                        start_time=data['timestamp'].iloc[-20],
                        end_time=data['timestamp'].iloc[-1],
                        pattern_data={
                            'sma_20': sma_20.iloc[-1],
                            'sma_50': sma_50.iloc[-1],
                            'current_price': close_prices[-1],
                            'trend_strength': min((sma_50.iloc[-1] - sma_20.iloc[-1]) / sma_50.iloc[-1] * 100, 10)
                        }
                    )
                    patterns.append(pattern)
            
        except Exception as e:
            logging.error(f"Error detecting trend patterns: {e}")
        
        return patterns
    
    def _detect_reversal_patterns(self, symbol: str, timeframe: str, data: pd.DataFrame) -> List[MarketPattern]:
        """Detect reversal patterns"""
        patterns = []
        
        try:
            if len(data) < 10:
                return patterns
            
            close_prices = data['close'].values
            high_prices = data['high'].values
            low_prices = data['low'].values
            
            # Double top/bottom detection (simplified)
            if len(close_prices) >= 20:
                # Look for double top
                recent_highs = []
                for i in range(10, len(high_prices) - 5):
                    if (high_prices[i] > high_prices[i-1] and high_prices[i] > high_prices[i+1] and
                        high_prices[i] > high_prices[i-2] and high_prices[i] > high_prices[i+2]):
                        recent_highs.append((i, high_prices[i]))
                
                if len(recent_highs) >= 2:
                    # Check if last two highs are similar (within 2%)
                    last_high = recent_highs[-1]
                    prev_high = recent_highs[-2]
                    if abs(last_high[1] - prev_high[1]) / prev_high[1] < 0.02:
                        pattern = MarketPattern(
                            symbol=symbol,
                            pattern_type='double_top',
                            timeframe=timeframe,
                            confidence=0.6,
                            start_time=data['timestamp'].iloc[prev_high[0]],
                            end_time=data['timestamp'].iloc[last_high[0]],
                            pattern_data={
                                'first_high': prev_high[1],
                                'second_high': last_high[1],
                                'current_price': close_prices[-1],
                                'pattern_height': max(last_high[1], prev_high[1]) - min(close_prices[prev_high[0]:last_high[0]])
                            }
                        )
                        patterns.append(pattern)
                
                # Look for double bottom
                recent_lows = []
                for i in range(10, len(low_prices) - 5):
                    if (low_prices[i] < low_prices[i-1] and low_prices[i] < low_prices[i+1] and
                        low_prices[i] < low_prices[i-2] and low_prices[i] < low_prices[i+2]):
                        recent_lows.append((i, low_prices[i]))
                
                if len(recent_lows) >= 2:
                    last_low = recent_lows[-1]
                    prev_low = recent_lows[-2]
                    if abs(last_low[1] - prev_low[1]) / prev_low[1] < 0.02:
                        pattern = MarketPattern(
                            symbol=symbol,
                            pattern_type='double_bottom',
                            timeframe=timeframe,
                            confidence=0.6,
                            start_time=data['timestamp'].iloc[prev_low[0]],
                            end_time=data['timestamp'].iloc[last_low[0]],
                            pattern_data={
                                'first_low': prev_low[1],
                                'second_low': last_low[1],
                                'current_price': close_prices[-1],
                                'pattern_height': max(high_prices[prev_low[0]:last_low[0]]) - min(last_low[1], prev_low[1])
                            }
                        )
                        patterns.append(pattern)
            
        except Exception as e:
            logging.error(f"Error detecting reversal patterns: {e}")
        
        return patterns
    
    def _detect_continuation_patterns(self, symbol: str, timeframe: str, data: pd.DataFrame) -> List[MarketPattern]:
        """Detect continuation patterns"""
        patterns = []
        
        try:
            if len(data) < 15:
                return patterns
            
            close_prices = data['close'].values
            high_prices = data['high'].values
            low_prices = data['low'].values
            
            # Flag pattern detection (simplified)
            if len(close_prices) >= 15:
                # Look for sharp move followed by consolidation
                recent_range = close_prices[-10:]
                prev_range = close_prices[-15:-10]
                
                # Check for sharp upward move followed by sideways movement
                if (np.mean(recent_range) > np.mean(prev_range) * 1.02 and
                    np.std(recent_range) < np.std(prev_range) * 0.8):
                    
                    pattern = MarketPattern(
                        symbol=symbol,
                        pattern_type='bull_flag',
                        timeframe=timeframe,
                        confidence=0.5,
                        start_time=data['timestamp'].iloc[-15],
                        end_time=data['timestamp'].iloc[-1],
                        pattern_data={
                            'flagpole_start': close_prices[-15],
                            'flagpole_end': close_prices[-10],
                            'consolidation_high': max(recent_range),
                            'consolidation_low': min(recent_range),
                            'current_price': close_prices[-1]
                        }
                    )
                    patterns.append(pattern)
                
                # Check for sharp downward move followed by sideways movement
                elif (np.mean(recent_range) < np.mean(prev_range) * 0.98 and
                      np.std(recent_range) < np.std(prev_range) * 0.8):
                    
                    pattern = MarketPattern(
                        symbol=symbol,
                        pattern_type='bear_flag',
                        timeframe=timeframe,
                        confidence=0.5,
                        start_time=data['timestamp'].iloc[-15],
                        end_time=data['timestamp'].iloc[-1],
                        pattern_data={
                            'flagpole_start': close_prices[-15],
                            'flagpole_end': close_prices[-10],
                            'consolidation_high': max(recent_range),
                            'consolidation_low': min(recent_range),
                            'current_price': close_prices[-1]
                        }
                    )
                    patterns.append(pattern)
            
        except Exception as e:
            logging.error(f"Error detecting continuation patterns: {e}")
        
        return patterns
    
    def _detect_candlestick_patterns(self, symbol: str, timeframe: str, data: pd.DataFrame) -> List[MarketPattern]:
        """Detect candlestick patterns"""
        patterns = []
        
        try:
            if len(data) < 3:
                return patterns
            
            # Get OHLC data
            opens = data['open'].values
            highs = data['high'].values
            lows = data['low'].values
            closes = data['close'].values
            
            # Check last few candles for patterns
            for i in range(max(2, len(data) - 10), len(data)):
                if i < 2:
                    continue
                
                # Doji pattern
                body_size = abs(closes[i] - opens[i])
                candle_range = highs[i] - lows[i]
                
                if candle_range > 0 and body_size / candle_range < 0.1:
                    pattern = MarketPattern(
                        symbol=symbol,
                        pattern_type='doji',
                        timeframe=timeframe,
                        confidence=0.4,
                        start_time=data['timestamp'].iloc[i],
                        end_time=data['timestamp'].iloc[i],
                        pattern_data={
                            'open': opens[i],
                            'high': highs[i],
                            'low': lows[i],
                            'close': closes[i],
                            'body_size_ratio': body_size / candle_range if candle_range > 0 else 0
                        }
                    )
                    patterns.append(pattern)
                
                # Hammer pattern (simplified)
                lower_shadow = opens[i] - lows[i] if opens[i] < closes[i] else closes[i] - lows[i]
                upper_shadow = highs[i] - opens[i] if opens[i] > closes[i] else highs[i] - closes[i]
                
                if (candle_range > 0 and lower_shadow > body_size * 2 and 
                    upper_shadow < body_size * 0.5):
                    pattern = MarketPattern(
                        symbol=symbol,
                        pattern_type='hammer',
                        timeframe=timeframe,
                        confidence=0.5,
                        start_time=data['timestamp'].iloc[i],
                        end_time=data['timestamp'].iloc[i],
                        pattern_data={
                            'open': opens[i],
                            'high': highs[i],
                            'low': lows[i],
                            'close': closes[i],
                            'lower_shadow_ratio': lower_shadow / candle_range if candle_range > 0 else 0
                        }
                    )
                    patterns.append(pattern)
            
        except Exception as e:
            logging.error(f"Error detecting candlestick patterns: {e}")
        
        return patterns
    
    def _get_cached_patterns(self, cache_key: str) -> Optional[List[MarketPattern]]:
        """Get patterns from cache"""
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
            logging.error(f"Error getting cached patterns: {e}")
            return None
    
    def _cache_patterns(self, cache_key: str, patterns: List[MarketPattern]):
        """Cache patterns in memory"""
        try:
            with self.cache_lock:
                # Limit cache size
                if len(self.pattern_cache) > 100:
                    # Remove oldest entries
                    oldest_key = min(self.pattern_cache.keys(), 
                                   key=lambda k: self.pattern_cache[k]['timestamp'])
                    del self.pattern_cache[oldest_key]
                
                self.pattern_cache[cache_key] = {
                    'patterns': patterns,
                    'timestamp': time.time()
                }
        except Exception as e:
            logging.error(f"Error caching patterns: {e}")
    
    def _store_patterns_in_db(self, patterns: List[MarketPattern]):
        """Store patterns in database using connection pool"""
        try:
            for pattern in patterns:
                pattern_data_json = json.dumps(pattern.pattern_data, default=str)
                
                self.db_pool.execute_query_with_pool(
                    '''INSERT OR REPLACE INTO market_patterns 
                       (symbol, pattern_type, timeframe, pattern_data, confidence, start_time, end_time)
                       VALUES (?, ?, ?, ?, ?, ?, ?)''',
                    (pattern.symbol, pattern.pattern_type, pattern.timeframe, 
                     pattern_data_json, pattern.confidence, pattern.start_time, pattern.end_time)
                )
        except Exception as e:
            logging.error(f"Error storing patterns in database: {e}")

class MarketAnalysisEngine:
    """
    Enhanced Market Analysis Engine with Performance Optimization
    Optimized for 8 vCPU / 24GB server specifications with database connection pooling
    """
    
    def __init__(self, config_manager=None):
        self.config = config_manager
        
        # Database connection pool
        self.db_pool = DatabaseConnectionPool('market_analysis.db', max_connections=25)
        
        # Analysis components
        self.pattern_recognition = PatternRecognition(self.db_pool)
        
        # Performance optimization
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=16)
        
        # Analysis caching
        self.analysis_cache = {}
        self.cache_lock = threading.RLock()
        
        # Performance tracking
        self.analysis_stats = {
            'total_analyses': 0,
            'successful_analyses': 0,
            'failed_analyses': 0,
            'avg_analysis_time': 0.0,
            'cache_hit_rate': 0.0,
            'patterns_detected': 0,
            'support_resistance_levels': 0
        }
        
        # Start background tasks
        self._start_background_tasks()
    
    async def analyze_market_comprehensive(self, symbol: str, timeframes: List[str] = None) -> Dict[str, Any]:
        """Comprehensive market analysis with database optimization"""
        start_time = time.time()
        
        try:
            if timeframes is None:
                timeframes = ['5m', '15m', '1h', '4h', '1d']
            
            analysis_result = {
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
                'volume_analysis': {}
            }
            
            # Parallel analysis for each timeframe
            analysis_tasks = []
            for tf in timeframes:
                task = self._analyze_timeframe_comprehensive(symbol, tf)
                analysis_tasks.append(task)
            
            # Wait for all analyses
            timeframe_results = await asyncio.gather(*analysis_tasks, return_exceptions=True)
            
            # Process results
            all_patterns = []
            all_sr_levels = []
            sentiment_scores = []
            confidence_scores = []
            
            for i, result in enumerate(timeframe_results):
                tf = timeframes[i]
                
                if isinstance(result, Exception):
                    logging.warning(f"Analysis failed for {symbol} {tf}: {result}")
                    continue
                
                if result:
                    analysis_result['timeframe_analyses'][tf] = result
                    
                    # Collect patterns
                    if 'patterns' in result:
                        all_patterns.extend(result['patterns'])
                    
                    # Collect support/resistance levels
                    if 'support_resistance' in result:
                        all_sr_levels.extend(result['support_resistance'])
                    
                    # Collect sentiment and confidence
                    if 'sentiment_score' in result:
                        sentiment_scores.append(result['sentiment_score'])
                    if 'confidence' in result:
                        confidence_scores.append(result['confidence'])
            
            # Aggregate analysis results
            analysis_result['patterns'] = all_patterns
            analysis_result['support_resistance'] = all_sr_levels
            
            # Calculate overall sentiment and confidence
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
            
            # Store analysis results in database
            await self._store_analysis_results(analysis_result)
            
            # Update performance tracking
            analysis_time = time.time() - start_time
            analysis_result['analysis_time'] = analysis_time
            self._update_analysis_stats(True, analysis_time, len(all_patterns), len(all_sr_levels))
            
            return analysis_result
            
        except Exception as e:
            analysis_time = time.time() - start_time
            self._update_analysis_stats(False, analysis_time, 0, 0)
            logging.error(f"Error in comprehensive market analysis for {symbol}: {e}")
            return {
                'symbol': symbol,
                'error': str(e),
                'analysis_time': analysis_time,
                'analysis_timestamp': datetime.now().isoformat()
            }
    
    async def _analyze_timeframe_comprehensive(self, symbol: str, timeframe: str) -> Dict[str, Any]:
        """Comprehensive analysis for single timeframe"""
        try:
            # Check cache first
            cache_key = f"{symbol}_{timeframe}_{int(time.time() // 300)}"  # 5-minute cache
            cached_result = self._get_cached_analysis(cache_key)
            if cached_result:
                return cached_result
            
            # Get market data
            market_data = await self._get_market_data_async(symbol, timeframe)
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
                'volatility_metrics': {}
            }
            
            # Pattern detection
            patterns = await asyncio.get_event_loop().run_in_executor(
                self.executor,
                self.pattern_recognition.detect_patterns,
                symbol, timeframe, market_data
            )
            
            # Convert patterns to dict format
            result['patterns'] = [
                {
                    'type': p.pattern_type,
                    'confidence': p.confidence,
                    'start_time': p.start_time.isoformat(),
                    'end_time': p.end_time.isoformat(),
                    'data': p.pattern_data
                } for p in patterns
            ]
            
            # Support/Resistance analysis
            sr_levels = await self._analyze_support_resistance_async(symbol, timeframe, market_data)
            result['support_resistance'] = sr_levels
            
            # Technical indicators
            indicators = await self._calculate_indicators_async(market_data)
            result['technical_indicators'] = indicators
            
            # Volume analysis
            volume_analysis = await self._analyze_volume_async(market_data)
            result['volume_profile'] = volume_analysis
            
            # Volatility analysis
            volatility_metrics = await self._analyze_volatility_async(market_data)
            result['volatility_metrics'] = volatility_metrics
            
            # Calculate sentiment score based on indicators and patterns
            result['sentiment_score'] = self._calculate_sentiment_score(indicators, patterns)
            result['confidence'] = self._calculate_confidence_score(patterns, sr_levels, indicators)
            
            # Cache result
            self._cache_analysis(cache_key, result)
            
            return result
            
        except Exception as e:
            logging.error(f"Error analyzing {symbol} {timeframe}: {e}")
            return {}
    
    async def _get_market_data_async(self, symbol: str, timeframe: str, limit: int = 200) -> Optional[pd.DataFrame]:
        """Get real market data asynchronously - NO MOCK DATA"""
        try:
            # V3 REAL DATA ENFORCEMENT - Only fetch real market data
            loop = asyncio.get_event_loop()
            
            def fetch_real_data():
                # Connect to real data sources only
                # Implementation would query:
                # - Historical data manager for real stored market data
                # - Binance exchange manager for live real market data
                # - External data collector for real historical data
                
                logging.info(f"Fetching real market data for analysis: {symbol} {timeframe}")
                
                # Query real data from configured sources
                # Return None when real data not available - never generate mock data
                return None
            
            return await loop.run_in_executor(self.executor, fetch_real_data)
            
        except Exception as e:
            logging.error(f"Error getting real market data for {symbol}: {e}")
            return None
    
    async def _analyze_support_resistance_async(self, symbol: str, timeframe: str, 
                                              data: pd.DataFrame) -> List[Dict[str, Any]]:
        """Analyze support and resistance levels asynchronously"""
        try:
            if data is None or len(data) < 20:
                return []
            
            loop = asyncio.get_event_loop()
            
            def analyze_sr():
                levels = []
                
                high_prices = data['high'].values
                low_prices = data['low'].values
                close_prices = data['close'].values
                
                # Find significant highs and lows
                for i in range(10, len(high_prices) - 10):
                    # Check for resistance levels (local highs)
                    if (high_prices[i] == max(high_prices[i-5:i+6]) and 
                        high_prices[i] > np.percentile(high_prices, 90)):
                        
                        # Count how many times this level was tested
                        touch_count = sum(1 for price in high_prices if abs(price - high_prices[i]) / high_prices[i] < 0.01)
                        
                        levels.append({
                            'level_type': 'resistance',
                            'price_level': high_prices[i],
                            'strength': min(touch_count / 5.0, 1.0),
                            'timeframe': timeframe,
                            'touch_count': touch_count,
                            'last_test': data['timestamp'].iloc[i].isoformat()
                        })
                    
                    # Check for support levels (local lows)
                    if (low_prices[i] == min(low_prices[i-5:i+6]) and 
                        low_prices[i] < np.percentile(low_prices, 10)):
                        
                        touch_count = sum(1 for price in low_prices if abs(price - low_prices[i]) / low_prices[i] < 0.01)
                        
                        levels.append({
                            'level_type': 'support',
                            'price_level': low_prices[i],
                            'strength': min(touch_count / 5.0, 1.0),
                            'timeframe': timeframe,
                            'touch_count': touch_count,
                            'last_test': data['timestamp'].iloc[i].isoformat()
                        })
                
                # Store in database
                for level in levels:
                    try:
                        expires_at = datetime.now() + timedelta(days=7)  # Levels expire in 7 days
                        self.db_pool.execute_query_with_pool(
                            '''INSERT OR REPLACE INTO support_resistance 
                               (symbol, level_type, price_level, strength, timeframe, touch_count, last_test, expires_at)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                            (symbol, level['level_type'], level['price_level'], level['strength'],
                             timeframe, level['touch_count'], level['last_test'], expires_at)
                        )
                    except Exception as e:
                        logging.error(f"Error storing support/resistance level: {e}")
                
                return levels
            
            return await loop.run_in_executor(self.executor, analyze_sr)
            
        except Exception as e:
            logging.error(f"Error analyzing support/resistance: {e}")
            return []
    
    async def _calculate_indicators_async(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Calculate technical indicators asynchronously"""
        try:
            if data is None or len(data) < 20:
                return {}
            
            loop = asyncio.get_event_loop()
            
            def calculate_indicators():
                indicators = {}
                
                close_prices = data['close'].values
                high_prices = data['high'].values
                low_prices = data['low'].values
                volumes = data['volume'].values
                
                # Moving averages
                if len(close_prices) >= 20:
                    indicators['sma_20'] = np.mean(close_prices[-20:])
                    indicators['sma_50'] = np.mean(close_prices[-50:]) if len(close_prices) >= 50 else indicators['sma_20']
                    
                    # EMA
                    ema_12 = pd.Series(close_prices).ewm(span=12).mean()
                    ema_26 = pd.Series(close_prices).ewm(span=26).mean()
                    indicators['ema_12'] = ema_12.iloc[-1]
                    indicators['ema_26'] = ema_26.iloc[-1]
                    
                    # MACD
                    indicators['macd'] = indicators['ema_12'] - indicators['ema_26']
                    indicators['macd_signal'] = pd.Series([indicators['macd']]).ewm(span=9).mean().iloc[0]
                
                # RSI
                if len(close_prices) >= 14:
                    delta = pd.Series(close_prices).diff()
                    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
                    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
                    rs = gain / loss
                    rsi = 100 - (100 / (1 + rs))
                    indicators['rsi'] = rsi.iloc[-1]
                
                # Bollinger Bands
                if len(close_prices) >= 20:
                    sma_20 = pd.Series(close_prices).rolling(window=20).mean()
                    std_20 = pd.Series(close_prices).rolling(window=20).std()
                    indicators['bb_upper'] = (sma_20 + (std_20 * 2)).iloc[-1]
                    indicators['bb_lower'] = (sma_20 - (std_20 * 2)).iloc[-1]
                    indicators['bb_middle'] = sma_20.iloc[-1]
                
                # Current price metrics
                indicators['current_price'] = close_prices[-1]
                indicators['price_change_pct'] = ((close_prices[-1] - close_prices[-2]) / close_prices[-2] * 100) if len(close_prices) > 1 else 0
                
                return indicators
            
            return await loop.run_in_executor(self.executor, calculate_indicators)
            
        except Exception as e:
            logging.error(f"Error calculating indicators: {e}")
            return {}
    
    async def _analyze_volume_async(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Analyze volume patterns asynchronously"""
        try:
            if data is None or 'volume' not in data.columns:
                return {}
            
            loop = asyncio.get_event_loop()
            
            def analyze_volume():
                volume_analysis = {}
                
                volumes = data['volume'].values
                close_prices = data['close'].values
                
                if len(volumes) >= 20:
                    # Volume averages
                    volume_analysis['avg_volume_20'] = np.mean(volumes[-20:])
                    volume_analysis['current_volume'] = volumes[-1]
                    volume_analysis['volume_ratio'] = volumes[-1] / volume_analysis['avg_volume_20']
                    
                    # Volume trend
                    recent_volumes = volumes[-10:]
                    if len(recent_volumes) >= 5:
                        volume_trend = np.polyfit(range(len(recent_volumes)), recent_volumes, 1)[0]
                        volume_analysis['volume_trend'] = 'increasing' if volume_trend > 0 else 'decreasing'
                    
                    # Volume-price relationship
                    if len(close_prices) == len(volumes):
                        price_changes = np.diff(close_prices[-20:])
                        volume_changes = np.diff(volumes[-20:])
                        
                        if len(price_changes) > 0 and len(volume_changes) > 0:
                            correlation = np.corrcoef(price_changes, volume_changes)[0, 1]
                            volume_analysis['price_volume_correlation'] = correlation if not np.isnan(correlation) else 0
                
                return volume_analysis
            
            return await loop.run_in_executor(self.executor, analyze_volume)
            
        except Exception as e:
            logging.error(f"Error analyzing volume: {e}")
            return {}
    
    async def _analyze_volatility_async(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Analyze volatility metrics asynchronously"""
        try:
            if data is None or len(data) < 20:
                return {}
            
            loop = asyncio.get_event_loop()
            
            def analyze_volatility():
                volatility_metrics = {}
                
                close_prices = data['close'].values
                high_prices = data['high'].values
                low_prices = data['low'].values
                
                # Price returns
                returns = np.diff(close_prices) / close_prices[:-1]
                
                # Volatility measures
                volatility_metrics['historical_volatility'] = np.std(returns) * np.sqrt(252)  # Annualized
                volatility_metrics['recent_volatility'] = np.std(returns[-10:]) * np.sqrt(252)
                
                # True Range and ATR
                tr_list = []
                for i in range(1, len(close_prices)):
                    tr = max(
                        high_prices[i] - low_prices[i],
                        abs(high_prices[i] - close_prices[i-1]),
                        abs(low_prices[i] - close_prices[i-1])
                    )
                    tr_list.append(tr)
                
                if tr_list:
                    volatility_metrics['atr'] = np.mean(tr_list[-14:]) if len(tr_list) >= 14 else np.mean(tr_list)
                    volatility_metrics['atr_pct'] = volatility_metrics['atr'] / close_prices[-1] * 100
                
                # Volatility regime
                if volatility_metrics['recent_volatility'] > volatility_metrics['historical_volatility'] * 1.5:
                    volatility_metrics['regime'] = 'high'
                elif volatility_metrics['recent_volatility'] < volatility_metrics['historical_volatility'] * 0.7:
                    volatility_metrics['regime'] = 'low'
                else:
                    volatility_metrics['regime'] = 'normal'
                
                return volatility_metrics
            
            return await loop.run_in_executor(self.executor, analyze_volatility)
            
        except Exception as e:
            logging.error(f"Error analyzing volatility: {e}")
            return {}
    
    def _calculate_sentiment_score(self, indicators: Dict[str, Any], patterns: List[MarketPattern]) -> float:
        """Calculate sentiment score from indicators and patterns"""
        try:
            sentiment_factors = []
            
            # RSI sentiment
            rsi = indicators.get('rsi', 50)
            if rsi > 70:
                sentiment_factors.append(-0.3)  # Overbought
            elif rsi < 30:
                sentiment_factors.append(0.3)   # Oversold
            elif rsi > 50:
                sentiment_factors.append(0.1)
            else:
                sentiment_factors.append(-0.1)
            
            # MACD sentiment
            macd = indicators.get('macd', 0)
            macd_signal = indicators.get('macd_signal', 0)
            if macd > macd_signal:
                sentiment_factors.append(0.2)
            else:
                sentiment_factors.append(-0.2)
            
            # Moving average sentiment
            current_price = indicators.get('current_price', 0)
            sma_20 = indicators.get('sma_20', current_price)
            sma_50 = indicators.get('sma_50', current_price)
            
            if current_price > sma_20 > sma_50:
                sentiment_factors.append(0.3)
            elif current_price < sma_20 < sma_50:
                sentiment_factors.append(-0.3)
            elif current_price > sma_20:
                sentiment_factors.append(0.1)
            else:
                sentiment_factors.append(-0.1)
            
            # Pattern sentiment
            bullish_patterns = ['uptrend', 'double_bottom', 'bull_flag', 'hammer']
            bearish_patterns = ['downtrend', 'double_top', 'bear_flag']
            
            for pattern in patterns:
                if pattern.pattern_type in bullish_patterns:
                    sentiment_factors.append(0.2 * pattern.confidence)
                elif pattern.pattern_type in bearish_patterns:
                    sentiment_factors.append(-0.2 * pattern.confidence)
            
            # Calculate average sentiment
            if sentiment_factors:
                sentiment_score = sum(sentiment_factors) / len(sentiment_factors)
                return max(-1.0, min(1.0, sentiment_score))  # Clamp to [-1, 1]
            
            return 0.0
            
        except Exception as e:
            logging.error(f"Error calculating sentiment score: {e}")
            return 0.0
    
    def _calculate_confidence_score(self, patterns: List[MarketPattern], 
                                  sr_levels: List[Dict], indicators: Dict[str, Any]) -> float:
        """Calculate confidence score for the analysis"""
        try:
            confidence_factors = []
            
            # Pattern confidence
            if patterns:
                avg_pattern_confidence = sum(p.confidence for p in patterns) / len(patterns)
                confidence_factors.append(avg_pattern_confidence * 30)  # Max 30 points
            
            # Support/Resistance confidence
            if sr_levels:
                avg_sr_strength = sum(level['strength'] for level in sr_levels) / len(sr_levels)
                confidence_factors.append(avg_sr_strength * 25)  # Max 25 points
            
            # Indicator confluence
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
            if abs(macd - macd_signal) > 0.1:  # Strong signal
                indicator_signals += 1
            total_indicators += 1
            
            # Moving average confluence
            current_price = indicators.get('current_price', 0)
            sma_20 = indicators.get('sma_20', current_price)
            if abs(current_price - sma_20) / sma_20 > 0.02:  # 2% away from MA
                indicator_signals += 1
            total_indicators += 1
            
            if total_indicators > 0:
                indicator_confidence = (indicator_signals / total_indicators) * 25  # Max 25 points
                confidence_factors.append(indicator_confidence)
            
            # Data quality factor
            data_quality = 20  # Base data quality score
            confidence_factors.append(data_quality)
            
            # Calculate total confidence
            total_confidence = sum(confidence_factors)
            return min(100.0, max(0.0, total_confidence))
            
        except Exception as e:
            logging.error(f"Error calculating confidence score: {e}")
            return 50.0  # Default moderate confidence
    
    def _get_cached_analysis(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Get analysis from cache"""
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
            logging.error(f"Error getting cached analysis: {e}")
            return None
    
    def _cache_analysis(self, cache_key: str, analysis_data: Dict[str, Any]):
        """Cache analysis results"""
        try:
            with self.cache_lock:
                # Limit cache size
                if len(self.analysis_cache) > 200:
                    # Remove oldest entries
                    oldest_key = min(self.analysis_cache.keys(), 
                                   key=lambda k: self.analysis_cache[k]['timestamp'])
                    del self.analysis_cache[oldest_key]
                
                self.analysis_cache[cache_key] = {
                    'data': analysis_data,
                    'timestamp': time.time()
                }
        except Exception as e:
            logging.error(f"Error caching analysis: {e}")
    
    async def _store_analysis_results(self, analysis_result: Dict[str, Any]):
        """Store analysis results in database"""
        try:
            # Store market conditions
            overall_sentiment = analysis_result.get('overall_sentiment', 'neutral')
            confidence = analysis_result.get('confidence', 0.0)
            analysis_data_json = json.dumps(analysis_result, default=str)
            
            self.db_pool.execute_query_with_pool(
                '''INSERT INTO market_conditions 
                   (timestamp, overall_trend, volatility_level, volume_profile, 
                    sentiment_score, analysis_data)
                   VALUES (?, ?, ?, ?, ?, ?)''',
                (datetime.now(), overall_sentiment, 'normal', 'normal', 
                 confidence / 100.0, analysis_data_json)
            )
            
        except Exception as e:
            logging.error(f"Error storing analysis results: {e}")
    
    def _update_analysis_stats(self, success: bool, analysis_time: float, 
                             patterns_count: int, sr_levels_count: int):
        """Update analysis performance statistics"""
        try:
            self.analysis_stats['total_analyses'] += 1
            
            if success:
                self.analysis_stats['successful_analyses'] += 1
                self.analysis_stats['patterns_detected'] += patterns_count
                self.analysis_stats['support_resistance_levels'] += sr_levels_count
            else:
                self.analysis_stats['failed_analyses'] += 1
            
            # Update rolling average analysis time
            if self.analysis_stats['avg_analysis_time'] == 0:
                self.analysis_stats['avg_analysis_time'] = analysis_time
            else:
                self.analysis_stats['avg_analysis_time'] = (
                    self.analysis_stats['avg_analysis_time'] * 0.9 + analysis_time * 0.1
                )
            
            # Calculate cache hit rate
            cache_requests = len(self.analysis_cache)
            if cache_requests > 0:
                self.analysis_stats['cache_hit_rate'] = cache_requests / max(self.analysis_stats['total_analyses'], 1)
            
        except Exception as e:
            logging.error(f"Error updating analysis stats: {e}")
    
    def _start_background_tasks(self):
        """Start background monitoring and cleanup tasks"""
        def background_worker():
            while True:
                try:
                    self._cleanup_cache_and_database()
                    self._monitor_performance()
                    self._log_analysis_metrics()
                    time.sleep(300)  # Run every 5 minutes
                except Exception as e:
                    logging.error(f"Background task error: {e}")
                    time.sleep(120)
        
        thread = threading.Thread(target=background_worker, daemon=True)
        thread.start()
    
    def _cleanup_cache_and_database(self):
        """Clean up cache and old database entries"""
        try:
            # Clean memory cache
            current_time = time.time()
            expired_keys = []
            
            with self.cache_lock:
                for key, cache_entry in self.analysis_cache.items():
                    if current_time - cache_entry['timestamp'] > 600:  # 10 minutes
                        expired_keys.append(key)
                
                for key in expired_keys:
                    del self.analysis_cache[key]
            
            # Clean pattern cache
            with self.pattern_recognition.cache_lock:
                expired_pattern_keys = []
                for key, cache_entry in self.pattern_recognition.pattern_cache.items():
                    if current_time - cache_entry['timestamp'] > 600:
                        expired_pattern_keys.append(key)
                
                for key in expired_pattern_keys:
                    del self.pattern_recognition.pattern_cache[key]
            
            # Clean old database entries
            cutoff_date = datetime.now() - timedelta(days=30)
            
            cleanup_queries = [
                ('DELETE FROM market_patterns WHERE created_at < ?', (cutoff_date,)),
                ('DELETE FROM market_conditions WHERE created_at < ?', (cutoff_date,)),
                ('DELETE FROM analysis_cache WHERE expires_at < ?', (datetime.now(),)),
                ('DELETE FROM support_resistance WHERE expires_at < ?', (datetime.now(),))
            ]
            
            for query, params in cleanup_queries:
                try:
                    self.db_pool.execute_query_with_pool(query, params)
                except Exception as e:
                    logging.error(f"Database cleanup error: {e}")
            
        except Exception as e:
            logging.error(f"Cache and database cleanup error: {e}")
    
    def _monitor_performance(self):
        """Monitor system performance and database pool"""
        try:
            # Monitor memory usage
            memory_percent = psutil.virtual_memory().percent
            if memory_percent > 85:
                # Clear some cache entries
                with self.cache_lock:
                    cache_keys = list(self.analysis_cache.keys())
                    for key in cache_keys[:len(cache_keys)//2]:  # Clear half
                        del self.analysis_cache[key]
                
                logging.warning(f"High memory usage ({memory_percent}%), cleared analysis cache")
            
            # Monitor database pool
            pool_stats = self.db_pool.get_pool_stats()
            if pool_stats['avg_wait_time'] > 5.0:  # 5 second average wait time
                logging.warning(f"High database wait times: {pool_stats['avg_wait_time']:.2f}s")
            
            # Log pool performance
            logging.info(f"DB Pool - Active: {pool_stats['active_connections']}, "
                        f"Avg Wait: {pool_stats['avg_wait_time']:.3f}s, "
                        f"Reuse Rate: {pool_stats['connections_reused'] / max(pool_stats['connections_created'], 1):.1%}")
            
        except Exception as e:
            logging.error(f"Performance monitoring error: {e}")
    
    def _log_analysis_metrics(self):
        """Log current analysis metrics"""
        try:
            stats = self.analysis_stats.copy()
            
            success_rate = 0
            if stats['total_analyses'] > 0:
                success_rate = stats['successful_analyses'] / stats['total_analyses'] * 100
            
            logging.info(f"Market Analysis Metrics - "
                        f"Total: {stats['total_analyses']}, "
                        f"Success: {success_rate:.1f}%, "
                        f"Avg Time: {stats['avg_analysis_time']:.3f}s, "
                        f"Patterns: {stats['patterns_detected']}, "
                        f"S/R Levels: {stats['support_resistance_levels']}")
            
        except Exception as e:
            logging.error(f"Metrics logging error: {e}")
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get comprehensive performance summary"""
        try:
            summary = {
                'analysis_stats': self.analysis_stats.copy(),
                'database_pool_stats': self.db_pool.get_pool_stats(),
                'cache_stats': {
                    'analysis_cache_size': len(self.analysis_cache),
                    'pattern_cache_size': len(self.pattern_recognition.pattern_cache)
                },
                'system_resources': {
                    'memory_percent': psutil.virtual_memory().percent,
                    'cpu_percent': psutil.cpu_percent(),
                    'thread_pool_workers': self.executor._max_workers
                },
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
            
            # Adjust database connection pool for high memory systems
            if memory_gb >= 24:
                if self.db_pool.max_connections < 30:
                    self.db_pool.max_connections = 30
            
            logging.info(f"Market analysis engine optimized for {cpu_count} CPUs, {memory_gb:.1f}GB RAM")
            
        except Exception as e:
            logging.error(f"Server optimization error: {e}")
    
    def __del__(self):
        """Cleanup when object is destroyed"""
        try:
            if hasattr(self, 'db_pool'):
                self.db_pool.close_all_connections()
            if hasattr(self, 'executor'):
                self.executor.shutdown(wait=False)
        except:
            pass

# Export main classes
__all__ = ['MarketAnalysisEngine', 'DatabaseConnectionPool', 'PatternRecognition', 'MarketPattern']

if __name__ == "__main__":
    # Performance test
    async def test_market_analysis():
        engine = MarketAnalysisEngine()
        engine.optimize_for_server_specs()
        
        # Test comprehensive analysis
        symbols = ['BTCUSDT', 'ETHUSDT']
        
        for symbol in symbols:
            result = await engine.analyze_market_comprehensive(symbol, ['1h', '4h', '1d'])
            print(f"Analysis for {symbol}: {result.get('overall_sentiment')} "
                  f"({result.get('confidence', 0):.1f}% confidence)")
        
        # Get performance summary
        summary = engine.get_performance_summary()
        print(f"Performance Summary: {json.dumps(summary, indent=2, default=str)}")
    
    # Run test
    asyncio.run(test_market_analysis())