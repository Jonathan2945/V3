#!/usr/bin/env python3
"""
V3 MARKET ANALYSIS ENGINE - FIXED CROSS-COMMUNICATION & REAL DATA ONLY
=======================================================================
FIXES APPLIED:
- Enhanced cross-communication with all V3 components
- Proper integration with external data collector
- Thread-safe operations and memory management
- Real data only compliance (no mock/simulated data)
- Better async/sync coordination
- Improved error handling and recovery
"""

import numpy as np
import pandas as pd
import logging
import asyncio
import threading
import weakref
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple, Union
from dataclasses import dataclass, asdict
import json
import sqlite3
import os
from binance.client import Client

# Import V3 components with error handling
try:
    from api_rotation_manager import get_api_key, report_api_result
except ImportError:
    logging.warning("API rotation manager not available")
    get_api_key = lambda x: None
    report_api_result = lambda *args, **kwargs: None

try:
    import talib
    TALIB_AVAILABLE = True
except ImportError:
    TALIB_AVAILABLE = False
    logging.warning("TA-Lib not available - using basic technical analysis")

@dataclass
class MarketAnalysis:
    """Market analysis result data structure"""
    symbol: str
    timeframe: str
    timestamp: str
    
    # Technical indicators
    rsi: float
    macd: float
    macd_signal: float
    bb_upper: float
    bb_lower: float
    bb_middle: float
    
    # Trend analysis
    trend_direction: str
    trend_strength: float
    support_level: float
    resistance_level: float
    
    # Volatility
    volatility: float
    atr: float
    
    # Volume analysis
    volume_ratio: float
    volume_trend: str
    
    # Overall signals
    buy_signals: int
    sell_signals: int
    neutral_signals: int
    overall_signal: str
    confidence: float
    
    # External data integration
    market_sentiment_factor: float
    news_sentiment_factor: float
    
    # Data source verification
    data_source: str = "REAL_BINANCE_DATA"
    live_data_only: bool = True

@dataclass
class MultiTimeframeAnalysis:
    """Multi-timeframe analysis result"""
    symbol: str
    primary_timeframe: str
    timeframes_analyzed: List[str]
    
    # Individual timeframe results
    timeframe_analyses: Dict[str, MarketAnalysis]
    
    # Confluence analysis
    confluence_strength: int
    confluence_direction: str
    confluence_confidence: float
    
    # Final recommendation
    recommended_action: str
    risk_level: str
    position_sizing_factor: float
    
    timestamp: str

class V3MarketAnalysisEngine:
    """V3 Market analysis engine with enhanced cross-communication"""
    
    def __init__(self, controller=None, trading_engine=None, external_data_collector=None):
        """Initialize with component references for cross-communication"""
        
        # Component references with weak references to prevent circular dependencies
        self.controller = weakref.ref(controller) if controller else None
        self.trading_engine = weakref.ref(trading_engine) if trading_engine else None
        self.external_data_collector = weakref.ref(external_data_collector) if external_data_collector else None
        
        # Initialize logger
        self.logger = logging.getLogger(f"{__name__}.V3MarketAnalysisEngine")
        
        # Thread safety
        self._analysis_lock = threading.Lock()
        self._cache_lock = threading.Lock()
        
        # Initialize Binance client with API rotation
        self.client = self._initialize_binance_client()
        
        # Configuration
        self.supported_timeframes = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']
        self.default_timeframe = '15m'
        self.confluence_timeframes = ['5m', '15m', '1h', '4h']
        
        # Analysis cache for performance
        self.analysis_cache = {}
        self.cache_ttl = 300  # 5 minutes
        
        # Database for analysis history
        self.db_path = "data/market_analysis.db"
        self._initialize_database()
        
        # Statistics tracking
        self.analysis_count = 0
        self.successful_predictions = 0
        self.total_predictions = 0
        
        self.logger.info("V3 Market Analysis Engine initialized - REAL DATA ONLY")
        self.logger.info(f"Cross-communication: Controller={'Yes' if self.controller else 'No'}, "
                        f"Trading Engine={'Yes' if self.trading_engine else 'No'}, "
                        f"External Data={'Yes' if self.external_data_collector else 'No'}")
        self.logger.info(f"TA-Lib available: {TALIB_AVAILABLE}")
    
    def _initialize_binance_client(self) -> Optional[Client]:
        """Initialize Binance client using API rotation system"""
        try:
            binance_creds = get_api_key('binance')
            if binance_creds:
                if isinstance(binance_creds, dict):
                    return Client(
                        binance_creds['api_key'], 
                        binance_creds['api_secret'], 
                        testnet=True
                    )
                else:
                    # Legacy format
                    api_secret = os.getenv('BINANCE_API_SECRET_1', '')
                    return Client(binance_creds, api_secret, testnet=True)
            return None
        except Exception as e:
            self.logger.error(f"Failed to initialize Binance client: {e}")
            return None
    
    def _initialize_database(self):
        """Initialize database for analysis history"""
        try:
            os.makedirs("data", exist_ok=True)
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Market analysis history
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS market_analysis_history (
                    id INTEGER PRIMARY KEY,
                    symbol TEXT,
                    timeframe TEXT,
                    timestamp TEXT,
                    
                    -- Technical indicators
                    rsi REAL,
                    macd REAL,
                    macd_signal REAL,
                    bb_upper REAL,
                    bb_lower REAL,
                    bb_middle REAL,
                    
                    -- Trend analysis
                    trend_direction TEXT,
                    trend_strength REAL,
                    support_level REAL,
                    resistance_level REAL,
                    
                    -- Volatility
                    volatility REAL,
                    atr REAL,
                    
                    -- Volume analysis
                    volume_ratio REAL,
                    volume_trend TEXT,
                    
                    -- Signals
                    buy_signals INTEGER,
                    sell_signals INTEGER,
                    neutral_signals INTEGER,
                    overall_signal TEXT,
                    confidence REAL,
                    
                    -- External factors
                    market_sentiment_factor REAL,
                    news_sentiment_factor REAL,
                    
                    -- Metadata
                    data_source TEXT DEFAULT 'REAL_BINANCE_DATA'
                )
            ''')
            
            # Multi-timeframe analysis history
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS mtf_analysis_history (
                    id INTEGER PRIMARY KEY,
                    symbol TEXT,
                    primary_timeframe TEXT,
                    timeframes_analyzed TEXT,
                    confluence_strength INTEGER,
                    confluence_direction TEXT,
                    confluence_confidence REAL,
                    recommended_action TEXT,
                    risk_level TEXT,
                    position_sizing_factor REAL,
                    timestamp TEXT,
                    data_source TEXT DEFAULT 'REAL_BINANCE_DATA'
                )
            ''')
            
            # Prediction tracking for model improvement
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS prediction_tracking (
                    id INTEGER PRIMARY KEY,
                    symbol TEXT,
                    predicted_direction TEXT,
                    predicted_confidence REAL,
                    actual_direction TEXT,
                    prediction_accuracy REAL,
                    timeframe TEXT,
                    prediction_time TEXT,
                    validation_time TEXT,
                    was_correct BOOLEAN
                )
            ''')
            
            conn.commit()
            conn.close()
            
            self.logger.info("Market analysis database initialized")
            
        except Exception as e:
            self.logger.error(f"Database initialization failed: {e}")
    
    async def get_real_market_data(self, symbol: str, timeframe: str, limit: int = 500) -> Optional[pd.DataFrame]:
        """Get real market data from Binance with error handling"""
        if not self.client:
            self.logger.error("No Binance client available")
            return None
        
        try:
            start_time = time.time()
            
            # Get real klines from Binance
            klines = self.client.get_historical_klines(
                symbol, timeframe, f"{limit} hours ago UTC"
            )
            
            if not klines:
                self.logger.warning(f"No real data received for {symbol} {timeframe}")
                return None
            
            # Convert to DataFrame
            df = pd.DataFrame(klines, columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_asset_volume', 'number_of_trades',
                'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
            ])
            
            # Process data
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = pd.to_numeric(df[col])
            
            df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].copy()
            df.set_index('timestamp', inplace=True)
            
            response_time = time.time() - start_time
            report_api_result('binance', success=True, response_time=response_time)
            
            self.logger.debug(f"Retrieved {len(df)} real candles for {symbol} {timeframe}")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to get real market data for {symbol} {timeframe}: {e}")
            report_api_result('binance', success=False, error_code=str(e))
            return None
    
    def calculate_technical_indicators(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate comprehensive technical indicators using real data"""
        try:
            if df is None or len(df) < 20:
                return {}
            
            indicators = {}
            
            # Price data
            high = df['high'].values
            low = df['low'].values
            close = df['close'].values
            volume = df['volume'].values
            
            if TALIB_AVAILABLE:
                # RSI
                indicators['rsi'] = talib.RSI(close, timeperiod=14)[-1] if len(close) >= 14 else 50.0
                
                # MACD
                macd, macd_signal, macd_hist = talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
                indicators['macd'] = macd[-1] if len(macd) > 0 and not np.isnan(macd[-1]) else 0.0
                indicators['macd_signal'] = macd_signal[-1] if len(macd_signal) > 0 and not np.isnan(macd_signal[-1]) else 0.0
                
                # Bollinger Bands
                bb_upper, bb_middle, bb_lower = talib.BBANDS(close, timeperiod=20, nbdevup=2, nbdevdn=2)
                indicators['bb_upper'] = bb_upper[-1] if len(bb_upper) > 0 and not np.isnan(bb_upper[-1]) else close[-1]
                indicators['bb_middle'] = bb_middle[-1] if len(bb_middle) > 0 and not np.isnan(bb_middle[-1]) else close[-1]
                indicators['bb_lower'] = bb_lower[-1] if len(bb_lower) > 0 and not np.isnan(bb_lower[-1]) else close[-1]
                
                # ATR
                indicators['atr'] = talib.ATR(high, low, close, timeperiod=14)[-1] if len(close) >= 14 else 0.0
                
                # Volume indicators
                indicators['volume_sma'] = talib.SMA(volume, timeperiod=20)[-1] if len(volume) >= 20 else volume[-1]
                
            else:
                # Fallback calculations without TA-Lib
                indicators.update(self._calculate_basic_indicators(df))
            
            # Custom calculations
            indicators['current_price'] = close[-1]
            indicators['volume_ratio'] = volume[-1] / indicators.get('volume_sma', volume[-1]) if indicators.get('volume_sma', 0) > 0 else 1.0
            
            # Support and resistance levels
            recent_highs = high[-20:] if len(high) >= 20 else high
            recent_lows = low[-20:] if len(low) >= 20 else low
            
            indicators['resistance_level'] = np.max(recent_highs)
            indicators['support_level'] = np.min(recent_lows)
            
            # Volatility
            returns = np.diff(np.log(close[-20:])) if len(close) >= 20 else np.array([0])
            indicators['volatility'] = np.std(returns) * np.sqrt(24) if len(returns) > 1 else 0.0
            
            return indicators
            
        except Exception as e:
            self.logger.error(f"Technical indicator calculation failed: {e}")
            return {}
    
    def _calculate_basic_indicators(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Basic indicator calculations without TA-Lib"""
        indicators = {}
        
        try:
            close = df['close']
            high = df['high']
            low = df['low']
            volume = df['volume']
            
            # RSI calculation
            delta = close.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            indicators['rsi'] = (100 - (100 / (1 + rs))).iloc[-1] if len(rs) > 0 else 50.0
            
            # Simple moving averages for MACD
            ema12 = close.ewm(span=12).mean()
            ema26 = close.ewm(span=26).mean()
            macd = ema12 - ema26
            indicators['macd'] = macd.iloc[-1] if len(macd) > 0 else 0.0
            indicators['macd_signal'] = macd.ewm(span=9).mean().iloc[-1] if len(macd) > 0 else 0.0
            
            # Bollinger Bands
            sma20 = close.rolling(20).mean()
            std20 = close.rolling(20).std()
            indicators['bb_middle'] = sma20.iloc[-1] if len(sma20) > 0 else close.iloc[-1]
            indicators['bb_upper'] = (sma20 + (std20 * 2)).iloc[-1] if len(sma20) > 0 else close.iloc[-1]
            indicators['bb_lower'] = (sma20 - (std20 * 2)).iloc[-1] if len(sma20) > 0 else close.iloc[-1]
            
            # ATR
            high_low = high - low
            high_close = np.abs(high - close.shift())
            low_close = np.abs(low - close.shift())
            ranges = np.maximum(high_low, np.maximum(high_close, low_close))
            indicators['atr'] = ranges.rolling(14).mean().iloc[-1] if len(ranges) > 0 else 0.0
            
            # Volume SMA
            indicators['volume_sma'] = volume.rolling(20).mean().iloc[-1] if len(volume) >= 20 else volume.iloc[-1]
            
        except Exception as e:
            self.logger.error(f"Basic indicator calculation failed: {e}")
        
        return indicators
    
    def analyze_trend(self, df: pd.DataFrame, indicators: Dict[str, Any]) -> Tuple[str, float]:
        """Analyze trend direction and strength using real data"""
        try:
            close = df['close'].values
            
            if len(close) < 20:
                return "NEUTRAL", 0.5
            
            # Multiple trend analysis methods
            trend_signals = []
            
            # Method 1: Price vs Moving Averages
            current_price = indicators.get('current_price', close[-1])
            bb_middle = indicators.get('bb_middle', current_price)
            
            if current_price > bb_middle:
                trend_signals.append(("BULLISH", 0.6))
            elif current_price < bb_middle:
                trend_signals.append(("BEARISH", 0.6))
            else:
                trend_signals.append(("NEUTRAL", 0.5))
            
            # Method 2: MACD
            macd = indicators.get('macd', 0)
            macd_signal = indicators.get('macd_signal', 0)
            
            if macd > macd_signal and macd > 0:
                trend_signals.append(("BULLISH", 0.7))
            elif macd < macd_signal and macd < 0:
                trend_signals.append(("BEARISH", 0.7))
            else:
                trend_signals.append(("NEUTRAL", 0.5))
            
            # Method 3: Recent price action
            recent_close = close[-5:]
            if len(recent_close) >= 5:
                if recent_close[-1] > recent_close[0]:
                    trend_signals.append(("BULLISH", 0.5))
                elif recent_close[-1] < recent_close[0]:
                    trend_signals.append(("BEARISH", 0.5))
                else:
                    trend_signals.append(("NEUTRAL", 0.5))
            
            # Aggregate trend signals
            bullish_strength = sum([s[1] for s in trend_signals if s[0] == "BULLISH"])
            bearish_strength = sum([s[1] for s in trend_signals if s[0] == "BEARISH"])
            neutral_strength = sum([s[1] for s in trend_signals if s[0] == "NEUTRAL"])
            
            if bullish_strength > bearish_strength and bullish_strength > neutral_strength:
                return "BULLISH", min(bullish_strength / len(trend_signals), 1.0)
            elif bearish_strength > bullish_strength and bearish_strength > neutral_strength:
                return "BEARISH", min(bearish_strength / len(trend_signals), 1.0)
            else:
                return "NEUTRAL", 0.5
                
        except Exception as e:
            self.logger.error(f"Trend analysis failed: {e}")
            return "NEUTRAL", 0.5
    
    def generate_trading_signals(self, indicators: Dict[str, Any], trend_direction: str, external_factors: Dict[str, float]) -> Tuple[int, int, int, str, float]:
        """Generate trading signals based on technical analysis and external factors"""
        try:
            buy_signals = 0
            sell_signals = 0
            neutral_signals = 0
            
            # Technical signals
            current_price = indicators.get('current_price', 0)
            rsi = indicators.get('rsi', 50)
            macd = indicators.get('macd', 0)
            macd_signal = indicators.get('macd_signal', 0)
            bb_upper = indicators.get('bb_upper', current_price)
            bb_lower = indicators.get('bb_lower', current_price)
            volume_ratio = indicators.get('volume_ratio', 1.0)
            
            # RSI signals
            if rsi < 30:  # Oversold
                buy_signals += 1
            elif rsi > 70:  # Overbought
                sell_signals += 1
            else:
                neutral_signals += 1
            
            # MACD signals
            if macd > macd_signal and macd > 0:
                buy_signals += 1
            elif macd < macd_signal and macd < 0:
                sell_signals += 1
            else:
                neutral_signals += 1
            
            # Bollinger Band signals
            if current_price <= bb_lower:  # Oversold
                buy_signals += 1
            elif current_price >= bb_upper:  # Overbought
                sell_signals += 1
            else:
                neutral_signals += 1
            
            # Volume confirmation
            if volume_ratio > 1.5:  # High volume
                if trend_direction == "BULLISH":
                    buy_signals += 1
                elif trend_direction == "BEARISH":
                    sell_signals += 1
            else:
                neutral_signals += 1
            
            # External factor adjustments
            market_sentiment = external_factors.get('market_sentiment_factor', 0.0)
            news_sentiment = external_factors.get('news_sentiment_factor', 0.0)
            
            # Adjust signals based on external sentiment
            if market_sentiment > 0.1 or news_sentiment > 0.1:
                buy_signals += 1
            elif market_sentiment < -0.1 or news_sentiment < -0.1:
                sell_signals += 1
            else:
                neutral_signals += 1
            
            # Determine overall signal
            total_signals = buy_signals + sell_signals + neutral_signals
            if total_signals == 0:
                return 0, 0, 1, "NEUTRAL", 0.5
            
            buy_ratio = buy_signals / total_signals
            sell_ratio = sell_signals / total_signals
            
            if buy_ratio >= 0.6:
                overall_signal = "BUY"
                confidence = buy_ratio
            elif sell_ratio >= 0.6:
                overall_signal = "SELL"
                confidence = sell_ratio
            else:
                overall_signal = "NEUTRAL"
                confidence = max(buy_ratio, sell_ratio)
            
            return buy_signals, sell_signals, neutral_signals, overall_signal, confidence
            
        except Exception as e:
            self.logger.error(f"Signal generation failed: {e}")
            return 0, 0, 1, "NEUTRAL", 0.5
    
    def get_external_factors(self) -> Dict[str, float]:
        """Get external factors from external data collector"""
        external_factors = {
            'market_sentiment_factor': 0.0,
            'news_sentiment_factor': 0.0
        }
        
        try:
            if self.external_data_collector and self.external_data_collector():
                collector = self.external_data_collector()
                latest_data = collector.get_latest_data()
                
                # Extract sentiment factors
                market_sentiment = latest_data.get('market_sentiment', {})
                news_sentiment = latest_data.get('news_sentiment', {})
                
                external_factors['market_sentiment_factor'] = market_sentiment.get('overall_sentiment', 0.0)
                external_factors['news_sentiment_factor'] = news_sentiment.get('sentiment_score', 0.0)
                
                self.logger.debug(f"External factors updated: {external_factors}")
                
        except Exception as e:
            self.logger.debug(f"Could not get external factors: {e}")
        
        return external_factors
    
    async def analyze_symbol(self, symbol: str, timeframe: str = None) -> Optional[MarketAnalysis]:
        """Perform comprehensive market analysis for a symbol"""
        if not timeframe:
            timeframe = self.default_timeframe
        
        # Check cache first
        cache_key = f"{symbol}_{timeframe}"
        with self._cache_lock:
            if cache_key in self.analysis_cache:
                cached_result, cache_time = self.analysis_cache[cache_key]
                if time.time() - cache_time < self.cache_ttl:
                    return cached_result
        
        try:
            self.logger.info(f"Analyzing {symbol} on {timeframe} timeframe with real data")
            
            # Get real market data
            df = await self.get_real_market_data(symbol, timeframe)
            if df is None or len(df) < 20:
                self.logger.warning(f"Insufficient real data for {symbol} {timeframe}")
                return None
            
            # Calculate technical indicators
            indicators = self.calculate_technical_indicators(df)
            if not indicators:
                self.logger.warning(f"Failed to calculate indicators for {symbol} {timeframe}")
                return None
            
            # Analyze trend
            trend_direction, trend_strength = self.analyze_trend(df, indicators)
            
            # Get external factors
            external_factors = self.get_external_factors()
            
            # Generate trading signals
            buy_signals, sell_signals, neutral_signals, overall_signal, confidence = self.generate_trading_signals(
                indicators, trend_direction, external_factors
            )
            
            # Determine volume trend
            volume_ratio = indicators.get('volume_ratio', 1.0)
            if volume_ratio > 1.2:
                volume_trend = "INCREASING"
            elif volume_ratio < 0.8:
                volume_trend = "DECREASING"
            else:
                volume_trend = "STABLE"
            
            # Create analysis result
            analysis = MarketAnalysis(
                symbol=symbol,
                timeframe=timeframe,
                timestamp=datetime.now().isoformat(),
                
                # Technical indicators
                rsi=indicators.get('rsi', 50.0),
                macd=indicators.get('macd', 0.0),
                macd_signal=indicators.get('macd_signal', 0.0),
                bb_upper=indicators.get('bb_upper', 0.0),
                bb_lower=indicators.get('bb_lower', 0.0),
                bb_middle=indicators.get('bb_middle', 0.0),
                
                # Trend analysis
                trend_direction=trend_direction,
                trend_strength=trend_strength,
                support_level=indicators.get('support_level', 0.0),
                resistance_level=indicators.get('resistance_level', 0.0),
                
                # Volatility
                volatility=indicators.get('volatility', 0.0),
                atr=indicators.get('atr', 0.0),
                
                # Volume analysis
                volume_ratio=volume_ratio,
                volume_trend=volume_trend,
                
                # Overall signals
                buy_signals=buy_signals,
                sell_signals=sell_signals,
                neutral_signals=neutral_signals,
                overall_signal=overall_signal,
                confidence=confidence,
                
                # External data integration
                market_sentiment_factor=external_factors.get('market_sentiment_factor', 0.0),
                news_sentiment_factor=external_factors.get('news_sentiment_factor', 0.0)
            )
            
            # Cache result
            with self._cache_lock:
                self.analysis_cache[cache_key] = (analysis, time.time())
            
            # Save to database
            self._save_analysis_to_database(analysis)
            
            # Cross-communication: Update controller if available
            self._update_controller_analysis(analysis)
            
            # Update statistics
            with self._analysis_lock:
                self.analysis_count += 1
            
            self.logger.info(f"Analysis completed for {symbol}: {overall_signal} (confidence: {confidence:.2f})")
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Analysis failed for {symbol} {timeframe}: {e}")
            return None
    
    async def analyze_multi_timeframe(self, symbol: str, timeframes: List[str] = None) -> Optional[MultiTimeframeAnalysis]:
        """Perform multi-timeframe confluence analysis"""
        if not timeframes:
            timeframes = self.confluence_timeframes
        
        try:
            self.logger.info(f"Multi-timeframe analysis for {symbol}: {timeframes}")
            
            # Analyze each timeframe
            timeframe_analyses = {}
            successful_analyses = 0
            
            for tf in timeframes:
                analysis = await self.analyze_symbol(symbol, tf)
                if analysis:
                    timeframe_analyses[tf] = analysis
                    successful_analyses += 1
                
                # Small delay to respect API limits
                await asyncio.sleep(0.1)
            
            if successful_analyses < 2:
                self.logger.warning(f"Insufficient timeframe analyses for {symbol}: {successful_analyses}/{len(timeframes)}")
                return None
            
            # Analyze confluence
            confluence_strength, confluence_direction, confluence_confidence = self._analyze_confluence(timeframe_analyses)
            
            # Determine recommendation
            recommended_action, risk_level, position_sizing_factor = self._generate_recommendation(
                confluence_strength, confluence_direction, confluence_confidence, timeframe_analyses
            )
            
            # Create multi-timeframe result
            mtf_analysis = MultiTimeframeAnalysis(
                symbol=symbol,
                primary_timeframe=timeframes[0],
                timeframes_analyzed=list(timeframe_analyses.keys()),
                timeframe_analyses=timeframe_analyses,
                confluence_strength=confluence_strength,
                confluence_direction=confluence_direction,
                confluence_confidence=confluence_confidence,
                recommended_action=recommended_action,
                risk_level=risk_level,
                position_sizing_factor=position_sizing_factor,
                timestamp=datetime.now().isoformat()
            )
            
            # Save to database
            self._save_mtf_analysis_to_database(mtf_analysis)
            
            # Cross-communication: Update trading engine
            self._update_trading_engine_analysis(mtf_analysis)
            
            self.logger.info(f"Multi-timeframe analysis completed for {symbol}: "
                           f"{recommended_action} (confluence: {confluence_strength}/{len(timeframes)})")
            
            return mtf_analysis
            
        except Exception as e:
            self.logger.error(f"Multi-timeframe analysis failed for {symbol}: {e}")
            return None
    
    def _analyze_confluence(self, timeframe_analyses: Dict[str, MarketAnalysis]) -> Tuple[int, str, float]:
        """Analyze confluence across timeframes"""
        try:
            buy_votes = 0
            sell_votes = 0
            neutral_votes = 0
            
            total_confidence = 0
            
            for tf, analysis in timeframe_analyses.items():
                if analysis.overall_signal == "BUY":
                    buy_votes += 1
                elif analysis.overall_signal == "SELL":
                    sell_votes += 1
                else:
                    neutral_votes += 1
                
                total_confidence += analysis.confidence
            
            total_votes = len(timeframe_analyses)
            avg_confidence = total_confidence / total_votes if total_votes > 0 else 0.5
            
            if buy_votes >= sell_votes and buy_votes >= neutral_votes:
                return buy_votes, "BUY", avg_confidence
            elif sell_votes >= buy_votes and sell_votes >= neutral_votes:
                return sell_votes, "SELL", avg_confidence
            else:
                return neutral_votes, "NEUTRAL", avg_confidence
                
        except Exception as e:
            self.logger.error(f"Confluence analysis failed: {e}")
            return 0, "NEUTRAL", 0.5
    
    def _generate_recommendation(self, confluence_strength: int, confluence_direction: str, 
                               confluence_confidence: float, timeframe_analyses: Dict[str, MarketAnalysis]) -> Tuple[str, str, float]:
        """Generate final trading recommendation"""
        try:
            total_timeframes = len(timeframe_analyses)
            
            # Determine action
            if confluence_strength >= total_timeframes * 0.7 and confluence_confidence >= 0.7:
                recommended_action = confluence_direction
                risk_level = "LOW"
                position_sizing_factor = 1.0
            elif confluence_strength >= total_timeframes * 0.6 and confluence_confidence >= 0.6:
                recommended_action = confluence_direction
                risk_level = "MEDIUM"
                position_sizing_factor = 0.7
            elif confluence_strength >= total_timeframes * 0.5:
                recommended_action = confluence_direction
                risk_level = "HIGH"
                position_sizing_factor = 0.5
            else:
                recommended_action = "HOLD"
                risk_level = "HIGH"
                position_sizing_factor = 0.3
            
            return recommended_action, risk_level, position_sizing_factor
            
        except Exception as e:
            self.logger.error(f"Recommendation generation failed: {e}")
            return "HOLD", "HIGH", 0.3
    
    def _save_analysis_to_database(self, analysis: MarketAnalysis):
        """Save single timeframe analysis to database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO market_analysis_history (
                    symbol, timeframe, timestamp, rsi, macd, macd_signal,
                    bb_upper, bb_lower, bb_middle, trend_direction, trend_strength,
                    support_level, resistance_level, volatility, atr, volume_ratio,
                    volume_trend, buy_signals, sell_signals, neutral_signals,
                    overall_signal, confidence, market_sentiment_factor, news_sentiment_factor
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                analysis.symbol, analysis.timeframe, analysis.timestamp,
                analysis.rsi, analysis.macd, analysis.macd_signal,
                analysis.bb_upper, analysis.bb_lower, analysis.bb_middle,
                analysis.trend_direction, analysis.trend_strength,
                analysis.support_level, analysis.resistance_level,
                analysis.volatility, analysis.atr, analysis.volume_ratio,
                analysis.volume_trend, analysis.buy_signals, analysis.sell_signals,
                analysis.neutral_signals, analysis.overall_signal, analysis.confidence,
                analysis.market_sentiment_factor, analysis.news_sentiment_factor
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Failed to save analysis to database: {e}")
    
    def _save_mtf_analysis_to_database(self, mtf_analysis: MultiTimeframeAnalysis):
        """Save multi-timeframe analysis to database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO mtf_analysis_history (
                    symbol, primary_timeframe, timeframes_analyzed, confluence_strength,
                    confluence_direction, confluence_confidence, recommended_action,
                    risk_level, position_sizing_factor, timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                mtf_analysis.symbol, mtf_analysis.primary_timeframe,
                json.dumps(mtf_analysis.timeframes_analyzed),
                mtf_analysis.confluence_strength, mtf_analysis.confluence_direction,
                mtf_analysis.confluence_confidence, mtf_analysis.recommended_action,
                mtf_analysis.risk_level, mtf_analysis.position_sizing_factor,
                mtf_analysis.timestamp
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Failed to save MTF analysis to database: {e}")
    
    def _update_controller_analysis(self, analysis: MarketAnalysis):
        """Update controller with analysis results"""
        if not self.controller or not self.controller():
            return
        
        try:
            controller = self.controller()
            
            # Update analysis statistics in controller
            with controller._state_lock:
                # Update system resources with analysis activity
                controller.system_resources['data_points_processed'] += 1
                
                # Store latest analysis (could be used by dashboard)
                if not hasattr(controller, 'latest_analysis'):
                    controller.latest_analysis = {}
                
                controller.latest_analysis[analysis.symbol] = asdict(analysis)
                
        except Exception as e:
            self.logger.debug(f"Could not update controller with analysis: {e}")
    
    def _update_trading_engine_analysis(self, mtf_analysis: MultiTimeframeAnalysis):
        """Update trading engine with multi-timeframe analysis"""
        if not self.trading_engine or not self.trading_engine():
            return
        
        try:
            engine = self.trading_engine()
            
            # Update engine with analysis results for decision making
            if hasattr(engine, 'market_analysis'):
                engine.market_analysis = {
                    'symbol': mtf_analysis.symbol,
                    'recommendation': mtf_analysis.recommended_action,
                    'confidence': mtf_analysis.confluence_confidence,
                    'risk_level': mtf_analysis.risk_level,
                    'position_sizing_factor': mtf_analysis.position_sizing_factor,
                    'confluence_strength': mtf_analysis.confluence_strength,
                    'total_timeframes': len(mtf_analysis.timeframes_analyzed)
                }
                
        except Exception as e:
            self.logger.debug(f"Could not update trading engine with analysis: {e}")
    
    def get_analysis_statistics(self) -> Dict[str, Any]:
        """Get analysis engine statistics"""
        with self._analysis_lock:
            win_rate = (self.successful_predictions / max(1, self.total_predictions)) * 100
            
            return {
                'total_analyses': self.analysis_count,
                'successful_predictions': self.successful_predictions,
                'total_predictions': self.total_predictions,
                'prediction_accuracy': win_rate,
                'cache_size': len(self.analysis_cache),
                'supported_timeframes': len(self.supported_timeframes),
                'talib_available': TALIB_AVAILABLE,
                'binance_connected': bool(self.client),
                'cross_communication_active': bool(self.controller or self.trading_engine or self.external_data_collector),
                'real_data_only': True
            }

# Legacy compatibility
class MarketAnalysisEngine(V3MarketAnalysisEngine):
    """Legacy compatibility wrapper"""
    pass

if __name__ == "__main__":
    # Test the V3 market analysis engine
    import asyncio
    
    async def test_v3_analysis():
        print("Testing V3 Market Analysis Engine - REAL DATA & CROSS-COMMUNICATION")
        print("=" * 75)
        
        engine = V3MarketAnalysisEngine()
        
        # Test single timeframe analysis
        analysis = await engine.analyze_symbol('BTCUSDT', '15m')
        if analysis:
            print(f"? Single analysis: {analysis.symbol} - {analysis.overall_signal} (conf: {analysis.confidence:.2f})")
        
        # Test multi-timeframe analysis
        mtf_analysis = await engine.analyze_multi_timeframe('BTCUSDT', ['5m', '15m', '1h'])
        if mtf_analysis:
            print(f"? MTF analysis: {mtf_analysis.recommended_action} (confluence: {mtf_analysis.confluence_strength}/3)")
        
        # Get statistics
        stats = engine.get_analysis_statistics()
        print(f"? Statistics: {stats['total_analyses']} analyses completed")
        print(f"? Cross-communication: {stats['cross_communication_active']}")
        print(f"? Real data only: {stats['real_data_only']}")
        
        print("V3 Market Analysis Engine test completed")
    
    asyncio.run(test_v3_analysis())