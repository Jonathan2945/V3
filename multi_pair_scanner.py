#!/usr/bin/env python3
"""
MULTI-PAIR OPPORTUNITY SCANNER
==============================
Scans all Binance.US trading pairs for opportunities across multiple timeframes
Features:
- Real-time scanning of all tradeable pairs
- Volume spike detection
- Price momentum analysis
- Technical indicator calculations
- Opportunity scoring and ranking
- Multi-timeframe confirmation
"""

import os
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
import numpy as np
import pandas as pd
from binance.client import Client
from binance.exceptions import BinanceAPIException
import time
import statistics
from dotenv import load_dotenv
from api_rotation_manager import get_api_key, report_api_result
from binance_exchange_manager import exchange_manager, get_tradeable_pairs

# Load environment variables
load_dotenv()

@dataclass
class MarketData:
    """Market data for a trading pair"""
    symbol: str
    price: float
    price_change_24h: float
    price_change_percent_24h: float
    volume_24h: float
    high_24h: float
    low_24h: float
    open_time: datetime
    close_time: datetime
    trades_count: int
    quote_volume: float

@dataclass
class TechnicalIndicators:
    """Technical indicators for analysis"""
    rsi: float
    macd: float
    macd_signal: float
    bb_upper: float
    bb_lower: float
    bb_middle: float
    volume_sma: float
    price_sma_short: float
    price_sma_long: float
    atr: float
    stoch_k: float
    stoch_d: float

@dataclass
class OpportunitySignal:
    """Trading opportunity signal"""
    symbol: str
    signal_type: str  # 'BUY', 'SELL', 'HOLD'
    confidence: float
    opportunity_score: float
    reasons: List[str]
    timeframe: str
    price: float
    volume: float
    technical_indicators: TechnicalIndicators
    market_data: MarketData
    risk_reward_ratio: float
    entry_price: float
    stop_loss: float
    take_profit: float
    timestamp: datetime

class MultiPairScanner:
    """Multi-pair opportunity scanner for Binance.US"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Configuration from .env
        self.scanner_enabled = os.getenv('OPPORTUNITY_SCANNER_ENABLED', 'true').lower() == 'true'
        self.scan_interval = int(os.getenv('SCAN_INTERVAL_SECONDS', '30'))
        self.min_opportunity_score = float(os.getenv('MIN_OPPORTUNITY_SCORE', '75.0'))
        self.volume_spike_threshold = float(os.getenv('VOLUME_SPIKE_THRESHOLD', '2.0'))
        self.price_momentum_threshold = float(os.getenv('PRICE_MOMENTUM_THRESHOLD', '1.5'))
        self.news_sentiment_weight = float(os.getenv('NEWS_SENTIMENT_WEIGHT', '0.3'))
        
        # Multi-timeframe configuration
        self.timeframes = os.getenv('TIMEFRAMES', '1m,5m,15m,30m,1h,4h,1d').split(',')
        self.timeframes = [tf.strip() for tf in self.timeframes if tf.strip()]
        self.primary_timeframe = os.getenv('PRIMARY_TIMEFRAME', '15m')
        self.confirm_timeframes = os.getenv('CONFIRM_TIMEFRAMES', '5m,1h').split(',')
        self.confirm_timeframes = [tf.strip() for tf in self.confirm_timeframes if tf.strip()]
        
        # Risk management
        self.max_concurrent_pairs = int(os.getenv('MAX_CONCURRENT_PAIRS', '50'))
        self.min_volume_24h = float(os.getenv('MIN_VOLUME_24H', '100000'))
        self.max_correlation_threshold = float(os.getenv('MAX_CORRELATION_THRESHOLD', '0.7'))
        
        # Scanner state
        self.client: Optional[Client] = None
        self.is_testnet = os.getenv('TESTNET', 'true').lower() == 'true'
        self.scanning_task: Optional[asyncio.Task] = None
        self.is_scanning = False
        self.last_scan_time = datetime.min
        
        # Data storage
        self.market_data_cache: Dict[str, MarketData] = {}
        self.historical_data_cache: Dict[str, Dict[str, pd.DataFrame]] = {}
        self.opportunity_signals: Dict[str, List[OpportunitySignal]] = {}
        self.pair_correlations: Dict[Tuple[str, str], float] = {}
        
        # Performance tracking
        self.scan_performance = {
            'total_scans': 0,
            'successful_scans': 0,
            'opportunities_found': 0,
            'avg_scan_time': 0.0,
            'last_scan_duration': 0.0
        }
        
        # Initialize client
        self._initialize_client()
        
        self.logger.info(f"[SCANNER] Multi-pair scanner initialized (testnet={self.is_testnet})")
        self.logger.info(f"[SCANNER] Timeframes: {self.timeframes}")
        self.logger.info(f"[SCANNER] Primary TF: {self.primary_timeframe}")
    
    def _initialize_client(self):
        """Initialize Binance client with rotation support"""
        try:
            if self.is_testnet:
                binance_creds = get_api_key('binance')
            else:
                binance_creds = get_api_key('binance_live')
            
            if not binance_creds:
                self.logger.warning("[SCANNER] No valid Binance credentials found")
                return
            
            if isinstance(binance_creds, dict):
                api_key = binance_creds.get('api_key')
                api_secret = binance_creds.get('api_secret')
            else:
                self.logger.warning("[SCANNER] Invalid credential format")
                return
            
            if not api_key or not api_secret:
                self.logger.warning("[SCANNER] Incomplete Binance credentials")
                return
            
            # Initialize client
            if self.is_testnet:
                self.client = Client(api_key, api_secret, testnet=True)
                self.logger.info("[SCANNER] Connected to Binance testnet")
            else:
                self.client = Client(api_key, api_secret, testnet=False, tld='us')
                self.logger.info("[SCANNER] Connected to Binance.US live")
        
        except Exception as e:
            self.logger.error(f"[SCANNER] Failed to initialize client: {e}")
            self.client = None
    
    async def start_scanning(self):
        """Start the multi-pair scanning process"""
        if not self.scanner_enabled or not self.client:
            self.logger.warning("[SCANNER] Scanner not enabled or client not available")
            return False
        
        if self.is_scanning:
            self.logger.warning("[SCANNER] Scanner already running")
            return True
        
        try:
            self.is_scanning = True
            self.scanning_task = asyncio.create_task(self._scanning_loop())
            
            self.logger.info("[SCANNER] Multi-pair scanning started")
            return True
        
        except Exception as e:
            self.logger.error(f"[SCANNER] Failed to start scanning: {e}")
            self.is_scanning = False
            return False
    
    async def stop_scanning(self):
        """Stop the scanning process"""
        self.is_scanning = False
        
        if self.scanning_task and not self.scanning_task.done():
            self.scanning_task.cancel()
            try:
                await self.scanning_task
            except asyncio.CancelledError:
                pass
        
        self.logger.info("[SCANNER] Multi-pair scanning stopped")
    
    async def _scanning_loop(self):
        """Main scanning loop"""
        while self.is_scanning:
            try:
                scan_start = time.time()
                
                # Perform scan
                await self._perform_full_scan()
                
                # Update performance metrics
                scan_duration = time.time() - scan_start
                self.scan_performance['total_scans'] += 1
                self.scan_performance['successful_scans'] += 1
                self.scan_performance['last_scan_duration'] = scan_duration
                self.scan_performance['avg_scan_time'] = (
                    (self.scan_performance['avg_scan_time'] * (self.scan_performance['total_scans'] - 1) + scan_duration)
                    / self.scan_performance['total_scans']
                )
                
                self.last_scan_time = datetime.now()
                
                # Log progress
                if self.scan_performance['total_scans'] % 10 == 0:
                    self.logger.info(
                        f"[SCANNER] Scan {self.scan_performance['total_scans']} completed in {scan_duration:.2f}s. "
                        f"Found {len(self.opportunity_signals)} opportunities"
                    )
                
                # Wait for next scan
                await asyncio.sleep(self.scan_interval)
            
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"[SCANNER] Error in scanning loop: {e}")
                await asyncio.sleep(min(self.scan_interval, 60))  # Wait at least 1 minute on error
    
    async def _perform_full_scan(self):
        """Perform full scan of all tradeable pairs"""
        try:
            # Get tradeable pairs
            pairs = get_tradeable_pairs()
            if not pairs:
                self.logger.warning("[SCANNER] No tradeable pairs available")
                return
            
            # Limit pairs to scan
            pairs_to_scan = pairs[:self.max_concurrent_pairs]
            
            # Update market data for all pairs
            await self._update_market_data(pairs_to_scan)
            
            # Filter pairs by volume and other criteria
            filtered_pairs = self._filter_pairs_by_criteria(pairs_to_scan)
            
            # Scan each pair for opportunities
            opportunities = []
            
            for symbol in filtered_pairs:
                try:
                    pair_opportunities = await self._scan_pair_opportunities(symbol)
                    opportunities.extend(pair_opportunities)
                except Exception as e:
                    self.logger.debug(f"[SCANNER] Error scanning {symbol}: {e}")
                    continue
            
            # Update opportunity signals
            self._update_opportunity_signals(opportunities)
            
            # Calculate pair correlations
            await self._update_pair_correlations(filtered_pairs)
            
        except Exception as e:
            self.logger.error(f"[SCANNER] Error in full scan: {e}")
    
    async def _update_market_data(self, pairs: List[str]):
        """Update 24hr market data for all pairs"""
        if not self.client:
            return
        
        try:
            start_time = time.time()
            
            # Get 24hr ticker stats for all pairs
            tickers = self.client.get_ticker()
            
            response_time = time.time() - start_time
            
            # Report API call
            service_name = 'binance' if self.is_testnet else 'binance_live'
            report_api_result(service_name, success=True, response_time=response_time)
            
            # Process ticker data
            for ticker in tickers:
                symbol = ticker['symbol']
                if symbol not in pairs:
                    continue
                
                try:
                    market_data = MarketData(
                        symbol=symbol,
                        price=float(ticker['lastPrice']),
                        price_change_24h=float(ticker['priceChange']),
                        price_change_percent_24h=float(ticker['priceChangePercent']),
                        volume_24h=float(ticker['volume']),
                        high_24h=float(ticker['highPrice']),
                        low_24h=float(ticker['lowPrice']),
                        open_time=datetime.fromtimestamp(ticker['openTime'] / 1000),
                        close_time=datetime.fromtimestamp(ticker['closeTime'] / 1000),
                        trades_count=int(ticker['count']),
                        quote_volume=float(ticker['quoteVolume'])
                    )
                    
                    self.market_data_cache[symbol] = market_data
                
                except (ValueError, KeyError) as e:
                    self.logger.debug(f"[SCANNER] Error processing ticker for {symbol}: {e}")
        
        except BinanceAPIException as e:
            service_name = 'binance' if self.is_testnet else 'binance_live'
            rate_limited = e.code in [-1003, -1015]
            report_api_result(service_name, success=False, rate_limited=rate_limited, error_code=str(e.code))
            self.logger.error(f"[SCANNER] Binance API error updating market data: {e}")
        
        except Exception as e:
            self.logger.error(f"[SCANNER] Error updating market data: {e}")
    
    def _filter_pairs_by_criteria(self, pairs: List[str]) -> List[str]:
        """Filter pairs based on volume, change, and other criteria"""
        filtered_pairs = []
        
        for symbol in pairs:
            market_data = self.market_data_cache.get(symbol)
            if not market_data:
                continue
            
            # Volume filter
            if market_data.quote_volume < self.min_volume_24h:
                continue
            
            # Price change filter (minimum movement to be interesting)
            min_price_change = float(os.getenv('MIN_PRICE_CHANGE_24H', '0.5'))
            if abs(market_data.price_change_percent_24h) < min_price_change:
                continue
            
            # Exclude stablecoins and low-activity pairs
            if any(stable in symbol for stable in ['USDC', 'USDT', 'BUSD', 'TUSD']):
                if not symbol.endswith('USDT'):  # Keep USDT pairs for trading
                    continue
            
            filtered_pairs.append(symbol)
        
        # Sort by volume (descending) and take top pairs
        filtered_pairs.sort(
            key=lambda s: self.market_data_cache.get(s, MarketData('', 0, 0, 0, 0, 0, 0, datetime.now(), datetime.now(), 0, 0)).quote_volume,
            reverse=True
        )
        
        return filtered_pairs[:self.max_concurrent_pairs]
    
    async def _scan_pair_opportunities(self, symbol: str) -> List[OpportunitySignal]:
        """Scan a specific pair for trading opportunities"""
        opportunities = []
        
        try:
            # Get market data
            market_data = self.market_data_cache.get(symbol)
            if not market_data:
                return opportunities
            
            # Get historical data for primary timeframe
            historical_data = await self._get_historical_data(symbol, self.primary_timeframe)
            if historical_data is None or len(historical_data) < 50:
                return opportunities
            
            # Calculate technical indicators
            indicators = self._calculate_technical_indicators(historical_data)
            
            # Generate signals based on multiple criteria
            signals = self._generate_trading_signals(symbol, market_data, indicators, historical_data)
            
            # Apply multi-timeframe confirmation if enabled
            if os.getenv('MULTI_TIMEFRAME_CONFIRMATION', 'true').lower() == 'true':
                confirmed_signals = await self._apply_timeframe_confirmation(symbol, signals)
                opportunities.extend(confirmed_signals)
            else:
                opportunities.extend(signals)
        
        except Exception as e:
            self.logger.debug(f"[SCANNER] Error scanning {symbol}: {e}")
        
        return opportunities
    
    async def _get_historical_data(self, symbol: str, timeframe: str, limit: int = 100) -> Optional[pd.DataFrame]:
        """Get historical kline data for a symbol and timeframe"""
        if not self.client:
            return None
        
        try:
            # Check cache first
            cache_key = f"{symbol}_{timeframe}"
            if cache_key in self.historical_data_cache:
                cached_data = self.historical_data_cache[cache_key]
                
                # Check if cache is recent enough (within 1 scan interval)
                if len(cached_data) > 0:
                    last_timestamp = pd.to_datetime(cached_data.index[-1])
                    if (datetime.now() - last_timestamp).total_seconds() < self.scan_interval * 2:
                        return cached_data
            
            # Get klines from Binance
            klines = self.client.get_klines(symbol=symbol, interval=timeframe, limit=limit)
            
            if not klines:
                return None
            
            # Convert to DataFrame
            df = pd.DataFrame(klines, columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_volume', 'trades_count', 'taker_buy_base',
                'taker_buy_quote', 'ignore'
            ])
            
            # Convert data types
            numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'quote_volume']
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            
            # Cache the data
            if symbol not in self.historical_data_cache:
                self.historical_data_cache[symbol] = {}
            self.historical_data_cache[symbol][timeframe] = df
            
            return df
        
        except Exception as e:
            self.logger.debug(f"[SCANNER] Error getting historical data for {symbol} {timeframe}: {e}")
            return None
    
    def _calculate_technical_indicators(self, df: pd.DataFrame) -> TechnicalIndicators:
        """Calculate technical indicators for the data"""
        try:
            # RSI
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            
            # MACD
            ema12 = df['close'].ewm(span=12).mean()
            ema26 = df['close'].ewm(span=26).mean()
            macd = ema12 - ema26
            macd_signal = macd.ewm(span=9).mean()
            
            # Bollinger Bands
            sma20 = df['close'].rolling(window=20).mean()
            std20 = df['close'].rolling(window=20).std()
            bb_upper = sma20 + (std20 * 2)
            bb_lower = sma20 - (std20 * 2)
            
            # Volume SMA
            volume_sma = df['volume'].rolling(window=20).mean()
            
            # Price SMAs
            price_sma_short = df['close'].rolling(window=10).mean()
            price_sma_long = df['close'].rolling(window=50).mean()
            
            # ATR (Average True Range)
            high_low = df['high'] - df['low']
            high_close = np.abs(df['high'] - df['close'].shift())
            low_close = np.abs(df['low'] - df['close'].shift())
            tr = np.maximum(high_low, np.maximum(high_close, low_close))
            atr = tr.rolling(window=14).mean()
            
            # Stochastic Oscillator
            low_min = df['low'].rolling(window=14).min()
            high_max = df['high'].rolling(window=14).max()
            stoch_k = 100 * ((df['close'] - low_min) / (high_max - low_min))
            stoch_d = stoch_k.rolling(window=3).mean()
            
            return TechnicalIndicators(
                rsi=float(rsi.iloc[-1]) if not np.isnan(rsi.iloc[-1]) else 50.0,
                macd=float(macd.iloc[-1]) if not np.isnan(macd.iloc[-1]) else 0.0,
                macd_signal=float(macd_signal.iloc[-1]) if not np.isnan(macd_signal.iloc[-1]) else 0.0,
                bb_upper=float(bb_upper.iloc[-1]) if not np.isnan(bb_upper.iloc[-1]) else 0.0,
                bb_lower=float(bb_lower.iloc[-1]) if not np.isnan(bb_lower.iloc[-1]) else 0.0,
                bb_middle=float(sma20.iloc[-1]) if not np.isnan(sma20.iloc[-1]) else 0.0,
                volume_sma=float(volume_sma.iloc[-1]) if not np.isnan(volume_sma.iloc[-1]) else 0.0,
                price_sma_short=float(price_sma_short.iloc[-1]) if not np.isnan(price_sma_short.iloc[-1]) else 0.0,
                price_sma_long=float(price_sma_long.iloc[-1]) if not np.isnan(price_sma_long.iloc[-1]) else 0.0,
                atr=float(atr.iloc[-1]) if not np.isnan(atr.iloc[-1]) else 0.0,
                stoch_k=float(stoch_k.iloc[-1]) if not np.isnan(stoch_k.iloc[-1]) else 50.0,
                stoch_d=float(stoch_d.iloc[-1]) if not np.isnan(stoch_d.iloc[-1]) else 50.0
            )
        
        except Exception as e:
            self.logger.debug(f"[SCANNER] Error calculating indicators: {e}")
            # Return default indicators
            return TechnicalIndicators(
                rsi=50.0, macd=0.0, macd_signal=0.0, bb_upper=0.0, bb_lower=0.0,
                bb_middle=0.0, volume_sma=0.0, price_sma_short=0.0, price_sma_long=0.0,
                atr=0.0, stoch_k=50.0, stoch_d=50.0
            )
    
    def _generate_trading_signals(self, symbol: str, market_data: MarketData, 
                                indicators: TechnicalIndicators, df: pd.DataFrame) -> List[OpportunitySignal]:
        """Generate trading signals based on technical analysis"""
        signals = []
        
        try:
            current_price = market_data.price
            reasons = []
            signal_type = 'HOLD'
            confidence = 50.0
            opportunity_score = 0.0
            
            # Volume analysis
            volume_ratio = market_data.volume_24h / max(indicators.volume_sma, 1)
            if volume_ratio >= self.volume_spike_threshold:
                reasons.append(f"High volume spike ({volume_ratio:.1f}x average)")
                opportunity_score += 20
                confidence += 10
            
            # Price momentum analysis
            momentum_score = abs(market_data.price_change_percent_24h) / self.price_momentum_threshold
            if momentum_score >= 1.0:
                reasons.append(f"Strong momentum ({market_data.price_change_percent_24h:.1f}%)")
                opportunity_score += 15 * min(momentum_score, 2.0)
            
            # RSI analysis
            if indicators.rsi < 30:
                reasons.append(f"Oversold RSI ({indicators.rsi:.1f})")
                signal_type = 'BUY'
                confidence += 15
                opportunity_score += 25
            elif indicators.rsi > 70:
                reasons.append(f"Overbought RSI ({indicators.rsi:.1f})")
                signal_type = 'SELL'
                confidence += 15
                opportunity_score += 25
            elif 45 <= indicators.rsi <= 55:
                reasons.append("Neutral RSI")
                opportunity_score += 5
            
            # MACD analysis
            macd_diff = indicators.macd - indicators.macd_signal
            if macd_diff > 0 and indicators.macd > 0:
                reasons.append("MACD bullish")
                if signal_type != 'SELL':
                    signal_type = 'BUY'
                confidence += 10
                opportunity_score += 15
            elif macd_diff < 0 and indicators.macd < 0:
                reasons.append("MACD bearish")
                if signal_type != 'BUY':
                    signal_type = 'SELL'
                confidence += 10
                opportunity_score += 15
            
            # Bollinger Bands analysis
            bb_width = (indicators.bb_upper - indicators.bb_lower) / indicators.bb_middle * 100
            if current_price <= indicators.bb_lower:
                reasons.append("Price at lower Bollinger Band")
                if signal_type != 'SELL':
                    signal_type = 'BUY'
                confidence += 15
                opportunity_score += 20
            elif current_price >= indicators.bb_upper:
                reasons.append("Price at upper Bollinger Band")
                if signal_type != 'BUY':
                    signal_type = 'SELL'
                confidence += 15
                opportunity_score += 20
            
            # Moving average analysis
            if indicators.price_sma_short > indicators.price_sma_long:
                reasons.append("Short MA above long MA")
                confidence += 5
                opportunity_score += 10
            elif indicators.price_sma_short < indicators.price_sma_long:
                reasons.append("Short MA below long MA")
                confidence += 5
                opportunity_score += 10
            
            # Stochastic analysis
            if indicators.stoch_k < 20 and indicators.stoch_d < 20:
                reasons.append(f"Oversold Stochastic ({indicators.stoch_k:.1f})")
                confidence += 10
                opportunity_score += 15
            elif indicators.stoch_k > 80 and indicators.stoch_d > 80:
                reasons.append(f"Overbought Stochastic ({indicators.stoch_k:.1f})")
                confidence += 10
                opportunity_score += 15
            
            # Calculate entry, stop loss, and take profit
            atr_value = max(indicators.atr, current_price * 0.01)  # Minimum 1% ATR
            
            if signal_type == 'BUY':
                entry_price = current_price
                stop_loss = entry_price - (atr_value * 2)
                take_profit = entry_price + (atr_value * 3)
                risk_reward_ratio = (take_profit - entry_price) / (entry_price - stop_loss)
            elif signal_type == 'SELL':
                entry_price = current_price
                stop_loss = entry_price + (atr_value * 2)
                take_profit = entry_price - (atr_value * 3)
                risk_reward_ratio = (entry_price - take_profit) / (stop_loss - entry_price)
            else:
                entry_price = current_price
                stop_loss = current_price
                take_profit = current_price
                risk_reward_ratio = 1.0
            
            # Adjust opportunity score based on risk/reward
            if risk_reward_ratio >= 1.5:
                opportunity_score += 10
                reasons.append(f"Good R:R ratio ({risk_reward_ratio:.1f})")
            
            # Ensure confidence is within bounds
            confidence = max(0, min(100, confidence))
            
            # Only create signal if it meets minimum criteria
            if opportunity_score >= self.min_opportunity_score or len(reasons) >= 3:
                signal = OpportunitySignal(
                    symbol=symbol,
                    signal_type=signal_type,
                    confidence=confidence,
                    opportunity_score=opportunity_score,
                    reasons=reasons,
                    timeframe=self.primary_timeframe,
                    price=current_price,
                    volume=market_data.volume_24h,
                    technical_indicators=indicators,
                    market_data=market_data,
                    risk_reward_ratio=risk_reward_ratio,
                    entry_price=entry_price,
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                    timestamp=datetime.now()
                )
                
                signals.append(signal)
        
        except Exception as e:
            self.logger.debug(f"[SCANNER] Error generating signals for {symbol}: {e}")
        
        return signals
    
    async def _apply_timeframe_confirmation(self, symbol: str, signals: List[OpportunitySignal]) -> List[OpportunitySignal]:
        """Apply multi-timeframe confirmation to signals"""
        confirmed_signals = []
        
        try:
            min_agreement = int(os.getenv('MIN_TIMEFRAME_AGREEMENT', '2'))
            
            for signal in signals:
                if signal.signal_type == 'HOLD':
                    confirmed_signals.append(signal)
                    continue
                
                # Check confirmation timeframes
                confirmations = 0
                confirmation_reasons = []
                
                for tf in self.confirm_timeframes:
                    if tf == self.primary_timeframe:
                        continue
                    
                    # Get data for confirmation timeframe
                    tf_data = await self._get_historical_data(symbol, tf, limit=50)
                    if tf_data is None or len(tf_data) < 20:
                        continue
                    
                    # Calculate indicators for this timeframe
                    tf_indicators = self._calculate_technical_indicators(tf_data)
                    
                    # Check for trend alignment
                    trend_aligned = self._check_trend_alignment(signal, tf_indicators, tf)
                    
                    if trend_aligned:
                        confirmations += 1
                        confirmation_reasons.append(f"{tf} confirms trend")
                
                # Only include signal if sufficient confirmations
                if confirmations >= min_agreement:
                    signal.reasons.extend(confirmation_reasons)
                    signal.confidence += (confirmations * 5)  # Bonus for confirmations
                    signal.opportunity_score += (confirmations * 10)
                    confirmed_signals.append(signal)
        
        except Exception as e:
            self.logger.debug(f"[SCANNER] Error in timeframe confirmation for {symbol}: {e}")
            # Return original signals if confirmation fails
            return signals
        
        return confirmed_signals
    
    def _check_trend_alignment(self, signal: OpportunitySignal, tf_indicators: TechnicalIndicators, timeframe: str) -> bool:
        """Check if trend aligns across timeframes"""
        try:
            if signal.signal_type == 'BUY':
                # Look for bullish confirmations
                bullish_indicators = 0
                
                if tf_indicators.rsi < 50 and tf_indicators.rsi > 30:
                    bullish_indicators += 1
                
                if tf_indicators.macd > tf_indicators.macd_signal:
                    bullish_indicators += 1
                
                if tf_indicators.price_sma_short > tf_indicators.price_sma_long:
                    bullish_indicators += 1
                
                return bullish_indicators >= 2
            
            elif signal.signal_type == 'SELL':
                # Look for bearish confirmations
                bearish_indicators = 0
                
                if tf_indicators.rsi > 50 and tf_indicators.rsi < 70:
                    bearish_indicators += 1
                
                if tf_indicators.macd < tf_indicators.macd_signal:
                    bearish_indicators += 1
                
                if tf_indicators.price_sma_short < tf_indicators.price_sma_long:
                    bearish_indicators += 1
                
                return bearish_indicators >= 2
        
        except Exception as e:
            self.logger.debug(f"[SCANNER] Error checking trend alignment: {e}")
        
        return False
    
    def _update_opportunity_signals(self, opportunities: List[OpportunitySignal]):
        """Update the opportunity signals storage"""
        # Clear old signals
        self.opportunity_signals.clear()
        
        # Group by symbol
        for opportunity in opportunities:
            symbol = opportunity.symbol
            if symbol not in self.opportunity_signals:
                self.opportunity_signals[symbol] = []
            
            self.opportunity_signals[symbol].append(opportunity)
        
        # Update performance metrics
        self.scan_performance['opportunities_found'] = len(opportunities)
    
    async def _update_pair_correlations(self, pairs: List[str]):
        """Update correlation matrix for pairs"""
        try:
            if len(pairs) < 2:
                return
            
            # Get price changes for correlation calculation
            price_changes = {}
            for symbol in pairs:
                market_data = self.market_data_cache.get(symbol)
                if market_data:
                    price_changes[symbol] = market_data.price_change_percent_24h
            
            # Calculate pairwise correlations
            symbols = list(price_changes.keys())
            for i, symbol1 in enumerate(symbols):
                for j, symbol2 in enumerate(symbols[i+1:], i+1):
                    # Simple correlation based on price movements
                    # In a full implementation, you'd use historical price series
                    correlation = 0.0  # Placeholder - implement proper correlation
                    
                    self.pair_correlations[(symbol1, symbol2)] = correlation
        
        except Exception as e:
            self.logger.debug(f"[SCANNER] Error updating correlations: {e}")
    
    def get_top_opportunities(self, limit: int = 10, signal_type: Optional[str] = None) -> List[OpportunitySignal]:
        """Get top opportunities sorted by score"""
        all_opportunities = []
        
        for symbol_opportunities in self.opportunity_signals.values():
            all_opportunities.extend(symbol_opportunities)
        
        # Filter by signal type if specified
        if signal_type:
            all_opportunities = [opp for opp in all_opportunities if opp.signal_type == signal_type]
        
        # Sort by opportunity score (descending)
        all_opportunities.sort(key=lambda x: x.opportunity_score, reverse=True)
        
        return all_opportunities[:limit]
    
    def get_scanner_status(self) -> Dict[str, Any]:
        """Get scanner status and performance metrics"""
        return {
            'is_scanning': self.is_scanning,
            'scanner_enabled': self.scanner_enabled,
            'scan_interval': self.scan_interval,
            'last_scan_time': self.last_scan_time.isoformat() if self.last_scan_time != datetime.min else None,
            'cached_pairs': len(self.market_data_cache),
            'total_opportunities': sum(len(opps) for opps in self.opportunity_signals.values()),
            'performance': self.scan_performance,
            'timeframes': self.timeframes,
            'primary_timeframe': self.primary_timeframe,
            'min_opportunity_score': self.min_opportunity_score,
            'client_connected': self.client is not None
        }
    
    def get_pair_opportunities(self, symbol: str) -> List[OpportunitySignal]:
        """Get opportunities for a specific pair"""
        return self.opportunity_signals.get(symbol, [])
    
    async def cleanup(self):
        """Clean up resources"""
        await self.stop_scanning()
        self.logger.info("[SCANNER] Cleanup completed")

# Global instance
multi_pair_scanner = MultiPairScanner()

# Convenience functions
async def start_scanner():
    """Start the multi-pair scanner"""
    return await multi_pair_scanner.start_scanning()

async def stop_scanner():
    """Stop the multi-pair scanner"""
    await multi_pair_scanner.stop_scanning()

def get_top_opportunities(limit: int = 10, signal_type: Optional[str] = None) -> List[OpportunitySignal]:
    """Get top trading opportunities"""
    return multi_pair_scanner.get_top_opportunities(limit, signal_type)

def get_scanner_status() -> Dict[str, Any]:
    """Get scanner status"""
    return multi_pair_scanner.get_scanner_status()

if __name__ == "__main__":
    # Test the scanner
    import asyncio
    
    async def test_scanner():
        print("Testing Multi-Pair Scanner")
        print("=" * 40)
        
        # Start scanner
        success = await start_scanner()
        print(f"Scanner started: {'Success' if success else 'Failed'}")
        
        if success:
            # Wait for a few scans
            print("Waiting for scans to complete...")
            await asyncio.sleep(120)  # Wait 2 minutes
            
            # Get status
            status = get_scanner_status()
            print(f"Scanner status: {json.dumps(status, indent=2, default=str)}")
            
            # Get opportunities
            opportunities = get_top_opportunities(5, 'BUY')
            print(f"Top BUY opportunities: {len(opportunities)}")
            
            for opp in opportunities:
                print(f"  {opp.symbol}: {opp.opportunity_score:.1f} score, {opp.confidence:.1f}% confidence")
                print(f"    Reasons: {', '.join(opp.reasons[:3])}")
        
        # Stop scanner
        await stop_scanner()
        await multi_pair_scanner.cleanup()
    
    asyncio.run(test_scanner())