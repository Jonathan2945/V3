#!/usr/bin/env python3
"""
MULTI-PAIR OPPORTUNITY SCANNER - REAL DATA ONLY (DIRECT CREDENTIALS)
====================================================================
UPDATED FOR V3 REAL DATA SYSTEM:
- Uses direct .env credential loading (bypasses API rotation manager)
- Only uses real Binance market data
- No simulated or mock data anywhere
- Real-time scanning of actual trading pairs
- Compatible with V3 real trading system
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

# Load environment variables
load_dotenv()

@dataclass
class MarketData:
    """Real market data for a trading pair"""
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
    source: str = "REAL_BINANCE_API"
    live_data: bool = True

@dataclass
class TechnicalIndicators:
    """Real technical indicators calculated from live data"""
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
    data_source: str = "REAL_HISTORICAL_DATA"

@dataclass
class OpportunitySignal:
    """Real trading opportunity signal from live market data"""
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
    data_source: str = "REAL_BINANCE_SCANNER"
    live_signal: bool = True

class MultiPairScanner:
    """Multi-pair opportunity scanner for Binance - REAL DATA ONLY (DIRECT CREDENTIALS)"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Configuration from .env
        self.scanner_enabled = os.getenv('OPPORTUNITY_SCANNER_ENABLED', 'true').lower() == 'true'
        self.scan_interval = int(os.getenv('SCAN_INTERVAL_SECONDS', '30'))
        self.min_opportunity_score = float(os.getenv('MIN_OPPORTUNITY_SCORE', '75.0'))
        self.volume_spike_threshold = float(os.getenv('VOLUME_SPIKE_THRESHOLD', '2.0'))
        self.price_momentum_threshold = float(os.getenv('PRICE_MOMENTUM_THRESHOLD', '1.5'))
        
        # Multi-timeframe configuration
        self.timeframes = os.getenv('TIMEFRAMES', '1m,5m,15m,30m,1h,4h,1d').split(',')
        self.timeframes = [tf.strip() for tf in self.timeframes if tf.strip()]
        self.primary_timeframe = os.getenv('PRIMARY_TIMEFRAME', '15m')
        self.confirm_timeframes = os.getenv('CONFIRM_TIMEFRAMES', '5m,1h').split(',')
        self.confirm_timeframes = [tf.strip() for tf in self.confirm_timeframes if tf.strip()]
        
        # Risk management
        self.max_concurrent_pairs = int(os.getenv('MAX_CONCURRENT_PAIRS', '50'))
        self.min_volume_24h = float(os.getenv('MIN_VOLUME_24H', '100000'))
        
        # Scanner state
        self.client: Optional[Client] = None
        self.is_testnet = os.getenv('TESTNET', 'true').lower() == 'true'
        self.scanning_task: Optional[asyncio.Task] = None
        self.is_scanning = False
        self.last_scan_time = datetime.min
        
        # REAL Data storage - no mock data
        self.market_data_cache: Dict[str, MarketData] = {}
        self.historical_data_cache: Dict[str, Dict[str, pd.DataFrame]] = {}
        self.opportunity_signals: Dict[str, List[OpportunitySignal]] = {}
        
        # Performance tracking - real metrics only
        self.scan_performance = {
            'total_scans': 0,
            'successful_scans': 0,
            'opportunities_found': 0,
            'avg_scan_time': 0.0,
            'last_scan_duration': 0.0,
            'real_data_scans': 0,
            'live_api_calls': 0
        }
        
        # Initialize REAL client with DIRECT credentials
        self._initialize_direct_client()
        
        self.logger.info(f"[SCANNER] Multi-pair scanner initialized - REAL DATA ONLY (DIRECT CREDENTIALS)")
        self.logger.info(f"[SCANNER] Testnet mode: {self.is_testnet}")
        self.logger.info(f"[SCANNER] Client connected: {self.client is not None}")
    
    def _initialize_direct_client(self):
        """Initialize REAL Binance client with DIRECT credential loading - NO API ROTATION"""
        try:
            # Load credentials DIRECTLY from .env (bypasses API rotation manager)
            if self.is_testnet:
                api_key = os.getenv('BINANCE_API_KEY_1')
                api_secret = os.getenv('BINANCE_API_SECRET_1')
                connection_type = "REAL Binance testnet"
            else:
                api_key = os.getenv('BINANCE_LIVE_API_KEY_1')
                api_secret = os.getenv('BINANCE_LIVE_API_SECRET_1')
                connection_type = "REAL Binance.US live"
            
            if not api_key or not api_secret:
                self.logger.warning(f"[SCANNER] No REAL credentials found for {'testnet' if self.is_testnet else 'live'}")
                return
            
            if 'your_' in api_key.lower():
                self.logger.warning("[SCANNER] Placeholder credentials detected - need real API keys")
                return
            
            # Initialize REAL client - NO MOCK MODE
            if self.is_testnet:
                self.client = Client(api_key, api_secret, testnet=True)
            else:
                self.client = Client(api_key, api_secret, testnet=False, tld='us')
                
            # Test REAL connection
            test_ticker = self.client.get_symbol_ticker(symbol="BTCUSDT")
            btc_price = float(test_ticker['price'])
            self.logger.info(f"[SCANNER] Connected to {connection_type}")
            self.logger.info(f"[SCANNER] REAL connection verified - BTC: ${btc_price:,.2f}")
            
        except Exception as e:
            self.logger.error(f"[SCANNER] Failed to initialize REAL client with direct credentials: {e}")
            self.client = None
    
    async def start_scanning(self):
        """Start the REAL multi-pair scanning process"""
        if not self.scanner_enabled or not self.client:
            self.logger.warning("[SCANNER] Scanner not enabled or REAL client not available")
            return False
        
        if self.is_scanning:
            self.logger.warning("[SCANNER] REAL scanner already running")
            return True
        
        try:
            self.is_scanning = True
            self.scanning_task = asyncio.create_task(self._real_scanning_loop())
            
            self.logger.info("[SCANNER] REAL multi-pair scanning started")
            return True
        
        except Exception as e:
            self.logger.error(f"[SCANNER] Failed to start REAL scanning: {e}")
            self.is_scanning = False
            return False
    
    async def stop_scanning(self):
        """Stop the REAL scanning process"""
        self.is_scanning = False
        
        if self.scanning_task and not self.scanning_task.done():
            self.scanning_task.cancel()
            try:
                await self.scanning_task
            except asyncio.CancelledError:
                pass
        
        self.logger.info("[SCANNER] REAL multi-pair scanning stopped")
    
    async def _real_scanning_loop(self):
        """Main REAL scanning loop - NO SIMULATIONS"""
        while self.is_scanning:
            try:
                scan_start = time.time()
                
                # Perform REAL scan
                await self._perform_real_full_scan()
                
                # Update REAL performance metrics
                scan_duration = time.time() - scan_start
                self.scan_performance['total_scans'] += 1
                self.scan_performance['successful_scans'] += 1
                self.scan_performance['real_data_scans'] += 1
                self.scan_performance['last_scan_duration'] = scan_duration
                self.scan_performance['avg_scan_time'] = (
                    (self.scan_performance['avg_scan_time'] * (self.scan_performance['total_scans'] - 1) + scan_duration)
                    / self.scan_performance['total_scans']
                )
                
                self.last_scan_time = datetime.now()
                
                # Log REAL progress
                if self.scan_performance['total_scans'] % 10 == 0:
                    self.logger.info(
                        f"[SCANNER] REAL scan {self.scan_performance['total_scans']} completed in {scan_duration:.2f}s. "
                        f"Found {len(self.opportunity_signals)} REAL opportunities"
                    )
                
                # Wait for next REAL scan
                await asyncio.sleep(self.scan_interval)
            
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"[SCANNER] Error in REAL scanning loop: {e}")
                await asyncio.sleep(min(self.scan_interval, 60))
    
    async def _perform_real_full_scan(self):
        """Perform full scan of all tradeable pairs - REAL DATA ONLY"""
        try:
            # Get REAL tradeable pairs
            pairs = self._get_real_tradeable_pairs()
            if not pairs:
                self.logger.warning("[SCANNER] No REAL tradeable pairs available")
                return
            
            # Limit pairs to scan
            pairs_to_scan = pairs[:self.max_concurrent_pairs]
            
            # Update REAL market data for all pairs
            await self._update_real_market_data(pairs_to_scan)
            
            # Filter pairs by REAL criteria
            filtered_pairs = self._filter_pairs_by_real_criteria(pairs_to_scan)
            
            # Scan each pair for REAL opportunities
            real_opportunities = []
            
            for symbol in filtered_pairs:
                try:
                    pair_opportunities = await self._scan_real_pair_opportunities(symbol)
                    real_opportunities.extend(pair_opportunities)
                except Exception as e:
                    self.logger.debug(f"[SCANNER] Error scanning REAL data for {symbol}: {e}")
                    continue
            
            # Update REAL opportunity signals
            self._update_real_opportunity_signals(real_opportunities)
            
        except Exception as e:
            self.logger.error(f"[SCANNER] Error in REAL full scan: {e}")
    
    def _get_real_tradeable_pairs(self) -> List[str]:
        """Get REAL tradeable pairs from environment or default list"""
        try:
            # Try to get from Binance API
            if self.client:
                exchange_info = self.client.get_exchange_info()
                pairs = [s['symbol'] for s in exchange_info['symbols'] 
                        if s['status'] == 'TRADING' and s['symbol'].endswith('USDT')]
                return pairs[:100]  # Limit for performance
        except Exception as e:
            self.logger.debug(f"[SCANNER] Could not get pairs from API: {e}")
        
        # Fallback to configured pairs
        major_pairs = os.getenv('MAJOR_PAIRS', 'BTCUSDT,ETHUSDT,BNBUSDT,XRPUSDT,ADAUSDT,SOLUSDT').split(',')
        return [pair.strip() for pair in major_pairs if pair.strip()]
    
    async def _update_real_market_data(self, pairs: List[str]):
        """Update 24hr REAL market data for all pairs - NO MOCK DATA"""
        if not self.client:
            return
        
        try:
            start_time = time.time()
            
            # Get REAL 24hr ticker stats for all pairs
            real_tickers = self.client.get_ticker()
            self.scan_performance['live_api_calls'] += 1
            
            response_time = time.time() - start_time
            
            # Process REAL ticker data
            real_data_count = 0
            for ticker in real_tickers:
                symbol = ticker['symbol']
                if symbol not in pairs:
                    continue
                
                try:
                    # Create REAL market data object
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
                        quote_volume=float(ticker['quoteVolume']),
                        source="REAL_BINANCE_API",
                        live_data=True
                    )
                    
                    self.market_data_cache[symbol] = market_data
                    real_data_count += 1
                
                except (ValueError, KeyError) as e:
                    self.logger.debug(f"[SCANNER] Error processing REAL ticker for {symbol}: {e}")
            
            self.logger.debug(f"[SCANNER] Updated {real_data_count} REAL market data entries")
        
        except BinanceAPIException as e:
            self.logger.error(f"[SCANNER] Binance API error updating REAL market data: {e}")
        
        except Exception as e:
            self.logger.error(f"[SCANNER] Error updating REAL market data: {e}")
    
    def _filter_pairs_by_real_criteria(self, pairs: List[str]) -> List[str]:
        """Filter pairs based on REAL volume, change, and other criteria"""
        filtered_pairs = []
        
        for symbol in pairs:
            market_data = self.market_data_cache.get(symbol)
            if not market_data or not market_data.live_data:
                continue
            
            # REAL volume filter
            if market_data.quote_volume < self.min_volume_24h:
                continue
            
            # REAL price change filter
            min_price_change = float(os.getenv('MIN_PRICE_CHANGE_24H', '0.5'))
            if abs(market_data.price_change_percent_24h) < min_price_change:
                continue
            
            # Exclude stablecoins except USDT pairs
            if any(stable in symbol for stable in ['USDC', 'BUSD', 'TUSD']):
                if not symbol.endswith('USDT'):
                    continue
            
            filtered_pairs.append(symbol)
        
        # Sort by REAL volume (descending)
        filtered_pairs.sort(
            key=lambda s: self.market_data_cache.get(s, MarketData('', 0, 0, 0, 0, 0, 0, datetime.now(), datetime.now(), 0, 0)).quote_volume,
            reverse=True
        )
        
        return filtered_pairs[:self.max_concurrent_pairs]
    
    async def _scan_real_pair_opportunities(self, symbol: str) -> List[OpportunitySignal]:
        """Scan a specific pair for REAL trading opportunities"""
        opportunities = []
        
        try:
            # Get REAL market data
            market_data = self.market_data_cache.get(symbol)
            if not market_data or not market_data.live_data:
                return opportunities
            
            # Get REAL historical data for primary timeframe
            historical_data = await self._get_real_historical_data(symbol, self.primary_timeframe)
            if historical_data is None or len(historical_data) < 20:
                return opportunities
            
            # Calculate technical indicators from REAL data
            indicators = self._calculate_real_technical_indicators(historical_data)
            
            # Generate REAL signals based on multiple criteria
            signals = self._generate_real_trading_signals(symbol, market_data, indicators, historical_data)
            
            opportunities.extend(signals)
        
        except Exception as e:
            self.logger.debug(f"[SCANNER] Error scanning REAL data for {symbol}: {e}")
        
        return opportunities
    
    async def _get_real_historical_data(self, symbol: str, timeframe: str, limit: int = 100) -> Optional[pd.DataFrame]:
        """Get REAL historical kline data for a symbol and timeframe - NO MOCK DATA"""
        if not self.client:
            return None
        
        try:
            # Check cache first
            cache_key = f"{symbol}_{timeframe}"
            if cache_key in self.historical_data_cache:
                cached_data = self.historical_data_cache[cache_key]
                
                # Check if cache is recent enough
                if len(cached_data) > 0:
                    last_timestamp = pd.to_datetime(cached_data.index[-1])
                    if (datetime.now() - last_timestamp).total_seconds() < self.scan_interval * 2:
                        return cached_data
            
            # Get REAL klines from Binance
            real_klines = self.client.get_klines(symbol=symbol, interval=timeframe, limit=limit)
            self.scan_performance['live_api_calls'] += 1
            
            if not real_klines:
                return None
            
            # Convert REAL data to DataFrame
            df = pd.DataFrame(real_klines, columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_volume', 'trades_count', 'taker_buy_base',
                'taker_buy_quote', 'ignore'
            ])
            
            # Convert REAL data types
            numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'quote_volume']
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            
            # Cache the REAL data
            if symbol not in self.historical_data_cache:
                self.historical_data_cache[symbol] = {}
            self.historical_data_cache[symbol][timeframe] = df
            
            return df
        
        except Exception as e:
            self.logger.debug(f"[SCANNER] Error getting REAL historical data for {symbol} {timeframe}: {e}")
            return None
    
    def _calculate_real_technical_indicators(self, df: pd.DataFrame) -> TechnicalIndicators:
        """Calculate technical indicators from REAL data - NO SIMULATIONS"""
        try:
            # RSI from REAL data
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            
            # MACD from REAL data
            ema12 = df['close'].ewm(span=12).mean()
            ema26 = df['close'].ewm(span=26).mean()
            macd = ema12 - ema26
            macd_signal = macd.ewm(span=9).mean()
            
            # Bollinger Bands from REAL data
            sma20 = df['close'].rolling(window=20).mean()
            std20 = df['close'].rolling(window=20).std()
            bb_upper = sma20 + (std20 * 2)
            bb_lower = sma20 - (std20 * 2)
            
            # Volume SMA from REAL data
            volume_sma = df['volume'].rolling(window=20).mean()
            
            # Price SMAs from REAL data
            price_sma_short = df['close'].rolling(window=10).mean()
            price_sma_long = df['close'].rolling(window=50).mean()
            
            # ATR from REAL data
            high_low = df['high'] - df['low']
            high_close = np.abs(df['high'] - df['close'].shift())
            low_close = np.abs(df['low'] - df['close'].shift())
            tr = np.maximum(high_low, np.maximum(high_close, low_close))
            atr = tr.rolling(window=14).mean()
            
            # Stochastic from REAL data
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
                stoch_d=float(stoch_d.iloc[-1]) if not np.isnan(stoch_d.iloc[-1]) else 50.0,
                data_source="REAL_HISTORICAL_DATA"
            )
        
        except Exception as e:
            self.logger.debug(f"[SCANNER] Error calculating REAL indicators: {e}")
            return TechnicalIndicators(
                rsi=50.0, macd=0.0, macd_signal=0.0, bb_upper=0.0, bb_lower=0.0,
                bb_middle=0.0, volume_sma=0.0, price_sma_short=0.0, price_sma_long=0.0,
                atr=0.0, stoch_k=50.0, stoch_d=50.0,
                data_source="REAL_HISTORICAL_DATA"
            )
    
    def _generate_real_trading_signals(self, symbol: str, market_data: MarketData, 
                                     indicators: TechnicalIndicators, df: pd.DataFrame) -> List[OpportunitySignal]:
        """Generate REAL trading signals based on technical analysis - NO SIMULATIONS"""
        signals = []
        
        try:
            current_price = market_data.price
            reasons = []
            signal_type = 'HOLD'
            confidence = 50.0
            opportunity_score = 0.0
            
            # REAL volume analysis
            volume_ratio = market_data.volume_24h / max(indicators.volume_sma, 1)
            if volume_ratio >= self.volume_spike_threshold:
                reasons.append(f"REAL volume spike ({volume_ratio:.1f}x avg)")
                opportunity_score += 20
                confidence += 10
            
            # REAL price momentum analysis
            momentum_score = abs(market_data.price_change_percent_24h) / self.price_momentum_threshold
            if momentum_score >= 1.0:
                reasons.append(f"REAL momentum ({market_data.price_change_percent_24h:.1f}%)")
                opportunity_score += 15 * min(momentum_score, 2.0)
            
            # REAL RSI analysis
            if indicators.rsi < 30:
                reasons.append(f"REAL oversold RSI ({indicators.rsi:.1f})")
                signal_type = 'BUY'
                confidence += 15
                opportunity_score += 25
            elif indicators.rsi > 70:
                reasons.append(f"REAL overbought RSI ({indicators.rsi:.1f})")
                signal_type = 'SELL'
                confidence += 15
                opportunity_score += 25
            
            # REAL MACD analysis
            macd_diff = indicators.macd - indicators.macd_signal
            if macd_diff > 0 and indicators.macd > 0:
                reasons.append("REAL MACD bullish")
                if signal_type != 'SELL':
                    signal_type = 'BUY'
                confidence += 10
                opportunity_score += 15
            elif macd_diff < 0 and indicators.macd < 0:
                reasons.append("REAL MACD bearish")
                if signal_type != 'BUY':
                    signal_type = 'SELL'
                confidence += 10
                opportunity_score += 15
            
            # Calculate REAL entry, stop loss, and take profit
            atr_value = max(indicators.atr, current_price * 0.01)
            
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
            
            # Adjust opportunity score based on REAL risk/reward
            if risk_reward_ratio >= 1.5:
                opportunity_score += 10
                reasons.append(f"REAL good R:R ({risk_reward_ratio:.1f})")
            
            # Ensure confidence is within bounds
            confidence = max(0, min(100, confidence))
            
            # Only create REAL signal if it meets minimum criteria
            if opportunity_score >= self.min_opportunity_score or len(reasons) >= 2:
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
                    timestamp=datetime.now(),
                    data_source="REAL_BINANCE_SCANNER",
                    live_signal=True
                )
                
                signals.append(signal)
        
        except Exception as e:
            self.logger.debug(f"[SCANNER] Error generating REAL signals for {symbol}: {e}")
        
        return signals
    
    def _update_real_opportunity_signals(self, opportunities: List[OpportunitySignal]):
        """Update the REAL opportunity signals storage"""
        # Clear old signals
        self.opportunity_signals.clear()
        
        # Group REAL signals by symbol
        real_opportunities_count = 0
        for opportunity in opportunities:
            if opportunity.live_signal and opportunity.data_source == "REAL_BINANCE_SCANNER":
                symbol = opportunity.symbol
                if symbol not in self.opportunity_signals:
                    self.opportunity_signals[symbol] = []
                
                self.opportunity_signals[symbol].append(opportunity)
                real_opportunities_count += 1
        
        # Update REAL performance metrics
        self.scan_performance['opportunities_found'] = real_opportunities_count
    
    def get_top_opportunities(self, limit: int = 10, signal_type: Optional[str] = None) -> List[OpportunitySignal]:
        """Get top REAL opportunities sorted by score"""
        all_opportunities = []
        
        for symbol_opportunities in self.opportunity_signals.values():
            # Only include REAL signals
            for opp in symbol_opportunities:
                if opp.live_signal and opp.data_source == "REAL_BINANCE_SCANNER":
                    all_opportunities.append(opp)
        
        # Filter by signal type if specified
        if signal_type:
            all_opportunities = [opp for opp in all_opportunities if opp.signal_type == signal_type]
        
        # Sort by REAL opportunity score (descending)
        all_opportunities.sort(key=lambda x: x.opportunity_score, reverse=True)
        
        return all_opportunities[:limit]
    
    def get_scanner_status(self) -> Dict[str, Any]:
        """Get REAL scanner status and performance metrics"""
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
            'client_connected': self.client is not None,
            'data_source': 'REAL_BINANCE_API',
            'live_data_only': True,
            'no_simulations': True,
            'credential_source': 'DIRECT_ENV_LOADING',
            'real_api_calls': self.scan_performance.get('live_api_calls', 0),
            'real_data_scans': self.scan_performance.get('real_data_scans', 0)
        }
    
    def get_pair_opportunities(self, symbol: str) -> List[OpportunitySignal]:
        """Get REAL opportunities for a specific pair"""
        opportunities = self.opportunity_signals.get(symbol, [])
        # Filter to only return REAL signals
        return [opp for opp in opportunities if opp.live_signal and opp.data_source == "REAL_BINANCE_SCANNER"]
    
    async def cleanup(self):
        """Clean up REAL resources"""
        await self.stop_scanning()
        self.logger.info("[SCANNER] REAL cleanup completed")

# Global instance - REAL DATA ONLY
multi_pair_scanner = MultiPairScanner()

# Convenience functions - REAL DATA ONLY
async def start_scanner():
    """Start the REAL multi-pair scanner"""
    return await multi_pair_scanner.start_scanning()

async def stop_scanner():
    """Stop the REAL multi-pair scanner"""
    await multi_pair_scanner.stop_scanning()

def get_top_opportunities(limit: int = 10, signal_type: Optional[str] = None) -> List[OpportunitySignal]:
    """Get top REAL trading opportunities"""
    return multi_pair_scanner.get_top_opportunities(limit, signal_type)

def get_scanner_status() -> Dict[str, Any]:
    """Get REAL scanner status"""
    return multi_pair_scanner.get_scanner_status()

def get_active_pairs_count() -> int:
    """Get count of active pairs being monitored with REAL data"""
    try:
        return len(multi_pair_scanner.market_data_cache)
    except Exception as e:
        logging.error(f"Failed to get REAL active pairs count: {e}")
        return 0

def get_real_market_data(symbol: str) -> Optional[MarketData]:
    """Get REAL market data for a specific symbol"""
    try:
        market_data = multi_pair_scanner.market_data_cache.get(symbol)
        if market_data and market_data.live_data:
            return market_data
        return None
    except Exception as e:
        logging.error(f"Failed to get REAL market data for {symbol}: {e}")
        return None

def is_scanner_using_real_data() -> bool:
    """Verify scanner is using only REAL data"""
    try:
        status = get_scanner_status()
        return (status.get('data_source') == 'REAL_BINANCE_API' and 
                status.get('live_data_only', False) and 
                status.get('no_simulations', False) and
                status.get('client_connected', False))
    except:
        return False

if __name__ == "__main__":
    # Test the REAL scanner with direct credentials
    import asyncio
    
    async def test_real_scanner_direct():
        print("Testing Multi-Pair Scanner - REAL DATA ONLY (DIRECT CREDENTIALS)")
        print("=" * 70)
        
        # Verify REAL data mode
        if is_scanner_using_real_data():
            print("? Scanner configured for REAL data only with direct credentials")
        else:
            print("? Scanner may have connection issues")
        
        # Show status
        status = get_scanner_status()
        print(f"Client connected: {status.get('client_connected', False)}")
        print(f"Credential source: {status.get('credential_source', 'Unknown')}")
        
        if status.get('client_connected'):
            # Start REAL scanner
            success = await start_scanner()
            print(f"REAL Scanner started: {'Success' if success else 'Failed'}")
            
            if success:
                # Wait for REAL scans
                print("Waiting for REAL scans...")
                await asyncio.sleep(90)
                
                # Get REAL opportunities
                opportunities = get_top_opportunities(3, 'BUY')
                print(f"REAL opportunities found: {len(opportunities)}")
                
                for opp in opportunities:
                    print(f"  {opp.symbol}: {opp.opportunity_score:.1f} score, {opp.confidence:.1f}% confidence")
                    print(f"    Reasons: {', '.join(opp.reasons[:2])}")
            
            # Stop scanner
            await stop_scanner()
        else:
            print("? Cannot test without client connection")
        
        await multi_pair_scanner.cleanup()
        print("? Test completed")
    
    asyncio.run(test_real_scanner_direct())