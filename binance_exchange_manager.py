#!/usr/bin/env python3
"""
BINANCE EXCHANGE MANAGER - DYNAMIC TRADING RULES
================================================
Manages Binance.US exchange information, trading rules, and position sizing
Features:
- Dynamic exchange info updates
- Automatic trading rule enforcement
- Position sizing based on exchange constraints
- Multi-pair support with individual rules
- Caches exchange info to reduce API calls
"""

import os
import json
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from binance.client import Client
from binance.exceptions import BinanceAPIException
from decimal import Decimal, ROUND_DOWN
import time
from dotenv import load_dotenv
from api_rotation_manager import get_api_key, report_api_result

# Load environment variables
load_dotenv()

@dataclass
class TradingRule:
    """Trading rule for a specific symbol"""
    symbol: str
    min_qty: Decimal
    max_qty: Decimal
    step_size: Decimal
    min_notional: Decimal
    tick_size: Decimal
    base_asset: str
    quote_asset: str
    status: str
    permissions: List[str]
    
    def calculate_valid_quantity(self, desired_qty: Decimal) -> Decimal:
        """Calculate valid quantity based on step size"""
        if desired_qty < self.min_qty:
            return Decimal('0')
        
        if desired_qty > self.max_qty:
            desired_qty = self.max_qty
        
        # Round down to valid step size
        steps = int(desired_qty / self.step_size)
        valid_qty = Decimal(str(steps)) * self.step_size
        
        return max(valid_qty, self.min_qty) if valid_qty >= self.min_qty else Decimal('0')
    
    def calculate_valid_price(self, desired_price: Decimal) -> Decimal:
        """Calculate valid price based on tick size"""
        if self.tick_size == Decimal('0'):
            return desired_price
        
        steps = int(desired_price / self.tick_size)
        return Decimal(str(steps)) * self.tick_size
    
    def is_valid_order(self, quantity: Decimal, price: Decimal) -> Tuple[bool, str]:
        """Check if order parameters are valid"""
        if quantity < self.min_qty:
            return False, f"Quantity {quantity} below minimum {self.min_qty}"
        
        if quantity > self.max_qty:
            return False, f"Quantity {quantity} above maximum {self.max_qty}"
        
        notional = quantity * price
        if notional < self.min_notional:
            return False, f"Notional {notional} below minimum {self.min_notional}"
        
        if self.status != 'TRADING':
            return False, f"Symbol not tradeable: {self.status}"
        
        return True, "Valid order"

@dataclass
class ExchangeInfo:
    """Complete exchange information"""
    server_time: datetime
    rate_limits: List[Dict]
    trading_rules: Dict[str, TradingRule]
    symbols: List[str]
    base_assets: List[str]
    quote_assets: List[str]
    last_updated: datetime

class BinanceExchangeManager:
    """Manages Binance.US exchange information and trading rules"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Configuration from .env
        self.update_interval = int(os.getenv('UPDATE_EXCHANGE_INFO_INTERVAL', '3600'))  # 1 hour
        self.cache_enabled = os.getenv('CACHE_EXCHANGE_INFO', 'true').lower() == 'true'
        self.backup_file = os.getenv('EXCHANGE_INFO_BACKUP_FILE', 'data/exchange_info_backup.json')
        self.auto_update_rules = os.getenv('AUTO_UPDATE_TRADING_RULES', 'true').lower() == 'true'
        
        # Trading configuration
        self.enable_all_pairs = os.getenv('ENABLE_ALL_PAIRS', 'true').lower() == 'true'
        self.excluded_pairs = os.getenv('EXCLUDED_PAIRS', '').split(',')
        self.excluded_pairs = [pair.strip() for pair in self.excluded_pairs if pair.strip()]
        self.min_volume_24h = float(os.getenv('MIN_VOLUME_24H', '100000'))
        self.max_concurrent_pairs = int(os.getenv('MAX_CONCURRENT_PAIRS', '50'))
        self.min_price_change_24h = float(os.getenv('MIN_PRICE_CHANGE_24H', '0.5'))
        self.max_spread_percent = float(os.getenv('MAX_SPREAD_PERCENT', '0.5'))
        
        # Risk management
        self.trade_mode = os.getenv('TRADE_MODE', 'MINIMUM')
        self.fixed_trade_amount = float(os.getenv('FIXED_TRADE_AMOUNT', '50.0'))
        self.min_trade_amount = float(os.getenv('MIN_TRADE_AMOUNT', '10.0'))
        
        # State
        self.exchange_info: Optional[ExchangeInfo] = None
        self.client: Optional[Client] = None
        self.last_update = datetime.min
        self.update_task: Optional[asyncio.Task] = None
        self.is_testnet = os.getenv('TESTNET', 'true').lower() == 'true'
        
        # Initialize client
        self._initialize_client()
        
        # Load cached data if available
        if self.cache_enabled:
            self._load_cached_exchange_info()
        
        self.logger.info(f"[EXCHANGE] Manager initialized (testnet={self.is_testnet})")
    
    def _initialize_client(self):
        """Initialize Binance client with rotation support"""
        try:
            if self.is_testnet:
                # Get testnet credentials from rotation manager
                binance_creds = get_api_key('binance')
            else:
                # Get live credentials from rotation manager
                binance_creds = get_api_key('binance_live')
            
            if not binance_creds:
                self.logger.warning("[EXCHANGE] No valid Binance credentials found")
                return
            
            if isinstance(binance_creds, dict):
                api_key = binance_creds.get('api_key')
                api_secret = binance_creds.get('api_secret')
            else:
                # Fallback for single key (shouldn't happen with rotation manager)
                self.logger.warning("[EXCHANGE] Using fallback credential format")
                return
            
            if not api_key or not api_secret:
                self.logger.warning("[EXCHANGE] Incomplete Binance credentials")
                return
            
            # Initialize client
            if self.is_testnet:
                self.client = Client(api_key, api_secret, testnet=True)
                self.logger.info("[EXCHANGE] Connected to Binance testnet")
            else:
                self.client = Client(api_key, api_secret, testnet=False, tld='us')
                self.logger.info("[EXCHANGE] Connected to Binance.US live")
        
        except Exception as e:
            self.logger.error(f"[EXCHANGE] Failed to initialize client: {e}")
            self.client = None
    
    async def initialize(self):
        """Initialize exchange manager"""
        try:
            # Get initial exchange info
            await self.update_exchange_info()
            
            # Start background update task
            if self.auto_update_rules:
                self.update_task = asyncio.create_task(self._background_update_loop())
            
            self.logger.info("[EXCHANGE] Initialization complete")
            return True
        
        except Exception as e:
            self.logger.error(f"[EXCHANGE] Initialization failed: {e}")
            return False
    
    async def update_exchange_info(self) -> bool:
        """Update exchange information from Binance"""
        if not self.client:
            self.logger.warning("[EXCHANGE] No client available for update")
            return False
        
        try:
            start_time = time.time()
            
            # Get exchange info
            exchange_info_raw = self.client.get_exchange_info()
            
            response_time = time.time() - start_time
            
            # Report successful API call
            service_name = 'binance' if self.is_testnet else 'binance_live'
            report_api_result(service_name, success=True, response_time=response_time)
            
            # Parse exchange info
            self.exchange_info = self._parse_exchange_info(exchange_info_raw)
            
            # Save to cache if enabled
            if self.cache_enabled:
                await self._save_exchange_info_cache()
            
            self.last_update = datetime.now()
            
            self.logger.info(f"[EXCHANGE] Updated exchange info for {len(self.exchange_info.symbols)} symbols")
            return True
        
        except BinanceAPIException as e:
            # Report API failure
            service_name = 'binance' if self.is_testnet else 'binance_live'
            rate_limited = e.code in [-1003, -1015]  # Request weight exceeded or Too many orders
            
            report_api_result(service_name, success=False, rate_limited=rate_limited, 
                            error_code=str(e.code))
            
            self.logger.error(f"[EXCHANGE] Binance API error: {e}")
            
            # If rate limited, try to get new credentials
            if rate_limited and hasattr(self, '_rotate_api_key'):
                await self._rotate_api_key()
            
            return False
        
        except Exception as e:
            self.logger.error(f"[EXCHANGE] Exchange info update failed: {e}")
            return False
    
    def _parse_exchange_info(self, raw_info: Dict) -> ExchangeInfo:
        """Parse raw exchange info into structured format"""
        trading_rules = {}
        symbols = []
        base_assets = set()
        quote_assets = set()
        
        for symbol_info in raw_info.get('symbols', []):
            symbol = symbol_info['symbol']
            base_asset = symbol_info['baseAsset']
            quote_asset = symbol_info['quoteAsset']
            
            # Extract filters
            filters = {f['filterType']: f for f in symbol_info.get('filters', [])}
            
            # Get lot size filter
            lot_size = filters.get('LOT_SIZE', {})
            min_qty = Decimal(lot_size.get('minQty', '0'))
            max_qty = Decimal(lot_size.get('maxQty', '999999999'))
            step_size = Decimal(lot_size.get('stepSize', '1'))
            
            # Get notional filter
            notional = filters.get('MIN_NOTIONAL', filters.get('NOTIONAL', {}))
            min_notional = Decimal(notional.get('minNotional', '10'))
            
            # Get price filter
            price_filter = filters.get('PRICE_FILTER', {})
            tick_size = Decimal(price_filter.get('tickSize', '0.01'))
            
            # Create trading rule
            trading_rule = TradingRule(
                symbol=symbol,
                min_qty=min_qty,
                max_qty=max_qty,
                step_size=step_size,
                min_notional=min_notional,
                tick_size=tick_size,
                base_asset=base_asset,
                quote_asset=quote_asset,
                status=symbol_info.get('status', 'UNKNOWN'),
                permissions=symbol_info.get('permissions', [])
            )
            
            trading_rules[symbol] = trading_rule
            symbols.append(symbol)
            base_assets.add(base_asset)
            quote_assets.add(quote_asset)
        
        return ExchangeInfo(
            server_time=datetime.fromtimestamp(raw_info.get('serverTime', 0) / 1000),
            rate_limits=raw_info.get('rateLimits', []),
            trading_rules=trading_rules,
            symbols=symbols,
            base_assets=list(base_assets),
            quote_assets=list(quote_assets),
            last_updated=datetime.now()
        )
    
    async def _save_exchange_info_cache(self):
        """Save exchange info to cache file"""
        try:
            os.makedirs(os.path.dirname(self.backup_file), exist_ok=True)
            
            cache_data = {
                'timestamp': self.exchange_info.last_updated.isoformat(),
                'server_time': self.exchange_info.server_time.isoformat(),
                'symbols_count': len(self.exchange_info.symbols),
                'trading_rules': {
                    symbol: {
                        'min_qty': str(rule.min_qty),
                        'max_qty': str(rule.max_qty),
                        'step_size': str(rule.step_size),
                        'min_notional': str(rule.min_notional),
                        'tick_size': str(rule.tick_size),
                        'base_asset': rule.base_asset,
                        'quote_asset': rule.quote_asset,
                        'status': rule.status,
                        'permissions': rule.permissions
                    }
                    for symbol, rule in self.exchange_info.trading_rules.items()
                }
            }
            
            with open(self.backup_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
            
            self.logger.debug(f"[EXCHANGE] Cached exchange info to {self.backup_file}")
        
        except Exception as e:
            self.logger.warning(f"[EXCHANGE] Failed to cache exchange info: {e}")
    
    def _load_cached_exchange_info(self):
        """Load exchange info from cache file"""
        try:
            if not os.path.exists(self.backup_file):
                return
            
            with open(self.backup_file, 'r') as f:
                cache_data = json.load(f)
            
            # Check if cache is still valid (not older than update interval)
            cached_time = datetime.fromisoformat(cache_data['timestamp'])
            if (datetime.now() - cached_time).total_seconds() > self.update_interval:
                self.logger.info("[EXCHANGE] Cache is outdated, will update from API")
                return
            
            # Reconstruct trading rules
            trading_rules = {}
            for symbol, rule_data in cache_data['trading_rules'].items():
                trading_rules[symbol] = TradingRule(
                    symbol=symbol,
                    min_qty=Decimal(rule_data['min_qty']),
                    max_qty=Decimal(rule_data['max_qty']),
                    step_size=Decimal(rule_data['step_size']),
                    min_notional=Decimal(rule_data['min_notional']),
                    tick_size=Decimal(rule_data['tick_size']),
                    base_asset=rule_data['base_asset'],
                    quote_asset=rule_data['quote_asset'],
                    status=rule_data['status'],
                    permissions=rule_data['permissions']
                )
            
            self.exchange_info = ExchangeInfo(
                server_time=datetime.fromisoformat(cache_data['server_time']),
                rate_limits=[],  # Not cached
                trading_rules=trading_rules,
                symbols=list(trading_rules.keys()),
                base_assets=list(set(rule.base_asset for rule in trading_rules.values())),
                quote_assets=list(set(rule.quote_asset for rule in trading_rules.values())),
                last_updated=cached_time
            )
            
            self.logger.info(f"[EXCHANGE] Loaded {len(trading_rules)} symbols from cache")
        
        except Exception as e:
            self.logger.warning(f"[EXCHANGE] Failed to load cached exchange info: {e}")
    
    def get_tradeable_pairs(self, include_volume_filter: bool = True) -> List[str]:
        """Get list of tradeable pairs based on configuration"""
        if not self.exchange_info:
            return []
        
        tradeable_pairs = []
        
        for symbol, rule in self.exchange_info.trading_rules.items():
            # Skip if not trading
            if rule.status != 'TRADING':
                continue
            
            # Skip excluded pairs
            if symbol in self.excluded_pairs:
                continue
            
            # Check permissions
            if 'SPOT' not in rule.permissions:
                continue
            
            # Add to tradeable list
            tradeable_pairs.append(symbol)
        
        # Limit concurrent pairs
        if len(tradeable_pairs) > self.max_concurrent_pairs:
            tradeable_pairs = tradeable_pairs[:self.max_concurrent_pairs]
        
        return tradeable_pairs
    
    def calculate_position_size(self, symbol: str, confidence: float, 
                              account_balance: float, current_price: float) -> Tuple[Decimal, Decimal]:
        """Calculate position size based on trading rules and risk management"""
        if not self.exchange_info or symbol not in self.exchange_info.trading_rules:
            return Decimal('0'), Decimal('0')
        
        rule = self.exchange_info.trading_rules[symbol]
        
        try:
            # Calculate base position size based on mode
            if self.trade_mode == 'FIXED':
                position_value = self.fixed_trade_amount
            else:  # MINIMUM mode
                position_value = float(rule.min_notional) * 1.1  # 10% above minimum
            
            # Apply confidence factor
            confidence_factor = confidence / 100.0
            position_value *= confidence_factor
            
            # Ensure minimum trade amount
            position_value = max(position_value, self.min_trade_amount)
            
            # Calculate quantity
            quantity = Decimal(str(position_value / current_price))
            
            # Apply trading rule constraints
            valid_quantity = rule.calculate_valid_quantity(quantity)
            
            # Calculate actual position value
            actual_position_value = valid_quantity * Decimal(str(current_price))
            
            return valid_quantity, actual_position_value
        
        except Exception as e:
            self.logger.error(f"[EXCHANGE] Position size calculation failed for {symbol}: {e}")
            return Decimal('0'), Decimal('0')
    
    def validate_order_parameters(self, symbol: str, quantity: Decimal, 
                                price: Decimal) -> Tuple[bool, str, Decimal, Decimal]:
        """Validate and adjust order parameters"""
        if not self.exchange_info or symbol not in self.exchange_info.trading_rules:
            return False, f"No trading rules found for {symbol}", Decimal('0'), Decimal('0')
        
        rule = self.exchange_info.trading_rules[symbol]
        
        # Adjust quantity and price to valid values
        valid_quantity = rule.calculate_valid_quantity(quantity)
        valid_price = rule.calculate_valid_price(price)
        
        # Validate the adjusted order
        is_valid, message = rule.is_valid_order(valid_quantity, valid_price)
        
        return is_valid, message, valid_quantity, valid_price
    
    def get_symbol_info(self, symbol: str) -> Optional[TradingRule]:
        """Get trading rule for specific symbol"""
        if not self.exchange_info:
            return None
        return self.exchange_info.trading_rules.get(symbol)
    
    def get_quote_assets(self) -> List[str]:
        """Get list of available quote assets (e.g., USDT, USD, BTC)"""
        if not self.exchange_info:
            return ['USDT']  # Default fallback
        return self.exchange_info.quote_assets
    
    def get_base_assets(self) -> List[str]:
        """Get list of available base assets"""
        if not self.exchange_info:
            return []
        return self.exchange_info.base_assets
    
    def is_exchange_info_current(self) -> bool:
        """Check if exchange info is current (within update interval)"""
        if not self.exchange_info:
            return False
        
        age = (datetime.now() - self.last_update).total_seconds()
        return age < self.update_interval
    
    async def _background_update_loop(self):
        """Background task to update exchange info periodically"""
        while True:
            try:
                await asyncio.sleep(self.update_interval)
                
                if not self.is_exchange_info_current():
                    await self.update_exchange_info()
            
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"[EXCHANGE] Background update error: {e}")
                await asyncio.sleep(300)  # Wait 5 minutes on error
    
    def get_manager_status(self) -> Dict[str, Any]:
        """Get exchange manager status"""
        status = {
            'client_connected': self.client is not None,
            'exchange_info_loaded': self.exchange_info is not None,
            'last_update': self.last_update.isoformat() if self.last_update != datetime.min else None,
            'is_current': self.is_exchange_info_current(),
            'is_testnet': self.is_testnet,
            'update_interval': self.update_interval,
            'cache_enabled': self.cache_enabled,
            'auto_update_enabled': self.auto_update_rules
        }
        
        if self.exchange_info:
            status.update({
                'total_symbols': len(self.exchange_info.symbols),
                'tradeable_pairs': len(self.get_tradeable_pairs()),
                'base_assets': len(self.exchange_info.base_assets),
                'quote_assets': len(self.exchange_info.quote_assets),
                'server_time': self.exchange_info.server_time.isoformat()
            })
        
        return status
    
    async def cleanup(self):
        """Clean up resources"""
        if self.update_task and not self.update_task.done():
            self.update_task.cancel()
            try:
                await self.update_task
            except asyncio.CancelledError:
                pass
        
        self.logger.info("[EXCHANGE] Cleanup completed")

# Global instance
exchange_manager = BinanceExchangeManager()

# Convenience functions
def get_tradeable_pairs() -> List[str]:
    """Get list of tradeable pairs"""
    return exchange_manager.get_tradeable_pairs()

def calculate_position_size(symbol: str, confidence: float, account_balance: float, 
                          current_price: float) -> Tuple[Decimal, Decimal]:
    """Calculate position size for symbol"""
    return exchange_manager.calculate_position_size(symbol, confidence, account_balance, current_price)

def validate_order(symbol: str, quantity: Decimal, price: Decimal) -> Tuple[bool, str, Decimal, Decimal]:
    """Validate order parameters"""
    return exchange_manager.validate_order_parameters(symbol, quantity, price)

def get_symbol_info(symbol: str) -> Optional[TradingRule]:
    """Get symbol trading info"""
    return exchange_manager.get_symbol_info(symbol)

if __name__ == "__main__":
    # Test the exchange manager
    import asyncio
    
    async def test_exchange_manager():
        print("Testing Binance Exchange Manager")
        print("=" * 40)
        
        # Initialize manager
        success = await exchange_manager.initialize()
        print(f"Initialization: {'Success' if success else 'Failed'}")
        
        if success:
            # Get tradeable pairs
            pairs = get_tradeable_pairs()
            print(f"Tradeable pairs: {len(pairs)}")
            print(f"First 10 pairs: {pairs[:10]}")
            
            # Test position sizing for BTC
            if 'BTCUSDT' in pairs:
                qty, value = calculate_position_size('BTCUSDT', 75.0, 1000.0, 50000.0)
                print(f"BTCUSDT position: {qty} BTC (${value})")
            
            # Test order validation
            if pairs:
                symbol = pairs[0]
                is_valid, message, valid_qty, valid_price = validate_order(
                    symbol, Decimal('0.001'), Decimal('50000')
                )
                print(f"{symbol} order validation: {message}")
        
        # Get status
        status = exchange_manager.get_manager_status()
        print(f"Status: {json.dumps(status, indent=2, default=str)}")
        
        # Cleanup
        await exchange_manager.cleanup()
    
    asyncio.run(test_exchange_manager())