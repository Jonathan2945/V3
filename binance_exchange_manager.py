#!/usr/bin/env python3
"""
V3 BINANCE EXCHANGE MANAGER - FIXED INTEGRATION & REAL DATA ONLY
================================================================
FIXES APPLIED:
- Enhanced integration with API rotation system
- Proper cross-communication with trading engine and controller
- Thread-safe operations and error handling
- Real data only compliance (no mock/simulated data)
- Better order validation and risk management
- Memory management and resource cleanup
"""

import logging
import time
import threading
import weakref
from decimal import Decimal, ROUND_DOWN
from typing import Dict, List, Optional, Any, Tuple, Union
from datetime import datetime, timedelta
import json
import os
import sqlite3
from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceOrderException

# Import V3 components with error handling
try:
    from api_rotation_manager import get_api_key, report_api_result
except ImportError:
    logging.warning("API rotation manager not available")
    get_api_key = lambda x: None
    report_api_result = lambda *args, **kwargs: None

class V3BinanceExchangeManager:
    """V3 Binance exchange manager with enhanced integration"""
    
    def __init__(self, controller=None, trading_engine=None):
        """Initialize with component references for cross-communication"""
        
        # Component references with weak references to prevent circular dependencies
        self.controller = weakref.ref(controller) if controller else None
        self.trading_engine = weakref.ref(trading_engine) if trading_engine else None
        
        # Initialize logger
        self.logger = logging.getLogger(f"{__name__}.V3BinanceExchangeManager")
        
        # Thread safety
        self._client_lock = threading.Lock()
        self._order_lock = threading.Lock()
        self._balance_lock = threading.Lock()
        
        # Initialize Binance clients with API rotation
        self.testnet_client = None
        self.live_client = None
        self.current_client = None
        
        # Exchange info and symbol data
        self.exchange_info = {}
        self.symbol_info = {}
        self.min_notionals = {}
        self.step_sizes = {}
        self.price_filters = {}
        
        # Order tracking
        self.active_orders = {}
        self.order_history = []
        
        # Balance tracking
        self.balances = {}
        self.last_balance_update = None
        
        # Configuration
        self.testnet_mode = os.getenv('TESTNET', 'true').lower() == 'true'
        self.default_order_type = 'MARKET'
        self.max_retries = 3
        self.retry_delay = 1.0
        
        # Database for order and balance history
        self.db_path = "data/exchange_manager.db"
        self._initialize_database()
        
        # Initialize clients
        self._initialize_clients()
        
        # Load exchange info
        self._load_exchange_info()
        
        self.logger.info("V3 Binance Exchange Manager initialized - REAL DATA ONLY")
        self.logger.info(f"Mode: {'Testnet' if self.testnet_mode else 'Live'}")
        self.logger.info(f"Cross-communication: Controller={'Yes' if self.controller else 'No'}, "
                        f"Trading Engine={'Yes' if self.trading_engine else 'No'}")
    
    def _initialize_database(self):
        """Initialize database for order and balance tracking"""
        try:
            os.makedirs("data", exist_ok=True)
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Order history table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS order_history (
                    id INTEGER PRIMARY KEY,
                    order_id TEXT,
                    symbol TEXT,
                    side TEXT,
                    type TEXT,
                    quantity REAL,
                    price REAL,
                    status TEXT,
                    executed_qty REAL,
                    executed_price REAL,
                    commission REAL,
                    commission_asset TEXT,
                    timestamp TEXT,
                    client_order_id TEXT,
                    fills TEXT,
                    data_source TEXT DEFAULT 'REAL_BINANCE_API'
                )
            ''')
            
            # Balance history table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS balance_history (
                    id INTEGER PRIMARY KEY,
                    asset TEXT,
                    free REAL,
                    locked REAL,
                    total REAL,
                    timestamp TEXT,
                    data_source TEXT DEFAULT 'REAL_BINANCE_API'
                )
            ''')
            
            # Symbol info table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS symbol_info (
                    symbol TEXT PRIMARY KEY,
                    base_asset TEXT,
                    quote_asset TEXT,
                    min_notional REAL,
                    step_size REAL,
                    tick_size REAL,
                    min_qty REAL,
                    max_qty REAL,
                    status TEXT,
                    last_updated TEXT
                )
            ''')
            
            conn.commit()
            conn.close()
            
            self.logger.info("Exchange manager database initialized")
            
        except Exception as e:
            self.logger.error(f"Database initialization failed: {e}")
    
    def _initialize_clients(self):
        """Initialize Binance clients using API rotation system"""
        try:
            with self._client_lock:
                # Initialize testnet client
                testnet_creds = get_api_key('binance')
                if testnet_creds:
                    if isinstance(testnet_creds, dict):
                        self.testnet_client = Client(
                            testnet_creds['api_key'], 
                            testnet_creds['api_secret'], 
                            testnet=True
                        )
                    else:
                        # Legacy format
                        api_secret = os.getenv('BINANCE_API_SECRET_1', '')
                        self.testnet_client = Client(testnet_creds, api_secret, testnet=True)
                    
                    self.logger.info("Testnet client initialized via API rotation")
                
                # Initialize live client
                live_creds = get_api_key('binance_live')
                if live_creds:
                    if isinstance(live_creds, dict):
                        self.live_client = Client(
                            live_creds['api_key'], 
                            live_creds['api_secret'], 
                            testnet=False
                        )
                    else:
                        # Fallback to environment
                        api_key = os.getenv('BINANCE_LIVE_API_KEY_1', '')
                        api_secret = os.getenv('BINANCE_LIVE_API_SECRET_1', '')
                        if api_key and api_secret:
                            self.live_client = Client(api_key, api_secret, testnet=False)
                    
                    self.logger.info("Live client initialized")
                
                # Set current client based on mode
                if self.testnet_mode:
                    self.current_client = self.testnet_client
                else:
                    self.current_client = self.live_client
                
                if not self.current_client:
                    raise Exception("No valid Binance client available")
                
                # Test connection
                account_info = self.current_client.get_account()
                self.logger.info(f"Connected to Binance {'testnet' if self.testnet_mode else 'live'} successfully")
                
        except Exception as e:
            self.logger.error(f"Client initialization failed: {e}")
            raise Exception(f"Failed to initialize Binance clients: {e}")
    
    def _load_exchange_info(self):
        """Load exchange information and symbol details"""
        try:
            if not self.current_client:
                self.logger.warning("No client available for loading exchange info")
                return
            
            start_time = time.time()
            
            # Get exchange info from real API
            self.exchange_info = self.current_client.get_exchange_info()
            
            # Process symbol information
            for symbol_data in self.exchange_info.get('symbols', []):
                symbol = symbol_data['symbol']
                
                if symbol_data['status'] != 'TRADING':
                    continue
                
                self.symbol_info[symbol] = {
                    'baseAsset': symbol_data['baseAsset'],
                    'quoteAsset': symbol_data['quoteAsset'],
                    'status': symbol_data['status'],
                    'isSpotTradingAllowed': symbol_data.get('isSpotTradingAllowed', False),
                    'permissions': symbol_data.get('permissions', [])
                }
                
                # Process filters
                for filter_data in symbol_data.get('filters', []):
                    if filter_data['filterType'] == 'MIN_NOTIONAL':
                        self.min_notionals[symbol] = float(filter_data['minNotional'])
                    elif filter_data['filterType'] == 'LOT_SIZE':
                        self.step_sizes[symbol] = float(filter_data['stepSize'])
                    elif filter_data['filterType'] == 'PRICE_FILTER':
                        self.price_filters[symbol] = {
                            'minPrice': float(filter_data['minPrice']),
                            'maxPrice': float(filter_data['maxPrice']),
                            'tickSize': float(filter_data['tickSize'])
                        }
                
                # Save to database
                self._save_symbol_info_to_db(symbol, symbol_data)
            
            response_time = time.time() - start_time
            service_name = 'binance' if self.testnet_mode else 'binance_live'
            report_api_result(service_name, success=True, response_time=response_time)
            
            self.logger.info(f"Exchange info loaded: {len(self.symbol_info)} trading symbols")
            
        except Exception as e:
            service_name = 'binance' if self.testnet_mode else 'binance_live'
            report_api_result(service_name, success=False, error_code=str(e))
            self.logger.error(f"Failed to load exchange info: {e}")
    
    def _save_symbol_info_to_db(self, symbol: str, symbol_data: Dict):
        """Save symbol information to database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Extract filter data
            min_notional = self.min_notionals.get(symbol, 0.0)
            step_size = self.step_sizes.get(symbol, 0.0)
            price_filter = self.price_filters.get(symbol, {})
            
            cursor.execute('''
                INSERT OR REPLACE INTO symbol_info (
                    symbol, base_asset, quote_asset, min_notional, step_size,
                    tick_size, min_qty, max_qty, status, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                symbol,
                symbol_data['baseAsset'],
                symbol_data['quoteAsset'],
                min_notional,
                step_size,
                price_filter.get('tickSize', 0.0),
                0.0,  # Would need to extract from LOT_SIZE filter
                0.0,  # Would need to extract from LOT_SIZE filter
                symbol_data['status'],
                datetime.now().isoformat()
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Failed to save symbol info to database: {e}")
    
    def get_symbol_info(self, symbol: str) -> Optional[Dict]:
        """Get symbol information"""
        return self.symbol_info.get(symbol)
    
    def calculate_position_size(self, symbol: str, confidence: float, available_balance: float, current_price: float) -> Tuple[float, float]:
        """Calculate optimal position size based on risk management"""
        try:
            if not self.symbol_info.get(symbol):
                self.logger.warning(f"No symbol info available for {symbol}")
                return 0.0, 0.0
            
            # Risk management parameters
            max_risk_percent = float(os.getenv('MAX_RISK_PERCENT', '1.0'))
            trade_amount_usdt = float(os.getenv('TRADE_AMOUNT_USDT', '100.0'))
            
            # Base position size on confidence and available balance
            confidence_factor = max(0.5, min(1.0, confidence / 100.0))
            
            # Calculate risk amount
            risk_amount = min(
                available_balance * (max_risk_percent / 100),
                trade_amount_usdt
            )
            
            # Adjust by confidence
            adjusted_risk_amount = risk_amount * confidence_factor
            
            # Calculate quantity
            quantity = adjusted_risk_amount / current_price
            
            # Round to step size
            step_size = self.step_sizes.get(symbol, 0.001)
            if step_size > 0:
                quantity = float(Decimal(str(quantity)).quantize(
                    Decimal(str(step_size)), rounding=ROUND_DOWN
                ))
            
            # Check minimum notional
            min_notional = self.min_notionals.get(symbol, 10.0)
            position_value = quantity * current_price
            
            if position_value < min_notional:
                self.logger.warning(f"Position value ${position_value:.2f} below minimum ${min_notional:.2f} for {symbol}")
                return 0.0, 0.0
            
            self.logger.info(f"Position size calculated: {quantity:.6f} {symbol} (${position_value:.2f})")
            
            # Cross-communication: Update controller if available
            self._update_controller_position_sizing(symbol, quantity, position_value, confidence)
            
            return quantity, position_value
            
        except Exception as e:
            self.logger.error(f"Position size calculation failed for {symbol}: {e}")
            return 0.0, 0.0
    
    def validate_order(self, symbol: str, side: str, quantity: float, price: float = None) -> bool:
        """Validate order parameters against exchange rules"""
        try:
            # Check if symbol exists and is trading
            symbol_data = self.symbol_info.get(symbol)
            if not symbol_data:
                self.logger.error(f"Symbol {symbol} not found")
                return False
            
            if symbol_data['status'] != 'TRADING':
                self.logger.error(f"Symbol {symbol} not trading (status: {symbol_data['status']})")
                return False
            
            # Check step size
            step_size = self.step_sizes.get(symbol, 0.001)
            if step_size > 0:
                remainder = quantity % step_size
                if remainder != 0:
                    self.logger.error(f"Quantity {quantity} doesn't match step size {step_size} for {symbol}")
                    return False
            
            # Check minimum notional
            min_notional = self.min_notionals.get(symbol, 10.0)
            if price:
                notional_value = quantity * price
            else:
                # Get current price for market orders
                try:
                    ticker = self.current_client.get_symbol_ticker(symbol=symbol)
                    current_price = float(ticker['price'])
                    notional_value = quantity * current_price
                except Exception as e:
                    self.logger.error(f"Failed to get current price for {symbol}: {e}")
                    return False
            
            if notional_value < min_notional:
                self.logger.error(f"Notional value ${notional_value:.2f} below minimum ${min_notional:.2f} for {symbol}")
                return False
            
            # Check price filter if limit order
            if price:
                price_filter = self.price_filters.get(symbol)
                if price_filter:
                    if price < price_filter['minPrice'] or price > price_filter['maxPrice']:
                        self.logger.error(f"Price {price} outside valid range for {symbol}")
                        return False
                    
                    tick_size = price_filter['tickSize']
                    if tick_size > 0 and (price % tick_size) != 0:
                        self.logger.error(f"Price {price} doesn't match tick size {tick_size} for {symbol}")
                        return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Order validation failed for {symbol}: {e}")
            return False
    
    def place_market_order(self, symbol: str, side: str, quantity: float) -> Optional[Dict]:
        """Place a market order with proper error handling"""
        try:
            if not self.current_client:
                self.logger.error("No Binance client available")
                return None
            
            # Validate order
            if not self.validate_order(symbol, side, quantity):
                return None
            
            with self._order_lock:
                start_time = time.time()
                
                self.logger.info(f"Placing market {side} order: {quantity:.6f} {symbol}")
                
                # Place real order
                if side.upper() == 'BUY':
                    order = self.current_client.order_market_buy(
                        symbol=symbol,
                        quantity=quantity
                    )
                else:
                    order = self.current_client.order_market_sell(
                        symbol=symbol,
                        quantity=quantity
                    )
                
                response_time = time.time() - start_time
                service_name = 'binance' if self.testnet_mode else 'binance_live'
                report_api_result(service_name, success=True, response_time=response_time)
                
                # Process order result
                processed_order = self._process_order_result(order)
                
                # Save to database
                self._save_order_to_database(processed_order)
                
                # Add to active orders if not filled
                if processed_order['status'] not in ['FILLED', 'CANCELED', 'REJECTED']:
                    self.active_orders[processed_order['orderId']] = processed_order
                
                # Cross-communication: Update components
                self._update_components_with_order(processed_order)
                
                self.logger.info(f"Market order placed successfully: {processed_order['orderId']}")
                
                return processed_order
                
        except BinanceAPIException as e:
            service_name = 'binance' if self.testnet_mode else 'binance_live'
            report_api_result(service_name, success=False, error_code=f"API_{e.code}")
            self.logger.error(f"Binance API error placing order: {e}")
            return None
        except BinanceOrderException as e:
            self.logger.error(f"Binance order error: {e}")
            return None
        except Exception as e:
            service_name = 'binance' if self.testnet_mode else 'binance_live'
            report_api_result(service_name, success=False, error_code=str(e))
            self.logger.error(f"Failed to place market order: {e}")
            return None
    
    def place_limit_order(self, symbol: str, side: str, quantity: float, price: float) -> Optional[Dict]:
        """Place a limit order with proper error handling"""
        try:
            if not self.current_client:
                self.logger.error("No Binance client available")
                return None
            
            # Validate order
            if not self.validate_order(symbol, side, quantity, price):
                return None
            
            with self._order_lock:
                start_time = time.time()
                
                self.logger.info(f"Placing limit {side} order: {quantity:.6f} {symbol} @ ${price:.4f}")
                
                # Place real limit order
                order = self.current_client.order_limit(
                    symbol=symbol,
                    side=side.upper(),
                    quantity=quantity,
                    price=price
                )
                
                response_time = time.time() - start_time
                service_name = 'binance' if self.testnet_mode else 'binance_live'
                report_api_result(service_name, success=True, response_time=response_time)
                
                # Process order result
                processed_order = self._process_order_result(order)
                
                # Save to database
                self._save_order_to_database(processed_order)
                
                # Add to active orders
                self.active_orders[processed_order['orderId']] = processed_order
                
                # Cross-communication: Update components
                self._update_components_with_order(processed_order)
                
                self.logger.info(f"Limit order placed successfully: {processed_order['orderId']}")
                
                return processed_order
                
        except BinanceAPIException as e:
            service_name = 'binance' if self.testnet_mode else 'binance_live'
            report_api_result(service_name, success=False, error_code=f"API_{e.code}")
            self.logger.error(f"Binance API error placing limit order: {e}")
            return None
        except BinanceOrderException as e:
            self.logger.error(f"Binance order error: {e}")
            return None
        except Exception as e:
            service_name = 'binance' if self.testnet_mode else 'binance_live'
            report_api_result(service_name, success=False, error_code=str(e))
            self.logger.error(f"Failed to place limit order: {e}")
            return None
    
    def cancel_order(self, symbol: str, order_id: str) -> bool:
        """Cancel an order"""
        try:
            if not self.current_client:
                return False
            
            with self._order_lock:
                self.logger.info(f"Canceling order {order_id} for {symbol}")
                
                result = self.current_client.cancel_order(
                    symbol=symbol,
                    orderId=order_id
                )
                
                # Remove from active orders
                if order_id in self.active_orders:
                    del self.active_orders[order_id]
                
                self.logger.info(f"Order {order_id} canceled successfully")
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to cancel order {order_id}: {e}")
            return False
    
    def get_account_balance(self, asset: str = None) -> Union[Dict, float]:
        """Get account balance for specific asset or all assets"""
        try:
            if not self.current_client:
                return {} if asset is None else 0.0
            
            with self._balance_lock:
                start_time = time.time()
                
                # Get real account info from API
                account_info = self.current_client.get_account()
                
                response_time = time.time() - start_time
                service_name = 'binance' if self.testnet_mode else 'binance_live'
                report_api_result(service_name, success=True, response_time=response_time)
                
                # Update balances cache
                self.balances = {}
                for balance in account_info['balances']:
                    asset_name = balance['asset']
                    free_balance = float(balance['free'])
                    locked_balance = float(balance['locked'])
                    total_balance = free_balance + locked_balance
                    
                    if total_balance > 0:  # Only store non-zero balances
                        self.balances[asset_name] = {
                            'free': free_balance,
                            'locked': locked_balance,
                            'total': total_balance
                        }
                
                self.last_balance_update = datetime.now()
                
                # Save to database
                self._save_balances_to_database()
                
                # Cross-communication: Update controller
                self._update_controller_balances()
                
                if asset:
                    balance_info = self.balances.get(asset, {'free': 0.0, 'locked': 0.0, 'total': 0.0})
                    return balance_info['free']
                else:
                    return self.balances
                    
        except Exception as e:
            service_name = 'binance' if self.testnet_mode else 'binance_live'
            report_api_result(service_name, success=False, error_code=str(e))
            self.logger.error(f"Failed to get account balance: {e}")
            return {} if asset is None else 0.0
    
    def get_order_status(self, symbol: str, order_id: str) -> Optional[Dict]:
        """Get order status"""
        try:
            if not self.current_client:
                return None
            
            order = self.current_client.get_order(
                symbol=symbol,
                orderId=order_id
            )
            
            return self._process_order_result(order)
            
        except Exception as e:
            self.logger.error(f"Failed to get order status for {order_id}: {e}")
            return None
    
    def _process_order_result(self, order: Dict) -> Dict:
        """Process and standardize order result"""
        try:
            processed = {
                'orderId': order['orderId'],
                'clientOrderId': order['clientOrderId'],
                'symbol': order['symbol'],
                'side': order['side'],
                'type': order['type'],
                'status': order['status'],
                'quantity': float(order['origQty']),
                'price': float(order['price']) if order['price'] != '0.00000000' else 0.0,
                'executedQty': float(order['executedQty']),
                'cummulativeQuoteQty': float(order['cummulativeQuoteQty']),
                'timeInForce': order.get('timeInForce'),
                'transactTime': order['transactTime'],
                'fills': order.get('fills', []),
                'commission': 0.0,
                'commissionAsset': '',
                'avgPrice': 0.0
            }
            
            # Calculate average execution price and commission
            if processed['fills']:
                total_qty = 0.0
                total_value = 0.0
                total_commission = 0.0
                commission_asset = ''
                
                for fill in processed['fills']:
                    qty = float(fill['qty'])
                    price = float(fill['price'])
                    commission = float(fill['commission'])
                    
                    total_qty += qty
                    total_value += qty * price
                    total_commission += commission
                    commission_asset = fill['commissionAsset']
                
                if total_qty > 0:
                    processed['avgPrice'] = total_value / total_qty
                
                processed['commission'] = total_commission
                processed['commissionAsset'] = commission_asset
            
            return processed
            
        except Exception as e:
            self.logger.error(f"Failed to process order result: {e}")
            return order
    
    def _save_order_to_database(self, order: Dict):
        """Save order to database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO order_history (
                    order_id, symbol, side, type, quantity, price, status,
                    executed_qty, executed_price, commission, commission_asset,
                    timestamp, client_order_id, fills
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                str(order['orderId']),
                order['symbol'],
                order['side'],
                order['type'],
                order['quantity'],
                order['price'],
                order['status'],
                order['executedQty'],
                order.get('avgPrice', 0.0),
                order.get('commission', 0.0),
                order.get('commissionAsset', ''),
                datetime.now().isoformat(),
                order['clientOrderId'],
                json.dumps(order.get('fills', []))
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Failed to save order to database: {e}")
    
    def _save_balances_to_database(self):
        """Save current balances to database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            timestamp = datetime.now().isoformat()
            
            for asset, balance_info in self.balances.items():
                cursor.execute('''
                    INSERT INTO balance_history (
                        asset, free, locked, total, timestamp
                    ) VALUES (?, ?, ?, ?, ?)
                ''', (
                    asset,
                    balance_info['free'],
                    balance_info['locked'],
                    balance_info['total'],
                    timestamp
                ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Failed to save balances to database: {e}")
    
    # Cross-communication methods
    
    def _update_controller_position_sizing(self, symbol: str, quantity: float, position_value: float, confidence: float):
        """Update controller with position sizing information"""
        if not self.controller or not self.controller():
            return
        
        try:
            controller = self.controller()
            
            # Update controller with position sizing data
            with controller._state_lock:
                if not hasattr(controller, 'position_sizing_data'):
                    controller.position_sizing_data = {}
                
                controller.position_sizing_data[symbol] = {
                    'quantity': quantity,
                    'position_value': position_value,
                    'confidence': confidence,
                    'timestamp': datetime.now().isoformat()
                }
                
        except Exception as e:
            self.logger.debug(f"Could not update controller position sizing: {e}")
    
    def _update_components_with_order(self, order: Dict):
        """Update components with order information"""
        # Update controller
        if self.controller and self.controller():
            try:
                controller = self.controller()
                with controller._state_lock:
                    if not hasattr(controller, 'recent_orders'):
                        controller.recent_orders = []
                    
                    controller.recent_orders.append({
                        'order_id': order['orderId'],
                        'symbol': order['symbol'],
                        'side': order['side'],
                        'quantity': order['quantity'],
                        'status': order['status'],
                        'timestamp': datetime.now().isoformat()
                    })
                    
                    # Keep only recent orders
                    if len(controller.recent_orders) > 100:
                        controller.recent_orders = controller.recent_orders[-100:]
                        
            except Exception as e:
                self.logger.debug(f"Could not update controller with order: {e}")
        
        # Update trading engine
        if self.trading_engine and self.trading_engine():
            try:
                engine = self.trading_engine()
                
                # Update engine with order information
                if hasattr(engine, 'recent_orders'):
                    engine.recent_orders.append(order)
                    
            except Exception as e:
                self.logger.debug(f"Could not update trading engine with order: {e}")
    
    def _update_controller_balances(self):
        """Update controller with balance information"""
        if not self.controller or not self.controller():
            return
        
        try:
            controller = self.controller()
            
            with controller._state_lock:
                # Update balance data in controller
                if not hasattr(controller, 'account_balances'):
                    controller.account_balances = {}
                
                controller.account_balances = self.balances.copy()
                controller.balance_last_updated = self.last_balance_update
                
                # Calculate total portfolio value in USDT
                total_usdt = 0.0
                for asset, balance_info in self.balances.items():
                    if asset == 'USDT':
                        total_usdt += balance_info['total']
                    # Could add price conversion for other assets here
                
                controller.total_portfolio_value = total_usdt
                
        except Exception as e:
            self.logger.debug(f"Could not update controller balances: {e}")
    
    def get_manager_status(self) -> Dict[str, Any]:
        """Get exchange manager status"""
        return {
            'connected': bool(self.current_client),
            'testnet_mode': self.testnet_mode,
            'symbols_loaded': len(self.symbol_info),
            'active_orders': len(self.active_orders),
            'cached_balances': len(self.balances),
            'last_balance_update': self.last_balance_update.isoformat() if self.last_balance_update else None,
            'exchange_info_loaded': bool(self.exchange_info),
            'api_rotation_enabled': True,
            'cross_communication_active': bool(self.controller or self.trading_engine),
            'real_data_only': True
        }

# Legacy compatibility and convenience functions
class BinanceExchangeManager(V3BinanceExchangeManager):
    """Legacy compatibility wrapper"""
    pass

def calculate_position_size(symbol: str, confidence: float, available_balance: float, current_price: float) -> Tuple[float, float]:
    """Global function for position size calculation (for backward compatibility)"""
    # Create a temporary manager instance
    temp_manager = V3BinanceExchangeManager()
    return temp_manager.calculate_position_size(symbol, confidence, available_balance, current_price)

def validate_order(symbol: str, side: str, quantity: float, price: float = None) -> bool:
    """Global function for order validation (for backward compatibility)"""
    # Create a temporary manager instance
    temp_manager = V3BinanceExchangeManager()
    return temp_manager.validate_order(symbol, side, quantity, price)

if __name__ == "__main__":
    # Test the V3 Binance exchange manager
    def test_v3_exchange_manager():
        print("Testing V3 Binance Exchange Manager - REAL DATA & INTEGRATION")
        print("=" * 70)
        
        manager = V3BinanceExchangeManager()
        
        # Test connection and status
        status = manager.get_manager_status()
        print(f"? Connected: {status['connected']}")
        print(f"? Mode: {'Testnet' if status['testnet_mode'] else 'Live'}")
        print(f"? Symbols loaded: {status['symbols_loaded']}")
        print(f"? Cross-communication: {status['cross_communication_active']}")
        
        # Test symbol info
        btc_info = manager.get_symbol_info('BTCUSDT')
        if btc_info:
            print(f"? BTCUSDT info: {btc_info['baseAsset']}/{btc_info['quoteAsset']}")
        
        # Test balance retrieval
        usdt_balance = manager.get_account_balance('USDT')
        print(f"? USDT Balance: {usdt_balance}")
        
        # Test position size calculation
        quantity, value = manager.calculate_position_size('BTCUSDT', 75.0, 1000.0, 50000.0)
        print(f"? Position sizing: {quantity:.6f} BTC (${value:.2f})")
        
        # Test order validation
        is_valid = manager.validate_order('BTCUSDT', 'BUY', quantity)
        print(f"? Order validation: {is_valid}")
        
        print("V3 Binance Exchange Manager test completed")
    
    test_v3_exchange_manager()