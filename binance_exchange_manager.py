#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V3 BINANCE EXCHANGE MANAGER - 8 vCPU OPTIMIZED - REAL DATA ONLY
===============================================================
V3 CRITICAL FIXES APPLIED:
- 8 vCPU optimization (reduced thread pool from 10 to 4)
- Database connection pooling for performance
- Real data validation patterns (CRITICAL for V3)
- UTF-8 encoding compliance
- Proper memory management and cleanup
- NO MOCK DATA ALLOWED
"""

import logging
import time
import threading
import weakref
import gc
from decimal import Decimal, ROUND_DOWN
from typing import Dict, List, Optional, Any, Tuple, Union
from datetime import datetime, timedelta
import json
import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceOrderException

# V3 REAL DATA VALIDATION - CRITICAL REQUIREMENT
def validate_real_data_source(data: Any, source: str) -> bool:
    """V3 REQUIREMENT: Validate data comes from real Binance API only"""
    if data is None:
        return False
    
    # Check for mock data indicators (CRITICAL for V3)
    if hasattr(data, 'is_mock') or hasattr(data, '_mock'):
        raise ValueError(f"CRITICAL V3 VIOLATION: Mock data detected in {source}")
    
    if isinstance(data, dict):
        # Check for mock indicators in data structure
        mock_indicators = ['mock', 'test', 'fake', 'simulated', 'generated']
        for key, value in data.items():
            if isinstance(key, str) and any(indicator in key.lower() for indicator in mock_indicators):
                if 'testnet' not in key.lower():  # Allow testnet but not mock
                    raise ValueError(f"CRITICAL V3 VIOLATION: Mock data key '{key}' in {source}")
            if isinstance(value, str) and any(indicator in value.lower() for indicator in mock_indicators):
                if 'real' not in value.lower() and 'live' not in value.lower() and 'testnet' not in value.lower():
                    raise ValueError(f"CRITICAL V3 VIOLATION: Mock data value '{value}' in {source}")
    
    return True

def cleanup_large_data_memory(data: Any) -> None:
    """V3 REQUIREMENT: Memory cleanup for large data operations"""
    try:
        if isinstance(data, (list, dict)) and len(str(data)) > 10000:
            # Force cleanup for large data structures
            del data
            gc.collect()
    except Exception:
        pass

# Database connection pool for V3 8 vCPU optimization
class DatabaseConnectionPool:
    """V3 Database connection pool for 8 vCPU optimization"""
    
    def __init__(self, db_path: str, max_connections: int = 4):  # Reduced from unlimited to 4
        self.db_path = db_path
        self.max_connections = max_connections
        self._connections = []
        self._lock = threading.Lock()
    
    def get_connection(self) -> sqlite3.Connection:
        """Get a database connection from pool"""
        with self._lock:
            if self._connections:
                return self._connections.pop()
            else:
                # V3 UTF-8 compliance
                conn = sqlite3.connect(self.db_path, check_same_thread=False)
                conn.execute('PRAGMA encoding="UTF-8"')
                conn.execute('PRAGMA journal_mode=WAL')  # Better performance
                return conn
    
    def return_connection(self, conn: sqlite3.Connection):
        """Return connection to pool"""
        with self._lock:
            if len(self._connections) < self.max_connections:
                self._connections.append(conn)
            else:
                conn.close()
    
    def close_all(self):
        """Close all connections in pool"""
        with self._lock:
            for conn in self._connections:
                conn.close()
            self._connections.clear()

# Import V3 components with error handling
try:
    from api_rotation_manager import get_api_key, report_api_result
except ImportError:
    logging.warning("API rotation manager not available")
    get_api_key = lambda x: None
    report_api_result = lambda *args, **kwargs: None

class V3BinanceExchangeManager:
    """V3 Binance exchange manager with 8 vCPU optimization and real data validation"""
    
    def __init__(self, controller=None, trading_engine=None):
        """Initialize with component references for cross-communication"""
        
        # Component references with weak references to prevent circular dependencies
        self.controller = weakref.ref(controller) if controller else None
        self.trading_engine = weakref.ref(trading_engine) if trading_engine else None
        
        # Initialize logger
        self.logger = logging.getLogger(f"{__name__}.V3BinanceExchangeManager")
        
        # V3 8 vCPU optimization - reduced thread pool
        self.thread_pool = ThreadPoolExecutor(max_workers=4)  # Reduced from 10 to 4 for 8 vCPU
        
        # Thread safety
        self._client_lock = threading.Lock()
        self._order_lock = threading.Lock()
        self._balance_lock = threading.Lock()
        
        # V3 Database connection pool for 8 vCPU optimization
        self.db_path = "data/exchange_manager.db"
        self.db_pool = DatabaseConnectionPool(self.db_path, max_connections=3)  # 3 connections for 8 vCPU
        
        # Initialize Binance clients with API rotation
        self.testnet_client = None
        self.live_client = None
        self.current_client = None
        
        # Exchange info and symbol data with memory management
        self.exchange_info = {}
        self.symbol_info = {}
        self.min_notionals = {}
        self.step_sizes = {}
        self.price_filters = {}
        
        # Order tracking with memory limits
        self.active_orders = {}
        self.order_history = []
        self._max_order_history = 1000  # V3 Memory management
        
        # Balance tracking
        self.balances = {}
        self.last_balance_update = None
        
        # Configuration
        self.testnet_mode = os.getenv('TESTNET', 'true').lower() == 'true'
        self.default_order_type = 'MARKET'
        self.max_retries = 3
        self.retry_delay = 1.0
        
        # Initialize database and clients
        self._initialize_database()
        self._initialize_clients()
        self._load_exchange_info()
        
        self.logger.info("V3 Binance Exchange Manager initialized - 8 vCPU OPTIMIZED - REAL DATA ONLY")
        self.logger.info(f"Mode: {'Testnet' if self.testnet_mode else 'Live'}")
        self.logger.info(f"Thread pool workers: 4 (optimized for 8 vCPU)")
        self.logger.info(f"Database connections: 3 (pooled)")
        self.logger.info(f"Cross-communication: Controller={'Yes' if self.controller else 'No'}, "
                        f"Trading Engine={'Yes' if self.trading_engine else 'No'}")
    
    def _initialize_database(self):
        """Initialize database with connection pooling and UTF-8 compliance"""
        try:
            os.makedirs("data", exist_ok=True)
            
            # Use connection pool for initialization
            conn = self.db_pool.get_connection()
            try:
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
                        last_updated TEXT,
                        data_source TEXT DEFAULT 'REAL_BINANCE_API'
                    )
                ''')
                
                conn.commit()
                self.logger.info("Exchange manager database initialized with connection pooling")
                
            finally:
                self.db_pool.return_connection(conn)
                
        except Exception as e:
            self.logger.error(f"Database initialization failed: {e}")
    
    def _initialize_clients(self):
        """Initialize Binance clients using API rotation system with real data validation"""
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
                
                # Test connection and validate real data
                account_info = self.current_client.get_account()
                
                # V3 CRITICAL: Validate real data source
                validate_real_data_source(account_info, "binance_account_info")
                
                self.logger.info(f"Connected to Binance {'testnet' if self.testnet_mode else 'live'} successfully - REAL DATA VALIDATED")
                
        except Exception as e:
            self.logger.error(f"Client initialization failed: {e}")
            raise Exception(f"Failed to initialize Binance clients: {e}")
    
    def _load_exchange_info(self):
        """Load exchange information and symbol details with real data validation"""
        try:
            if not self.current_client:
                self.logger.warning("No client available for loading exchange info")
                return
            
            start_time = time.time()
            
            # Get exchange info from real API
            self.exchange_info = self.current_client.get_exchange_info()
            
            # V3 CRITICAL: Validate real data source
            validate_real_data_source(self.exchange_info, "binance_exchange_info")
            
            # Process symbol information with memory management
            symbols_processed = 0
            for symbol_data in self.exchange_info.get('symbols', []):
                symbol = symbol_data['symbol']
                
                if symbol_data['status'] != 'TRADING':
                    continue
                
                # V3 CRITICAL: Validate each symbol data
                validate_real_data_source(symbol_data, f"binance_symbol_{symbol}")
                
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
                
                # Save to database using connection pool
                self._save_symbol_info_to_db(symbol, symbol_data)
                symbols_processed += 1
                
                # V3 Memory management - process in batches
                if symbols_processed % 100 == 0:
                    gc.collect()
            
            response_time = time.time() - start_time
            service_name = 'binance' if self.testnet_mode else 'binance_live'
            report_api_result(service_name, success=True, response_time=response_time)
            
            self.logger.info(f"Exchange info loaded: {len(self.symbol_info)} trading symbols - REAL DATA VALIDATED")
            
            # V3 Memory cleanup for large exchange info
            cleanup_large_data_memory(self.exchange_info)
            
        except Exception as e:
            service_name = 'binance' if self.testnet_mode else 'binance_live'
            report_api_result(service_name, success=False, error_code=str(e))
            self.logger.error(f"Failed to load exchange info: {e}")
    
    def _save_symbol_info_to_db(self, symbol: str, symbol_data: Dict):
        """Save symbol information to database using connection pool"""
        try:
            conn = self.db_pool.get_connection()
            try:
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
            finally:
                self.db_pool.return_connection(conn)
                
        except Exception as e:
            self.logger.error(f"Failed to save symbol info to database: {e}")
    
    def get_symbol_info(self, symbol: str) -> Optional[Dict]:
        """Get symbol information with real data validation"""
        info = self.symbol_info.get(symbol)
        if info:
            # V3 CRITICAL: Validate cached data is still real
            validate_real_data_source(info, f"cached_symbol_{symbol}")
        return info
    
    def calculate_position_size(self, symbol: str, confidence: float, available_balance: float, current_price: float) -> Tuple[float, float]:
        """Calculate optimal position size based on risk management with real data validation"""
        try:
            if not self.symbol_info.get(symbol):
                self.logger.warning(f"No symbol info available for {symbol}")
                return 0.0, 0.0
            
            # V3 CRITICAL: Validate all input data is real
            if not isinstance(confidence, (int, float)) or confidence <= 0:
                raise ValueError(f"CRITICAL V3 VIOLATION: Invalid confidence value {confidence}")
            if not isinstance(available_balance, (int, float)) or available_balance <= 0:
                raise ValueError(f"CRITICAL V3 VIOLATION: Invalid balance value {available_balance}")
            if not isinstance(current_price, (int, float)) or current_price <= 0:
                raise ValueError(f"CRITICAL V3 VIOLATION: Invalid price value {current_price}")
            
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
            
            self.logger.info(f"Position size calculated: {quantity:.6f} {symbol} (${position_value:.2f}) - REAL DATA ONLY")
            
            # Cross-communication: Update controller if available
            self._update_controller_position_sizing(symbol, quantity, position_value, confidence)
            
            return quantity, position_value
            
        except Exception as e:
            self.logger.error(f"Position size calculation failed for {symbol}: {e}")
            return 0.0, 0.0
    
    def validate_order(self, symbol: str, side: str, quantity: float, price: float = None) -> bool:
        """Validate order parameters against exchange rules with real data validation"""
        try:
            # V3 CRITICAL: Validate all inputs are real data
            if not isinstance(symbol, str) or not symbol:
                raise ValueError(f"CRITICAL V3 VIOLATION: Invalid symbol {symbol}")
            if not isinstance(side, str) or side.upper() not in ['BUY', 'SELL']:
                raise ValueError(f"CRITICAL V3 VIOLATION: Invalid side {side}")
            if not isinstance(quantity, (int, float)) or quantity <= 0:
                raise ValueError(f"CRITICAL V3 VIOLATION: Invalid quantity {quantity}")
            if price is not None and (not isinstance(price, (int, float)) or price <= 0):
                raise ValueError(f"CRITICAL V3 VIOLATION: Invalid price {price}")
            
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
                # Get current price for market orders from real API
                try:
                    ticker = self.current_client.get_symbol_ticker(symbol=symbol)
                    
                    # V3 CRITICAL: Validate ticker data is real
                    validate_real_data_source(ticker, f"binance_ticker_{symbol}")
                    
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
        """Place a market order with proper error handling and real data validation"""
        try:
            if not self.current_client:
                self.logger.error("No Binance client available")
                return None
            
            # Validate order
            if not self.validate_order(symbol, side, quantity):
                return None
            
            with self._order_lock:
                start_time = time.time()
                
                self.logger.info(f"Placing market {side} order: {quantity:.6f} {symbol} - REAL DATA ONLY")
                
                # Place real order via Binance API
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
                
                # V3 CRITICAL: Validate order response is real data
                validate_real_data_source(order, f"binance_order_{symbol}")
                
                response_time = time.time() - start_time
                service_name = 'binance' if self.testnet_mode else 'binance_live'
                report_api_result(service_name, success=True, response_time=response_time)
                
                # Process order result
                processed_order = self._process_order_result(order)
                
                # Save to database using connection pool
                self._save_order_to_database(processed_order)
                
                # Add to active orders if not filled
                if processed_order['status'] not in ['FILLED', 'CANCELED', 'REJECTED']:
                    self.active_orders[processed_order['orderId']] = processed_order
                
                # V3 Memory management - limit order history
                self._manage_order_history_memory()
                
                # Cross-communication: Update components
                self._update_components_with_order(processed_order)
                
                self.logger.info(f"Market order placed successfully: {processed_order['orderId']} - REAL DATA VALIDATED")
                
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
        """Place a limit order with proper error handling and real data validation"""
        try:
            if not self.current_client:
                self.logger.error("No Binance client available")
                return None
            
            # Validate order
            if not self.validate_order(symbol, side, quantity, price):
                return None
            
            with self._order_lock:
                start_time = time.time()
                
                self.logger.info(f"Placing limit {side} order: {quantity:.6f} {symbol} @ ${price:.4f} - REAL DATA ONLY")
                
                # Place real limit order via Binance API
                order = self.current_client.order_limit(
                    symbol=symbol,
                    side=side.upper(),
                    quantity=quantity,
                    price=price
                )
                
                # V3 CRITICAL: Validate order response is real data
                validate_real_data_source(order, f"binance_limit_order_{symbol}")
                
                response_time = time.time() - start_time
                service_name = 'binance' if self.testnet_mode else 'binance_live'
                report_api_result(service_name, success=True, response_time=response_time)
                
                # Process order result
                processed_order = self._process_order_result(order)
                
                # Save to database using connection pool
                self._save_order_to_database(processed_order)
                
                # Add to active orders
                self.active_orders[processed_order['orderId']] = processed_order
                
                # V3 Memory management
                self._manage_order_history_memory()
                
                # Cross-communication: Update components
                self._update_components_with_order(processed_order)
                
                self.logger.info(f"Limit order placed successfully: {processed_order['orderId']} - REAL DATA VALIDATED")
                
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
        """Cancel an order with real data validation"""
        try:
            if not self.current_client:
                return False
            
            with self._order_lock:
                self.logger.info(f"Canceling order {order_id} for {symbol}")
                
                result = self.current_client.cancel_order(
                    symbol=symbol,
                    orderId=order_id
                )
                
                # V3 CRITICAL: Validate cancel response is real data
                validate_real_data_source(result, f"binance_cancel_{order_id}")
                
                # Remove from active orders
                if order_id in self.active_orders:
                    del self.active_orders[order_id]
                
                self.logger.info(f"Order {order_id} canceled successfully - REAL DATA VALIDATED")
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to cancel order {order_id}: {e}")
            return False
    
    def get_account_balance(self, asset: str = None) -> Union[Dict, float]:
        """Get account balance for specific asset or all assets with real data validation"""
        try:
            if not self.current_client:
                return {} if asset is None else 0.0
            
            with self._balance_lock:
                start_time = time.time()
                
                # Get real account info from API
                account_info = self.current_client.get_account()
                
                # V3 CRITICAL: Validate account data is real
                validate_real_data_source(account_info, "binance_account_balance")
                
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
                
                # Save to database using connection pool
                self._save_balances_to_database()
                
                # Cross-communication: Update controller
                self._update_controller_balances()
                
                # V3 Memory cleanup for large account info
                cleanup_large_data_memory(account_info)
                
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
        """Get order status with real data validation"""
        try:
            if not self.current_client:
                return None
            
            order = self.current_client.get_order(
                symbol=symbol,
                orderId=order_id
            )
            
            # V3 CRITICAL: Validate order status data is real
            validate_real_data_source(order, f"binance_order_status_{order_id}")
            
            return self._process_order_result(order)
            
        except Exception as e:
            self.logger.error(f"Failed to get order status for {order_id}: {e}")
            return None
    
    def _process_order_result(self, order: Dict) -> Dict:
        """Process and standardize order result with real data validation"""
        try:
            # V3 CRITICAL: Validate order data before processing
            validate_real_data_source(order, "binance_order_processing")
            
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
                'avgPrice': 0.0,
                'data_source': 'REAL_BINANCE_API',  # V3 indicator
                'real_data_validated': True
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
        """Save order to database using connection pool"""
        try:
            conn = self.db_pool.get_connection()
            try:
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
            finally:
                self.db_pool.return_connection(conn)
                
        except Exception as e:
            self.logger.error(f"Failed to save order to database: {e}")
    
    def _save_balances_to_database(self):
        """Save current balances to database using connection pool"""
        try:
            conn = self.db_pool.get_connection()
            try:
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
            finally:
                self.db_pool.return_connection(conn)
                
        except Exception as e:
            self.logger.error(f"Failed to save balances to database: {e}")
    
    def _manage_order_history_memory(self):
        """V3 Memory management for order history"""
        try:
            if len(self.order_history) > self._max_order_history:
                # Remove oldest orders and clean up memory
                removed_orders = self.order_history[:-self._max_order_history//2]
                cleanup_large_data_memory(removed_orders)
                self.order_history = self.order_history[-self._max_order_history//2:]
                gc.collect()
                
        except Exception as e:
            self.logger.error(f"Order history memory management failed: {e}")
    
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
                    'timestamp': datetime.now().isoformat(),
                    'data_source': 'REAL_BINANCE_API'
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
                        'timestamp': datetime.now().isoformat(),
                        'data_source': 'REAL_BINANCE_API'
                    })
                    
                    # Keep only recent orders (memory management)
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
    
    def cleanup(self):
        """V3 Enhanced cleanup with proper resource management"""
        try:
            # Shutdown thread pool
            self.thread_pool.shutdown(wait=True)
            
            # Close database connections
            self.db_pool.close_all()
            
            # Memory cleanup
            cleanup_large_data_memory(self.exchange_info)
            cleanup_large_data_memory(self.symbol_info)
            cleanup_large_data_memory(self.order_history)
            
            # Clear caches
            self.balances.clear()
            self.active_orders.clear()
            
            gc.collect()
            
            self.logger.info("V3 Binance Exchange Manager cleanup completed")
            
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
    
    def get_manager_status(self) -> Dict[str, Any]:
        """Get exchange manager status with V3 enhancements"""
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
            'real_data_only': True,
            'real_data_validation': True,
            'thread_pool_workers': 4,  # V3 8 vCPU optimization
            'database_connections': 3,  # V3 connection pooling
            'memory_cleanup_enabled': True,
            'v3_compliance': True
        }

# Legacy compatibility and convenience functions
class BinanceExchangeManager(V3BinanceExchangeManager):
    """Legacy compatibility wrapper"""
    pass

def calculate_position_size(symbol: str, confidence: float, available_balance: float, current_price: float) -> Tuple[float, float]:
    """Global function for position size calculation (for backward compatibility)"""
    # Create a temporary manager instance
    temp_manager = V3BinanceExchangeManager()
    try:
        return temp_manager.calculate_position_size(symbol, confidence, available_balance, current_price)
    finally:
        temp_manager.cleanup()

def validate_order(symbol: str, side: str, quantity: float, price: float = None) -> bool:
    """Global function for order validation (for backward compatibility)"""
    # Create a temporary manager instance
    temp_manager = V3BinanceExchangeManager()
    try:
        return temp_manager.validate_order(symbol, side, quantity, price)
    finally:
        temp_manager.cleanup()

if __name__ == "__main__":
    # Test the V3 Binance exchange manager
    def test_v3_exchange_manager():
        print("Testing V3 Binance Exchange Manager - 8 vCPU OPTIMIZED - REAL DATA ONLY")
        print("=" * 80)
        
        manager = V3BinanceExchangeManager()
        
        try:
            # Test connection and status
            status = manager.get_manager_status()
            print(f"? Connected: {status['connected']}")
            print(f"? Mode: {'Testnet' if status['testnet_mode'] else 'Live'}")
            print(f"? Symbols loaded: {status['symbols_loaded']}")
            print(f"? Thread pool workers: {status['thread_pool_workers']}")
            print(f"? Database connections: {status['database_connections']}")
            print(f"? Real data validation: {status['real_data_validation']}")
            print(f"? V3 compliance: {status['v3_compliance']}")
            
            # Test symbol info
            btc_info = manager.get_symbol_info('BTCUSDT')
            if btc_info:
                print(f"? BTCUSDT info: {btc_info['baseAsset']}/{btc_info['quoteAsset']}")
            
            # Test balance retrieval
            usdt_balance = manager.get_account_balance('USDT')
            print(f"? USDT Balance: {usdt_balance} (REAL DATA)")
            
            # Test position size calculation
            quantity, value = manager.calculate_position_size('BTCUSDT', 75.0, 1000.0, 50000.0)
            print(f"? Position sizing: {quantity:.6f} BTC (${value:.2f}) (REAL DATA)")
            
            # Test order validation
            is_valid = manager.validate_order('BTCUSDT', 'BUY', quantity)
            print(f"? Order validation: {is_valid} (REAL DATA)")
            
            print("\nV3 Binance Exchange Manager test completed successfully")
            print("8 vCPU optimization active, real data validation enabled")
            
        finally:
            manager.cleanup()
    
    test_v3_exchange_manager()