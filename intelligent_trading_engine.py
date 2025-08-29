#!/usr/bin/env python3
"""
V3 INTELLIGENT TRADING ENGINE - REAL TRADING ONLY
=================================================
REAL IMPLEMENTATION:
- Actual Binance order execution
- Real position tracking from Binance account
- Real P&L from actual trades
- Live market data only
- No simulation or fake data
"""
from binance.client import Client
from binance.exceptions import BinanceAPIException
import logging
import asyncio
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import json
import numpy as np
from pnl_persistence import PnLPersistence
from api_rotation_manager import get_api_key, report_api_result
import time

# Trading configuration from environment
TRADE_AMOUNT_USDT = float(os.getenv('TRADE_AMOUNT_USDT', '100.0'))
MIN_CONFIDENCE = float(os.getenv('MIN_CONFIDENCE', '70.0'))
MAX_TOTAL_POSITIONS = int(os.getenv('MAX_TOTAL_POSITIONS', '3'))
MAX_RISK_PERCENT = float(os.getenv('MAX_RISK_PERCENT', '1.0'))

class IntelligentTradingEngine:
    """V3 Trading Engine: Real Binance Trading Only"""
    
    def __init__(self, data_manager=None, data_collector=None, 
                 market_analyzer=None, ml_engine=None):
        """Initialize real trading engine"""
        self.data_manager = data_manager
        self.data_collector = data_collector
        self.market_analyzer = market_analyzer
        self.ml_engine = ml_engine
        
        # Real trading state
        self.is_trading = False
        self.positions = {}
        self.open_orders = {}
        
        # Real persistence system
        self.pnl_persistence = PnLPersistence()
        saved_metrics = self.pnl_persistence.load_metrics()
        
        # Real performance tracking from database
        self.total_trades = saved_metrics.get('total_trades', 0)
        self.winning_trades = saved_metrics.get('winning_trades', 0)
        self.total_pnl = saved_metrics.get('total_pnl', 0.0)
        self.daily_trades = saved_metrics.get('daily_trades', 0)
        
        # Real risk management
        self.max_positions = MAX_TOTAL_POSITIONS
        self.max_risk_percent = MAX_RISK_PERCENT
        self.min_confidence = MIN_CONFIDENCE
        self.trade_amount_usdt = TRADE_AMOUNT_USDT
        
        # Trading mode
        self.testnet_mode = os.getenv('TESTNET', 'true').lower() == 'true'
        self.last_trade_time = None
        
        # Initialize real Binance client
        self.client = None
        self._initialize_real_binance_client()
        
        logging.info(f"[REAL_ENGINE] Intelligent Trading Engine - REAL BINANCE TRADING ONLY")
        logging.info(f"[REAL_ENGINE] Trade Amount: ${self.trade_amount_usdt}, Min Confidence: {self.min_confidence}%")
    
    def _initialize_real_binance_client(self):
        """Initialize real Binance client with API rotation"""
        try:
            # Get real credentials from API rotation
            if self.testnet_mode:
                binance_creds = get_api_key('binance')
            else:
                binance_creds = get_api_key('binance_live')
            
            if not binance_creds:
                raise Exception("No real Binance credentials available")
            
            if isinstance(binance_creds, dict):
                api_key = binance_creds.get('api_key')
                api_secret = binance_creds.get('api_secret')
            else:
                raise Exception("Invalid credential format")
            
            if not api_key or not api_secret:
                raise Exception("Incomplete Binance credentials")
            
            # Create real Binance client
            if self.testnet_mode:
                self.client = Client(api_key, api_secret, testnet=True)
                logging.info("[REAL_ENGINE] Connected to REAL Binance testnet")
            else:
                self.client = Client(api_key, api_secret, testnet=False)
                logging.info("[REAL_ENGINE] Connected to REAL Binance live")
            
            # Test real connection
            account_info = self.client.get_account()
            ticker = self.client.get_symbol_ticker(symbol="BTCUSDT")
            current_btc = float(ticker['price'])
            
            logging.info(f"[REAL_ENGINE] Real connection verified - BTC: ${current_btc:,.2f}")
            print(f"[REAL_ENGINE] Real connection verified - BTC: ${current_btc:,.2f}")
            
            # Get real account balance
            self._sync_real_account_state()
            
            return True
            
        except Exception as e:
            logging.error(f"Real Binance client initialization failed: {e}")
            raise Exception(f"Real Binance connection failed: {e}")
    
    def _sync_real_account_state(self):
        """Sync with real Binance account state"""
        try:
            account = self.client.get_account()
            
            # Get real USDT balance
            usdt_balance = 0.0
            for balance in account['balances']:
                if balance['asset'] == 'USDT':
                    usdt_balance = float(balance['free']) + float(balance['locked'])
                    break
            
            # Check for real open positions
            real_positions = {}
            for balance in account['balances']:
                asset = balance['asset']
                free = float(balance['free'])
                locked = float(balance['locked'])
                
                if free > 0 or locked > 0:
                    if asset != 'USDT':  # Non-USDT assets are positions
                        real_positions[asset] = {
                            'asset': asset,
                            'free': free,
                            'locked': locked,
                            'total': free + locked
                        }
            
            logging.info(f"[REAL_ENGINE] Real account sync - USDT: ${usdt_balance:.2f}, Positions: {len(real_positions)}")
            
            # Store real state
            self.real_usdt_balance = usdt_balance
            self.real_positions = real_positions
            
            return True
            
        except Exception as e:
            logging.error(f"Real account sync failed: {e}")
            return False
    
    def get_live_market_data(self, symbol="BTCUSDT"):
        """Get real market data from Binance API"""
        try:
            if not self.client:
                raise Exception("No real Binance client connected")
            
            start_time = datetime.now().timestamp()
            
            # Get real market data
            ticker = self.client.get_symbol_ticker(symbol=symbol)
            stats = self.client.get_ticker(symbol=symbol)
            klines = self.client.get_historical_klines(
                symbol, Client.KLINE_INTERVAL_1HOUR, "24 hours ago UTC"
            )
            
            response_time = datetime.now().timestamp() - start_time
            
            # Report to API rotation manager
            service_name = 'binance' if self.testnet_mode else 'binance_live'
            report_api_result(service_name, success=True, response_time=response_time)
            
            return {
                'symbol': symbol,
                'price': float(ticker['price']),
                'volume': float(stats['volume']),
                'change_24h': float(stats['priceChangePercent']),
                'high_24h': float(stats['highPrice']),
                'low_24h': float(stats['lowPrice']),
                'klines': klines,
                'timestamp': datetime.now().isoformat(),
                'source': 'REAL_BINANCE_API'
            }
            
        except Exception as e:
            service_name = 'binance' if self.testnet_mode else 'binance_live'
            report_api_result(service_name, success=False, error_code=str(e))
            logging.error(f"Failed to get real market data: {e}")
            return None
    
    async def execute_real_trade(self, signal: Dict):
        """Execute real trade with actual Binance orders"""
        try:
            symbol = signal['symbol']
            side = signal['type']
            confidence = signal.get('confidence', 70)
            
            logging.info(f"[REAL_TRADE] Executing REAL {side} {symbol} (conf: {confidence:.1f}%)")
            
            # Sync real account state first
            self._sync_real_account_state()
            
            # Check real USDT balance
            if self.real_usdt_balance < 10:
                logging.warning("Insufficient real USDT balance for trade")
                return None
            
            # Get real current price
            market_data = self.get_live_market_data(symbol)
            if not market_data:
                logging.error("No real market data available")
                return None
            
            current_price = market_data['price']
            
            # Calculate real position size
            risk_amount = min(
                self.real_usdt_balance * (self.max_risk_percent / 100), 
                self.trade_amount_usdt
            )
            
            if side == 'BUY':
                quantity = round(risk_amount / current_price, 6)
            else:
                # For sell, check if we have the asset
                base_asset = symbol.replace('USDT', '')
                if base_asset not in self.real_positions:
                    logging.warning(f"No {base_asset} to sell")
                    return None
                quantity = min(self.real_positions[base_asset]['free'], 
                              round(risk_amount / current_price, 6))
            
            if quantity <= 0:
                logging.warning("Invalid quantity for real trade")
                return None
            
            # Execute real Binance order
            try:
                if side == 'BUY':
                    order = self.client.order_market_buy(
                        symbol=symbol,
                        quantity=quantity
                    )
                else:
                    order = self.client.order_market_sell(
                        symbol=symbol,
                        quantity=quantity
                    )
                
                # Extract real execution details
                execution_price = float(order['fills'][0]['price']) if order['fills'] else current_price
                execution_qty = float(order['executedQty'])
                order_id = order['orderId']
                
                # Store real position
                self.positions[symbol] = {
                    'side': side,
                    'quantity': execution_qty,
                    'entry_price': execution_price,
                    'entry_time': datetime.now(),
                    'current_price': execution_price,
                    'unrealized_pnl': 0,
                    'order_id': order_id,
                    'confidence': confidence,
                    'method': 'REAL_BINANCE_ORDER'
                }
                
                # Update real metrics
                self.total_trades += 1
                self.daily_trades += 1
                self.last_trade_time = datetime.now()
                
                # Save to real trade history
                self.save_real_trade_to_history({
                    'symbol': symbol,
                    'side': side,
                    'quantity': execution_qty,
                    'price': execution_price,
                    'confidence': confidence,
                    'timestamp': datetime.now().isoformat(),
                    'order_id': order_id,
                    'method': 'REAL_BINANCE_ORDER'
                })
                
                logging.info(f"[REAL_TRADE] EXECUTED: {side} {execution_qty:.6f} {symbol} @ ${execution_price:.2f} (Order: {order_id})")
                
                return {
                    'trade_id': self.total_trades,
                    'symbol': symbol,
                    'side': side,
                    'quantity': execution_qty,
                    'price': execution_price,
                    'order_id': order_id,
                    'timestamp': datetime.now().isoformat(),
                    'method': 'REAL_BINANCE_ORDER'
                }
                
            except BinanceAPIException as e:
                logging.error(f"Real Binance order failed: {e}")
                return None
                
        except Exception as e:
            logging.error(f"Real trade execution failed: {e}")
            return None
    
    async def monitor_real_positions(self):
        """Monitor real positions using actual Binance account data"""
        try:
            if not self.positions:
                return
            
            # Sync real account to get current balances/positions
            self._sync_real_account_state()
            
            positions_to_close = []
            
            for symbol, position in self.positions.items():
                try:
                    # Get real current price
                    market_data = self.get_live_market_data(symbol)
                    if not market_data:
                        continue
                    
                    current_price = market_data['price']
                    entry_price = position['entry_price']
                    side = position['side']
                    quantity = position['quantity']
                    
                    # Calculate real unrealized P&L
                    if side == 'BUY':
                        unrealized_pnl = (current_price - entry_price) * quantity
                    else:
                        unrealized_pnl = (entry_price - current_price) * quantity
                    
                    position['current_price'] = current_price
                    position['unrealized_pnl'] = unrealized_pnl
                    
                    # Real exit conditions
                    should_close = False
                    close_reason = ""
                    
                    # Stop loss (2% loss)
                    if unrealized_pnl < -0.02 * (entry_price * quantity):
                        should_close = True
                        close_reason = "Stop Loss"
                    
                    # Take profit (3% profit)
                    elif unrealized_pnl > 0.03 * (entry_price * quantity):
                        should_close = True
                        close_reason = "Take Profit"
                    
                    # Time-based exit (4 hours max hold)
                    elif (datetime.now() - position['entry_time']).total_seconds() > 14400:
                        should_close = True
                        close_reason = "Time Limit"
                    
                    if should_close:
                        positions_to_close.append((symbol, close_reason))
                        
                except Exception as e:
                    logging.error(f"Error monitoring position {symbol}: {e}")
            
            # Close positions that meet exit criteria
            for symbol, reason in positions_to_close:
                await self.close_real_position(symbol, reason)
                
        except Exception as e:
            logging.error(f"Real position monitoring failed: {e}")
    
    async def close_real_position(self, symbol: str, reason: str):
        """Close real position with actual Binance order"""
        try:
            if symbol not in self.positions:
                return
            
            position = self.positions[symbol]
            side = position['side']
            quantity = position['quantity']
            
            # Place real closing order
            try:
                if side == 'BUY':  # Close long position by selling
                    close_order = self.client.order_market_sell(
                        symbol=symbol,
                        quantity=quantity
                    )
                else:  # Close short position by buying
                    close_order = self.client.order_market_buy(
                        symbol=symbol,
                        quantity=quantity
                    )
                
                # Get real execution details
                exit_price = float(close_order['fills'][0]['price']) if close_order['fills'] else position['current_price']
                exit_qty = float(close_order['executedQty'])
                
                # Calculate real P&L
                if side == 'BUY':
                    realized_pnl = (exit_price - position['entry_price']) * exit_qty
                else:
                    realized_pnl = (position['entry_price'] - exit_price) * exit_qty
                
                # Update real metrics
                if realized_pnl > 0:
                    self.winning_trades += 1
                self.total_pnl += realized_pnl
                
                # Save real trade result
                self.save_real_trade_to_history({
                    'symbol': symbol,
                    'side': f"CLOSE_{side}",
                    'quantity': exit_qty,
                    'entry_price': position['entry_price'],
                    'exit_price': exit_price,
                    'realized_pnl': realized_pnl,
                    'hold_duration_minutes': (datetime.now() - position['entry_time']).total_seconds() / 60,
                    'close_reason': reason,
                    'timestamp': datetime.now().isoformat(),
                    'order_id': close_order['orderId'],
                    'method': 'REAL_BINANCE_CLOSE'
                })
                
                # Remove from tracking
                del self.positions[symbol]
                
                logging.info(f"[REAL_CLOSE] {symbol} closed: {reason} | P&L: ${realized_pnl:+.2f} | Order: {close_order['orderId']}")
                
                return {
                    'symbol': symbol,
                    'realized_pnl': realized_pnl,
                    'exit_price': exit_price,
                    'close_reason': reason,
                    'order_id': close_order['orderId']
                }
                
            except BinanceAPIException as e:
                logging.error(f"Real position close failed for {symbol}: {e}")
                return None
                
        except Exception as e:
            logging.error(f"Error closing real position {symbol}: {e}")
            return None
    
    def get_real_account_summary(self):
        """Get real account summary from Binance"""
        try:
            if not self.client:
                return None
            
            # Sync real account data
            self._sync_real_account_state()
            
            # Get real trade history for today
            today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            today_timestamp = int(today_start.timestamp() * 1000)
            
            real_trades_today = []
            try:
                trades = self.client.get_my_trades(symbol="BTCUSDT", startTime=today_timestamp)
                real_trades_today.extend(trades)
                
                # Check other major pairs
                for symbol in ['ETHUSDT', 'ADAUSDT', 'DOTUSDT']:
                    try:
                        trades = self.client.get_my_trades(symbol=symbol, startTime=today_timestamp)
                        real_trades_today.extend(trades)
                    except:
                        pass
                        
            except Exception as e:
                logging.warning(f"Could not fetch real trade history: {e}")
            
            return {
                'real_usdt_balance': self.real_usdt_balance,
                'real_positions_count': len(self.real_positions),
                'real_positions': self.real_positions,
                'real_trades_today': len(real_trades_today),
                'tracked_positions': len(self.positions),
                'total_trades': self.total_trades,
                'total_pnl': self.total_pnl,
                'winning_trades': self.winning_trades,
                'win_rate': (self.winning_trades / max(1, self.total_trades)) * 100,
                'last_sync': datetime.now().isoformat(),
                'method': 'REAL_BINANCE_ACCOUNT'
            }
            
        except Exception as e:
            logging.error(f"Real account summary failed: {e}")
            return None
    
    def calculate_real_position_size(self, confidence: float, symbol: str):
        """Calculate real position size based on actual account balance"""
        try:
            if not self.client:
                return 0
            
            # Sync real balance
            self._sync_real_account_state()
            
            # Base amount from configuration
            base_amount = self.trade_amount_usdt
            
            # Adjust based on confidence
            confidence_multiplier = confidence / 100
            adjusted_amount = base_amount * confidence_multiplier
            
            # Risk management with real balance
            max_risk_amount = self.real_usdt_balance * (self.max_risk_percent / 100)
            final_amount = min(adjusted_amount, max_risk_amount)
            
            # Get real current price
            ticker = self.client.get_symbol_ticker(symbol=symbol)
            current_price = float(ticker['price'])
            
            # Calculate real quantity
            quantity = final_amount / current_price
            
            # Round to valid precision for symbol
            try:
                symbol_info = self.client.get_symbol_info(symbol)
                step_size = float(symbol_info['filters'][2]['stepSize'])
                precision = len(str(step_size).rstrip('0').split('.')[1]) if '.' in str(step_size) else 0
                quantity = round(quantity, precision)
            except:
                quantity = round(quantity, 6)  # Default precision
            
            return max(0, quantity)
            
        except Exception as e:
            logging.error(f"Real position size calculation failed: {e}")
            return 0
    
    def save_real_trade_to_history(self, trade_data):
        """Save real trade to persistent history"""
        try:
            trade_record = {
                'trade_id': self.total_trades,
                'symbol': trade_data.get('symbol'),
                'side': trade_data.get('side'),
                'quantity': trade_data.get('quantity', 0),
                'entry_price': trade_data.get('price', trade_data.get('entry_price', 0)),
                'exit_price': trade_data.get('exit_price'),
                'realized_pnl': trade_data.get('realized_pnl', 0),
                'confidence': trade_data.get('confidence', 0),
                'timestamp': trade_data.get('timestamp', datetime.now().isoformat()),
                'order_id': trade_data.get('order_id'),
                'close_reason': trade_data.get('close_reason'),
                'method': 'REAL_BINANCE_TRADING',
                'hold_duration_minutes': trade_data.get('hold_duration_minutes', 0),
                'session_id': datetime.now().strftime('%Y%m%d')
            }
            
            # Save to persistence system
            self.pnl_persistence.save_trade(trade_record)
            
            # Update and save current metrics
            self.save_real_metrics_to_db()
            
            logging.info(f"[REAL_PERSISTENCE] Trade saved: ${trade_data.get('realized_pnl', 0):.2f} | Total P&L: ${self.total_pnl:.2f}")
            
        except Exception as e:
            logging.error(f"Failed to save real trade: {e}")

    def save_real_metrics_to_db(self):
        """Save real trading metrics to database"""
        try:
            current_metrics = {
                'total_trades': self.total_trades,
                'winning_trades': self.winning_trades,
                'total_pnl': self.total_pnl,
                'win_rate': (self.winning_trades / max(1, self.total_trades)) * 100,
                'active_positions': len(self.positions),
                'daily_trades': self.daily_trades,
                'last_updated': datetime.now().isoformat(),
                'trading_method': 'REAL_BINANCE_TRADING',
                'real_usdt_balance': getattr(self, 'real_usdt_balance', 0),
                'real_positions_count': len(getattr(self, 'real_positions', {}))
            }
            
            self.pnl_persistence.save_metrics(current_metrics)
            logging.info(f"[REAL_PERSISTENCE] Metrics saved: {self.total_trades} trades, ${self.total_pnl:.2f} P&L")
            
        except Exception as e:
            logging.error(f"Failed to save real metrics: {e}")
    
    def get_real_metrics(self) -> Dict:
        """Get real trading performance metrics"""
        win_rate = (self.winning_trades / max(1, self.total_trades)) * 100
        
        # Get real account data
        real_account = self.get_real_account_summary() or {}
        
        return {
            'total_trades': self.total_trades,
            'winning_trades': self.winning_trades,
            'losing_trades': self.total_trades - self.winning_trades,
            'total_pnl': self.total_pnl,
            'win_rate': win_rate,
            'daily_trades': self.daily_trades,
            'open_positions': len(self.positions),
            'is_trading': self.is_trading,
            'testnet_mode': self.testnet_mode,
            'connection': 'REAL_BINANCE_TESTNET' if self.testnet_mode else 'REAL_BINANCE_LIVE',
            'avg_trade': self.total_pnl / max(1, self.total_trades),
            'real_usdt_balance': real_account.get('real_usdt_balance', 0),
            'real_positions_count': real_account.get('real_positions_count', 0),
            'method': 'REAL_BINANCE_TRADING',
            'last_trade_time': self.last_trade_time.isoformat() if self.last_trade_time else None
        }
    
    def get_real_status(self) -> Dict:
        """Get real trading engine status"""
        real_account = self.get_real_account_summary() or {}
        
        return {
            'is_trading': self.is_trading,
            'testnet_mode': self.testnet_mode,
            'positions_count': len(self.positions),
            'connection': 'REAL_BINANCE_CONNECTED' if self.client else 'DISCONNECTED',
            'method': 'REAL_BINANCE_TRADING',
            'last_trade_time': self.last_trade_time.isoformat() if self.last_trade_time else None,
            'real_account_data': real_account,
            'metrics': self.get_real_metrics()
        }

    # Remove all simulation methods - implement only real trading
    async def execute_v3_trade(self, signal: Dict, use_multi_pair: bool = True):
        """Execute real V3 trade - wrapper for backward compatibility"""
        return await self.execute_real_trade(signal)