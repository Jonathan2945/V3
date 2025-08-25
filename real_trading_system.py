#!/usr/bin/env python3
"""
UPGRADED REAL TRADING SYSTEM - PRODUCTION READY
=============================================
CRITICAL WARNING: This system handles real money. Test extensively in paper mode first.

Upgrades:
- Enhanced error handling and recovery
- Improved configuration management
- Better position management
- Comprehensive logging
- Circuit breaker patterns
- Data validation
- Performance optimizations
"""

import sqlite3
import json
import logging
import os
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from enum import Enum
import uuid
import time
from dataclasses import dataclass, asdict
import threading
from contextlib import asynccontextmanager
import traceback

# Enhanced configuration
@dataclass
class TradingSystemConfig:
    """Centralized configuration management"""
    # Trading modes
    trading_mode: str = "PAPER_TRADING"
    testnet_enabled: bool = True
    live_trading_enabled: bool = False
    
    # Risk management
    max_positions: int = 3
    max_daily_loss: float = 500.0
    max_position_size: float = 100.0
    position_size_percent: float = 2.0
    
    # Safety settings
    circuit_breaker_loss_threshold: float = 200.0
    circuit_breaker_trades_threshold: int = 10
    max_consecutive_losses: int = 5
    
    # System settings
    position_check_interval: int = 30
    heartbeat_interval: int = 60
    data_retention_days: int = 90
    
    # Database settings
    db_path: str = "data/trading_system.db"
    backup_enabled: bool = True
    
    @classmethod
    def from_env(cls) -> 'TradingSystemConfig':
        return cls(
            max_positions=int(os.getenv('MAX_POSITIONS', 3)),
            max_daily_loss=float(os.getenv('MAX_DAILY_LOSS', 500.0)),
            max_position_size=float(os.getenv('MAX_POSITION_SIZE', 100.0)),
            testnet_enabled=os.getenv('TESTNET', 'true').lower() == 'true',
            live_trading_enabled=os.getenv('LIVE_TRADING_ENABLED', 'false').lower() == 'true',
            db_path=os.getenv('DB_PATH', 'data/trading_system.db'),
        )

class TradingMode(Enum):
    BACKTESTING = "backtesting"
    PAPER_TRADING = "paper_trading"
    LIVE_TRADING = "live_trading"

class TradeStatus(Enum):
    PENDING = "PENDING"
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"

class SystemState(Enum):
    INITIALIZING = "initializing"
    OPERATIONAL = "operational"
    CIRCUIT_BREAKER = "circuit_breaker"
    MAINTENANCE = "maintenance"
    SHUTDOWN = "shutdown"

# Enhanced errors
class TradingSystemError(Exception):
    """Base trading system error"""
    pass

class CircuitBreakerError(TradingSystemError):
    """Circuit breaker activated"""
    pass

class RiskLimitError(TradingSystemError):
    """Risk limit exceeded"""
    pass

class DataValidationError(TradingSystemError):
    """Data validation failed"""
    pass

# Circuit breaker implementation
class CircuitBreaker:
    """Circuit breaker to prevent runaway losses"""
    
    def __init__(self, config: TradingSystemConfig):
        self.config = config
        self.daily_loss = 0.0
        self.consecutive_losses = 0
        self.trade_count = 0
        self.is_open = False
        self.last_reset = datetime.now()
        
    def check_circuit_breaker(self) -> bool:
        """Check if circuit breaker should be triggered"""
        # Daily reset
        if (datetime.now() - self.last_reset).days >= 1:
            self.reset_daily_counters()
        
        # Loss threshold
        if abs(self.daily_loss) >= self.config.circuit_breaker_loss_threshold:
            self.trigger("Daily loss threshold exceeded")
            return False
        
        # Consecutive losses
        if self.consecutive_losses >= self.config.max_consecutive_losses:
            self.trigger("Too many consecutive losses")
            return False
        
        # Trade count (prevent over-trading)
        if self.trade_count >= self.config.circuit_breaker_trades_threshold:
            self.trigger("Daily trade limit exceeded")
            return False
        
        return not self.is_open
    
    def trigger(self, reason: str):
        """Trigger circuit breaker"""
        self.is_open = True
        logging.critical(f"CIRCUIT BREAKER TRIGGERED: {reason}")
        logging.critical(f"Daily Loss: ${self.daily_loss:.2f}")
        logging.critical(f"Consecutive Losses: {self.consecutive_losses}")
        logging.critical(f"Trade Count: {self.trade_count}")
    
    def reset_daily_counters(self):
        """Reset daily counters"""
        self.daily_loss = 0.0
        self.trade_count = 0
        self.last_reset = datetime.now()
        logging.info("Circuit breaker daily counters reset")
    
    def update_trade_result(self, pnl: float):
        """Update circuit breaker with trade result"""
        self.daily_loss += pnl
        self.trade_count += 1
        
        if pnl < 0:
            self.consecutive_losses += 1
        else:
            self.consecutive_losses = 0

# Enhanced position management
@dataclass
class Position:
    """Enhanced position data structure"""
    trade_id: str
    symbol: str
    side: str
    quantity: float
    entry_price: float
    entry_time: datetime
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    status: str = "OPEN"
    current_price: float = 0.0
    unrealized_pnl: float = 0.0
    max_profit: float = 0.0
    max_loss: float = 0.0
    last_update: datetime = None
    
    def __post_init__(self):
        if self.last_update is None:
            self.last_update = datetime.now()
    
    def update_current_price(self, price: float):
        """Update position with current market price"""
        self.current_price = price
        self.last_update = datetime.now()
        
        # Calculate unrealized P&L
        if self.side == 'BUY':
            self.unrealized_pnl = (price - self.entry_price) * self.quantity
        else:
            self.unrealized_pnl = (self.entry_price - price) * self.quantity
        
        # Track max profit/loss
        self.max_profit = max(self.max_profit, self.unrealized_pnl)
        if self.unrealized_pnl < 0:
            self.max_loss = min(self.max_loss, self.unrealized_pnl)
    
    def should_close(self) -> Tuple[bool, str]:
        """Check if position should be closed"""
        if self.status != "OPEN":
            return False, ""
        
        # Stop loss check
        if self.stop_loss:
            if self.side == 'BUY' and self.current_price <= self.stop_loss:
                return True, "Stop Loss Hit"
            elif self.side == 'SELL' and self.current_price >= self.stop_loss:
                return True, "Stop Loss Hit"
        
        # Take profit check
        if self.take_profit:
            if self.side == 'BUY' and self.current_price >= self.take_profit:
                return True, "Take Profit Hit"
            elif self.side == 'SELL' and self.current_price <= self.take_profit:
                return True, "Take Profit Hit"
        
        # Time-based exit (24 hours max)
        if (datetime.now() - self.entry_time).total_seconds() > 86400:
            return True, "Max Hold Time Reached"
        
        return False, ""

class EnhancedTradingSystem:
    """Production-ready trading system with comprehensive safety features"""
    
    def __init__(self, trading_engine=None, trade_logger=None, data_manager=None):
        self.config = TradingSystemConfig.from_env()
        self.trading_engine = trading_engine
        self.trade_logger = trade_logger
        self.data_manager = data_manager
        
        # System state
        self.current_mode = TradingMode.PAPER_TRADING
        self.system_state = SystemState.INITIALIZING
        self.circuit_breaker = CircuitBreaker(self.config)
        
        # Position management
        self.positions: Dict[str, Position] = {}
        self.trade_history: List[Dict] = []
        
        # Performance tracking
        self.session_metrics = {
            'start_time': datetime.now(),
            'total_trades': 0,
            'winning_trades': 0,
            'total_pnl': 0.0,
            'max_drawdown': 0.0,
            'peak_balance': 0.0
        }
        
        # Background tasks
        self.monitoring_task = None
        self.heartbeat_task = None
        self.shutdown_event = asyncio.Event()
        
        # Initialize logging
        self._setup_logging()
        
        # Initialize database
        self._initialize_database()
        
        logging.info("Enhanced Trading System initialized")
        logging.info(f"Configuration: {asdict(self.config)}")
    
    def _setup_logging(self):
        """Setup comprehensive logging"""
        os.makedirs('logs', exist_ok=True)
        
        # File handler with rotation
        from logging.handlers import RotatingFileHandler
        file_handler = RotatingFileHandler(
            'logs/trading_system.log',
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        file_handler.setLevel(logging.INFO)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # Configure logger
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    
    def _initialize_database(self):
        """Initialize enhanced database schema"""
        try:
            os.makedirs(os.path.dirname(self.config.db_path), exist_ok=True)
            
            with sqlite3.connect(self.config.db_path) as conn:
                # Enhanced positions table
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS positions (
                        trade_id TEXT PRIMARY KEY,
                        symbol TEXT NOT NULL,
                        side TEXT NOT NULL,
                        quantity REAL NOT NULL,
                        entry_price REAL NOT NULL,
                        entry_time DATETIME NOT NULL,
                        stop_loss REAL,
                        take_profit REAL,
                        status TEXT DEFAULT 'OPEN',
                        current_price REAL DEFAULT 0,
                        unrealized_pnl REAL DEFAULT 0,
                        max_profit REAL DEFAULT 0,
                        max_loss REAL DEFAULT 0,
                        last_update DATETIME,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Enhanced trades table
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS trades (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        trade_id TEXT UNIQUE NOT NULL,
                        symbol TEXT NOT NULL,
                        side TEXT NOT NULL,
                        quantity REAL NOT NULL,
                        entry_price REAL NOT NULL,
                        exit_price REAL,
                        entry_time DATETIME NOT NULL,
                        exit_time DATETIME,
                        pnl REAL DEFAULT 0,
                        pnl_percent REAL DEFAULT 0,
                        exit_reason TEXT,
                        strategy TEXT,
                        trading_mode TEXT,
                        status TEXT DEFAULT 'OPEN',
                        metadata TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # System metrics table
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS system_metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                        total_trades INTEGER DEFAULT 0,
                        winning_trades INTEGER DEFAULT 0,
                        total_pnl REAL DEFAULT 0,
                        max_drawdown REAL DEFAULT 0,
                        active_positions INTEGER DEFAULT 0,
                        system_state TEXT,
                        trading_mode TEXT,
                        circuit_breaker_status BOOLEAN DEFAULT 0,
                        metadata TEXT
                    )
                ''')
                
                # Create indexes
                conn.execute('CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol)')
                conn.execute('CREATE INDEX IF NOT EXISTS idx_trades_time ON trades(entry_time)')
                conn.execute('CREATE INDEX IF NOT EXISTS idx_positions_symbol ON positions(symbol)')
                
                conn.commit()
                
            logging.info("Database initialized successfully")
            
        except Exception as e:
            logging.error(f"Database initialization failed: {e}")
            raise TradingSystemError(f"Database initialization failed: {e}")
    
    def set_trading_mode(self, mode: TradingMode):
        """Set trading mode with comprehensive validation"""
        try:
            # Validation checks
            if mode == TradingMode.LIVE_TRADING:
                if not self.config.live_trading_enabled:
                    raise TradingSystemError("Live trading not enabled in configuration")
                
                if not self.trading_engine:
                    raise TradingSystemError("Trading engine not available")
                
                if not hasattr(self.trading_engine, 'client') or not self.trading_engine.client:
                    raise TradingSystemError("Trading engine not connected")
                
                # Additional live trading checks
                if self.config.testnet_enabled:
                    raise TradingSystemError("Cannot enable live trading while testnet is enabled")
                
                logging.critical("=" * 60)
                logging.critical("SWITCHING TO LIVE TRADING MODE")
                logging.critical("REAL MONEY WILL BE AT RISK")
                logging.critical("=" * 60)
            
            old_mode = self.current_mode
            self.current_mode = mode
            
            # Log mode change
            logging.warning(f"Trading mode changed: {old_mode.value} -> {mode.value}")
            
            # Update database
            self._log_system_event(f"Trading mode changed to {mode.value}")
            
        except Exception as e:
            logging.error(f"Failed to set trading mode: {e}")
            raise TradingSystemError(f"Failed to set trading mode: {e}")
    
    async def start_system(self):
        """Start the enhanced trading system"""
        try:
            logging.info("Starting Enhanced Trading System")
            
            # System validation
            await self._validate_system_components()
            
            # Load existing positions
            await self._load_existing_positions()
            
            # Start background tasks
            await self._start_background_tasks()
            
            # Update system state
            self.system_state = SystemState.OPERATIONAL
            
            logging.info("Enhanced Trading System started successfully")
            
        except Exception as e:
            logging.error(f"System startup failed: {e}")
            self.system_state = SystemState.SHUTDOWN
            raise TradingSystemError(f"System startup failed: {e}")
    
    async def stop_system(self):
        """Stop the enhanced trading system safely"""
        try:
            logging.info("Stopping Enhanced Trading System")
            
            # Set shutdown flag
            self.shutdown_event.set()
            self.system_state = SystemState.SHUTDOWN
            
            # Close all open positions (paper trading only)
            if self.current_mode == TradingMode.PAPER_TRADING:
                await self._close_all_positions("System Shutdown")
            
            # Stop background tasks
            if self.monitoring_task:
                self.monitoring_task.cancel()
            if self.heartbeat_task:
                self.heartbeat_task.cancel()
            
            # Save final metrics
            await self._save_system_metrics()
            
            logging.info("Enhanced Trading System stopped")
            
        except Exception as e:
            logging.error(f"System shutdown error: {e}")
    
    async def execute_trade_signal(self, signal: Dict) -> Optional[str]:
        """Execute trade signal with comprehensive validation and safety checks"""
        try:
            # System state check
            if self.system_state != SystemState.OPERATIONAL:
                logging.warning(f"Cannot execute trade - system state: {self.system_state}")
                return None
            
            # Circuit breaker check
            if not self.circuit_breaker.check_circuit_breaker():
                raise CircuitBreakerError("Circuit breaker is open")
            
            # Signal validation
            if not self._validate_signal(signal):
                raise DataValidationError("Invalid signal data")
            
            # Risk management checks
            if not await self._check_risk_limits(signal):
                raise RiskLimitError("Risk limits exceeded")
            
            # Execute based on mode
            if self.current_mode == TradingMode.BACKTESTING:
                return await self._execute_backtest_trade(signal)
            elif self.current_mode == TradingMode.PAPER_TRADING:
                return await self._execute_paper_trade(signal)
            elif self.current_mode == TradingMode.LIVE_TRADING:
                return await self._execute_live_trade(signal)
            
        except CircuitBreakerError:
            self.system_state = SystemState.CIRCUIT_BREAKER
            logging.critical("System moved to circuit breaker state")
            return None
        except Exception as e:
            logging.error(f"Trade execution failed: {e}")
            logging.error(f"Signal: {signal}")
            logging.error(f"Traceback: {traceback.format_exc()}")
            return None
    
    def _validate_signal(self, signal: Dict) -> bool:
        """Comprehensive signal validation"""
        try:
            required_fields = ['symbol', 'side', 'confidence']
            
            # Check required fields
            if not all(field in signal for field in required_fields):
                logging.error(f"Missing required fields in signal: {signal}")
                return False
            
            # Validate side
            if signal['side'] not in ['BUY', 'SELL']:
                logging.error(f"Invalid side: {signal['side']}")
                return False
            
            # Validate confidence
            try:
                confidence = float(signal['confidence'])
                if not 0 <= confidence <= 100:
                    logging.error(f"Invalid confidence: {confidence}")
                    return False
            except (ValueError, TypeError):
                logging.error(f"Invalid confidence type: {signal['confidence']}")
                return False
            
            # Validate symbol format
            symbol = signal['symbol']
            if not isinstance(symbol, str) or len(symbol) < 6:
                logging.error(f"Invalid symbol: {symbol}")
                return False
            
            return True
            
        except Exception as e:
            logging.error(f"Signal validation error: {e}")
            return False
    
    async def _check_risk_limits(self, signal: Dict) -> bool:
        """Comprehensive risk limit checks"""
        try:
            # Position count limit
            if len(self.positions) >= self.config.max_positions:
                logging.warning(f"Position limit exceeded: {len(self.positions)}/{self.config.max_positions}")
                return False
            
            # Daily loss limit
            if abs(self.circuit_breaker.daily_loss) >= self.config.max_daily_loss:
                logging.warning(f"Daily loss limit exceeded: ${self.circuit_breaker.daily_loss:.2f}")
                return False
            
            # Position size validation
            if self.trading_engine:
                try:
                    position_size = self.trading_engine.calculate_position_size(signal['confidence'])
                    if position_size > self.config.max_position_size:
                        logging.warning(f"Position size too large: ${position_size:.2f}")
                        return False
                except Exception as e:
                    logging.error(f"Position size calculation failed: {e}")
                    return False
            
            # Symbol-specific checks
            symbol = signal['symbol']
            existing_positions = [p for p in self.positions.values() if p.symbol == symbol]
            if len(existing_positions) > 0:
                logging.warning(f"Already have position in {symbol}")
                return False
            
            return True
            
        except Exception as e:
            logging.error(f"Risk limit check failed: {e}")
            return False
    
    async def _execute_paper_trade(self, signal: Dict) -> Optional[str]:
        """Execute paper trade with real market data"""
        try:
            symbol = signal['symbol']
            side = signal['side']
            confidence = float(signal['confidence'])
            
            # Get real market price
            market_data = None
            if self.trading_engine and hasattr(self.trading_engine, 'get_real_market_data'):
                market_data = self.trading_engine.get_real_market_data(symbol)
            
            if not market_data:
                logging.error(f"No market data available for {symbol}")
                return None
            
            current_price = float(market_data['price'])
            
            # Calculate position parameters
            if self.trading_engine and hasattr(self.trading_engine, 'calculate_position_size'):
                position_size = self.trading_engine.calculate_position_size(confidence)
            else:
                position_size = self.config.max_position_size * (confidence / 100) * 0.5
            
            quantity = position_size / current_price
            
            # Create trade ID
            trade_id = f"paper_{uuid.uuid4().hex[:8]}"
            
            # Calculate stop loss and take profit
            stop_loss = signal.get('stop_loss')
            take_profit = signal.get('take_profit')
            
            if not stop_loss:
                stop_loss = current_price * (0.98 if side == 'BUY' else 1.02)
            if not take_profit:
                take_profit = current_price * (1.04 if side == 'BUY' else 0.96)
            
            # Create position
            position = Position(
                trade_id=trade_id,
                symbol=symbol,
                side=side,
                quantity=quantity,
                entry_price=current_price,
                entry_time=datetime.now(),
                stop_loss=stop_loss,
                take_profit=take_profit,
                current_price=current_price
            )
            
            # Store position
            self.positions[trade_id] = position
            await self._save_position_to_db(position)
            
            # Log trade
            if self.trade_logger and hasattr(self.trade_logger, 'log_trade_entry'):
                trade_data = {
                    'trade_id': trade_id,
                    'symbol': symbol,
                    'side': side,
                    'quantity': quantity,
                    'entry_price': current_price,
                    'entry_time': datetime.now().isoformat(),
                    'stop_loss': stop_loss,
                    'take_profit': take_profit,
                    'trading_mode': self.current_mode.value,
                    'confidence': confidence
                }
                self.trade_logger.log_trade_entry(trade_data)
            
            # Update metrics
            self.session_metrics['total_trades'] += 1
            
            logging.info(f"Paper trade executed: {trade_id} - {side} {symbol} @ ${current_price:.2f}")
            return trade_id
            
        except Exception as e:
            logging.error(f"Paper trade execution failed: {e}")
            return None
    
    async def _execute_live_trade(self, signal: Dict) -> Optional[str]:
        """Execute live trade with real money - EXTREME CAUTION"""
        try:
            if self.current_mode != TradingMode.LIVE_TRADING:
                raise TradingSystemError("Not in live trading mode")
            
            symbol = signal['symbol']
            side = signal['side']
            confidence = float(signal['confidence'])
            
            # Final safety confirmation
            if not self.config.live_trading_enabled:
                raise TradingSystemError("Live trading not enabled")
            
            # Get market data
            if not self.trading_engine or not hasattr(self.trading_engine, 'get_real_market_data'):
                raise TradingSystemError("Trading engine not available")
            
            market_data = self.trading_engine.get_real_market_data(symbol)
            if not market_data:
                raise TradingSystemError(f"No market data for {symbol}")
            
            current_price = float(market_data['price'])
            
            # Calculate position size
            position_size = self.trading_engine.calculate_position_size(confidence)
            quantity = position_size / current_price
            
            # Validate sufficient balance
            if hasattr(self.trading_engine, 'available_balance'):
                if position_size > self.trading_engine.available_balance:
                    raise TradingSystemError("Insufficient balance")
            
            trade_id = f"live_{uuid.uuid4().hex[:8]}"
            
            logging.critical(f"EXECUTING LIVE TRADE: {side} {symbol} @ ${current_price:.2f} - REAL MONEY")
            
            # Execute real order through exchange
            if self.trading_engine.client:
                try:
                    order = self.trading_engine.client.create_order(
                        symbol=symbol,
                        side=side,
                        type='MARKET',
                        quantity=round(quantity, 6)
                    )
                    
                    # Extract execution details
                    executed_price = current_price
                    executed_quantity = quantity
                    
                    fills = order.get('fills', [])
                    if fills:
                        executed_price = float(fills[0].get('price', current_price))
                        executed_quantity = float(order.get('executedQty', quantity))
                    
                    logging.critical(f"LIVE ORDER EXECUTED: {order.get('orderId')} @ ${executed_price:.2f}")
                    
                    # Create position
                    position = Position(
                        trade_id=trade_id,
                        symbol=symbol,
                        side=side,
                        quantity=executed_quantity,
                        entry_price=executed_price,
                        entry_time=datetime.now(),
                        stop_loss=signal.get('stop_loss', executed_price * (0.98 if side == 'BUY' else 1.02)),
                        take_profit=signal.get('take_profit', executed_price * (1.04 if side == 'BUY' else 0.96)),
                        current_price=executed_price
                    )
                    
                    # Store position
                    self.positions[trade_id] = position
                    await self._save_position_to_db(position)
                    
                    # Update session metrics
                    self.session_metrics['total_trades'] += 1
                    
                    logging.critical(f"LIVE POSITION OPENED: {trade_id}")
                    return trade_id
                    
                except Exception as e:
                    logging.critical(f"LIVE ORDER EXECUTION FAILED: {e}")
                    raise TradingSystemError(f"Live order execution failed: {e}")
            else:
                raise TradingSystemError("No trading client available")
                
        except Exception as e:
            logging.critical(f"Live trade execution failed: {e}")
            return None
    
    async def monitor_positions(self):
        """Monitor all active positions"""
        try:
            positions_to_close = []
            
            for trade_id, position in self.positions.items():
                if position.status != "OPEN":
                    continue
                
                try:
                    # Get current market price
                    market_data = None
                    if self.trading_engine and hasattr(self.trading_engine, 'get_real_market_data'):
                        market_data = self.trading_engine.get_real_market_data(position.symbol)
                    
                    if market_data:
                        current_price = float(market_data['price'])
                        position.update_current_price(current_price)
                        
                        # Update in database
                        await self._update_position_in_db(position)
                        
                        # Check exit conditions
                        should_close, reason = position.should_close()
                        if should_close:
                            positions_to_close.append((trade_id, reason))
                
                except Exception as e:
                    logging.error(f"Error monitoring position {trade_id}: {e}")
            
            # Close positions that need to be closed
            for trade_id, reason in positions_to_close:
                await self._close_position(trade_id, reason)
                
        except Exception as e:
            logging.error(f"Position monitoring error: {e}")
    
    async def _close_position(self, trade_id: str, reason: str):
        """Close a position safely"""
        try:
            if trade_id not in self.positions:
                logging.warning(f"Position {trade_id} not found")
                return
            
            position = self.positions[trade_id]
            if position.status != "OPEN":
                logging.warning(f"Position {trade_id} already closed")
                return
            
            exit_price = position.current_price
            
            # Execute closing order for live trading
            if self.current_mode == TradingMode.LIVE_TRADING:
                if self.trading_engine and hasattr(self.trading_engine, 'client') and self.trading_engine.client:
                    try:
                        close_side = 'SELL' if position.side == 'BUY' else 'BUY'
                        close_order = self.trading_engine.client.create_order(
                            symbol=position.symbol,
                            side=close_side,
                            type='MARKET',
                            quantity=round(position.quantity, 6)
                        )
                        
                        fills = close_order.get('fills', [])
                        if fills:
                            exit_price = float(fills[0].get('price', position.current_price))
                        
                        logging.critical(f"LIVE POSITION CLOSED: {close_order.get('orderId')} @ ${exit_price:.2f}")
                        
                    except Exception as e:
                        logging.error(f"Failed to close live position: {e}")
                        exit_price = position.current_price
            
            # Calculate final P&L
            if position.side == 'BUY':
                pnl = (exit_price - position.entry_price) * position.quantity
            else:
                pnl = (position.entry_price - exit_price) * position.quantity
            
            pnl_percent = (pnl / (position.entry_price * position.quantity)) * 100
            
            # Update position
            position.status = "CLOSED"
            position.current_price = exit_price
            
            # Log trade exit
            if self.trade_logger and hasattr(self.trade_logger, 'log_trade_exit'):
                exit_data = {
                    'exit_time': datetime.now().isoformat(),
                    'exit_price': exit_price,
                    'exit_reason': reason,
                    'pnl': pnl,
                    'pnl_percent': pnl_percent
                }
                self.trade_logger.log_trade_exit(trade_id, exit_data)
            
            # Update circuit breaker
            self.circuit_breaker.update_trade_result(pnl)
            
            # Update session metrics
            self.session_metrics['total_pnl'] += pnl
            if pnl > 0:
                self.session_metrics['winning_trades'] += 1
            
            # Remove from active positions
            del self.positions[trade_id]
            
            # Update database
            await self._update_trade_in_db(trade_id, exit_price, pnl, pnl_percent, reason)
            
            logging.info(f"Position closed: {trade_id} - {reason} - P&L: ${pnl:.2f} ({pnl_percent:.2f}%)")
            
        except Exception as e:
            logging.error(f"Error closing position {trade_id}: {e}")
    
    async def _close_all_positions(self, reason: str):
        """Close all open positions"""
        try:
            trade_ids = list(self.positions.keys())
            for trade_id in trade_ids:
                await self._close_position(trade_id, reason)
            
            logging.info(f"All positions closed: {reason}")
            
        except Exception as e:
            logging.error(f"Error closing all positions: {e}")
    
    # Database operations
    async def _save_position_to_db(self, position: Position):
        """Save position to database"""
        try:
            with sqlite3.connect(self.config.db_path) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO positions 
                    (trade_id, symbol, side, quantity, entry_price, entry_time, 
                     stop_loss, take_profit, status, current_price, unrealized_pnl,
                     max_profit, max_loss, last_update)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    position.trade_id, position.symbol, position.side, position.quantity,
                    position.entry_price, position.entry_time, position.stop_loss,
                    position.take_profit, position.status, position.current_price,
                    position.unrealized_pnl, position.max_profit, position.max_loss,
                    position.last_update
                ))
                conn.commit()
        except Exception as e:
            logging.error(f"Failed to save position to database: {e}")
    
    async def _update_position_in_db(self, position: Position):
        """Update position in database"""
        try:
            with sqlite3.connect(self.config.db_path) as conn:
                conn.execute('''
                    UPDATE positions SET 
                    current_price = ?, unrealized_pnl = ?, max_profit = ?, 
                    max_loss = ?, last_update = ?
                    WHERE trade_id = ?
                ''', (
                    position.current_price, position.unrealized_pnl,
                    position.max_profit, position.max_loss, position.last_update,
                    position.trade_id
                ))
                conn.commit()
        except Exception as e:
            logging.error(f"Failed to update position in database: {e}")
    
    async def _update_trade_in_db(self, trade_id: str, exit_price: float, 
                                 pnl: float, pnl_percent: float, exit_reason: str):
        """Update completed trade in database"""
        try:
            with sqlite3.connect(self.config.db_path) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO trades 
                    (trade_id, symbol, side, quantity, entry_price, exit_price,
                     entry_time, exit_time, pnl, pnl_percent, exit_reason,
                     trading_mode, status, updated_at)
                    SELECT trade_id, symbol, side, quantity, entry_price, ?,
                           entry_time, ?, ?, ?, ?, ?, 'CLOSED', ?
                    FROM positions WHERE trade_id = ?
                ''', (
                    exit_price, datetime.now(), pnl, pnl_percent, exit_reason,
                    self.current_mode.value, datetime.now(), trade_id
                ))
                
                # Remove from positions table
                conn.execute('DELETE FROM positions WHERE trade_id = ?', (trade_id,))
                conn.commit()
        except Exception as e:
            logging.error(f"Failed to update trade in database: {e}")
    
    async def _load_existing_positions(self):
        """Load existing positions from database"""
        try:
            with sqlite3.connect(self.config.db_path) as conn:
                cursor = conn.execute('SELECT * FROM positions WHERE status = "OPEN"')
                
                for row in cursor.fetchall():
                    position = Position(
                        trade_id=row[0],
                        symbol=row[1],
                        side=row[2],
                        quantity=row[3],
                        entry_price=row[4],
                        entry_time=datetime.fromisoformat(row[5]),
                        stop_loss=row[6],
                        take_profit=row[7],
                        status=row[8],
                        current_price=row[9],
                        unrealized_pnl=row[10],
                        max_profit=row[11],
                        max_loss=row[12],
                        last_update=datetime.fromisoformat(row[13]) if row[13] else datetime.now()
                    )
                    self.positions[position.trade_id] = position
                
                logging.info(f"Loaded {len(self.positions)} existing positions")
                
        except Exception as e:
            logging.error(f"Failed to load existing positions: {e}")
    
    async def _save_system_metrics(self):
        """Save system metrics to database"""
        try:
            with sqlite3.connect(self.config.db_path) as conn:
                conn.execute('''
                    INSERT INTO system_metrics 
                    (total_trades, winning_trades, total_pnl, max_drawdown,
                     active_positions, system_state, trading_mode, circuit_breaker_status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    self.session_metrics['total_trades'],
                    self.session_metrics['winning_trades'],
                    self.session_metrics['total_pnl'],
                    self.session_metrics['max_drawdown'],
                    len(self.positions),
                    self.system_state.value,
                    self.current_mode.value,
                    self.circuit_breaker.is_open
                ))
                conn.commit()
        except Exception as e:
            logging.error(f"Failed to save system metrics: {e}")
    
    def _log_system_event(self, event: str):
        """Log system event"""
        try:
            logging.info(f"SYSTEM EVENT: {event}")
            # Could also save to database events table
        except Exception as e:
            logging.error(f"Failed to log system event: {e}")
    
    # Background tasks
    async def _start_background_tasks(self):
        """Start background monitoring tasks"""
        try:
            self.monitoring_task = asyncio.create_task(self._monitoring_loop())
            self.heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            
            logging.info("Background tasks started")
            
        except Exception as e:
            logging.error(f"Failed to start background tasks: {e}")
    
    async def _monitoring_loop(self):
        """Background position monitoring loop"""
        while not self.shutdown_event.is_set():
            try:
                if self.system_state == SystemState.OPERATIONAL:
                    await self.monitor_positions()
                
                await asyncio.sleep(self.config.position_check_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"Monitoring loop error: {e}")
                await asyncio.sleep(60)
    
    async def _heartbeat_loop(self):
        """Background heartbeat and metrics loop"""
        while not self.shutdown_event.is_set():
            try:
                # Save metrics
                await self._save_system_metrics()
                
                # Log heartbeat
                logging.info(f"HEARTBEAT - Positions: {len(self.positions)}, "
                           f"State: {self.system_state.value}, "
                           f"Mode: {self.current_mode.value}")
                
                await asyncio.sleep(self.config.heartbeat_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"Heartbeat loop error: {e}")
                await asyncio.sleep(120)
    
    async def _validate_system_components(self):
        """Validate all system components"""
        try:
            # Check database
            if not os.path.exists(self.config.db_path):
                raise TradingSystemError("Database not found")
            
            # Check trading engine if live trading
            if self.current_mode == TradingMode.LIVE_TRADING:
                if not self.trading_engine:
                    raise TradingSystemError("Trading engine required for live trading")
                
                if not hasattr(self.trading_engine, 'client') or not self.trading_engine.client:
                    raise TradingSystemError("Trading engine not connected")
            
            logging.info("System component validation passed")
            
        except Exception as e:
            logging.error(f"System validation failed: {e}")
            raise TradingSystemError(f"System validation failed: {e}")
    
    # Public interface methods
    def get_system_status(self) -> Dict:
        """Get comprehensive system status"""
        try:
            return {
                'system_state': self.system_state.value,
                'trading_mode': self.current_mode.value,
                'active_positions': len(self.positions),
                'circuit_breaker_open': self.circuit_breaker.is_open,
                'session_metrics': self.session_metrics,
                'circuit_breaker_metrics': {
                    'daily_loss': self.circuit_breaker.daily_loss,
                    'consecutive_losses': self.circuit_breaker.consecutive_losses,
                    'trade_count': self.circuit_breaker.trade_count
                },
                'configuration': asdict(self.config),
                'uptime_seconds': (datetime.now() - self.session_metrics['start_time']).total_seconds()
            }
        except Exception as e:
            logging.error(f"Error getting system status: {e}")
            return {'error': str(e)}
    
    def get_open_positions(self) -> List[Dict]:
        """Get all open positions"""
        try:
            return [asdict(position) for position in self.positions.values()]
        except Exception as e:
            logging.error(f"Error getting open positions: {e}")
            return []
    
    def get_performance_summary(self, days: int = 30) -> Dict:
        """Get performance summary"""
        try:
            # Calculate from session metrics
            total_trades = self.session_metrics['total_trades']
            winning_trades = self.session_metrics['winning_trades']
            
            win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
            total_pnl = self.session_metrics['total_pnl']
            
            return {
                'total_trades': total_trades,
                'winning_trades': winning_trades,
                'losing_trades': total_trades - winning_trades,
                'win_rate': win_rate,
                'total_pnl': total_pnl,
                'max_drawdown': self.session_metrics['max_drawdown'],
                'period_days': days,
                'session_duration_hours': (datetime.now() - self.session_metrics['start_time']).total_seconds() / 3600
            }
        except Exception as e:
            logging.error(f"Error getting performance summary: {e}")
            return {'error': str(e)}

# Usage example with safety warnings
if __name__ == "__main__":
    print("=" * 80)
    print("ENHANCED TRADING SYSTEM - PRODUCTION VERSION")
    print("=" * 80)
    print("CRITICAL SAFETY WARNINGS:")
    print("1. This system can trade with REAL MONEY")
    print("2. ALWAYS test in paper trading mode first")
    print("3. Verify all configurations before live trading")
    print("4. Monitor the system constantly during operation")
    print("5. Have emergency stop procedures ready")
    print("6. Never risk more than you can afford to lose")
    print("=" * 80)
    
    async def main():
        system = EnhancedTradingSystem()
        
        try:
            await system.start_system()
            
            # Example signal - DO NOT USE IN PRODUCTION WITHOUT PROPER STRATEGY
            test_signal = {
                'symbol': 'BTCUSDT',
                'side': 'BUY',
                'confidence': 75.0,
                'stop_loss': 45000,
                'take_profit': 52000
            }
            
            # Execute only in paper mode for testing
            if system.current_mode == TradingMode.PAPER_TRADING:
                trade_id = await system.execute_trade_signal(test_signal)
                print(f"Test trade executed: {trade_id}")
            
            # Keep running for demonstration
            await asyncio.sleep(60)
            
        except KeyboardInterrupt:
            print("\nShutdown requested...")
        except Exception as e:
            print(f"System error: {e}")
        finally:
            await system.stop_system()
    
    # Only run if this is the main module
    asyncio.run(main())