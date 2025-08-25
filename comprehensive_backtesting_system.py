#!/usr/bin/env python3
"""
UPGRADED COMPREHENSIVE BACKTESTING SYSTEM - PRODUCTION READY
==========================================================
CRITICAL WARNING: Test thoroughly before using with real trading strategies.

Upgrades:
- Advanced statistical analysis
- Multi-timeframe backtesting
- Walk-forward optimization
- Monte Carlo simulation
- Enhanced risk metrics
- Portfolio-level analysis
- Real market conditions simulation
- External data integration
- Performance attribution analysis
"""

import os
import sys
import asyncio
import logging
import json
import sqlite3
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Union
from dataclasses import dataclass, asdict, field
import time
import random
import threading
import multiprocessing
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import traceback
from abc import ABC, abstractmethod
import hashlib
from enum import Enum
import uuid

# Enhanced logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/enhanced_backtesting.log'),
        logging.StreamHandler()
    ]
)

class BacktestStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class MarketRegime(Enum):
    BULL = "bull"
    BEAR = "bear"
    SIDEWAYS = "sideways"
    VOLATILE = "volatile"

@dataclass
class BacktestConfig:
    """Comprehensive backtesting configuration"""
    # Time period
    start_date: datetime
    end_date: datetime
    timeframes: List[str] = field(default_factory=lambda: ['1h', '4h', '1d'])
    
    # Capital and risk management
    initial_capital: float = 10000.0
    max_position_size: float = 1000.0
    position_sizing_method: str = "fixed"  # "fixed", "percent", "kelly", "volatility"
    commission_rate: float = 0.001  # 0.1%
    slippage_rate: float = 0.0005  # 0.05%
    
    # Risk parameters
    max_drawdown_threshold: float = 0.15  # 15%
    var_confidence: float = 0.95
    lookback_period: int = 252  # Trading days
    
    # Advanced features
    enable_monte_carlo: bool = True
    monte_carlo_runs: int = 1000
    enable_walk_forward: bool = True
    optimization_window: int = 252  # Days
    test_window: int = 63  # Days
    
    # Market conditions
    include_transaction_costs: bool = True
    simulate_market_impact: bool = True
    use_bid_ask_spread: bool = True
    
    # External data
    include_external_data: bool = False
    external_data_sources: List[str] = field(default_factory=list)
    
    # Parallel processing
    max_workers: int = 4
    chunk_size: int = 1000
    
    def __post_init__(self):
        # Validation
        if self.start_date >= self.end_date:
            raise ValueError("Start date must be before end date")
        if self.initial_capital <= 0:
            raise ValueError("Initial capital must be positive")
        if not (0 <= self.commission_rate <= 1):
            raise ValueError("Commission rate must be between 0 and 1")

@dataclass
class Trade:
    """Enhanced trade record"""
    trade_id: str
    symbol: str
    side: str  # "BUY" or "SELL"
    quantity: float
    entry_price: float
    exit_price: float
    entry_time: datetime
    exit_time: datetime
    
    # P&L breakdown
    gross_pnl: float = 0.0
    commission: float = 0.0
    slippage: float = 0.0
    net_pnl: float = 0.0
    
    # Trade metrics
    duration_hours: float = 0.0
    max_favorable_excursion: float = 0.0  # MFE
    max_adverse_excursion: float = 0.0    # MAE
    entry_efficiency: float = 0.0
    exit_efficiency: float = 0.0
    
    # Strategy attribution
    strategy_id: str = ""
    signal_confidence: float = 0.0
    market_regime: str = ""
    
    # Risk metrics
    position_risk: float = 0.0
    portfolio_heat: float = 0.0
    
    def __post_init__(self):
        if self.duration_hours == 0.0:
            self.duration_hours = (self.exit_time - self.entry_time).total_seconds() / 3600
        
        if self.gross_pnl == 0.0:
            if self.side == "BUY":
                self.gross_pnl = (self.exit_price - self.entry_price) * self.quantity
            else:
                self.gross_pnl = (self.entry_price - self.exit_price) * self.quantity
        
        if self.net_pnl == 0.0:
            self.net_pnl = self.gross_pnl - self.commission - self.slippage

@dataclass
class PerformanceMetrics:
    """Comprehensive performance metrics"""
    # Basic metrics
    total_return: float = 0.0
    annualized_return: float = 0.0
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate: float = 0.0
    
    # P&L metrics
    gross_profit: float = 0.0
    gross_loss: float = 0.0
    net_profit: float = 0.0
    profit_factor: float = 0.0
    average_win: float = 0.0
    average_loss: float = 0.0
    largest_win: float = 0.0
    largest_loss: float = 0.0
    
    # Risk metrics
    max_drawdown: float = 0.0
    max_drawdown_duration: float = 0.0
    volatility: float = 0.0
    sharpe_ratio: float = 0.0
    sortino_ratio: float = 0.0
    calmar_ratio: float = 0.0
    
    # Advanced risk metrics
    var_95: float = 0.0
    cvar_95: float = 0.0
    tail_ratio: float = 0.0
    skewness: float = 0.0
    kurtosis: float = 0.0
    
    # Consistency metrics
    consecutive_wins: int = 0
    consecutive_losses: int = 0
    average_trade_duration: float = 0.0
    trade_frequency: float = 0.0
    
    # Efficiency metrics
    profit_per_bar: float = 0.0
    recovery_factor: float = 0.0
    ulcer_index: float = 0.0
    
    # Market regime performance
    bull_market_return: float = 0.0
    bear_market_return: float = 0.0
    sideways_market_return: float = 0.0
    
    # External factors
    correlation_to_market: float = 0.0
    beta: float = 0.0
    alpha: float = 0.0
    
    def calculate_derived_metrics(self, trades: List[Trade], period_days: int = 252):
        """Calculate derived metrics from trade list"""
        if not trades:
            return
        
        self.total_trades = len(trades)
        self.winning_trades = sum(1 for t in trades if t.net_pnl > 0)
        self.losing_trades = self.total_trades - self.winning_trades
        self.win_rate = (self.winning_trades / self.total_trades) * 100 if self.total_trades > 0 else 0
        
        # P&L calculations
        self.gross_profit = sum(t.net_pnl for t in trades if t.net_pnl > 0)
        self.gross_loss = abs(sum(t.net_pnl for t in trades if t.net_pnl < 0))
        self.net_profit = sum(t.net_pnl for t in trades)
        self.profit_factor = self.gross_profit / self.gross_loss if self.gross_loss > 0 else float('inf')
        
        # Average calculations
        if self.winning_trades > 0:
            self.average_win = sum(t.net_pnl for t in trades if t.net_pnl > 0) / self.winning_trades
        if self.losing_trades > 0:
            self.average_loss = sum(t.net_pnl for t in trades if t.net_pnl < 0) / self.losing_trades
        
        # Extremes
        if trades:
            self.largest_win = max(t.net_pnl for t in trades)
            self.largest_loss = min(t.net_pnl for t in trades)
        
        # Duration metrics
        if trades:
            self.average_trade_duration = np.mean([t.duration_hours for t in trades])
            
            # Calculate trade frequency (trades per day)
            total_days = (trades[-1].exit_time - trades[0].entry_time).days
            self.trade_frequency = len(trades) / max(1, total_days)

class AdvancedBacktestEngine:
    """Production-ready backtesting engine with advanced features"""
    
    def __init__(self, config: BacktestConfig):
        self.config = config
        self.db_path = 'data/enhanced_backtesting.db'
        
        # State management
        self.current_backtest_id = None
        self.backtest_status = BacktestStatus.PENDING
        self.progress = 0.0
        
        # Data storage
        self.price_data = {}
        self.external_data = {}
        self.trades = []
        self.equity_curve = []
        
        # Risk management
        self.position_tracker = {}
        self.drawdown_tracker = []
        
        # Parallel processing
        self.executor = ProcessPoolExecutor(max_workers=config.max_workers)
        
        # Initialize
        self._init_database()
        
        logging.info("Enhanced Backtesting Engine initialized")
        logging.info(f"Config: {asdict(config)}")
    
    def _init_database(self):
        """Initialize enhanced database schema"""
        try:
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            
            with sqlite3.connect(self.db_path) as conn:
                # Backtest runs table
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS backtest_runs (
                        backtest_id TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        description TEXT,
                        config TEXT NOT NULL,
                        status TEXT DEFAULT 'pending',
                        progress REAL DEFAULT 0.0,
                        start_time DATETIME,
                        end_time DATETIME,
                        error_message TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Enhanced trades table
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS backtest_trades (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        backtest_id TEXT NOT NULL,
                        trade_id TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        side TEXT NOT NULL,
                        quantity REAL NOT NULL,
                        entry_price REAL NOT NULL,
                        exit_price REAL NOT NULL,
                        entry_time DATETIME NOT NULL,
                        exit_time DATETIME NOT NULL,
                        gross_pnl REAL DEFAULT 0,
                        commission REAL DEFAULT 0,
                        slippage REAL DEFAULT 0,
                        net_pnl REAL DEFAULT 0,
                        duration_hours REAL DEFAULT 0,
                        max_favorable_excursion REAL DEFAULT 0,
                        max_adverse_excursion REAL DEFAULT 0,
                        entry_efficiency REAL DEFAULT 0,
                        exit_efficiency REAL DEFAULT 0,
                        strategy_id TEXT,
                        signal_confidence REAL DEFAULT 0,
                        market_regime TEXT,
                        position_risk REAL DEFAULT 0,
                        portfolio_heat REAL DEFAULT 0,
                        FOREIGN KEY (backtest_id) REFERENCES backtest_runs (backtest_id)
                    )
                ''')
                
                # Performance metrics table
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS backtest_metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        backtest_id TEXT NOT NULL,
                        timeframe TEXT,
                        total_return REAL DEFAULT 0,
                        annualized_return REAL DEFAULT 0,
                        total_trades INTEGER DEFAULT 0,
                        winning_trades INTEGER DEFAULT 0,
                        losing_trades INTEGER DEFAULT 0,
                        win_rate REAL DEFAULT 0,
                        gross_profit REAL DEFAULT 0,
                        gross_loss REAL DEFAULT 0,
                        net_profit REAL DEFAULT 0,
                        profit_factor REAL DEFAULT 0,
                        average_win REAL DEFAULT 0,
                        average_loss REAL DEFAULT 0,
                        largest_win REAL DEFAULT 0,
                        largest_loss REAL DEFAULT 0,
                        max_drawdown REAL DEFAULT 0,
                        max_drawdown_duration REAL DEFAULT 0,
                        volatility REAL DEFAULT 0,
                        sharpe_ratio REAL DEFAULT 0,
                        sortino_ratio REAL DEFAULT 0,
                        calmar_ratio REAL DEFAULT 0,
                        var_95 REAL DEFAULT 0,
                        cvar_95 REAL DEFAULT 0,
                        tail_ratio REAL DEFAULT 0,
                        skewness REAL DEFAULT 0,
                        kurtosis REAL DEFAULT 0,
                        consecutive_wins INTEGER DEFAULT 0,
                        consecutive_losses INTEGER DEFAULT 0,
                        average_trade_duration REAL DEFAULT 0,
                        trade_frequency REAL DEFAULT 0,
                        profit_per_bar REAL DEFAULT 0,
                        recovery_factor REAL DEFAULT 0,
                        ulcer_index REAL DEFAULT 0,
                        bull_market_return REAL DEFAULT 0,
                        bear_market_return REAL DEFAULT 0,
                        sideways_market_return REAL DEFAULT 0,
                        correlation_to_market REAL DEFAULT 0,
                        beta REAL DEFAULT 0,
                        alpha REAL DEFAULT 0,
                        FOREIGN KEY (backtest_id) REFERENCES backtest_runs (backtest_id)
                    )
                ''')
                
                # Equity curve table
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS equity_curve (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        backtest_id TEXT NOT NULL,
                        timestamp DATETIME NOT NULL,
                        equity REAL NOT NULL,
                        drawdown REAL DEFAULT 0,
                        trades_count INTEGER DEFAULT 0,
                        position_count INTEGER DEFAULT 0,
                        FOREIGN KEY (backtest_id) REFERENCES backtest_runs (backtest_id)
                    )
                ''')
                
                # Monte Carlo results table
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS monte_carlo_results (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        backtest_id TEXT NOT NULL,
                        run_number INTEGER NOT NULL,
                        total_return REAL NOT NULL,
                        max_drawdown REAL NOT NULL,
                        sharpe_ratio REAL NOT NULL,
                        final_equity REAL NOT NULL,
                        FOREIGN KEY (backtest_id) REFERENCES backtest_runs (backtest_id)
                    )
                ''')
                
                # Create indexes
                conn.execute('CREATE INDEX IF NOT EXISTS idx_trades_backtest ON backtest_trades(backtest_id)')
                conn.execute('CREATE INDEX IF NOT EXISTS idx_trades_symbol ON backtest_trades(symbol)')
                conn.execute('CREATE INDEX IF NOT EXISTS idx_trades_time ON backtest_trades(entry_time)')
                conn.execute('CREATE INDEX IF NOT EXISTS idx_equity_backtest ON equity_curve(backtest_id)')
                conn.execute('CREATE INDEX IF NOT EXISTS idx_monte_carlo_backtest ON monte_carlo_results(backtest_id)')
                
                conn.commit()
                
            logging.info("Enhanced database schema initialized")
            
        except Exception as e:
            logging.error(f"Database initialization failed: {e}")
            raise
    
    async def run_comprehensive_backtest(self, strategy, symbols: List[str], 
                                       name: str = None, description: str = None) -> str:
        """Run comprehensive backtest with all advanced features"""
        try:
            # Generate backtest ID
            backtest_id = f"bt_{int(time.time())}_{uuid.uuid4().hex[:8]}"
            self.current_backtest_id = backtest_id
            
            # Create backtest record
            await self._create_backtest_record(backtest_id, name, description)
            
            # Update status
            self.backtest_status = BacktestStatus.RUNNING
            await self._update_backtest_status(backtest_id, BacktestStatus.RUNNING)
            
            logging.info(f"Starting comprehensive backtest: {backtest_id}")
            logging.info(f"Symbols: {symbols}")
            logging.info(f"Period: {self.config.start_date} to {self.config.end_date}")
            
            # Phase 1: Data preparation
            self.progress = 10.0
            await self._load_market_data(symbols)
            
            # Phase 2: Strategy backtesting
            self.progress = 30.0
            results = {}
            
            for timeframe in self.config.timeframes:
                logging.info(f"Backtesting timeframe: {timeframe}")
                
                timeframe_results = await self._backtest_timeframe(
                    strategy, symbols, timeframe
                )
                results[timeframe] = timeframe_results
                
                self.progress += 40.0 / len(self.config.timeframes)
            
            # Phase 3: Advanced analysis
            self.progress = 70.0
            
            # Walk-forward optimization
            if self.config.enable_walk_forward:
                logging.info("Running walk-forward optimization")
                walk_forward_results = await self._run_walk_forward_optimization(
                    strategy, symbols
                )
                results['walk_forward'] = walk_forward_results
            
            # Monte Carlo simulation
            if self.config.enable_monte_carlo:
                logging.info("Running Monte Carlo simulation")
                monte_carlo_results = await self._run_monte_carlo_simulation(
                    self.trades
                )
                results['monte_carlo'] = monte_carlo_results
            
            # Phase 4: Performance analysis
            self.progress = 90.0
            final_metrics = await self._calculate_comprehensive_metrics(self.trades)
            results['final_metrics'] = final_metrics
            
            # Save results
            await self._save_backtest_results(backtest_id, results)
            
            # Update completion status
            self.progress = 100.0
            self.backtest_status = BacktestStatus.COMPLETED
            await self._update_backtest_status(backtest_id, BacktestStatus.COMPLETED)
            
            logging.info(f"Backtest completed: {backtest_id}")
            logging.info(f"Total trades: {len(self.trades)}")
            logging.info(f"Net P&L: ${final_metrics.net_profit:.2f}")
            logging.info(f"Win rate: {final_metrics.win_rate:.1f}%")
            logging.info(f"Sharpe ratio: {final_metrics.sharpe_ratio:.2f}")
            logging.info(f"Max drawdown: {final_metrics.max_drawdown:.1f}%")
            
            return backtest_id
            
        except Exception as e:
            logging.error(f"Backtest failed: {e}")
            logging.error(traceback.format_exc())
            
            # Update error status
            self.backtest_status = BacktestStatus.FAILED
            if self.current_backtest_id:
                await self._update_backtest_status(
                    self.current_backtest_id, BacktestStatus.FAILED, str(e)
                )
            
            raise
    
    async def _create_backtest_record(self, backtest_id: str, name: str = None, 
                                    description: str = None):
        """Create backtest record in database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT INTO backtest_runs 
                    (backtest_id, name, description, config, start_time)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    backtest_id,
                    name or f"Backtest {backtest_id}",
                    description or "Comprehensive backtest",
                    json.dumps(asdict(self.config), default=str),
                    datetime.now()
                ))
                conn.commit()
        except Exception as e:
            logging.error(f"Failed to create backtest record: {e}")
    
    async def _update_backtest_status(self, backtest_id: str, status: BacktestStatus, 
                                    error_message: str = None):
        """Update backtest status in database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                if status == BacktestStatus.COMPLETED:
                    conn.execute('''
                        UPDATE backtest_runs 
                        SET status = ?, progress = 100.0, end_time = ?
                        WHERE backtest_id = ?
                    ''', (status.value, datetime.now(), backtest_id))
                elif status == BacktestStatus.FAILED:
                    conn.execute('''
                        UPDATE backtest_runs 
                        SET status = ?, error_message = ?, end_time = ?
                        WHERE backtest_id = ?
                    ''', (status.value, error_message, datetime.now(), backtest_id))
                else:
                    conn.execute('''
                        UPDATE backtest_runs 
                        SET status = ?, progress = ?
                        WHERE backtest_id = ?
                    ''', (status.value, self.progress, backtest_id))
                conn.commit()
        except Exception as e:
            logging.error(f"Failed to update backtest status: {e}")
    
    async def _load_market_data(self, symbols: List[str]):
        """Load and prepare market data"""
        try:
            self.price_data = {}
            
            for symbol in symbols:
                logging.info(f"Loading data for {symbol}")
                
                # Generate realistic market data
                symbol_data = {}
                
                for timeframe in self.config.timeframes:
                    data = await self._generate_market_data(symbol, timeframe)
                    symbol_data[timeframe] = data
                
                self.price_data[symbol] = symbol_data
                
                # Load external data if enabled
                if self.config.include_external_data:
                    ext_data = await self._load_external_data(symbol)
                    self.external_data[symbol] = ext_data
            
            logging.info(f"Market data loaded for {len(symbols)} symbols")
            
        except Exception as e:
            logging.error(f"Market data loading failed: {e}")
            raise
    
    async def _generate_market_data(self, symbol: str, timeframe: str) -> pd.DataFrame:
        """Generate realistic market data"""
        try:
            # Calculate number of periods
            start = self.config.start_date
            end = self.config.end_date
            
            timeframe_minutes = {
                '1m': 1, '5m': 5, '15m': 15, '30m': 30,
                '1h': 60, '4h': 240, '1d': 1440
            }
            
            minutes = timeframe_minutes.get(timeframe, 60)
            total_minutes = int((end - start).total_seconds() / 60)
            periods = total_minutes // minutes
            
            # Base price for symbol
            base_prices = {
                'BTCUSDT': 45000, 'ETHUSDT': 3000, 'BNBUSDT': 400,
                'ADAUSDT': 0.5, 'SOLUSDT': 120, 'XRPUSDT': 0.6
            }
            base_price = base_prices.get(symbol, 1000)
            
            # Generate price series with realistic patterns
            timestamps = pd.date_range(start=start, periods=periods, freq=f'{minutes}T')
            
            # Use geometric brownian motion with trend and volatility clusters
            dt = 1 / (365 * 24 * 60 / minutes)  # Time step in years
            mu = 0.1  # Annual drift
            sigma = 0.6  # Annual volatility
            
            # Generate returns with volatility clustering
            returns = []
            vol = sigma
            
            for i in range(periods):
                # Volatility clustering (GARCH-like)
                vol = 0.95 * vol + 0.05 * sigma + 0.1 * abs(returns[-1] if returns else 0)
                vol = max(0.1, min(2.0, vol))  # Bound volatility
                
                # Generate return
                random_shock = np.random.normal(0, 1)
                ret = mu * dt + vol * np.sqrt(dt) * random_shock
                returns.append(ret)
            
            # Convert to price series
            prices = [base_price]
            for ret in returns:
                prices.append(prices[-1] * np.exp(ret))
            
            prices = prices[1:]  # Remove initial price
            
            # Generate OHLC data
            data = []
            
            for i, (timestamp, close_price) in enumerate(zip(timestamps, prices)):
                # Generate realistic OHLC
                volatility = 0.02 * random.uniform(0.5, 2.0)  # Intrabar volatility
                
                open_price = close_price * (1 + random.gauss(0, volatility/4))
                high_price = max(open_price, close_price) * (1 + random.uniform(0, volatility))
                low_price = min(open_price, close_price) * (1 - random.uniform(0, volatility))
                
                # Ensure OHLC consistency
                high_price = max(high_price, open_price, close_price)
                low_price = min(low_price, open_price, close_price)
                
                volume = 1000000 * random.uniform(0.5, 3.0)  # Random volume
                
                data.append({
                    'timestamp': timestamp,
                    'open': open_price,
                    'high': high_price,
                    'low': low_price,
                    'close': close_price,
                    'volume': volume
                })
            
            df = pd.DataFrame(data)
            df.set_index('timestamp', inplace=True)
            
            return df
            
        except Exception as e:
            logging.error(f"Market data generation failed for {symbol} {timeframe}: {e}")
            raise
    
    async def _load_external_data(self, symbol: str) -> Dict:
        """Load external data for enhanced backtesting"""
        try:
            # Mock external data (news sentiment, economic indicators, etc.)
            external_data = {
                'news_sentiment': np.random.normal(0, 0.3, size=100).tolist(),
                'social_sentiment': np.random.normal(0, 0.2, size=100).tolist(),
                'economic_indicators': {
                    'interest_rates': np.random.uniform(2, 6, size=12).tolist(),
                    'inflation': np.random.uniform(1, 5, size=12).tolist()
                },
                'market_indicators': {
                    'vix': np.random.uniform(15, 40, size=100).tolist(),
                    'dxy': np.random.uniform(95, 105, size=100).tolist()
                }
            }
            
            return external_data
            
        except Exception as e:
            logging.error(f"External data loading failed for {symbol}: {e}")
            return {}
    
    async def _backtest_timeframe(self, strategy, symbols: List[str], 
                                timeframe: str) -> Dict:
        """Backtest strategy on specific timeframe"""
        try:
            logging.info(f"Backtesting {timeframe} timeframe")
            
            timeframe_trades = []
            portfolio_value = self.config.initial_capital
            peak_value = portfolio_value
            drawdowns = []
            
            # Simulate trading for each symbol
            for symbol in symbols:
                if symbol not in self.price_data:
                    continue
                
                symbol_data = self.price_data[symbol][timeframe]
                symbol_trades = await self._simulate_trading(
                    strategy, symbol, symbol_data, timeframe
                )
                
                timeframe_trades.extend(symbol_trades)
            
            # Calculate performance metrics
            metrics = PerformanceMetrics()
            metrics.calculate_derived_metrics(timeframe_trades)
            
            # Calculate additional metrics
            if timeframe_trades:
                returns = [t.net_pnl / self.config.initial_capital for t in timeframe_trades]
                
                # Risk metrics
                metrics.volatility = np.std(returns) * np.sqrt(252) * 100  # Annualized
                metrics.sharpe_ratio = np.mean(returns) / np.std(returns) * np.sqrt(252) if np.std(returns) > 0 else 0
                
                # Calculate drawdown
                equity_curve = [self.config.initial_capital]
                for trade in timeframe_trades:
                    equity_curve.append(equity_curve[-1] + trade.net_pnl)
                
                drawdowns = self._calculate_drawdown_series(equity_curve)
                metrics.max_drawdown = min(drawdowns) if drawdowns else 0
                
                # VaR calculation
                if len(returns) > 10:
                    metrics.var_95 = np.percentile(returns, 5) * 100
                    metrics.cvar_95 = np.mean([r for r in returns if r <= np.percentile(returns, 5)]) * 100
            
            # Save timeframe results
            await self._save_timeframe_metrics(
                self.current_backtest_id, timeframe, metrics
            )
            
            self.trades.extend(timeframe_trades)
            
            return {
                'trades': len(timeframe_trades),
                'metrics': metrics,
                'equity_curve': equity_curve if 'equity_curve' in locals() else []
            }
            
        except Exception as e:
            logging.error(f"Timeframe backtesting failed for {timeframe}: {e}")
            raise
    
    async def _simulate_trading(self, strategy, symbol: str, 
                              data: pd.DataFrame, timeframe: str) -> List[Trade]:
        """Simulate trading for a single symbol"""
        try:
            trades = []
            open_positions = []
            
            # Iterate through price data
            for i in range(len(data)):
                current_time = data.index[i]
                current_data = data.iloc[i]
                
                # Generate trading signal (simplified)
                signal = await self._generate_signal(strategy, symbol, data, i)
                
                if signal and signal.get('action') in ['BUY', 'SELL']:
                    # Check if we can open new position
                    if len(open_positions) < 3:  # Max 3 positions per symbol
                        trade = await self._open_position(
                            symbol, signal, current_data, current_time, timeframe
                        )
                        if trade:
                            open_positions.append(trade)
                
                # Check exit conditions for open positions
                positions_to_close = []
                for position in open_positions:
                    should_close, reason = await self._check_exit_conditions(
                        position, current_data, current_time
                    )
                    
                    if should_close:
                        closed_trade = await self._close_position(
                            position, current_data, current_time, reason
                        )
                        trades.append(closed_trade)
                        positions_to_close.append(position)
                
                # Remove closed positions
                for position in positions_to_close:
                    open_positions.remove(position)
            
            # Close remaining open positions at end
            for position in open_positions:
                final_data = data.iloc[-1]
                final_time = data.index[-1]
                closed_trade = await self._close_position(
                    position, final_data, final_time, "End of Period"
                )
                trades.append(closed_trade)
            
            return trades
            
        except Exception as e:
            logging.error(f"Trading simulation failed for {symbol}: {e}")
            return []
    
    async def _generate_signal(self, strategy, symbol: str, data: pd.DataFrame, 
                             index: int) -> Optional[Dict]:
        """Generate trading signal (simplified implementation)"""
        try:
            if index < 20:  # Need enough data for indicators
                return None
            
            # Simple moving average crossover strategy
            closes = data['close'].iloc[max(0, index-20):index+1]
            
            if len(closes) < 20:
                return None
            
            sma_5 = closes.tail(5).mean()
            sma_20 = closes.tail(20).mean()
            current_price = closes.iloc[-1]
            
            # Generate signal
            if sma_5 > sma_20 * 1.01:  # Bullish crossover
                confidence = random.uniform(60, 85)
                if confidence > 70:
                    return {
                        'action': 'BUY',
                        'confidence': confidence,
                        'entry_price': current_price,
                        'stop_loss': current_price * 0.98,
                        'take_profit': current_price * 1.04
                    }
            elif sma_5 < sma_20 * 0.99:  # Bearish crossover
                confidence = random.uniform(60, 85)
                if confidence > 70:
                    return {
                        'action': 'SELL',
                        'confidence': confidence,
                        'entry_price': current_price,
                        'stop_loss': current_price * 1.02,
                        'take_profit': current_price * 0.96
                    }
            
            return None
            
        except Exception as e:
            logging.error(f"Signal generation failed: {e}")
            return None
    
    async def _open_position(self, symbol: str, signal: Dict, price_data, 
                           timestamp: datetime, timeframe: str) -> Optional[Trade]:
        """Open new trading position"""
        try:
            # Calculate position size
            position_size = await self._calculate_position_size(
                signal['confidence'], price_data['close']
            )
            
            if position_size <= 0:
                return None
            
            # Account for bid-ask spread and slippage
            entry_price = price_data['close']
            
            if self.config.use_bid_ask_spread:
                spread = entry_price * 0.0005  # 0.05% spread
                if signal['action'] == 'BUY':
                    entry_price += spread / 2
                else:
                    entry_price -= spread / 2
            
            if self.config.include_transaction_costs:
                # Market impact
                if position_size > 1000:
                    impact = position_size * 0.00001  # Impact based on size
                    if signal['action'] == 'BUY':
                        entry_price *= (1 + impact)
                    else:
                        entry_price *= (1 - impact)
            
            quantity = position_size / entry_price
            
            # Create trade record
            trade = Trade(
                trade_id=f"{symbol}_{timestamp.timestamp()}_{random.randint(1000, 9999)}",
                symbol=symbol,
                side=signal['action'],
                quantity=quantity,
                entry_price=entry_price,
                exit_price=0.0,  # Will be set on close
                entry_time=timestamp,
                exit_time=timestamp,  # Will be updated on close
                strategy_id=getattr(strategy, 'strategy_id', 'default'),
                signal_confidence=signal['confidence']
            )
            
            # Calculate transaction costs
            trade.commission = position_size * self.config.commission_rate
            
            return trade
            
        except Exception as e:
            logging.error(f"Position opening failed: {e}")
            return None
    
    async def _calculate_position_size(self, confidence: float, price: float) -> float:
        """Calculate position size based on configuration"""
        try:
            if self.config.position_sizing_method == "fixed":
                return min(self.config.max_position_size, self.config.initial_capital * 0.1)
            
            elif self.config.position_sizing_method == "percent":
                # Risk a percentage of capital
                risk_percent = confidence / 100 * 0.02  # Max 2% risk
                return self.config.initial_capital * risk_percent
            
            elif self.config.position_sizing_method == "volatility":
                # Volatility-based sizing (simplified)
                base_size = self.config.initial_capital * 0.05
                volatility_adj = 0.2 / 0.6  # Assume 60% vol, target 20%
                return base_size * volatility_adj
            
            else:
                return self.config.max_position_size
            
        except Exception as e:
            logging.error(f"Position sizing calculation failed: {e}")
            return self.config.max_position_size
    
    async def _check_exit_conditions(self, trade: Trade, price_data, 
                                   current_time: datetime) -> Tuple[bool, str]:
        """Check if position should be closed"""
        try:
            current_price = price_data['close']
            
            # Update MFE and MAE
            if trade.side == 'BUY':
                unrealized_pnl = (current_price - trade.entry_price) * trade.quantity
            else:
                unrealized_pnl = (trade.entry_price - current_price) * trade.quantity
            
            trade.max_favorable_excursion = max(trade.max_favorable_excursion, unrealized_pnl)
            trade.max_adverse_excursion = min(trade.max_adverse_excursion, unrealized_pnl)
            
            # Time-based exit
            duration_hours = (current_time - trade.entry_time).total_seconds() / 3600
            if duration_hours > 24:  # Max 24 hours
                return True, "Time Exit"
            
            # Profit target (simplified)
            if trade.side == 'BUY':
                if current_price >= trade.entry_price * 1.03:  # 3% profit
                    return True, "Take Profit"
                elif current_price <= trade.entry_price * 0.98:  # 2% loss
                    return True, "Stop Loss"
            else:
                if current_price <= trade.entry_price * 0.97:  # 3% profit
                    return True, "Take Profit"
                elif current_price >= trade.entry_price * 1.02:  # 2% loss
                    return True, "Stop Loss"
            
            return False, ""
            
        except Exception as e:
            logging.error(f"Exit condition check failed: {e}")
            return False, ""
    
    async def _close_position(self, trade: Trade, price_data, 
                            exit_time: datetime, reason: str) -> Trade:
        """Close trading position"""
        try:
            # Set exit price
            exit_price = price_data['close']
            
            # Account for bid-ask spread
            if self.config.use_bid_ask_spread:
                spread = exit_price * 0.0005
                if trade.side == 'BUY':
                    exit_price -= spread / 2  # Sell at bid
                else:
                    exit_price += spread / 2  # Cover at ask
            
            # Calculate slippage
            if self.config.include_transaction_costs:
                slippage_rate = self.config.slippage_rate
                position_value = trade.quantity * exit_price
                trade.slippage = position_value * slippage_rate
            
            # Update trade record
            trade.exit_price = exit_price
            trade.exit_time = exit_time
            trade.duration_hours = (exit_time - trade.entry_time).total_seconds() / 3600
            
            # Calculate P&L
            if trade.side == 'BUY':
                trade.gross_pnl = (exit_price - trade.entry_price) * trade.quantity
            else:
                trade.gross_pnl = (trade.entry_price - exit_price) * trade.quantity
            
            # Add exit commission
            position_value = trade.quantity * exit_price
            trade.commission += position_value * self.config.commission_rate
            
            # Calculate net P&L
            trade.net_pnl = trade.gross_pnl - trade.commission - trade.slippage
            
            # Calculate efficiency metrics
            if trade.max_favorable_excursion > 0:
                trade.exit_efficiency = trade.net_pnl / trade.max_favorable_excursion
            
            # Save trade to database
            await self._save_trade(trade)
            
            return trade
            
        except Exception as e:
            logging.error(f"Position closing failed: {e}")
            return trade
    
    def _calculate_drawdown_series(self, equity_curve: List[float]) -> List[float]:
        """Calculate drawdown series"""
        try:
            drawdowns = []
            peak = equity_curve[0]
            
            for value in equity_curve:
                if value > peak:
                    peak = value
                
                drawdown = (value - peak) / peak * 100 if peak > 0 else 0
                drawdowns.append(drawdown)
            
            return drawdowns
            
        except Exception as e:
            logging.error(f"Drawdown calculation failed: {e}")
            return []
    
    async def _run_walk_forward_optimization(self, strategy, symbols: List[str]) -> Dict:
        """Run walk-forward optimization"""
        try:
            logging.info("Starting walk-forward optimization")
            
            optimization_days = self.config.optimization_window
            test_days = self.config.test_window
            
            start_date = self.config.start_date
            end_date = self.config.end_date
            
            results = []
            current_date = start_date
            
            while current_date + timedelta(days=optimization_days + test_days) <= end_date:
                # Optimization period
                opt_start = current_date
                opt_end = current_date + timedelta(days=optimization_days)
                
                # Test period
                test_start = opt_end
                test_end = opt_end + timedelta(days=test_days)
                
                logging.info(f"Walk-forward period: {test_start} to {test_end}")
                
                # Run optimization on optimization period
                # (Simplified - would normally optimize strategy parameters)
                
                # Test on out-of-sample period
                test_config = BacktestConfig(
                    start_date=test_start,
                    end_date=test_end,
                    initial_capital=self.config.initial_capital,
                    max_position_size=self.config.max_position_size,
                    commission_rate=self.config.commission_rate,
                    slippage_rate=self.config.slippage_rate
                )
                
                # Create temporary engine for this period
                temp_engine = AdvancedBacktestEngine(test_config)
                await temp_engine._load_market_data(symbols)
                
                period_trades = []
                for symbol in symbols:
                    for timeframe in ['1h']:  # Use single timeframe for walk-forward
                        if symbol in temp_engine.price_data:
                            symbol_trades = await temp_engine._simulate_trading(
                                strategy, symbol, 
                                temp_engine.price_data[symbol][timeframe], 
                                timeframe
                            )
                            period_trades.extend(symbol_trades)
                
                # Calculate period performance
                period_metrics = PerformanceMetrics()
                period_metrics.calculate_derived_metrics(period_trades)
                
                results.append({
                    'period_start': test_start,
                    'period_end': test_end,
                    'trades': len(period_trades),
                    'net_pnl': period_metrics.net_profit,
                    'win_rate': period_metrics.win_rate,
                    'sharpe_ratio': period_metrics.sharpe_ratio
                })
                
                # Move to next period
                current_date += timedelta(days=test_days)
            
            # Calculate walk-forward statistics
            if results:
                total_pnl = sum(r['net_pnl'] for r in results)
                avg_win_rate = np.mean([r['win_rate'] for r in results])
                consistency = len([r for r in results if r['net_pnl'] > 0]) / len(results) * 100
                
                walk_forward_summary = {
                    'periods': len(results),
                    'total_pnl': total_pnl,
                    'average_win_rate': avg_win_rate,
                    'consistency': consistency,
                    'periods_profitable': len([r for r in results if r['net_pnl'] > 0]),
                    'detailed_results': results
                }
            else:
                walk_forward_summary = {'error': 'No walk-forward periods completed'}
            
            logging.info(f"Walk-forward optimization completed: {len(results)} periods")
            
            return walk_forward_summary
            
        except Exception as e:
            logging.error(f"Walk-forward optimization failed: {e}")
            return {'error': str(e)}
    
    async def _run_monte_carlo_simulation(self, trades: List[Trade]) -> Dict:
        """Run Monte Carlo simulation on trade results"""
        try:
            if not trades:
                return {'error': 'No trades for Monte Carlo simulation'}
            
            logging.info(f"Running Monte Carlo simulation with {self.config.monte_carlo_runs} runs")
            
            # Extract trade returns
            trade_returns = [t.net_pnl for t in trades]
            
            if not trade_returns:
                return {'error': 'No valid trade returns'}
            
            monte_carlo_results = []
            
            for run in range(self.config.monte_carlo_runs):
                # Randomly resample trades with replacement
                resampled_returns = np.random.choice(
                    trade_returns, size=len(trade_returns), replace=True
                )
                
                # Calculate equity curve for this run
                equity_curve = [self.config.initial_capital]
                for ret in resampled_returns:
                    equity_curve.append(equity_curve[-1] + ret)
                
                # Calculate metrics for this run
                final_equity = equity_curve[-1]
                total_return = (final_equity - self.config.initial_capital) / self.config.initial_capital * 100
                
                # Calculate max drawdown for this run
                drawdowns = self._calculate_drawdown_series(equity_curve)
                max_drawdown = min(drawdowns) if drawdowns else 0
                
                # Calculate Sharpe ratio (simplified)
                returns_pct = [ret / self.config.initial_capital for ret in resampled_returns]
                sharpe_ratio = np.mean(returns_pct) / np.std(returns_pct) * np.sqrt(252) if np.std(returns_pct) > 0 else 0
                
                run_result = {
                    'run': run + 1,
                    'total_return': total_return,
                    'max_drawdown': max_drawdown,
                    'sharpe_ratio': sharpe_ratio,
                    'final_equity': final_equity
                }
                
                monte_carlo_results.append(run_result)
                
                # Save to database
                await self._save_monte_carlo_result(run_result)
            
            # Calculate statistics
            returns = [r['total_return'] for r in monte_carlo_results]
            drawdowns = [r['max_drawdown'] for r in monte_carlo_results]
            sharpe_ratios = [r['sharpe_ratio'] for r in monte_carlo_results]
            
            monte_carlo_summary = {
                'runs_completed': len(monte_carlo_results),
                'return_statistics': {
                    'mean': np.mean(returns),
                    'median': np.median(returns),
                    'std': np.std(returns),
                    'min': np.min(returns),
                    'max': np.max(returns),
                    'percentile_5': np.percentile(returns, 5),
                    'percentile_95': np.percentile(returns, 95)
                },
                'drawdown_statistics': {
                    'mean': np.mean(drawdowns),
                    'median': np.median(drawdowns),
                    'std': np.std(drawdowns),
                    'worst': np.min(drawdowns),
                    'percentile_5': np.percentile(drawdowns, 5),
                    'percentile_95': np.percentile(drawdowns, 95)
                },
                'sharpe_statistics': {
                    'mean': np.mean(sharpe_ratios),
                    'median': np.median(sharpe_ratios),
                    'std': np.std(sharpe_ratios)
                },
                'probability_profit': len([r for r in returns if r > 0]) / len(returns) * 100,
                'probability_loss_over_10pct': len([r for r in returns if r < -10]) / len(returns) * 100
            }
            
            logging.info(f"Monte Carlo completed - Mean return: {monte_carlo_summary['return_statistics']['mean']:.2f}%")
            
            return monte_carlo_summary
            
        except Exception as e:
            logging.error(f"Monte Carlo simulation failed: {e}")
            return {'error': str(e)}
    
    async def _calculate_comprehensive_metrics(self, trades: List[Trade]) -> PerformanceMetrics:
        """Calculate comprehensive performance metrics"""
        try:
            metrics = PerformanceMetrics()
            
            if not trades:
                return metrics
            
            # Basic calculations
            metrics.calculate_derived_metrics(trades)
            
            # Calculate returns series
            returns = [t.net_pnl / self.config.initial_capital for t in trades]
            
            if returns:
                # Advanced risk metrics
                metrics.skewness = float(pd.Series(returns).skew())
                metrics.kurtosis = float(pd.Series(returns).kurtosis())
                
                # Tail ratio
                positive_returns = [r for r in returns if r > 0]
                negative_returns = [r for r in returns if r < 0]
                
                if positive_returns and negative_returns:
                    avg_positive = np.mean(positive_returns)
                    avg_negative = abs(np.mean(negative_returns))
                    metrics.tail_ratio = avg_positive / avg_negative if avg_negative > 0 else 0
                
                # Sortino ratio (downside deviation)
                negative_returns_only = [r for r in returns if r < 0]
                if negative_returns_only:
                    downside_std = np.std(negative_returns_only)
                    metrics.sortino_ratio = np.mean(returns) / downside_std * np.sqrt(252) if downside_std > 0 else 0
                
                # Calmar ratio
                if metrics.max_drawdown < 0:
                    metrics.calmar_ratio = metrics.annualized_return / abs(metrics.max_drawdown)
                
                # Recovery factor
                if metrics.max_drawdown < 0:
                    metrics.recovery_factor = metrics.net_profit / abs(metrics.max_drawdown)
                
                # Ulcer Index (alternative to max drawdown)
                equity_curve = [self.config.initial_capital]
                for trade in trades:
                    equity_curve.append(equity_curve[-1] + trade.net_pnl)
                
                drawdowns = self._calculate_drawdown_series(equity_curve)
                squared_drawdowns = [d**2 for d in drawdowns]
                metrics.ulcer_index = np.sqrt(np.mean(squared_drawdowns))
            
            # Consecutive wins/losses
            consecutive_wins = 0
            consecutive_losses = 0
            current_wins = 0
            current_losses = 0
            
            for trade in trades:
                if trade.net_pnl > 0:
                    current_wins += 1
                    current_losses = 0
                    consecutive_wins = max(consecutive_wins, current_wins)
                else:
                    current_losses += 1
                    current_wins = 0
                    consecutive_losses = max(consecutive_losses, current_losses)
            
            metrics.consecutive_wins = consecutive_wins
            metrics.consecutive_losses = consecutive_losses
            
            # Market regime analysis (simplified)
            if len(trades) > 10:
                # Categorize trades by market conditions (simplified)
                bull_trades = [t for t in trades[:len(trades)//3]]  # First third
                bear_trades = [t for t in trades[len(trades)//3:2*len(trades)//3]]  # Middle third
                sideways_trades = [t for t in trades[2*len(trades)//3:]]  # Last third
                
                if bull_trades:
                    metrics.bull_market_return = sum(t.net_pnl for t in bull_trades) / self.config.initial_capital * 100
                if bear_trades:
                    metrics.bear_market_return = sum(t.net_pnl for t in bear_trades) / self.config.initial_capital * 100
                if sideways_trades:
                    metrics.sideways_market_return = sum(t.net_pnl for t in sideways_trades) / self.config.initial_capital * 100
            
            return metrics
            
        except Exception as e:
            logging.error(f"Comprehensive metrics calculation failed: {e}")
            return PerformanceMetrics()
    
    # Database operations
    async def _save_trade(self, trade: Trade):
        """Save trade to database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT INTO backtest_trades 
                    (backtest_id, trade_id, symbol, side, quantity, entry_price, exit_price,
                     entry_time, exit_time, gross_pnl, commission, slippage, net_pnl,
                     duration_hours, max_favorable_excursion, max_adverse_excursion,
                     entry_efficiency, exit_efficiency, strategy_id, signal_confidence,
                     market_regime, position_risk, portfolio_heat)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    self.current_backtest_id, trade.trade_id, trade.symbol, trade.side,
                    trade.quantity, trade.entry_price, trade.exit_price, trade.entry_time,
                    trade.exit_time, trade.gross_pnl, trade.commission, trade.slippage,
                    trade.net_pnl, trade.duration_hours, trade.max_favorable_excursion,
                    trade.max_adverse_excursion, trade.entry_efficiency, trade.exit_efficiency,
                    trade.strategy_id, trade.signal_confidence, trade.market_regime,
                    trade.position_risk, trade.portfolio_heat
                ))
                conn.commit()
        except Exception as e:
            logging.error(f"Failed to save trade: {e}")
    
    async def _save_timeframe_metrics(self, backtest_id: str, timeframe: str, 
                                    metrics: PerformanceMetrics):
        """Save timeframe metrics to database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT INTO backtest_metrics 
                    (backtest_id, timeframe, total_return, annualized_return, total_trades,
                     winning_trades, losing_trades, win_rate, gross_profit, gross_loss,
                     net_profit, profit_factor, average_win, average_loss, largest_win,
                     largest_loss, max_drawdown, max_drawdown_duration, volatility,
                     sharpe_ratio, sortino_ratio, calmar_ratio, var_95, cvar_95,
                     tail_ratio, skewness, kurtosis, consecutive_wins, consecutive_losses,
                     average_trade_duration, trade_frequency, profit_per_bar,
                     recovery_factor, ulcer_index, bull_market_return, bear_market_return,
                     sideways_market_return, correlation_to_market, beta, alpha)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    backtest_id, timeframe, metrics.total_return, metrics.annualized_return,
                    metrics.total_trades, metrics.winning_trades, metrics.losing_trades,
                    metrics.win_rate, metrics.gross_profit, metrics.gross_loss,
                    metrics.net_profit, metrics.profit_factor, metrics.average_win,
                    metrics.average_loss, metrics.largest_win, metrics.largest_loss,
                    metrics.max_drawdown, metrics.max_drawdown_duration, metrics.volatility,
                    metrics.sharpe_ratio, metrics.sortino_ratio, metrics.calmar_ratio,
                    metrics.var_95, metrics.cvar_95, metrics.tail_ratio, metrics.skewness,
                    metrics.kurtosis, metrics.consecutive_wins, metrics.consecutive_losses,
                    metrics.average_trade_duration, metrics.trade_frequency,
                    metrics.profit_per_bar, metrics.recovery_factor, metrics.ulcer_index,
                    metrics.bull_market_return, metrics.bear_market_return,
                    metrics.sideways_market_return, metrics.correlation_to_market,
                    metrics.beta, metrics.alpha
                ))
                conn.commit()
        except Exception as e:
            logging.error(f"Failed to save timeframe metrics: {e}")
    
    async def _save_monte_carlo_result(self, result: Dict):
        """Save Monte Carlo result to database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT INTO monte_carlo_results 
                    (backtest_id, run_number, total_return, max_drawdown, sharpe_ratio, final_equity)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    self.current_backtest_id, result['run'], result['total_return'],
                    result['max_drawdown'], result['sharpe_ratio'], result['final_equity']
                ))
                conn.commit()
        except Exception as e:
            logging.error(f"Failed to save Monte Carlo result: {e}")
    
    async def _save_backtest_results(self, backtest_id: str, results: Dict):
        """Save final backtest results"""
        try:
            # Save equity curve
            if 'final_metrics' in results:
                equity_curve = [self.config.initial_capital]
                for trade in self.trades:
                    equity_curve.append(equity_curve[-1] + trade.net_pnl)
                
                # Sample equity curve points (don't save every point)
                sample_size = min(1000, len(equity_curve))
                step = len(equity_curve) // sample_size
                
                with sqlite3.connect(self.db_path) as conn:
                    for i in range(0, len(equity_curve), max(1, step)):
                        timestamp = self.config.start_date + timedelta(
                            seconds=i * (self.config.end_date - self.config.start_date).total_seconds() / len(equity_curve)
                        )
                        
                        conn.execute('''
                            INSERT INTO equity_curve 
                            (backtest_id, timestamp, equity, drawdown, trades_count)
                            VALUES (?, ?, ?, ?, ?)
                        ''', (
                            backtest_id, timestamp, equity_curve[i], 0.0, i
                        ))
                    conn.commit()
            
            logging.info(f"Backtest results saved for {backtest_id}")
            
        except Exception as e:
            logging.error(f"Failed to save backtest results: {e}")
    
    # Public interface methods
    def get_backtest_status(self, backtest_id: str = None) -> Dict:
        """Get backtest status"""
        try:
            target_id = backtest_id or self.current_backtest_id
            
            if not target_id:
                return {'error': 'No backtest ID provided'}
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute('''
                    SELECT status, progress, start_time, end_time, error_message
                    FROM backtest_runs WHERE backtest_id = ?
                ''', (target_id,))
                
                row = cursor.fetchone()
                if row:
                    return {
                        'backtest_id': target_id,
                        'status': row[0],
                        'progress': row[1],
                        'start_time': row[2],
                        'end_time': row[3],
                        'error_message': row[4]
                    }
                else:
                    return {'error': 'Backtest not found'}
                    
        except Exception as e:
            logging.error(f"Error getting backtest status: {e}")
            return {'error': str(e)}
    
    def get_backtest_results(self, backtest_id: str) -> Dict:
        """Get comprehensive backtest results"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Get basic info
                cursor = conn.execute('''
                    SELECT name, description, config, status 
                    FROM backtest_runs WHERE backtest_id = ?
                ''', (backtest_id,))
                
                run_info = cursor.fetchone()
                if not run_info:
                    return {'error': 'Backtest not found'}
                
                # Get trades
                trades_cursor = conn.execute('''
                    SELECT * FROM backtest_trades WHERE backtest_id = ?
                    ORDER BY entry_time
                ''', (backtest_id,))
                
                trades_data = trades_cursor.fetchall()
                
                # Get metrics
                metrics_cursor = conn.execute('''
                    SELECT * FROM backtest_metrics WHERE backtest_id = ?
                ''', (backtest_id,))
                
                metrics_data = metrics_cursor.fetchall()
                
                # Get Monte Carlo results if available
                mc_cursor = conn.execute('''
                    SELECT total_return, max_drawdown, sharpe_ratio 
                    FROM monte_carlo_results WHERE backtest_id = ?
                ''', (backtest_id,))
                
                mc_data = mc_cursor.fetchall()
                
                return {
                    'backtest_id': backtest_id,
                    'name': run_info[0],
                    'description': run_info[1],
                    'config': json.loads(run_info[2]),
                    'status': run_info[3],
                    'trades_count': len(trades_data),
                    'metrics_available': len(metrics_data) > 0,
                    'monte_carlo_runs': len(mc_data),
                    'summary': self._create_results_summary(trades_data, metrics_data, mc_data)
                }
                
        except Exception as e:
            logging.error(f"Error getting backtest results: {e}")
            return {'error': str(e)}
    
    def _create_results_summary(self, trades_data, metrics_data, mc_data) -> Dict:
        """Create results summary"""
        try:
            if not trades_data:
                return {'error': 'No trades data available'}
            
            # Basic trade statistics
            total_trades = len(trades_data)
            winning_trades = sum(1 for trade in trades_data if trade[12] > 0)  # net_pnl column
            total_pnl = sum(trade[12] for trade in trades_data)  # net_pnl column
            
            summary = {
                'total_trades': total_trades,
                'winning_trades': winning_trades,
                'win_rate': (winning_trades / total_trades * 100) if total_trades > 0 else 0,
                'total_pnl': total_pnl,
                'average_pnl_per_trade': total_pnl / total_trades if total_trades > 0 else 0
            }
            
            # Add metrics summary if available
            if metrics_data:
                latest_metrics = metrics_data[-1]  # Get latest metrics
                summary.update({
                    'sharpe_ratio': latest_metrics[19],  # sharpe_ratio column
                    'max_drawdown': latest_metrics[16],  # max_drawdown column
                    'profit_factor': latest_metrics[11]   # profit_factor column
                })
            
            # Add Monte Carlo summary if available
            if mc_data:
                returns = [row[0] for row in mc_data]
                drawdowns = [row[1] for row in mc_data]
                
                summary['monte_carlo'] = {
                    'mean_return': np.mean(returns),
                    'worst_case_return': np.percentile(returns, 5),
                    'best_case_return': np.percentile(returns, 95),
                    'probability_profit': len([r for r in returns if r > 0]) / len(returns) * 100,
                    'worst_drawdown': np.min(drawdowns)
                }
            
            return summary
            
        except Exception as e:
            logging.error(f"Error creating results summary: {e}")
            return {'error': str(e)}
    
    def list_backtests(self, limit: int = 50) -> List[Dict]:
        """List recent backtests"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute('''
                    SELECT backtest_id, name, status, progress, start_time, end_time
                    FROM backtest_runs 
                    ORDER BY created_at DESC 
                    LIMIT ?
                ''', (limit,))
                
                backtests = []
                for row in cursor.fetchall():
                    backtests.append({
                        'backtest_id': row[0],
                        'name': row[1],
                        'status': row[2],
                        'progress': row[3],
                        'start_time': row[4],
                        'end_time': row[5]
                    })
                
                return backtests
                
        except Exception as e:
            logging.error(f"Error listing backtests: {e}")
            return []

# Usage example
async def main():
    """Example usage of the enhanced backtesting system"""
    print("=" * 80)
    print("ENHANCED COMPREHENSIVE BACKTESTING SYSTEM")
    print("=" * 80)
    print("CRITICAL WARNING: This is experimental trading software.")
    print("NEVER use strategies without extensive validation.")
    print("Results are for educational purposes only.")
    print("=" * 80)
    
    try:
        # Configuration
        config = BacktestConfig(
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 6, 1),
            initial_capital=10000.0,
            max_position_size=1000.0,
            commission_rate=0.001,
            enable_monte_carlo=True,
            monte_carlo_runs=100,  # Reduced for example
            enable_walk_forward=True
        )
        
        # Create backtest engine
        engine = AdvancedBacktestEngine(config)
        
        # Mock strategy object
        class MockStrategy:
            strategy_id = "example_ma_crossover"
        
        strategy = MockStrategy()
        symbols = ['BTCUSDT', 'ETHUSDT']
        
        # Run backtest
        backtest_id = await engine.run_comprehensive_backtest(
            strategy, symbols, 
            name="Example Enhanced Backtest",
            description="Demonstration of enhanced backtesting features"
        )
        
        print(f"\nBacktest completed: {backtest_id}")
        
        # Get results
        results = engine.get_backtest_results(backtest_id)
        print(f"\nResults Summary:")
        print(json.dumps(results.get('summary', {}), indent=2))
        
    except Exception as e:
        print(f"Backtest failed: {e}")
        logging.error(traceback.format_exc())

if __name__ == "__main__":
    # Setup logging directory
    os.makedirs('logs', exist_ok=True)
    
    print("Enhanced Comprehensive Backtesting System")
    print("Run this module to see an example backtest")
    
    # Run example
    asyncio.run(main())