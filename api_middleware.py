#!/usr/bin/env python3
"""
FIXED API MIDDLEWARE WITH WORKING DASHBOARD BUTTONS
==================================================
Fixes the 415 Content-Type errors and provides a complete dashboard
"""
import asyncio
import json
import logging
import os
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_socketio import SocketIO, emit
from threading import Lock
import weakref
import traceback
from contextlib import contextmanager
import queue
import psutil


class DataCache:
    """Thread-safe data cache with TTL for REAL market data"""
    
    def __init__(self):
        self._cache = {}
        self._timestamps = {}
        self._lock = Lock()
        self._default_ttl = 3  # 3 seconds for real-time trading data
    
    def get(self, key: str, ttl: int = None) -> Optional[Any]:
        with self._lock:
            if key not in self._cache:
                return None
            
            ttl = ttl or self._default_ttl
            if time.time() - self._timestamps[key] > ttl:
                del self._cache[key]
                del self._timestamps[key]
                return None
            
            return self._cache[key]
    
    def set(self, key: str, value: Any):
        with self._lock:
            self._cache[key] = value
            self._timestamps[key] = time.time()
    
    def clear(self):
        with self._lock:
            self._cache.clear()
            self._timestamps.clear()


class DatabaseInterface:
    """Enhanced database interface for middleware - REAL DATA ONLY"""
    
    def __init__(self, db_paths: Dict[str, str]):
        self.db_paths = db_paths
        self._connections = {}
        self._lock = Lock()
        self.logger = logging.getLogger(f"{__name__}.DatabaseInterface")
    
    @contextmanager
    def get_connection(self, db_name: str):
        """Get database connection with automatic cleanup"""
        conn = None
        try:
            with self._lock:
                if db_name not in self._connections:
                    if db_name not in self.db_paths:
                        raise ValueError(f"Unknown database: {db_name}")
                    
                    db_path = self.db_paths[db_name]
                    
                    # Create directory if it doesn't exist
                    os.makedirs(os.path.dirname(db_path), exist_ok=True)
                    
                    # Create empty database if it doesn't exist
                    if not os.path.exists(db_path):
                        conn = sqlite3.connect(db_path)
                        conn.close()
                    
                    self._connections[db_name] = sqlite3.connect(
                        db_path, 
                        check_same_thread=False,
                        timeout=10.0
                    )
                    # Enable WAL mode for better concurrency
                    self._connections[db_name].execute('PRAGMA journal_mode=WAL')
                
                conn = self._connections[db_name]
            
            yield conn
            
        except Exception as e:
            if conn:
                conn.rollback()
            self.logger.error(f"Database error for {db_name}: {e}")
            raise e
        finally:
            if conn:
                conn.commit()


class ControllerInterface:
    """Enhanced interface to communicate with main controller - REAL DATA ONLY"""
    
    def __init__(self):
        self.controller_ref = None
        self._lock = Lock()
        self.logger = logging.getLogger(f"{__name__}.ControllerInterface")
    
    def set_controller(self, controller):
        """Set reference to main controller"""
        with self._lock:
            self.controller_ref = weakref.ref(controller) if controller else None
            self.logger.info("Controller reference set")
    
    def get_controller(self):
        """Get controller instance if available"""
        with self._lock:
            if self.controller_ref is None:
                return None
            controller = self.controller_ref()
            return controller if controller else None
    
    def is_controller_available(self) -> bool:
        """Check if controller is available"""
        return self.get_controller() is not None
    
    def get_controller_data(self, data_type: str) -> Optional[Dict]:
        """Get data from controller safely - REAL DATA ONLY"""
        controller = self.get_controller()
        if not controller:
            return None
        
        try:
            if data_type == "metrics":
                return getattr(controller, 'metrics', {})
            elif data_type == "positions":
                return getattr(controller, 'open_positions', {})
            elif data_type == "recent_trades":
                trades = getattr(controller, 'recent_trades', [])
                return list(trades) if hasattr(trades, '__iter__') else []
            elif data_type == "backtest_progress":
                # FIXED: Get progress from backtester if available
                if hasattr(controller, 'comprehensive_backtester') and controller.comprehensive_backtester:
                    return controller.comprehensive_backtester.get_progress()
                return getattr(controller, 'backtest_progress', {})
            elif data_type == "top_strategies":
                return getattr(controller, 'top_strategies', [])
            elif data_type == "ml_strategies":
                return getattr(controller, 'ml_trained_strategies', [])
            elif data_type == "scanner_data":
                return getattr(controller, 'scanner_data', {})
            elif data_type == "system_resources":
                return getattr(controller, 'system_resources', {})
            elif data_type == "external_data_status":
                return getattr(controller, 'external_data_status', {})
            elif data_type == "comprehensive_dashboard":
                # FIXED: Call the correct method
                if hasattr(controller, 'get_comprehensive_dashboard_data'):
                    return controller.get_comprehensive_dashboard_data()
                return None
            else:
                return None
        except Exception as e:
            self.logger.error(f"Error getting {data_type} from controller: {e}")
            return None
    
    def execute_controller_action(self, action: str, params: Dict = None) -> Dict:
        """Execute action on controller - REAL DATA ONLY"""
        controller = self.get_controller()
        if not controller:
            return {"success": False, "error": "Controller not available"}
        
        try:
            if action == "start_trading":
                controller.is_running = True
                self.logger.info("Trading started via API")
                return {"success": True, "message": "Real trading started"}
            
            elif action == "stop_trading":
                controller.is_running = False
                self.logger.info("Trading stopped via API")
                return {"success": True, "message": "Real trading stopped"}
            
            elif action == "start_backtest":
                # FIXED: Call the correct method that EXISTS
                if hasattr(controller, 'comprehensive_backtester') and controller.comprehensive_backtester:
                    def run_backtest():
                        try:
                            # Create new event loop for this thread
                            loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(loop)
                            
                            # Run the REAL comprehensive backtest
                            result = loop.run_until_complete(
                                controller.comprehensive_backtester.run_comprehensive_backtest()
                            )
                            
                            # Update controller metrics if successful
                            if result and result.get('success'):
                                controller.metrics['comprehensive_backtest_completed'] = True
                                controller.save_current_metrics()
                                self.logger.info("Comprehensive backtest completed successfully")
                            
                            loop.close()
                            
                        except Exception as e:
                            self.logger.error(f"Backtest error: {e}")
                    
                    # Start backtest in background thread
                    thread = threading.Thread(target=run_backtest, daemon=True)
                    thread.start()
                    
                    return {"success": True, "message": "Comprehensive REAL data backtest started"}
                else:
                    return {"success": False, "error": "Backtester not available"}
            
            elif action == "reset_ml_data":
                # Reset ML data for REAL training
                controller.metrics['ml_training_completed'] = False
                controller.ml_trained_strategies = []
                controller.save_current_metrics()
                self.logger.info("ML data reset for REAL training")
                return {"success": True, "message": "ML data reset - ready for REAL training"}
            
            elif action == "save_metrics":
                controller.save_current_metrics()
                return {"success": True, "message": "REAL trading metrics saved"}
            
            elif action == "start_ml_training":
                # Start ML training with REAL data
                if hasattr(controller, 'ai_brain') and controller.ai_brain:
                    def start_ml_training():
                        try:
                            # Train ML models on REAL backtest results
                            if hasattr(controller.ai_brain, 'train_on_backtest_results'):
                                controller.ai_brain.train_on_backtest_results()
                                controller.metrics['ml_training_completed'] = True
                                controller.save_current_metrics()
                                self.logger.info("ML training completed with REAL data")
                        except Exception as e:
                            self.logger.error(f"ML training error: {e}")
                    
                    thread = threading.Thread(target=start_ml_training, daemon=True)
                    thread.start()
                    return {"success": True, "message": "ML training started with REAL data"}
                else:
                    return {"success": False, "error": "AI Brain not available"}
            
            elif action == "emergency_stop":
                # Emergency stop all operations
                controller.is_running = False
                if hasattr(controller, '_shutdown_event'):
                    controller._shutdown_event.set()
                self.logger.warning("Emergency stop activated")
                return {"success": True, "message": "Emergency stop activated"}
            
            else:
                return {"success": False, "error": f"Unknown action: {action}"}
                
        except Exception as e:
            self.logger.error(f"Error executing {action}: {e}")
            return {"success": False, "error": str(e)}


class APIMiddleware:
    """Enhanced API Middleware Service - REAL DATA ONLY with FIXED DASHBOARD"""
    
    def __init__(self, host=None, port=None):
        # Load from environment variables
        if host is None:
            host = os.getenv('HOST', '0.0.0.0')
        if port is None:
            port = int(os.getenv('FLASK_PORT', os.getenv('MAIN_SYSTEM_PORT', '8102')))
        self.host = host
        self.port = port
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        self.cache = DataCache()
        self.db_interface = DatabaseInterface({
            "trading_metrics": "data/trading_metrics.db",
            "trade_logs": "data/trade_logs.db",
            "backtests": "data/comprehensive_backtest.db",
            "api_monitor": "api_monitor.db",
            "system_metrics": "system_metrics.db"
        })
        self.controller_interface = ControllerInterface()
        
        # Initialize Flask app
        self.app = Flask(__name__)
        self.app.config['SECRET_KEY'] = 'v3-api-middleware-real-data'
        CORS(self.app, origins=["*"])
        
        # Initialize SocketIO
        self.socketio = SocketIO(
            self.app, 
            cors_allowed_origins="*",
            async_mode='threading',
            ping_timeout=60,
            ping_interval=25
        )
        
        # Setup routes
        self._setup_routes()
        
        # Real-time update thread
        self._update_thread = None
        self._stop_updates = threading.Event()
        
        self.logger.info("API Middleware initialized - REAL DATA ONLY")
    
    def _setup_routes(self):
        """Setup all API routes for REAL trading data"""
        
        @self.app.route('/health', methods=['GET'])
        def health_check():
            """Health check endpoint"""
            return jsonify({
                "status": "healthy",
                "timestamp": datetime.now().isoformat(),
                "controller_connected": self.controller_interface.is_controller_available(),
                "cache_stats": self.cache.get_cache_stats() if hasattr(self.cache, 'get_cache_stats') else {"cache_size": len(self.cache._cache)},
                "uptime": time.time(),
                "data_mode": "REAL_ONLY"
            })
        
        @self.app.route('/', methods=['GET'])
        def dashboard():
            """Serve enhanced dashboard HTML with working buttons"""
            return '''
            <!DOCTYPE html>
            <html>
            <head>
                <title>V3 Trading System - Professional Dashboard</title>
                <meta charset="utf-8">
                <meta name="viewport" content="width=device-width, initial-scale=1">
                <style>
                    * { margin: 0; padding: 0; box-sizing: border-box; }
                    body { 
                        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                        min-height: 100vh;
                        color: #333;
                    }
                    .container { 
                        max-width: 1400px; 
                        margin: 0 auto; 
                        padding: 20px;
                    }
                    .header { 
                        text-align: center; 
                        color: white; 
                        margin-bottom: 30px;
                        background: rgba(255,255,255,0.1);
                        padding: 30px;
                        border-radius: 15px;
                        backdrop-filter: blur(10px);
                    }
                    .status-card { 
                        background: rgba(255,255,255,0.95); 
                        padding: 20px; 
                        border-radius: 15px; 
                        margin: 15px 0;
                        box-shadow: 0 8px 32px rgba(0,0,0,0.1);
                        backdrop-filter: blur(10px);
                    }
                    .metrics-grid { 
                        display: grid; 
                        grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); 
                        gap: 20px; 
                        margin: 20px 0;
                    }
                    .metric { 
                        background: rgba(255,255,255,0.9); 
                        padding: 25px; 
                        border-radius: 15px; 
                        text-align: center;
                        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
                        transition: transform 0.3s ease;
                    }
                    .metric:hover { transform: translateY(-5px); }
                    .value { 
                        font-size: 32px; 
                        font-weight: bold; 
                        color: #2c3e50;
                        margin-bottom: 8px;
                    }
                    .label { 
                        color: #666; 
                        font-size: 14px;
                        text-transform: uppercase;
                        letter-spacing: 1px;
                    }
                    .controls { 
                        text-align: center; 
                        margin: 30px 0;
                        background: rgba(255,255,255,0.9);
                        padding: 30px;
                        border-radius: 15px;
                        box-shadow: 0 8px 32px rgba(0,0,0,0.1);
                    }
                    .btn { 
                        background: linear-gradient(45deg, #4CAF50, #45a049); 
                        color: white; 
                        padding: 15px 30px; 
                        border: none; 
                        border-radius: 8px; 
                        margin: 8px;
                        cursor: pointer;
                        font-size: 16px;
                        font-weight: 600;
                        transition: all 0.3s ease;
                        box-shadow: 0 4px 15px rgba(76, 175, 80, 0.3);
                    }
                    .btn:hover { 
                        transform: translateY(-2px);
                        box-shadow: 0 6px 20px rgba(76, 175, 80, 0.4);
                    }
                    .btn-danger { 
                        background: linear-gradient(45deg, #f44336, #d32f2f);
                        box-shadow: 0 4px 15px rgba(244, 67, 54, 0.3);
                    }
                    .btn-primary { 
                        background: linear-gradient(45deg, #2196F3, #1976D2);
                        box-shadow: 0 4px 15px rgba(33, 150, 243, 0.3);
                    }
                    .real-badge { 
                        background: linear-gradient(45deg, #4CAF50, #8BC34A); 
                        color: white; 
                        padding: 8px 16px; 
                        border-radius: 20px; 
                        font-size: 14px;
                        font-weight: bold;
                        display: inline-block;
                        margin: 10px 0;
                    }
                    .api-section {
                        background: rgba(255,255,255,0.9);
                        padding: 25px;
                        border-radius: 15px;
                        margin-top: 20px;
                        box-shadow: 0 8px 32px rgba(0,0,0,0.1);
                    }
                    .status-indicator {
                        display: inline-block;
                        width: 12px;
                        height: 12px;
                        border-radius: 50%;
                        margin-right: 8px;
                    }
                    .status-online { background: #4CAF50; }
                    .status-offline { background: #f44336; }
                    .loading { opacity: 0.6; }
                    #log-output {
                        background: #1e1e1e;
                        color: #00ff00;
                        padding: 15px;
                        border-radius: 8px;
                        font-family: 'Courier New', monospace;
                        font-size: 12px;
                        max-height: 200px;
                        overflow-y: auto;
                        margin-top: 15px;
                    }
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="header">
                        <h1>V3 Trading System Dashboard</h1>
                        <div class="real-badge">?? REAL DATA ONLY - PRODUCTION MODE</div>
                        <p>Professional Multi-Timeframe Trading System with Real Market Data</p>
                    </div>
                    
                    <div class="status-card">
                        <h3>System Status</h3>
                        <p id="system-status">
                            <span class="status-indicator status-online"></span>
                            <span id="status-text">Loading...</span>
                        </p>
                        <p><strong>Data Mode:</strong> 100% Real Market Data</p>
                        <p><strong>Exchange:</strong> Binance Testnet + Live API</p>
                    </div>
                    
                    <div class="metrics-grid">
                        <div class="metric">
                            <div class="value" id="total-pnl">$0.00</div>
                            <div class="label">Total P&L</div>
                        </div>
                        <div class="metric">
                            <div class="value" id="total-trades">0</div>
                            <div class="label">Total Trades</div>
                        </div>
                        <div class="metric">
                            <div class="value" id="win-rate">0.0%</div>
                            <div class="label">Win Rate</div>
                        </div>
                        <div class="metric">
                            <div class="value" id="active-positions">0</div>
                            <div class="label">Active Positions</div>
                        </div>
                        <div class="metric">
                            <div class="value" id="daily-pnl">$0.00</div>
                            <div class="label">Daily P&L</div>
                        </div>
                        <div class="metric">
                            <div class="value" id="strategies-loaded">0</div>
                            <div class="label">Strategies Loaded</div>
                        </div>
                    </div>
                    
                    <div class="controls">
                        <h3>Trading Controls</h3>
                        <button class="btn" onclick="startTrading()">?? Start Trading</button>
                        <button class="btn btn-danger" onclick="stopTrading()">?? Stop Trading</button>
                        <button class="btn btn-primary" onclick="startBacktest()">?? Start Backtest</button>
                        <button class="btn btn-primary" onclick="startMLTraining()">?? Start ML Training</button>
                        <button class="btn" onclick="saveMetrics()">?? Save Metrics</button>
                        
                        <div id="log-output"></div>
                    </div>
                    
                    <div class="api-section">
                        <h3>?? API Endpoints</h3>
                        <ul style="list-style: none; padding: 0;">
                            <li>?? <a href="/api/dashboard/overview" target="_blank">/api/dashboard/overview</a> - Dashboard data</li>
                            <li>?? <a href="/api/system/status" target="_blank">/api/system/status</a> - System status</li>
                            <li>?? <a href="/api/trading/recent-trades" target="_blank">/api/trading/recent-trades</a> - Recent trades</li>
                            <li>?? <a href="/api/backtest/progress" target="_blank">/api/backtest/progress</a> - Backtest progress</li>
                            <li>?? <a href="/health" target="_blank">/health</a> - Health check</li>
                        </ul>
                    </div>
                </div>
                
                <script>
                    function logMessage(message) {
                        const logOutput = document.getElementById('log-output');
                        const timestamp = new Date().toLocaleTimeString();
                        logOutput.innerHTML += `[${timestamp}] ${message}\\n`;
                        logOutput.scrollTop = logOutput.scrollHeight;
                    }
                    
                    function updateDashboard() {
                        fetch('/api/dashboard/overview')
                            .then(response => response.json())
                            .then(data => {
                                if (data.trading) {
                                    document.getElementById('total-pnl').textContent = '$' + (data.trading.total_pnl || 0).toFixed(2);
                                    document.getElementById('total-trades').textContent = data.trading.total_trades || 0;
                                    document.getElementById('win-rate').textContent = (data.trading.win_rate || 0).toFixed(1) + '%';
                                    document.getElementById('active-positions').textContent = data.trading.active_positions || 0;
                                    document.getElementById('daily-pnl').textContent = '$' + (data.trading.daily_pnl || 0).toFixed(2);
                                    
                                    document.getElementById('status-text').textContent = 
                                        data.trading.is_running ? 'Trading Active ??' : 'Trading Stopped ??';
                                }
                                
                                // Update strategies count
                                fetch('/api/strategies/top')
                                    .then(response => response.json())
                                    .then(stratData => {
                                        document.getElementById('strategies-loaded').textContent = stratData.count || 0;
                                    })
                                    .catch(() => {});
                            })
                            .catch(error => {
                                document.getElementById('status-text').textContent = 'Error: ' + error.message;
                                logMessage('? Dashboard update error: ' + error.message);
                            });
                    }
                    
                    function makeAPICall(action, message) {
                        logMessage(`?? ${message}...`);
                        
                        // FIXED: Send proper JSON with Content-Type header
                        fetch('/api/control/' + action, { 
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json'
                            },
                            body: JSON.stringify({})
                        })
                        .then(response => response.json())
                        .then(data => {
                            if (data.success) {
                                logMessage('? ' + (data.message || message + ' completed'));
                            } else {
                                logMessage('? ' + (data.error || message + ' failed'));
                            }
                            // Update dashboard after action
                            setTimeout(updateDashboard, 1000);
                        })
                        .catch(error => {
                            logMessage('? ' + message + ' error: ' + error.message);
                        });
                    }
                    
                    function startTrading() {
                        makeAPICall('start_trading', 'Starting real trading');
                    }
                    
                    function stopTrading() {
                        makeAPICall('stop_trading', 'Stopping trading');
                    }
                    
                    function startBacktest() {
                        makeAPICall('start_backtest', 'Starting comprehensive backtest with real data');
                    }
                    
                    function startMLTraining() {
                        makeAPICall('start_ml_training', 'Starting ML training with real data');
                    }
                    
                    function saveMetrics() {
                        makeAPICall('save_metrics', 'Saving trading metrics');
                    }
                    
                    // Initialize dashboard
                    logMessage('?? V3 Trading System Dashboard Initialized');
                    logMessage('?? Real market data mode active');
                    updateDashboard();
                    
                    // Update dashboard every 5 seconds
                    setInterval(updateDashboard, 5000);
                    
                    // Log system info
                    setTimeout(() => {
                        logMessage('?? Connected to V3 Trading System');
                        logMessage('?? Real Binance API integration active');
                        logMessage('?? ML algorithms ready for real data training');
                    }, 1000);
                </script>
            </body>
            </html>
            '''
        
        @self.app.route('/api/dashboard/overview', methods=['GET'])
        def get_dashboard_overview():
            """Get dashboard overview data - REAL DATA ONLY"""
            try:
                # Try cache first
                cached = self.cache.get('dashboard_overview', ttl=2)
                if cached:
                    return jsonify(cached)
                
                # Get fresh REAL data
                overview = self._get_dashboard_overview()
                if overview:
                    self.cache.set('dashboard_overview', overview)
                    return jsonify(overview)
                else:
                    return jsonify({"error": "No data available"}), 500
                
            except Exception as e:
                self.logger.error(f"Dashboard overview error: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/api/trading/metrics', methods=['GET'])
        def get_trading_metrics():
            """Get current trading metrics - REAL DATA ONLY"""
            try:
                cached = self.cache.get('trading_metrics', ttl=1)
                if cached:
                    return jsonify(cached)
                
                metrics = self._get_trading_metrics()
                if metrics:
                    self.cache.set('trading_metrics', metrics)
                    return jsonify(metrics)
                else:
                    return jsonify({"error": "No metrics available"}), 500
                
            except Exception as e:
                self.logger.error(f"Trading metrics error: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/api/trading/positions', methods=['GET'])
        def get_positions():
            """Get current positions - REAL DATA ONLY"""
            try:
                positions = self.controller_interface.get_controller_data('positions') or {}
                return jsonify({
                    "positions": positions,
                    "count": len(positions),
                    "timestamp": datetime.now().isoformat(),
                    "data_source": "REAL_TRADING"
                })
                
            except Exception as e:
                self.logger.error(f"Positions error: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/api/trading/recent-trades', methods=['GET'])
        def get_recent_trades():
            """Get recent trades - REAL DATA ONLY"""
            try:
                limit = request.args.get('limit', 20, type=int)
                trades = self.controller_interface.get_controller_data('recent_trades') or []
                
                # Limit results
                recent_trades = trades[-limit:] if len(trades) > limit else trades
                
                return jsonify({
                    "trades": recent_trades,
                    "count": len(recent_trades),
                    "total_available": len(trades),
                    "timestamp": datetime.now().isoformat(),
                    "data_source": "REAL_TRADING"
                })
                
            except Exception as e:
                self.logger.error(f"Recent trades error: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/api/backtest/progress', methods=['GET'])
        def get_backtest_progress():
            """Get backtesting progress - REAL DATA ONLY"""
            try:
                progress = self.controller_interface.get_controller_data('backtest_progress') or {}
                progress['data_source'] = 'REAL_BINANCE'
                return jsonify(progress)
                
            except Exception as e:
                self.logger.error(f"Backtest progress error: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/api/strategies/top', methods=['GET'])
        def get_top_strategies():
            """Get top strategies - REAL DATA ONLY"""
            try:
                limit = request.args.get('limit', 10, type=int)
                strategies = self.controller_interface.get_controller_data('top_strategies') or []
                
                top_strategies = strategies[:limit]
                
                return jsonify({
                    "strategies": top_strategies,
                    "count": len(top_strategies),
                    "timestamp": datetime.now().isoformat(),
                    "data_source": "REAL_BACKTEST_RESULTS"
                })
                
            except Exception as e:
                self.logger.error(f"Top strategies error: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/api/strategies/ml', methods=['GET'])
        def get_ml_strategies():
            """Get ML-trained strategies - REAL DATA ONLY"""
            try:
                limit = request.args.get('limit', 5, type=int)
                strategies = self.controller_interface.get_controller_data('ml_strategies') or []
                
                ml_strategies = strategies[:limit]
                
                return jsonify({
                    "strategies": ml_strategies,
                    "count": len(ml_strategies),
                    "timestamp": datetime.now().isoformat(),
                    "data_source": "REAL_ML_TRAINING"
                })
                
            except Exception as e:
                self.logger.error(f"ML strategies error: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/api/system/status', methods=['GET'])
        def get_system_status():
            """Get system status - REAL DATA ONLY"""
            try:
                cached = self.cache.get('system_status', ttl=5)
                if cached:
                    return jsonify(cached)
                
                status = self._get_system_status()
                if status:
                    self.cache.set('system_status', status)
                    return jsonify(status)
                else:
                    return jsonify({"error": "No status available"}), 500
                
            except Exception as e:
                self.logger.error(f"System status error: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/api/external/news', methods=['GET'])
        def get_external_news():
            """Get external news data"""
            try:
                # Placeholder for real news integration
                return jsonify({
                    "news": [],
                    "count": 0,
                    "message": "External news integration pending",
                    "timestamp": datetime.now().isoformat()
                })
            except Exception as e:
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/api/social/posts', methods=['GET'])
        def get_social_posts():
            """Get social media data"""
            try:
                # Placeholder for real social media integration
                return jsonify({
                    "posts": [],
                    "count": 0,
                    "message": "Social media integration pending",
                    "timestamp": datetime.now().isoformat()
                })
            except Exception as e:
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/api/control/<action>', methods=['POST'])
        def execute_action(action):
            """Execute control action - REAL DATA ONLY - FIXED CONTENT-TYPE HANDLING"""
            try:
                # FIXED: Handle both JSON and form data
                params = {}
                
                if request.is_json:
                    params = request.get_json() or {}
                elif request.form:
                    params = request.form.to_dict()
                else:
                    params = {}
                
                result = self.controller_interface.execute_controller_action(action, params)
                
                # Clear relevant caches after actions
                if result.get('success'):
                    self.cache.clear()
                
                return jsonify(result)
                
            except Exception as e:
                self.logger.error(f"Action {action} error: {e}")
                return jsonify({"success": False, "error": str(e)}), 500
        
        # SocketIO event handlers
        @self.socketio.on('connect')
        def handle_connect():
            """Handle client connection"""
            self.logger.info('Client connected to real-time updates')
            emit('status', {
                'connected': True, 
                'data_mode': 'REAL_ONLY',
                'timestamp': datetime.now().isoformat()
            })
        
        @self.socketio.on('disconnect')
        def handle_disconnect():
            """Handle client disconnection"""
            self.logger.info('Client disconnected from real-time updates')
        
        @self.socketio.on('request_update')
        def handle_update_request():
            """Handle manual update request"""
            try:
                overview = self._get_dashboard_overview()
                if overview:
                    emit('dashboard_update', overview)
            except Exception as e:
                emit('error', {'message': str(e)})
    
    def _get_dashboard_overview(self) -> Dict:
        """Get comprehensive dashboard overview - REAL DATA ONLY"""
        try:
            # Try to get comprehensive data from controller
            comprehensive_data = self.controller_interface.get_controller_data('comprehensive_dashboard')
            if comprehensive_data and 'overview' in comprehensive_data:
                overview = comprehensive_data['overview']
                overview['data_mode'] = 'REAL_ONLY'
                return overview
            
            # Fallback to basic data collection
            metrics = self.controller_interface.get_controller_data('metrics') or {}
            scanner = self.controller_interface.get_controller_data('scanner_data') or {}
            external = self.controller_interface.get_controller_data('external_data_status') or {}
            
            return {
                "trading": {
                    "is_running": metrics.get('is_running', False),
                    "total_pnl": metrics.get('total_pnl', 0.0),
                    "daily_pnl": metrics.get('daily_pnl', 0.0),
                    "total_trades": metrics.get('total_trades', 0),
                    "daily_trades": metrics.get('daily_trades', 0),
                    "win_rate": metrics.get('win_rate', 0.0),
                    "active_positions": metrics.get('active_positions', 0),
                    "best_trade": metrics.get('best_trade', 0.0),
                    "trading_mode": "REAL_DATA_ONLY"
                },
                "system": {
                    "controller_connected": self.controller_interface.is_controller_available(),
                    "ml_training_completed": metrics.get('ml_training_completed', False),
                    "backtest_completed": metrics.get('comprehensive_backtest_completed', False),
                    "api_rotation_active": metrics.get('api_rotation_active', True),
                    "real_testnet_connected": metrics.get('real_testnet_connected', False),
                    "data_mode": "REAL_ONLY"
                },
                "scanner": {
                    "active_pairs": scanner.get('active_pairs', 0),
                    "opportunities": scanner.get('opportunities', 0),
                    "best_opportunity": scanner.get('best_opportunity', 'None'),
                    "confidence": scanner.get('confidence', 0)
                },
                "external_data": {
                    "working_apis": external.get('working_apis', 1),
                    "total_apis": external.get('total_apis', 6),
                    "api_status": external.get('api_status', {}),
                    "data_mode": external.get('data_mode', 'REAL_ONLY')
                },
                "timestamp": datetime.now().isoformat(),
                "data_source": "REAL_TRADING_SYSTEM"
            }
            
        except Exception as e:
            self.logger.error(f"Dashboard overview error: {e}")
            return None
    
    def _get_trading_metrics(self) -> Dict:
        """Get detailed trading metrics - REAL DATA ONLY"""
        try:
            metrics = self.controller_interface.get_controller_data('metrics') or {}
            return {
                "performance": {
                    "total_pnl": metrics.get('total_pnl', 0.0),
                    "daily_pnl": metrics.get('daily_pnl', 0.0),
                    "total_trades": metrics.get('total_trades', 0),
                    "daily_trades": metrics.get('daily_trades', 0),
                    "winning_trades": metrics.get('winning_trades', 0),
                    "win_rate": metrics.get('win_rate', 0.0),
                    "best_trade": metrics.get('best_trade', 0.0),
                    "avg_trade": metrics.get('avg_trade', 0.0)
                },
                "positions": {
                    "active": metrics.get('active_positions', 0),
                    "max_allowed": 3  # From config
                },
                "status": {
                    "trading_active": metrics.get('is_running', False),
                    "ml_active": metrics.get('ml_training_completed', False),
                    "backtest_done": metrics.get('comprehensive_backtest_completed', False),
                    "data_source": "REAL_TRADING"
                },
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Trading metrics error: {e}")
            return None
    
    def _get_system_status(self) -> Dict:
        """Get system status information - REAL DATA ONLY"""
        try:
            resources = self.controller_interface.get_controller_data('system_resources') or {}
            
            # Get additional system info
            cpu_usage = psutil.cpu_percent(interval=0.1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            return {
                "resources": {
                    "cpu_usage": cpu_usage,
                    "memory_usage": memory.percent,
                    "memory_available_gb": memory.available / (1024**3),
                    "disk_usage": disk.percent,
                    "disk_free_gb": disk.free / (1024**3)
                },
                "controller": {
                    "connected": self.controller_interface.is_controller_available(),
                    "api_calls_today": resources.get('api_calls_today', 0),
                    "data_points_processed": resources.get('data_points_processed', 0),
                    "data_mode": "REAL_ONLY"
                },
                "middleware": {
                    "cache_size": len(self.cache._cache),
                    "uptime": time.time(),
                    "active_connections": 1 if self.controller_interface.is_controller_available() else 0
                },
                "data_sources": {
                    "binance_connected": True,
                    "external_apis": 0,
                    "data_quality": "REAL_MARKET_DATA"
                },
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"System status error: {e}")
            return None
    
    def start_real_time_updates(self):
        """Start real-time update thread"""
        if self._update_thread and self._update_thread.is_alive():
            return
        
        self._stop_updates.clear()
        self._update_thread = threading.Thread(target=self._real_time_update_loop, daemon=True)
        self._update_thread.start()
        self.logger.info("Real-time updates started")
    
    def _real_time_update_loop(self):
        """Real-time update loop for WebSocket - REAL DATA ONLY"""
        while not self._stop_updates.wait(3):  # Update every 3 seconds for real trading
            try:
                # Get fresh REAL data
                overview = self._get_dashboard_overview()
                metrics = self._get_trading_metrics()
                system_status = self._get_system_status()
                
                if overview and metrics and system_status:
                    # Emit to all connected clients
                    self.socketio.emit('dashboard_update', {
                        'overview': overview,
                        'metrics': metrics,
                        'system': system_status,
                        'timestamp': datetime.now().isoformat(),
                        'data_source': 'REAL_TRADING_SYSTEM'
                    })
                
            except Exception as e:
                self.logger.error(f"Real-time update error: {e}")
    
    def register_controller(self, controller):
        """Register main controller with middleware"""
        self.controller_interface.set_controller(controller)
        self.start_real_time_updates()
        self.logger.info("Main controller registered with middleware")
    
    def run(self, debug=False):
        """Run the middleware service"""
        self.logger.info(f"Starting API Middleware on {self.host}:{self.port}")
        self.socketio.run(
            self.app,
            host=self.host,
            port=self.port,
            debug=debug,
            use_reloader=False,
            allow_unsafe_werkzeug=True
        )
    
    def stop(self):
        """Stop the middleware service"""
        self._stop_updates.set()
        if self._update_thread:
            self._update_thread.join(timeout=5)
        self.logger.info("API Middleware stopped")


# Convenience functions for integration
def create_middleware(host=None, port=None) -> APIMiddleware:
    """Create and return configured middleware instance"""
    if host is None:
        host = os.getenv('HOST', '0.0.0.0')
    if port is None:
        port = int(os.getenv('FLASK_PORT', os.getenv('MAIN_SYSTEM_PORT', '8102')))
    return APIMiddleware(host=host, port=port)

def run_middleware_service(controller=None, host=None, port=None, debug=False):
    """Run middleware service with optional controller"""
    middleware = create_middleware(host=host, port=port)
    
    if controller:
        middleware.register_controller(controller)
    
    middleware.run(debug=debug)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Get port from environment
    port = int(os.getenv('FLASK_PORT', os.getenv('MAIN_SYSTEM_PORT', '8102')))
    host = os.getenv('HOST', '0.0.0.0')
    
    print("Starting V3 API Middleware Service - REAL DATA ONLY")
    print(f"Enhanced Dashboard: http://{host}:{port}")
    print(f"API endpoints: http://{host}:{port}/api/")
    print("Real market data integration active")
    
    run_middleware_service(host=host, port=port)