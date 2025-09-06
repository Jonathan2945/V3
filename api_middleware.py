#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V3 API MIDDLEWARE - PURE REAL DATA ONLY
======================================
Zero mock data, zero placeholders, zero fake content
Only displays actual data from real APIs
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
    """Cache for real data only"""
    
    def __init__(self):
        self._cache = {}
        self._timestamps = {}
        self._lock = Lock()
        self._default_ttl = 5
    
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
    """Database interface for real data only"""
    
    def __init__(self, db_paths: Dict[str, str]):
        self.db_paths = db_paths
        self._connections = {}
        self._lock = Lock()
    
    @contextmanager
    def get_connection(self, db_name: str):
        """Get database connection"""
        conn = None
        try:
            with self._lock:
                if db_name not in self._connections:
                    if db_name not in self.db_paths:
                        raise ValueError(f"Unknown database: {db_name}")
                    
                    db_path = self.db_paths[db_name]
                    if not os.path.exists(db_path):
                        conn = sqlite3.connect(db_path)
                        conn.close()
                    
                    self._connections[db_name] = sqlite3.connect(
                        db_path, 
                        check_same_thread=False,
                        timeout=10.0
                    )
                
                conn = self._connections[db_name]
            
            yield conn
            
        except Exception as e:
            if conn:
                conn.rollback()
            raise e
        finally:
            if conn:
                conn.commit()
    
    def query(self, db_name: str, query: str, params: tuple = ()) -> List[Dict]:
        """Execute query and return real results only"""
        try:
            with self.get_connection(db_name) as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                
                columns = [description[0] for description in cursor.description]
                results = []
                for row in cursor.fetchall():
                    results.append(dict(zip(columns, row)))
                
                return results
        except Exception:
            return []


class ControllerInterface:
    """Interface to main controller - real data only"""
    
    def __init__(self):
        self.controller_ref = None
        self._lock = Lock()
        self.logger = logging.getLogger(f"{__name__}.ControllerInterface")
    
    def set_controller(self, controller):
        """Set reference to main controller"""
        with self._lock:
            self.controller_ref = weakref.ref(controller) if controller else None
    
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
        """Get real data from controller - NO FAKE DATA"""
        controller = self.get_controller()
        if not controller:
            return None  # Return None instead of fake data
        
        try:
            if data_type == "metrics":
                return getattr(controller, 'metrics', None)
            elif data_type == "positions":
                return getattr(controller, 'open_positions', None)
            elif data_type == "recent_trades":
                trades = getattr(controller, 'recent_trades', None)
                return list(trades) if trades and hasattr(trades, '__iter__') else None
            elif data_type == "backtest_progress":
                return getattr(controller, 'backtest_progress', None)
            elif data_type == "top_strategies":
                return getattr(controller, 'top_strategies', None)
            elif data_type == "scanner_data":
                return getattr(controller, 'scanner_data', None)
            elif data_type == "system_resources":
                return getattr(controller, 'system_resources', None)
            elif data_type == "external_data_status":
                return getattr(controller, 'external_data_status', None)
            else:
                return None
        except Exception as e:
            self.logger.error(f"Error getting {data_type} from controller: {e}")
            return None
    
    def execute_controller_action(self, action: str, params: Dict = None) -> Dict:
        """Execute real action on controller"""
        controller = self.get_controller()
        if not controller:
            return {"success": False, "error": "Controller not available"}
        
        try:
            if action == "start_trading":
                controller.is_running = True
                return {"success": True, "message": "Trading started"}
            
            elif action == "stop_trading":
                controller.is_running = False
                return {"success": True, "message": "Trading stopped"}
            
            elif action == "start_backtest":
                if hasattr(controller, 'comprehensive_backtester') and controller.comprehensive_backtester:
                    def run_backtest():
                        try:
                            import asyncio
                            asyncio.run(controller.comprehensive_backtester.run_comprehensive_backtest())
                        except Exception as e:
                            self.logger.error(f"Backtest error: {e}")
                    
                    thread = threading.Thread(target=run_backtest, daemon=True)
                    thread.start()
                    return {"success": True, "message": "Backtest started"}
                else:
                    return {"success": False, "error": "Backtester not available"}
            
            elif action == "reset_ml_data":
                if hasattr(controller, 'metrics'):
                    controller.metrics['ml_training_completed'] = False
                if hasattr(controller, 'ml_trained_strategies'):
                    controller.ml_trained_strategies = []
                return {"success": True, "message": "ML data reset"}
            
            elif action == "save_metrics":
                if hasattr(controller, 'save_current_metrics'):
                    controller.save_current_metrics()
                return {"success": True, "message": "Metrics saved"}
            
            else:
                return {"success": False, "error": f"Unknown action: {action}"}
                
        except Exception as e:
            self.logger.error(f"Error executing action {action}: {e}")
            return {"success": False, "error": str(e)}


class RealDataAPIMiddleware:
    """API Middleware that only serves real data - NO MOCK DATA"""
    
    def __init__(self, host=None, port=None):
        if host is None:
            host = os.getenv('HOST', '0.0.0.0')
        if port is None:
            port = int(os.getenv('FLASK_PORT', os.getenv('MAIN_SYSTEM_PORT', '8102')))
        self.host = host
        self.port = port
        self.logger = logging.getLogger(__name__)
        
        self.cache = DataCache()
        self.db_interface = DatabaseInterface({
            "trading_metrics": "data/trading_metrics.db",
            "backtests": "data/comprehensive_backtest.db",
            "api_monitor": "api_monitor.db",
            "system_metrics": "system_metrics.db"
        })
        self.controller_interface = ControllerInterface()
        
        # Initialize Flask app
        self.app = Flask(__name__)
        self.app.config['SECRET_KEY'] = 'v3-real-data-middleware'
        CORS(self.app)
        
        # Initialize SocketIO
        self.socketio = SocketIO(
            self.app, 
            cors_allowed_origins="*",
            async_mode='threading',
            logger=False,
            engineio_logger=False
        )
        
        self._setup_routes()
        
        # Real-time update thread
        self._update_thread = None
        self._stop_updates = threading.Event()
        
        self.logger.info("Real Data API Middleware initialized")
    
    def _setup_routes(self):
        """Setup API routes that only serve real data"""
        
        @self.app.route('/', methods=['GET'])
        def dashboard():
            """Serve dashboard that only shows real data"""
            return self._get_real_data_dashboard_html()
        
        @self.app.route('/health', methods=['GET'])
        def health_check():
            """Health check"""
            return jsonify({
                "status": "healthy",
                "data_mode": "REAL_ONLY",
                "timestamp": datetime.now().isoformat(),
                "controller_connected": self.controller_interface.is_controller_available()
            })
        
        @self.app.route('/api/dashboard/overview', methods=['GET'])
        def get_dashboard_overview():
            """Get real dashboard overview - empty if no real data"""
            try:
                overview = self._get_real_dashboard_overview()
                return jsonify(overview) if overview else jsonify({})
                
            except Exception as e:
                self.logger.error(f"Dashboard overview error: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/api/trading/metrics', methods=['GET'])
        def get_trading_metrics():
            """Get real trading metrics only"""
            try:
                metrics = self._get_real_trading_metrics()
                return jsonify(metrics) if metrics else jsonify({})
                
            except Exception as e:
                self.logger.error(f"Trading metrics error: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/api/trading/positions', methods=['GET'])
        def get_positions():
            """Get real positions only"""
            try:
                positions = self.controller_interface.get_controller_data('positions')
                
                if not positions:
                    return jsonify({"positions": [], "count": 0, "timestamp": datetime.now().isoformat()})
                
                # Convert to list format
                position_list = []
                for symbol, position in positions.items():
                    if position:  # Only include real position data
                        position_list.append({
                            'symbol': symbol,
                            'side': position.get('side'),
                            'quantity': position.get('quantity'),
                            'entry_price': position.get('entry_price'),
                            'current_price': position.get('current_price'),
                            'unrealized_pnl': position.get('unrealized_pnl')
                        })
                
                return jsonify({
                    "positions": position_list,
                    "count": len(position_list),
                    "timestamp": datetime.now().isoformat()
                })
                
            except Exception as e:
                self.logger.error(f"Positions error: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/api/trading/recent-trades', methods=['GET'])
        def get_recent_trades():
            """Get real recent trades only"""
            try:
                limit = request.args.get('limit', 50, type=int)
                trades = self.controller_interface.get_controller_data('recent_trades')
                
                if not trades:
                    return jsonify({"trades": [], "count": 0, "timestamp": datetime.now().isoformat()})
                
                recent_trades = trades[-limit:] if len(trades) > limit else trades
                
                return jsonify({
                    "trades": recent_trades,
                    "count": len(recent_trades),
                    "total_available": len(trades),
                    "timestamp": datetime.now().isoformat()
                })
                
            except Exception as e:
                self.logger.error(f"Recent trades error: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/api/backtest/progress', methods=['GET'])
        def get_backtest_progress():
            """Get real backtesting progress only"""
            try:
                progress = self.controller_interface.get_controller_data('backtest_progress')
                return jsonify(progress) if progress else jsonify({})
                
            except Exception as e:
                self.logger.error(f"Backtest progress error: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/api/strategies/top', methods=['GET'])
        def get_top_strategies():
            """Get real top strategies only"""
            try:
                limit = request.args.get('limit', 10, type=int)
                strategies = self.controller_interface.get_controller_data('top_strategies')
                
                if not strategies:
                    return jsonify({"strategies": [], "count": 0, "timestamp": datetime.now().isoformat()})
                
                top_strategies = strategies[:limit]
                
                return jsonify({
                    "strategies": top_strategies,
                    "count": len(top_strategies),
                    "timestamp": datetime.now().isoformat()
                })
                
            except Exception as e:
                self.logger.error(f"Top strategies error: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/api/system/status', methods=['GET'])
        def get_system_status():
            """Get real system status only"""
            try:
                status = self._get_real_system_status()
                return jsonify(status) if status else jsonify({})
                
            except Exception as e:
                self.logger.error(f"System status error: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/api/market/news', methods=['GET'])
        def get_market_news():
            """Get real market news only - empty until APIs configured"""
            try:
                # Check if external data collector has real news
                external_data = self.controller_interface.get_controller_data('external_data_status')
                
                if not external_data or not external_data.get('latest_data'):
                    return jsonify({"news": [], "count": 0, "timestamp": datetime.now().isoformat()})
                
                # Only return if we have actual news data
                news_data = external_data.get('latest_data', {}).get('news_sentiment', {})
                if news_data.get('articles_analyzed', 0) > 0:
                    # Return real news data
                    return jsonify({
                        "news": news_data,
                        "count": news_data.get('articles_analyzed', 0),
                        "timestamp": datetime.now().isoformat()
                    })
                
                return jsonify({"news": [], "count": 0, "timestamp": datetime.now().isoformat()})
                
            except Exception as e:
                self.logger.error(f"Market news error: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/api/market/sentiment', methods=['GET'])
        def get_market_sentiment():
            """Get real market sentiment only"""
            try:
                external_data = self.controller_interface.get_controller_data('external_data_status')
                
                if not external_data or not external_data.get('latest_data'):
                    return jsonify({})
                
                sentiment_data = external_data.get('latest_data', {})
                
                # Only return if we have real sentiment data
                if sentiment_data.get('market_sentiment', {}).get('overall_sentiment') != 0:
                    return jsonify({
                        "sentiment": sentiment_data,
                        "timestamp": datetime.now().isoformat()
                    })
                
                return jsonify({})
                
            except Exception as e:
                self.logger.error(f"Market sentiment error: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/api/control/<action>', methods=['POST'])
        def execute_action(action):
            """Execute control action"""
            try:
                params = {}
                try:
                    if request.data:
                        params = request.get_json() or {}
                except Exception:
                    params = {}
                
                result = self.controller_interface.execute_controller_action(action, params)
                self.cache.clear()
                
                return jsonify(result)
                
            except Exception as e:
                self.logger.error(f"Action {action} error: {e}")
                return jsonify({"success": False, "error": str(e)}), 500
        
        # WebSocket events
        @self.socketio.on('connect')
        def handle_connect():
            """Handle client connection"""
            self.logger.info('Client connected')
            emit('status', {'connected': True, 'data_mode': 'REAL_ONLY'})
        
        @self.socketio.on('disconnect')
        def handle_disconnect():
            """Handle client disconnection"""
            self.logger.info('Client disconnected')
    
    def _get_real_data_dashboard_html(self):
        """Get dashboard HTML that only displays real data"""
        return '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>V3 Trading System - Real Data Only</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #0f0f23, #1a1a2e, #16213e);
            color: #e0e6ed;
            min-height: 100vh;
        }
        
        .header {
            background: rgba(15, 15, 35, 0.95);
            padding: 20px;
            border-bottom: 2px solid #4ade80;
            text-align: center;
        }
        
        .header h1 {
            font-size: 2.5rem;
            color: #4ade80;
            margin-bottom: 5px;
            text-shadow: 0 0 10px rgba(74, 222, 128, 0.3);
        }
        
        .status-bar {
            display: flex;
            justify-content: space-between;
            align-items: center;
            background: rgba(26, 26, 46, 0.8);
            padding: 10px 20px;
            margin: 20px;
            border-radius: 10px;
            border: 1px solid #374151;
        }
        
        .status-indicator {
            width: 12px;
            height: 12px;
            border-radius: 50%;
            animation: pulse 2s infinite;
        }
        
        .status-online { background: #4ade80; }
        .status-offline { background: #ef4444; }
        
        @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }
        
        .dashboard-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            padding: 20px;
        }
        
        .widget {
            background: rgba(26, 26, 46, 0.8);
            border-radius: 15px;
            padding: 20px;
            border: 1px solid #374151;
            transition: all 0.3s ease;
        }
        
        .widget:hover {
            border-color: #4ade80;
            box-shadow: 0 5px 20px rgba(74, 222, 128, 0.1);
        }
        
        .widget-title {
            font-size: 1.2rem;
            font-weight: 600;
            color: #4ade80;
            margin-bottom: 15px;
            padding-bottom: 10px;
            border-bottom: 1px solid #374151;
        }
        
        .metric-row {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 8px 0;
            border-bottom: 1px solid rgba(55, 65, 81, 0.3);
        }
        
        .metric-label { opacity: 0.8; font-size: 0.9rem; }
        .metric-value { font-weight: 600; font-size: 1.1rem; }
        .positive { color: #4ade80; }
        .negative { color: #ef4444; }
        .no-data { color: #6b7280; font-style: italic; }
        
        .controls {
            display: flex;
            gap: 10px;
            margin: 20px;
            flex-wrap: wrap;
            justify-content: center;
        }
        
        .btn {
            padding: 12px 24px;
            border: none;
            border-radius: 8px;
            cursor: pointer;
            font-weight: 600;
            transition: all 0.3s ease;
            font-size: 0.9rem;
        }
        
        .btn-primary { background: linear-gradient(135deg, #3b82f6, #1d4ed8); color: white; }
        .btn-success { background: linear-gradient(135deg, #10b981, #059669); color: white; }
        .btn-danger { background: linear-gradient(135deg, #ef4444, #dc2626); color: white; }
        
        .btn:hover { transform: translateY(-2px); }
        .btn:disabled { opacity: 0.5; cursor: not-allowed; transform: none !important; }
        
        .empty-state {
            text-align: center;
            padding: 40px 20px;
            color: #6b7280;
            font-style: italic;
        }
        
        .progress-bar {
            width: 100%;
            height: 8px;
            background: #374151;
            border-radius: 4px;
            overflow: hidden;
            margin: 10px 0;
        }
        
        .progress-fill {
            height: 100%;
            background: linear-gradient(90deg, #4ade80, #22c55e);
            border-radius: 4px;
            transition: width 0.3s ease;
        }
        
        .api-grid {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 10px;
        }
        
        .api-item {
            background: rgba(55, 65, 81, 0.3);
            padding: 10px;
            border-radius: 6px;
            text-align: center;
            border: 2px solid transparent;
        }
        
        .api-item.connected { border-color: #4ade80; }
        .api-item.disconnected { border-color: #ef4444; }
        
        .scrollable {
            max-height: 250px;
            overflow-y: auto;
        }
        
        .scrollable::-webkit-scrollbar { width: 6px; }
        .scrollable::-webkit-scrollbar-track { background: #374151; }
        .scrollable::-webkit-scrollbar-thumb { background: #4ade80; border-radius: 3px; }
    </style>
</head>
<body>
    <div class="header">
        <h1>V3 Trading System</h1>
        <p>Real Data Only - No Placeholders</p>
    </div>
    
    <div class="status-bar">
        <div style="display: flex; align-items: center; gap: 10px;">
            <div class="status-indicator" id="system-status"></div>
            <span id="system-status-text">Checking System</span>
        </div>
        <div>
            <span>Last Update: <span id="last-update">Never</span></span>
        </div>
    </div>
    
    <div class="controls">
        <button class="btn btn-success" onclick="startTrading()" id="start-btn">Start Trading</button>
        <button class="btn btn-danger" onclick="stopTrading()" id="stop-btn">Stop Trading</button>
        <button class="btn btn-primary" onclick="startBacktest()" id="backtest-btn">Run Backtest</button>
        <button class="btn btn-primary" onclick="resetML()" id="reset-ml-btn">Reset ML</button>
        <button class="btn btn-primary" onclick="saveMetrics()" id="save-btn">Save Data</button>
    </div>
    
    <div class="dashboard-grid">
        <!-- P&L Overview -->
        <div class="widget">
            <div class="widget-title">P&L Overview</div>
            <div id="pnl-content">
                <div class="empty-state">Waiting for trading data...</div>
            </div>
        </div>
        
        <!-- Trading Stats -->
        <div class="widget">
            <div class="widget-title">Trading Statistics</div>
            <div id="trading-stats-content">
                <div class="empty-state">No trading data available</div>
            </div>
        </div>
        
        <!-- System Resources -->
        <div class="widget">
            <div class="widget-title">System Resources</div>
            <div id="system-resources-content">
                <div class="empty-state">Loading system data...</div>
            </div>
        </div>
        
        <!-- Backtest Progress -->
        <div class="widget">
            <div class="widget-title">Backtest Progress</div>
            <div id="backtest-content">
                <div class="empty-state">No backtest running</div>
            </div>
        </div>
        
        <!-- API Connections -->
        <div class="widget">
            <div class="widget-title">API Connections</div>
            <div class="api-grid" id="api-status">
                <div class="api-item disconnected"><strong>Binance</strong><br>Disconnected</div>
                <div class="api-item disconnected"><strong>Alpha Vantage</strong><br>Disconnected</div>
                <div class="api-item disconnected"><strong>News API</strong><br>Disconnected</div>
                <div class="api-item disconnected"><strong>FRED</strong><br>Disconnected</div>
                <div class="api-item disconnected"><strong>Twitter</strong><br>Disconnected</div>
                <div class="api-item disconnected"><strong>Reddit</strong><br>Disconnected</div>
            </div>
        </div>
        
        <!-- Recent Trades -->
        <div class="widget">
            <div class="widget-title">Recent Trades</div>
            <div class="scrollable" id="recent-trades">
                <div class="empty-state">No trades recorded</div>
            </div>
        </div>
        
        <!-- Market News -->
        <div class="widget">
            <div class="widget-title">Market News</div>
            <div class="scrollable" id="market-news">
                <div class="empty-state">Configure news APIs for real updates</div>
            </div>
        </div>
        
        <!-- Active Positions -->
        <div class="widget">
            <div class="widget-title">Active Positions</div>
            <div class="scrollable" id="active-positions">
                <div class="empty-state">No active positions</div>
            </div>
        </div>
    </div>

    <script>
        let updateInterval;
        
        function updateSystemStatus(isOnline) {
            const statusEl = document.getElementById('system-status');
            const statusTextEl = document.getElementById('system-status-text');
            
            if (isOnline) {
                statusEl.className = 'status-indicator status-online';
                statusTextEl.textContent = 'System Online';
            } else {
                statusEl.className = 'status-indicator status-offline';
                statusTextEl.textContent = 'System Offline';
            }
        }
        
        function updateLastUpdate() {
            document.getElementById('last-update').textContent = new Date().toLocaleTimeString();
        }
        
        async function fetchData(endpoint) {
            try {
                const response = await fetch(`/api/${endpoint}`);
                if (!response.ok) throw new Error(`HTTP ${response.status}`);
                const data = await response.json();
                return Object.keys(data).length > 0 ? data : null;
            } catch (error) {
                console.error(`Failed to fetch ${endpoint}:`, error);
                return null;
            }
        }
        
        function updatePnLOverview(data) {
            const container = document.getElementById('pnl-content');
            if (!data || !data.trading) {
                container.innerHTML = '<div class="empty-state">Waiting for trading data...</div>';
                return;
            }
            
            const trading = data.trading;
            container.innerHTML = `
                <div class="metric-row">
                    <span class="metric-label">Total P&L</span>
                    <span class="metric-value ${trading.total_pnl >= 0 ? 'positive' : 'negative'}">$${trading.total_pnl.toFixed(2)}</span>
                </div>
                <div class="metric-row">
                    <span class="metric-label">Daily P&L</span>
                    <span class="metric-value ${trading.daily_pnl >= 0 ? 'positive' : 'negative'}">$${trading.daily_pnl.toFixed(2)}</span>
                </div>
                <div class="metric-row">
                    <span class="metric-label">Unrealized P&L</span>
                    <span class="metric-value ${trading.unrealized_pnl >= 0 ? 'positive' : 'negative'}">$${(trading.unrealized_pnl || 0).toFixed(2)}</span>
                </div>
                <div class="metric-row">
                    <span class="metric-label">Win Rate</span>
                    <span class="metric-value">${trading.win_rate.toFixed(1)}%</span>
                </div>
            `;
        }
        
        function updateTradingStats(data) {
            const container = document.getElementById('trading-stats-content');
            if (!data || !data.performance) {
                container.innerHTML = '<div class="empty-state">No trading data available</div>';
                return;
            }
            
            const perf = data.performance;
            container.innerHTML = `
                <div class="metric-row">
                    <span class="metric-label">Total Trades</span>
                    <span class="metric-value">${perf.total_trades}</span>
                </div>
                <div class="metric-row">
                    <span class="metric-label">Winning Trades</span>
                    <span class="metric-value">${perf.winning_trades}</span>
                </div>
                <div class="metric-row">
                    <span class="metric-label">Best Trade</span>
                    <span class="metric-value positive">$${perf.best_trade.toFixed(2)}</span>
                </div>
            `;
        }
        
        function updateSystemResources(data) {
            const container = document.getElementById('system-resources-content');
            if (!data || !data.resources) {
                container.innerHTML = '<div class="empty-state">Loading system data...</div>';
                return;
            }
            
            const res = data.resources;
            container.innerHTML = `
                <div class="metric-row">
                    <span class="metric-label">CPU Usage</span>
                    <span class="metric-value">${res.cpu_usage.toFixed(1)}%</span>
                </div>
                <div class="metric-row">
                    <span class="metric-label">Memory Usage</span>
                    <span class="metric-value">${res.memory_usage.toFixed(1)}%</span>
                </div>
            `;
        }
        
        function updateBacktestProgress(data) {
            const container = document.getElementById('backtest-content');
            if (!data || !data.status || data.status === 'not_started') {
                container.innerHTML = '<div class="empty-state">No backtest running</div>';
                return;
            }
            
            container.innerHTML = `
                <div class="metric-row">
                    <span class="metric-label">Status</span>
                    <span class="metric-value">${data.status}</span>
                </div>
                <div class="progress-bar">
                    <div class="progress-fill" style="width: ${data.progress_percent || 0}%"></div>
                </div>
                <div class="metric-row">
                    <span class="metric-label">Progress</span>
                    <span class="metric-value">${(data.progress_percent || 0).toFixed(1)}%</span>
                </div>
                ${data.current_symbol ? `
                <div class="metric-row">
                    <span class="metric-label">Current Symbol</span>
                    <span class="metric-value">${data.current_symbol}</span>
                </div>
                ` : ''}
            `;
        }
        
        function updateAPIStatus(data) {
            if (!data || !data.external_data || !data.external_data.api_status) return;
            
            const apiStatus = data.external_data.api_status;
            const container = document.getElementById('api-status');
            
            const apiMap = {
                'binance': 'Binance',
                'alpha_vantage': 'Alpha Vantage', 
                'news_api': 'News API',
                'fred_api': 'FRED',
                'twitter_api': 'Twitter',
                'reddit_api': 'Reddit'
            };
            
            container.innerHTML = Object.entries(apiMap).map(([key, name]) => {
                const connected = apiStatus[key] || false;
                return `
                    <div class="api-item ${connected ? 'connected' : 'disconnected'}">
                        <strong>${name}</strong><br>
                        ${connected ? 'Connected' : 'Disconnected'}
                    </div>
                `;
            }).join('');
        }
        
        function updateRecentTrades(data) {
            const container = document.getElementById('recent-trades');
            if (!data || !data.trades || data.trades.length === 0) {
                container.innerHTML = '<div class="empty-state">No trades recorded</div>';
                return;
            }
            
            container.innerHTML = data.trades.slice(-5).map(trade => `
                <div style="background: rgba(55, 65, 81, 0.3); padding: 10px; margin: 5px 0; border-radius: 5px; border-left: 3px solid ${trade.profit_loss >= 0 ? '#4ade80' : '#ef4444'}">
                    <div style="display: flex; justify-content: space-between;">
                        <strong>${trade.symbol}</strong>
                        <span class="${trade.profit_loss >= 0 ? 'positive' : 'negative'}">$${trade.profit_loss.toFixed(2)}</span>
                    </div>
                    <div style="font-size: 0.8rem; opacity: 0.8;">
                        ${trade.side} | ${trade.quantity.toFixed(4)} | $${trade.entry_price.toFixed(2)}
                    </div>
                </div>
            `).join('');
        }
        
        async function updateDashboard() {
            try {
                const [overview, metrics, systemStatus, trades, backtest] = await Promise.all([
                    fetchData('dashboard/overview'),
                    fetchData('trading/metrics'),
                    fetchData('system/status'),
                    fetchData('trading/recent-trades'),
                    fetchData('backtest/progress')
                ]);
                
                updateSystemStatus(overview && overview.system && overview.system.controller_connected);
                updatePnLOverview(overview);
                updateTradingStats(metrics);
                updateSystemResources(systemStatus);
                updateBacktestProgress(backtest);
                updateAPIStatus(overview);
                updateRecentTrades(trades);
                updateLastUpdate();
                
            } catch (error) {
                console.error('Dashboard update failed:', error);
                updateSystemStatus(false);
            }
        }
        
        async function executeAction(action, buttonId) {
            const button = document.getElementById(buttonId);
            const originalText = button.textContent;
            
            button.disabled = true;
            button.textContent = 'Processing...';
            
            try {
                const response = await fetch(`/api/control/${action}`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({})
                });
                
                const result = await response.json();
                console.log(`${action}:`, result.success ? result.message : result.error);
                setTimeout(updateDashboard, 1000);
                
            } catch (error) {
                console.error(`${action} error:`, error);
            } finally {
                button.disabled = false;
                button.textContent = originalText;
            }
        }
        
        function startTrading() { executeAction('start_trading', 'start-btn'); }
        function stopTrading() { executeAction('stop_trading', 'stop-btn'); }
        function startBacktest() { executeAction('start_backtest', 'backtest-btn'); }
        function resetML() { executeAction('reset_ml_data', 'reset-ml-btn'); }
        function saveMetrics() { executeAction('save_metrics', 'save-btn'); }
        
        document.addEventListener('DOMContentLoaded', function() {
            console.log('V3 Real Data Dashboard initialized');
            updateDashboard();
            updateInterval = setInterval(updateDashboard, 5000);
        });
        
        window.addEventListener('beforeunload', function() {
            if (updateInterval) clearInterval(updateInterval);
        });
    </script>
</body>
</html>'''
    
    def _get_real_dashboard_overview(self) -> Optional[Dict]:
        """Get real dashboard overview - None if no data"""
        try:
            controller = self.controller_interface.get_controller()
            if not controller:
                return None
            
            if hasattr(controller, 'get_comprehensive_dashboard_data'):
                try:
                    data = controller.get_comprehensive_dashboard_data()
                    return data.get('overview') if data else None
                except Exception:
                    pass
            
            # Get individual data components
            metrics = self.controller_interface.get_controller_data('metrics')
            external = self.controller_interface.get_controller_data('external_data_status')
            
            if not metrics:
                return None
            
            return {
                "trading": {
                    "is_running": getattr(controller, 'is_running', False),
                    "total_pnl": metrics.get('total_pnl', 0.0),
                    "daily_pnl": metrics.get('daily_pnl', 0.0),
                    "unrealized_pnl": metrics.get('unrealized_pnl', 0.0),
                    "total_trades": metrics.get('total_trades', 0),
                    "win_rate": metrics.get('win_rate', 0.0),
                    "active_positions": metrics.get('active_positions', 0),
                    "best_trade": metrics.get('best_trade', 0.0)
                },
                "system": {
                    "controller_connected": True,
                    "ml_training_completed": metrics.get('ml_training_completed', False),
                    "backtest_completed": metrics.get('comprehensive_backtest_completed', False)
                },
                "external_data": external,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Dashboard overview error: {e}")
            return None
    
    def _get_real_trading_metrics(self) -> Optional[Dict]:
        """Get real trading metrics - None if no data"""
        try:
            metrics = self.controller_interface.get_controller_data('metrics')
            if not metrics:
                return None
            
            return {
                "performance": {
                    "total_pnl": metrics.get('total_pnl', 0.0),
                    "daily_pnl": metrics.get('daily_pnl', 0.0),
                    "unrealized_pnl": metrics.get('unrealized_pnl', 0.0),
                    "total_trades": metrics.get('total_trades', 0),
                    "winning_trades": metrics.get('winning_trades', 0),
                    "win_rate": metrics.get('win_rate', 0.0),
                    "best_trade": metrics.get('best_trade', 0.0)
                },
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Trading metrics error: {e}")
            return None
    
    def _get_real_system_status(self) -> Optional[Dict]:
        """Get real system status - None if no data"""
        try:
            resources = self.controller_interface.get_controller_data('system_resources')
            
            # Get real system info
            cpu_usage = psutil.cpu_percent(interval=0.1)
            memory = psutil.virtual_memory()
            
            return {
                "resources": {
                    "cpu_usage": cpu_usage,
                    "memory_usage": memory.percent,
                    "memory_available_gb": memory.available / (1024**3)
                },
                "controller": {
                    "connected": self.controller_interface.is_controller_available(),
                    "api_calls_today": resources.get('api_calls_today', 0) if resources else 0,
                    "data_points_processed": resources.get('data_points_processed', 0) if resources else 0
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
        """Real-time update loop - only sends real data"""
        while not self._stop_updates.wait(5):
            try:
                overview = self._get_real_dashboard_overview()
                metrics = self._get_real_trading_metrics() 
                system_status = self._get_real_system_status()
                
                # Only emit if we have real data
                if overview or metrics or system_status:
                    self.socketio.emit('dashboard_update', {
                        'overview': overview,
                        'metrics': metrics,
                        'system': system_status,
                        'timestamp': datetime.now().isoformat()
                    })
                
            except Exception as e:
                self.logger.error(f"Real-time update error: {e}")
    
    def register_controller(self, controller):
        """Register main controller"""
        self.controller_interface.set_controller(controller)
        self.start_real_time_updates()
        self.logger.info("Controller registered - REAL DATA ONLY")
    
    def run(self, debug=False):
        """Run the middleware service"""
        self.logger.info(f"Starting Real Data Middleware on {self.host}:{self.port}")
        
        try:
            self.socketio.run(
                self.app,
                host=self.host,
                port=self.port,
                debug=debug,
                use_reloader=False,
                allow_unsafe_werkzeug=True
            )
        except Exception as e:
            self.logger.error(f"Middleware run error: {e}")
            raise
    
    def stop(self):
        """Stop the middleware service"""
        self._stop_updates.set()
        if self._update_thread and self._update_thread.is_alive():
            self._update_thread.join(timeout=5)
        self.logger.info("Real Data Middleware stopped")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    port = int(os.getenv('FLASK_PORT', os.getenv('MAIN_SYSTEM_PORT', '8102')))
    host = os.getenv('HOST', '0.0.0.0')
    
    print("Starting V3 Real Data API Middleware")
    print(f"Dashboard: http://{host}:{port}")
    print("REAL DATA ONLY - No mock data, no placeholders")
    
    middleware = RealDataAPIMiddleware(host=host, port=port)
    middleware.run()