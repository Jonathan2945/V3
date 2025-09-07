#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V3 API MIDDLEWARE - COMPLETE WITH RUN METHOD
============================================
ARCHITECTURE: main.py -> api_middleware.py -> main_controller.py -> dashboard
Handles all Flask routes and controller communication
"""

import os
import logging
import json
import asyncio
from datetime import datetime
from typing import Dict, Any, Optional
from flask import Flask, jsonify, request, render_template_string
from flask_cors import CORS
from dotenv import load_dotenv

load_dotenv()

class ControllerInterface:
    """Interface for communicating with the trading controller"""
    
    def __init__(self):
        self.controller = None
        self.logger = logging.getLogger(f"{__name__}.ControllerInterface")
    
    def register_controller(self, controller):
        """Register the trading controller"""
        self.controller = controller
        self.logger.info("Controller successfully registered")
    
    def call_controller_method(self, method_name: str, *args, **kwargs):
        """Safely call a controller method"""
        if not self.controller:
            return {'error': 'Controller not registered'}
        
        try:
            if hasattr(self.controller, method_name):
                method = getattr(self.controller, method_name)
                return method(*args, **kwargs)
            else:
                self.logger.error(f"Controller method not found: {method_name}")
                return {'error': f'Method {method_name} not found'}
        except Exception as e:
            self.logger.error(f"Error calling controller method {method_name}: {e}")
            return {'error': str(e)}

class V3APIMiddleware:
    """V3 API Middleware with complete Flask implementation"""
    
    def __init__(self, host: str = '0.0.0.0', port: int = 8102):
        self.host = host
        self.port = port
        self.logger = logging.getLogger(f"{__name__}.V3APIMiddleware")
        
        # Initialize Flask app
        self.app = Flask(__name__)
        CORS(self.app)
        
        # Initialize controller interface
        self.controller_interface = ControllerInterface()
        
        # Register all routes
        self._register_routes()
        
        self.logger.info(f"Registered {len(self.app.url_map._rules)} API routes")
        self.logger.info("Flask app initialized with 19 routes")
        self.logger.info("V3 API Middleware initialized")
    
    def register_controller(self, controller):
        """Register the trading controller"""
        self.controller_interface.register_controller(controller)
    
    def _register_routes(self):
        """Register all API routes"""
        
        # Main dashboard route
        @self.app.route('/')
        def dashboard():
            return self._get_dashboard_html()
        
        # API Routes
        @self.app.route('/api/dashboard/overview')
        def api_dashboard_overview():
            try:
                data = self.controller_interface.call_controller_method('get_dashboard_overview')
                return jsonify(data)
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/system/status')
        def api_system_status():
            try:
                status = self.controller_interface.call_controller_method('get_system_status')
                return jsonify(status)
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/trading/status')
        def api_trading_status():
            try:
                status = self.controller_interface.call_controller_method('get_trading_status')
                return jsonify(status)
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/trading/metrics')
        def api_trading_metrics():
            try:
                metrics = self.controller_interface.call_controller_method('get_performance_metrics')
                return jsonify(metrics)
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/trading/trades')
        def api_recent_trades():
            try:
                limit = request.args.get('limit', 10, type=int)
                trades = self.controller_interface.call_controller_method('get_recent_trades', limit)
                return jsonify({'trades': trades})
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/trading/positions')
        def api_active_positions():
            try:
                positions = self.controller_interface.call_controller_method('get_active_positions')
                return jsonify({'positions': positions})
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/strategies/top')
        def api_top_strategies():
            try:
                strategies = self.controller_interface.call_controller_method('get_top_strategies')
                return jsonify({'strategies': strategies})
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/strategies/ml')
        def api_ml_strategies():
            try:
                strategies = self.controller_interface.call_controller_method('get_ml_strategies')
                return jsonify({'strategies': strategies})
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/backtest/progress')
        def api_backtest_progress():
            try:
                progress = self.controller_interface.call_controller_method('get_backtest_progress')
                return jsonify(progress)
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/system/health')
        def api_system_health():
            try:
                health = self.controller_interface.call_controller_method('get_system_health')
                return jsonify(health)
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/scanner/data')
        def api_scanner_data():
            try:
                data = self.controller_interface.call_controller_method('get_scanner_data')
                return jsonify(data)
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/external/status')
        def api_external_status():
            try:
                status = self.controller_interface.call_controller_method('get_external_data_status')
                return jsonify(status)
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/ml/status')
        def api_ml_status():
            try:
                status = self.controller_interface.call_controller_method('get_ml_status')
                return jsonify(status)
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/config')
        def api_config():
            try:
                config = self.controller_interface.call_controller_method('get_config')
                return jsonify(config)
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        # Control endpoints
        @self.app.route('/api/control/start_discovery', methods=['POST'])
        def api_start_discovery():
            try:
                result = asyncio.run(self.controller_interface.call_controller_method('start_strategy_discovery'))
                return jsonify(result)
            except Exception as e:
                return jsonify({'success': False, 'error': str(e)}), 500
        
        @self.app.route('/api/control/start_trading', methods=['POST'])
        def api_start_trading():
            try:
                result = asyncio.run(self.controller_interface.call_controller_method('start_trading'))
                return jsonify(result)
            except Exception as e:
                return jsonify({'success': False, 'error': str(e)}), 500
        
        @self.app.route('/api/control/stop_trading', methods=['POST'])
        def api_stop_trading():
            try:
                result = asyncio.run(self.controller_interface.call_controller_method('stop_trading'))
                return jsonify(result)
            except Exception as e:
                return jsonify({'success': False, 'error': str(e)}), 500
        
        # Health check
        @self.app.route('/health')
        def health_check():
            return jsonify({
                'status': 'healthy',
                'version': 'V3_REAL_DATA_ONLY',
                'timestamp': datetime.now().isoformat(),
                'middleware': 'V3APIMiddleware'
            })
        
        self.logger.info("Registered 19 API routes")
    
    def _get_dashboard_html(self) -> str:
        """Get the dashboard HTML"""
        return '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>V3 Trading System - Real Data Dashboard</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #0a0a0a; color: #ffffff; }
        .header { background: linear-gradient(135deg, #1e3c72, #2a5298); padding: 20px; text-align: center; }
        .header h1 { font-size: 2.5em; margin-bottom: 10px; }
        .header .subtitle { font-size: 1.2em; opacity: 0.9; }
        .container { max-width: 1400px; margin: 0 auto; padding: 20px; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin-bottom: 30px; }
        .card { background: linear-gradient(145deg, #1a1a1a, #2d2d2d); border-radius: 15px; padding: 20px; box-shadow: 0 8px 32px rgba(0,0,0,0.3); border: 1px solid #333; }
        .card h3 { color: #4CAF50; margin-bottom: 15px; font-size: 1.3em; }
        .metric { display: flex; justify-content: space-between; margin: 10px 0; padding: 8px 0; border-bottom: 1px solid #333; }
        .metric:last-child { border-bottom: none; }
        .metric-label { color: #bbb; }
        .metric-value { font-weight: bold; }
        .positive { color: #4CAF50; }
        .negative { color: #f44336; }
        .neutral { color: #2196F3; }
        .warning { color: #ff9800; }
        .button { background: linear-gradient(45deg, #4CAF50, #45a049); color: white; border: none; padding: 12px 24px; border-radius: 8px; cursor: pointer; font-size: 16px; margin: 5px; transition: all 0.3s; }
        .button:hover { transform: translateY(-2px); box-shadow: 0 4px 12px rgba(76,175,80,0.4); }
        .button:disabled { background: #666; cursor: not-allowed; transform: none; }
        .button.danger { background: linear-gradient(45deg, #f44336, #da190b); }
        .button.danger:hover { box-shadow: 0 4px 12px rgba(244,67,54,0.4); }
        .progress-bar { background: #333; height: 20px; border-radius: 10px; overflow: hidden; margin: 10px 0; }
        .progress-fill { background: linear-gradient(45deg, #4CAF50, #45a049); height: 100%; transition: width 0.3s; border-radius: 10px; }
        .trades-table { width: 100%; border-collapse: collapse; margin-top: 15px; }
        .trades-table th, .trades-table td { padding: 10px; text-align: left; border-bottom: 1px solid #333; }
        .trades-table th { background: #2d2d2d; color: #4CAF50; }
        #realDataStatus { background: linear-gradient(45deg, #4CAF50, #45a049); color: white; padding: 10px; border-radius: 8px; text-align: center; margin-bottom: 20px; font-weight: bold; }
    </style>
</head>
<body>
    <div class="header">
        <h1>V3 Trading System</h1>
        <div class="subtitle">Real Market Data - Genetic Strategy Discovery - ML-Enhanced Trading</div>
    </div>
    
    <div id="realDataStatus">
        REAL DATA MODE ACTIVE - No Mock or Simulated Data
    </div>
    
    <div class="container">
        <div class="grid">
            <div class="card">
                <h3>System Status</h3>
                <div class="metric">
                    <span class="metric-label">System Status:</span>
                    <span class="metric-value" id="systemStatus">Loading...</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Trading Active:</span>
                    <span class="metric-value" id="tradingActive">Loading...</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Real Data Mode:</span>
                    <span class="metric-value positive">ENABLED</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Binance Connection:</span>
                    <span class="metric-value" id="binanceConnection">Loading...</span>
                </div>
                <div class="metric">
                    <span class="metric-label">CPU Usage:</span>
                    <span class="metric-value" id="cpuUsage">Loading...</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Memory Usage:</span>
                    <span class="metric-value" id="memoryUsage">Loading...</span>
                </div>
            </div>
            
            <div class="card">
                <h3>Trading Metrics</h3>
                <div class="metric">
                    <span class="metric-label">Total P&L:</span>
                    <span class="metric-value" id="totalPnl">Loading...</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Daily P&L:</span>
                    <span class="metric-value" id="dailyPnl">Loading...</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Win Rate:</span>
                    <span class="metric-value" id="winRate">Loading...</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Total Trades:</span>
                    <span class="metric-value" id="totalTrades">Loading...</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Daily Trades:</span>
                    <span class="metric-value" id="dailyTrades">Loading...</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Best Trade:</span>
                    <span class="metric-value" id="bestTrade">Loading...</span>
                </div>
            </div>
            
            <div class="card">
                <h3>Strategy Discovery</h3>
                <div class="metric">
                    <span class="metric-label">Discovery Status:</span>
                    <span class="metric-value" id="discoveryStatus">Loading...</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Progress:</span>
                    <span class="metric-value" id="discoveryProgress">Loading...</span>
                </div>
                <div class="progress-bar">
                    <div class="progress-fill" id="progressFill" style="width: 0%"></div>
                </div>
                <div class="metric">
                    <span class="metric-label">Current Pair:</span>
                    <span class="metric-value" id="currentPair">Loading...</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Strategies Found:</span>
                    <span class="metric-value" id="strategiesFound">Loading...</span>
                </div>
                <div class="metric">
                    <span class="metric-label">ML Strategies:</span>
                    <span class="metric-value" id="mlStrategies">Loading...</span>
                </div>
            </div>
        </div>
        
        <div class="card">
            <h3>Trading Controls</h3>
            <button class="button" onclick="startDiscovery()" id="startDiscoveryBtn">Start Strategy Discovery</button>
            <button class="button" onclick="startTrading()" id="startTradingBtn" disabled>Start Trading</button>
            <button class="button danger" onclick="stopTrading()" id="stopTradingBtn" disabled>Stop Trading</button>
            <div style="margin-top: 15px;">
                <div class="metric">
                    <span class="metric-label">Note:</span>
                    <span class="metric-value">Complete strategy discovery and ML training before trading</span>
                </div>
            </div>
        </div>
        
        <div class="card">
            <h3>Recent Trades (Real Data Only)</h3>
            <table class="trades-table">
                <thead>
                    <tr>
                        <th>Time</th>
                        <th>Symbol</th>
                        <th>Side</th>
                        <th>P&L</th>
                        <th>P&L %</th>
                        <th>Strategy</th>
                        <th>Confidence</th>
                    </tr>
                </thead>
                <tbody id="tradesTableBody">
                    <tr><td colspan="7" style="text-align: center; padding: 20px;">No trades yet...</td></tr>
                </tbody>
            </table>
        </div>
    </div>
    
    <script>
        let updateInterval;
        
        function updateDashboard() {
            fetch('/api/dashboard/overview')
                .then(response => response.json())
                .then(data => {
                    updateSystemStatus(data);
                    updateTradingMetrics(data);
                    updateStrategyDiscovery(data);
                    updateRecentTrades(data);
                    updateControls(data);
                })
                .catch(error => console.error('Dashboard update error:', error));
        }
        
        function updateSystemStatus(data) {
            const metrics = data.metrics || {};
            const resources = data.system_resources || {};
            
            document.getElementById('systemStatus').textContent = metrics.real_data_mode ? 'Online (Real Data)' : 'Offline';
            document.getElementById('tradingActive').textContent = data.trading_status && data.trading_status.is_running ? 'Active' : 'Stopped';
            document.getElementById('binanceConnection').textContent = metrics.testnet_connected ? 'Connected' : 'Disconnected';
            document.getElementById('cpuUsage').textContent = (resources.cpu_usage || 0).toFixed(1) + '%';
            document.getElementById('memoryUsage').textContent = (resources.memory_usage || 0).toFixed(1) + '%';
        }
        
        function updateTradingMetrics(data) {
            const metrics = data.metrics || {};
            
            const totalPnl = metrics.total_pnl || 0;
            const dailyPnl = metrics.daily_pnl || 0;
            
            document.getElementById('totalPnl').textContent = '$' + totalPnl.toFixed(2);
            document.getElementById('totalPnl').className = 'metric-value ' + (totalPnl >= 0 ? 'positive' : 'negative');
            
            document.getElementById('dailyPnl').textContent = '$' + dailyPnl.toFixed(2);
            document.getElementById('dailyPnl').className = 'metric-value ' + (dailyPnl >= 0 ? 'positive' : 'negative');
            
            document.getElementById('winRate').textContent = (metrics.win_rate || 0).toFixed(1) + '%';
            document.getElementById('totalTrades').textContent = metrics.total_trades || 0;
            document.getElementById('dailyTrades').textContent = metrics.daily_trades || 0;
            document.getElementById('bestTrade').textContent = '$' + (metrics.best_trade || 0).toFixed(2);
        }
        
        function updateStrategyDiscovery(data) {
            fetch('/api/backtest/progress')
                .then(response => response.json())
                .then(progress => {
                    document.getElementById('discoveryStatus').textContent = progress.status || 'not_started';
                    document.getElementById('discoveryProgress').textContent = (progress.progress_percent || 0).toFixed(1) + '%';
                    document.getElementById('progressFill').style.width = (progress.progress_percent || 0) + '%';
                    document.getElementById('currentPair').textContent = progress.current_pair || 'None';
                    document.getElementById('strategiesFound').textContent = progress.discovered_strategies || 0;
                    document.getElementById('mlStrategies').textContent = data.metrics && data.metrics.ml_trained_strategies || 0;
                })
                .catch(error => console.error('Progress update error:', error));
        }
        
        function updateRecentTrades(data) {
            const trades = data.recent_trades || [];
            const tbody = document.getElementById('tradesTableBody');
            
            if (trades.length === 0) {
                tbody.innerHTML = '<tr><td colspan="7" style="text-align: center; padding: 20px;">No trades yet...</td></tr>';
                return;
            }
            
            tbody.innerHTML = trades.slice(-10).reverse().map(trade => 
                '<tr>' +
                    '<td>' + new Date(trade.timestamp).toLocaleTimeString() + '</td>' +
                    '<td>' + trade.symbol + '</td>' +
                    '<td>' + trade.side + '</td>' +
                    '<td class="' + (trade.profit_loss >= 0 ? 'positive' : 'negative') + '">$' + trade.profit_loss.toFixed(2) + '</td>' +
                    '<td class="' + (trade.profit_pct >= 0 ? 'positive' : 'negative') + '">' + trade.profit_pct.toFixed(2) + '%</td>' +
                    '<td>' + trade.source + '</td>' +
                    '<td>' + trade.confidence.toFixed(1) + '%</td>' +
                '</tr>'
            ).join('');
        }
        
        function updateControls(data) {
            const metrics = data.metrics || {};
            const tradingActive = data.trading_status && data.trading_status.is_running || false;
            const discoveryComplete = metrics.comprehensive_backtest_completed || false;
            const mlComplete = metrics.ml_training_completed || false;
            
            document.getElementById('startDiscoveryBtn').disabled = false;
            document.getElementById('startTradingBtn').disabled = !discoveryComplete || !mlComplete || tradingActive;
            document.getElementById('stopTradingBtn').disabled = !tradingActive;
        }
        
        function startDiscovery() {
            fetch('/api/control/start_discovery', { method: 'POST' })
                .then(response => response.json())
                .then(result => {
                    if (result.success) {
                        alert('Strategy discovery started! This will take several minutes...');
                    } else {
                        alert('Failed to start discovery: ' + result.error);
                    }
                })
                .catch(error => {
                    console.error('Discovery start error:', error);
                    alert('Error starting discovery');
                });
        }
        
        function startTrading() {
            fetch('/api/control/start_trading', { method: 'POST' })
                .then(response => response.json())
                .then(result => {
                    if (result.success) {
                        alert('Trading started with ML-enhanced strategies!');
                    } else {
                        alert('Failed to start trading: ' + result.error);
                    }
                })
                .catch(error => {
                    console.error('Trading start error:', error);
                    alert('Error starting trading');
                });
        }
        
        function stopTrading() {
            fetch('/api/control/stop_trading', { method: 'POST' })
                .then(response => response.json())
                .then(result => {
                    if (result.success) {
                        alert('Trading stopped successfully');
                    } else {
                        alert('Failed to stop trading: ' + result.error);
                    }
                })
                .catch(error => {
                    console.error('Trading stop error:', error);
                    alert('Error stopping trading');
                });
        }
        
        updateDashboard();
        updateInterval = setInterval(updateDashboard, 3000);
        
        window.addEventListener('beforeunload', function() {
            if (updateInterval) clearInterval(updateInterval);
        });
    </script>
</body>
</html>'''
    
    def run(self):
        """Start the Flask server - THE MISSING METHOD"""
        try:
            self.logger.info(f"Starting Flask server on {self.host}:{self.port}")
            self.app.run(
                host=self.host,
                port=self.port,
                debug=False,
                threaded=True,
                use_reloader=False
            )
        except Exception as e:
            self.logger.error(f"Flask server error: {e}")
            raise

# Create middleware instance
def create_middleware(host: str = '0.0.0.0', port: int = 8102):
    """Create API middleware instance"""
    return V3APIMiddleware(host, port)