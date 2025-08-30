#!/usr/bin/env python3
"""
Simple Flask server for V3 dashboard testing
"""

from flask import Flask, render_template_string, jsonify
from flask_cors import CORS
import os

app = Flask(__name__)
CORS(app)

# Simple dashboard template
DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>V3 Trading System Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #1a1a1a; color: white; }
        .metrics { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; }
        .metric-card { background: #2a2a2a; padding: 20px; border-radius: 8px; }
        .metric-value { font-size: 24px; font-weight: bold; color: #00ff88; }
        .controls { margin: 20px 0; text-align: center; }
        .btn { padding: 10px 20px; margin: 5px; border: none; border-radius: 5px; cursor: pointer; }
        .btn-success { background: #28a745; color: white; }
        .btn-danger { background: #dc3545; color: white; }
    </style>
</head>
<body>
    <h1>V3 Trading System</h1>
    
    <div class="metrics">
        <div class="metric-card">
            <div class="metric-value" id="total-pnl">$2,847.52</div>
            <div>Total PnL</div>
        </div>
        <div class="metric-card">
            <div class="metric-value" id="win-rate">68.5%</div>
            <div>Win Rate</div>
        </div>
        <div class="metric-card">
            <div class="metric-value" id="total-trades">156</div>
            <div>Total Trades</div>
        </div>
    </div>
    
    <div class="controls">
        <button class="btn btn-success" id="start-trading-btn">Start Trading</button>
        <button class="btn btn-danger" id="emergency-stop-btn">Emergency Stop</button>
    </div>
    
    <div id="system-status">System: Online</div>
    <div id="api-status">API: Connected</div>
    <div id="trading-status">Trading: Active</div>
    
    <div id="recent-trades">
        <h3>Recent Trades</h3>
        <p>BTCUSDT: +$23.45</p>
        <p>ETHUSDT: +$67.89</p>
    </div>
</body>
</html>
"""

@app.route('/')
def dashboard():
    return render_template_string(DASHBOARD_HTML)

@app.route('/api/status')
def status():
    return jsonify({
        'system_status': 'online',
        'trading_status': 'active',
        'api_status': 'connected'
    })

if __name__ == '__main__':
    print("Starting V3 Trading System dashboard on port 8102...")
    app.run(host='0.0.0.0', port=8102, debug=False)
