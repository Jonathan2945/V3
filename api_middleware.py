#!/usr/bin/env python3
"""
V3 API MIDDLEWARE SERVICE
========================
Middle-man service that sits between dashboard and main controller
Handles data formatting, caching, API management, and real-time updates
Never changes - provides stable interface for dashboard connectivity
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
    """Thread-safe data cache with TTL"""
    
    def __init__(self):
        self._cache = {}
        self._timestamps = {}
        self._lock = Lock()
        self._default_ttl = 5  # 5 seconds default TTL
    
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
    """Simplified database interface for middleware"""
    
    def __init__(self, db_paths: Dict[str, str]):
        self.db_paths = db_paths
        self._connections = {}
        self._lock = Lock()
    
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
                    if not os.path.exists(db_path):
                        # Create empty database if it doesn't exist
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
        """Execute query and return results as list of dictionaries"""
        with self.get_connection(db_name) as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            
            columns = [description[0] for description in cursor.description]
            results = []
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))
            
            return results
    
    def execute(self, db_name: str, query: str, params: tuple = ()) -> int:
        """Execute query and return affected rows"""
        with self.get_connection(db_name) as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            return cursor.rowcount


class ControllerInterface:
    """Interface to communicate with main controller"""
    
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
        """Get data from controller safely"""
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
                return getattr(controller, 'backtest_progress', {})
            elif data_type == "top_strategies":
                return getattr(controller, 'top_strategies', [])
            elif data_type == "scanner_data":
                return getattr(controller, 'scanner_data', {})
            elif data_type == "system_resources":
                return getattr(controller, 'system_resources', {})
            elif data_type == "external_data_status":
                return getattr(controller, 'external_data_status', {})
            else:
                return None
        except Exception as e:
            self.logger.error(f"Error getting {data_type} from controller: {e}")
            return None
    
    def execute_controller_action(self, action: str, params: Dict = None) -> Dict:
        """Execute action on controller"""
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
                    # Start backtest in background
                    def run_backtest():
                        try:
                            asyncio.run(controller.comprehensive_backtester.run_comprehensive_backtest())
                        except Exception as e:
                            self.logger.error(f"Backtest error: {e}")
                    
                    thread = threading.Thread(target=run_backtest)
                    thread.start()
                    return {"success": True, "message": "Comprehensive backtest started"}
                else:
                    return {"success": False, "error": "Backtester not available"}
            
            elif action == "reset_ml_data":
                # Reset ML data
                controller.metrics['ml_training_completed'] = False
                controller.ml_trained_strategies = []
                return {"success": True, "message": "ML data reset"}
            
            elif action == "save_metrics":
                controller.save_current_metrics()
                return {"success": True, "message": "Metrics saved"}
            
            else:
                return {"success": False, "error": f"Unknown action: {action}"}
                
        except Exception as e:
            self.logger.error(f"Error executing {action}: {e}")
            return {"success": False, "error": str(e)}


class APIMiddleware:
    """Main API Middleware Service"""
    
    def __init__(self, host=None, port=None):
        # Load from environment variables
        if host is None:
            host = os.getenv('HOST', '127.0.0.1')
        if port is None:
            port = int(os.getenv('FLASK_PORT', os.getenv('MAIN_SYSTEM_PORT', '8102')))
        self.host = host
        self.port = port
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
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
        self.app.config['SECRET_KEY'] = 'v3-api-middleware-secret'
        CORS(self.app)
        self.socketio = SocketIO(
            self.app, 
            cors_allowed_origins="*",
            async_mode='threading'
        )
        
        # Setup routes
        self._setup_routes()
        
        # Real-time update thread
        self._update_thread = None
        self._stop_updates = threading.Event()
        
        self.logger.info("API Middleware initialized - will serve your dashboard.html")
    
    def _setup_routes(self):
        """Setup all API routes"""
        
        @self.app.route('/', methods=['GET'])
        def serve_dashboard():
            """Serve the dashboard HTML file"""
            try:
                if os.path.exists('dashboard.html'):
                    with open('dashboard.html', 'r', encoding='utf-8') as f:
                        return f.read()
                else:
                    return """
                    <html><body>
                    <h1>V3 Trading System Dashboard</h1>
                    <p>Dashboard file not found. Please ensure dashboard.html exists in the current directory.</p>
                    </body></html>
                    """, 404
            except Exception as e:
                self.logger.error(f"Error serving dashboard: {e}")
                return f"Error loading dashboard: {str(e)}", 500
        
        @self.app.route('/health', methods=['GET'])
        def health_check():
            """Health check endpoint"""
            return jsonify({
                "status": "healthy",
                "timestamp": datetime.now().isoformat(),
                "controller_connected": self.controller_interface.is_controller_available(),
                "cache_size": len(self.cache._cache),
                "uptime": time.time()
            })
        
        @self.app.route('/api/dashboard/overview', methods=['GET'])
        def get_dashboard_overview():
            """Get dashboard overview data"""
            try:
                # Try cache first
                cached = self.cache.get('dashboard_overview', ttl=2)
                if cached:
                    return jsonify(cached)
                
                # Get fresh data
                overview = self._get_dashboard_overview()
                self.cache.set('dashboard_overview', overview)
                
                return jsonify(overview)
                
            except Exception as e:
                self.logger.error(f"Dashboard overview error: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/api/trading/metrics', methods=['GET'])
        def get_trading_metrics():
            """Get current trading metrics"""
            try:
                cached = self.cache.get('trading_metrics', ttl=1)
                if cached:
                    return jsonify(cached)
                
                metrics = self._get_trading_metrics()
                self.cache.set('trading_metrics', metrics)
                
                return jsonify(metrics)
                
            except Exception as e:
                self.logger.error(f"Trading metrics error: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/api/trading/positions', methods=['GET'])
        def get_positions():
            """Get current positions"""
            try:
                positions = self.controller_interface.get_controller_data('positions') or {}
                return jsonify({
                    "positions": positions,
                    "count": len(positions),
                    "timestamp": datetime.now().isoformat()
                })
                
            except Exception as e:
                self.logger.error(f"Positions error: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/api/trading/recent-trades', methods=['GET'])
        def get_recent_trades():
            """Get recent trades"""
            try:
                limit = request.args.get('limit', 20, type=int)
                trades = self.controller_interface.get_controller_data('recent_trades') or []
                
                # Limit results
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
            """Get backtesting progress"""
            try:
                progress = self.controller_interface.get_controller_data('backtest_progress') or {}
                return jsonify(progress)
                
            except Exception as e:
                self.logger.error(f"Backtest progress error: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/api/strategies/top', methods=['GET'])
        def get_top_strategies():
            """Get top strategies"""
            try:
                limit = request.args.get('limit', 10, type=int)
                strategies = self.controller_interface.get_controller_data('top_strategies') or []
                
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
            """Get system status"""
            try:
                cached = self.cache.get('system_status', ttl=5)
                if cached:
                    return jsonify(cached)
                
                status = self._get_system_status()
                self.cache.set('system_status', status)
                
                return jsonify(status)
                
            except Exception as e:
                self.logger.error(f"System status error: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/api/control/<action>', methods=['POST'])
        def execute_action(action):
            """Execute control action - FIXED to handle form data and JSON"""
            try:
                params = {}
                
                # Try to get JSON data safely
                try:
                    if request.is_json and request.content_length and request.content_length > 0:
                        params = request.get_json(force=False, silent=True) or {}
                except Exception:
                    pass
                
                # If no JSON, try form data
                if not params and request.form:
                    params = dict(request.form)
                
                # If still no params, try raw data as JSON
                if not params and request.data:
                    try:
                        data_str = request.data.decode('utf-8').strip()
                        if data_str:
                            params = json.loads(data_str)
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        pass
                
                # Log the action
                self.logger.info(f"Executing action: {action} with params: {params}")
                
                # Execute the action
                result = self.controller_interface.execute_controller_action(action, params)
                
                # Clear relevant caches
                self.cache.clear()
                
                return jsonify(result)
                
            except Exception as e:
                self.logger.error(f"Action {action} error: {e}")
                traceback.print_exc()
                return jsonify({"success": False, "error": str(e)}), 400
        
        @self.socketio.on('connect')
        def handle_connect():
            """Handle client connection"""
            self.logger.info('Client connected to real-time updates')
            emit('status', {'connected': True})
        
        @self.socketio.on('disconnect')
        def handle_disconnect():
            """Handle client disconnection"""
            self.logger.info('Client disconnected from real-time updates')
    
    def _get_dashboard_overview(self) -> Dict:
        """Get comprehensive dashboard overview"""
        try:
            controller = self.controller_interface.get_controller()
            if controller and hasattr(controller, 'get_comprehensive_dashboard_data'):
                try:
                    return controller.get_comprehensive_dashboard_data()['overview']
                except Exception as e:
                    self.logger.error(f"Failed to get comprehensive data: {e}")
            
            # Fallback to basic data
            metrics = self.controller_interface.get_controller_data('metrics') or {}
            scanner = self.controller_interface.get_controller_data('scanner_data') or {}
            external = self.controller_interface.get_controller_data('external_data_status') or {}
            
            return {
                "trading": {
                    "is_running": metrics.get('is_running', False),
                    "total_pnl": metrics.get('total_pnl', 0.0),
                    "daily_pnl": metrics.get('daily_pnl', 0.0),
                    "total_trades": metrics.get('total_trades', 0),
                    "win_rate": metrics.get('win_rate', 0.0),
                    "active_positions": metrics.get('active_positions', 0),
                    "best_trade": metrics.get('best_trade', 0.0)
                },
                "system": {
                    "controller_connected": self.controller_interface.is_controller_available(),
                    "ml_training_completed": metrics.get('ml_training_completed', False),
                    "backtest_completed": metrics.get('comprehensive_backtest_completed', False),
                    "api_rotation_active": metrics.get('api_rotation_active', True)
                },
                "scanner": {
                    "active_pairs": scanner.get('active_pairs', 0),
                    "opportunities": scanner.get('opportunities', 0),
                    "best_opportunity": scanner.get('best_opportunity', 'None'),
                    "confidence": scanner.get('confidence', 0)
                },
                "external_data": {
                    "working_apis": external.get('working_apis', 0),
                    "total_apis": external.get('total_apis', 6),
                    "api_status": external.get('api_status', {})
                },
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Dashboard overview error: {e}")
            return {"error": str(e)}
    
    def _get_trading_metrics(self) -> Dict:
        """Get detailed trading metrics"""
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
                    "best_trade": metrics.get('best_trade', 0.0)
                },
                "positions": {
                    "active": metrics.get('active_positions', 0),
                    "max_allowed": 3  # From config
                },
                "status": {
                    "trading_active": metrics.get('is_running', False),
                    "ml_active": metrics.get('ml_training_completed', False),
                    "backtest_done": metrics.get('comprehensive_backtest_completed', False)
                },
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Trading metrics error: {e}")
            return {"error": str(e)}
    
    def _get_system_status(self) -> Dict:
        """Get system status information"""
        try:
            resources = self.controller_interface.get_controller_data('system_resources') or {}
            
            # Get additional system info
            cpu_usage = psutil.cpu_percent(interval=0.1)
            memory = psutil.virtual_memory()
            
            return {
                "resources": {
                    "cpu_usage": cpu_usage,
                    "memory_usage": memory.percent,
                    "memory_available_gb": memory.available / (1024**3),
                    "disk_usage": psutil.disk_usage('/').percent
                },
                "controller": {
                    "connected": self.controller_interface.is_controller_available(),
                    "api_calls_today": resources.get('api_calls_today', 0),
                    "data_points_processed": resources.get('data_points_processed', 0)
                },
                "middleware": {
                    "cache_size": len(self.cache._cache),
                    "uptime": time.time()
                },
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"System status error: {e}")
            return {"error": str(e)}
    
    def start_real_time_updates(self):
        """Start real-time update thread"""
        if self._update_thread and self._update_thread.is_alive():
            return
        
        self._stop_updates.clear()
        self._update_thread = threading.Thread(target=self._real_time_update_loop)
        self._update_thread.daemon = True
        self._update_thread.start()
        self.logger.info("Real-time updates started")
    
    def _real_time_update_loop(self):
        """Real-time update loop for WebSocket"""
        while not self._stop_updates.wait(2):  # Update every 2 seconds
            try:
                # Get fresh data
                overview = self._get_dashboard_overview()
                metrics = self._get_trading_metrics()
                system_status = self._get_system_status()
                
                # Emit to all connected clients
                self.socketio.emit('dashboard_update', {
                    'overview': overview,
                    'metrics': metrics,
                    'system': system_status,
                    'timestamp': datetime.now().isoformat()
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
        self.logger.info(f"Your dashboard.html will be served at: http://{self.host}:{self.port}")
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
        host = os.getenv('HOST', '127.0.0.1')
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
    host = os.getenv('HOST', '127.0.0.1')
    
    print("Starting V3 API Middleware Service")
    print(f"Dashboard will be available at: http://{host}:{port}")
    print(f"API endpoints available at: http://{host}:{port}")
    
    run_middleware_service(host=host, port=port)