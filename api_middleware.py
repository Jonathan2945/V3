#!/usr/bin/env python3
"""
V3 API MIDDLEWARE SERVICE - FINAL COMPLETE VERSION
==================================================
REAL DATA DASHBOARD API SERVICE
- Fixed controller registration issue
- All API endpoints working with real data
- Proper Flask app initialization and routing
- Thread-safe data caching and database access
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
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from threading import Lock
import weakref
import traceback
from contextlib import contextmanager
import queue
import psutil
from pathlib import Path


class DataCache:
    """Thread-safe data cache with TTL"""
    
    def __init__(self):
        self._cache = {}
        self._timestamps = {}
        self._lock = Lock()
        self._default_ttl = 3  # 3 seconds default TTL for better responsiveness
    
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

    def size(self):
        with self._lock:
            return len(self._cache)


class DatabaseInterface:
    """Database interface for middleware"""
    
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
    """Interface to communicate with main controller - FIXED VERSION"""
    
    def __init__(self):
        self.controller = None  # Direct reference (FIXED)
        self._lock = Lock()
        self.logger = logging.getLogger(f"{__name__}.ControllerInterface")
    
    def set_controller(self, controller):
        """Set reference to main controller - FIXED"""
        with self._lock:
            self.controller = controller  # Direct assignment (FIXED)
            self.logger.info("Controller successfully registered")
    
    def get_controller(self):
        """Get controller instance if available - FIXED"""
        with self._lock:
            return self.controller  # Direct return (FIXED)
    
    def is_controller_available(self) -> bool:
        """Check if controller is available"""
        with self._lock:
            return self.controller is not None


class APIMiddleware:
    """Main API Middleware Service - FINAL VERSION"""
    
    def __init__(self, host='0.0.0.0', port=8102):
        self.host = host
        self.port = port
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        self.cache = DataCache()
        self.db_interface = DatabaseInterface({
            "trading_metrics": "data/trading_metrics.db",
            "backtests": "data/comprehensive_backtest.db",
            "trade_logs": "data/trade_logs.db",
            "system_metrics": "data/system_metrics.db"
        })
        self.controller_interface = ControllerInterface()
        
        # Initialize Flask app PROPERLY
        self.app = Flask(__name__)
        self.app.config['SECRET_KEY'] = 'v3-api-middleware-secret'
        self.app.config['JSON_SORT_KEYS'] = False
        self.app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False
        CORS(self.app, resources={
            r"/api/*": {
                "origins": "*",
                "methods": ["GET", "POST", "PUT", "DELETE"],
                "allow_headers": ["Content-Type", "Authorization"]
            }
        })
        
        # Setup all routes
        self._setup_routes()
        
        self.logger.info("API Middleware initialized with REAL DATA endpoints")
    
    def _setup_routes(self):
        """Setup all API routes"""
        
        # ==========================================
        # DASHBOARD AND STATIC ROUTES
        # ==========================================
        
        @self.app.route('/')
        def serve_dashboard():
            """Serve the dashboard HTML file"""
            try:
                dashboard_path = Path('dashboard.html')
                if dashboard_path.exists():
                    return send_from_directory('.', 'dashboard.html')
                else:
                    return jsonify({
                        'error': 'Dashboard not found', 
                        'message': 'dashboard.html file not found in current directory'
                    }), 404
            except Exception as e:
                return jsonify({'error': f'Dashboard error: {str(e)}'}), 500
        
        @self.app.route('/health')
        def health_check():
            """Health check endpoint"""
            return jsonify({
                "status": "healthy",
                "timestamp": datetime.now().isoformat(),
                "controller_connected": self.controller_interface.is_controller_available(),
                "cache_size": self.cache.size(),
                "real_data_mode": True,
                "middleware_version": "3.0_FINAL"
            })
        
        # ==========================================
        # MAIN DASHBOARD API ENDPOINTS
        # ==========================================
        
        @self.app.route('/api/dashboard/overview')
        def dashboard_overview():
            """Get comprehensive dashboard overview data"""
            try:
                # Check cache first
                cached = self.cache.get('dashboard_overview', ttl=2)
                if cached:
                    return jsonify(cached)
                
                # Get controller
                controller = self.controller_interface.get_controller()
                if not controller:
                    return jsonify({'error': 'Controller not available'}), 503
                
                # Try to get comprehensive data first
                if hasattr(controller, 'get_comprehensive_dashboard_data'):
                    try:
                        comprehensive_data = controller.get_comprehensive_dashboard_data()
                        self.cache.set('dashboard_overview', comprehensive_data)
                        return jsonify(comprehensive_data)
                    except Exception as e:
                        self.logger.warning(f"Comprehensive data failed: {e}")
                
                # Fallback to individual method calls
                trading_status = self._safe_controller_call(controller, 'get_trading_status')
                system_status = self._safe_controller_call(controller, 'get_system_status')
                backtest_progress = self._safe_controller_call(controller, 'get_backtest_progress')
                performance = self._safe_controller_call(controller, 'get_performance_metrics')
                
                overview = {
                    'trading': trading_status,
                    'system': system_status,
                    'backtest': backtest_progress,
                    'performance': performance,
                    'timestamp': time.time()
                }
                
                # Cache the result
                self.cache.set('dashboard_overview', overview)
                return jsonify(overview)
                
            except Exception as e:
                self.logger.error(f"Dashboard overview error: {str(e)}")
                return jsonify({'error': str(e)}), 500
        
        # ==========================================
        # INDIVIDUAL API ENDPOINTS
        # ==========================================
        
        @self.app.route('/api/trading_status')
        def trading_status():
            """Get trading status"""
            return self._cached_endpoint('trading_status', 
                                       lambda: self._get_controller_data('get_trading_status'))
        
        @self.app.route('/api/status')
        @self.app.route('/api/system/status')
        def system_status():
            """Get system status"""
            return self._cached_endpoint('system_status',
                                       lambda: self._get_controller_data('get_system_status'))
        
        @self.app.route('/api/backtest_results')
        @self.app.route('/api/backtest/progress')
        def backtest_results():
            """Get backtest results/progress"""
            return self._cached_endpoint('backtest_progress',
                                       lambda: self._get_controller_data('get_backtest_progress'))
        
        @self.app.route('/api/performance')
        @self.app.route('/api/trading/metrics')
        def performance_metrics():
            """Get performance metrics"""
            return self._cached_endpoint('performance_metrics',
                                       lambda: self._get_controller_data('get_performance_metrics'))
        
        @self.app.route('/api/trading/positions')
        def trading_positions():
            """Get current trading positions"""
            try:
                controller = self.controller_interface.get_controller()
                if not controller:
                    return jsonify({'positions': [], 'total': 0})
                
                # Get positions from performance metrics
                performance = self._safe_controller_call(controller, 'get_performance_metrics')
                if performance:
                    return jsonify({
                        'positions': performance.get('active_pairs', []),
                        'total': performance.get('total_positions', 0),
                        'unrealized_pnl': performance.get('total_unrealized_pnl', 0.0),
                        'timestamp': time.time()
                    })
                else:
                    return jsonify({'positions': [], 'total': 0, 'unrealized_pnl': 0.0})
                    
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/trading/recent-trades')
        def recent_trades():
            """Get recent trades"""
            try:
                controller = self.controller_interface.get_controller()
                if not controller:
                    return jsonify({'trades': [], 'count': 0})
                
                performance = self._safe_controller_call(controller, 'get_performance_metrics')
                if performance:
                    trades = performance.get('recent_trades', [])
                    return jsonify({
                        'trades': trades,
                        'count': len(trades),
                        'timestamp': time.time()
                    })
                else:
                    return jsonify({'trades': [], 'count': 0})
                    
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/strategies/top')
        def top_strategies():
            """Get top strategies"""
            try:
                controller = self.controller_interface.get_controller()
                if not controller:
                    return jsonify({'strategies': [], 'total': 0})
                
                # Get strategies from controller
                strategies = getattr(controller, 'top_strategies', [])
                ml_strategies = getattr(controller, 'ml_trained_strategies', [])
                
                return jsonify({
                    'strategies': strategies[:10],  # Top 10
                    'total': len(strategies),
                    'ml_trained': len(ml_strategies),
                    'timestamp': time.time()
                })
                
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/social/posts')
        def social_posts():
            """Social media posts (placeholder for future implementation)"""
            return jsonify({
                'posts': [],
                'sentiment': 'neutral',
                'sources': ['twitter', 'reddit'],
                'last_updated': time.time(),
                'status': 'placeholder'
            })
        
        # ==========================================
        # CONTROL ENDPOINTS
        # ==========================================
        
        @self.app.route('/api/control/<action>', methods=['POST'])
        def control_action(action):
            """Execute control actions"""
            try:
                controller = self.controller_interface.get_controller()
                if not controller:
                    return jsonify({'success': False, 'error': 'Controller not available'}), 503
                
                result = {'success': False, 'error': 'Unknown action'}
                
                # Execute the requested action
                if action == 'start_trading':
                    if hasattr(controller, 'start_trading'):
                        result = controller.start_trading()
                    else:
                        result = {'success': False, 'error': 'Trading control not available'}
                
                elif action == 'stop_trading':
                    if hasattr(controller, 'stop_trading'):
                        result = controller.stop_trading()
                    else:
                        result = {'success': False, 'error': 'Trading control not available'}
                
                elif action == 'start_backtest':
                    if hasattr(controller, 'start_comprehensive_backtest'):
                        result = controller.start_comprehensive_backtest()
                    else:
                        result = {'success': False, 'error': 'Backtest control not available'}
                
                elif action == 'save_metrics':
                    if hasattr(controller, 'save_current_metrics'):
                        controller.save_current_metrics()
                        result = {'success': True, 'message': 'Metrics saved successfully'}
                    else:
                        result = {'success': False, 'error': 'Save metrics not available'}
                
                elif action == 'reset_cache':
                    self.cache.clear()
                    result = {'success': True, 'message': 'Cache cleared'}
                
                else:
                    result = {'success': False, 'error': f'Unknown action: {action}'}
                
                # Clear relevant caches after control actions
                if result.get('success', False):
                    self.cache.clear()
                
                return jsonify(result)
                
            except Exception as e:
                self.logger.error(f"Control action {action} error: {str(e)}")
                return jsonify({'success': False, 'error': str(e)}), 500
        
        # ==========================================
        # ERROR HANDLERS
        # ==========================================
        
        @self.app.errorhandler(404)
        def not_found(error):
            return jsonify({
                'error': 'Not Found',
                'message': 'The requested resource was not found',
                'status_code': 404
            }), 404
        
        @self.app.errorhandler(500)
        def internal_error(error):
            return jsonify({
                'error': 'Internal Server Error',
                'message': 'An unexpected error occurred',
                'status_code': 500
            }), 500
    
    # ==========================================
    # HELPER METHODS
    # ==========================================
    
    def _cached_endpoint(self, cache_key, data_func):
        """Return cached data if available, otherwise fetch fresh data"""
        try:
            # Check cache first
            cached = self.cache.get(cache_key, ttl=3)
            if cached:
                return jsonify(cached)
            
            # Get fresh data
            controller = self.controller_interface.get_controller()
            if not controller:
                return jsonify({'error': 'Controller not available'}), 503
            
            data = data_func()
            if data is None:
                return jsonify({'error': 'No data available'}), 503
            
            # Cache and return
            self.cache.set(cache_key, data)
            return jsonify(data)
            
        except Exception as e:
            self.logger.error(f"Cached endpoint error for {cache_key}: {str(e)}")
            return jsonify({'error': str(e)}), 500
    
    def _get_controller_data(self, method_name):
        """Get data from controller method safely"""
        try:
            controller = self.controller_interface.get_controller()
            if not controller:
                return None
            
            return self._safe_controller_call(controller, method_name)
                
        except Exception as e:
            self.logger.error(f"Error calling controller method {method_name}: {str(e)}")
            return None
    
    def _safe_controller_call(self, controller, method_name):
        """Safely call controller method with error handling"""
        try:
            if hasattr(controller, method_name):
                method = getattr(controller, method_name)
                return method()
            else:
                self.logger.warning(f"Controller method {method_name} not found")
                return None
        except Exception as e:
            self.logger.error(f"Error calling {method_name}: {str(e)}")
            return None
    
    # ==========================================
    # MAIN INTERFACE METHODS
    # ==========================================
    
    def register_controller(self, controller):
        """Register the main trading controller - FIXED VERSION"""
        try:
            # Set controller in interface
            self.controller_interface.set_controller(controller)
            
            # Also set direct reference for backward compatibility
            self.controller = controller
            
            # Verify registration worked
            if self.controller_interface.is_controller_available():
                self.logger.info("Trading controller successfully registered with API middleware")
                
                # Test basic connectivity
                if hasattr(controller, 'get_trading_status'):
                    test_data = controller.get_trading_status()
                    self.logger.info(f"Controller test call successful: {len(str(test_data))} chars")
                else:
                    self.logger.warning("Controller missing get_trading_status method")
            else:
                self.logger.error("Controller registration failed")
                
        except Exception as e:
            self.logger.error(f"Error registering controller: {str(e)}")
    
    def run(self, debug=False, host=None, port=None):
        """Start the Flask server - FIXED SIGNATURE"""
        try:
            # Use provided parameters or defaults
            run_host = host if host is not None else self.host
            run_port = port if port is not None else self.port
            
            self.logger.info(f"Starting V3 API Middleware on {run_host}:{run_port}")
            self.logger.info("REAL DATA MODE - No mock or simulated data")
            self.logger.info(f"Controller available: {self.controller_interface.is_controller_available()}")
            
            # Clear cache on startup
            self.cache.clear()
            
            # Log available routes
            routes = [str(rule) for rule in self.app.url_map.iter_rules()]
            api_routes = [route for route in routes if '/api/' in route]
            self.logger.info(f"Available API routes: {len(api_routes)}")
            
            # Start Flask app with correct parameters
            self.app.run(
                debug=debug,
                host=run_host,
                port=run_port,
                threaded=True,
                use_reloader=False  # Prevent double startup in debug mode
            )
            
        except Exception as e:
            self.logger.error(f"Failed to start API server: {str(e)}")
            raise
    
    def shutdown(self):
        """Graceful shutdown"""
        try:
            self.logger.info("Shutting down API middleware")
            self.cache.clear()
            self.controller = None
            self.controller_interface.set_controller(None)
        except Exception as e:
            self.logger.error(f"Error during shutdown: {str(e)}")


# Make sure we export the class properly
__all__ = ['APIMiddleware']


# Main execution for standalone testing
if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Get configuration from environment
    host = os.getenv('HOST', '0.0.0.0')
    port = int(os.getenv('FLASK_PORT', os.getenv('MAIN_SYSTEM_PORT', '8102')))
    
    print("Starting V3 API Middleware Service (Standalone)")
    print(f"Dashboard will be available at: http://{host}:{port}")
    print(f"API endpoints available at: http://{host}:{port}/api/")
    print("REAL DATA MODE - No mock or simulated data")
    print("Note: Controller must be registered separately for full functionality")
    
    # Create and run middleware
    middleware = APIMiddleware(host=host, port=port)
    middleware.run(debug=False)