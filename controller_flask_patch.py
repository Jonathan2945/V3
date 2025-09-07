#!/usr/bin/env python3
"""
CONTROLLER FLASK INTEGRATION PATCH
Add these methods to the V3TradingController class
"""


    def run_flask_app(self):
        """
        Run Flask app with proper integration
        This method was empty before - now it properly starts the middleware
        """
        try:
            from api_middleware import APIMiddleware
            
            host = os.getenv('HOST', '0.0.0.0')
            port = int(os.getenv('FLASK_PORT', '8102'))
            
            self.logger.info(f"Starting Flask app on {host}:{port}")
            
            # Create middleware instance
            self.flask_middleware = APIMiddleware(host=host, port=port)
            
            # Register this controller with middleware
            self.flask_middleware.register_controller(self)
            
            # Start the Flask app
            self.flask_middleware.run(debug=False)
            
        except Exception as e:
            self.logger.error(f"Flask app error: {e}")
            raise
    
    def get_trading_status(self) -> Dict:
        """Get current trading status for API"""
        try:
            with self._state_lock:
                return {
                    'is_running': self.is_running,
                    'trades': len(self.recent_trades),
                    'pnl': self.metrics.get('total_pnl', 0.0),
                    'daily_pnl': self.metrics.get('daily_pnl', 0.0),
                    'positions': len(self.open_positions),
                    'win_rate': self.metrics.get('win_rate', 0.0),
                    'timestamp': datetime.now().isoformat()
                }
        except Exception as e:
            self.logger.error(f"Get trading status error: {e}")
            return {'error': str(e)}
    
    def get_system_status(self) -> Dict:
        """Get current system status for API"""
        try:
            return {
                'is_initialized': self.is_initialized,
                'strategies': len(self.top_strategies),
                'ml_strategies': len(self.ml_trained_strategies),
                'backtest_completed': self.metrics.get('comprehensive_backtest_completed', False),
                'ml_training_completed': self.metrics.get('ml_training_completed', False),
                'api_rotation_active': self.metrics.get('api_rotation_active', True),
                'testnet_mode': self.testnet_mode,
                'trading_mode': self.trading_mode,
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.error(f"Get system status error: {e}")
            return {'error': str(e)}
    
    def get_comprehensive_dashboard_data(self) -> Dict:
        """Get comprehensive data for dashboard"""
        try:
            return {
                'overview': {
                    'trading': {
                        'is_running': self.is_running,
                        'total_pnl': self.metrics.get('total_pnl', 0.0),
                        'daily_pnl': self.metrics.get('daily_pnl', 0.0),
                        'total_trades': self.metrics.get('total_trades', 0),
                        'daily_trades': self.metrics.get('daily_trades', 0),
                        'win_rate': self.metrics.get('win_rate', 0.0),
                        'active_positions': len(self.open_positions),
                        'best_trade': self.metrics.get('best_trade', 0.0)
                    },
                    'system': {
                        'controller_connected': True,
                        'is_initialized': self.is_initialized,
                        'ml_training_completed': self.metrics.get('ml_training_completed', False),
                        'backtest_completed': self.metrics.get('comprehensive_backtest_completed', False),
                        'api_rotation_active': self.metrics.get('api_rotation_active', True),
                        'testnet_mode': self.testnet_mode
                    },
                    'scanner': self.scanner_data,
                    'external_data': self.external_data_status,
                    'resources': self.system_resources
                },
                'metrics': self.metrics,
                'positions': self.open_positions,
                'recent_trades': list(self.recent_trades),
                'strategies': self.top_strategies[:10],
                'ml_strategies': self.ml_trained_strategies[:10],
                'backtest_progress': self.backtest_progress,
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.error(f"Get comprehensive dashboard data error: {e}")
            return {'error': str(e)}
    
    async def start_trading(self) -> Dict:
        """Start trading system"""
        try:
            if not self.is_initialized:
                return {'success': False, 'error': 'System not initialized'}
            
            if not self.metrics.get('comprehensive_backtest_completed', False):
                return {'success': False, 'error': 'Complete backtest required before trading'}
            
            if not self.metrics.get('ml_training_completed', False):
                return {'success': False, 'error': 'ML training required before trading'}
            
            with self._state_lock:
                self.is_running = True
            
            self.logger.info("Trading started")
            return {'success': True, 'message': 'Trading started successfully'}
            
        except Exception as e:
            self.logger.error(f"Start trading error: {e}")
            return {'success': False, 'error': str(e)}
    
    async def stop_trading(self) -> Dict:
        """Stop trading system"""
        try:
            with self._state_lock:
                self.is_running = False
            
            self.logger.info("Trading stopped")
            return {'success': True, 'message': 'Trading stopped successfully'}
            
        except Exception as e:
            self.logger.error(f"Stop trading error: {e}")
            return {'success': False, 'error': str(e)}
    
