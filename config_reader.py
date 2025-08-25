#!/usr/bin/env python3
"""
ENHANCED CONFIGURATION READER - CENTRALIZED .env MANAGEMENT
===========================================================
Enhanced configuration reader that ensures all components use .env settings
This ensures the algorithm pulls all information from the .env file with API rotation support
"""

import os
from typing import Dict, Any, Optional, List, Tuple
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

class EnhancedConfigReader:
    """Enhanced configuration reader for all system components with API rotation support"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._config_cache = {}
        self._api_rotation_cache = {}
        self._load_all_config()
        self._load_api_rotation_config()
        
        self.logger.info("[CONFIG] Enhanced configuration reader initialized")
    
    def _load_all_config(self):
        """Load all configuration from .env file including new features"""
        self._config_cache = {
            # Port Configuration
            'port': int(os.getenv('MAIN_SYSTEM_PORT', os.getenv('FLASK_PORT', '8102'))),
            'flask_port': int(os.getenv('FLASK_PORT', '8102')),
            'main_system_port': int(os.getenv('MAIN_SYSTEM_PORT', '8102')),
            'host': os.getenv('HOST', '0.0.0.0'),
            
            # Trading Mode Configuration
            'trading_mode': os.getenv('DEFAULT_TRADING_MODE', 'PAPER_TRADING'),
            'testnet_enabled': os.getenv('TESTNET', 'true').lower() == 'true',
            'live_trading_enabled': os.getenv('LIVE_TRADING_ENABLED', 'false').lower() == 'true',
            'allow_live_trading': os.getenv('ALLOW_LIVE_TRADING', 'true').lower() == 'true',
            
            # API Rotation Configuration
            'api_rotation_enabled': os.getenv('API_ROTATION_ENABLED', 'true').lower() == 'true',
            'api_rotation_strategy': os.getenv('API_ROTATION_STRATEGY', 'ROUND_ROBIN'),
            'api_rate_limit_threshold': int(os.getenv('API_RATE_LIMIT_THRESHOLD', '80')),
            'api_cooldown_period': int(os.getenv('API_COOLDOWN_PERIOD', '300')),
            'api_health_check_interval': int(os.getenv('API_HEALTH_CHECK_INTERVAL', '60')),
            
            # Dynamic Trading Configuration
            'trade_mode': os.getenv('TRADE_MODE', 'MINIMUM'),
            'fixed_trade_amount': float(os.getenv('FIXED_TRADE_AMOUNT', '50.0')),
            'min_trade_amount': float(os.getenv('MIN_TRADE_AMOUNT', '10.0')),
            'dynamic_position_sizing': os.getenv('DYNAMIC_POSITION_SIZING', 'true').lower() == 'true',
            
            # Multi-Pair Trading Configuration
            'enable_all_pairs': os.getenv('ENABLE_ALL_PAIRS', 'true').lower() == 'true',
            'excluded_pairs': [pair.strip() for pair in os.getenv('EXCLUDED_PAIRS', '').split(',') if pair.strip()],
            'min_volume_24h': float(os.getenv('MIN_VOLUME_24H', '100000')),
            'max_concurrent_pairs': int(os.getenv('MAX_CONCURRENT_PAIRS', '50')),
            'min_price_change_24h': float(os.getenv('MIN_PRICE_CHANGE_24H', '0.5')),
            'max_spread_percent': float(os.getenv('MAX_SPREAD_PERCENT', '0.5')),
            
            # Multi-Timeframe Analysis
            'timeframes': [tf.strip() for tf in os.getenv('TIMEFRAMES', '1m,5m,15m,30m,1h,4h,1d').split(',') if tf.strip()],
            'primary_timeframe': os.getenv('PRIMARY_TIMEFRAME', '15m'),
            'confirm_timeframes': [tf.strip() for tf in os.getenv('CONFIRM_TIMEFRAMES', '5m,1h').split(',') if tf.strip()],
            'multi_timeframe_confirmation': os.getenv('MULTI_TIMEFRAME_CONFIRMATION', 'true').lower() == 'true',
            'min_timeframe_agreement': int(os.getenv('MIN_TIMEFRAME_AGREEMENT', '2')),
            
            # Exchange Info Management
            'update_exchange_info_interval': int(os.getenv('UPDATE_EXCHANGE_INFO_INTERVAL', '3600')),
            'cache_exchange_info': os.getenv('CACHE_EXCHANGE_INFO', 'true').lower() == 'true',
            'exchange_info_backup_file': os.getenv('EXCHANGE_INFO_BACKUP_FILE', 'data/exchange_info_backup.json'),
            'auto_update_trading_rules': os.getenv('AUTO_UPDATE_TRADING_RULES', 'true').lower() == 'true',
            
            # Enhanced Risk Management
            'max_position_percent': float(os.getenv('MAX_POSITION_PERCENT', '2.0')),
            'max_position_size': float(os.getenv('MAX_POSITION_SIZE', '100.0')),
            'min_position_size': float(os.getenv('MIN_POSITION_SIZE', '10.0')),
            'max_daily_loss': float(os.getenv('MAX_DAILY_LOSS', '500.0')),
            'max_total_positions': int(os.getenv('MAX_TOTAL_POSITIONS', os.getenv('MAX_POSITIONS', '3'))),
            'max_positions_per_pair': int(os.getenv('MAX_POSITIONS_PER_PAIR', '1')),
            'min_confidence': float(os.getenv('MIN_CONFIDENCE', '70.0')),
            
            # Pair-Specific Risk Limits
            'btc_max_position_percent': float(os.getenv('BTC_MAX_POSITION_PERCENT', '3.0')),
            'eth_max_position_percent': float(os.getenv('ETH_MAX_POSITION_PERCENT', '2.5')),
            'altcoin_max_position_percent': float(os.getenv('ALTCOIN_MAX_POSITION_PERCENT', '1.5')),
            'risk_scaling_by_volatility': os.getenv('RISK_SCALING_BY_VOLATILITY', 'true').lower() == 'true',
            
            # Market Opportunity Detection
            'opportunity_scanner_enabled': os.getenv('OPPORTUNITY_SCANNER_ENABLED', 'true').lower() == 'true',
            'scan_interval_seconds': int(os.getenv('SCAN_INTERVAL_SECONDS', '30')),
            'min_opportunity_score': float(os.getenv('MIN_OPPORTUNITY_SCORE', '75.0')),
            'volume_spike_threshold': float(os.getenv('VOLUME_SPIKE_THRESHOLD', '2.0')),
            'price_momentum_threshold': float(os.getenv('PRICE_MOMENTUM_THRESHOLD', '1.5')),
            'news_sentiment_weight': float(os.getenv('NEWS_SENTIMENT_WEIGHT', '0.3')),
            
            # Portfolio Diversification
            'max_correlation_threshold': float(os.getenv('MAX_CORRELATION_THRESHOLD', '0.7')),
            'diversification_enabled': os.getenv('DIVERSIFICATION_ENABLED', 'true').lower() == 'true',
            'max_exposure_per_sector': float(os.getenv('MAX_EXPOSURE_PER_SECTOR', '30.0')),
            'rebalancing_enabled': os.getenv('REBALANCING_ENABLED', 'false').lower() == 'true',
            
            # Position and Risk Management
            'max_positions': int(os.getenv('MAX_POSITIONS', os.getenv('MAX_TOTAL_POSITIONS', '3'))),
            'trade_amount_usdt': float(os.getenv('TRADE_AMOUNT_USDT', '25.0')),
            'starting_balance': float(os.getenv('STARTING_BALANCE', '50.0')),
            
            # Safety Settings
            'max_consecutive_losses': int(os.getenv('MAX_CONSECUTIVE_LOSSES', '3')),
            'emergency_stop_loss_pct': float(os.getenv('EMERGENCY_STOP_LOSS_PCT', '10.0')),
            'max_trades_per_hour': int(os.getenv('MAX_TRADES_PER_HOUR', '2')),
            'daily_trade_limit': int(os.getenv('DAILY_TRADE_LIMIT', '10')),
            'enable_kill_switch': os.getenv('ENABLE_KILL_SWITCH', 'true').lower() == 'true',
            'cooling_off_period_minutes': int(os.getenv('COOLING_OFF_PERIOD_MINUTES', '60')),
            'require_manual_restart': os.getenv('REQUIRE_MANUAL_RESTART', 'true').lower() == 'true',
            'enable_circuit_breaker': os.getenv('ENABLE_CIRCUIT_BREAKER', 'true').lower() == 'true',
            
            # Trade Execution Settings
            'log_all_trades': os.getenv('LOG_ALL_TRADES', 'true').lower() == 'true',
            'real_time_monitoring': os.getenv('REAL_TIME_MONITORING', 'true').lower() == 'true',
            'auto_close_positions': os.getenv('AUTO_CLOSE_POSITIONS', 'true').lower() == 'true',
            'max_hold_time_hours': int(os.getenv('MAX_HOLD_TIME_HOURS', '24')),
            'live_max_hold_time_hours': int(os.getenv('LIVE_MAX_HOLD_TIME_HOURS', '4')),
            'slippage_tolerance': float(os.getenv('SLIPPAGE_TOLERANCE', '0.001')),
            'order_timeout_seconds': int(os.getenv('ORDER_TIMEOUT_SECONDS', '30')),
            
            # System Performance
            'max_concurrent_api_calls': int(os.getenv('MAX_CONCURRENT_API_CALLS', '10')),
            'api_call_delay_ms': int(os.getenv('API_CALL_DELAY_MS', '100')),
            'enable_request_caching': os.getenv('ENABLE_REQUEST_CACHING', 'true').lower() == 'true',
            'cache_duration_seconds': int(os.getenv('CACHE_DURATION_SECONDS', '300')),
            'background_processing_enabled': os.getenv('BACKGROUND_PROCESSING_ENABLED', 'true').lower() == 'true',
            'max_retry_attempts': int(os.getenv('MAX_RETRY_ATTEMPTS', '3')),
            
            # Paper Trading Configuration
            'paper_use_live_prices': os.getenv('PAPER_USE_LIVE_PRICES', 'true').lower() == 'true',
            'paper_enable_slippage': os.getenv('PAPER_ENABLE_SLIPPAGE', 'true').lower() == 'true',
            'paper_commission_rate': float(os.getenv('PAPER_COMMISSION_RATE', '0.001')),
            'paper_track_spreads': os.getenv('PAPER_TRACK_SPREADS', 'true').lower() == 'true',
            'paper_trading_max_positions': int(os.getenv('PAPER_TRADING_MAX_POSITIONS', '5')),
            
            # Live Trading Configuration
            'live_order_type': os.getenv('LIVE_ORDER_TYPE', 'MARKET'),
            'live_enable_stop_loss': os.getenv('LIVE_ENABLE_STOP_LOSS', 'true').lower() == 'true',
            'live_enable_take_profit': os.getenv('LIVE_ENABLE_TAKE_PROFIT', 'true').lower() == 'true',
            'live_verify_balance': os.getenv('LIVE_VERIFY_BALANCE', 'true').lower() == 'true',
            'live_double_check_orders': os.getenv('LIVE_DOUBLE_CHECK_ORDERS', 'true').lower() == 'true',
            'live_trading_max_positions': int(os.getenv('LIVE_TRADING_MAX_POSITIONS', '3')),
            
            # Database Configuration
            'db_path': os.getenv('DB_PATH', 'data/trade_logs.db'),
            'backup_trades_daily': os.getenv('BACKUP_TRADES_DAILY', 'true').lower() == 'true',
            'cleanup_old_trades': os.getenv('CLEANUP_OLD_TRADES', 'true').lower() == 'true',
            'keep_trade_history_days': int(os.getenv('KEEP_TRADE_HISTORY_DAYS', '90')),
            'enable_database_optimization': os.getenv('ENABLE_DATABASE_OPTIMIZATION', 'true').lower() == 'true',
            'database_vacuum_interval_hours': int(os.getenv('DATABASE_VACUUM_INTERVAL_HOURS', '24')),
            
            # Logging and Monitoring
            'log_level': os.getenv('LOG_LEVEL', 'INFO'),
            'log_trades_to_file': os.getenv('LOG_TRADES_TO_FILE', 'true').lower() == 'true',
            'log_performance_metrics': os.getenv('LOG_PERFORMANCE_METRICS', 'true').lower() == 'true',
            'log_position_updates': os.getenv('LOG_POSITION_UPDATES', 'true').lower() == 'true',
            'log_real_time_pnl': os.getenv('LOG_REAL_TIME_PNL', 'true').lower() == 'true',
            'log_api_calls': os.getenv('LOG_API_CALLS', 'true').lower() == 'true',
            'log_market_data': os.getenv('LOG_MARKET_DATA', 'true').lower() == 'true',
            
            # Notification Settings
            'notify_on_trade_execution': os.getenv('NOTIFY_ON_TRADE_EXECUTION', 'true').lower() == 'true',
            'notify_on_stop_loss': os.getenv('NOTIFY_ON_STOP_LOSS', 'true').lower() == 'true',
            'notify_on_take_profit': os.getenv('NOTIFY_ON_TAKE_PROFIT', 'true').lower() == 'true',
            'notify_on_system_errors': os.getenv('NOTIFY_ON_SYSTEM_ERRORS', 'true').lower() == 'true',
            'notify_on_new_opportunities': os.getenv('NOTIFY_ON_NEW_OPPORTUNITIES', 'true').lower() == 'true',
            'notify_on_api_rotation': os.getenv('NOTIFY_ON_API_ROTATION', 'false').lower() == 'true',
            
            # Advanced Features
            'enable_machine_learning': os.getenv('ENABLE_MACHINE_LEARNING', 'true').lower() == 'true',
            'ml_prediction_weight': float(os.getenv('ML_PREDICTION_WEIGHT', '0.2')),
            'enable_sentiment_analysis': os.getenv('ENABLE_SENTIMENT_ANALYSIS', 'true').lower() == 'true',
            'sentiment_analysis_weight': float(os.getenv('SENTIMENT_ANALYSIS_WEIGHT', '0.15')),
            'enable_technical_analysis': os.getenv('ENABLE_TECHNICAL_ANALYSIS', 'true').lower() == 'true',
            'technical_analysis_weight': float(os.getenv('TECHNICAL_ANALYSIS_WEIGHT', '0.65')),
            
            # Monitoring and Notifications
            'position_monitoring_enabled': os.getenv('POSITION_MONITORING_ENABLED', 'true').lower() == 'true',
            'monitor_positions_interval': int(os.getenv('MONITOR_POSITIONS_INTERVAL', '30')),
            
            # Auto-start Configuration
            'auto_start_trading': os.getenv('AUTO_START_TRADING', 'false').lower() == 'true',
            'auto_start_mode': os.getenv('AUTO_START_MODE', 'PAPER_TRADING'),
            'require_web_confirmation': os.getenv('REQUIRE_WEB_CONFIRMATION', 'true').lower() == 'true',
            
            # WebSocket and API Configuration
            'enable_api_auth': os.getenv('ENABLE_API_AUTH', 'true').lower() == 'true',
            'enable_rate_limiting': os.getenv('ENABLE_RATE_LIMITING', 'true').lower() == 'true',
            'api_secret_key': os.getenv('API_SECRET_KEY', 'enhanced-trading-api-key-change-in-production'),
            'jwt_secret_key': os.getenv('JWT_SECRET_KEY', 'enhanced-trading-jwt-secret-key-change-in-production'),
            'broadcast_interval': int(os.getenv('BROADCAST_INTERVAL', '5')),
            'websocket_ping_timeout': int(os.getenv('WEBSOCKET_PING_TIMEOUT', '60')),
            'websocket_ping_interval': int(os.getenv('WEBSOCKET_PING_INTERVAL', '25')),
            'max_api_requests_per_hour': int(os.getenv('MAX_API_REQUESTS_PER_HOUR', '1000')),
            
            # System Settings
            'debug_mode': os.getenv('DEBUG', 'false').lower() == 'true',
            'verbose_logging': os.getenv('VERBOSE_LOGGING', 'false').lower() == 'true',
            
            # Version Tracking
            'system_version': os.getenv('SYSTEM_VERSION', '7.0_MULTI_PAIR_API_ROTATION'),
            'config_version': os.getenv('CONFIG_VERSION', '3.0'),
        }
    
    def _load_api_rotation_config(self):
        """Load API rotation specific configuration"""
        self._api_rotation_cache = {
            # Alpha Vantage Keys
            'alpha_vantage_keys': self._get_api_keys('ALPHA_VANTAGE_API_KEY'),
            
            # News API Keys
            'news_api_keys': self._get_api_keys('NEWS_API_KEY'),
            
            # FRED API Keys
            'fred_api_keys': self._get_api_keys('FRED_API_KEY'),
            
            # Twitter API Keys
            'twitter_keys': self._get_api_keys('TWITTER_BEARER_TOKEN'),
            
            # Reddit API Keys (pairs)
            'reddit_keys': self._get_reddit_key_pairs(),
            
            # Binance API Keys (pairs)
            'binance_keys': self._get_binance_key_pairs('BINANCE_API_KEY', 'BINANCE_API_SECRET'),
            
            # Binance Live API Keys (pairs)
            'binance_live_keys': self._get_binance_key_pairs('BINANCE_LIVE_API_KEY', 'BINANCE_LIVE_API_SECRET'),
        }
        
        # Log available API keys (without showing actual keys)
        for service, keys in self._api_rotation_cache.items():
            if keys:
                self.logger.info(f"[CONFIG] Found {len(keys)} valid {service}")
    
    def _get_api_keys(self, base_name: str) -> List[str]:
        """Get valid API keys for a service (numbered 1-3)"""
        valid_keys = []
        
        # Check numbered keys (1-3)
        for i in range(1, 4):
            key = os.getenv(f'{base_name}_{i}', '').strip()
            if key and key != '' and key != 'your_key_here':
                valid_keys.append(key)
        
        # Fallback to legacy single key if no numbered keys
        if not valid_keys:
            legacy_key = os.getenv(base_name, '').strip()
            if legacy_key and legacy_key != '' and legacy_key != 'your_key_here':
                valid_keys.append(legacy_key)
        
        return valid_keys
    
    def _get_reddit_key_pairs(self) -> List[Dict[str, str]]:
        """Get valid Reddit API key pairs"""
        valid_pairs = []
        
        for i in range(1, 4):
            client_id = os.getenv(f'REDDIT_CLIENT_ID_{i}', '').strip()
            client_secret = os.getenv(f'REDDIT_CLIENT_SECRET_{i}', '').strip()
            
            if (client_id and client_secret and 
                client_id != '' and client_secret != '' and
                client_id != 'your_client_id' and client_secret != 'your_client_secret'):
                valid_pairs.append({
                    'client_id': client_id,
                    'client_secret': client_secret,
                    'key_id': f'reddit_{i}'
                })
        
        return valid_pairs
    
    def _get_binance_key_pairs(self, key_base: str, secret_base: str) -> List[Dict[str, str]]:
        """Get valid Binance API key pairs"""
        valid_pairs = []
        
        for i in range(1, 4):
            api_key = os.getenv(f'{key_base}_{i}', '').strip()
            api_secret = os.getenv(f'{secret_base}_{i}', '').strip()
            
            if (api_key and api_secret and 
                api_key != '' and api_secret != '' and
                api_key != 'your_api_key' and api_secret != 'your_api_secret'):
                valid_pairs.append({
                    'api_key': api_key,
                    'api_secret': api_secret,
                    'key_id': f'binance_{i}' if 'LIVE' not in key_base else f'binance_live_{i}'
                })
        
        return valid_pairs
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by key"""
        return self._config_cache.get(key, default)
    
    def get_all(self) -> Dict[str, Any]:
        """Get all configuration"""
        return self._config_cache.copy()
    
    def get_api_rotation_config(self) -> Dict[str, Any]:
        """Get API rotation configuration"""
        return self._api_rotation_cache.copy()
    
    def get_trading_config(self) -> Dict[str, Any]:
        """Get trading-specific configuration"""
        trading_keys = [
            'trading_mode', 'testnet_enabled', 'live_trading_enabled',
            'trade_mode', 'fixed_trade_amount', 'min_trade_amount',
            'max_positions', 'max_daily_loss', 'max_position_size',
            'min_confidence', 'trade_amount_usdt', 'starting_balance',
            'max_consecutive_losses', 'emergency_stop_loss_pct',
            'enable_kill_switch', 'paper_use_live_prices',
            'live_enable_stop_loss', 'live_enable_take_profit',
            'enable_all_pairs', 'excluded_pairs', 'max_concurrent_pairs',
            'dynamic_position_sizing', 'min_volume_24h'
        ]
        return {key: self._config_cache.get(key) for key in trading_keys}
    
    def get_multi_pair_config(self) -> Dict[str, Any]:
        """Get multi-pair trading configuration"""
        multi_pair_keys = [
            'enable_all_pairs', 'excluded_pairs', 'min_volume_24h',
            'max_concurrent_pairs', 'min_price_change_24h', 'max_spread_percent',
            'opportunity_scanner_enabled', 'scan_interval_seconds',
            'min_opportunity_score', 'volume_spike_threshold', 'price_momentum_threshold'
        ]
        return {key: self._config_cache.get(key) for key in multi_pair_keys}
    
    def get_multi_timeframe_config(self) -> Dict[str, Any]:
        """Get multi-timeframe analysis configuration"""
        timeframe_keys = [
            'timeframes', 'primary_timeframe', 'confirm_timeframes',
            'multi_timeframe_confirmation', 'min_timeframe_agreement'
        ]
        return {key: self._config_cache.get(key) for key in timeframe_keys}
    
    def get_api_config(self) -> Dict[str, Any]:
        """Get API configuration including rotation settings"""
        api_keys = [
            'api_rotation_enabled', 'api_rotation_strategy',
            'api_rate_limit_threshold', 'api_cooldown_period',
            'api_health_check_interval', 'max_concurrent_api_calls',
            'enable_request_caching', 'cache_duration_seconds'
        ]
        config = {key: self._config_cache.get(key) for key in api_keys}
        config.update(self._api_rotation_cache)
        return config
    
    def get_system_config(self) -> Dict[str, Any]:
        """Get system configuration"""
        system_keys = [
            'port', 'flask_port', 'host', 'debug_mode', 'log_level',
            'enable_api_auth', 'enable_rate_limiting', 'broadcast_interval',
            'websocket_ping_timeout', 'websocket_ping_interval',
            'background_processing_enabled', 'max_retry_attempts'
        ]
        return {key: self._config_cache.get(key) for key in system_keys}
    
    def get_risk_management_config(self) -> Dict[str, Any]:
        """Get risk management configuration"""
        risk_keys = [
            'max_position_percent', 'max_position_size', 'min_position_size',
            'max_daily_loss', 'max_consecutive_losses', 'emergency_stop_loss_pct',
            'enable_kill_switch', 'cooling_off_period_minutes', 'enable_circuit_breaker',
            'max_correlation_threshold', 'diversification_enabled',
            'btc_max_position_percent', 'eth_max_position_percent',
            'altcoin_max_position_percent', 'risk_scaling_by_volatility'
        ]
        return {key: self._config_cache.get(key) for key in risk_keys}
    
    def get_performance_config(self) -> Dict[str, Any]:
        """Get performance and optimization configuration"""
        performance_keys = [
            'max_concurrent_api_calls', 'api_call_delay_ms',
            'enable_request_caching', 'cache_duration_seconds',
            'background_processing_enabled', 'max_retry_attempts',
            'update_exchange_info_interval', 'cache_exchange_info'
        ]
        return {key: self._config_cache.get(key) for key in performance_keys}
    
    def is_testnet_mode(self) -> bool:
        """Check if system is in testnet mode"""
        return self.get('testnet_enabled', True)
    
    def is_live_trading_enabled(self) -> bool:
        """Check if live trading is enabled"""
        return self.get('live_trading_enabled', False)
    
    def is_paper_trading_mode(self) -> bool:
        """Check if system is in paper trading mode"""
        return self.get('trading_mode', 'PAPER_TRADING') == 'PAPER_TRADING'
    
    def is_api_rotation_enabled(self) -> bool:
        """Check if API rotation is enabled"""
        return self.get('api_rotation_enabled', True)
    
    def is_multi_pair_enabled(self) -> bool:
        """Check if multi-pair trading is enabled"""
        return self.get('enable_all_pairs', True)
    
    def is_opportunity_scanner_enabled(self) -> bool:
        """Check if opportunity scanner is enabled"""
        return self.get('opportunity_scanner_enabled', True)
    
    def get_valid_api_services(self) -> List[str]:
        """Get list of API services with valid keys"""
        services = []
        for service, keys in self._api_rotation_cache.items():
            if keys:  # Has valid keys
                services.append(service.replace('_keys', ''))
        return services
    
    def get_api_key_count(self, service: str) -> int:
        """Get number of valid API keys for a service"""
        service_key = f"{service}_keys"
        keys = self._api_rotation_cache.get(service_key, [])
        return len(keys)
    
    def reload_config(self):
        """Reload configuration from .env file"""
        load_dotenv(override=True)
        self._load_all_config()
        self._load_api_rotation_config()
        self.logger.info("[CONFIG] Configuration reloaded")
    
    def validate_configuration(self) -> Tuple[bool, List[str]]:
        """Validate configuration and return issues"""
        issues = []
        
        # Check critical settings
        if not self.get('starting_balance') or self.get('starting_balance') <= 0:
            issues.append("STARTING_BALANCE must be > 0")
        
        if self.get('max_positions', 0) <= 0:
            issues.append("MAX_POSITIONS must be > 0")
        
        if self.get('min_confidence', 0) <= 0 or self.get('min_confidence', 0) > 100:
            issues.append("MIN_CONFIDENCE must be between 0 and 100")
        
        # Check API keys availability
        if self.is_api_rotation_enabled():
            valid_services = self.get_valid_api_services()
            if not valid_services:
                issues.append("API rotation enabled but no valid API keys found")
        
        # Check multi-pair settings
        if self.is_multi_pair_enabled():
            if self.get('max_concurrent_pairs', 0) <= 0:
                issues.append("MAX_CONCURRENT_PAIRS must be > 0 when multi-pair trading is enabled")
        
        # Check timeframe settings
        timeframes = self.get('timeframes', [])
        primary_tf = self.get('primary_timeframe', '')
        if primary_tf and primary_tf not in timeframes:
            issues.append(f"PRIMARY_TIMEFRAME '{primary_tf}' not in TIMEFRAMES list")
        
        # Check live trading prerequisites
        if self.is_live_trading_enabled():
            if not self.get_api_key_count('binance_live'):
                issues.append("Live trading enabled but no valid Binance Live API keys")
        
        return len(issues) == 0, issues
    
    def get_config_summary(self) -> Dict[str, Any]:
        """Get configuration summary for display"""
        return {
            'system_version': self.get('system_version'),
            'config_version': self.get('config_version'),
            'trading_mode': self.get('trading_mode'),
            'testnet_enabled': self.is_testnet_mode(),
            'live_trading_enabled': self.is_live_trading_enabled(),
            'api_rotation_enabled': self.is_api_rotation_enabled(),
            'multi_pair_enabled': self.is_multi_pair_enabled(),
            'opportunity_scanner_enabled': self.is_opportunity_scanner_enabled(),
            'valid_api_services': self.get_valid_api_services(),
            'timeframes': self.get('timeframes'),
            'primary_timeframe': self.get('primary_timeframe'),
            'max_concurrent_pairs': self.get('max_concurrent_pairs'),
            'max_positions': self.get('max_positions'),
            'total_config_items': len(self._config_cache),
            'api_keys_configured': sum(len(keys) for keys in self._api_rotation_cache.values())
        }

# Global configuration instance
config = EnhancedConfigReader()

# Convenience functions for easy access
def get_config(key: str, default: Any = None) -> Any:
    """Get configuration value"""
    return config.get(key, default)

def get_trading_config() -> Dict[str, Any]:
    """Get trading configuration"""
    return config.get_trading_config()

def get_multi_pair_config() -> Dict[str, Any]:
    """Get multi-pair configuration"""
    return config.get_multi_pair_config()

def get_api_config() -> Dict[str, Any]:
    """Get API configuration"""
    return config.get_api_config()

def get_system_config() -> Dict[str, Any]:
    """Get system configuration"""
    return config.get_system_config()

def reload_config():
    """Reload configuration"""
    config.reload_config()

def validate_config() -> Tuple[bool, List[str]]:
    """Validate configuration"""
    return config.validate_configuration()

if __name__ == "__main__":
    # Test configuration reading
    print("Enhanced Trading System Configuration")
    print("=" * 50)
    
    # Validate configuration
    is_valid, issues = validate_config()
    print(f"Configuration Valid: {'Yes' if is_valid else 'No'}")
    if issues:
        print("Issues found:")
        for issue in issues:
            print(f"  - {issue}")
    
    # Show summary
    summary = config.get_config_summary()
    print(f"\nConfiguration Summary:")
    for key, value in summary.items():
        if isinstance(value, list):
            value = f"{len(value)} items: {', '.join(value[:3])}{'...' if len(value) > 3 else ''}"
        print(f"  {key}: {value}")
    
    # Show valid API services
    services = config.get_valid_api_services()
    print(f"\nValid API Services ({len(services)}):")
    for service in services:
        key_count = config.get_api_key_count(service)
        print(f"  {service}: {key_count} keys")
    
    print("\nEnhanced configuration loaded successfully!")