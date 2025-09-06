#!/usr/bin/env python3
"""
API ROTATION MANAGER - REAL DATA ONLY
====================================
Manages rotation of real API keys for reliable data access
NO MOCK DATA - All APIs return real market data only
"""

import os
import time
import logging
import random
from typing import Dict, Optional, List, Tuple
from datetime import datetime, timedelta
import sqlite3
from pathlib import Path
import threading
from dotenv import load_dotenv

load_dotenv()

class APIRotationManager:
    """Manages real API key rotation for reliable data access"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Database for tracking API usage
        self.db_path = Path('api_monitor.db')
        self.db_path.parent.mkdir(exist_ok=True)
        self._init_database()
        
        # Thread safety
        self._lock = threading.Lock()
        
        # API configurations - REAL DATA ONLY
        self.api_configs = {
            'binance': {
                'keys': self._load_binance_keys(),
                'rate_limit': 1200,  # requests per minute
                'cooldown': 60,      # seconds
                'last_used': {},
                'error_count': {},
                'is_live': False
            },
            'binance_live': {
                'keys': self._load_binance_live_keys(),
                'rate_limit': 1200,
                'cooldown': 60,
                'last_used': {},
                'error_count': {},
                'is_live': True
            },
            'alpha_vantage': {
                'keys': self._load_alpha_vantage_keys(),
                'rate_limit': 5,      # requests per minute
                'cooldown': 60,
                'last_used': {},
                'error_count': {}
            },
            'news_api': {
                'keys': self._load_news_api_keys(),
                'rate_limit': 1000,   # requests per day
                'cooldown': 3600,
                'last_used': {},
                'error_count': {}
            },
            'fred': {
                'keys': self._load_fred_keys(),
                'rate_limit': 120,    # requests per hour
                'cooldown': 30,
                'last_used': {},
                'error_count': {}
            },
            'twitter': {
                'keys': self._load_twitter_keys(),
                'rate_limit': 300,    # requests per 15 min
                'cooldown': 900,
                'last_used': {},
                'error_count': {}
            },
            'reddit': {
                'keys': self._load_reddit_keys(),
                'rate_limit': 60,     # requests per minute
                'cooldown': 60,
                'last_used': {},
                'error_count': {}
            }
        }
        
        self.logger.info("API Rotation Manager initialized - REAL DATA ONLY")
    
    def _init_database(self):
        """Initialize database for API usage tracking"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS api_usage (
                    id INTEGER PRIMARY KEY,
                    api_name TEXT,
                    key_index INTEGER,
                    timestamp TEXT,
                    success BOOLEAN,
                    error_message TEXT
                )
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS api_stats (
                    api_name TEXT PRIMARY KEY,
                    total_calls INTEGER DEFAULT 0,
                    successful_calls INTEGER DEFAULT 0,
                    failed_calls INTEGER DEFAULT 0,
                    last_success TEXT,
                    last_failure TEXT
                )
            ''')
    
    def _load_binance_keys(self) -> List[Dict[str, str]]:
        """Load Binance testnet API keys"""
        keys = []
        for i in range(1, 4):
            api_key = os.getenv(f'BINANCE_API_KEY_{i}')
            api_secret = os.getenv(f'BINANCE_API_SECRET_{i}')
            if api_key and api_secret:
                keys.append({
                    'api_key': api_key,
                    'api_secret': api_secret,
                    'index': i
                })
        return keys
    
    def _load_binance_live_keys(self) -> List[Dict[str, str]]:
        """Load Binance live API keys"""
        keys = []
        for i in range(1, 4):
            api_key = os.getenv(f'BINANCE_LIVE_API_KEY_{i}')
            api_secret = os.getenv(f'BINANCE_LIVE_API_SECRET_{i}')
            if api_key and api_secret:
                keys.append({
                    'api_key': api_key,
                    'api_secret': api_secret,
                    'index': i
                })
        return keys
    
    def _load_alpha_vantage_keys(self) -> List[Dict[str, str]]:
        """Load Alpha Vantage API keys"""
        keys = []
        for i in range(1, 4):
            api_key = os.getenv(f'ALPHA_VANTAGE_API_KEY_{i}')
            if api_key:
                keys.append({
                    'api_key': api_key,
                    'index': i
                })
        return keys
    
    def _load_news_api_keys(self) -> List[Dict[str, str]]:
        """Load News API keys"""
        keys = []
        for i in range(1, 4):
            api_key = os.getenv(f'NEWS_API_KEY_{i}')
            if api_key:
                keys.append({
                    'api_key': api_key,
                    'index': i
                })
        return keys
    
    def _load_fred_keys(self) -> List[Dict[str, str]]:
        """Load FRED API keys"""
        keys = []
        for i in range(1, 4):
            api_key = os.getenv(f'FRED_API_KEY_{i}')
            if api_key:
                keys.append({
                    'api_key': api_key,
                    'index': i
                })
        return keys
    
    def _load_twitter_keys(self) -> List[Dict[str, str]]:
        """Load Twitter API keys"""
        keys = []
        for i in range(1, 4):
            bearer_token = os.getenv(f'TWITTER_BEARER_TOKEN_{i}')
            if bearer_token:
                keys.append({
                    'bearer_token': bearer_token,
                    'index': i
                })
        return keys
    
    def _load_reddit_keys(self) -> List[Dict[str, str]]:
        """Load Reddit API keys"""
        keys = []
        for i in range(1, 4):
            client_id = os.getenv(f'REDDIT_CLIENT_ID_{i}')
            client_secret = os.getenv(f'REDDIT_CLIENT_SECRET_{i}')
            if client_id and client_secret:
                keys.append({
                    'client_id': client_id,
                    'client_secret': client_secret,
                    'index': i
                })
        return keys
    
    def get_best_api_key(self, api_name: str) -> Optional[Dict[str, str]]:
        """Get the best available API key for the specified service"""
        with self._lock:
            if api_name not in self.api_configs:
                self.logger.error(f"Unknown API service: {api_name}")
                return None
            
            config = self.api_configs[api_name]
            if not config['keys']:
                self.logger.warning(f"No API keys configured for {api_name}")
                return None
            
            # Find the best key (least recently used with lowest error count)
            best_key = None
            best_score = float('inf')
            
            current_time = time.time()
            
            for key in config['keys']:
                key_index = key['index']
                
                # Check if key is in cooldown
                last_used = config['last_used'].get(key_index, 0)
                if current_time - last_used < config['cooldown']:
                    continue
                
                # Calculate score (lower is better)
                error_count = config['error_count'].get(key_index, 0)
                time_since_use = current_time - last_used
                
                # Score: prioritize keys with fewer errors and longer time since use
                score = error_count * 100 - time_since_use
                
                if score < best_score:
                    best_score = score
                    best_key = key
            
            if best_key:
                # Update last used time
                config['last_used'][best_key['index']] = current_time
                self.logger.debug(f"Selected {api_name} key {best_key['index']}")
                return best_key
            else:
                self.logger.warning(f"No available {api_name} keys (all in cooldown)")
                return None
    
    def report_api_result(self, api_name: str, key_index: int, success: bool, error_message: str = None):
        """Report the result of an API call"""
        with self._lock:
            if api_name not in self.api_configs:
                return
            
            config = self.api_configs[api_name]
            
            # Update error count
            if not success:
                config['error_count'][key_index] = config['error_count'].get(key_index, 0) + 1
                self.logger.warning(f"{api_name} key {key_index} error: {error_message}")
            else:
                # Reset error count on success
                config['error_count'][key_index] = 0
            
            # Log to database
            timestamp = datetime.now().isoformat()
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT INTO api_usage (api_name, key_index, timestamp, success, error_message)
                    VALUES (?, ?, ?, ?, ?)
                ''', (api_name, key_index, timestamp, success, error_message))
                
                # Update stats
                conn.execute('''
                    INSERT OR REPLACE INTO api_stats 
                    (api_name, total_calls, successful_calls, failed_calls, last_success, last_failure)
                    VALUES (
                        ?,
                        COALESCE((SELECT total_calls FROM api_stats WHERE api_name = ?), 0) + 1,
                        COALESCE((SELECT successful_calls FROM api_stats WHERE api_name = ?), 0) + ?,
                        COALESCE((SELECT failed_calls FROM api_stats WHERE api_name = ?), 0) + ?,
                        CASE WHEN ? THEN ? ELSE (SELECT last_success FROM api_stats WHERE api_name = ?) END,
                        CASE WHEN ? THEN ? ELSE (SELECT last_failure FROM api_stats WHERE api_name = ?) END
                    )
                ''', (
                    api_name, api_name, api_name, 1 if success else 0, api_name, 1 if not success else 0,
                    success, timestamp if success else None, api_name,
                    not success, timestamp if not success else None, api_name
                ))
    
    def get_api_stats(self) -> Dict[str, Dict]:
        """Get API usage statistics"""
        stats = {}
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT * FROM api_stats')
            for row in cursor.fetchall():
                api_name = row[0]
                stats[api_name] = {
                    'total_calls': row[1],
                    'successful_calls': row[2],
                    'failed_calls': row[3],
                    'success_rate': (row[2] / row[1] * 100) if row[1] > 0 else 0,
                    'last_success': row[4],
                    'last_failure': row[5],
                    'available_keys': len(self.api_configs.get(api_name, {}).get('keys', [])),
                    'keys_in_cooldown': self._count_keys_in_cooldown(api_name)
                }
        
        return stats
    
    def _count_keys_in_cooldown(self, api_name: str) -> int:
        """Count how many keys are currently in cooldown"""
        if api_name not in self.api_configs:
            return 0
        
        config = self.api_configs[api_name]
        current_time = time.time()
        cooldown_count = 0
        
        for key in config['keys']:
            key_index = key['index']
            last_used = config['last_used'].get(key_index, 0)
            if current_time - last_used < config['cooldown']:
                cooldown_count += 1
        
        return cooldown_count
    
    def reset_error_counts(self, api_name: str = None):
        """Reset error counts for specified API or all APIs"""
        with self._lock:
            if api_name:
                if api_name in self.api_configs:
                    self.api_configs[api_name]['error_count'] = {}
                    self.logger.info(f"Reset error counts for {api_name}")
            else:
                for config in self.api_configs.values():
                    config['error_count'] = {}
                self.logger.info("Reset error counts for all APIs")
    
    def force_cooldown_reset(self, api_name: str = None):
        """Force reset cooldowns for specified API or all APIs"""
        with self._lock:
            if api_name:
                if api_name in self.api_configs:
                    self.api_configs[api_name]['last_used'] = {}
                    self.logger.info(f"Reset cooldowns for {api_name}")
            else:
                for config in self.api_configs.values():
                    config['last_used'] = {}
                self.logger.info("Reset cooldowns for all APIs")


# Global instance
_rotation_manager = None

def get_rotation_manager() -> APIRotationManager:
    """Get global rotation manager instance"""
    global _rotation_manager
    if _rotation_manager is None:
        _rotation_manager = APIRotationManager()
    return _rotation_manager

def get_api_key(api_name: str) -> Optional[Dict[str, str]]:
    """Get API key for specified service - REAL DATA ONLY"""
    manager = get_rotation_manager()
    return manager.get_best_api_key(api_name)

def report_api_result(api_name: str, key_index: int, success: bool, error_message: str = None):
    """Report API call result"""
    manager = get_rotation_manager()
    manager.report_api_result(api_name, key_index, success, error_message)

def get_api_stats() -> Dict[str, Dict]:
    """Get API usage statistics"""
    manager = get_rotation_manager()
    return manager.get_api_stats()

def reset_api_errors(api_name: str = None):
    """Reset API error counts"""
    manager = get_rotation_manager()
    manager.reset_error_counts(api_name)

def reset_api_cooldowns(api_name: str = None):
    """Reset API cooldowns"""
    manager = get_rotation_manager()
    manager.force_cooldown_reset(api_name)


if __name__ == "__main__":
    # Test the rotation manager
    manager = APIRotationManager()
    
    print("API Rotation Manager - REAL DATA ONLY")
    print("=" * 50)
    
    for api_name in manager.api_configs.keys():
        key = manager.get_best_api_key(api_name)
        if key:
            print(f"? {api_name}: Key {key['index']} available")
        else:
            print(f"? {api_name}: No keys available")
    
    print("\nAPI Statistics:")
    stats = manager.get_api_stats()
    for api_name, api_stats in stats.items():
        print(f"{api_name}: {api_stats['available_keys']} keys, "
              f"{api_stats['success_rate']:.1f}% success rate")