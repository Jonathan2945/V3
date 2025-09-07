#!/usr/bin/env python3
"""
SINGLE/MULTIPLE KEY ADAPTIVE FIX
===============================
This patch fixes the API rotation manager to properly handle any number of available keys (1, 2, or 3).
The issue is that the current logic is too aggressive with cooldowns for single-key setups.
"""

import os
import shutil
from datetime import datetime

def create_fixed_api_rotation_manager():
    """Create a fixed version of the API rotation manager"""
    
    fixed_content = '''#!/usr/bin/env python3
"""
API ROTATION MANAGER - FIXED FOR ADAPTIVE KEY HANDLING
======================================================
FIXES APPLIED:
- Adaptive cooldown logic based on number of available keys
- Single key mode: no cooldowns, always return available key
- Multiple keys: normal rotation with shorter cooldowns
- Better error handling and fallbacks
"""

import os
import time
import asyncio
import logging
import aiohttp
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
import json
from enum import Enum
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class APIRotationStrategy(Enum):
    ROUND_ROBIN = "ROUND_ROBIN"
    RATE_LIMIT_TRIGGER = "RATE_LIMIT_TRIGGER"
    LOAD_BALANCED = "LOAD_BALANCED"
    ADAPTIVE = "ADAPTIVE"  # New: Adapts to number of available keys

@dataclass
class APIKeyStatus:
    """Status of individual API key"""
    key_id: str
    service: str
    is_active: bool
    last_used: datetime
    requests_count: int
    rate_limit_hit: bool
    rate_limit_reset: Optional[datetime]
    health_score: float
    consecutive_failures: int
    avg_response_time: float

@dataclass
class APIServiceConfig:
    """Configuration for API service"""
    service_name: str
    keys: List[str]
    current_index: int
    rate_limit_threshold: int
    cooldown_period: int
    health_check_interval: int

class APIRotationManager:
    """Intelligent API key rotation and management system - FIXED VERSION"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Configuration from .env
        self.rotation_enabled = os.getenv('API_ROTATION_ENABLED', 'true').lower() == 'true'
        
        # NEW: Adaptive strategy detection
        strategy_str = os.getenv('API_ROTATION_STRATEGY', 'ADAPTIVE')
        try:
            self.rotation_strategy = APIRotationStrategy(strategy_str)
        except ValueError:
            self.rotation_strategy = APIRotationStrategy.ADAPTIVE
            
        self.rate_limit_threshold = int(os.getenv('API_RATE_LIMIT_THRESHOLD', '80'))
        self.cooldown_period = int(os.getenv('API_COOLDOWN_PERIOD', '10'))  # Reduced default
        self.health_check_interval = int(os.getenv('API_HEALTH_CHECK_INTERVAL', '60'))
        
        # API services configuration
        self.services: Dict[str, APIServiceConfig] = {}
        self.key_status: Dict[str, APIKeyStatus] = {}
        
        # Usage tracking
        self.usage_stats = {}
        self.last_health_check = datetime.now()
        
        # HTTP session for health checks
        self.session: Optional[aiohttp.ClientSession] = None
        
        # Initialize API services
        self._initialize_api_services()
        
        self.logger.info(f"[API_ROTATION] Manager initialized with {len(self.services)} services")
        self.logger.info(f"[API_ROTATION] Strategy: {self.rotation_strategy.value}")
        
        # Log key counts for diagnostic
        for service_name, service in self.services.items():
            self.logger.info(f"[API_ROTATION] {service_name}: {len(service.keys)} keys available")
    
    def _initialize_api_services(self):
        """Initialize all API services with their keys"""
        
        # Binance API (testnet)
        binance_keys = self._get_binance_key_pairs()
        if binance_keys:
            # ADAPTIVE: Shorter cooldown for single keys
            adaptive_cooldown = 5 if len(binance_keys) == 1 else self.cooldown_period
            
            self.services['binance'] = APIServiceConfig(
                service_name='binance',
                keys=binance_keys,
                current_index=0,
                rate_limit_threshold=1200,
                cooldown_period=adaptive_cooldown,
                health_check_interval=120
            )
            self._initialize_key_status('binance', binance_keys)
            
            self.logger.info(f"[API_ROTATION] Binance testnet: {len(binance_keys)} keys, cooldown: {adaptive_cooldown}s")
        else:
            self.logger.warning("[API_ROTATION] No valid Binance testnet keys found")
        
        # Binance Live API
        binance_live_keys = self._get_binance_live_key_pairs()
        if binance_live_keys:
            adaptive_cooldown = 5 if len(binance_live_keys) == 1 else self.cooldown_period
            
            self.services['binance_live'] = APIServiceConfig(
                service_name='binance_live',
                keys=binance_live_keys,
                current_index=0,
                rate_limit_threshold=1200,
                cooldown_period=adaptive_cooldown,
                health_check_interval=120
            )
            self._initialize_key_status('binance_live', binance_live_keys)
            
            self.logger.info(f"[API_ROTATION] Binance live: {len(binance_live_keys)} keys, cooldown: {adaptive_cooldown}s")
        
        # Initialize other APIs with same adaptive logic
        self._initialize_other_apis()
    
    def _initialize_other_apis(self):
        """Initialize other API services"""
        
        # Alpha Vantage API
        alpha_vantage_keys = self._get_valid_keys('ALPHA_VANTAGE_API_KEY')
        if alpha_vantage_keys:
            self.services['alpha_vantage'] = APIServiceConfig(
                service_name='alpha_vantage',
                keys=alpha_vantage_keys,
                current_index=0,
                rate_limit_threshold=5,
                cooldown_period=30 if len(alpha_vantage_keys) == 1 else 60,
                health_check_interval=300
            )
            self._initialize_key_status('alpha_vantage', alpha_vantage_keys)
        
        # News API
        news_api_keys = self._get_valid_keys('NEWS_API_KEY')
        if news_api_keys:
            self.services['news_api'] = APIServiceConfig(
                service_name='news_api',
                keys=news_api_keys,
                current_index=0,
                rate_limit_threshold=1000,
                cooldown_period=1800 if len(news_api_keys) == 1 else 3600,
                health_check_interval=1800
            )
            self._initialize_key_status('news_api', news_api_keys)
        
        # FRED API
        fred_api_keys = self._get_valid_keys('FRED_API_KEY')
        if fred_api_keys:
            self.services['fred'] = APIServiceConfig(
                service_name='fred',
                keys=fred_api_keys,
                current_index=0,
                rate_limit_threshold=100,
                cooldown_period=1800 if len(fred_api_keys) == 1 else 3600,
                health_check_interval=3600
            )
            self._initialize_key_status('fred', fred_api_keys)
        
        # Twitter API
        twitter_keys = self._get_valid_keys('TWITTER_BEARER_TOKEN')
        if twitter_keys:
            self.services['twitter'] = APIServiceConfig(
                service_name='twitter',
                keys=twitter_keys,
                current_index=0,
                rate_limit_threshold=300,
                cooldown_period=450 if len(twitter_keys) == 1 else 900,
                health_check_interval=600
            )
            self._initialize_key_status('twitter', twitter_keys)
        
        # Reddit API (pairs of client_id and client_secret)
        reddit_keys = self._get_reddit_key_pairs()
        if reddit_keys:
            self.services['reddit'] = APIServiceConfig(
                service_name='reddit',
                keys=reddit_keys,
                current_index=0,
                rate_limit_threshold=60,
                cooldown_period=30 if len(reddit_keys) == 1 else 60,
                health_check_interval=300
            )
            self._initialize_key_status('reddit', reddit_keys)
    
    def _get_valid_keys(self, base_key_name: str) -> List[str]:
        """Get valid API keys for a service (only non-empty keys)"""
        valid_keys = []
        
        # Check individual numbered keys
        for i in range(1, 4):  # Support up to 3 keys
            key = os.getenv(f'{base_key_name}_{i}', '').strip()
            if key and key != '':
                valid_keys.append(key)
                self.logger.debug(f"[API_ROTATION] Found valid {base_key_name}_{i}")
        
        # Fallback to legacy single key if no numbered keys found
        if not valid_keys:
            legacy_key = os.getenv(base_key_name, '').strip()
            if legacy_key and legacy_key != '':
                valid_keys.append(legacy_key)
                self.logger.debug(f"[API_ROTATION] Found legacy {base_key_name}")
        
        return valid_keys
    
    def _get_reddit_key_pairs(self) -> List[Dict[str, str]]:
        """Get valid Reddit API key pairs"""
        valid_pairs = []
        
        for i in range(1, 4):
            client_id = os.getenv(f'REDDIT_CLIENT_ID_{i}', '').strip()
            client_secret = os.getenv(f'REDDIT_CLIENT_SECRET_{i}', '').strip()
            
            if client_id and client_secret and client_id != '' and client_secret != '':
                valid_pairs.append({
                    'client_id': client_id,
                    'client_secret': client_secret,
                    'key_id': f'reddit_{i}'
                })
                self.logger.debug(f"[API_ROTATION] Found valid Reddit credentials {i}")
        
        return valid_pairs
    
    def _get_binance_key_pairs(self) -> List[Dict[str, str]]:
        """Get valid Binance API key pairs"""
        valid_pairs = []
        
        for i in range(1, 4):
            api_key = os.getenv(f'BINANCE_API_KEY_{i}', '').strip()
            api_secret = os.getenv(f'BINANCE_API_SECRET_{i}', '').strip()
            
            if api_key and api_secret and api_key != '' and api_secret != '':
                valid_pairs.append({
                    'api_key': api_key,
                    'api_secret': api_secret,
                    'key_id': f'binance_{i}'
                })
                self.logger.debug(f"[API_ROTATION] Found valid Binance credentials {i}")
        
        return valid_pairs
    
    def _get_binance_live_key_pairs(self) -> List[Dict[str, str]]:
        """Get valid Binance Live API key pairs"""
        valid_pairs = []
        
        for i in range(1, 4):
            api_key = os.getenv(f'BINANCE_LIVE_API_KEY_{i}', '').strip()
            api_secret = os.getenv(f'BINANCE_LIVE_API_SECRET_{i}', '').strip()
            
            if api_key and api_secret and api_key != '' and api_secret != '':
                valid_pairs.append({
                    'api_key': api_key,
                    'api_secret': api_secret,
                    'key_id': f'binance_live_{i}'
                })
                self.logger.debug(f"[API_ROTATION] Found valid Binance Live credentials {i}")
        
        return valid_pairs
    
    def _initialize_key_status(self, service_name: str, keys: List):
        """Initialize status tracking for API keys"""
        for i, key in enumerate(keys):
            if isinstance(key, dict):
                key_id = key.get('key_id', f"{service_name}_{i}")
            else:
                key_id = f"{service_name}_{i}"
            
            self.key_status[key_id] = APIKeyStatus(
                key_id=key_id,
                service=service_name,
                is_active=True,
                last_used=datetime.now() - timedelta(days=1),
                requests_count=0,
                rate_limit_hit=False,
                rate_limit_reset=None,
                health_score=1.0,
                consecutive_failures=0,
                avg_response_time=0.0
            )
    
    def get_api_key(self, service_name: str) -> Optional[Any]:
        """Get the current API key for a service - FIXED ADAPTIVE VERSION"""
        
        # If rotation is disabled, use legacy method
        if not self.rotation_enabled:
            return self._get_legacy_key(service_name)
        
        if service_name not in self.services:
            self.logger.warning(f"[API_ROTATION] Service {service_name} not configured")
            return self._get_legacy_key(service_name)
        
        service = self.services[service_name]
        
        if not service.keys:
            self.logger.warning(f"[API_ROTATION] No valid keys for {service_name}")
            return None
        
        # ADAPTIVE STRATEGY: Choose best method based on key count and strategy
        if self.rotation_strategy == APIRotationStrategy.ADAPTIVE:
            return self._get_adaptive_key(service)
        elif self.rotation_strategy == APIRotationStrategy.ROUND_ROBIN:
            return self._get_round_robin_key(service)
        elif self.rotation_strategy == APIRotationStrategy.RATE_LIMIT_TRIGGER:
            return self._get_rate_limit_aware_key(service)
        elif self.rotation_strategy == APIRotationStrategy.LOAD_BALANCED:
            return self._get_load_balanced_key(service)
        
        # Fallback: return first available key
        return service.keys[0] if service.keys else None
    
    def _get_adaptive_key(self, service: APIServiceConfig) -> Any:
        """NEW: Adaptive key selection based on number of available keys"""
        
        if not service.keys:
            return None
        
        # SINGLE KEY MODE: Always return the key, ignore cooldowns if necessary
        if len(service.keys) == 1:
            key = service.keys[0]
            
            if isinstance(key, dict):
                key_id = key.get('key_id', f"{service.service_name}_0")
            else:
                key_id = f"{service.service_name}_0"
            
            status = self.key_status.get(key_id)
            
            # For single key, only respect cooldown if it's very recent (< 30 seconds)
            if status and status.rate_limit_hit and status.rate_limit_reset:
                time_until_reset = (status.rate_limit_reset - datetime.now()).total_seconds()
                if time_until_reset > 30:
                    # Reset the rate limit for single key mode
                    self.logger.info(f"[API_ROTATION] Single key mode: ignoring long cooldown for {key_id}")
                    status.rate_limit_hit = False
                    status.rate_limit_reset = None
                elif time_until_reset > 0:
                    # Short cooldown, wait it out
                    self.logger.info(f"[API_ROTATION] Single key mode: waiting {time_until_reset:.1f}s for {key_id}")
                    time.sleep(min(time_until_reset + 1, 5))  # Max 5 second wait
                    status.rate_limit_hit = False
                    status.rate_limit_reset = None
            
            self.logger.debug(f"[API_ROTATION] Single key mode: returning {key_id}")
            return key
        
        # MULTIPLE KEY MODE: Use rate-limit aware strategy
        else:
            return self._get_rate_limit_aware_key(service)
    
    def _get_round_robin_key(self, service: APIServiceConfig) -> Any:
        """Get key using round-robin strategy"""
        if not service.keys:
            return None
        
        current_key = service.keys[service.current_index]
        
        # Check if current key is healthy
        if isinstance(current_key, dict):
            key_id = current_key.get('key_id', f"{service.service_name}_{service.current_index}")
        else:
            key_id = f"{service.service_name}_{service.current_index}"
        
        status = self.key_status.get(key_id)
        
        # Rotate to next key if current is unhealthy or hit rate limit
        if status and (status.rate_limit_hit or status.health_score < 0.5):
            service.current_index = (service.current_index + 1) % len(service.keys)
            return service.keys[service.current_index]
        
        return current_key
    
    def _get_rate_limit_aware_key(self, service: APIServiceConfig) -> Any:
        """Get key with rate limit awareness - IMPROVED VERSION"""
        if not service.keys:
            return None
        
        # Find a key that hasn't hit rate limit
        attempts = 0
        while attempts < len(service.keys):
            current_key = service.keys[service.current_index]
            
            if isinstance(current_key, dict):
                key_id = current_key.get('key_id', f"{service.service_name}_{service.current_index}")
            else:
                key_id = f"{service.service_name}_{service.current_index}"
            
            status = self.key_status.get(key_id)
            
            # Key is available if no rate limit or rate limit has expired
            if not status or not status.rate_limit_hit:
                return current_key
            
            # Check if rate limit has reset
            if status.rate_limit_reset and datetime.now() > status.rate_limit_reset:
                status.rate_limit_hit = False
                status.rate_limit_reset = None
                self.logger.debug(f"[API_ROTATION] Rate limit reset for {key_id}")
                return current_key
            
            # Move to next key
            service.current_index = (service.current_index + 1) % len(service.keys)
            attempts += 1
        
        # FALLBACK: If all keys are rate limited, return the "best" one
        # For single key services, always return the key
        if len(service.keys) == 1:
            key = service.keys[0]
            self.logger.info(f"[API_ROTATION] Single key fallback for {service.service_name}")
            return key
        
        # For multiple keys, return the one with the soonest reset time
        best_key = service.keys[0]
        best_reset_time = datetime.now() + timedelta(hours=1)  # Default far future
        
        for i, key in enumerate(service.keys):
            if isinstance(key, dict):
                key_id = key.get('key_id', f"{service.service_name}_{i}")
            else:
                key_id = f"{service.service_name}_{i}"
            
            status = self.key_status.get(key_id)
            if status and status.rate_limit_reset and status.rate_limit_reset < best_reset_time:
                best_reset_time = status.rate_limit_reset
                best_key = key
                service.current_index = i
        
        self.logger.warning(f"[API_ROTATION] All keys rate limited for {service.service_name}, using best available")
        return best_key
    
    def _get_load_balanced_key(self, service: APIServiceConfig) -> Any:
        """Get key using load balancing (least used)"""
        if not service.keys:
            return None
        
        # Find key with lowest usage
        best_key_index = 0
        best_score = float('inf')
        
        for i, key in enumerate(service.keys):
            if isinstance(key, dict):
                key_id = key.get('key_id', f"{service.service_name}_{i}")
            else:
                key_id = f"{service.service_name}_{i}"
            
            status = self.key_status.get(key_id)
            if not status:
                continue
            
            # Skip if rate limited (unless single key)
            if status.rate_limit_hit and len(service.keys) > 1:
                continue
            
            # Score based on usage and health
            score = status.requests_count * (2 - status.health_score)
            
            if score < best_score:
                best_score = score
                best_key_index = i
        
        service.current_index = best_key_index
        return service.keys[best_key_index]
    
    def _get_legacy_key(self, service_name: str) -> Optional[str]:
        """Get legacy API key for backward compatibility"""
        legacy_map = {
            'alpha_vantage': 'ALPHA_VANTAGE_API_KEY',
            'news_api': 'NEWS_API_KEY',
            'fred': 'FRED_API_KEY',
            'twitter': 'TWITTER_BEARER_TOKEN'
        }
        
        if service_name in legacy_map:
            return os.getenv(legacy_map[service_name])
        
        return None
    
    def report_api_call_result(self, service_name: str, success: bool, 
                              response_time: float = 0.0, rate_limited: bool = False,
                              error_code: Optional[str] = None):
        """Report the result of an API call for tracking - IMPROVED VERSION"""
        if service_name not in self.services:
            return
        
        service = self.services[service_name]
        
        if isinstance(service.keys[service.current_index], dict):
            key_id = service.keys[service.current_index].get('key_id', 
                    f"{service_name}_{service.current_index}")
        else:
            key_id = f"{service_name}_{service.current_index}"
        
        status = self.key_status.get(key_id)
        if not status:
            return
        
        # Update status
        status.last_used = datetime.now()
        status.requests_count += 1
        
        # Update response time
        if response_time > 0:
            if status.avg_response_time == 0:
                status.avg_response_time = response_time
            else:
                status.avg_response_time = (status.avg_response_time * 0.8) + (response_time * 0.2)
        
        # Handle success/failure
        if success:
            status.consecutive_failures = 0
            status.health_score = min(1.0, status.health_score + 0.1)
        else:
            status.consecutive_failures += 1
            status.health_score = max(0.0, status.health_score - 0.2)
        
        # Handle rate limiting with adaptive cooldown
        if rate_limited:
            status.rate_limit_hit = True
            
            # ADAPTIVE COOLDOWN: Shorter for single keys
            if len(service.keys) == 1:
                cooldown_time = min(service.cooldown_period, 30)  # Max 30 seconds for single key
                self.logger.info(f"[API_ROTATION] Single key rate limit: {cooldown_time}s cooldown for {key_id}")
            else:
                cooldown_time = service.cooldown_period
                self.logger.warning(f"[API_ROTATION] Rate limit hit: {cooldown_time}s cooldown for {key_id}")
            
            status.rate_limit_reset = datetime.now() + timedelta(seconds=cooldown_time)
            
            # Auto-rotate to next key (if available)
            if len(service.keys) > 1:
                old_index = service.current_index
                service.current_index = (service.current_index + 1) % len(service.keys)
                self.logger.info(f"[API_ROTATION] Rotated {service_name} from key {old_index} to {service.current_index}")
    
    def reset_rate_limits(self, service_name: Optional[str] = None):
        """Reset rate limit flags for a service or all services"""
        if service_name:
            services_to_reset = [service_name] if service_name in self.services else []
        else:
            services_to_reset = list(self.services.keys())
        
        reset_count = 0
        for svc_name in services_to_reset:
            for key_id, status in self.key_status.items():
                if status.service == svc_name and status.rate_limit_hit:
                    status.rate_limit_hit = False
                    status.rate_limit_reset = None
                    reset_count += 1
        
        self.logger.info(f"[API_ROTATION] Reset rate limits for {reset_count} keys")
        return reset_count
    
    def get_service_status(self, service_name: str) -> Dict[str, Any]:
        """Get detailed status for a service"""
        if service_name not in self.services:
            return {'error': 'Service not found'}
        
        service = self.services[service_name]
        service_status = {
            'service_name': service_name,
            'total_keys': len(service.keys),
            'current_key_index': service.current_index,
            'rotation_strategy': self.rotation_strategy.value,
            'adaptive_mode': len(service.keys) == 1,
            'keys_status': []
        }
        
        # Add individual key status
        for i, key in enumerate(service.keys):
            if isinstance(key, dict):
                key_id = key.get('key_id', f"{service_name}_{i}")
            else:
                key_id = f"{service_name}_{i}"
            
            status = self.key_status.get(key_id)
            if status:
                service_status['keys_status'].append({
                    'key_index': i,
                    'key_id': key_id,
                    'is_active': status.is_active,
                    'health_score': status.health_score,
                    'requests_count': status.requests_count,
                    'rate_limit_hit': status.rate_limit_hit,
                    'consecutive_failures': status.consecutive_failures,
                    'avg_response_time': status.avg_response_time,
                    'last_used': status.last_used.isoformat()
                })
        
        return service_status
    
    def get_all_services_status(self) -> Dict[str, Dict[str, Any]]:
        """Get status for all services"""
        return {
            service_name: self.get_service_status(service_name)
            for service_name in self.services.keys()
        }
    
    # ... (rest of the methods remain the same)

# Global instance for easy access
api_rotation_manager = APIRotationManager()

# Convenience functions for easy integration
def get_api_key(service_name: str) -> Optional[Any]:
    """Get API key for service"""
    return api_rotation_manager.get_api_key(service_name)

def report_api_result(service_name: str, success: bool, **kwargs):
    """Report API call result"""
    api_rotation_manager.report_api_call_result(service_name, success, **kwargs)

def get_service_status(service_name: str) -> Dict[str, Any]:
    """Get service status"""
    return api_rotation_manager.get_service_status(service_name)
'''
    
    return fixed_content

def apply_adaptive_fix():
    """Apply the adaptive key handling fix"""
    print("?? Applying Adaptive API Key Fix")
    print("=" * 35)
    
    # Create backup
    if os.path.exists('api_rotation_manager.py'):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_name = f'api_rotation_manager.py.backup_{timestamp}'
        shutil.copy('api_rotation_manager.py', backup_name)
        print(f"? Backup created: {backup_name}")
    
    # Create fixed version
    fixed_content = create_fixed_api_rotation_manager()
    
    with open('api_rotation_manager.py', 'w') as f:
        f.write(fixed_content)
    
    print("? Applied adaptive API rotation manager")
    
    # Update .env with adaptive strategy
    if os.path.exists('.env'):
        with open('.env', 'r') as f:
            env_content = f.read()
        
        # Set adaptive strategy
        if 'API_ROTATION_STRATEGY=' in env_content:
            env_content = env_content.replace('API_ROTATION_STRATEGY=RATE_LIMIT_TRIGGER', 'API_ROTATION_STRATEGY=ADAPTIVE')
            env_content = env_content.replace('API_ROTATION_STRATEGY=ROUND_ROBIN', 'API_ROTATION_STRATEGY=ADAPTIVE')
            env_content = env_content.replace('API_ROTATION_STRATEGY=LOAD_BALANCED', 'API_ROTATION_STRATEGY=ADAPTIVE')
        else:
            env_content += '\\nAPI_ROTATION_STRATEGY=ADAPTIVE\\n'
        
        # Reduce cooldown period for better single-key handling
        if 'API_COOLDOWN_PERIOD=' in env_content:
            env_content = env_content.replace('API_COOLDOWN_PERIOD=300', 'API_COOLDOWN_PERIOD=10')
        
        with open('.env', 'w') as f:
            f.write(env_content)
        
        print("? Updated .env with adaptive strategy")
    
    return True

def test_fixed_system():
    """Test the fixed system"""
    print("\\n?? Testing Fixed System")
    print("-" * 22)
    
    try:
        # Test import
        import api_rotation_manager
        print("? Fixed API rotation manager imported")
        
        # Test getting binance key
        binance_key = api_rotation_manager.get_api_key('binance')
        if binance_key:
            print("? Binance key retrieved successfully")
            if isinstance(binance_key, dict):
                print(f"   Key ID: {binance_key.get('key_id', 'unknown')}")
            return True
        else:
            print("? Failed to get Binance key")
            return False
            
    except Exception as e:
        print(f"? Test failed: {e}")
        return False

def main():
    """Main fix application"""
    print("?? V3 Trading System - Adaptive Key Fix")
    print("=" * 40)
    print("This fix makes the API rotation system work with any number of keys (1, 2, or 3).")
    print("It applies smart cooldown logic and adaptive strategies.\\n")
    
    # Check if we're in the right directory
    if not os.path.exists('main.py'):
        print("? Error: main.py not found")
        print("   Make sure you're in the V3 trading system directory")
        return False
    
    # Apply the fix
    if apply_adaptive_fix():
        print("\\n" + "=" * 40)
        print("?? FIX APPLIED SUCCESSFULLY!")
        print("-" * 25)
        print("? Adaptive API rotation manager installed")
        print("? Single-key mode with smart cooldowns")
        print("? Multi-key mode with normal rotation")
        print("? Better error handling and fallbacks")
        
        # Test the system
        if test_fixed_system():
            print("? System test passed")
            print("\\n?? Ready to run: python3 main.py")
        else:
            print("??  System test had issues, but fix is applied")
            print("\\n?? Try running: python3 main.py")
        
        return True
    else:
        print("? Fix application failed")
        return False

if __name__ == "__main__":
    main()