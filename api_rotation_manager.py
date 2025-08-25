#!/usr/bin/env python3
"""
API ROTATION MANAGER - INTELLIGENT API KEY CYCLING
==================================================
Manages multiple API keys for each service with intelligent rotation
Features:
- Round-robin rotation
- Rate limit detection and switching
- Health monitoring for each API key
- Only uses valid/filled API keys
- Automatic failover and recovery
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
    """Intelligent API key rotation and management system"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Configuration from .env
        self.rotation_enabled = os.getenv('API_ROTATION_ENABLED', 'true').lower() == 'true'
        self.rotation_strategy = APIRotationStrategy(os.getenv('API_ROTATION_STRATEGY', 'ROUND_ROBIN'))
        self.rate_limit_threshold = int(os.getenv('API_RATE_LIMIT_THRESHOLD', '80'))
        self.cooldown_period = int(os.getenv('API_COOLDOWN_PERIOD', '300'))
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
    
    def _initialize_api_services(self):
        """Initialize all API services with their keys"""
        
        # Alpha Vantage API
        alpha_vantage_keys = self._get_valid_keys('ALPHA_VANTAGE_API_KEY')
        if alpha_vantage_keys:
            self.services['alpha_vantage'] = APIServiceConfig(
                service_name='alpha_vantage',
                keys=alpha_vantage_keys,
                current_index=0,
                rate_limit_threshold=5,  # 5 calls per minute
                cooldown_period=60,
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
                rate_limit_threshold=1000,  # 1000 calls per day
                cooldown_period=3600,
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
                rate_limit_threshold=100,  # 100 calls per day
                cooldown_period=3600,
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
                rate_limit_threshold=300,  # 300 calls per 15 minutes
                cooldown_period=900,
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
                rate_limit_threshold=60,  # 60 calls per minute
                cooldown_period=60,
                health_check_interval=300
            )
            self._initialize_key_status('reddit', reddit_keys)
        
        # Binance API
        binance_keys = self._get_binance_key_pairs()
        if binance_keys:
            self.services['binance'] = APIServiceConfig(
                service_name='binance',
                keys=binance_keys,
                current_index=0,
                rate_limit_threshold=1200,  # 1200 calls per minute
                cooldown_period=60,
                health_check_interval=120
            )
            self._initialize_key_status('binance', binance_keys)
        
        # Binance Live API
        binance_live_keys = self._get_binance_live_key_pairs()
        if binance_live_keys:
            self.services['binance_live'] = APIServiceConfig(
                service_name='binance_live',
                keys=binance_live_keys,
                current_index=0,
                rate_limit_threshold=1200,
                cooldown_period=60,
                health_check_interval=120
            )
            self._initialize_key_status('binance_live', binance_live_keys)
    
    def _get_valid_keys(self, base_key_name: str) -> List[str]:
        """Get valid API keys for a service (only non-empty keys)"""
        valid_keys = []
        
        # Check individual numbered keys
        for i in range(1, 4):  # Support up to 3 keys
            key = os.getenv(f'{base_key_name}_{i}', '').strip()
            if key and key != '':
                valid_keys.append(key)
                self.logger.info(f"[API_ROTATION] Found valid {base_key_name}_{i}")
        
        # Fallback to legacy single key if no numbered keys found
        if not valid_keys:
            legacy_key = os.getenv(base_key_name, '').strip()
            if legacy_key and legacy_key != '':
                valid_keys.append(legacy_key)
                self.logger.info(f"[API_ROTATION] Found legacy {base_key_name}")
        
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
                self.logger.info(f"[API_ROTATION] Found valid Reddit credentials {i}")
        
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
                self.logger.info(f"[API_ROTATION] Found valid Binance credentials {i}")
        
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
                self.logger.info(f"[API_ROTATION] Found valid Binance Live credentials {i}")
        
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
        """Get the current API key for a service"""
        if not self.rotation_enabled or service_name not in self.services:
            return self._get_legacy_key(service_name)
        
        service = self.services[service_name]
        
        if not service.keys:
            self.logger.warning(f"[API_ROTATION] No valid keys for {service_name}")
            return None
        
        # Apply rotation strategy
        if self.rotation_strategy == APIRotationStrategy.ROUND_ROBIN:
            return self._get_round_robin_key(service)
        elif self.rotation_strategy == APIRotationStrategy.RATE_LIMIT_TRIGGER:
            return self._get_rate_limit_aware_key(service)
        elif self.rotation_strategy == APIRotationStrategy.LOAD_BALANCED:
            return self._get_load_balanced_key(service)
        
        return service.keys[service.current_index] if service.keys else None
    
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
        """Get key with rate limit awareness"""
        if not service.keys:
            return None
        
        # Find a key that hasn't hit rate limit
        for _ in range(len(service.keys)):
            current_key = service.keys[service.current_index]
            
            if isinstance(current_key, dict):
                key_id = current_key.get('key_id', f"{service.service_name}_{service.current_index}")
            else:
                key_id = f"{service.service_name}_{service.current_index}"
            
            status = self.key_status.get(key_id)
            
            if not status or not status.rate_limit_hit:
                return current_key
            
            # Check if rate limit has reset
            if status.rate_limit_reset and datetime.now() > status.rate_limit_reset:
                status.rate_limit_hit = False
                status.rate_limit_reset = None
                return current_key
            
            # Move to next key
            service.current_index = (service.current_index + 1) % len(service.keys)
        
        # If all keys are rate limited, return current anyway (with warning)
        self.logger.warning(f"[API_ROTATION] All keys rate limited for {service.service_name}")
        return service.keys[service.current_index]
    
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
            
            # Skip if rate limited
            if status.rate_limit_hit:
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
        """Report the result of an API call for tracking"""
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
        
        # Handle rate limiting
        if rate_limited:
            status.rate_limit_hit = True
            status.rate_limit_reset = datetime.now() + timedelta(seconds=service.cooldown_period)
            self.logger.warning(f"[API_ROTATION] Rate limit hit for {key_id}")
            
            # Auto-rotate to next key
            if self.rotation_strategy != APIRotationStrategy.ROUND_ROBIN:
                service.current_index = (service.current_index + 1) % len(service.keys)
                self.logger.info(f"[API_ROTATION] Rotated to next key for {service_name}")
    
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
    
    async def health_check_all_services(self):
        """Perform health checks on all API services"""
        if not self.session:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
        
        self.logger.info("[API_ROTATION] Starting health checks for all services")
        
        tasks = []
        for service_name in self.services.keys():
            if service_name in ['alpha_vantage', 'news_api', 'fred']:
                tasks.append(self._health_check_service(service_name))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        self.last_health_check = datetime.now()
        self.logger.info("[API_ROTATION] Health checks completed")
    
    async def _health_check_service(self, service_name: str):
        """Perform health check for a specific service"""
        try:
            service = self.services.get(service_name)
            if not service:
                return
            
            # Test endpoints for different services
            test_urls = {
                'alpha_vantage': 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=1min&outputsize=compact&apikey=',
                'news_api': 'https://newsapi.org/v2/everything?q=test&pageSize=1&apiKey=',
                'fred': 'https://api.stlouisfed.org/fred/series?series_id=GDP&api_key='
            }
            
            if service_name not in test_urls:
                return
            
            base_url = test_urls[service_name]
            
            # Test each key
            for i, key in enumerate(service.keys):
                if isinstance(key, dict):
                    api_key = key.get('api_key', '')
                    key_id = key.get('key_id', f"{service_name}_{i}")
                else:
                    api_key = key
                    key_id = f"{service_name}_{i}"
                
                if not api_key:
                    continue
                
                try:
                    start_time = time.time()
                    
                    # Special handling for different APIs
                    if service_name == 'fred':
                        test_url = f"{base_url}{api_key}&file_type=json&limit=1"
                    else:
                        test_url = f"{base_url}{api_key}"
                    
                    async with self.session.get(test_url) as response:
                        response_time = time.time() - start_time
                        
                        # Update key status based on response
                        status = self.key_status.get(key_id)
                        if status:
                            if response.status == 200:
                                status.health_score = min(1.0, status.health_score + 0.1)
                                status.consecutive_failures = 0
                            elif response.status == 429:  # Rate limited
                                status.rate_limit_hit = True
                                status.rate_limit_reset = datetime.now() + timedelta(seconds=service.cooldown_period)
                            else:
                                status.consecutive_failures += 1
                                status.health_score = max(0.0, status.health_score - 0.1)
                            
                            status.avg_response_time = response_time
                
                except asyncio.TimeoutError:
                    status = self.key_status.get(key_id)
                    if status:
                        status.consecutive_failures += 1
                        status.health_score = max(0.0, status.health_score - 0.2)
                
                except Exception as e:
                    self.logger.warning(f"[API_ROTATION] Health check failed for {key_id}: {e}")
        
        except Exception as e:
            self.logger.error(f"[API_ROTATION] Health check error for {service_name}: {e}")
    
    def rotate_service_key(self, service_name: str):
        """Manually rotate to next key for a service"""
        if service_name not in self.services:
            return False
        
        service = self.services[service_name]
        if len(service.keys) <= 1:
            return False
        
        old_index = service.current_index
        service.current_index = (service.current_index + 1) % len(service.keys)
        
        self.logger.info(f"[API_ROTATION] Manually rotated {service_name} from key {old_index} to {service.current_index}")
        return True
    
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
    
    def get_rotation_statistics(self) -> Dict[str, Any]:
        """Get comprehensive rotation statistics"""
        stats = {
            'rotation_enabled': self.rotation_enabled,
            'rotation_strategy': self.rotation_strategy.value,
            'total_services': len(self.services),
            'total_keys': len(self.key_status),
            'last_health_check': self.last_health_check.isoformat(),
            'services_summary': {},
            'overall_health': 0.0
        }
        
        total_health = 0.0
        total_keys = 0
        
        for service_name, service in self.services.items():
            service_health_scores = []
            rate_limited_keys = 0
            total_requests = 0
            
            for key in service.keys:
                if isinstance(key, dict):
                    key_id = key.get('key_id', f"{service_name}_{len(service_health_scores)}")
                else:
                    key_id = f"{service_name}_{len(service_health_scores)}"
                
                status = self.key_status.get(key_id)
                if status:
                    service_health_scores.append(status.health_score)
                    if status.rate_limit_hit:
                        rate_limited_keys += 1
                    total_requests += status.requests_count
                    total_health += status.health_score
                    total_keys += 1
            
            avg_health = sum(service_health_scores) / len(service_health_scores) if service_health_scores else 0.0
            
            stats['services_summary'][service_name] = {
                'total_keys': len(service.keys),
                'current_key_index': service.current_index,
                'average_health': avg_health,
                'rate_limited_keys': rate_limited_keys,
                'total_requests': total_requests
            }
        
        stats['overall_health'] = total_health / total_keys if total_keys > 0 else 0.0
        
        return stats
    
    async def cleanup(self):
        """Clean up resources"""
        if self.session and not self.session.closed:
            await self.session.close()
        
        self.logger.info("[API_ROTATION] Cleanup completed")

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

if __name__ == "__main__":
    # Test the rotation manager
    import asyncio
    
    async def test_rotation():
        print("Testing API Rotation Manager")
        print("=" * 40)
        
        # Test getting keys for different services
        services = ['alpha_vantage', 'news_api', 'fred', 'twitter', 'binance']
        
        for service in services:
            key = get_api_key(service)
            if key:
                print(f"✓ {service}: Key available")
                
                # Simulate some API calls
                for i in range(3):
                    report_api_result(service, success=True, response_time=0.5)
                
                # Get status
                status = get_service_status(service)
                print(f"  Keys: {status.get('total_keys', 0)}")
            else:
                print(f"⚠ {service}: No valid keys configured")
        
        print("\nOverall Statistics:")
        stats = api_rotation_manager.get_rotation_statistics()
        print(f"Total Services: {stats['total_services']}")
        print(f"Total Keys: {stats['total_keys']}")
        print(f"Overall Health: {stats['overall_health']:.2f}")
        
        # Test health checks
        print("\nRunning health checks...")
        await api_rotation_manager.health_check_all_services()
        print("Health checks completed")
        
        # Cleanup
        await api_rotation_manager.cleanup()
    
    asyncio.run(test_rotation())