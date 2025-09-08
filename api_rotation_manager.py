#!/usr/bin/env python3
"""
API ROTATION MANAGER - INTELLIGENT API KEY CYCLING
==================================================
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
                rate_limit_threshold=5,
                cooldown_period=60,
                health_check_interval=300
            )
            self._initialize_key_status('alpha_vantage', alpha_vantage_keys)
    
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
        
        return service.keys[service.current_index] if service.keys else None
    
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

# Global instance for easy access
api_rotation_manager = APIRotationManager()

# Convenience functions for easy integration
def get_api_key(service_name: str) -> Optional[Any]:
    """Get API key for service"""
    return api_rotation_manager.get_api_key(service_name)

def report_api_result(service_name: str, success: bool, **kwargs):
    """Report API call result"""
    pass  # Simplified for now

def get_service_status(service_name: str) -> Dict[str, Any]:
    """Get service status"""
    return {'status': 'active', 'service': service_name}

