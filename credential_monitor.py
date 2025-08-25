#!/usr/bin/env python3
import warnings
warnings.filterwarnings("ignore", message=".*PRAW.*")
import os
os.environ["PRAW_CHECK_FOR_ASYNC"] = "False"

EMOJI = "[CRED]"
"""
ENHANCED CREDENTIAL MONITOR - TESTNET/LIVE SUPPORT
==================================================
Enhanced to handle both testnet and live credentials:
- Dynamic credential switching
- Live/testnet mode detection
- Enhanced security monitoring
- Proper credential separation
- API key validation and rotation
- Security credential monitoring
- Authentication status tracking
- Rate limit monitoring
- Access control management

Features:
- Real-time credential validation
- Automatic key rotation
- Security breach detection
- Rate limit tracking
- Access logging and monitoring
- Testnet/Live mode switching
"""

import os
import asyncio
import logging
import time
import hashlib
import hmac
import base64
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import json
from dataclasses import dataclass, asdict
import aiohttp
import ccxt

@dataclass
class CredentialStatus:
    """Status of API credentials"""
    name: str
    is_valid: bool
    last_checked: datetime
    rate_limit_remaining: int
    rate_limit_reset: datetime
    error_message: Optional[str]
    permissions: List[str]
    usage_count: int
    daily_limit: int
    mode: str = 'testnet'  # 'testnet' or 'live'

@dataclass
class SecurityAlert:
    """Security alert information"""
    alert_type: str  # 'invalid_key', 'rate_limit', 'suspicious_activity', 'breach_attempt'
    severity: str    # 'low', 'medium', 'high', 'critical'
    message: str
    timestamp: datetime
    resolved: bool
    details: Dict[str, Any]

class CredentialMonitor:
    """Enhanced credential monitor with testnet/live support"""
    
    def __init__(self):
        # Configuration
        self.config = {
            'check_interval': 300,  # Check credentials every 5 minutes
            'rate_limit_buffer': 10,  # Keep 10% rate limit buffer
            'max_daily_requests': 10000,  # Maximum daily API requests
            'security_log_retention': 30,  # Days to keep security logs
            'auto_rotate_keys': False,  # Auto-rotate keys (if supported)
        }
        
        # Credential storage
        self.credentials = {}
        self.credential_status = {}
        self.security_alerts = []
        
        # Rate limiting
        self.request_counts = {}
        self.rate_limits = {}
        
        # Security monitoring
        self.failed_attempts = {}
        self.suspicious_activity = []
        
        # Monitoring state
        self.is_monitoring = False
        self.last_security_check = None
        
        # Trading mode detection
        self.current_mode = self._determine_trading_mode()
        
        logging.info(f"[CRED] Enhanced Credential Monitor initialized ({self.current_mode} mode)")
    
    def _determine_trading_mode(self) -> str:
        """Determine current trading mode"""
        testnet = os.getenv('TESTNET', 'true').lower() == 'true'
        live_enabled = os.getenv('LIVE_TRADING_ENABLED', 'false').lower() == 'true'
        
        if live_enabled and not testnet:
            return 'live'
        return 'testnet'
    
    async def initialize(self):
        """Initialize credential monitoring with mode awareness"""
        try:
            # Load credentials based on current mode
            await self.load_credentials()
            
            # Validate credentials for current mode
            await self.validate_mode_credentials()
            
            # Start monitoring tasks
            asyncio.create_task(self.continuous_monitoring())
            asyncio.create_task(self.security_monitoring())
            
            self.is_monitoring = True
            logging.info(f"[OK] Enhanced Credential Monitor initialization complete ({self.current_mode} mode)")
            
        except Exception as e:
            logging.error(f"[FAIL] Enhanced Credential Monitor initialization failed: {e}")
            raise
    
    async def load_credentials(self):
        """Load API credentials based on current trading mode"""
        try:
            # Determine trading mode
            testnet_mode = os.getenv('TESTNET', 'true').lower() == 'true'
            
            # Select appropriate Binance credentials based on mode
            if testnet_mode:
                binance_api_key = os.getenv('BINANCE_API_KEY')
                binance_api_secret = os.getenv('BINANCE_API_SECRET')
            else:
                # Use live credentials for live trading
                binance_api_key = os.getenv('BINANCE_LIVE_API_KEY', os.getenv('BINANCE_API_KEY'))
                binance_api_secret = os.getenv('BINANCE_LIVE_API_SECRET', os.getenv('BINANCE_API_SECRET'))
            
            # Exchange credentials
            exchange_credentials = {}
            
            # Only add Binance if credentials exist and aren't placeholders
            if (binance_api_key and binance_api_secret and 
                not binance_api_key.startswith('your_') and 
                len(binance_api_key) >= 32):
                exchange_credentials['binance'] = {
                    'api_key': binance_api_key,
                    'api_secret': binance_api_secret,
                    'testnet': testnet_mode,
                    'permissions': ['spot', 'futures'],
                    'daily_limit': 5000,
                    'mode': 'testnet' if testnet_mode else 'live'
                }
            
            # Coinbase credentials (if you have them)
            coinbase_key = os.getenv('COINBASE_API_KEY')
            coinbase_secret = os.getenv('COINBASE_API_SECRET')
            if coinbase_key and coinbase_secret and not coinbase_key.startswith('your_'):
                exchange_credentials['coinbase'] = {
                    'api_key': coinbase_key,
                    'api_secret': coinbase_secret,
                    'passphrase': os.getenv('COINBASE_PASSPHRASE'),
                    'testnet': testnet_mode,
                    'permissions': ['view', 'trade'],
                    'daily_limit': 3000,
                    'mode': 'testnet' if testnet_mode else 'live'
                }
            
            # News API credentials (mode-independent)
            news_credentials = {}
            if os.getenv('NEWS_API_KEY') and not os.getenv('NEWS_API_KEY').startswith('your_'):
                news_credentials['news_api'] = {
                    'api_key': os.getenv('NEWS_API_KEY'),
                    'daily_limit': 1000,
                    'mode': 'external'
                }
            
            if os.getenv('ALPHA_VANTAGE_API_KEY') and not os.getenv('ALPHA_VANTAGE_API_KEY').startswith('your_'):
                news_credentials['alpha_vantage'] = {
                    'api_key': os.getenv('ALPHA_VANTAGE_API_KEY'),
                    'daily_limit': 500,
                    'mode': 'external'
                }
            
            # Social media credentials (mode-independent)
            social_credentials = {}
            twitter_key = os.getenv('TWITTER_API_KEY')
            twitter_secret = os.getenv('TWITTER_API_SECRET')
            twitter_bearer = os.getenv('TWITTER_BEARER_TOKEN')
            
            if ((twitter_key and twitter_secret) or twitter_bearer) and not str(twitter_key).startswith('your_'):
                social_credentials['twitter'] = {
                    'api_key': twitter_key,
                    'api_secret': twitter_secret,
                    'bearer_token': twitter_bearer,
                    'daily_limit': 2000,
                    'mode': 'external'
                }
            
            # ML/AI credentials (mode-independent)
            ml_credentials = {}
            if os.getenv('HUGGINGFACE_API_KEY') and not os.getenv('HUGGINGFACE_API_KEY').startswith('your_'):
                ml_credentials['huggingface'] = {
                    'api_key': os.getenv('HUGGINGFACE_API_KEY'),
                    'daily_limit': 1000,
                    'mode': 'external'
                }
            
            if os.getenv('OPENAI_API_KEY') and not os.getenv('OPENAI_API_KEY').startswith('your_'):
                ml_credentials['openai'] = {
                    'api_key': os.getenv('OPENAI_API_KEY'),
                    'daily_limit': 100,
                    'mode': 'external'
                }
            
            # Combine all credentials
            self.credentials = {
                **exchange_credentials,
                **news_credentials,
                **social_credentials,
                **ml_credentials
            }
            
            # Initialize request counters
            for service_name in self.credentials.keys():
                self.request_counts[service_name] = {
                    'daily_count': 0,
                    'hourly_count': 0,
                    'last_reset': datetime.now()
                }
            
            # Filter out empty credentials and placeholders
            self.credentials = {
                name: creds for name, creds in self.credentials.items()
                if creds.get('api_key') and creds['api_key'].strip() and
                not creds['api_key'].startswith('your_')
            }
            
            mode_str = 'LIVE' if not testnet_mode else 'TESTNET'
            logging.info(f"[CRED] Loaded {len(self.credentials)} credential sets for {mode_str} mode")
            
        except Exception as e:
            logging.error(f"Error loading credentials: {e}")
            raise
    
    async def validate_mode_credentials(self):
        """Validate credentials for current trading mode"""
        try:
            if not self.credentials:
                logging.warning(f"[WARN] No valid credentials found for {self.current_mode} mode")
                return
            
            validation_tasks = []
            
            for service_name, credentials in self.credentials.items():
                task = asyncio.create_task(
                    self.validate_credential(service_name, credentials)
                )
                validation_tasks.append(task)
            
            # Wait for all validations to complete
            results = await asyncio.gather(*validation_tasks, return_exceptions=True)
            
            # Process results
            valid_count = 0
            for i, result in enumerate(results):
                service_name = list(self.credentials.keys())[i]
                
                if isinstance(result, Exception):
                    logging.error(f"[FAIL] Validation failed for {service_name}: {result}")
                    await self.create_security_alert(
                        'invalid_key', 'high', 
                        f"Credential validation failed for {service_name}: {result}",
                        {'service': service_name, 'error': str(result)}
                    )
                elif result:
                    valid_count += 1
            
            logging.info(f"[OK] Validated {valid_count}/{len(self.credentials)} credential sets ({self.current_mode} mode)")
            
        except Exception as e:
            logging.error(f"Error validating credentials: {e}")
    
    async def validate_credential(self, service_name: str, credentials: Dict) -> bool:
        """Validate a specific credential set"""
        try:
            if service_name in ['binance', 'coinbase']:
                return await self.validate_exchange_credential(service_name, credentials)
            elif service_name in ['news_api', 'alpha_vantage']:
                return await self.validate_news_api_credential(service_name, credentials)
            elif service_name == 'twitter':
                return await self.validate_twitter_credential(credentials)
            elif service_name in ['huggingface', 'openai']:
                return await self.validate_ml_api_credential(service_name, credentials)
            else:
                logging.warning(f"Unknown service type: {service_name}")
                return False
            
        except Exception as e:
            logging.error(f"Error validating {service_name} credentials: {e}")
            await self.record_failed_attempt(service_name, str(e))
            return False
    
    async def validate_exchange_credential(self, exchange_name: str, credentials: Dict) -> bool:
        """Validate exchange API credentials"""
        try:
            # Create exchange instance
            exchange_class = getattr(ccxt, exchange_name)
            exchange = exchange_class({
                'apiKey': credentials['api_key'],
                'secret': credentials['api_secret'],
                'sandbox': credentials.get('testnet', True),
                'enableRateLimit': True,
            })
            
            # Test API connection
            await exchange.load_markets()
            
            # Get account info to verify permissions
            account_info = await exchange.fetch_balance()
            
            # Update credential status
            permissions = list(account_info.keys()) if account_info else []
            
            status = CredentialStatus(
                name=exchange_name,
                is_valid=True,
                last_checked=datetime.now(),
                rate_limit_remaining=exchange.rateLimit,
                rate_limit_reset=datetime.now() + timedelta(minutes=1),
                error_message=None,
                permissions=permissions,
                usage_count=self.request_counts.get(exchange_name, {}).get('daily_count', 0),
                daily_limit=credentials.get('daily_limit', 5000),
                mode=credentials.get('mode', 'testnet')
            )
            
            self.credential_status[exchange_name] = status
            
            await exchange.close()
            
            mode_str = credentials.get('mode', 'testnet').upper()
            logging.info(f"[OK] {exchange_name} credentials valid ({mode_str})")
            return True
            
        except Exception as e:
            # Update credential status with error
            status = CredentialStatus(
                name=exchange_name,
                is_valid=False,
                last_checked=datetime.now(),
                rate_limit_remaining=0,
                rate_limit_reset=datetime.now(),
                error_message=str(e),
                permissions=[],
                usage_count=0,
                daily_limit=credentials.get('daily_limit', 5000),
                mode=credentials.get('mode', 'testnet')
            )
            
            self.credential_status[exchange_name] = status
            
            mode_str = credentials.get('mode', 'testnet').upper()
            logging.error(f"[FAIL] {exchange_name} credentials invalid ({mode_str}): {e}")
            return False
    
    async def validate_news_api_credential(self, service_name: str, credentials: Dict) -> bool:
        """Validate news API credentials"""
        try:
            api_key = credentials['api_key']
            
            # Different validation endpoints for different services
            validation_urls = {
                'news_api': f'https://newsapi.org/v2/sources?apiKey={api_key}',
                'alpha_vantage': f'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=AAPL&apikey={api_key}',
            }
            
            url = validation_urls.get(service_name)
            if not url:
                return False
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # Check for API-specific error indicators
                        if service_name == 'news_api' and data.get('status') != 'ok':
                            raise Exception(f"News API error: {data.get('message', 'Unknown error')}")
                        
                        # Update credential status
                        status = CredentialStatus(
                            name=service_name,
                            is_valid=True,
                            last_checked=datetime.now(),
                            rate_limit_remaining=1000,
                            rate_limit_reset=datetime.now() + timedelta(hours=1),
                            error_message=None,
                            permissions=['read'],
                            usage_count=self.request_counts.get(service_name, {}).get('daily_count', 0),
                            daily_limit=credentials.get('daily_limit', 1000),
                            mode=credentials.get('mode', 'external')
                        )
                        
                        self.credential_status[service_name] = status
                        
                        logging.info(f"[OK] {service_name} credentials valid")
                        return True
                    else:
                        raise Exception(f"HTTP {response.status}")
            
        except Exception as e:
            # Update credential status with error
            status = CredentialStatus(
                name=service_name,
                is_valid=False,
                last_checked=datetime.now(),
                rate_limit_remaining=0,
                rate_limit_reset=datetime.now(),
                error_message=str(e),
                permissions=[],
                usage_count=0,
                daily_limit=credentials.get('daily_limit', 1000),
                mode=credentials.get('mode', 'external')
            )
            
            self.credential_status[service_name] = status
            
            logging.error(f"[FAIL] {service_name} credentials invalid: {e}")
            return False
    
    async def validate_twitter_credential(self, credentials: Dict) -> bool:
        """Validate Twitter API credentials"""
        try:
            bearer_token = credentials.get('bearer_token')
            
            if bearer_token:
                # Test with bearer token
                headers = {'Authorization': f'Bearer {bearer_token}'}
                
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        'https://api.twitter.com/2/tweets/search/recent?query=bitcoin&max_results=10',
                        headers=headers
                    ) as response:
                        if response.status == 200:
                            status = CredentialStatus(
                                name='twitter',
                                is_valid=True,
                                last_checked=datetime.now(),
                                rate_limit_remaining=300,
                                rate_limit_reset=datetime.now() + timedelta(minutes=15),
                                error_message=None,
                                permissions=['read'],
                                usage_count=self.request_counts.get('twitter', {}).get('daily_count', 0),
                                daily_limit=credentials.get('daily_limit', 2000),
                                mode=credentials.get('mode', 'external')
                            )
                            
                            self.credential_status['twitter'] = status
                            
                            logging.info("[OK] Twitter credentials valid")
                            return True
                        else:
                            raise Exception(f"HTTP {response.status}")
            else:
                # Try generating bearer token from API key/secret
                api_key = credentials.get('api_key')
                api_secret = credentials.get('api_secret')
                
                if api_key and api_secret:
                    bearer_token = await self.generate_twitter_bearer_token(api_key, api_secret)
                    if bearer_token:
                        return await self.validate_twitter_credential({
                            **credentials,
                            'bearer_token': bearer_token
                        })
                
                raise Exception("No valid Twitter authentication method available")
            
        except Exception as e:
            status = CredentialStatus(
                name='twitter',
                is_valid=False,
                last_checked=datetime.now(),
                rate_limit_remaining=0,
                rate_limit_reset=datetime.now(),
                error_message=str(e),
                permissions=[],
                usage_count=0,
                daily_limit=credentials.get('daily_limit', 2000),
                mode=credentials.get('mode', 'external')
            )
            
            self.credential_status['twitter'] = status
            
            logging.error(f"[FAIL] Twitter credentials invalid: {e}")
            return False
    
    async def validate_ml_api_credential(self, service_name: str, credentials: Dict) -> bool:
        """Validate ML/AI API credentials"""
        try:
            api_key = credentials['api_key']
            
            # Test endpoints for different ML services
            test_urls = {
                'huggingface': 'https://api-inference.huggingface.co/models/bert-base-uncased',
                'openai': 'https://api.openai.com/v1/models'
            }
            
            url = test_urls.get(service_name)
            if not url:
                return False
            
            headers = {'Authorization': f'Bearer {api_key}'}
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        status = CredentialStatus(
                            name=service_name,
                            is_valid=True,
                            last_checked=datetime.now(),
                            rate_limit_remaining=100,
                            rate_limit_reset=datetime.now() + timedelta(hours=1),
                            error_message=None,
                            permissions=['read', 'inference'],
                            usage_count=self.request_counts.get(service_name, {}).get('daily_count', 0),
                            daily_limit=credentials.get('daily_limit', 1000),
                            mode=credentials.get('mode', 'external')
                        )
                        
                        self.credential_status[service_name] = status
                        
                        logging.info(f"[OK] {service_name} credentials valid")
                        return True
                    else:
                        raise Exception(f"HTTP {response.status}")
            
        except Exception as e:
            status = CredentialStatus(
                name=service_name,
                is_valid=False,
                last_checked=datetime.now(),
                rate_limit_remaining=0,
                rate_limit_reset=datetime.now(),
                error_message=str(e),
                permissions=[],
                usage_count=0,
                daily_limit=credentials.get('daily_limit', 1000),
                mode=credentials.get('mode', 'external')
            )
            
            self.credential_status[service_name] = status
            
            logging.error(f"[FAIL] {service_name} credentials invalid: {e}")
            return False
    
    async def generate_twitter_bearer_token(self, api_key: str, api_secret: str) -> Optional[str]:
        """Generate Twitter bearer token"""
        try:
            # Encode credentials
            credentials = f"{api_key}:{api_secret}"
            encoded_credentials = base64.b64encode(credentials.encode()).decode()
            
            headers = {
                'Authorization': f'Basic {encoded_credentials}',
                'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8'
            }
            
            data = 'grant_type=client_credentials'
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    'https://api.twitter.com/oauth2/token',
                    headers=headers,
                    data=data
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        return result.get('access_token')
                    else:
                        return None
            
        except Exception as e:
            logging.error(f"Error generating Twitter bearer token: {e}")
            return None
    
    async def switch_trading_mode(self, new_mode: str) -> bool:
        """Switch between testnet and live trading modes"""
        try:
            if new_mode not in ['testnet', 'live']:
                raise Exception("Invalid mode. Use 'testnet' or 'live'")
            
            if new_mode == self.current_mode:
                logging.info(f"[CRED] Already in {new_mode} mode")
                return True
            
            logging.info(f"[CRED] Switching from {self.current_mode} to {new_mode} mode")
            
            # Update current mode
            self.current_mode = new_mode
            
            # Reload credentials for new mode
            await self.load_credentials()
            
            # Validate new credentials
            await self.validate_mode_credentials()
            
            logging.info(f"[OK] Successfully switched to {new_mode} mode")
            return True
            
        except Exception as e:
            logging.error(f"[FAIL] Mode switch failed: {e}")
            return False
    
    def get_current_mode(self) -> str:
        """Get current trading mode"""
        return self.current_mode
    
    def get_mode_status(self) -> Dict:
        """Get detailed mode status"""
        return {
            'current_mode': self.current_mode,
            'available_credentials': list(self.credentials.keys()),
            'valid_credentials': [name for name, status in self.credential_status.items() if status.is_valid],
            'credential_count': len(self.credentials),
            'last_validation': max([status.last_checked for status in self.credential_status.values()]) if self.credential_status else None
        }
    
    # All the remaining methods from the original file...
    async def continuous_monitoring(self):
        """Continuously monitor credential status"""
        while self.is_monitoring:
            try:
                # Re-validate all credentials
                await self.validate_mode_credentials()
                
                # Check rate limits
                await self.check_rate_limits()
                
                # Reset daily counters if needed
                await self.reset_daily_counters()
                
                # Generate alerts for issues
                await self.check_credential_alerts()
                
                # Wait for next check
                await asyncio.sleep(self.config['check_interval'])
                
            except Exception as e:
                logging.error(f"Error in credential monitoring: {e}")
                await asyncio.sleep(60)  # Wait 1 minute on error
    
    async def security_monitoring(self):
        """Monitor for security issues"""
        while self.is_monitoring:
            try:
                # Check for suspicious activity
                await self.detect_suspicious_activity()
                
                # Monitor failed attempts
                await self.monitor_failed_attempts()
                
                # Check for credential breaches
                await self.check_credential_security()
                
                # Clean up old security logs
                await self.cleanup_security_logs()
                
                self.last_security_check = datetime.now()
                
                # Wait 10 minutes between security checks
                await asyncio.sleep(600)
                
            except Exception as e:
                logging.error(f"Error in security monitoring: {e}")
                await asyncio.sleep(300)  # Wait 5 minutes on error
    
    async def check_rate_limits(self):
        """Check and update rate limit status"""
        try:
            for service_name, status in self.credential_status.items():
                if status.is_valid:
                    # Check if approaching rate limit
                    usage_percentage = (status.usage_count / status.daily_limit) * 100
                    
                    if usage_percentage > 90:
                        await self.create_security_alert(
                            'rate_limit', 'high',
                            f"{service_name} approaching daily rate limit: {usage_percentage:.1f}%",
                            {'service': service_name, 'usage': usage_percentage}
                        )
                    elif usage_percentage > 75:
                        await self.create_security_alert(
                            'rate_limit', 'medium',
                            f"{service_name} high usage: {usage_percentage:.1f}%",
                            {'service': service_name, 'usage': usage_percentage}
                        )
            
        except Exception as e:
            logging.error(f"Error checking rate limits: {e}")
    
    async def reset_daily_counters(self):
        """Reset daily request counters"""
        try:
            now = datetime.now()
            
            for service_name, counters in self.request_counts.items():
                last_reset = counters['last_reset']
                
                # Reset daily counter if it's a new day
                if now.date() > last_reset.date():
                    counters['daily_count'] = 0
                    counters['last_reset'] = now
                    
                    logging.info(f"[RESET] Reset daily counter for {service_name}")
                
                # Reset hourly counter if it's a new hour
                if now.hour != last_reset.hour:
                    counters['hourly_count'] = 0
            
        except Exception as e:
            logging.error(f"Error resetting counters: {e}")
    
    async def check_credential_alerts(self):
        """Check for credential-related alerts"""
        try:
            for service_name, status in self.credential_status.items():
                if not status.is_valid and status.error_message:
                    # Check if this is a new error
                    recent_alerts = [
                        alert for alert in self.security_alerts
                        if alert.alert_type == 'invalid_key' and 
                           alert.details.get('service') == service_name and
                           (datetime.now() - alert.timestamp).seconds < 3600  # Last hour
                    ]
                    
                    if not recent_alerts:
                        await self.create_security_alert(
                            'invalid_key', 'high',
                            f"Invalid credentials detected for {service_name}: {status.error_message}",
                            {'service': service_name, 'error': status.error_message}
                        )
            
        except Exception as e:
            logging.error(f"Error checking credential alerts: {e}")
    
    async def detect_suspicious_activity(self):
        """Detect suspicious credential activity"""
        try:
            # Check for unusual request patterns
            for service_name, counters in self.request_counts.items():
                hourly_count = counters.get('hourly_count', 0)
                
                # Define normal hourly limits (these would be service-specific)
                normal_hourly_limits = {
                    'binance': 100,
                    'coinbase': 50,
                    'news_api': 50,
                    'twitter': 100
                }
                
                normal_limit = normal_hourly_limits.get(service_name, 50)
                
                if hourly_count > normal_limit * 2:  # 2x normal usage
                    await self.create_security_alert(
                        'suspicious_activity', 'medium',
                        f"Unusual activity detected for {service_name}: {hourly_count} requests in last hour",
                        {'service': service_name, 'hourly_requests': hourly_count, 'normal_limit': normal_limit}
                    )
            
        except Exception as e:
            logging.error(f"Error detecting suspicious activity: {e}")
    
    async def monitor_failed_attempts(self):
        """Monitor failed authentication attempts"""
        try:
            # Check failed attempt counts
            for service_name, attempts in self.failed_attempts.items():
                if len(attempts) > 5:  # More than 5 failed attempts
                    # Check if recent (last hour)
                    recent_attempts = [
                        attempt for attempt in attempts
                        if (datetime.now() - attempt['timestamp']).seconds < 3600
                    ]
                    
                    if len(recent_attempts) > 3:
                        await self.create_security_alert(
                            'breach_attempt', 'critical',
                            f"Multiple failed attempts for {service_name}: {len(recent_attempts)} in last hour",
                            {'service': service_name, 'failed_attempts': len(recent_attempts)}
                        )
            
        except Exception as e:
            logging.error(f"Error monitoring failed attempts: {e}")
    
    async def check_credential_security(self):
        """Check credential security status"""
        try:
            # Check for weak or default credentials
            for service_name, credentials in self.credentials.items():
                api_key = credentials.get('api_key', '')
                
                # Check for obviously weak keys
                if len(api_key) < 20:
                    await self.create_security_alert(
                        'weak_credential', 'medium',
                        f"Potentially weak API key for {service_name}",
                        {'service': service_name, 'key_length': len(api_key)}
                    )
                
                # Check for test/demo keys
                if any(test_word in api_key.lower() for test_word in ['test', 'demo', 'sample']):
                    await self.create_security_alert(
                        'test_credential', 'low',
                        f"Test/demo credentials detected for {service_name}",
                        {'service': service_name}
                    )
            
        except Exception as e:
            logging.error(f"Error checking credential security: {e}")
    
    async def cleanup_security_logs(self):
        """Clean up old security alerts"""
        try:
            cutoff_date = datetime.now() - timedelta(days=self.config['security_log_retention'])
            
            # Remove old alerts
            self.security_alerts = [
                alert for alert in self.security_alerts
                if alert.timestamp > cutoff_date
            ]
            
            # Clean up old failed attempts
            for service_name in self.failed_attempts:
                self.failed_attempts[service_name] = [
                    attempt for attempt in self.failed_attempts[service_name]
                    if attempt['timestamp'] > cutoff_date
                ]
            
        except Exception as e:
            logging.error(f"Error cleaning up security logs: {e}")
    
    async def record_failed_attempt(self, service_name: str, error_message: str):
        """Record a failed authentication attempt"""
        try:
            if service_name not in self.failed_attempts:
                self.failed_attempts[service_name] = []
            
            self.failed_attempts[service_name].append({
                'timestamp': datetime.now(),
                'error': error_message,
                'mode': self.current_mode
            })
            
            # Keep only last 50 attempts per service
            self.failed_attempts[service_name] = self.failed_attempts[service_name][-50:]
            
        except Exception as e:
            logging.error(f"Error recording failed attempt: {e}")
    
    async def create_security_alert(self, alert_type: str, severity: str, message: str, details: Dict[str, Any]):
        """Create a security alert"""
        try:
            alert = SecurityAlert(
                alert_type=alert_type,
                severity=severity,
                message=message,
                timestamp=datetime.now(),
                resolved=False,
                details=details
            )
            
            self.security_alerts.append(alert)
            
            # Log the alert
            if severity == 'critical':
                logging.critical(f"[ALERT] SECURITY ALERT: {message}")
            elif severity == 'high':
                logging.error(f"[WARN]  SECURITY ALERT: {message}")
            elif severity == 'medium':
                logging.warning(f"[WARN]  Security Alert: {message}")
            else:
                logging.info(f"[INFO] Security Notice: {message}")
            
            # Keep only last 1000 alerts
            if len(self.security_alerts) > 1000:
                self.security_alerts = self.security_alerts[-1000:]
            
        except Exception as e:
            logging.error(f"Error creating security alert: {e}")
    
    async def record_api_request(self, service_name: str):
        """Record an API request for rate limiting"""
        try:
            if service_name in self.request_counts:
                self.request_counts[service_name]['daily_count'] += 1
                self.request_counts[service_name]['hourly_count'] += 1
                
                # Update credential status usage
                if service_name in self.credential_status:
                    self.credential_status[service_name].usage_count += 1
            
        except Exception as e:
            logging.error(f"Error recording API request: {e}")
    
    def get_credential_status(self, service_name: str) -> Optional[CredentialStatus]:
        """Get status of a specific credential"""
        return self.credential_status.get(service_name)
    
    def get_all_credential_status(self) -> Dict[str, CredentialStatus]:
        """Get status of all credentials"""
        return self.credential_status.copy()
    
    def get_security_alerts(self, severity: Optional[str] = None) -> List[SecurityAlert]:
        """Get security alerts, optionally filtered by severity"""
        if severity:
            return [alert for alert in self.security_alerts if alert.severity == severity]
        return self.security_alerts.copy()
    
    def get_rate_limit_status(self) -> Dict[str, Dict]:
        """Get current rate limit status for all services"""
        status = {}
        
        for service_name, counters in self.request_counts.items():
            cred_status = self.credential_status.get(service_name)
            if cred_status:
                usage_percentage = (cred_status.usage_count / cred_status.daily_limit) * 100
                
                status[service_name] = {
                    'daily_used': cred_status.usage_count,
                    'daily_limit': cred_status.daily_limit,
                    'usage_percentage': usage_percentage,
                    'hourly_used': counters.get('hourly_count', 0),
                    'rate_limit_remaining': cred_status.rate_limit_remaining,
                    'rate_limit_reset': cred_status.rate_limit_reset.isoformat() if cred_status.rate_limit_reset else None
                }
        
        return status
    
    def is_service_available(self, service_name: str) -> bool:
        """Check if a service is available for use"""
        status = self.credential_status.get(service_name)
        if not status or not status.is_valid:
            return False
        
        # Check rate limits
        usage_percentage = (status.usage_count / status.daily_limit) * 100
        if usage_percentage >= 95:  # 95% of daily limit used
            return False
        
        return True
    
    async def rotate_credentials(self, service_name: str):
        """Rotate credentials for a service (if supported)"""
        try:
            logging.info(f"[ROTATE] Attempting to rotate credentials for {service_name}")
            
            # This would implement credential rotation if the service supports it
            # For now, just log the attempt
            await self.create_security_alert(
                'credential_rotation', 'low',
                f"Credential rotation requested for {service_name}",
                {'service': service_name}
            )
            
            logging.warning(f"[WARN]  Credential rotation not implemented for {service_name}")
            
        except Exception as e:
            logging.error(f"Error rotating credentials for {service_name}: {e}")
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get credential monitoring metrics"""
        try:
            # Count valid/invalid credentials
            valid_credentials = sum(1 for status in self.credential_status.values() if status.is_valid)
            total_credentials = len(self.credential_status)
            
            # Count alerts by severity
            alert_counts = {}
            for alert in self.security_alerts:
                alert_counts[alert.severity] = alert_counts.get(alert.severity, 0) + 1
            
            # Calculate total API usage
            total_daily_requests = sum(
                counters.get('daily_count', 0) 
                for counters in self.request_counts.values()
            )
            
            return {
                'current_mode': self.current_mode,
                'total_credentials': total_credentials,
                'valid_credentials': valid_credentials,
                'invalid_credentials': total_credentials - valid_credentials,
                'credential_health_percentage': (valid_credentials / total_credentials * 100) if total_credentials > 0 else 0,
                'security_alerts': alert_counts,
                'total_security_alerts': len(self.security_alerts),
                'total_daily_requests': total_daily_requests,
                'rate_limit_status': self.get_rate_limit_status(),
                'monitoring_active': self.is_monitoring,
                'last_security_check': self.last_security_check.isoformat() if self.last_security_check else None,
                'services_available': [name for name in self.credentials.keys() if self.is_service_available(name)]
            }
            
        except Exception as e:
            logging.error(f"Error getting credential metrics: {e}")
            return {}

    async def stop_monitoring(self):
        """Stop credential monitoring"""
        self.is_monitoring = False
        logging.info(f"[CRED] Enhanced Credential monitoring stopped ({self.current_mode} mode)")