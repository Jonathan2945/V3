#!/usr/bin/env python3
"""
API Monitor - System and API Health Monitoring
==============================================
Monitors API health, system metrics, and logs events
"""

import asyncio
import sqlite3
import logging
import time
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import requests
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)

@dataclass
class APIHealthStatus:
    """API health status data"""
    name: str
    status: bool
    last_check: str
    response_time: float
    error_message: Optional[str] = None
    success_rate: float = 0.0

@dataclass
class SystemEvent:
    """System event data"""
    timestamp: str
    event_type: str
    component: str
    message: str
    severity: str
    metadata: Optional[Dict] = None

class APIMonitor:
    """Monitor API health and system events"""
    
    def __init__(self, api_db_path: str = "api_monitor.db", metrics_db_path: str = "system_metrics.db"):
        self.api_db_path = api_db_path
        self.metrics_db_path = metrics_db_path
        self.monitoring_active = False
        self.check_interval = 300  # 5 minutes
        
        # API endpoints to monitor
        self.api_endpoints = {
            'binance_testnet': {
                'name': 'Binance Testnet',
                'url': 'https://testnet.binance.vision/api/v3/ping',
                'timeout': 10
            },
            'binance_live': {
                'name': 'Binance Live',
                'url': 'https://api.binance.com/api/v3/ping',
                'timeout': 10
            },
            'alpha_vantage': {
                'name': 'Alpha Vantage',
                'url': 'https://www.alphavantage.co',
                'timeout': 10
            },
            'news_api': {
                'name': 'NewsAPI',
                'url': 'https://newsapi.org',
                'timeout': 10
            },
            'fred': {
                'name': 'FRED Economic Data',
                'url': 'https://api.stlouisfed.org',
                'timeout': 10
            },
            'reddit': {
                'name': 'Reddit API',
                'url': 'https://www.reddit.com',
                'timeout': 10
            },
            'twitter': {
                'name': 'Twitter API',
                'url': 'https://api.twitter.com',
                'timeout': 10
            }
        }
        
        self._initialize_databases()
        logger.info("API Monitor initialized")
    
    def _initialize_databases(self):
        """Initialize monitoring databases"""
        try:
            # API monitoring database
            with sqlite3.connect(self.api_db_path) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS api_health (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        api_name TEXT NOT NULL,
                        status BOOLEAN NOT NULL,
                        response_time REAL,
                        error_message TEXT,
                        timestamp TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS api_config (
                        api_name TEXT PRIMARY KEY,
                        display_name TEXT,
                        url TEXT,
                        enabled BOOLEAN DEFAULT TRUE,
                        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Insert default API configurations
                for api_name, config in self.api_endpoints.items():
                    conn.execute("""
                        INSERT OR REPLACE INTO api_config (api_name, display_name, url)
                        VALUES (?, ?, ?)
                    """, (api_name, config['name'], config['url']))
                
                conn.commit()
            
            # System metrics database
            with sqlite3.connect(self.metrics_db_path) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS system_events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT NOT NULL,
                        event_type TEXT NOT NULL,
                        component TEXT NOT NULL,
                        message TEXT NOT NULL,
                        severity TEXT NOT NULL,
                        metadata TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS system_metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT NOT NULL,
                        metric_name TEXT NOT NULL,
                        metric_value REAL NOT NULL,
                        component TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                conn.commit()
            
            logger.info("✅ API Monitor databases initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize databases: {e}")
    
    async def check_api_health(self, api_name: str, config: Dict) -> APIHealthStatus:
        """Check health of a specific API"""
        start_time = time.time()
        
        try:
            # Skip check if we don't have API keys for this service
            if not self._has_api_keys(api_name):
                return APIHealthStatus(
                    name=config['name'],
                    status=False,
                    last_check=datetime.now().isoformat(),
                    response_time=0.0,
                    error_message="No API keys configured",
                    success_rate=0.0
                )
            
            response = requests.get(
                config['url'],
                timeout=config.get('timeout', 10),
                headers={'User-Agent': 'TradingSystem/1.0'}
            )
            
            response_time = time.time() - start_time
            
            # Consider 2xx and some 4xx responses as "API is reachable"
            status = response.status_code < 500
            error_message = None if status else f"HTTP {response.status_code}"
            
        except requests.exceptions.Timeout:
            response_time = time.time() - start_time
            status = False
            error_message = "Timeout"
            
        except requests.exceptions.ConnectionError:
            response_time = time.time() - start_time
            status = False
            error_message = "Connection failed"
            
        except Exception as e:
            response_time = time.time() - start_time
            status = False
            error_message = str(e)
        
        # Calculate success rate from recent history
        success_rate = self._calculate_success_rate(api_name)
        
        health_status = APIHealthStatus(
            name=config['name'],
            status=status,
            last_check=datetime.now().isoformat(),
            response_time=response_time,
            error_message=error_message,
            success_rate=success_rate
        )
        
        # Log the health check result
        self._log_health_check(api_name, health_status)
        
        return health_status
    
    def _has_api_keys(self, api_name: str) -> bool:
        """Check if API keys are configured for this service"""
        # Map API names to environment variable patterns
        key_patterns = {
            'binance_testnet': 'BINANCE_API_KEY_1',
            'binance_live': 'BINANCE_LIVE_API_KEY_1',
            'alpha_vantage': 'ALPHA_VANTAGE_API_KEY_1',
            'news_api': 'NEWS_API_KEY_1',
            'fred': 'FRED_API_KEY_1',
            'reddit': 'REDDIT_CLIENT_ID_1',
            'twitter': 'TWITTER_API_KEY_1'
        }
        
        pattern = key_patterns.get(api_name)
        if not pattern:
            return False
        
        key = os.getenv(pattern, '').strip()
        return bool(key and key != '')
    
    def _calculate_success_rate(self, api_name: str, hours: int = 24) -> float:
        """Calculate success rate for API over recent period"""
        try:
            cutoff_time = (datetime.now() - timedelta(hours=hours)).isoformat()
            
            with sqlite3.connect(self.api_db_path) as conn:
                cursor = conn.execute("""
                    SELECT COUNT(*) as total, 
                           SUM(CASE WHEN status = 1 THEN 1 ELSE 0 END) as successful
                    FROM api_health 
                    WHERE api_name = ? AND timestamp > ?
                """, (api_name, cutoff_time))
                
                result = cursor.fetchone()
                if result and result[0] > 0:
                    return (result[1] / result[0]) * 100
                
        except Exception as e:
            logger.warning(f"Could not calculate success rate for {api_name}: {e}")
        
        return 0.0
    
    def _log_health_check(self, api_name: str, health_status: APIHealthStatus):
        """Log health check result to database"""
        try:
            with sqlite3.connect(self.api_db_path) as conn:
                conn.execute("""
                    INSERT INTO api_health 
                    (api_name, status, response_time, error_message, timestamp)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    api_name,
                    health_status.status,
                    health_status.response_time,
                    health_status.error_message,
                    health_status.last_check
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to log health check for {api_name}: {e}")
    
    async def check_all_apis(self) -> Dict[str, APIHealthStatus]:
        """Check health of all configured APIs"""
        results = {}
        
        for api_name, config in self.api_endpoints.items():
            try:
                health_status = await self.check_api_health(api_name, config)
                results[api_name] = health_status
            except Exception as e:
                logger.error(f"Health check failed for {api_name}: {e}")
                results[api_name] = APIHealthStatus(
                    name=config['name'],
                    status=False,
                    last_check=datetime.now().isoformat(),
                    response_time=0.0,
                    error_message=f"Check failed: {e}"
                )
        
        return results
    
    def get_current_status(self) -> Dict[str, Any]:
        """Get current status of all APIs"""
        api_status = {}
        online_count = 0
        total_count = len(self.api_endpoints)
        
        for api_name, config in self.api_endpoints.items():
            # Get latest status from database
            try:
                with sqlite3.connect(self.api_db_path) as conn:
                    cursor = conn.execute("""
                        SELECT status, response_time, error_message, timestamp
                        FROM api_health 
                        WHERE api_name = ? 
                        ORDER BY created_at DESC 
                        LIMIT 1
                    """, (api_name,))
                    
                    result = cursor.fetchone()
                    
                    if result:
                        status, response_time, error_message, timestamp = result
                        success_rate = self._calculate_success_rate(api_name)
                        
                        api_status[api_name] = {
                            'name': config['name'],
                            'status': bool(status),
                            'response_time': response_time,
                            'error_message': error_message,
                            'last_check': timestamp,
                            'success_rate': success_rate,
                            'has_keys': self._has_api_keys(api_name)
                        }
                        
                        if bool(status):
                            online_count += 1
                    else:
                        # No previous checks
                        api_status[api_name] = {
                            'name': config['name'],
                            'status': False,
                            'response_time': 0.0,
                            'error_message': 'Never checked',
                            'last_check': None,
                            'success_rate': 0.0,
                            'has_keys': self._has_api_keys(api_name)
                        }
                        
            except Exception as e:
                logger.error(f"Failed to get status for {api_name}: {e}")
                api_status[api_name] = {
                    'name': config['name'],
                    'status': False,
                    'response_time': 0.0,
                    'error_message': f'Status check failed: {e}',
                    'last_check': None,
                    'success_rate': 0.0,
                    'has_keys': self._has_api_keys(api_name)
                }
        
        return {
            'summary': {
                'total_apis': total_count,
                'online_apis': online_count,
                'offline_apis': total_count - online_count,
                'health_percentage': (online_count / total_count) * 100 if total_count > 0 else 0,
                'last_updated': datetime.now().isoformat()
            },
            'api_status': api_status
        }
    
    async def log_system_event(self, event_type: str, component: str, message: str, 
                              severity: str = 'INFO', metadata: Dict = None):
        """Log a system event"""
        try:
            event = SystemEvent(
                timestamp=datetime.now().isoformat(),
                event_type=event_type,
                component=component,
                message=message,
                severity=severity,
                metadata=metadata
            )
            
            with sqlite3.connect(self.metrics_db_path) as conn:
                conn.execute("""
                    INSERT INTO system_events 
                    (timestamp, event_type, component, message, severity, metadata)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    event.timestamp,
                    event.event_type,
                    event.component,
                    event.message,
                    event.severity,
                    json.dumps(event.metadata) if event.metadata else None
                ))
                conn.commit()
            
            logger.info(f"System event logged: {component} - {event_type}")
            
        except Exception as e:
            logger.error(f"Failed to log system event: {e}")
    
    async def log_metric(self, metric_name: str, value: float, component: str = None):
        """Log a system metric"""
        try:
            with sqlite3.connect(self.metrics_db_path) as conn:
                conn.execute("""
                    INSERT INTO system_metrics 
                    (timestamp, metric_name, metric_value, component)
                    VALUES (?, ?, ?, ?)
                """, (
                    datetime.now().isoformat(),
                    metric_name,
                    value,
                    component
                ))
                conn.commit()
                
        except Exception as e:
            logger.error(f"Failed to log metric {metric_name}: {e}")
    
    def get_system_events(self, hours: int = 24, severity: str = None) -> List[Dict]:
        """Get recent system events"""
        try:
            cutoff_time = (datetime.now() - timedelta(hours=hours)).isoformat()
            
            with sqlite3.connect(self.metrics_db_path) as conn:
                if severity:
                    cursor = conn.execute("""
                        SELECT timestamp, event_type, component, message, severity, metadata
                        FROM system_events 
                        WHERE timestamp > ? AND severity = ?
                        ORDER BY created_at DESC 
                        LIMIT 100
                    """, (cutoff_time, severity))
                else:
                    cursor = conn.execute("""
                        SELECT timestamp, event_type, component, message, severity, metadata
                        FROM system_events 
                        WHERE timestamp > ?
                        ORDER BY created_at DESC 
                        LIMIT 100
                    """, (cutoff_time,))
                
                events = []
                for row in cursor.fetchall():
                    timestamp, event_type, component, message, severity, metadata = row
                    events.append({
                        'timestamp': timestamp,
                        'event_type': event_type,
                        'component': component,
                        'message': message,
                        'severity': severity,
                        'metadata': json.loads(metadata) if metadata else None
                    })
                
                return events
                
        except Exception as e:
            logger.error(f"Failed to get system events: {e}")
            return []
    
    async def start_monitoring(self):
        """Start continuous API monitoring"""
        if self.monitoring_active:
            logger.warning("Monitoring already active")
            return
        
        self.monitoring_active = True
        logger.info("Starting API monitoring...")
        
        while self.monitoring_active:
            try:
                await self.log_system_event('MONITORING_CYCLE', 'APIMonitor', 'Starting health checks')
                
                # Check all APIs
                health_results = await self.check_all_apis()
                
                # Log summary
                online_count = sum(1 for status in health_results.values() if status.status)
                total_count = len(health_results)
                
                await self.log_system_event(
                    'HEALTH_CHECK_COMPLETE', 
                    'APIMonitor', 
                    f'Health check complete: {online_count}/{total_count} APIs online',
                    metadata={'online_count': online_count, 'total_count': total_count}
                )
                
                # Wait for next check
                await asyncio.sleep(self.check_interval)
                
            except Exception as e:
                logger.error(f"Error in monitoring cycle: {e}")
                await self.log_system_event(
                    'MONITORING_ERROR', 
                    'APIMonitor', 
                    f'Monitoring error: {e}', 
                    'ERROR'
                )
                await asyncio.sleep(60)  # Wait 1 minute on error
    
    def stop_monitoring(self):
        """Stop continuous monitoring"""
        self.monitoring_active = False
        logger.info("API monitoring stopped")
    
    def get_api_history(self, api_name: str, hours: int = 24) -> List[Dict]:
        """Get health history for specific API"""
        try:
            cutoff_time = (datetime.now() - timedelta(hours=hours)).isoformat()
            
            with sqlite3.connect(self.api_db_path) as conn:
                cursor = conn.execute("""
                    SELECT timestamp, status, response_time, error_message
                    FROM api_health 
                    WHERE api_name = ? AND timestamp > ?
                    ORDER BY created_at DESC 
                    LIMIT 100
                """, (api_name, cutoff_time))
                
                history = []
                for row in cursor.fetchall():
                    timestamp, status, response_time, error_message = row
                    history.append({
                        'timestamp': timestamp,
                        'status': bool(status),
                        'response_time': response_time,
                        'error_message': error_message
                    })
                
                return history
                
        except Exception as e:
            logger.error(f"Failed to get API history for {api_name}: {e}")
            return []
    
    def cleanup_old_data(self, days: int = 30):
        """Clean up old monitoring data"""
        try:
            cutoff_time = (datetime.now() - timedelta(days=days)).isoformat()
            
            # Clean API health data
            with sqlite3.connect(self.api_db_path) as conn:
                cursor = conn.execute("DELETE FROM api_health WHERE timestamp < ?", (cutoff_time,))
                api_deleted = cursor.rowcount
                conn.commit()
            
            # Clean system events
            with sqlite3.connect(self.metrics_db_path) as conn:
                cursor = conn.execute("DELETE FROM system_events WHERE timestamp < ?", (cutoff_time,))
                events_deleted = cursor.rowcount
                
                cursor = conn.execute("DELETE FROM system_metrics WHERE timestamp < ?", (cutoff_time,))
                metrics_deleted = cursor.rowcount
                
                conn.commit()
            
            logger.info(f"Cleanup complete: {api_deleted} API records, {events_deleted} events, {metrics_deleted} metrics deleted")
            
        except Exception as e:
            logger.error(f"Failed to clean up old data: {e}")

# Global instance
api_monitor = APIMonitor()

if __name__ == "__main__":
    # Test the API monitor
    async def test_api_monitor():
        monitor = APIMonitor("test_api.db", "test_metrics.db")
        
        print("Testing API health checks...")
        
        # Test individual API check
        binance_health = await monitor.check_api_health('binance_testnet', {
            'name': 'Binance Testnet',
            'url': 'https://testnet.binance.vision/api/v3/ping',
            'timeout': 10
        })
        
        print(f"Binance Testnet: {'✅' if binance_health.status else '❌'} ({binance_health.response_time:.2f}s)")
        
        # Test all APIs
        all_status = await monitor.check_all_apis()
        print(f"\nAll APIs checked: {len(all_status)} APIs")
        
        for api_name, status in all_status.items():
            print(f"  {status.name}: {'✅' if status.status else '❌'}")
        
        # Test current status
        current = monitor.get_current_status()
        print(f"\nCurrent Status: {current['summary']['online_apis']}/{current['summary']['total_apis']} APIs online")
        
        # Test event logging
        await monitor.log_system_event('TEST_EVENT', 'TestComponent', 'This is a test event', 'INFO')
        print("✅ Event logging test completed")
        
        # Test metric logging
        await monitor.log_metric('test_metric', 42.5, 'TestComponent')
        print("✅ Metric logging test completed")
        
        # Clean up test databases
        import os
        try:
            os.unlink("test_api.db")
            os.unlink("test_metrics.db")
        except:
            pass
        
        print("✅ API Monitor tests completed")
    
    asyncio.run(test_api_monitor())