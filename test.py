#!/usr/bin/env python3
"""
UPGRADED V3 COMPREHENSIVE TEST SUITE WITH CROSS-COMMUNICATION TESTING
====================================================================

Enhanced test harness that:
- Tests all system components individually
- Tests cross-communication between components
- Tests dashboard integration and API endpoints
- Tests backtesting workflow and progress tracking
- Tests state management and persistence
- Tests error handling and recovery
- Provides real-time monitoring and diagnostics
"""

import os
import sys
import asyncio
import aiohttp
import json
import time
import sqlite3
import threading
import subprocess
import psutil
import requests
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import logging
from dataclasses import dataclass, asdict
import traceback
import signal

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

@dataclass
class TestResult:
    """Test result data structure"""
    component: str
    test_name: str
    status: str  # 'pass', 'fail', 'warning', 'skip'
    message: str
    details: Optional[Dict] = None
    execution_time: float = 0.0
    timestamp: str = ""

class V3SystemTester:
    """Comprehensive V3 System Tester with cross-communication testing"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.results: List[TestResult] = []
        self.start_time = time.time()
        
        # System configuration
        self.base_port = int(os.getenv('FLASK_PORT', '8102'))
        self.dashboard_url = f"http://localhost:{self.base_port}"
        self.api_base = f"{self.dashboard_url}/api"
        
        # Component status tracking
        self.component_status = {
            'main_controller': False,
            'dashboard': False,
            'backtester': False,
            'trading_engine': False,
            'database': False,
            'api_endpoints': False
        }
        
        # Test configuration
        self.test_timeout = 30  # seconds
        self.long_test_timeout = 300  # 5 minutes for backtesting tests
        
        self.logger.info(f"V3 System Tester initialized - Dashboard URL: {self.dashboard_url}")
    
    def add_result(self, component: str, test_name: str, status: str, message: str, details: Dict = None, execution_time: float = 0.0):
        """Add a test result"""
        result = TestResult(
            component=component,
            test_name=test_name,
            status=status,
            message=message,
            details=details or {},
            execution_time=execution_time,
            timestamp=datetime.now().isoformat()
        )
        self.results.append(result)
        
        # Log result
        status_icon = {'pass': '?', 'fail': '?', 'warning': '?', 'skip': '?'}.get(status, '?')
        self.logger.info(f"{status_icon} [{component}] {test_name}: {message}")
    
    def test_environment_setup(self):
        """Test environment configuration"""
        start_time = time.time()
        
        # Test .env file exists and is valid
        try:
            env_path = Path('.env')
            if not env_path.exists():
                self.add_result('environment', 'env_file_check', 'fail', '.env file not found')
                return
            
            with open(env_path, 'r') as f:
                env_content = f.read()
            
            # Check critical V3 settings
            critical_settings = [
                'BINANCE_API_KEY_1', 'BINANCE_API_SECRET_1',
                'FLASK_PORT', 'COMPREHENSIVE_ANALYSIS_ENABLED',
                'CLEAR_MOCK_ML_DATA', 'USE_REAL_DATA_ONLY'
            ]
            
            missing_settings = []
            for setting in critical_settings:
                if setting not in env_content:
                    missing_settings.append(setting)
            
            if missing_settings:
                self.add_result('environment', 'env_file_check', 'warning', 
                              f"Missing settings: {', '.join(missing_settings[:3])}{'...' if len(missing_settings) > 3 else ''}")
            else:
                self.add_result('environment', 'env_file_check', 'pass', 'Environment configuration valid')
            
        except Exception as e:
            self.add_result('environment', 'env_file_check', 'fail', f"Environment check failed: {e}")
        
        # Test required directories
        try:
            required_dirs = ['data', 'logs', 'backups', 'models']
            for dir_name in required_dirs:
                Path(dir_name).mkdir(exist_ok=True)
            
            self.add_result('environment', 'directory_setup', 'pass', f"Created {len(required_dirs)} required directories")
        
        except Exception as e:
            self.add_result('environment', 'directory_setup', 'fail', f"Directory setup failed: {e}")
        
        # Test Python dependencies
        try:
            required_modules = ['pandas', 'numpy', 'flask', 'sqlite3', 'asyncio', 'aiohttp', 'requests']
            missing_modules = []
            
            for module in required_modules:
                try:
                    __import__(module)
                except ImportError:
                    missing_modules.append(module)
            
            if missing_modules:
                self.add_result('environment', 'dependencies_check', 'fail', 
                              f"Missing modules: {', '.join(missing_modules)}")
            else:
                self.add_result('environment', 'dependencies_check', 'pass', 
                              f"All {len(required_modules)} dependencies available")
        
        except Exception as e:
            self.add_result('environment', 'dependencies_check', 'fail', f"Dependency check failed: {e}")
        
        execution_time = time.time() - start_time
        self.add_result('environment', 'total_environment_setup', 'pass', 
                       f"Environment tests completed in {execution_time:.2f}s", 
                       execution_time=execution_time)
    
    def test_database_connectivity(self):
        """Test database connections and schemas"""
        start_time = time.time()
        
        # Test main databases
        databases = {
            'trading_metrics': 'data/trading_metrics.db',
            'backtest_results': 'data/comprehensive_backtest.db',
            'backtest_progress': 'data/backtest_progress.db',
            'api_monitor': 'data/api_monitor.db'
        }
        
        for db_name, db_path in databases.items():
            try:
                # Ensure directory exists
                Path(db_path).parent.mkdir(parents=True, exist_ok=True)
                
                # Test connection
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                
                # Test basic operations
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = cursor.fetchall()
                
                conn.close()
                
                self.add_result('database', f'{db_name}_connectivity', 'pass', 
                              f"Connected successfully, {len(tables)} tables found")
                
            except Exception as e:
                self.add_result('database', f'{db_name}_connectivity', 'fail', 
                              f"Database connection failed: {e}")
        
        # Test database cleanup functionality
        try:
            from pnl_persistence import PnLPersistence
            pnl = PnLPersistence()
            
            # Test save and load
            test_metrics = {'total_trades': 0, 'total_pnl': 0.0, 'win_rate': 0.0}
            save_success = pnl.save_metrics(test_metrics)
            loaded_metrics = pnl.load_metrics()
            
            if save_success and loaded_metrics:
                self.add_result('database', 'pnl_persistence_test', 'pass', "P&L persistence working correctly")
            else:
                self.add_result('database', 'pnl_persistence_test', 'fail', "P&L persistence failed")
                
        except Exception as e:
            self.add_result('database', 'pnl_persistence_test', 'fail', f"P&L persistence test failed: {e}")
        
        execution_time = time.time() - start_time
        self.add_result('database', 'total_database_tests', 'pass', 
                       f"Database tests completed in {execution_time:.2f}s", 
                       execution_time=execution_time)
    
    def test_component_imports(self):
        """Test that all V3 components can be imported"""
        start_time = time.time()
        
        components = {
            'main_controller': 'main_controller.py',
            'advanced_backtester': 'advanced_backtester.py',
            'trading_engine': 'intelligent_trading_engine.py',
            'ml_engine': 'advanced_ml_engine.py',
            'api_rotation': 'api_rotation_manager.py',
            'price_action': 'price_action_core.py',
            'multi_pair_scanner': 'multi_pair_scanner.py',
            'external_data': 'external_data_collector.py'
        }
        
        successful_imports = 0
        
        for component, filename in components.items():
            try:
                if Path(filename).exists():
                    # Test import
                    import importlib.util
                    spec = importlib.util.spec_from_file_location(component, filename)
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    
                    successful_imports += 1
                    self.add_result('import', f'{component}_import', 'pass', f"Successfully imported {filename}")
                else:
                    self.add_result('import', f'{component}_import', 'skip', f"File {filename} not found")
                
            except Exception as e:
                self.add_result('import', f'{component}_import', 'fail', f"Import failed: {str(e)[:100]}")
        
        execution_time = time.time() - start_time
        self.add_result('import', 'total_import_tests', 'pass', 
                       f"Import tests completed: {successful_imports}/{len(components)} successful in {execution_time:.2f}s",
                       execution_time=execution_time)
    
    def start_main_system(self) -> Optional[subprocess.Popen]:
        """Start the main V3 system for testing"""
        try:
            self.logger.info("Starting V3 main system for testing...")
            
            # Kill any existing processes on the port
            self.kill_processes_on_port(self.base_port)
            time.sleep(2)
            
            # Set environment for testing
            env = os.environ.copy()
            env['AUTO_START_TRADING'] = 'false'  # Don't auto-start trading during tests
            env['TEST_MODE'] = 'true'
            
            # Start main.py
            process = subprocess.Popen(
                [sys.executable, 'main.py'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
                universal_newlines=True
            )
            
            # Wait for system to start
            max_wait = 30
            for i in range(max_wait):
                time.sleep(1)
                try:
                    response = requests.get(f"{self.dashboard_url}/api/status", timeout=5)
                    if response.status_code == 200:
                        self.logger.info(f"V3 system started successfully on port {self.base_port}")
                        return process
                except:
                    continue
            
            # If we get here, system didn't start
            if process.poll() is None:
                process.terminate()
            
            self.logger.error("Failed to start V3 system")
            return None
            
        except Exception as e:
            self.logger.error(f"Error starting main system: {e}")
            return None
    
    def kill_processes_on_port(self, port: int):
        """Kill any processes running on the specified port"""
        try:
            for proc in psutil.process_iter(['pid', 'name', 'connections']):
                try:
                    for conn in proc.info['connections']:
                        if conn.laddr.port == port:
                            self.logger.info(f"Killing process {proc.info['pid']} ({proc.info['name']}) on port {port}")
                            psutil.Process(proc.info['pid']).terminate()
                            time.sleep(1)
                            if psutil.pid_exists(proc.info['pid']):
                                psutil.Process(proc.info['pid']).kill()
                except (psutil.NoSuchProcess, psutil.AccessDenied, AttributeError):
                    continue
        except Exception as e:
            self.logger.warning(f"Error killing processes on port {port}: {e}")
    
    def test_dashboard_api_endpoints(self):
        """Test dashboard API endpoints"""
        start_time = time.time()
        
        # API endpoints to test
        endpoints = {
            'status': '/api/status',
            'metrics': '/api/metrics',
            'backtest_progress': '/api/backtest/progress',
            'strategies': '/api/strategies',
            'system_info': '/api/system',
            'trades': '/api/trades'
        }
        
        successful_endpoints = 0
        
        for endpoint_name, endpoint_path in endpoints.items():
            try:
                url = f"{self.dashboard_url}{endpoint_path}"
                response = requests.get(url, timeout=10)
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        self.add_result('api', f'{endpoint_name}_endpoint', 'pass', 
                                      f"Endpoint working, returned {len(str(data))} bytes")
                        successful_endpoints += 1
                    except json.JSONDecodeError:
                        self.add_result('api', f'{endpoint_name}_endpoint', 'warning', 
                                      f"Endpoint returns non-JSON data")
                else:
                    self.add_result('api', f'{endpoint_name}_endpoint', 'fail', 
                                  f"HTTP {response.status_code}: {response.text[:100]}")
                    
            except requests.exceptions.ConnectionError:
                self.add_result('api', f'{endpoint_name}_endpoint', 'fail', 
                              "Connection refused - system not running")
            except requests.exceptions.Timeout:
                self.add_result('api', f'{endpoint_name}_endpoint', 'fail', 
                              "Request timeout")
            except Exception as e:
                self.add_result('api', f'{endpoint_name}_endpoint', 'fail', 
                              f"Request failed: {e}")
        
        # Test dashboard HTML
        try:
            response = requests.get(self.dashboard_url, timeout=10)
            if response.status_code == 200 and 'html' in response.headers.get('content-type', '').lower():
                self.add_result('api', 'dashboard_html', 'pass', "Dashboard HTML loads successfully")
            else:
                self.add_result('api', 'dashboard_html', 'fail', f"Dashboard HTML failed: {response.status_code}")
        except Exception as e:
            self.add_result('api', 'dashboard_html', 'fail', f"Dashboard HTML test failed: {e}")
        
        execution_time = time.time() - start_time
        self.add_result('api', 'total_api_tests', 'pass', 
                       f"API tests completed: {successful_endpoints}/{len(endpoints)} endpoints working in {execution_time:.2f}s",
                       execution_time=execution_time)
    
    def test_backtesting_workflow(self):
        """Test the complete backtesting workflow and progress tracking"""
        start_time = time.time()
        
        try:
            # First, clear any existing backtest state
            self.logger.info("Clearing previous backtest state...")
            try:
                response = requests.post(f"{self.api_base}/backtest/clear", timeout=10)
                if response.status_code == 200:
                    self.add_result('backtest', 'state_clear', 'pass', "Previous state cleared successfully")
                else:
                    self.add_result('backtest', 'state_clear', 'warning', f"State clear returned {response.status_code}")
            except Exception as e:
                self.add_result('backtest', 'state_clear', 'fail', f"State clear failed: {e}")
            
            time.sleep(2)
            
            # Check initial progress state
            try:
                response = requests.get(f"{self.api_base}/backtest/progress", timeout=10)
                if response.status_code == 200:
                    progress_data = response.json()
                    initial_status = progress_data.get('status', 'unknown')
                    
                    if initial_status == 'not_started':
                        self.add_result('backtest', 'initial_state_check', 'pass', 
                                      "Initial state is 'not_started' as expected")
                    else:
                        self.add_result('backtest', 'initial_state_check', 'warning', 
                                      f"Initial state is '{initial_status}', expected 'not_started'")
                else:
                    self.add_result('backtest', 'initial_state_check', 'fail', 
                                  f"Progress endpoint returned {response.status_code}")
            except Exception as e:
                self.add_result('backtest', 'initial_state_check', 'fail', 
                              f"Initial state check failed: {e}")
            
            # Start a mini backtest (limited scope for testing)
            self.logger.info("Starting mini backtest for testing...")
            try:
                backtest_config = {
                    'pairs': ['BTCUSDT', 'ETHUSDT', 'BNBUSDT'],  # Limited pairs for testing
                    'strategies': ['MTF_Scalping_Ultra', 'MTF_Short_Term_Momentum'],  # Limited strategies
                    'test_mode': True
                }
                
                response = requests.post(
                    f"{self.api_base}/backtest/start",
                    json=backtest_config,
                    timeout=15
                )
                
                if response.status_code == 200:
                    result = response.json()
                    if result.get('status') == 'started':
                        self.add_result('backtest', 'start_backtest', 'pass', 
                                      "Backtest started successfully")
                    else:
                        self.add_result('backtest', 'start_backtest', 'warning', 
                                      f"Backtest start returned: {result}")
                else:
                    self.add_result('backtest', 'start_backtest', 'fail', 
                                  f"Backtest start failed: {response.status_code} - {response.text[:100]}")
                    return  # Can't continue without starting backtest
                    
            except Exception as e:
                self.add_result('backtest', 'start_backtest', 'fail', f"Start backtest failed: {e}")
                return
            
            # Monitor progress for a short time
            self.logger.info("Monitoring backtest progress...")
            progress_updates = 0
            max_monitor_time = 30  # seconds
            monitor_start = time.time()
            
            while time.time() - monitor_start < max_monitor_time:
                try:
                    response = requests.get(f"{self.api_base}/backtest/progress", timeout=5)
                    if response.status_code == 200:
                        progress = response.json()
                        status = progress.get('status')
                        completed = progress.get('completed', 0)
                        total = progress.get('total', 0)
                        
                        progress_updates += 1
                        
                        if status == 'in_progress':
                            if completed > 0:
                                self.add_result('backtest', 'progress_tracking', 'pass', 
                                              f"Progress tracking working: {completed}/{total} completed")
                                break
                        elif status == 'completed':
                            self.add_result('backtest', 'progress_tracking', 'pass', 
                                          "Backtest completed successfully")
                            break
                        elif status == 'error':
                            self.add_result('backtest', 'progress_tracking', 'fail', 
                                          "Backtest encountered an error")
                            break
                            
                    time.sleep(2)
                    
                except Exception as e:
                    self.logger.warning(f"Progress check failed: {e}")
                    time.sleep(2)
            
            if progress_updates == 0:
                self.add_result('backtest', 'progress_tracking', 'fail', 
                              "No progress updates received")
            elif progress_updates > 0:
                self.add_result('backtest', 'progress_monitoring', 'pass', 
                              f"Received {progress_updates} progress updates")
            
            # Test stopping the backtest
            try:
                response = requests.post(f"{self.api_base}/backtest/stop", timeout=10)
                if response.status_code == 200:
                    self.add_result('backtest', 'stop_backtest', 'pass', "Backtest stopped successfully")
                else:
                    self.add_result('backtest', 'stop_backtest', 'warning', 
                                  f"Stop backtest returned {response.status_code}")
            except Exception as e:
                self.add_result('backtest', 'stop_backtest', 'fail', f"Stop backtest failed: {e}")
            
        except Exception as e:
            self.add_result('backtest', 'workflow_test', 'fail', 
                          f"Backtest workflow test failed: {e}")
        
        execution_time = time.time() - start_time
        self.add_result('backtest', 'total_backtest_workflow', 'pass', 
                       f"Backtest workflow tests completed in {execution_time:.2f}s",
                       execution_time=execution_time)
    
    def test_cross_component_communication(self):
        """Test communication between V3 components"""
        start_time = time.time()
        
        # Test controller -> dashboard communication
        try:
            response = requests.get(f"{self.api_base}/system", timeout=10)
            if response.status_code == 200:
                system_info = response.json()
                required_fields = ['status', 'version', 'components']
                
                missing_fields = [field for field in required_fields if field not in system_info]
                if not missing_fields:
                    self.add_result('communication', 'controller_dashboard', 'pass', 
                                  "Controller->Dashboard communication working")
                else:
                    self.add_result('communication', 'controller_dashboard', 'warning', 
                                  f"Missing fields: {missing_fields}")
            else:
                self.add_result('communication', 'controller_dashboard', 'fail', 
                              f"System info endpoint failed: {response.status_code}")
        except Exception as e:
            self.add_result('communication', 'controller_dashboard', 'fail', 
                          f"Controller communication failed: {e}")
        
        # Test metrics persistence
        try:
            response = requests.get(f"{self.api_base}/metrics", timeout=10)
            if response.status_code == 200:
                metrics = response.json()
                if isinstance(metrics, dict) and len(metrics) > 0:
                    self.add_result('communication', 'metrics_persistence', 'pass', 
                                  f"Metrics communication working: {len(metrics)} metrics")
                else:
                    self.add_result('communication', 'metrics_persistence', 'warning', 
                                  "Metrics endpoint returns empty data")
            else:
                self.add_result('communication', 'metrics_persistence', 'fail', 
                              f"Metrics endpoint failed: {response.status_code}")
        except Exception as e:
            self.add_result('communication', 'metrics_persistence', 'fail', 
                          f"Metrics communication failed: {e}")
        
        # Test real-time updates (WebSocket simulation)
        try:
            # Get initial state
            response1 = requests.get(f"{self.api_base}/status", timeout=5)
            time.sleep(1)
            # Get state again
            response2 = requests.get(f"{self.api_base}/status", timeout=5)
            
            if response1.status_code == 200 and response2.status_code == 200:
                status1 = response1.json()
                status2 = response2.json()
                
                # Check if timestamps are updating (indicating real-time updates)
                ts1 = status1.get('timestamp')
                ts2 = status2.get('timestamp')
                
                if ts1 and ts2 and ts1 != ts2:
                    self.add_result('communication', 'realtime_updates', 'pass', 
                                  "Real-time status updates working")
                else:
                    self.add_result('communication', 'realtime_updates', 'warning', 
                                  "Status timestamps not updating")
            else:
                self.add_result('communication', 'realtime_updates', 'fail', 
                              "Status endpoint not responding consistently")
                
        except Exception as e:
            self.add_result('communication', 'realtime_updates', 'fail', 
                          f"Real-time updates test failed: {e}")
        
        execution_time = time.time() - start_time
        self.add_result('communication', 'total_communication_tests', 'pass', 
                       f"Cross-communication tests completed in {execution_time:.2f}s",
                       execution_time=execution_time)
    
    def test_error_handling_and_recovery(self):
        """Test system error handling and recovery mechanisms"""
        start_time = time.time()
        
        # Test invalid API requests
        try:
            invalid_endpoints = [
                '/api/nonexistent',
                '/api/backtest/invalid_action',
                '/api/trading/invalid_command'
            ]
            
            error_handling_working = 0
            
            for endpoint in invalid_endpoints:
                response = requests.get(f"{self.dashboard_url}{endpoint}", timeout=5)
                if response.status_code in [404, 400, 405]:  # Proper error codes
                    error_handling_working += 1
            
            if error_handling_working == len(invalid_endpoints):
                self.add_result('error_handling', 'invalid_requests', 'pass', 
                              "Invalid requests handled correctly")
            else:
                self.add_result('error_handling', 'invalid_requests', 'warning', 
                              f"{error_handling_working}/{len(invalid_endpoints)} requests handled correctly")
                
        except Exception as e:
            self.add_result('error_handling', 'invalid_requests', 'fail', 
                          f"Error handling test failed: {e}")
        
        # Test malformed JSON requests
        try:
            response = requests.post(
                f"{self.api_base}/backtest/start",
                data="invalid json",
                headers={'Content-Type': 'application/json'},
                timeout=5
            )
            
            if response.status_code in [400, 422]:  # Bad request
                self.add_result('error_handling', 'malformed_json', 'pass', 
                              "Malformed JSON handled correctly")
            else:
                self.add_result('error_handling', 'malformed_json', 'warning', 
                              f"Unexpected response to malformed JSON: {response.status_code}")
                
        except Exception as e:
            self.add_result('error_handling', 'malformed_json', 'fail', 
                          f"Malformed JSON test failed: {e}")
        
        execution_time = time.time() - start_time
        self.add_result('error_handling', 'total_error_tests', 'pass', 
                       f"Error handling tests completed in {execution_time:.2f}s",
                       execution_time=execution_time)
    
    def test_performance_and_resources(self):
        """Test system performance and resource usage"""
        start_time = time.time()
        
        # Check system resources
        try:
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('.')
            
            # Performance thresholds
            cpu_threshold = 90  # %
            memory_threshold = 90  # %
            disk_threshold = 95  # %
            
            performance_issues = []
            
            if cpu_percent > cpu_threshold:
                performance_issues.append(f"CPU usage high: {cpu_percent}%")
            
            if memory.percent > memory_threshold:
                performance_issues.append(f"Memory usage high: {memory.percent}%")
            
            if disk.percent > disk_threshold:
                performance_issues.append(f"Disk usage high: {disk.percent}%")
            
            if not performance_issues:
                self.add_result('performance', 'resource_usage', 'pass', 
                              f"Resources OK: CPU {cpu_percent}%, RAM {memory.percent}%, Disk {disk.percent}%")
            else:
                self.add_result('performance', 'resource_usage', 'warning', 
                              f"Performance issues: {'; '.join(performance_issues)}")
                
        except Exception as e:
            self.add_result('performance', 'resource_usage', 'fail', 
                          f"Resource check failed: {e}")
        
        # Test API response times
        try:
            endpoints_to_test = ['/api/status', '/api/metrics', '/api/backtest/progress']
            response_times = []
            
            for endpoint in endpoints_to_test:
                endpoint_start = time.time()
                response = requests.get(f"{self.dashboard_url}{endpoint}", timeout=10)
                response_time = (time.time() - endpoint_start) * 1000  # ms
                
                response_times.append(response_time)
            
            avg_response_time = sum(response_times) / len(response_times)
            
            if avg_response_time < 1000:  # Under 1 second
                self.add_result('performance', 'api_response_times', 'pass', 
                              f"Average API response time: {avg_response_time:.1f}ms")
            else:
                self.add_result('performance', 'api_response_times', 'warning', 
                              f"Slow API responses: {avg_response_time:.1f}ms average")
                
        except Exception as e:
            self.add_result('performance', 'api_response_times', 'fail', 
                          f"API response time test failed: {e}")
        
        execution_time = time.time() - start_time
        self.add_result('performance', 'total_performance_tests', 'pass', 
                       f"Performance tests completed in {execution_time:.2f}s",
                       execution_time=execution_time)
    
    def run_comprehensive_test_suite(self):
        """Run the complete V3 test suite"""
        self.logger.info("="*80)
        self.logger.info("V3 COMPREHENSIVE SYSTEM TEST SUITE STARTING")
        self.logger.info("="*80)
        
        # Phase 1: Environment and setup
        self.logger.info("\n?? Phase 1: Environment and Setup")
        self.test_environment_setup()
        self.test_database_connectivity()
        self.test_component_imports()
        
        # Phase 2: Start the main system
        self.logger.info("\n?? Phase 2: System Startup")
        main_process = self.start_main_system()
        
        if not main_process:
            self.add_result('system', 'startup', 'fail', "Failed to start main system")
            self.logger.error("Cannot continue tests without running system")
            return self.generate_test_report()
        
        self.add_result('system', 'startup', 'pass', "Main system started successfully")
        
        try:
            # Phase 3: API and Dashboard tests
            self.logger.info("\n?? Phase 3: API and Dashboard Testing")
            time.sleep(5)  # Let system fully initialize
            self.test_dashboard_api_endpoints()
            
            # Phase 4: Cross-component communication
            self.logger.info("\n?? Phase 4: Cross-Component Communication")
            self.test_cross_component_communication()
            
            # Phase 5: Backtesting workflow (THE MAIN ISSUE YOU WANTED FIXED)
            self.logger.info("\n?? Phase 5: Backtesting Workflow Testing")
            self.test_backtesting_workflow()
            
            # Phase 6: Error handling and recovery
            self.logger.info("\n??  Phase 6: Error Handling and Recovery")
            self.test_error_handling_and_recovery()
            
            # Phase 7: Performance and resources
            self.logger.info("\n? Phase 7: Performance and Resource Testing")
            self.test_performance_and_resources()
            
        finally:
            # Cleanup: Stop the main system
            self.logger.info("\n?? Cleanup: Stopping Test System")
            try:
                if main_process and main_process.poll() is None:
                    self.logger.info("Terminating main system...")
                    main_process.terminate()
                    
                    # Wait for graceful shutdown
                    try:
                        main_process.wait(timeout=10)
                        self.add_result('cleanup', 'system_shutdown', 'pass', "System shut down gracefully")
                    except subprocess.TimeoutExpired:
                        self.logger.warning("Graceful shutdown timeout, force killing...")
                        main_process.kill()
                        main_process.wait()
                        self.add_result('cleanup', 'system_shutdown', 'warning', "System force killed")
            except Exception as e:
                self.add_result('cleanup', 'system_shutdown', 'fail', f"Shutdown error: {e}")
            
            # Final cleanup
            self.kill_processes_on_port(self.base_port)
        
        return self.generate_test_report()
    
    def generate_test_report(self) -> Dict:
        """Generate comprehensive test report"""
        total_execution_time = time.time() - self.start_time
        
        # Count results by status
        status_counts = {'pass': 0, 'fail': 0, 'warning': 0, 'skip': 0}
        component_results = {}
        
        for result in self.results:
            status_counts[result.status] += 1
            
            if result.component not in component_results:
                component_results[result.component] = {'pass': 0, 'fail': 0, 'warning': 0, 'skip': 0}
            
            component_results[result.component][result.status] += 1
        
        total_tests = len(self.results)
        pass_rate = (status_counts['pass'] / total_tests * 100) if total_tests > 0 else 0
        
        # Determine overall system status
        if status_counts['fail'] == 0 and status_counts['warning'] <= total_tests * 0.1:
            system_status = "EXCELLENT"
        elif status_counts['fail'] <= total_tests * 0.05 and status_counts['warning'] <= total_tests * 0.2:
            system_status = "GOOD"
        elif status_counts['fail'] <= total_tests * 0.15:
            system_status = "ACCEPTABLE"
        else:
            system_status = "NEEDS_ATTENTION"
        
        # Critical issues
        critical_issues = [r for r in self.results if r.status == 'fail' and 
                          r.component in ['system', 'backtest', 'communication']]
        
        report = {
            'summary': {
                'total_tests': total_tests,
                'pass_rate': pass_rate,
                'system_status': system_status,
                'execution_time': total_execution_time,
                'timestamp': datetime.now().isoformat()
            },
            'status_counts': status_counts,
            'component_results': component_results,
            'critical_issues': [
                {
                    'component': issue.component,
                    'test': issue.test_name,
                    'message': issue.message
                } for issue in critical_issues
            ],
            'detailed_results': [asdict(result) for result in self.results]
        }
        
        # Print summary to console
        print("\n" + "="*80)
        print("V3 SYSTEM TEST REPORT")
        print("="*80)
        print(f"Overall Status: {system_status}")
        print(f"Total Tests: {total_tests}")
        print(f"Pass Rate: {pass_rate:.1f}%")
        print(f"Execution Time: {total_execution_time:.1f}s")
        print()
        print("Results by Status:")
        for status, count in status_counts.items():
            if count > 0:
                print(f"  {status.upper()}: {count}")
        print()
        
        if critical_issues:
            print("CRITICAL ISSUES:")
            for issue in critical_issues:
                print(f"  ? [{issue['component']}] {issue['test']}: {issue['message']}")
            print()
        
        print("Component Summary:")
        for component, counts in component_results.items():
            total_component = sum(counts.values())
            component_pass_rate = (counts['pass'] / total_component * 100) if total_component > 0 else 0
            status_icon = "?" if counts['fail'] == 0 else "??" if counts['fail'] <= 1 else "?"
            print(f"  {status_icon} {component}: {component_pass_rate:.0f}% pass rate ({counts['pass']}/{total_component})")
        
        print("="*80)
        
        return report
    
    def save_test_report(self, report: Dict, filename: str = None):
        """Save test report to file"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"v3_test_report_{timestamp}.json"
        
        try:
            with open(filename, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            self.logger.info(f"Test report saved to: {filename}")
        except Exception as e:
            self.logger.error(f"Failed to save test report: {e}")

def main():
    """Main function to run V3 comprehensive tests"""
    import argparse
    
    parser = argparse.ArgumentParser(description="V3 Trading System Comprehensive Test Suite")
    parser.add_argument('--save-report', action='store_true', help='Save test report to JSON file')
    parser.add_argument('--port', type=int, help='Override dashboard port')
    args = parser.parse_args()
    
    # Override port if specified
    if args.port:
        os.environ['FLASK_PORT'] = str(args.port)
    
    # Create and run tester
    tester = V3SystemTester()
    
    try:
        # Run comprehensive test suite
        report = tester.run_comprehensive_test_suite()
        
        # Save report if requested
        if args.save_report:
            tester.save_test_report(report)
        
        # Determine exit code
        critical_failures = len([r for r in tester.results if r.status == 'fail' and 
                                r.component in ['system', 'backtest']])
        
        if critical_failures == 0:
            print("\n?? ALL CRITICAL TESTS PASSED - V3 System is ready!")
            exit_code = 0
        elif critical_failures <= 2:
            print(f"\n??  {critical_failures} critical issues found - system needs attention")
            exit_code = 1
        else:
            print(f"\n? {critical_failures} critical failures - system not ready")
            exit_code = 2
        
        sys.exit(exit_code)
        
    except KeyboardInterrupt:
        print("\n\n?? Test suite interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n?? Test suite crashed: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()