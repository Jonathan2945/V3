#!/usr/bin/env python3
"""
V3 COMPREHENSIVE TRADING SYSTEM TEST SUITE
==========================================

Enhanced test harness specifically designed for the V3 Trading System.
Tests all files, validates trading system components, checks dependencies,
ensures V3 architecture compliance, and validates REAL DATA ONLY usage.

Features:
- Complete file discovery and testing
- Trading system component validation
- V3 architecture compliance checks
- Real market data validation (NO MOCK DATA)
- Environment configuration validation
- API integration testing
- Database connectivity verification
- Performance benchmarking
- Comprehensive reporting with trading metrics
"""

import os
import sys
import importlib.util
import ast
import traceback
import time
import json
import sqlite3
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional, Set
from dataclasses import dataclass, asdict
from contextlib import contextmanager, redirect_stdout, redirect_stderr
import io
from datetime import datetime

# Third-party imports for trading system testing
try:
    import psutil
    import pandas as pd
    import numpy as np
    from dotenv import load_dotenv
    ADVANCED_TESTING = True
except ImportError:
    ADVANCED_TESTING = False

# Load environment if available
try:
    load_dotenv()
except:
    pass

@dataclass
class V3TestResult:
    """Enhanced test result for V3 Trading System components."""
    # Basic file info
    file_path: str
    file_name: str
    module_type: str  # 'core', 'ml', 'api', 'data', 'util', 'test', 'config'
    component_category: str  # Specific to trading system
    
    # Standard tests
    import_success: bool
    import_error: Optional[str]
    syntax_valid: bool
    syntax_error: Optional[str]
    dependencies_met: bool
    missing_dependencies: List[str]
    runtime_safe: bool
    runtime_error: Optional[str]
    
    # V3 Trading System specific tests
    trading_system_compatible: bool
    api_integration_valid: bool
    database_schema_valid: bool
    async_compatible: bool
    config_compliant: bool
    
    # V3 Real Data Validation - CRITICAL FOR TRADING
    mock_data_detected: bool
    mock_data_issues: List[str]
    real_market_data_only: bool
    env_config_valid: bool
    env_missing_vars: List[str]
    api_keys_configured: bool
    
    # Performance metrics
    execution_time: float
    file_size: int
    lines_of_code: int
    complexity_score: float
    
    # Metadata
    test_timestamp: str
    warnings: List[str]
    recommendations: List[str]


class V3TradingSystemTestHarness:
    """Comprehensive test harness for V3 Trading System."""
    
    def __init__(self, directory: str = ".", excluded_patterns: List[str] = None):
        self.directory = Path(directory).resolve()
        self.excluded_patterns = excluded_patterns or [
            "__pycache__", ".git", ".pytest_cache", "venv", "env", 
            "node_modules", ".vscode", ".idea", "logs"
        ]
        self.results: Dict[str, V3TestResult] = {}
        self.test_start_time = time.time()
        
        # V3 Trading System specific categorization - ONLY ACTUAL REPOSITORY FILES
        self.v3_file_categories = {
            # Core trading components (VERIFIED IN REPO)
            'core': [
                'main.py', 'main_controller.py', 'start.py', 'start_system.py',
                'quick_launcher.py', 'intelligent_trading_engine.py',
                'adaptive_trading_manager.py', 'real_trading_system.py'
            ],
            
            # Machine Learning components (VERIFIED IN REPO)
            'ml': [
                'advanced_ml_engine.py', 'ml_data_manager.py',
                'strategy_discovery_engine.py', 'confirmation_engine.py'
            ],
            
            # API and Exchange Management (VERIFIED IN REPO)
            'api': [
                'api_monitor.py', 'api_rotation_manager.py', 'binance_exchange_manager.py',
                'api-test.py', 'credential_monitor.py'
            ],
            
            # Data Management (VERIFIED IN REPO)
            'data': [
                'historical_data_manager.py', 'external_data_collector.py',
                'pnl_persistence.py', 'trade_logger.py'
            ],
            
            # Analysis and Scanning (VERIFIED IN REPO)
            'analysis': [
                'market_analysis_engine.py', 'multi_pair_scanner.py',
                'multi_timeframe_analyzer.py', 'price_action_core.py',
                'execution_cost_intelligence.py'
            ],
            
            # Backtesting and Testing (VERIFIED IN REPO)
            'backtest': [
                'advanced_backtester.py', 'test.py'
            ],
            
            # Configuration and Setup (VERIFIED IN REPO)
            'config': [
                'config_reader.py', 'setup_environment.py', 'health_check.py'
            ],
            
            # Utilities and Optimization (VERIFIED IN REPO)
            'util': [
                'resource_optimizer.py', 'emotion_simulator.py',
                'clear_mock_ml_data.py', 'reset_ml_only.py'
            ]
        }
        
        # ACTUAL FILES IN YOUR REPOSITORY (from JSON manifest)
        self.actual_python_files = {
            'adaptive_trading_manager.py', 'advanced_backtester.py', 'advanced_ml_engine.py',
            'api-test.py', 'api_monitor.py', 'api_rotation_manager.py', 'binance_exchange_manager.py',
            'clear_mock_ml_data.py', 'config_reader.py', 'confirmation_engine.py', 'credential_monitor.py',
            'emotion_simulator.py', 'execution_cost_intelligence.py', 'external_data_collector.py',
            'health_check.py', 'historical_data_manager.py', 'intelligent_trading_engine.py',
            'main.py', 'main_controller.py', 'market_analysis_engine.py', 'ml_data_manager.py',
            'multi_pair_scanner.py', 'multi_timeframe_analyzer.py', 'pnl_persistence.py',
            'price_action_core.py', 'quick_launcher.py', 'real_trading_system.py',
            'reset_ml_only.py', 'resource_optimizer.py', 'setup_environment.py',
            'start.py', 'start_system.py', 'strategy_discovery_engine.py', 'test.py', 'trade_logger.py'
        }
        
        # ACTUAL NON-PYTHON FILES IN YOUR REPOSITORY
        self.actual_other_files = {
            'dashbored.html', 'requirements.txt',
            'api_monitor.db', 'system_metrics.db',
            'data/api_management.db', 'data/trading_metrics.db', 'data/exchange_info_backup.json',
            'logs/enhanced_backtesting.log', 'logs/strategy_discovery.log'
        }
        
        # Known V3 dependencies
        self.v3_dependencies = {
            'trading': ['binance', 'ccxt', 'pandas', 'numpy'],
            'ml': ['sklearn', 'tensorflow', 'torch', 'xgboost'],
            'data': ['sqlite3', 'psycopg2', 'sqlalchemy'],
            'async': ['asyncio', 'aiohttp', 'websockets'],
            'monitoring': ['psutil', 'logging'],
            'external': ['requests', 'urllib3', 'python-dotenv']
        }
        
        # V3 Environment Configuration (from provided .env)
        self.required_env_vars = {
            'trading_core': [
                'TESTNET', 'MIN_CONFIDENCE', 'MAX_TOTAL_POSITIONS', 'MAX_RISK_PERCENT',
                'TRADE_AMOUNT_USDT', 'TIMEFRAMES', 'PRIMARY_TIMEFRAME'
            ],
            'real_data_only': [
                'ENABLE_REAL_MARKET_TRAINING', 'CLEAR_MOCK_ML_DATA', 'ENABLE_MOCK_APIS',
                'REALISTIC_SIMULATION'
            ],
            'v2_multi_pair': [
                'ENABLE_ALL_PAIRS', 'MAX_CONCURRENT_PAIRS', 'MIN_VOLUME_24H',
                'OPPORTUNITY_SCANNER_ENABLED', 'SCAN_INTERVAL_SECONDS'
            ],
            'api_rotation': [
                'API_ROTATION_ENABLED', 'API_ROTATION_STRATEGY', 'API_RATE_LIMIT_THRESHOLD'
            ],
            'binance_keys': [
                'BINANCE_API_KEY_1', 'BINANCE_API_SECRET_1',
                'BINANCE_LIVE_API_KEY_1', 'BINANCE_LIVE_API_SECRET_1'
            ],
            'external_apis': [
                'ALPHA_VANTAGE_API_KEY_1', 'NEWS_API_KEY_1', 'FRED_API_KEY_1',
                'TWITTER_BEARER_TOKEN_1', 'REDDIT_CLIENT_ID_1'
            ]
        }
        
        # Mock data patterns to detect and flag - CRITICAL FOR V3
        self.mock_data_patterns = [
            'mock_data', 'fake_data', 'simulated_data', 'dummy_data',
            'test_data', 'sample_data', 'mock_prices', 'fake_prices',
            'generate_fake', 'simulate_price', 'mock_api', 'fake_api',
            'MockClient', 'FakeClient', 'SimulatedClient', 'DummyClient',
            'mock=True', 'fake=True', 'simulated=True', 'test_mode=True',
            'USE_MOCK', 'ENABLE_MOCK', 'MOCK_MODE', 'SIMULATION_MODE'
        ]
        
        # Real data validation patterns
        self.real_data_patterns = [
            'binance', 'alpha_vantage', 'news_api', 'fred_api',
            'twitter_api', 'reddit_api', 'real_market', 'live_data',
            'actual_data', 'historical_data', 'market_data'
        ]
        
        print(f"\nV3 TRADING SYSTEM COMPREHENSIVE TEST SUITE")
        print(f"Testing directory: {self.directory}")
        print(f"V3 Architecture: Core + ML + Multi-pair + API Rotation")
        print(f"Advanced testing: {'Enabled' if ADVANCED_TESTING else 'Basic mode'}")
        
    def discover_v3_files(self) -> Tuple[List[Path], List[Path]]:
        """Discover all V3 trading system files - ONLY ACTUAL REPOSITORY FILES."""
        python_files = []
        other_files = []
        
        for root, dirs, files in os.walk(self.directory):
            # Filter directories
            dirs[:] = [d for d in dirs if not any(pattern in d for pattern in self.excluded_patterns)]
            
            for file in files:
                file_path = Path(root) / file
                relative_path = file_path.relative_to(self.directory)
                
                # Only include files that are ACTUALLY in the repository
                if file.endswith('.py') and file != Path(__file__).name:
                    if file in self.actual_python_files:
                        python_files.append(file_path)
                    else:
                        print(f"Skipping deleted/unknown Python file: {file}")
                
                # Important non-Python files - only if they exist in repo
                elif file.endswith(('.db', '.json', '.html', '.txt', '.yml', '.yaml', '.log')):
                    # Check if this file path exists in our known files
                    if (file in self.actual_other_files or 
                        str(relative_path) in self.actual_other_files or
                        str(relative_path).replace('\\', '/') in self.actual_other_files):
                        other_files.append(file_path)
        
        print(f"Discovered {len(python_files)} Python files, {len(other_files)} other files")
        print(f"Only testing files that exist in your current repository")
        
        # Verify we found the expected files
        missing_files = self.actual_python_files - {f.name for f in python_files}
        if missing_files:
            print(f"Expected but not found: {missing_files}")
        
        return sorted(python_files), sorted(other_files)
    
    def categorize_v3_file(self, file_path: Path) -> Tuple[str, str]:
        """Categorize file by V3 system component."""
        file_name = file_path.name
        
        for category, files in self.v3_file_categories.items():
            if file_name in files:
                return category, self.get_component_description(category, file_name)
        
        # Fallback categorization
        if 'test' in file_name.lower():
            return 'test', 'Testing module'
        elif any(keyword in file_name.lower() for keyword in ['api', 'client', 'exchange']):
            return 'api', 'API integration'
        elif any(keyword in file_name.lower() for keyword in ['ml', 'model', 'ai', 'brain']):
            return 'ml', 'Machine learning'
        elif any(keyword in file_name.lower() for keyword in ['data', 'database', 'persistence']):
            return 'data', 'Data management'
        else:
            return 'util', 'Utility module'
    
    def get_component_description(self, category: str, file_name: str) -> str:
        """Get detailed component description - ONLY FOR ACTUAL REPOSITORY FILES."""
        descriptions = {
            # Core components
            'main.py': 'Main entry point',
            'main_controller.py': 'V3 system controller with backtesting workflow',
            'start.py': 'System startup controller',
            'start_system.py': 'Simple system starter',
            'quick_launcher.py': 'Quick launch utility',
            'intelligent_trading_engine.py': 'Core trading execution engine',
            'adaptive_trading_manager.py': 'Adaptive trading manager',
            'real_trading_system.py': 'Real trading system implementation',
            
            # ML components
            'advanced_ml_engine.py': 'ML engine for strategy optimization',
            'ml_data_manager.py': 'ML data management system',
            'strategy_discovery_engine.py': 'Strategy discovery and optimization',
            'confirmation_engine.py': 'Signal confirmation engine',
            
            # API components
            'api_monitor.py': 'API monitoring and health checks',
            'api_rotation_manager.py': 'API key rotation system',
            'binance_exchange_manager.py': 'Binance exchange integration',
            'api-test.py': 'API testing utility',
            'credential_monitor.py': 'Credential monitoring system',
            
            # Data components
            'historical_data_manager.py': 'Historical data management',
            'external_data_collector.py': 'Economic and social data collector',
            'pnl_persistence.py': 'P&L persistence system',
            'trade_logger.py': 'Trade logging system',
            
            # Analysis components
            'market_analysis_engine.py': 'Market analysis engine',
            'multi_pair_scanner.py': 'Multi-pair opportunity scanner',
            'multi_timeframe_analyzer.py': 'Multi-timeframe analysis',
            'price_action_core.py': 'Price action analysis core',
            'execution_cost_intelligence.py': 'Execution cost optimization',
            
            # Backtesting components
            'advanced_backtester.py': 'Advanced backtesting system',
            'test.py': 'Comprehensive test suite',
            
            # Config components
            'config_reader.py': 'Configuration management',
            'setup_environment.py': 'Environment setup utility',
            'health_check.py': 'System health check',
            
            # Utility components
            'resource_optimizer.py': 'Resource optimization system',
            'emotion_simulator.py': 'Emotion simulation for testing',
            'clear_mock_ml_data.py': 'ML data cleanup utility',
            'reset_ml_only.py': 'ML-only reset utility'
        }
        return descriptions.get(file_name, f'{category.title()} component')
    
    def check_syntax(self, file_path: Path) -> Tuple[bool, Optional[str]]:
        """Check Python syntax."""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            ast.parse(content)
            return True, None
        except SyntaxError as e:
            return False, f"Line {e.lineno}: {e.msg}"
        except Exception as e:
            return False, f"Parse error: {str(e)}"
    
    def extract_imports(self, file_path: Path) -> List[str]:
        """Extract import statements."""
        imports = []
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            tree = ast.parse(content)
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        imports.append(alias.name.split('.')[0])
                elif isinstance(node, ast.ImportFrom):
                    if node.module:
                        imports.append(node.module.split('.')[0])
        except:
            pass
        return list(set(imports))
    
    def check_v3_dependencies(self, imports: List[str]) -> Tuple[bool, List[str]]:
        """Check V3-specific dependencies."""
        missing = []
        
        # Standard library modules
        stdlib_modules = {
            'os', 'sys', 'time', 'datetime', 'json', 'urllib', 'http', 're', 'math',
            'random', 'itertools', 'collections', 'functools', 'pathlib', 'subprocess',
            'threading', 'multiprocessing', 'asyncio', 'logging', 'argparse',
            'configparser', 'sqlite3', 'csv', 'xml', 'html', 'email', 'base64',
            'hashlib', 'hmac', 'uuid', 'tempfile', 'shutil', 'glob', 'fnmatch',
            'pickle', 'copy', 'operator', 'typing', 'dataclasses', 'contextlib',
            'warnings', 'traceback', 'io', 'socket', 'ssl', 'zipfile', 'gzip'
        }
        
        for module_name in imports:
            if module_name in stdlib_modules:
                continue
            
            try:
                importlib.import_module(module_name)
            except ImportError:
                missing.append(module_name)
            except Exception:
                pass
        
        return len(missing) == 0, missing
    
    def check_runtime_safety(self, file_path: Path) -> Tuple[bool, Optional[str]]:
        """Check runtime safety."""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # Dangerous patterns outside of main guard
            dangerous_patterns = [
                ('os.system(', 'System command execution'),
                ('subprocess.call(', 'Subprocess execution'),
                ('eval(', 'Code evaluation'),
                ('exec(', 'Code execution'),
                ('__import__(', 'Dynamic import'),
                ('shutil.rmtree(', 'Directory removal'),
                ('os.remove(', 'File removal')
            ]
            
            has_main_guard = 'if __name__ ==' in content
            
            if not has_main_guard:
                lines = content.split('\n')
                for i, line in enumerate(lines, 1):
                    for pattern, description in dangerous_patterns:
                        if pattern in line and not line.strip().startswith('#'):
                            return False, f"Line {i}: {description} without main guard"
            
            return True, None
            
        except Exception as e:
            return False, f"Safety check failed: {str(e)}"
    
    def check_mock_data_usage(self, file_path: Path) -> Tuple[bool, List[str]]:
        """Check for any mock data usage - V3 should use ONLY real market data."""
        mock_issues = []
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read().lower()
            
            # Check for mock data patterns
            for pattern in self.mock_data_patterns:
                if pattern.lower() in content:
                    # Get line number for better reporting
                    lines = content.split('\n')
                    for i, line in enumerate(lines, 1):
                        if pattern.lower() in line and not line.strip().startswith('#'):
                            mock_issues.append(f"Line {i}: Found '{pattern}' - V3 should use real data only")
                            break
            
            # Check for specific V3 violations
            v3_violations = [
                ('enable_mock_apis=true', 'Mock APIs enabled - should be false for V3'),
                ('realistic_simulation=true', 'Simulation mode - V3 uses real data only'),
                ('clear_mock_ml_data=false', 'Mock ML data not cleared - should be true for V3'),
                ('test_mode=true', 'Test mode enabled - check if using mock data'),
                ('fake_client', 'Using fake client instead of real API'),
                ('mock_response', 'Mock API responses detected'),
                ('simulated_price', 'Simulated prices instead of real market data')
            ]
            
            for violation, description in v3_violations:
                if violation in content:
                    mock_issues.append(f"V3 Violation: {description}")
            
            return len(mock_issues) == 0, mock_issues
            
        except Exception as e:
            return False, [f"Mock data check failed: {str(e)}"]
    
    def validate_env_configuration(self, file_path: Path) -> Tuple[bool, List[str]]:
        """Validate that file properly uses environment configuration."""
        missing_vars = []
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # Check if file uses environment variables
            if 'os.getenv' in content or 'os.environ' in content or 'load_dotenv' in content:
                
                # Core files should check critical V3 settings
                if file_path.name in ['main.py', 'main_controller.py', 'intelligent_trading_engine.py']:
                    required_vars = self.required_env_vars['trading_core'] + self.required_env_vars['real_data_only']
                    
                    for var in required_vars:
                        if var not in content:
                            missing_vars.append(f"Missing critical V3 env var: {var}")
                
                # API files should check API rotation settings
                elif any(keyword in file_path.name.lower() for keyword in ['api', 'exchange', 'rotation']):
                    api_vars = self.required_env_vars['api_rotation'] + self.required_env_vars['binance_keys']
                    
                    # Check for at least some API configuration
                    if not any(var in content for var in api_vars):
                        missing_vars.append("No API configuration environment variables detected")
                
                # Data collector should check external API keys
                elif 'external_data' in file_path.name or 'data_collector' in file_path.name:
                    external_vars = self.required_env_vars['external_apis']
                    
                    if not any(var in content for var in external_vars):
                        missing_vars.append("No external API keys configuration detected")
            
            return len(missing_vars) == 0, missing_vars
            
        except Exception as e:
            return False, [f"Environment validation failed: {str(e)}"]
    
    def check_real_market_data_compliance(self, file_path: Path) -> bool:
        """Check that file uses real market data sources only."""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read().lower()
            
            # Files that handle data should have real data patterns
            if any(keyword in file_path.name.lower() for keyword in ['data', 'market', 'price', 'trading', 'ml']):
                
                # Must have real data indicators
                has_real_data = any(pattern in content for pattern in self.real_data_patterns)
                
                # Must NOT have mock data (already checked separately)
                has_mock_data = any(pattern.lower() in content for pattern in self.mock_data_patterns)
                
                return has_real_data and not has_mock_data
            
            return True  # Non-data files pass by default
            
        except:
            return False
    
    def validate_api_key_usage(self, file_path: Path) -> bool:
        """Validate proper API key usage from environment."""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # API-related files should use environment variables for keys
            if any(keyword in file_path.name.lower() for keyword in ['api', 'exchange', 'client', 'binance']):
                
                # Should NOT have hardcoded API keys
                dangerous_patterns = [
                    'api_key="', "api_key='", 'api_secret="', "api_secret='",
                    'bearer_token="', "bearer_token='", 'client_id="', "client_id='"
                ]
                
                for pattern in dangerous_patterns:
                    if pattern in content.lower():
                        return False
                
                # Should use environment variables
                env_patterns = ['os.getenv', 'os.environ', 'getenv']
                return any(pattern in content for pattern in env_patterns)
            
            return True
            
        except:
            return False
    
    def check_v3_compliance(self, file_path: Path) -> Tuple[bool, List[str]]:
        """Check V3 trading system compliance."""
        compliance_issues = []
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # Check for V3 architecture patterns
            if 'main' in file_path.name.lower() or 'controller' in file_path.name.lower():
                if 'V3' not in content and 'v3' not in content.lower():
                    compliance_issues.append("Missing V3 branding in core component")
            
            # Check for proper async usage in core components
            if file_path.name in ['main_controller.py', 'intelligent_trading_engine.py']:
                if 'async def' not in content:
                    compliance_issues.append("Core component should use async patterns")
            
            # Check for proper error handling
            if 'try:' not in content and 'except' not in content:
                if file_path.name not in ['__init__.py', 'health_check.py']:
                    compliance_issues.append("Missing error handling")
            
            # Check for logging in core components
            if file_path.name.startswith(('main', 'trading', 'ml', 'api')):
                if 'logging' not in content and 'print(' in content:
                    compliance_issues.append("Should use logging instead of print statements")
            
            return len(compliance_issues) == 0, compliance_issues
            
        except Exception as e:
            return False, [f"Compliance check failed: {str(e)}"]
    
    def check_api_integration(self, file_path: Path) -> bool:
        """Check if API integration patterns are properly implemented."""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # API-related files should have proper patterns
            if any(keyword in file_path.name.lower() for keyword in ['api', 'exchange', 'client']):
                required_patterns = ['try:', 'except', 'Client', 'api_key']
                return any(pattern in content for pattern in required_patterns)
            
            return True  # Non-API files pass by default
            
        except:
            return False
    
    def check_database_schema(self, file_path: Path) -> bool:
        """Check database schema validity."""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # Database-related files should have proper schema patterns
            if any(keyword in file_path.name.lower() for keyword in ['data', 'persistence', 'logger']):
                if 'CREATE TABLE' in content:
                    # Should have proper SQL patterns
                    return 'PRIMARY KEY' in content and 'INTEGER' in content
                elif 'sqlite3' in content or 'database' in content.lower():
                    return 'connect' in content
            
            return True  # Non-database files pass by default
            
        except:
            return False
    
    def calculate_complexity_score(self, file_path: Path) -> float:
        """Calculate code complexity score."""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            tree = ast.parse(content)
            
            complexity = 0
            for node in ast.walk(tree):
                # Add complexity for control structures
                if isinstance(node, (ast.If, ast.For, ast.While, ast.Try)):
                    complexity += 1
                elif isinstance(node, ast.FunctionDef):
                    complexity += 1
                elif isinstance(node, ast.AsyncFunctionDef):
                    complexity += 2  # Async functions are more complex
                elif isinstance(node, ast.ClassDef):
                    complexity += 2
            
            return complexity / max(1, len(content.split('\n'))) * 100
            
        except:
            return 0.0
    
    def load_and_validate_env_file(self) -> Tuple[bool, Dict[str, Any]]:
        """Load and validate the .env file against V3 requirements."""
        env_status = {
            'file_exists': False,
            'can_load': False,
            'v3_compliant': False,
            'real_data_mode': False,
            'api_rotation_enabled': False,
            'missing_critical_vars': [],
            'mock_data_violations': []
        }
        
        try:
            env_path = Path('.env')
            if env_path.exists():
                env_status['file_exists'] = True
                
                with open(env_path, 'r', encoding='utf-8', errors='ignore') as f:
                    env_content = f.read()
                
                env_status['can_load'] = True
                
                # Check critical V3 settings for real data only
                critical_settings = {
                    'CLEAR_MOCK_ML_DATA': 'true',
                    'ENABLE_MOCK_APIS': 'false', 
                    'REALISTIC_SIMULATION': 'false',
                    'ENABLE_REAL_MARKET_TRAINING': 'true'
                }
                
                mock_violations = []
                for setting, expected_value in critical_settings.items():
                    if setting in env_content:
                        # Extract the value
                        for line in env_content.split('\n'):
                            if line.startswith(f'{setting}='):
                                actual_value = line.split('=', 1)[1].strip().lower()
                                if actual_value != expected_value.lower():
                                    mock_violations.append(f"{setting}={actual_value} (should be {expected_value})")
                                break
                    else:
                        env_status['missing_critical_vars'].append(setting)
                
                env_status['mock_data_violations'] = mock_violations
                env_status['real_data_mode'] = len(mock_violations) == 0
                
                # Check API rotation
                env_status['api_rotation_enabled'] = 'API_ROTATION_ENABLED=true' in env_content
                
                # Check for required variables
                all_required = []
                for category_vars in self.required_env_vars.values():
                    all_required.extend(category_vars)
                
                missing = []
                for var in all_required:
                    if var not in env_content:
                        missing.append(var)
                
                env_status['missing_critical_vars'].extend(missing)
                env_status['v3_compliant'] = len(env_status['missing_critical_vars']) == 0 and env_status['real_data_mode']
                
        except Exception as e:
            print(f"Error validating .env file: {e}")
        
        return env_status['v3_compliant'], env_status
    
    @contextmanager
    def sandboxed_environment(self):
        """Create sandboxed environment for testing."""
        old_path = sys.path.copy()
        old_modules = sys.modules.copy()
        old_argv = sys.argv.copy()
        
        try:
            sys.argv = ['test_script']
            yield
        finally:
            sys.path[:] = old_path
            modules_to_remove = set(sys.modules.keys()) - set(old_modules.keys())
            for module in modules_to_remove:
                try:
                    del sys.modules[module]
                except KeyError:
                    pass
            sys.argv[:] = old_argv
    
    def safe_import_test(self, file_path: Path) -> Tuple[bool, Optional[str], List[str]]:
        """Safely test importing a file."""
        warnings_list = []
        
        try:
            with self.sandboxed_environment():
                stdout_capture = io.StringIO()
                stderr_capture = io.StringIO()
                
                with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
                    spec = importlib.util.spec_from_file_location(file_path.stem, file_path)
                    if spec is None:
                        return False, "Could not create module spec", warnings_list
                    
                    module = importlib.util.module_from_spec(spec)
                    sys.modules[file_path.stem] = module
                    spec.loader.exec_module(module)
                
                stderr_content = stderr_capture.getvalue()
                if stderr_content:
                    warnings_list.extend(stderr_content.strip().split('\n'))
                
                return True, None, warnings_list
                
        except Exception as e:
            error_msg = f"{type(e).__name__}: {str(e)}"
            return False, error_msg, warnings_list
    
    def generate_recommendations(self, result: V3TestResult, file_path: Path) -> List[str]:
        """Generate improvement recommendations."""
        recommendations = []
        
        # Critical V3 violations first
        if result.mock_data_detected:
            recommendations.append("CRITICAL: Remove all mock data usage - V3 requires real market data only")
        
        # Missing dependencies
        if result.missing_dependencies:
            recommendations.append(f"Install missing dependencies: {', '.join(result.missing_dependencies)}")
        
        # Complexity too high
        if result.complexity_score > 50:
            recommendations.append("Consider refactoring to reduce complexity")
        
        # Large files
        if result.lines_of_code > 1000:
            recommendations.append("Consider splitting large file into smaller modules")
        
        # Missing async in core components
        if result.module_type == 'core' and not result.async_compatible:
            recommendations.append("Consider adding async support for better performance")
        
        # API components without proper error handling
        if result.module_type == 'api' and not result.api_integration_valid:
            recommendations.append("Add proper API error handling and retry logic")
        
        # Environment configuration issues
        if not result.env_config_valid:
            recommendations.append("Fix environment configuration compliance")
        
        return recommendations
    
    def test_v3_file(self, file_path: Path) -> V3TestResult:
        """Comprehensive test of a V3 file."""
        start_time = time.time()
        
        # Basic file info
        file_size = file_path.stat().st_size if file_path.exists() else 0
        module_type, component_category = self.categorize_v3_file(file_path)
        
        # Initialize result
        result = V3TestResult(
            file_path=str(file_path),
            file_name=file_path.name,
            module_type=module_type,
            component_category=component_category,
            import_success=False,
            import_error=None,
            syntax_valid=False,
            syntax_error=None,
            dependencies_met=False,
            missing_dependencies=[],
            runtime_safe=False,
            runtime_error=None,
            trading_system_compatible=False,
            api_integration_valid=False,
            database_schema_valid=False,
            async_compatible=False,
            config_compliant=False,
            mock_data_detected=False,
            mock_data_issues=[],
            real_market_data_only=False,
            env_config_valid=False,
            env_missing_vars=[],
            api_keys_configured=False,
            execution_time=0,
            file_size=file_size,
            lines_of_code=0,
            complexity_score=0,
            test_timestamp=datetime.now().isoformat(),
            warnings=[],
            recommendations=[]
        )
        
        try:
            # Count lines of code
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
                result.lines_of_code = sum(1 for line in lines 
                                         if line.strip() and not line.strip().startswith('#'))
            
            # Test syntax
            result.syntax_valid, result.syntax_error = self.check_syntax(file_path)
            
            # Extract imports and check dependencies
            imports = self.extract_imports(file_path)
            result.dependencies_met, result.missing_dependencies = self.check_v3_dependencies(imports)
            
            # Runtime safety check
            result.runtime_safe, result.runtime_error = self.check_runtime_safety(file_path)
            
            # V3-specific tests
            result.trading_system_compatible, compliance_issues = self.check_v3_compliance(file_path)
            result.api_integration_valid = self.check_api_integration(file_path)
            result.database_schema_valid = self.check_database_schema(file_path)
            
            # V3 Real Data Validation (CRITICAL)
            no_mock_data, mock_issues = self.check_mock_data_usage(file_path)
            result.mock_data_detected = not no_mock_data
            result.mock_data_issues = mock_issues
            
            # Environment configuration validation
            result.env_config_valid, env_missing = self.validate_env_configuration(file_path)
            result.env_missing_vars = env_missing
            
            # Real market data compliance
            result.real_market_data_only = self.check_real_market_data_compliance(file_path)
            
            # API key configuration
            result.api_keys_configured = self.validate_api_key_usage(file_path)
            
            # Check async compatibility
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                result.async_compatible = 'async' in content or 'await' in content
            
            # Calculate complexity
            result.complexity_score = self.calculate_complexity_score(file_path)
            
            # Safe import test (if syntax is valid)
            if result.syntax_valid:
                result.import_success, result.import_error, warnings = self.safe_import_test(file_path)
                result.warnings.extend(warnings)
            
            # Add compliance issues to warnings
            if compliance_issues:
                result.warnings.extend(compliance_issues)
            
            # Add mock data violations (CRITICAL for V3)
            if result.mock_data_detected:
                result.warnings.extend([f"CRITICAL V3 VIOLATION: {issue}" for issue in result.mock_data_issues])
            
            # Add environment configuration issues
            if not result.env_config_valid:
                result.warnings.extend([f"ENV CONFIG: {var}" for var in result.env_missing_vars])
            
            # Generate recommendations
            result.recommendations = self.generate_recommendations(result, file_path)
            
        except Exception as e:
            result.warnings.append(f"Test error: {str(e)}")
        
        result.execution_time = time.time() - start_time
        return result
    
    def run_comprehensive_test(self) -> Dict[str, V3TestResult]:
        """Run comprehensive V3 system test - ONLY ON ACTUAL REPOSITORY FILES."""
        # First, validate .env file
        print(f"\nVALIDATING V3 ENVIRONMENT CONFIGURATION")
        print("="*50)
        
        env_valid, env_status = self.load_and_validate_env_file()
        
        if env_status['file_exists']:
            print(f".env file found")
            print(f"V3 Real Data Mode: {'ENABLED' if env_status['real_data_mode'] else 'DISABLED'}")
            print(f"API Rotation: {'ENABLED' if env_status['api_rotation_enabled'] else 'DISABLED'}")
            
            if env_status['mock_data_violations']:
                print(f"CRITICAL: Mock data violations detected:")
                for violation in env_status['mock_data_violations']:
                    print(f"   {violation}")
            
            if env_status['missing_critical_vars']:
                print(f"Missing critical environment variables:")
                for var in env_status['missing_critical_vars'][:5]:  # Show first 5
                    print(f"   {var}")
                if len(env_status['missing_critical_vars']) > 5:
                    print(f"   ... and {len(env_status['missing_critical_vars']) - 5} more")
        else:
            print(f".env file not found - V3 system requires environment configuration")
        
        python_files, other_files = self.discover_v3_files()
        
        print(f"\nTESTING {len(python_files)} PYTHON FILES FROM YOUR REPOSITORY")
        print(f"Skipping any files not in your current repo manifest")
        print(f"CRITICAL: Checking for mock data usage (V3 uses REAL DATA ONLY)")
        print("=" * 80)
        
        tested_files = set()
        mock_data_violations = 0
        env_config_issues = 0
        
        for file_path in python_files:
            # Double-check this file is in our known repository files
            if file_path.name not in self.actual_python_files:
                print(f"Skipping unknown file: {file_path.name}")
                continue
                
            print(f"Testing: {file_path.name}")
            result = self.test_v3_file(file_path)
            self.results[str(file_path)] = result
            tested_files.add(file_path.name)
            
            # Track critical violations
            if result.mock_data_detected:
                mock_data_violations += 1
            if not result.env_config_valid:
                env_config_issues += 1
            
            # Status indicators
            symbols = []
            symbols.append("?" if result.syntax_valid else "?")
            symbols.append("??" if result.dependencies_met else "??")
            symbols.append("??" if result.runtime_safe else "??")
            symbols.append("?" if result.import_success else "?")
            symbols.append("??" if result.trading_system_compatible else "??")
            symbols.append("??" if not result.mock_data_detected else "??")  # Mock data check
            symbols.append("??" if result.env_config_valid else "??")  # Env config check
            
            status = ''.join(symbols)
            print(f"   {status} [{result.module_type.upper()}] {result.component_category} ({result.execution_time:.3f}s)")
            
            # Show critical violations first
            if result.mock_data_detected:
                for issue in result.mock_data_issues[:1]:  # Show first mock data issue
                    print(f"      ?? CRITICAL: {issue}")
            
            if result.warnings:
                for warning in result.warnings[:1]:  # Show first warning
                    print(f"      ??  {warning}")
        
        # Show critical summary
        print(f"\nCRITICAL V3 VALIDATION RESULTS:")
        print(f"   Files with mock data violations: {mock_data_violations}")
        print(f"   Files with env config issues: {env_config_issues}")
        
        # Show summary of what was actually tested
        print(f"\nTESTED FILES FROM YOUR REPOSITORY:")
        print(f"   Python files tested: {len(tested_files)}")
        expected_count = len(self.actual_python_files)
        if len(tested_files) < expected_count:
            missing = self.actual_python_files - tested_files
            print(f"   Missing files (not found): {missing}")
        
        print(f"\nTesting {len(other_files)} configuration/data files...")
        self.test_configuration_files(other_files)
        
        return self.results
    
    def test_configuration_files(self, files: List[Path]):
        """Test configuration and data files."""
        for file_path in files:
            try:
                if file_path.suffix == '.json':
                    with open(file_path, 'r') as f:
                        json.load(f)
                    print(f"   ? Valid JSON: {file_path.name}")
                
                elif file_path.suffix == '.db':
                    conn = sqlite3.connect(file_path)
                    conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
                    conn.close()
                    print(f"   ? Valid SQLite DB: {file_path.name}")
                
                elif file_path.suffix == '.html':
                    # Basic HTML validation
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                        if '<html' in content.lower() and '</html>' in content.lower():
                            print(f"   ? Valid HTML: {file_path.name}")
                        else:
                            print(f"   ??  HTML missing structure: {file_path.name}")
                
            except Exception as e:
                print(f"   ? Invalid {file_path.suffix}: {file_path.name} - {str(e)[:50]}")
    
    def generate_comprehensive_report(self) -> str:
        """Generate comprehensive V3 system report."""
        if not self.results:
            return "No test results available."
        
        total_files = len(self.results)
        
        # Calculate pass rates
        syntax_passed = sum(1 for r in self.results.values() if r.syntax_valid)
        import_passed = sum(1 for r in self.results.values() if r.import_success)
        v3_compliant = sum(1 for r in self.results.values() if r.trading_system_compatible)
        
        # V3 Critical Metrics
        no_mock_data = sum(1 for r in self.results.values() if not r.mock_data_detected)
        real_data_only = sum(1 for r in self.results.values() if r.real_market_data_only)
        env_config_valid = sum(1 for r in self.results.values() if r.env_config_valid)
        api_keys_proper = sum(1 for r in self.results.values() if r.api_keys_configured)
        
        overall_passed = sum(1 for r in self.results.values() 
                           if r.syntax_valid and r.import_success and r.dependencies_met and r.runtime_safe)
        
        # V3 specific pass rate (critical for trading system)
        v3_critical_passed = sum(1 for r in self.results.values() 
                               if (r.syntax_valid and r.import_success and r.dependencies_met and 
                                   r.runtime_safe and not r.mock_data_detected and r.real_market_data_only))
        
        # Calculate metrics by category
        category_stats = {}
        for result in self.results.values():
            cat = result.module_type
            if cat not in category_stats:
                category_stats[cat] = {'total': 0, 'passed': 0, 'complexity': []}
            
            category_stats[cat]['total'] += 1
            category_stats[cat]['complexity'].append(result.complexity_score)
            
            if (result.syntax_valid and result.import_success and 
                result.dependencies_met and result.runtime_safe):
                category_stats[cat]['passed'] += 1
        
        # Generate report
        total_time = time.time() - self.test_start_time
        total_loc = sum(r.lines_of_code for r in self.results.values())
        total_size = sum(r.file_size for r in self.results.values())
        avg_complexity = sum(r.complexity_score for r in self.results.values()) / total_files
        
        report = [
            "\n" + "="*100,
            "V3 TRADING SYSTEM - COMPREHENSIVE TEST REPORT",
            "="*100,
            f"OVERALL SUMMARY:",
            f"   Total Files Tested: {total_files}",
            f"   Overall Pass Rate: {overall_passed}/{total_files} ({(overall_passed/total_files)*100:.1f}%)",
            f"   Syntax Valid: {syntax_passed}/{total_files} ({(syntax_passed/total_files)*100:.1f}%)",
            f"   Import Success: {import_passed}/{total_files} ({(import_passed/total_files)*100:.1f}%)",
            f"   V3 Compliant: {v3_compliant}/{total_files} ({(v3_compliant/total_files)*100:.1f}%)",
            "",
            f"V3 CRITICAL REAL DATA VALIDATION:",
            f"   No Mock Data Detected: {no_mock_data}/{total_files} ({(no_mock_data/total_files)*100:.1f}%)",
            f"   Real Market Data Only: {real_data_only}/{total_files} ({(real_data_only/total_files)*100:.1f}%)",
            f"   Environment Config Valid: {env_config_valid}/{total_files} ({(env_config_valid/total_files)*100:.1f}%)",
            f"   API Keys Properly Configured: {api_keys_proper}/{total_files} ({(api_keys_proper/total_files)*100:.1f}%)",
            f"   V3 Trading Ready: {v3_critical_passed}/{total_files} ({(v3_critical_passed/total_files)*100:.1f}%)",
            "",
            f"PERFORMANCE METRICS:",
            f"   Total Test Time: {total_time:.2f}s",
            f"   Lines of Code: {total_loc:,}",
            f"   Total File Size: {total_size:,} bytes ({total_size/1024/1024:.1f} MB)",
            f"   Average Complexity: {avg_complexity:.1f}",
            "",
            f"V3 COMPONENT ANALYSIS:",
            "-"*80
        ]
        
        # Component breakdown
        for category, stats in sorted(category_stats.items()):
            pass_rate = (stats['passed'] / stats['total']) * 100
            avg_complexity = sum(stats['complexity']) / len(stats['complexity']) if stats['complexity'] else 0
            
            status_icon = "?" if pass_rate >= 90 else "??" if pass_rate >= 70 else "?"
            
            report.extend([
                f"{status_icon} {category.upper()}: {stats['passed']}/{stats['total']} ({pass_rate:.1f}%) | Complexity: {avg_complexity:.1f}"
            ])
        
        # System recommendations
        report.extend([
            "",
            "V3 SYSTEM RECOMMENDATIONS:",
            "-"*50
        ])
        
        # Generate V3-specific system-wide recommendations
        mock_data_files = [r for r in self.results.values() if r.mock_data_detected]
        env_config_files = [r for r in self.results.values() if not r.env_config_valid]
        high_complexity_files = [r for r in self.results.values() if r.complexity_score > 50]
        non_real_data_files = [r for r in self.results.values() if not r.real_market_data_only]
        
        missing_deps = set()
        for r in self.results.values():
            missing_deps.update(r.missing_dependencies)
        
        # Critical V3 violations first
        if mock_data_files:
            report.append(f"CRITICAL: {len(mock_data_files)} files contain mock data - V3 requires REAL DATA ONLY")
            
        if non_real_data_files:
            report.append(f"{len(non_real_data_files)} files need real market data validation")
        
        if env_config_files:
            report.append(f"{len(env_config_files)} files have environment configuration issues")
        
        # Standard recommendations
        if high_complexity_files:
            report.append(f"{len(high_complexity_files)} files have high complexity - consider refactoring")
        
        if missing_deps:
            report.append(f"Install missing dependencies: {', '.join(sorted(missing_deps)[:5])}")
        
        if v3_compliant < total_files * 0.9:
            report.append(f"{total_files - v3_compliant} files need V3 compliance improvements")
        
        # V3 specific recommendations
        if v3_critical_passed < total_files * 0.95:
            report.append(f"V3 Trading System Readiness: {v3_critical_passed}/{total_files} files pass critical validation")
            report.append("   ? Ensure all files use real market data only")
            report.append("   ? Validate environment configuration compliance")
            report.append("   ? Check API key usage from environment variables")
        
        # Final status determination
        system_ready = (overall_passed >= total_files * 0.9 and 
                       v3_critical_passed >= total_files * 0.95 and
                       len(mock_data_files) == 0)
        
        report.extend([
            "",
            f"V3 Trading System test completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"System Status: {'READY FOR LIVE TRADING' if system_ready else 'NEEDS CRITICAL FIXES' if mock_data_files else 'READY FOR TESTING'}",
            f"V3 Architecture: Hybrid V1 Performance + V2 Infrastructure",
            f"Real Data Compliance: {'VERIFIED' if len(mock_data_files) == 0 else 'VIOLATIONS DETECTED'}",
            "="*100
        ])
        
        return "\n".join(report)
    
    def save_json_report(self, filename: str = "v3_test_results.json"):
        """Save comprehensive JSON report."""
        # Convert results to JSON-serializable format
        json_results = {}
        for path, result in self.results.items():
            json_results[path] = asdict(result)
        
        # System summary
        total_files = len(self.results)
        overall_passed = sum(1 for r in self.results.values() 
                           if r.syntax_valid and r.import_success and r.dependencies_met and r.runtime_safe)
        
        report_data = {
            'test_summary': {
                'total_files': total_files,
                'passed': overall_passed,
                'pass_rate': (overall_passed / total_files) * 100 if total_files > 0 else 0,
                'v3_compliant': sum(1 for r in self.results.values() if r.trading_system_compatible),
                'mock_data_violations': sum(1 for r in self.results.values() if r.mock_data_detected),
                'real_data_compliance': sum(1 for r in self.results.values() if r.real_market_data_only),
                'test_timestamp': datetime.now().isoformat(),
                'total_execution_time': time.time() - self.test_start_time,
                'total_lines_of_code': sum(r.lines_of_code for r in self.results.values()),
                'average_complexity': sum(r.complexity_score for r in self.results.values()) / total_files if total_files > 0 else 0
            },
            'v3_system_info': {
                'architecture': 'V1 Performance + V2 Infrastructure',
                'components_tested': list(self.v3_file_categories.keys()),
                'advanced_testing_enabled': ADVANCED_TESTING,
                'real_data_only_mode': True
            },
            'results': json_results
        }
        
        with open(filename, 'w') as f:
            json.dump(report_data, f, indent=2)
        
        print(f"V3 test results saved to: {filename}")
    
    def run_performance_benchmark(self):
        """Run performance benchmark on V3 system."""
        if not ADVANCED_TESTING:
            print("Advanced testing not available - install pandas, numpy, psutil")
            return
        
        print(f"\nRUNNING V3 PERFORMANCE BENCHMARK")
        print("-" * 50)
        
        # System resources
        cpu_usage = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        print(f"System Resources:")
        print(f"   CPU Usage: {cpu_usage}%")
        print(f"   Memory: {memory.percent}% ({memory.used/1024/1024/1024:.1f}GB/{memory.total/1024/1024/1024:.1f}GB)")
        print(f"   Disk: {disk.percent}% ({disk.used/1024/1024/1024:.1f}GB/{disk.total/1024/1024/1024:.1f}GB)")
        
        # File analysis performance
        total_files = len(self.results)
        total_time = time.time() - self.test_start_time
        files_per_second = total_files / total_time if total_time > 0 else 0
        
        print(f"\nTesting Performance:")
        print(f"   Files per second: {files_per_second:.1f}")
        print(f"   Average test time: {(total_time / total_files)*1000:.1f}ms per file")
        
        # Complexity analysis
        complexities = [r.complexity_score for r in self.results.values()]
        if complexities:
            print(f"\nComplexity Analysis:")
            print(f"   Average complexity: {sum(complexities)/len(complexities):.1f}")
            print(f"   Highest complexity: {max(complexities):.1f}")
            print(f"   Files over 50 complexity: {sum(1 for c in complexities if c > 50)}")


def main():
    """Main function to run V3 comprehensive test."""
    import argparse
    
    parser = argparse.ArgumentParser(description="V3 Trading System Comprehensive Test Suite")
    parser.add_argument(
        "directory", 
        nargs="?", 
        default=".", 
        help="Directory to test (default: current directory)"
    )
    parser.add_argument(
        "--json-report", 
        action="store_true", 
        help="Generate JSON report"
    )
    parser.add_argument(
        "--benchmark", 
        action="store_true", 
        help="Run performance benchmark"
    )
    parser.add_argument(
        "--exclude", 
        nargs="*", 
        default=["__pycache__", ".git", ".pytest_cache", "venv", "env", "logs"],
        help="Patterns to exclude from testing"
    )
    
    args = parser.parse_args()
    
    # Create test harness
    harness = V3TradingSystemTestHarness(args.directory, args.exclude)
    
    print("Starting V3 Trading System Comprehensive Analysis...")
    
    # Run comprehensive test
    results = harness.run_comprehensive_test()
    
    # Generate and display report
    report = harness.generate_comprehensive_report()
    print(report)
    
    # Save JSON report if requested
    if args.json_report:
        harness.save_json_report()
    
    # Run benchmark if requested
    if args.benchmark:
        harness.run_performance_benchmark()
    
    # Final status
    total_files = len(results)
    passed_files = sum(1 for r in results.values() 
                      if r.syntax_valid and r.import_success and r.dependencies_met and r.runtime_safe)
    
    mock_data_violations = sum(1 for r in results.values() if r.mock_data_detected)
    
    print(f"\n{'='*60}")
    if passed_files >= total_files * 0.9 and mock_data_violations == 0:
        print("V3 TRADING SYSTEM: READY FOR LIVE TRADING!")
        print(f"{passed_files}/{total_files} files passed comprehensive testing")
        print("REAL DATA COMPLIANCE: VERIFIED")
        exit_code = 0
    elif mock_data_violations > 0:
        print("V3 TRADING SYSTEM: CRITICAL VIOLATIONS DETECTED")
        print(f"CRITICAL: {mock_data_violations} files contain mock data")
        print("V3 requires REAL MARKET DATA ONLY")
        exit_code = 2
    else:
        print("V3 TRADING SYSTEM: NEEDS IMPROVEMENTS")
        print(f"{total_files - passed_files}/{total_files} files need attention")
        exit_code = 1
    
    print(f"V3 Architecture: Hybrid V1 + V2 with ML Enhancement")
    print(f"Test Coverage: Core, ML, API, Data, Analysis, Backtesting")
    print(f"Completed in {time.time() - harness.test_start_time:.1f}s")
    print("="*60)
    
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
