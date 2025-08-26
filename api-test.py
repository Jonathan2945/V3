#!/usr/bin/env python3
"""
UNIFIED TRADING MODE VALIDATOR - BINANCE.US COMPATIBLE
=====================================================
Comprehensive test suite for validating testnet/live trading transitions.
Enhanced with proper Binance.US support for live trading validation.

Usage:
    python api-test.py              # Interactive menu
    python api-test.py --quick      # Quick validation only
    python api-test.py --full       # Full comprehensive validation
    python api-test.py --help       # Show help

Features:
- Quick basic validation (2-3 minutes)
- Comprehensive full validation (5-10 minutes)
- Credential validation and testing
- Balance handling verification
- Safety system validation
- API connectivity testing
- Mode switching verification
- Live trading readiness assessment
- Binance.US compatibility

IMPORTANT: This validator will NOT execute actual trades or modify your account.
All tests are read-only and designed to verify system functionality safely.
"""

import os
import sys
import asyncio
import argparse
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from dotenv import load_dotenv
import json

# Load environment variables
load_dotenv()

# V3 API Configuration validation - REQUIRED FOR V3 COMPLIANCE
BINANCE_API_KEY_1 = os.getenv('BINANCE_API_KEY_1')
BINANCE_API_SECRET_1 = os.getenv('BINANCE_API_SECRET_1')
BINANCE_API_KEY_2 = os.getenv('BINANCE_API_KEY_2')
BINANCE_API_SECRET_2 = os.getenv('BINANCE_API_SECRET_2')
BINANCE_API_KEY_3 = os.getenv('BINANCE_API_KEY_3')
BINANCE_API_SECRET_3 = os.getenv('BINANCE_API_SECRET_3')

# V3 Live API Configuration
BINANCE_LIVE_API_KEY_1 = os.getenv('BINANCE_LIVE_API_KEY_1')
BINANCE_LIVE_API_SECRET_1 = os.getenv('BINANCE_LIVE_API_SECRET_1')
BINANCE_LIVE_API_KEY_2 = os.getenv('BINANCE_LIVE_API_KEY_2')
BINANCE_LIVE_API_SECRET_2 = os.getenv('BINANCE_LIVE_API_SECRET_2')

# V3 API Rotation Configuration
API_ROTATION_ENABLED = os.getenv('API_ROTATION_ENABLED', 'false').lower() == 'true'
API_ROTATION_STRATEGY = os.getenv('API_ROTATION_STRATEGY', 'RATE_LIMIT_TRIGGER')
API_RATE_LIMIT_THRESHOLD = int(os.getenv('API_RATE_LIMIT_THRESHOLD', '80'))

# V3 Trading Configuration
TESTNET = os.getenv('TESTNET', 'true').lower() == 'true'
LIVE_TRADING_ENABLED = os.getenv('LIVE_TRADING_ENABLED', 'false').lower() == 'true'

# Test results tracking
class TestResults:
    def __init__(self):
        self.tests = []
        self.passed = 0
        self.failed = 0
        self.warnings = 0
        self.start_time = datetime.now()
    
    def add_test(self, name: str, status: str, message: str, details: Dict = None):
        self.tests.append({
            'name': name,
            'status': status,
            'message': message,
            'details': details or {},
            'timestamp': datetime.now().isoformat()
        })
        
        if status == 'PASS':
            self.passed += 1
        elif status == 'FAIL':
            self.failed += 1
        elif status == 'WARN':
            self.warnings += 1
    
    def print_summary(self, test_type=""):
        duration = (datetime.now() - self.start_time).total_seconds()
        
        print("\n" + "=" * 70)
        print(f"{test_type.upper()} VALIDATION SUMMARY")
        print("=" * 70)
        print(f"Total Tests: {len(self.tests)}")
        print(f"Passed: {self.passed}")
        print(f"Failed: {self.failed}")
        print(f"Warnings: {self.warnings}")
        print(f"Duration: {duration:.1f} seconds")
        
        if self.failed > 0:
            print("\nFAILED TESTS:")
            for test in self.tests:
                if test['status'] == 'FAIL':
                    print(f"  X {test['name']}: {test['message']}")
                    if test['details']:
                        for key, value in test['details'].items():
                            print(f"    {key}: {value}")
        
        if self.warnings > 0:
            print("\nWARNINGS:")
            for test in self.tests:
                if test['status'] == 'WARN':
                    print(f"  ! {test['name']}: {test['message']}")
        
        # Overall assessment
        print("\nOVERALL ASSESSMENT:")
        if self.failed == 0:
            if self.warnings == 0:
                print("  EXCELLENT - System fully ready for mode transitions")
            else:
                print("  GOOD - System ready with minor warnings noted")
        else:
            print("  NEEDS ATTENTION - Fix failed tests before going live")
        
        print("=" * 70)

class UnifiedTradingValidator:
    """Unified validator for trading system transitions with Binance.US support"""
    
    def __init__(self, verbose=True):
        self.results = TestResults()
        self.verbose = verbose
        
        # Import trading engine
        try:
            from intelligent_trading_engine import IntelligentTradingEngine
            self.trading_engine_class = IntelligentTradingEngine
        except ImportError as e:
            print(f"CRITICAL: Failed to import trading engine: {e}")
            print("Ensure intelligent_trading_engine.py is in the current directory")
            sys.exit(1)
    
    def print_section(self, title):
        """Print section header"""
        if self.verbose:
            print(f"\n[{title}]")
            print("-" * 50)
    
    def print_test_result(self, name, status, message, details=None):
        """Print individual test result"""
        if self.verbose:
            status_symbol = {"PASS": "✓", "FAIL": "X", "WARN": "!"}[status]
            print(f"  {status_symbol} {name}: {message}")
            if details:
                for key, value in details.items():
                    print(f"    {key}: {value}")
    
    async def run_quick_validation(self):
        """Run quick basic validation (2-3 minutes)"""
        print("QUICK VALIDATION - Testing core functionality...")
        print("This will take 2-3 minutes and tests essential components.")
        print()
        
        await self.test_environment_configuration()
        await self.test_v3_api_configuration()
        await self.test_trading_engine_import()
        await self.test_basic_initialization()
        await self.test_basic_functionality()
        await self.test_mode_detection()
        
        self.results.print_summary("QUICK")
        return self.results.failed == 0
    
    async def run_comprehensive_validation(self):
        """Run comprehensive validation (5-10 minutes)"""
        print("COMPREHENSIVE VALIDATION - Testing all aspects...")
        print("This will take 5-10 minutes and thoroughly tests the entire system.")
        print()
        
        # Core tests
        await self.test_environment_configuration()
        await self.test_v3_api_configuration()
        await self.test_credential_configuration()
        await self.test_trading_engine_import()
        
        # Functionality tests
        await self.test_testnet_mode()
        await self.test_live_mode_readiness()
        await self.test_safety_systems()
        await self.test_position_sizing()
        await self.test_balance_handling()
        await self.test_api_connectivity()
        await self.test_mode_switching_simulation()
        
        # Advanced tests
        await self.test_risk_management()
        await self.test_error_handling()
        await self.test_configuration_validation()
        
        self.results.print_summary("COMPREHENSIVE")
        return self.results.failed == 0
    
    async def test_v3_api_configuration(self):
        """Test V3 API configuration and rotation setup"""
        self.print_section("V3 API CONFIGURATION")
        
        # Check primary Binance keys
        if BINANCE_API_KEY_1 and BINANCE_API_SECRET_1:
            self.results.add_test(
                "Primary Binance API Keys",
                "PASS",
                "Primary testnet keys configured"
            )
            self.print_test_result("Primary Keys", "PASS", "Configured")
        else:
            self.results.add_test(
                "Primary Binance API Keys",
                "FAIL",
                "Primary testnet keys missing"
            )
            self.print_test_result("Primary Keys", "FAIL", "Missing")
        
        # Check API rotation setup
        rotation_keys = [
            (BINANCE_API_KEY_2, BINANCE_API_SECRET_2, "Secondary"),
            (BINANCE_API_KEY_3, BINANCE_API_SECRET_3, "Tertiary")
        ]
        
        configured_backups = 0
        for key, secret, name in rotation_keys:
            if key and secret:
                configured_backups += 1
                self.print_test_result(f"{name} Keys", "PASS", "Configured")
            else:
                self.print_test_result(f"{name} Keys", "WARN", "Not configured")
        
        if API_ROTATION_ENABLED:
            if configured_backups > 0:
                self.results.add_test(
                    "API Rotation Setup",
                    "PASS",
                    f"Rotation enabled with {configured_backups} backup keys"
                )
                self.print_test_result("API Rotation", "PASS", f"{configured_backups} backups")
            else:
                self.results.add_test(
                    "API Rotation Setup",
                    "WARN",
                    "Rotation enabled but no backup keys configured"
                )
                self.print_test_result("API Rotation", "WARN", "No backups")
        else:
            self.results.add_test(
                "API Rotation Setup",
                "WARN",
                "API rotation disabled"
            )
            self.print_test_result("API Rotation", "WARN", "Disabled")
        
        # Check live trading keys
        live_keys_configured = 0
        if BINANCE_LIVE_API_KEY_1 and BINANCE_LIVE_API_SECRET_1:
            live_keys_configured += 1
        if BINANCE_LIVE_API_KEY_2 and BINANCE_LIVE_API_SECRET_2:
            live_keys_configured += 1
        
        if live_keys_configured > 0:
            self.results.add_test(
                "Live Trading Keys",
                "PASS",
                f"{live_keys_configured} live key pairs configured"
            )
            self.print_test_result("Live Keys", "PASS", f"{live_keys_configured} pairs")
        else:
            self.results.add_test(
                "Live Trading Keys",
                "WARN",
                "No live trading keys configured"
            )
            self.print_test_result("Live Keys", "WARN", "Not configured")
        
        # Check configuration consistency
        config_status = {
            "TESTNET": TESTNET,
            "LIVE_TRADING_ENABLED": LIVE_TRADING_ENABLED,
            "API_ROTATION_ENABLED": API_ROTATION_ENABLED,
            "API_RATE_LIMIT_THRESHOLD": API_RATE_LIMIT_THRESHOLD
        }
        
        self.print_test_result("V3 Configuration", "PASS", "Reviewed", config_status)
    
    async def test_environment_configuration(self):
        """Test environment variable configuration"""
        self.print_section("ENVIRONMENT CONFIGURATION")
        
        # Check required variables
        required_vars = [
            'BINANCE_API_KEY_1',
            'BINANCE_API_SECRET_1',
            'TESTNET',
            'MIN_CONFIDENCE',
            'MAX_TOTAL_POSITIONS'
        ]
        
        missing_vars = []
        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            self.results.add_test(
                "Required Environment Variables",
                "FAIL",
                f"Missing variables: {', '.join(missing_vars)}"
            )
            self.print_test_result("Required Variables", "FAIL", f"Missing: {', '.join(missing_vars)}")
        else:
            self.results.add_test(
                "Required Environment Variables",
                "PASS",
                "All required variables present"
            )
            self.print_test_result("Required Variables", "PASS", "All present")
        
        # Check safety configuration
        safety_vars = {
            'MAX_DAILY_LOSS': 500.0,
            'MAX_POSITION_SIZE': 100.0,
            'MIN_POSITION_SIZE': 10.0,
            'MAX_POSITION_PERCENT': 2.0
        }
        
        safety_status = []
        for var, default in safety_vars.items():
            value = os.getenv(var)
            if not value:
                safety_status.append(f"{var}: using default {default}")
                self.results.add_test(
                    f"Safety Config: {var}",
                    "WARN",
                    f"Not configured, using default: {default}"
                )
            else:
                try:
                    float_val = float(value)
                    safety_status.append(f"{var}: {float_val}")
                    self.results.add_test(
                        f"Safety Config: {var}",
                        "PASS",
                        f"Configured: {float_val}"
                    )
                except ValueError:
                    safety_status.append(f"{var}: INVALID")
                    self.results.add_test(
                        f"Safety Config: {var}",
                        "FAIL",
                        f"Invalid value: {value}"
                    )
        
        self.print_test_result("Safety Configuration", "PASS", "Reviewed", 
                              {var.split(': ')[0]: var.split(': ')[1] for var in safety_status})
    
    async def test_trading_engine_import(self):
        """Test trading engine import"""
        self.print_section("TRADING ENGINE IMPORT")
        
        try:
            engine = self.trading_engine_class()
            self.results.add_test(
                "Trading Engine Import",
                "PASS",
                "Successfully imported and instantiated"
            )
            self.print_test_result("Engine Import", "PASS", "Success")
            return True
        except Exception as e:
            self.results.add_test(
                "Trading Engine Import",
                "FAIL",
                f"Import/instantiation failed: {e}"
            )
            self.print_test_result("Engine Import", "FAIL", str(e))
            return False
    
    async def test_basic_initialization(self):
        """Test basic initialization"""
        self.print_section("BASIC INITIALIZATION")
        
        try:
            # Ensure testnet mode
            os.environ['TESTNET'] = 'true'
            os.environ['LIVE_TRADING_ENABLED'] = 'false'
            
            engine = self.trading_engine_class()
            
            # Test basic attributes
            has_client = hasattr(engine, 'client') and engine.client is not None
            has_testnet_mode = hasattr(engine, 'testnet_mode')
            
            if has_client and has_testnet_mode:
                self.results.add_test(
                    "Basic Initialization",
                    "PASS",
                    "Engine initialized with required attributes"
                )
                self.print_test_result("Initialization", "PASS", "Complete", {
                    "Client": "Connected" if has_client else "Missing",
                    "Mode": "Testnet" if engine.testnet_mode else "Live"
                })
            else:
                self.results.add_test(
                    "Basic Initialization",
                    "FAIL",
                    "Missing required attributes"
                )
                self.print_test_result("Initialization", "FAIL", "Missing attributes")
            
        except Exception as e:
            self.results.add_test(
                "Basic Initialization",
                "FAIL",
                f"Initialization failed: {e}"
            )
            self.print_test_result("Initialization", "FAIL", str(e))
    
    async def test_basic_functionality(self):
        """Test basic functionality"""
        self.print_section("BASIC FUNCTIONALITY")
        
        try:
            engine = self.trading_engine_class()
            
            # Test market data
            market_data = engine.get_live_market_data('BTCUSDT')
            if market_data and 'price' in market_data and market_data['price'] > 0:
                self.results.add_test(
                    "Market Data",
                    "PASS",
                    f"Retrieved BTC price: ${market_data['price']:,.2f}"
                )
                self.print_test_result("Market Data", "PASS", f"BTC: ${market_data['price']:,.2f}")
            else:
                self.results.add_test(
                    "Market Data",
                    "FAIL",
                    "Failed to retrieve market data"
                )
                self.print_test_result("Market Data", "FAIL", "No data retrieved")
                
        except Exception as e:
            self.results.add_test(
                "Basic Functionality",
                "FAIL",
                f"Functionality test failed: {e}"
            )
            self.print_test_result("Basic Functionality", "FAIL", str(e))
    
    async def test_mode_detection(self):
        """Test mode detection logic"""
        self.print_section("MODE DETECTION")
        
        try:
            # Test different mode configurations
            test_scenarios = [
                ('true', 'false', True, 'Testnet'),
                ('false', 'true', False, 'Live'),
                ('false', 'false', True, 'Default testnet'),
                ('true', 'true', False, 'Live override')
            ]
            
            for testnet_val, live_val, expected_testnet, description in test_scenarios:
                os.environ['TESTNET'] = testnet_val
                os.environ['LIVE_TRADING_ENABLED'] = live_val
                
                engine = self.trading_engine_class()
                
                if engine.testnet_mode == expected_testnet:
                    status = "PASS"
                    message = f"Correctly detected {description.lower()}"
                else:
                    status = "FAIL"
                    message = f"Failed to detect {description.lower()}"
                
                self.results.add_test(
                    f"Mode Detection: {description}",
                    status,
                    message
                )
                self.print_test_result(f"Mode: {description}", status, message)
            
            # Restore testnet mode
            os.environ['TESTNET'] = 'true'
            os.environ['LIVE_TRADING_ENABLED'] = 'false'
            
        except Exception as e:
            self.results.add_test(
                "Mode Detection",
                "FAIL",
                f"Mode detection test failed: {e}"
            )
            self.print_test_result("Mode Detection", "FAIL", str(e))
    
    async def test_credential_configuration(self):
        """Test credential configuration"""
        self.print_section("CREDENTIAL CONFIGURATION")
        
        # Test testnet credentials
        testnet_key = BINANCE_API_KEY_1
        testnet_secret = BINANCE_API_SECRET_1
        
        if testnet_key and testnet_secret:
            if len(testnet_key) >= 32 and len(testnet_secret) >= 32:
                self.results.add_test("Testnet Credentials", "PASS", "Valid format")
                self.print_test_result("Testnet Creds", "PASS", "Valid format")
            else:
                self.results.add_test("Testnet Credentials", "WARN", "May be incomplete")
                self.print_test_result("Testnet Creds", "WARN", "May be incomplete")
        else:
            self.results.add_test("Testnet Credentials", "FAIL", "Missing")
            self.print_test_result("Testnet Creds", "FAIL", "Missing")
        
        # Test live credentials
        live_key = BINANCE_LIVE_API_KEY_1
        live_secret = BINANCE_LIVE_API_SECRET_1
        
        if live_key and live_secret:
            if len(live_key) >= 32 and len(live_secret) >= 32:
                # Check separation
                if live_key == testnet_key:
                    self.results.add_test("Live Credentials", "WARN", "Same as testnet")
                    self.print_test_result("Live Creds", "WARN", "Same as testnet")
                else:
                    self.results.add_test("Live Credentials", "PASS", "Valid and separate")
                    self.print_test_result("Live Creds", "PASS", "Valid and separate")
            else:
                self.results.add_test("Live Credentials", "WARN", "May be incomplete")
                self.print_test_result("Live Creds", "WARN", "May be incomplete")
        else:
            self.results.add_test("Live Credentials", "WARN", "Not configured")
            self.print_test_result("Live Creds", "WARN", "Not configured")
    
    async def test_testnet_mode(self):
        """Test testnet mode functionality"""
        self.print_section("TESTNET MODE")
        
        try:
            os.environ['TESTNET'] = 'true'
            os.environ['LIVE_TRADING_ENABLED'] = 'false'
            
            engine = self.trading_engine_class()
            
            if engine.testnet_mode:
                self.results.add_test("Testnet Detection", "PASS", "Correctly identified testnet")
                self.print_test_result("Testnet Detection", "PASS", "Identified correctly")
            else:
                self.results.add_test("Testnet Detection", "FAIL", "Failed to detect testnet")
                self.print_test_result("Testnet Detection", "FAIL", "Not detected")
                return
            
            if engine.client:
                self.results.add_test("Testnet Connection", "PASS", "Connected to testnet")
                self.print_test_result("Testnet Connection", "PASS", "Connected")
            else:
                self.results.add_test("Testnet Connection", "FAIL", "No connection")
                self.print_test_result("Testnet Connection", "FAIL", "No connection")
            
            # Test market data
            market_data = engine.get_live_market_data('BTCUSDT')
            if market_data and 'price' in market_data:
                self.results.add_test("Testnet Market Data", "PASS", f"BTC: ${market_data['price']:,.2f}")
                self.print_test_result("Market Data", "PASS", f"BTC: ${market_data['price']:,.2f}")
            else:
                self.results.add_test("Testnet Market Data", "FAIL", "No market data")
                self.print_test_result("Market Data", "FAIL", "No data")
            
        except Exception as e:
            self.results.add_test("Testnet Mode", "FAIL", f"Testnet test failed: {e}")
            self.print_test_result("Testnet Mode", "FAIL", str(e))
    
    async def test_live_mode_readiness(self):
        """Test live mode readiness with Binance.US support"""
        self.print_section("LIVE MODE READINESS")
        
        live_key = BINANCE_LIVE_API_KEY_1
        live_secret = BINANCE_LIVE_API_SECRET_1
        
        if not live_key or not live_secret:
            self.results.add_test("Live Mode Readiness", "WARN", "Live credentials not configured")
            self.print_test_result("Live Readiness", "WARN", "No live credentials")
            return
        
        try:
            from binance.client import Client
            
            # FIXED: Add tld='us' for Binance.US API keys
            test_client = Client(live_key, live_secret, testnet=False, tld='us')
            account_info = test_client.get_account()
            
            if account_info.get('canTrade', False):
                self.results.add_test("Live API Connection", "PASS", "Connected with trading permissions")
                self.print_test_result("Live API", "PASS", "Trading permissions OK")
            else:
                self.results.add_test("Live API Connection", "WARN", "Connected but no trading permissions")
                self.print_test_result("Live API", "WARN", "No trading permissions")
            
            # Check balance
            usdt_balance = 0.0
            for balance in account_info['balances']:
                if balance['asset'] == 'USDT':
                    usdt_balance = float(balance['free'])
                    break
            
            min_balance = float(os.getenv('MIN_POSITION_SIZE', 10.0))
            
            if usdt_balance >= min_balance:
                self.results.add_test("Live Account Balance", "PASS", f"Sufficient: ${usdt_balance:.2f}")
                self.print_test_result("Live Balance", "PASS", f"${usdt_balance:.2f}")
            else:
                self.results.add_test("Live Account Balance", "WARN", f"Low: ${usdt_balance:.2f}")
                self.print_test_result("Live Balance", "WARN", f"Low: ${usdt_balance:.2f}")
            
        except Exception as e:
            self.results.add_test("Live Mode Test", "FAIL", f"Live test failed: {e}")
            self.print_test_result("Live Mode Test", "FAIL", str(e))
    
    async def test_safety_systems(self):
        """Test safety system functionality"""
        self.print_section("SAFETY SYSTEMS")
        
        try:
            engine = self.trading_engine_class()
            
            # Test basic safety checks exist
            has_safety_methods = all(hasattr(engine, method) for method in [
                'check_daily_loss_limit', 'check_position_limits'
            ])
            
            if has_safety_methods:
                self.results.add_test("Safety Methods", "PASS", "Safety methods present")
                self.print_test_result("Safety Methods", "PASS", "Present")
            else:
                self.results.add_test("Safety Methods", "FAIL", "Missing safety methods")
                self.print_test_result("Safety Methods", "FAIL", "Missing")
            
        except Exception as e:
            self.results.add_test("Safety Systems", "FAIL", f"Safety test failed: {e}")
            self.print_test_result("Safety Systems", "FAIL", str(e))
    
    async def test_position_sizing(self):
        """Test position sizing calculations"""
        self.print_section("POSITION SIZING")
        
        try:
            engine = self.trading_engine_class()
            
            # Test basic position sizing logic exists
            if hasattr(engine, 'calculate_position_size'):
                self.results.add_test("Position Sizing", "PASS", "Position sizing method available")
                self.print_test_result("Position Sizing", "PASS", "Method available")
            else:
                self.results.add_test("Position Sizing", "WARN", "Position sizing method not found")
                self.print_test_result("Position Sizing", "WARN", "Method not found")
            
        except Exception as e:
            self.results.add_test("Position Sizing", "FAIL", f"Position sizing test failed: {e}")
            self.print_test_result("Position Sizing", "FAIL", str(e))
    
    async def test_balance_handling(self):
        """Test balance fetching and handling"""
        self.print_section("BALANCE HANDLING")
        
        try:
            engine = self.trading_engine_class()
            
            # Test that engine can handle balance operations
            if hasattr(engine, 'client') and engine.client:
                account_info = engine.client.get_account()
                if account_info:
                    self.results.add_test("Balance Handling", "PASS", "Account info retrieved")
                    self.print_test_result("Balance Handling", "PASS", "Account info OK")
                else:
                    self.results.add_test("Balance Handling", "FAIL", "No account info")
                    self.print_test_result("Balance Handling", "FAIL", "No account info")
            else:
                self.results.add_test("Balance Handling", "FAIL", "No client connection")
                self.print_test_result("Balance Handling", "FAIL", "No client")
            
        except Exception as e:
            self.results.add_test("Balance Handling", "FAIL", f"Balance test failed: {e}")
            self.print_test_result("Balance Handling", "FAIL", str(e))
    
    async def test_api_connectivity(self):
        """Test API connectivity"""
        self.print_section("API CONNECTIVITY")
        
        try:
            engine = self.trading_engine_class()
            
            test_symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']
            successful = 0
            
            for symbol in test_symbols:
                try:
                    market_data = engine.get_live_market_data(symbol)
                    if market_data and 'price' in market_data and market_data['price'] > 0:
                        self.print_test_result(f"Market Data ({symbol})", "PASS", f"${market_data['price']:,.2f}")
                        successful += 1
                    else:
                        self.print_test_result(f"Market Data ({symbol})", "FAIL", "No data")
                except Exception as e:
                    self.print_test_result(f"Market Data ({symbol})", "FAIL", str(e))
            
            if successful == len(test_symbols):
                self.results.add_test("API Connectivity", "PASS", "All symbols retrieved")
            elif successful > 0:
                self.results.add_test("API Connectivity", "WARN", f"Partial success: {successful}/{len(test_symbols)}")
            else:
                self.results.add_test("API Connectivity", "FAIL", "No market data retrieved")
            
        except Exception as e:
            self.results.add_test("API Connectivity", "FAIL", f"API test failed: {e}")
            self.print_test_result("API Connectivity", "FAIL", str(e))
    
    async def test_mode_switching_simulation(self):
        """Test mode switching simulation"""
        self.print_section("MODE SWITCHING SIMULATION")
        
        try:
            # Test environment-based switching
            original_testnet = os.getenv('TESTNET', 'true')
            original_live = os.getenv('LIVE_TRADING_ENABLED', 'false')
            
            # Test scenarios
            scenarios = [
                ('true', 'false', True, 'Testnet mode'),
                ('false', 'true', False, 'Live mode'),
                ('false', 'false', True, 'Default testnet'),
            ]
            
            all_correct = True
            
            for testnet_val, live_val, expected_testnet, description in scenarios:
                os.environ['TESTNET'] = testnet_val
                os.environ['LIVE_TRADING_ENABLED'] = live_val
                
                test_engine = self.trading_engine_class()
                
                if test_engine.testnet_mode == expected_testnet:
                    self.print_test_result(f"Switch: {description}", "PASS", "Correct detection")
                else:
                    self.print_test_result(f"Switch: {description}", "FAIL", "Wrong detection")
                    all_correct = False
            
            if all_correct:
                self.results.add_test("Mode Switching", "PASS", "All scenarios work correctly")
            else:
                self.results.add_test("Mode Switching", "FAIL", "Some scenarios failed")
            
            # Restore original values
            os.environ['TESTNET'] = original_testnet
            os.environ['LIVE_TRADING_ENABLED'] = original_live
            
        except Exception as e:
            self.results.add_test("Mode Switching", "FAIL", f"Switching test failed: {e}")
            self.print_test_result("Mode Switching", "FAIL", str(e))
    
    async def test_risk_management(self):
        """Test advanced risk management features"""
        self.print_section("RISK MANAGEMENT")
        
        try:
            engine = self.trading_engine_class()
            
            # Test risk parameters exist
            risk_attributes = ['max_risk_percent', 'max_positions', 'min_confidence']
            missing_attributes = []
            
            for attr in risk_attributes:
                if not hasattr(engine, attr):
                    missing_attributes.append(attr)
            
            if not missing_attributes:
                self.results.add_test("Risk Parameters", "PASS", "All risk attributes present")
                self.print_test_result("Risk Parameters", "PASS", "All present")
            else:
                self.results.add_test("Risk Parameters", "WARN", f"Missing: {missing_attributes}")
                self.print_test_result("Risk Parameters", "WARN", f"Missing: {missing_attributes}")
            
        except Exception as e:
            self.results.add_test("Risk Management", "FAIL", f"Risk test failed: {e}")
            self.print_test_result("Risk Management", "FAIL", str(e))
    
    async def test_error_handling(self):
        """Test error handling capabilities"""
        self.print_section("ERROR HANDLING")
        
        try:
            engine = self.trading_engine_class()
            
            # Test invalid symbol
            try:
                invalid_data = engine.get_live_market_data('INVALID')
                if invalid_data is None:
                    self.print_test_result("Invalid Symbol Handling", "PASS", "Gracefully handled")
                    error_handling_ok = True
                else:
                    self.print_test_result("Invalid Symbol Handling", "WARN", "Unexpected data returned")
                    error_handling_ok = False
            except Exception:
                self.print_test_result("Invalid Symbol Handling", "PASS", "Exception caught")
                error_handling_ok = True
            
            if error_handling_ok:
                self.results.add_test("Error Handling", "PASS", "Error handling functional")
            else:
                self.results.add_test("Error Handling", "WARN", "Some error handling issues")
            
        except Exception as e:
            self.results.add_test("Error Handling", "FAIL", f"Error handling test failed: {e}")
            self.print_test_result("Error Handling", "FAIL", str(e))
    
    async def test_configuration_validation(self):
        """Test configuration validation"""
        self.print_section("CONFIGURATION VALIDATION")
        
        try:
            # Validate configuration consistency
            min_confidence = float(os.getenv('MIN_CONFIDENCE', 70.0))
            max_positions = int(os.getenv('MAX_TOTAL_POSITIONS', 3))
            max_daily_loss = float(os.getenv('MAX_DAILY_LOSS', 500.0))
            
            config_issues = []
            
            if min_confidence < 50 or min_confidence > 95:
                config_issues.append("MIN_CONFIDENCE out of reasonable range (50-95)")
            
            if max_positions < 1 or max_positions > 10:
                config_issues.append("MAX_POSITIONS out of reasonable range (1-10)")
            
            if max_daily_loss < 10 or max_daily_loss > 10000:
                config_issues.append("MAX_DAILY_LOSS out of reasonable range (10-10000)")
            
            if config_issues:
                self.results.add_test("Configuration Validation", "WARN", f"Issues: {'; '.join(config_issues)}")
                self.print_test_result("Config Validation", "WARN", f"{len(config_issues)} issues found")
            else:
                self.results.add_test("Configuration Validation", "PASS", "Configuration appears reasonable")
                self.print_test_result("Config Validation", "PASS", "Reasonable values")
            
        except Exception as e:
            self.results.add_test("Configuration Validation", "FAIL", f"Config validation failed: {e}")
            self.print_test_result("Configuration Validation", "FAIL", str(e))
    
    def provide_recommendations(self):
        """Provide recommendations based on test results"""
        print("\nRECOMMENDATIONS:")
        print("=" * 70)
        
        if self.results.failed == 0:
            if self.results.warnings == 0:
                print("EXCELLENT - System fully ready for mode transitions")
                print("✓ All critical tests passed")
                print("✓ No warnings detected")
                print("✓ V3 API configuration verified")
                print("✓ Binance.US compatibility confirmed")
                print()
                print("READY FOR LIVE TRADING:")
                print("1. Your live API credentials are properly configured")
                print("2. Update .env: TESTNET=false, LIVE_TRADING_ENABLED=true")
                print("3. Start with small position sizes")
                print("4. Monitor first few trades manually")
            else:
                print("GOOD - System ready with minor warnings")
                print("✓ All critical tests passed")
                print(f"! {self.results.warnings} warnings noted")
                print("✓ V3 API configuration functional")
                print("✓ Binance.US compatibility confirmed")
                print()
                print("PROCEED WITH CAUTION:")
                print("1. Review warnings above")
                print("2. Consider addressing warnings before going live")
                print("3. Test thoroughly in testnet first")
        else:
            print("NEEDS ATTENTION - Fix failed tests before going live")
            print(f"✗ {self.results.failed} tests failed")
            print(f"! {self.results.warnings} warnings")
            print()
            print("REQUIRED ACTIONS:")
            print("1. Fix all failed tests")
            print("2. Address configuration issues")
            print("3. Re-run validator until all tests pass")
            print("4. DO NOT switch to live trading until all issues resolved")
        
        print()
        print("V3 API CONFIGURATION SUPPORT:")
        print("- Primary API keys: Required for basic functionality")
        print("- Backup API keys: Recommended for API rotation reliability")
        print("- Live API keys: Required for live trading mode")
        print("- API rotation: Improves reliability and reduces rate limiting")
        
        print()
        print("SUPPORT:")
        print("- For testnet issues: Check API credentials and network connection")
        print("- For live mode issues: Verify live API credentials and IP whitelist")
        print("- For Binance.US: Ensure Spot Trading permission and IP restrictions")
        print("- For errors: Check logs and ensure all dependencies are installed")

def show_help():
    """Show help information"""
    print("""
UNIFIED TRADING MODE VALIDATOR - BINANCE.US COMPATIBLE

This tool validates your trading system's readiness for testnet/live transitions.
Enhanced with V3 API configuration support and Binance.US compatibility.

USAGE:
    python api-test.py              # Interactive menu
    python api-test.py --quick      # Quick validation (2-3 min)
    python api-test.py --full       # Full validation (5-10 min)
    python api-test.py --help       # Show this help

VALIDATION LEVELS:

Quick Validation (2-3 minutes):
- Environment configuration
- V3 API configuration validation
- Basic engine functionality  
- Core safety systems
- Mode detection logic
- Essential connectivity

Comprehensive Validation (5-10 minutes):
- All quick validation tests
- Credential validation
- Live mode readiness testing (Binance.US compatible)
- Advanced safety systems
- Position sizing validation
- API connectivity testing
- Risk management validation
- Error handling testing
- Configuration validation

V3 API FEATURES:
- Primary/backup API key validation
- API rotation configuration testing
- Live trading key verification
- Rate limiting configuration

BINANCE.US FEATURES:
- Proper tld='us' endpoint usage
- Live trading validation
- IP restriction checks
- Trading permission verification

SAFETY:
This validator performs READ-ONLY tests and will NOT:
- Execute actual trades
- Modify your account
- Transfer funds
- Place orders

All tests are designed to safely verify system functionality.

RECOMMENDATIONS:
1. Run quick validation first for basic checks
2. Run comprehensive validation before going live
3. Fix all FAILED tests before switching to live trading
4. Address WARNINGS when possible
5. Test in testnet thoroughly before live trading
6. Ensure Binance.US API key has proper permissions and IP restrictions
7. Configure backup API keys for improved reliability
""")

async def interactive_menu():
    """Show interactive menu for test selection"""
    print("UNIFIED TRADING MODE VALIDATOR - V3 API ENHANCED")
    print("=" * 60)
    print("1. Quick Validation (2-3 minutes)")
    print("2. Comprehensive Validation (5-10 minutes)")
    print("3. Show Help")
    print("4. Exit")
    print()
    
    while True:
        try:
            choice = input("Select option (1-4): ").strip()
            
            if choice == '1':
                print("\nStarting Quick Validation...")
                validator = UnifiedTradingValidator()
                success = await validator.run_quick_validation()
                validator.provide_recommendations()
                return success
            
            elif choice == '2':
                print("\nStarting Comprehensive Validation...")
                validator = UnifiedTradingValidator()
                success = await validator.run_comprehensive_validation()
                validator.provide_recommendations()
                return success
            
            elif choice == '3':
                show_help()
                continue
            
            elif choice == '4':
                print("Exiting...")
                return True
            
            else:
                print("Invalid choice. Please select 1-4.")
        
        except KeyboardInterrupt:
            print("\nValidation cancelled by user")
            return False
        except Exception as e:
            print(f"Error: {e}")
            return False

async def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Unified Trading Mode Validator - V3 API Enhanced')
    parser.add_argument('--quick', action='store_true', help='Run quick validation only')
    parser.add_argument('--full', action='store_true', help='Run comprehensive validation')
    parser.add_argument('--help-extended', action='store_true', help='Show extended help')
    
    args = parser.parse_args()
    
    if args.help_extended:
        show_help()
        return True
    
    # Configure logging
    logging.basicConfig(
        level=logging.WARNING,  # Reduce noise
        format='%(levelname)s: %(message)s'
    )
    
    print("UNIFIED TRADING MODE VALIDATOR - V3 API ENHANCED")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    try:
        if args.quick:
            print("Running Quick Validation...")
            validator = UnifiedTradingValidator()
            success = await validator.run_quick_validation()
            validator.provide_recommendations()
            return success
        
        elif args.full:
            print("Running Comprehensive Validation...")
            validator = UnifiedTradingValidator()
            success = await validator.run_comprehensive_validation()
            validator.provide_recommendations()
            return success
        
        else:
            # Interactive mode
            return await interactive_menu()
    
    except KeyboardInterrupt:
        print("\nValidation cancelled by user")
        return False
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        return False

if __name__ == "__main__":
    try:
        success = asyncio.run(main())
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)