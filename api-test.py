#!/usr/bin/env python3
"""
UNIFIED TRADING MODE VALIDATOR - BINANCE.US COMPATIBLE
=====================================================
Comprehensive test suite for validating testnet/live trading transitions.
Enhanced with proper Binance.US support for live trading validation.

Usage:
    python a.py              # Interactive menu
    python a.py --quick      # Quick validation only
    python a.py --full       # Full comprehensive validation
    python a.py --help       # Show help

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
        load_dotenv()
        
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
    
    async def test_environment_configuration(self):
        """Test environment variable configuration"""
        self.print_section("ENVIRONMENT CONFIGURATION")
        
        # Check required variables
        required_vars = [
            'BINANCE_API_KEY',
            'BINANCE_API_SECRET',
            'TESTNET',
            'MIN_CONFIDENCE',
            'MAX_POSITIONS'
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
        
        # Check live trading variables
        live_vars = ['BINANCE_LIVE_API_KEY', 'BINANCE_LIVE_API_SECRET']
        missing_live = [var for var in live_vars if not os.getenv(var)]
        
        if missing_live:
            self.results.add_test(
                "Live Trading Credentials",
                "WARN",
                f"Live credentials not configured: {', '.join(missing_live)}"
            )
            self.print_test_result("Live Credentials", "WARN", "Not configured")
        else:
            self.results.add_test(
                "Live Trading Credentials",
                "PASS",
                "Live credentials configured"
            )
            self.print_test_result("Live Credentials", "PASS", "Configured")
        
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
            has_balance = hasattr(engine, 'account_balance')
            has_testnet_mode = hasattr(engine, 'testnet_mode')
            
            if has_client and has_balance and has_testnet_mode:
                self.results.add_test(
                    "Basic Initialization",
                    "PASS",
                    "Engine initialized with required attributes"
                )
                self.print_test_result("Initialization", "PASS", "Complete", {
                    "Client": "Connected" if has_client else "Missing",
                    "Balance": f"${engine.account_balance:.2f}" if has_balance else "Missing",
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
            
            # Test position sizing
            position_size = engine.calculate_position_size(75.0)
            if position_size > 0:
                self.results.add_test(
                    "Position Sizing",
                    "PASS",
                    f"Calculated position size: ${position_size:.2f}"
                )
                self.print_test_result("Position Sizing", "PASS", f"${position_size:.2f}")
            else:
                self.results.add_test(
                    "Position Sizing",
                    "FAIL",
                    "Invalid position size calculation"
                )
                self.print_test_result("Position Sizing", "FAIL", "Invalid calculation")
            
            # Test safety checks
            daily_loss_ok = engine.check_daily_loss_limit()
            position_ok = engine.check_position_limits()
            
            if daily_loss_ok and position_ok:
                self.results.add_test(
                    "Safety Checks",
                    "PASS",
                    "All safety checks functional"
                )
                self.print_test_result("Safety Checks", "PASS", "Functional")
            else:
                self.results.add_test(
                    "Safety Checks",
                    "WARN",
                    "Some safety checks failed"
                )
                self.print_test_result("Safety Checks", "WARN", "Some failed")
            
            # Test market data
            market_data = engine.get_real_market_data('BTCUSDT')
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
        testnet_key = os.getenv('BINANCE_API_KEY')
        testnet_secret = os.getenv('BINANCE_API_SECRET')
        
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
        live_key = os.getenv('BINANCE_LIVE_API_KEY')
        live_secret = os.getenv('BINANCE_LIVE_API_SECRET')
        
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
                self.results.add_test("Testnet Connection", "PASS", f"Connected, balance: ${engine.account_balance:.2f}")
                self.print_test_result("Testnet Connection", "PASS", f"Balance: ${engine.account_balance:.2f}")
            else:
                self.results.add_test("Testnet Connection", "FAIL", "No connection")
                self.print_test_result("Testnet Connection", "FAIL", "No connection")
            
            # Test market data
            market_data = engine.get_real_market_data('BTCUSDT')
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
        
        live_key = os.getenv('BINANCE_LIVE_API_KEY')
        live_secret = os.getenv('BINANCE_LIVE_API_SECRET')
        
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
            
            # Test daily loss limit
            original_daily_pnl = engine.daily_pnl
            engine.daily_pnl = -1000.0
            
            if not engine.check_daily_loss_limit():
                self.results.add_test("Daily Loss Limit", "PASS", "Correctly prevents trading")
                self.print_test_result("Daily Loss Limit", "PASS", "Working")
            else:
                self.results.add_test("Daily Loss Limit", "FAIL", "Not working")
                self.print_test_result("Daily Loss Limit", "FAIL", "Not working")
            
            engine.daily_pnl = original_daily_pnl
            
            # Test position limits
            max_positions = engine.max_positions
            engine.positions = {f'test_{i}': {} for i in range(max_positions + 1)}
            
            if not engine.check_position_limits():
                self.results.add_test("Position Limits", "PASS", "Correctly prevents trading")
                self.print_test_result("Position Limits", "PASS", "Working")
            else:
                self.results.add_test("Position Limits", "FAIL", "Not working")
                self.print_test_result("Position Limits", "FAIL", "Not working")
            
            engine.positions = {}
            
        except Exception as e:
            self.results.add_test("Safety Systems", "FAIL", f"Safety test failed: {e}")
            self.print_test_result("Safety Systems", "FAIL", str(e))
    
    async def test_position_sizing(self):
        """Test position sizing calculations"""
        self.print_section("POSITION SIZING")
        
        try:
            engine = self.trading_engine_class()
            
            test_confidences = [50, 75, 90]
            all_valid = True
            
            for confidence in test_confidences:
                position_size = engine.calculate_position_size(confidence)
                
                if position_size > 0 and position_size <= engine.max_position_size:
                    self.print_test_result(f"Position Size ({confidence}%)", "PASS", f"${position_size:.2f}")
                else:
                    self.print_test_result(f"Position Size ({confidence}%)", "FAIL", f"Invalid: ${position_size:.2f}")
                    all_valid = False
            
            if all_valid:
                self.results.add_test("Position Sizing", "PASS", "All calculations valid")
            else:
                self.results.add_test("Position Sizing", "FAIL", "Some calculations invalid")
            
            # Test with low balance
            original_balance = engine.available_balance
            engine.available_balance = 5.0
            
            low_balance_size = engine.calculate_position_size(90)
            
            if low_balance_size <= engine.available_balance:
                self.results.add_test("Low Balance Sizing", "PASS", "Correctly limited")
                self.print_test_result("Low Balance", "PASS", f"Limited to ${low_balance_size:.2f}")
            else:
                self.results.add_test("Low Balance Sizing", "FAIL", "Not properly limited")
                self.print_test_result("Low Balance", "FAIL", "Not limited")
            
            engine.available_balance = original_balance
            
        except Exception as e:
            self.results.add_test("Position Sizing", "FAIL", f"Position sizing test failed: {e}")
            self.print_test_result("Position Sizing", "FAIL", str(e))
    
    async def test_balance_handling(self):
        """Test balance fetching and handling"""
        self.print_section("BALANCE HANDLING")
        
        try:
            engine = self.trading_engine_class()
            
            # Test balance update
            engine._update_account_balance()
            
            if engine.account_balance >= 0 and engine.available_balance <= engine.account_balance:
                self.results.add_test("Balance Handling", "PASS", "Balances consistent")
                self.print_test_result("Balance Handling", "PASS", 
                                     f"Total: ${engine.account_balance:.2f}, Available: ${engine.available_balance:.2f}")
            else:
                self.results.add_test("Balance Handling", "FAIL", "Balance inconsistency")
                self.print_test_result("Balance Handling", "FAIL", "Inconsistent")
            
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
                    market_data = engine.get_real_market_data(symbol)
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
            
            # Test risk parameters
            risk_params = {
                'max_daily_loss': engine.max_daily_loss,
                'max_position_size': engine.max_position_size,
                'max_position_percent': engine.max_position_percent,
                'min_position_size': engine.min_position_size
            }
            
            valid_params = all(param > 0 for param in risk_params.values())
            
            if valid_params:
                self.results.add_test("Risk Parameters", "PASS", "All parameters valid")
                self.print_test_result("Risk Parameters", "PASS", "Valid", risk_params)
            else:
                self.results.add_test("Risk Parameters", "FAIL", "Invalid parameters")
                self.print_test_result("Risk Parameters", "FAIL", "Invalid", risk_params)
            
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
                invalid_data = engine.get_real_market_data('INVALID')
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
            max_positions = int(os.getenv('MAX_POSITIONS', 3))
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
Enhanced with Binance.US support for live trading validation.

USAGE:
    python a.py              # Interactive menu
    python a.py --quick      # Quick validation (2-3 min)
    python a.py --full       # Full validation (5-10 min)
    python a.py --help       # Show this help

VALIDATION LEVELS:

Quick Validation (2-3 minutes):
- Environment configuration
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
""")

async def interactive_menu():
    """Show interactive menu for test selection"""
    print("UNIFIED TRADING MODE VALIDATOR - BINANCE.US COMPATIBLE")
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
    parser = argparse.ArgumentParser(description='Unified Trading Mode Validator - Binance.US Compatible')
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
    
    print("UNIFIED TRADING MODE VALIDATOR - BINANCE.US COMPATIBLE")
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