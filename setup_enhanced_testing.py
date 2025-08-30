#!/usr/bin/env python3
"""
V3 ENHANCED TEST SUITE SETUP SCRIPT
===================================

Automated setup script for the V3 Enhanced Comprehensive Test Suite with Browser Automation.
This script will:
1. Install all required dependencies
2. Setup browser drivers for testing
3. Configure the testing environment
4. Validate the setup
5. Run initial system checks

Usage:
    python setup_enhanced_testing.py
    python setup_enhanced_testing.py --install-browsers
    python setup_enhanced_testing.py --validate-only
"""

import subprocess
import sys
import os
import platform
import urllib.request
import zipfile
import tarfile
from pathlib import Path
import json
import time


class EnhancedTestSuiteSetup:
    """Setup manager for V3 Enhanced Test Suite."""
    
    def __init__(self):
        self.platform = platform.system().lower()
        self.python_version = sys.version_info
        self.setup_dir = Path.cwd()
        self.drivers_dir = self.setup_dir / "drivers"
        self.drivers_dir.mkdir(exist_ok=True)
        
        # Required packages for enhanced testing
        self.required_packages = [
            # Core testing packages
            "pytest>=7.4.0",
            "pytest-asyncio>=0.21.0",
            "selenium>=4.15.0",
            "webdriver-manager>=4.0.0",
            
            # System monitoring and performance
            "psutil>=5.9.5",
            "memory-profiler>=0.61.0",
            
            # Data analysis for testing
            "pandas>=2.0.0",
            "numpy>=1.24.0",
            
            # Environment and configuration
            "python-dotenv>=1.0.0",
            "pyyaml>=6.0.1",
            
            # HTTP and network testing
            "requests>=2.31.0",
            "aiohttp>=3.8.5",
            
            # Additional testing utilities
            "faker>=19.3.0",
            "hypothesis>=6.82.0",
            "mock>=5.1.0",
            
            # Progress and reporting
            "tqdm>=4.65.0",
            "rich>=13.4.2",
            "coloredlogs>=15.0"
        ]
        
        # Browser driver URLs (latest stable versions)
        self.driver_urls = {
            'chrome': {
                'windows': 'https://chromedriver.storage.googleapis.com/LATEST_RELEASE',
                'linux': 'https://chromedriver.storage.googleapis.com/LATEST_RELEASE',
                'darwin': 'https://chromedriver.storage.googleapis.com/LATEST_RELEASE'
            }
        }
        
        print("V3 Enhanced Test Suite Setup Initialized")
        print(f"Platform: {platform.system()} {platform.machine()}")
        print(f"Python: {sys.version}")
        print(f"Setup directory: {self.setup_dir}")
    
    def check_python_version(self) -> bool:
        """Check if Python version is compatible."""
        print("\n1. Checking Python version compatibility...")
        
        if self.python_version < (3, 8):
            print(f"? Python 3.8+ required. Current: {sys.version}")
            print("Please upgrade Python and try again.")
            return False
        
        print(f"? Python {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro} is compatible")
        return True
    
    def install_packages(self) -> bool:
        """Install required Python packages."""
        print("\n2. Installing required packages...")
        
        try:
            # Upgrade pip first
            print("Upgrading pip...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "pip"])
            
            # Install packages
            for i, package in enumerate(self.required_packages, 1):
                print(f"Installing {package} ({i}/{len(self.required_packages)})...")
                try:
                    subprocess.check_call([
                        sys.executable, "-m", "pip", "install", package, "--upgrade"
                    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    print(f"? {package}")
                except subprocess.CalledProcessError as e:
                    print(f"??  Failed to install {package}: {e}")
                    # Continue with other packages
            
            print("? Package installation completed")
            return True
            
        except Exception as e:
            print(f"? Package installation failed: {e}")
            return False
    
    def install_browser_drivers(self) -> bool:
        """Install browser drivers for testing."""
        print("\n3. Installing browser drivers...")
        
        try:
            # Use webdriver-manager for automatic driver management
            print("Installing Chrome WebDriver using webdriver-manager...")
            subprocess.check_call([
                sys.executable, "-c",
                "from selenium import webdriver; from webdriver_manager.chrome import ChromeDriverManager; "
                "from selenium.webdriver.chrome.options import Options; "
                "options = Options(); options.add_argument('--headless'); "
                "driver = webdriver.Chrome(ChromeDriverManager().install(), options=options); "
                "print('Chrome driver installed successfully'); driver.quit()"
            ])
            
            print("? Chrome WebDriver installed and verified")
            
            # Try Firefox as fallback
            try:
                print("Installing Firefox WebDriver using webdriver-manager...")
                subprocess.check_call([
                    sys.executable, "-c",
                    "from selenium import webdriver; from webdriver_manager.firefox import GeckoDriverManager; "
                    "from selenium.webdriver.firefox.options import Options; "
                    "options = Options(); options.add_argument('--headless'); "
                    "driver = webdriver.Firefox(executable_path=GeckoDriverManager().install(), options=options); "
                    "print('Firefox driver installed successfully'); driver.quit()"
                ])
                print("? Firefox WebDriver installed and verified")
            except:
                print("??  Firefox WebDriver installation failed (Chrome is sufficient)")
            
            return True
            
        except Exception as e:
            print(f"? Browser driver installation failed: {e}")
            print("You may need to install Chrome browser manually")
            return False
    
    def validate_trading_system_files(self) -> bool:
        """Validate that V3 trading system files exist."""
        print("\n4. Validating V3 trading system files...")
        
        required_files = [
            'main.py', 'main_controller.py', 'requirements.txt',
            'dashbored.html', '.env'  # .env might not exist yet
        ]
        
        optional_files = [
            'api_monitor.py', 'intelligent_trading_engine.py',
            'advanced_ml_engine.py', 'market_analysis_engine.py'
        ]
        
        missing_required = []
        missing_optional = []
        
        for file in required_files:
            if file == '.env':
                # Check for .env or .env.template
                if not (Path(file).exists() or Path('.env.template').exists()):
                    missing_required.append(file + " (or .env.template)")
            elif not Path(file).exists():
                missing_required.append(file)
        
        for file in optional_files:
            if not Path(file).exists():
                missing_optional.append(file)
        
        if missing_required:
            print(f"? Missing required files: {missing_required}")
            print("Please ensure you're running this from the V3 project directory")
            return False
        
        print(f"? All required V3 system files found")
        
        if missing_optional:
            print(f"??  Optional files not found: {missing_optional[:3]}")
            print("Some advanced features may not be available for testing")
        
        return True
    
    def setup_environment_config(self) -> bool:
        """Setup environment configuration for testing."""
        print("\n5. Setting up testing environment configuration...")
        
        try:
            # Check if .env exists
            if Path('.env').exists():
                print("? .env file already exists")
            elif Path('.env.template').exists():
                print("Creating .env from .env.template...")
                with open('.env.template', 'r') as template:
                    content = template.read()
                
                # Modify for testing (ensure mock data is disabled)
                test_config = content
                test_config = test_config.replace('ENABLE_MOCK_APIS=true', 'ENABLE_MOCK_APIS=false')
                test_config = test_config.replace('CLEAR_MOCK_ML_DATA=false', 'CLEAR_MOCK_ML_DATA=true')
                test_config = test_config.replace('TESTNET=false', 'TESTNET=true')
                
                with open('.env', 'w') as env_file:
                    env_file.write(test_config)
                
                print("? .env file created with testing configuration")
            else:
                print("??  No .env or .env.template found")
                print("Creating basic .env for testing...")
                
                basic_env = """# V3 Trading System - Testing Configuration
TESTNET=true
ENABLE_MOCK_APIS=false
CLEAR_MOCK_ML_DATA=true
USE_REAL_DATA_ONLY=true

# Flask configuration
FLASK_PORT=8102
FLASK_DEBUG=false

# API Keys (add your keys here)
BINANCE_API_KEY_1=your_testnet_api_key
BINANCE_API_SECRET_1=your_testnet_api_secret

# Trading parameters
MAX_TOTAL_POSITIONS=3
TRADE_AMOUNT_USDT=10.0
MIN_CONFIDENCE=70.0

# Logging
LOG_LEVEL=INFO
"""
                with open('.env', 'w') as f:
                    f.write(basic_env)
                
                print("? Basic .env file created")
                print("??  Please add your API keys to the .env file")
            
            # Create test directories
            test_dirs = ['logs', 'data', 'test_screenshots', 'test_results']
            for dir_name in test_dirs:
                Path(dir_name).mkdir(exist_ok=True)
            
            print("? Test directories created")
            return True
            
        except Exception as e:
            print(f"? Environment setup failed: {e}")
            return False
    
    def validate_setup(self) -> bool:
        """Validate the complete setup."""
        print("\n6. Validating complete setup...")
        
        validation_results = {
            'selenium_import': False,
            'webdriver_chrome': False,
            'pandas_import': False,
            'dotenv_import': False,
            'psutil_import': False
        }
        
        # Test imports
        try:
            import selenium
            validation_results['selenium_import'] = True
            print("? Selenium import successful")
        except ImportError:
            print("? Selenium import failed")
        
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            options = Options()
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            
            driver = webdriver.Chrome(options=options)
            driver.quit()
            validation_results['webdriver_chrome'] = True
            print("? Chrome WebDriver functional")
        except Exception as e:
            print(f"? Chrome WebDriver test failed: {e}")
        
        try:
            import pandas as pd
            validation_results['pandas_import'] = True
            print("? Pandas import successful")
        except ImportError:
            print("? Pandas import failed")
        
        try:
            from dotenv import load_dotenv
            validation_results['dotenv_import'] = True
            print("? Python-dotenv import successful")
        except ImportError:
            print("? Python-dotenv import failed")
        
        try:
            import psutil
            validation_results['psutil_import'] = True
            print("? Psutil import successful")
        except ImportError:
            print("? Psutil import failed")
        
        # Overall validation
        critical_validations = ['selenium_import', 'webdriver_chrome']
        critical_passed = all(validation_results[key] for key in critical_validations)
        
        total_passed = sum(validation_results.values())
        total_tests = len(validation_results)
        
        if critical_passed and total_passed >= total_tests * 0.8:
            print(f"? Setup validation passed ({total_passed}/{total_tests} tests)")
            return True
        else:
            print(f"? Setup validation failed ({total_passed}/{total_tests} tests)")
            return False
    
    def run_quick_test(self) -> bool:
        """Run a quick test to ensure everything is working."""
        print("\n7. Running quick test...")
        
        try:
            # Create a simple test script
            test_script = '''
import sys
import os
from pathlib import Path

# Add current directory to path
sys.path.insert(0, str(Path.cwd()))

try:
    # Test basic imports
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    import pandas as pd
    import psutil
    from dotenv import load_dotenv
    
    print("? All critical imports successful")
    
    # Test browser automation (quick test)
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-logging')
    
    driver = webdriver.Chrome(options=options)
    driver.get("data:text/html,<html><body><h1>Test Page</h1></body></html>")
    title = driver.title
    driver.quit()
    
    print(f"? Browser automation test successful")
    
    # Test system monitoring
    cpu = psutil.cpu_percent()
    memory = psutil.virtual_memory().percent
    print(f"? System monitoring test successful (CPU: {cpu}%, Memory: {memory}%)")
    
    print("? Quick test completed successfully")
    
except Exception as e:
    print(f"? Quick test failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
'''
            
            # Write and run test script
            with open('quick_test.py', 'w') as f:
                f.write(test_script)
            
            result = subprocess.run([sys.executable, 'quick_test.py'], 
                                  capture_output=True, text=True, timeout=30)
            
            # Clean up
            Path('quick_test.py').unlink(missing_ok=True)
            
            if result.returncode == 0:
                print("? Quick test passed")
                return True
            else:
                print(f"? Quick test failed: {result.stderr}")
                return False
            
        except Exception as e:
            print(f"? Quick test error: {e}")
            return False
    
    def generate_test_commands(self):
        """Generate helpful test commands for the user."""
        print("\n" + "="*80)
        print("?? V3 ENHANCED TEST SUITE SETUP COMPLETED!")
        print("="*80)
        print("\nYou can now run the following commands:")
        print("\n?? BASIC TESTING:")
        print("   python test.py")
        print("   python test.py --headless")
        
        print("\n?? BROWSER TESTING:")
        print("   python test.py --browser chrome")
        print("   python test.py --browser firefox")
        print("   python test.py --dashboard-url http://localhost:8102")
        
        print("\n?? REPORTING:")
        print("   python test.py --json-report")
        print("   python test.py --screenshots")
        
        print("\n?? EXISTING TESTS:")
        print("   python test.py  # Run comprehensive test with browser automation")
        print("   pytest  # Run any pytest-based tests")
        
        print("\n??  SYSTEM SETUP:")
        print("   1. Configure your API keys in .env file")
        print("   2. Start the V3 system: python main.py") 
        print("   3. Run comprehensive tests: python test.py")
        
        print("\n?? FILES CREATED:")
        print(f"   .env (environment configuration)")
        print(f"   logs/ (log directory)")
        print(f"   data/ (data directory)")
        print(f"   test_screenshots/ (browser test screenshots)")
        print(f"   test_results/ (test results)")
        
        print("\n" + "="*80)
    
    def run_full_setup(self, install_browsers: bool = True) -> bool:
        """Run the complete setup process."""
        print("Starting V3 Enhanced Test Suite Setup...")
        print("="*80)
        
        steps = [
            self.check_python_version,
            self.install_packages,
            lambda: self.install_browser_drivers() if install_browsers else True,
            self.validate_trading_system_files,
            self.setup_environment_config,
            self.validate_setup,
            self.run_quick_test
        ]
        
        for i, step in enumerate(steps, 1):
            try:
                if not step():
                    print(f"\n? Setup failed at step {i}")
                    return False
                time.sleep(1)  # Brief pause between steps
            except Exception as e:
                print(f"\n? Setup error at step {i}: {e}")
                return False
        
        self.generate_test_commands()
        return True


def main():
    """Main setup function."""
    import argparse
    
    parser = argparse.ArgumentParser(description="V3 Enhanced Test Suite Setup")
    parser.add_argument("--install-browsers", action="store_true", 
                       help="Install browser drivers (default: True)")
    parser.add_argument("--validate-only", action="store_true",
                       help="Only run validation, skip installation")
    parser.add_argument("--quick-test", action="store_true",
                       help="Run quick test only")
    
    args = parser.parse_args()
    
    setup = EnhancedTestSuiteSetup()
    
    try:
        if args.quick_test:
            success = setup.run_quick_test()
        elif args.validate_only:
            success = setup.validate_setup()
        else:
            success = setup.run_full_setup(install_browsers=not args.install_browsers == False)
        
        if success:
            print("\n?? Setup completed successfully!")
            sys.exit(0)
        else:
            print("\n? Setup failed. Please check the errors above.")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\nSetup interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Setup failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()