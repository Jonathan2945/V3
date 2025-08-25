#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#!/usr/bin/env python3
"""
Advanced ML Trading Bot - Environment Setup Script
Automatically sets up the complete trading bot environment
"""

import os
import sys
sys.setrecursionlimit(2000)  # Prevent recursion errors
import subprocess
import shutil
from pathlib import Path
import logging
import platform

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EnvironmentSetup:
    """Complete environment setup for the trading bot"""
    
    def __init__(self):
        self.project_root = Path.cwd()
        self.required_dirs = [
            'core', 'data_collection', 'ml_system', 'trading',
            'monitoring', 'utils', 'tests', 'logs', 'data',
            'models', 'backtest_results', 'configs'
        ]
        self.system = platform.system().lower()
        self.test_mode = os.getenv('TESTING_MODE', 'false').lower() == 'true'
    
    def create_directory_structure(self):
        """Create all required directories"""
        logger.info("Creating directory structure...")
        
        try:
            for dir_name in self.required_dirs:
                dir_path = self.project_root / dir_name
                dir_path.mkdir(exist_ok=True)
                
                # Create __init__.py files for Python packages
                if dir_name not in ['logs', 'data', 'models', 'backtest_results', 'configs']:
                    init_file = dir_path / '__init__.py'
                    if not init_file.exists():
                        init_file.touch()
            
            logger.info("Directory structure created")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create directory structure: {e}")
            return False
    
    def install_system_dependencies(self):
        """Install system-level dependencies"""
        if self.test_mode:
            logger.info("Skipping system dependencies installation (test mode)")
            return True
            
        logger.info("Installing system dependencies...")
        
        system_deps = {
            'ubuntu': [
                'sudo apt-get update',
                'sudo apt-get install -y build-essential',
                'sudo apt-get install -y python3-dev',
                'sudo apt-get install -y libffi-dev',
                'sudo apt-get install -y libssl-dev',
                'sudo apt-get install -y libxml2-dev',
                'sudo apt-get install -y libxslt1-dev',
                'sudo apt-get install -y libjpeg-dev',
                'sudo apt-get install -y zlib1g-dev',
                'sudo apt-get install -y pkg-config',
                'sudo apt-get install -y libhdf5-dev'
            ],
            'darwin': [  # macOS
                'brew install python3',
                'brew install pkg-config',
                'brew install hdf5',
                'xcode-select --install'
            ],
            'windows': [
                'echo "Please install Visual Studio Build Tools manually"',
                'echo "Download from: https://visualstudio.microsoft.com/downloads/"'
            ]
        }
        
        success_count = 0
        total_commands = 0
        
        if self.system in system_deps:
            commands = system_deps[self.system]
            total_commands = len(commands)
            
            for cmd in commands:
                try:
                    logger.info(f"Running: {cmd}")
                    result = subprocess.run(cmd, shell=True, check=False, 
                                          capture_output=True, text=True, timeout=60)
                    
                    if result.returncode == 0:
                        success_count += 1
                    else:
                        logger.warning(f"Command failed: {cmd}")
                        
                except subprocess.TimeoutExpired:
                    logger.warning(f"Command timed out: {cmd}")
                except Exception as e:
                    logger.warning(f"Failed to run: {cmd}, Error: {e}")
        
        if total_commands > 0:
            success_rate = success_count / total_commands
            if success_rate > 0.5:
                logger.info(f"System dependencies installation attempted ({success_count}/{total_commands} successful)")
                return True
            else:
                logger.warning(f"Many system dependency installations failed ({success_count}/{total_commands} successful)")
                return False
        else:
            logger.info("No system dependencies defined for this platform")
            return True
    
    def setup_python_environment(self):
        """Setup Python virtual environment and install packages"""
        if self.test_mode:
            logger.info("Skipping Python environment setup (test mode)")
            return True
            
        logger.info("Setting up Python environment...")
        
        try:
            # Create virtual environment
            venv_path = self.project_root / 'venv'
            if not venv_path.exists():
                result = subprocess.run([sys.executable, '-m', 'venv', 'venv'], 
                                      check=False, capture_output=True, text=True, timeout=120)
                if result.returncode == 0:
                    logger.info("Virtual environment created")
                else:
                    logger.warning("Failed to create virtual environment")
                    return False
            
            # Determine pip and python paths
            if self.system == 'windows':
                pip_path = venv_path / 'Scripts' / 'pip'
                python_path = venv_path / 'Scripts' / 'python'
            else:
                pip_path = venv_path / 'bin' / 'pip'
                python_path = venv_path / 'bin' / 'python'
            
            # Upgrade pip
            try:
                subprocess.run([str(python_path), '-m', 'pip', 'install', '--upgrade', 'pip'], 
                             check=False, timeout=60, capture_output=True)
            except Exception as e:
                logger.warning(f"Failed to upgrade pip: {e}")
            
            # Install requirements if file exists
            requirements_file = self.project_root / 'requirements.txt'
            if requirements_file.exists():
                try:
                    result = subprocess.run([str(pip_path), 'install', '-r', 'requirements.txt'], 
                                          check=False, timeout=300, capture_output=True, text=True)
                    if result.returncode == 0:
                        logger.info("Python packages installed")
                        return True
                    else:
                        logger.warning("Some packages failed to install")
                        return False
                except Exception as e:
                    logger.warning(f"Failed to install requirements: {e}")
                    return False
            else:
                logger.warning("requirements.txt not found. Please create it first.")
                return False
                
        except Exception as e:
            logger.error(f"Python environment setup failed: {e}")
            return False
    
    def create_config_files(self):
        """Create configuration files"""
        logger.info("Creating configuration files...")
        
        try:
            configs_dir = self.project_root / 'configs'
            configs_dir.mkdir(exist_ok=True)
            
            # Main configuration
            main_config = configs_dir / 'config.yaml'
            if not main_config.exists():
                main_config.write_text("""
# Advanced ML Trading Bot Configuration
trading:
  exchange: 'binance'
  testnet: true
  symbols: ['BTCUSDT', 'ETHUSDT', 'ADAUSDT']
  base_currency: 'USDT'
  position_size: 100.0
  max_positions: 3
  
risk_management:
  max_daily_loss: 5.0  # Percentage
  stop_loss: 2.0       # Percentage
  take_profit: 4.0     # Percentage
  max_position_size: 1000.0  # USDT
  
ml_models:
  retrain_hours: 24
  min_accuracy: 0.6
  ensemble_threshold: 0.7
  lookback_periods: [5, 15, 30, 60]
  
data_collection:
  timeframes: ['1m', '5m', '15m', '1h', '4h']
  technical_indicators: true
  news_sentiment: true
  social_sentiment: true
  
monitoring:
  dashboard_port: 8080
  log_level: 'INFO'
  health_check_interval: 60
  
database:
  type: 'sqlite'
  path: 'data/trading_bot.db'
""")
            
            # Environment template
            env_template = self.project_root / '.env.template'
            if not env_template.exists():
                env_template.write_text("""
# API Keys (Fill these in and save as .env)
BINANCE_API_KEY=your_binance_api_key_here
BINANCE_API_SECRET=your_binance_secret_here
BINANCE_TESTNET=true

# News APIs
NEWS_API_KEY=your_news_api_key_here
ALPHA_VANTAGE_KEY=your_alpha_vantage_key_here

# Social Media APIs
TWITTER_API_KEY=your_twitter_api_key_here
TWITTER_API_SECRET=your_twitter_secret_here
TWITTER_ACCESS_TOKEN=your_twitter_access_token_here
TWITTER_ACCESS_SECRET=your_twitter_access_secret_here

# Database
DATABASE_URL=sqlite:///data/trading_bot.db

# Monitoring
TELEGRAM_BOT_TOKEN=your_telegram_bot_token_here
TELEGRAM_CHAT_ID=your_telegram_chat_id_here

# ML Models
HUGGINGFACE_API_KEY=your_huggingface_key_here

# Logging
LOG_LEVEL=INFO
""")
            
            logger.info("Configuration files created")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create configuration files: {e}")
            return False
    
    def create_startup_scripts(self):
        """Create startup scripts for different platforms"""
        logger.info("Creating startup scripts...")
        
        try:
            # Linux/Mac startup script
            if self.system != 'windows':
                start_script = self.project_root / 'start_bot.sh'
                start_script.write_text("""#!/bin/bash
# Advanced ML Trading Bot Startup Script

echo "Starting Advanced ML Trading Bot..."

# Activate virtual environment
source venv/bin/activate

# Check if .env exists
if [ ! -f .env ]; then
    echo ".env file not found. Please copy .env.template to .env and configure your API keys."
    exit 1
fi

# Start the main bot
echo "Launching trading bot..."
python main.py

# Keep script running
echo "Bot started successfully!"
""")
                try:
                    start_script.chmod(0o755)
                except Exception:
                    pass  # Ignore permission errors
            
            # Windows startup script  
            if self.system == 'windows':
                start_script = self.project_root / 'start_bot.bat'
                start_script.write_text("""@echo off
REM Advanced ML Trading Bot Startup Script

echo Starting Advanced ML Trading Bot...

REM Activate virtual environment
call venv\\Scripts\\activate.bat

REM Check if .env exists
if not exist .env (
    echo .env file not found. Please copy .env.template to .env and configure your API keys.
    pause
    exit /b 1
)

REM Start the main bot
echo Launching trading bot...
python main.py

echo Bot started successfully!
pause
""")
            
            # Python startup script
            python_starter = self.project_root / 'start_trading_bot.py'
            python_starter.write_text("""#!/usr/bin/env python3
\"\"\"
Advanced ML Trading Bot - Python Startup Script
Cross-platform startup script for the trading bot
\"\"\"

import os
import sys
import subprocess
from pathlib import Path

def main():
    \"\"\"Main startup function\"\"\"
    print("Starting Advanced ML Trading Bot...")
    
    # Check if we're in the right directory
    if not Path('main.py').exists():
        print("main.py not found. Please run this from the bot directory.")
        sys.exit(1)
    
    # Check if .env exists
    if not Path('.env').exists():
        print(".env file not found.")
        print("Please copy .env.template to .env and configure your API keys.")
        sys.exit(1)
    
    # Check if virtual environment exists
    venv_python = None
    if Path('venv/bin/python').exists():  # Linux/Mac
        venv_python = 'venv/bin/python'
    elif Path('venv/Scripts/python.exe').exists():  # Windows
        venv_python = 'venv/Scripts/python.exe'
    
    if venv_python:
        print("Using virtual environment...")
        subprocess.run([venv_python, 'main.py'])
    else:
        print("Using system Python...")
        subprocess.run([sys.executable, 'main.py'])

if __name__ == "__main__":
    main()
""")
            
            logger.info("Startup scripts created")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create startup scripts: {e}")
            return False
    
    def create_test_script(self):
        """Create a comprehensive test script"""
        logger.info("Creating test script...")
        
        try:
            test_script = self.project_root / 'test_setup.py'
            test_script.write_text("""#!/usr/bin/env python3
\"\"\"
Environment Setup Test Script
Tests all components of the trading bot setup
\"\"\"

import sys
import importlib
from pathlib import Path

def test_imports():
    \"\"\"Test all required Python imports\"\"\"
    print("Testing Python imports...")
    
    required_packages = [
        'pandas', 'numpy', 'scikit-learn', 'tensorflow',
        'ccxt', 'ta', 'requests', 'aiohttp', 'websockets',
        'flask', 'plotly', 'sqlite3', 'sqlalchemy'
    ]
    
    passed = 0
    failed = []
    
    for package in required_packages:
        try:
            importlib.import_module(package)
            print(f"  OK: {package}")
            passed += 1
        except ImportError:
            print(f"  FAIL: {package}")
            failed.append(package)
    
    print(f"\\nImport Results: {passed}/{len(required_packages)} passed")
    if failed:
        print(f"Failed imports: {', '.join(failed)}")
        return False
    return True

def test_directories():
    \"\"\"Test directory structure\"\"\"
    print("\\nTesting directory structure...")
    
    required_dirs = [
        'core', 'data_collection', 'ml_system', 'trading',
        'monitoring', 'utils', 'tests', 'logs', 'data',
        'models', 'backtest_results', 'configs'
    ]
    
    passed = 0
    for dir_name in required_dirs:
        if Path(dir_name).exists():
            print(f"  OK: {dir_name}/")
            passed += 1
        else:
            print(f"  MISSING: {dir_name}/")
    
    print(f"\\nDirectory Results: {passed}/{len(required_dirs)} exist")
    return passed == len(required_dirs)

def test_config_files():
    \"\"\"Test configuration files\"\"\"
    print("\\nTesting configuration files...")
    
    config_files = [
        'configs/config.yaml',
        '.env.template',
        'requirements.txt'
    ]
    
    passed = 0
    for file_name in config_files:
        if Path(file_name).exists():
            print(f"  OK: {file_name}")
            passed += 1
        else:
            print(f"  MISSING: {file_name}")
    
    # Check if .env exists (optional but recommended)
    if Path('.env').exists():
        print(f"  OK: .env (configured)")
    else:
        print(f"  WARNING: .env (not configured - copy from .env.template)")
    
    print(f"\\nConfig Results: {passed}/{len(config_files)} exist")
    return passed >= 2  # At least config.yaml and requirements.txt

def main():
    \"\"\"Run all tests\"\"\"
    print("Advanced ML Trading Bot - Setup Test\\n")
    print("=" * 50)
    
    tests = [
        test_directories,
        test_config_files,
        test_imports
    ]
    
    results = []
    for test_func in tests:
        try:
            result = test_func()
            results.append(result)
        except Exception as e:
            print(f"Test failed with error: {e}")
            results.append(False)
    
    print("\\n" + "=" * 50)
    print("FINAL RESULTS")
    print("=" * 50)
    
    passed_tests = sum(results)
    total_tests = len(tests)
    
    if passed_tests == total_tests:
        print("ALL TESTS PASSED! Your environment is ready!")
        print("\\nNext steps:")
        print("1. Copy .env.template to .env")
        print("2. Configure your API keys in .env")
        print("3. Run: python main.py")
    else:
        print(f"WARNING: {passed_tests}/{total_tests} tests passed")
        print("\\nPlease fix the failed tests before running the bot.")
    
    return passed_tests == total_tests

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
""")
            
            logger.info("Test script created")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create test script: {e}")
            return False
    
    def run_setup(self):
        """Run the complete setup process"""
        logger.info("Starting Advanced ML Trading Bot Environment Setup")
        logger.info("=" * 60)
        
        results = []
        
        try:
            # Step 1: Create directories
            results.append(self.create_directory_structure())
            
            # Step 2: Install system dependencies
            if '--skip-system' not in sys.argv:
                results.append(self.install_system_dependencies())
            else:
                logger.info("Skipping system dependencies (--skip-system flag)")
                results.append(True)
            
            # Step 3: Setup Python environment
            if '--skip-python' not in sys.argv:
                results.append(self.setup_python_environment())
            else:
                logger.info("Skipping Python setup (--skip-python flag)")
                results.append(True)
            
            # Step 4: Create config files
            results.append(self.create_config_files())
            
            # Step 5: Create startup scripts
            results.append(self.create_startup_scripts())
            
            # Step 6: Create test script
            results.append(self.create_test_script())
            
            # Calculate success rate
            success_count = sum(results)
            total_steps = len(results)
            success_rate = success_count / total_steps
            
            logger.info("\\n" + "=" * 60)
            
            if success_rate >= 0.8:  # 80% success rate
                logger.info("SETUP COMPLETE!")
                logger.info("=" * 60)
                logger.info("\\nNext steps:")
                logger.info("1. Copy .env.template to .env and configure your API keys")
                logger.info("2. Run: python test_setup.py (to verify installation)")
                logger.info("3. Run: python start_trading_bot.py (to start the bot)")
                logger.info("\\nCheck README.md for detailed documentation")
                return True
            else:
                logger.warning(f"SETUP PARTIALLY COMPLETE ({success_count}/{total_steps} steps successful)")
                logger.warning("Some components may not work correctly.")
                logger.warning("Please review the errors above and try again.")
                return False
            
        except Exception as e:
            logger.error(f"Setup failed: {e}")
            logger.error("Please check the logs and try again")
            return False

def main():
    """Main entry point"""
    setup = EnvironmentSetup()
    success = setup.run_setup()
    return success

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\\nSetup interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\\nUnexpected error: {e}")
        logging.error(f"Critical error in quick_launcher: {e}")
        sys.exit(1)