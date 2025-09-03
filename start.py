#!/usr/bin/env python3
"""
V3 MAIN TRADING SYSTEM STARTER - COMPLETE UPDATED VERSION
==========================================================
All encoding issues fixed, enhanced functionality, production ready
"""

import subprocess
import time
import os
import signal
import sys
import psutil
import requests
from pathlib import Path
from dotenv import load_dotenv
import logging
import threading
from typing import Optional, Dict

# Load environment variables
load_dotenv()

class V3MainSystemStarter:
    """Complete V3 system starter with all fixes applied"""
    
    def __init__(self):
        self.main_proc = None
        self.running = True
        
        # Enhanced port configuration
        self.port = int(os.getenv('FLASK_PORT', '8102'))
        self.main_system_port = int(os.getenv('MAIN_SYSTEM_PORT', self.port))
        
        # Setup logging
        self._setup_logging()
        
        # Process monitoring
        self.monitoring_thread = None
        self.last_health_check = time.time()
        
        self.logger.info("V3 Main System Starter initialized")
        
    def _setup_logging(self):
        """Setup comprehensive logging"""
        try:
            Path('logs').mkdir(exist_ok=True)
            
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                handlers=[
                    logging.FileHandler('logs/v3_starter.log', encoding='utf-8'),
                    logging.StreamHandler(sys.stdout)
                ]
            )
            
            self.logger = logging.getLogger(__name__)
            
        except Exception as e:
            print(f"Warning: Failed to setup logging: {e}")
            logging.basicConfig(level=logging.INFO)
            self.logger = logging.getLogger(__name__)

    def check_port_with_timeout(self, port: int, timeout: int = 5) -> bool:
        """Check if port is in use"""
        try:
            for conn in psutil.net_connections():
                if hasattr(conn.laddr, 'port') and conn.laddr.port == port and conn.status == 'LISTEN':
                    return True
            return False
        except Exception as e:
            self.logger.warning(f"Port check failed: {e}")
            return False

    def kill_port_enhanced(self, port: int) -> bool:
        """Enhanced port cleanup with multiple methods"""
        self.logger.info(f"Freeing port {port}...")
        
        killed_processes = 0
        
        try:
            # Method 1: Using psutil
            for proc in psutil.process_iter(['pid', 'connections', 'name']):
                try:
                    if proc.info['connections']:
                        for conn in proc.info['connections']:
                            if hasattr(conn, 'laddr') and hasattr(conn.laddr, 'port') and conn.laddr.port == port:
                                proc_name = proc.info.get('name', 'unknown')
                                self.logger.info(f"Terminating process {proc.pid} ({proc_name}) on port {port}")
                                
                                proc.terminate()
                                try:
                                    proc.wait(timeout=3)
                                    self.logger.info(f"Successfully terminated process {proc.pid}")
                                    killed_processes += 1
                                except psutil.TimeoutExpired:
                                    self.logger.warning(f"Force killing process {proc.pid}")
                                    proc.kill()
                                    killed_processes += 1
                                    
                except (psutil.NoSuchProcess, psutil.AccessDenied, AttributeError):
                    continue
                except Exception as e:
                    self.logger.warning(f"Error handling process: {e}")
                    continue
            
            # Method 2: System commands fallback
            if killed_processes == 0 and os.name == 'posix':
                try:
                    # Try lsof method
                    result = subprocess.run(
                        ['lsof', '-ti', f':{port}'], 
                        capture_output=True, text=True, timeout=5
                    )
                    
                    if result.stdout.strip():
                        pids = result.stdout.strip().split('\n')
                        for pid in pids:
                            if pid.strip():
                                try:
                                    subprocess.run(['kill', '-9', pid.strip()], timeout=3)
                                    self.logger.info(f"Force killed PID {pid}")
                                    killed_processes += 1
                                except Exception as e:
                                    self.logger.warning(f"Failed to kill PID {pid}: {e}")
                                    
                except Exception as e:
                    self.logger.warning(f"lsof method failed: {e}")
            
            # Wait and verify
            time.sleep(2)
            if self.check_port_with_timeout(port, 3):
                self.logger.warning(f"Port {port} still in use after cleanup attempts")
                return False
            else:
                self.logger.info(f"Port {port} successfully freed ({killed_processes} processes killed)")
                return True
                
        except Exception as e:
            self.logger.error(f"Error killing port {port}: {e}")
            return False

    def check_environment_enhanced(self) -> bool:
        """Complete environment validation"""
        self.logger.info("Performing comprehensive environment check...")
        
        # Check .env file
        if not Path('.env').exists():
            self.logger.error(".env file not found!")
            print("ERROR: .env file not found!")
            print("Please create a .env file with your configuration")
            return False
        
        # Check critical files
        required_files = {
            'main.py': 'Main system entry point',
            'main_controller.py': 'Main trading controller', 
            'intelligent_trading_engine.py': 'Trading engine',
            'dashboard.html': 'Web dashboard interface'
        }
        
        missing_files = []
        for file_path, description in required_files.items():
            if not Path(file_path).exists():
                missing_files.append(f"{file_path} ({description})")
        
        if missing_files:
            self.logger.error(f"Missing required files: {', '.join(missing_files)}")
            return False
        
        # Check critical environment variables
        critical_vars = [
            'BINANCE_API_KEY_1',
            'BINANCE_API_SECRET_1',
            'TRADE_AMOUNT_USDT',
            'MIN_CONFIDENCE'
        ]
        
        missing_vars = []
        for var in critical_vars:
            value = os.getenv(var, '').strip()
            if not value:
                missing_vars.append(var)
        
        if missing_vars:
            self.logger.error(f"Missing critical environment variables: {', '.join(missing_vars)}")
            print(f"ERROR: Missing required configuration: {', '.join(missing_vars)}")
            return False
        
        # Create required directories
        required_dirs = ['data', 'logs', 'models', 'backup']
        for dir_name in required_dirs:
            try:
                Path(dir_name).mkdir(parents=True, exist_ok=True)
            except Exception as e:
                self.logger.error(f"Failed to create directory {dir_name}: {e}")
                return False
        
        # Display configuration summary
        starting_balance = os.getenv('STARTING_BALANCE', '50.0')
        trade_amount = os.getenv('TRADE_AMOUNT_USDT', '25.0') 
        testnet_mode = os.getenv('TESTNET', 'true')
        min_confidence = os.getenv('MIN_CONFIDENCE', '70.0')
        
        self.logger.info("Environment check passed")
        self.logger.info(f"Configuration: Port={self.port}, Balance=${starting_balance}, "
                        f"TradeAmount=${trade_amount}, MinConfidence={min_confidence}%, "
                        f"Testnet={testnet_mode}")
        
        print("Environment check passed")
        print(f"  - System Port: {self.port}")
        print(f"  - Trade Amount: ${trade_amount}")
        print(f"  - Min Confidence: {min_confidence}%")
        print(f"  - Testnet Mode: {testnet_mode}")
        
        return True

    def free_ports(self) -> bool:
        """Free all required ports"""
        ports_to_free = [self.port, self.main_system_port]
        ports_to_free = list(dict.fromkeys(ports_to_free))
        
        all_freed = True
        
        for port in ports_to_free:
            if self.check_port_with_timeout(port):
                if not self.kill_port_enhanced(port):
                    self.logger.warning(f"Could not free port {port}")
                    all_freed = False
            else:
                self.logger.info(f"Port {port} is already free")
        
        return all_freed

    def setup_auto_start_environment(self, auto_start: bool = False):
        """Setup environment for auto-start"""
        if auto_start:
            os.environ['AUTO_START_TRADING'] = 'true'
            self.logger.info("AUTO_START_TRADING enabled")
        else:
            os.environ['AUTO_START_TRADING'] = 'false' 
            self.logger.info("Manual start mode enabled")

    def start_main_system_enhanced(self, auto_start: bool = False) -> bool:
        """Start the main trading system"""
        self.logger.info(f"Starting V3 Main Trading System on port {self.port}...")
        
        try:
            # Set up environment
            self.setup_auto_start_environment(auto_start)
            
            # Prepare environment
            env = os.environ.copy()
            env['PYTHONPATH'] = str(Path.cwd())
            env['FLASK_PORT'] = str(self.port)
            
            # Start main system
            self.main_proc = subprocess.Popen(
                [sys.executable, 'main.py'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                env=env,
                cwd=Path.cwd()
            )
            
            # Wait for startup
            startup_timeout = 15
            for attempt in range(startup_timeout):
                if self.main_proc.poll() is not None:
                    stdout, stderr = self.main_proc.communicate()
                    self.logger.error(f"Main system failed to start:")
                    self.logger.error(f"STDOUT: {stdout}")
                    self.logger.error(f"STDERR: {stderr}")
                    return False
                
                # Check if Flask is responding
                try:
                    response = requests.get(f'http://localhost:{self.port}/api/status', timeout=2)
                    if response.status_code == 200:
                        self.logger.info("Main system started successfully")
                        return True
                except requests.exceptions.RequestException:
                    pass
                
                time.sleep(1)
            
            self.logger.warning("Main system started but may not be fully ready")
            return True
            
        except Exception as e:
            self.logger.error(f"Error starting main system: {e}")
            return False

    def check_system_health(self) -> Dict[str, bool]:
        """Check system health status"""
        health = {
            'process_running': False,
            'flask_responding': False,
            'api_accessible': False
        }
        
        try:
            # Check if process is running
            if self.main_proc and self.main_proc.poll() is None:
                health['process_running'] = True
            
            # Check if Flask is responding
            try:
                response = requests.get(f'http://localhost:{self.port}/api/status', timeout=3)
                if response.status_code == 200:
                    health['flask_responding'] = True
                    
                    # Check API accessibility
                    try:
                        data = response.json()
                        if isinstance(data, dict):
                            health['api_accessible'] = True
                    except:
                        pass
                        
            except requests.exceptions.RequestException as e:
                self.logger.debug(f"Health check request failed: {e}")
            
        except Exception as e:
            self.logger.warning(f"Health check error: {e}")
        
        return health

    def monitor_system_health(self):
        """Background health monitoring"""
        while self.running:
            try:
                current_time = time.time()
                
                if current_time - self.last_health_check > 30:
                    health = self.check_system_health()
                    
                    if not health['process_running']:
                        self.logger.error("Main process has stopped!")
                        self.running = False
                        break
                    elif not health['flask_responding']:
                        self.logger.warning("Flask server not responding")
                    
                    self.last_health_check = current_time
                
                time.sleep(5)
                
            except Exception as e:
                self.logger.error(f"Health monitoring error: {e}")
                time.sleep(10)

    def display_system_status(self) -> bool:
        """Display system status"""
        self.logger.info("Checking system status...")
        
        max_wait = 30
        
        for attempt in range(max_wait):
            health = self.check_system_health()
            
            print(f"\nSystem Status Check (attempt {attempt + 1}/{max_wait}):")
            print("=" * 50)
            print(f"Main Process: {'Running' if health['process_running'] else 'Stopped'}")
            print(f"Flask Server: {'Responding' if health['flask_responding'] else 'Not Responding'}")
            print(f"API Access: {'Accessible' if health['api_accessible'] else 'Inaccessible'}")
            
            if all(health.values()):
                print("\nAll systems operational!")
                print(f"Dashboard: http://localhost:{self.port}")
                print(f"External: http://185.202.239.125:{self.port}")
                return True
            
            if not health['process_running']:
                print("\nMain process has stopped")
                return False
            
            print(f"Waiting for system to initialize... ({attempt + 1}/{max_wait})")
            time.sleep(1)
        
        print("\nSystem partially operational")
        print("Process is running but some components may not be ready")
        return health['process_running']

    def cleanup_enhanced(self, signum=None, frame=None):
        """Enhanced cleanup process"""
        if signum:
            signal_names = {signal.SIGINT: "SIGINT (Ctrl+C)", signal.SIGTERM: "SIGTERM"}
            signal_name = signal_names.get(signum, f"Signal {signum}")
            self.logger.info(f"Received {signal_name} - initiating shutdown")
        else:
            self.logger.info("Initiating shutdown")
        
        self.running = False
        
        # Stop monitoring thread
        if self.monitoring_thread and self.monitoring_thread.is_alive():
            self.logger.info("Stopping health monitoring...")
            self.monitoring_thread.join(timeout=2)
        
        # Stop main process
        if self.main_proc and self.main_proc.poll() is None:
            self.logger.info("Stopping main system...")
            
            try:
                self.main_proc.terminate()
                
                try:
                    self.main_proc.wait(timeout=10)
                    self.logger.info("Main system stopped gracefully")
                except subprocess.TimeoutExpired:
                    self.logger.warning("Force killing main system")
                    self.main_proc.kill()
                    try:
                        self.main_proc.wait(timeout=5)
                        self.logger.info("Main system force stopped")
                    except subprocess.TimeoutExpired:
                        self.logger.error("Failed to stop main system")
                        
            except Exception as e:
                self.logger.error(f"Error stopping main system: {e}")
        
        # Final port cleanup
        try:
            self.kill_port_enhanced(self.port)
        except Exception as e:
            self.logger.warning(f"Final port cleanup failed: {e}")
        
        self.logger.info("Shutdown completed")

    def prompt_for_startup_mode(self) -> str:
        """Startup mode selection"""
        print("\n" + "=" * 60)
        print("V3 TRADING SYSTEM - STARTUP MODE SELECTION")
        print("=" * 60)
        print("1. Auto-start trading (system starts trading automatically)")
        print("2. Manual start (use dashboard to start trading)")
        print("3. Monitor only (no trading, just monitoring)")
        print("4. Backtest mode (run comprehensive backtesting)")
        print("=" * 60)
        
        while True:
            try:
                choice = input("\nChoose startup mode (1/2/3/4) [default: 2]: ").strip()
                
                if choice == '1':
                    print("Auto-start trading mode selected")
                    return 'auto'
                elif choice == '3':
                    print("Monitor-only mode selected")
                    return 'monitor'
                elif choice == '4':
                    print("Backtest mode selected")
                    return 'backtest'
                elif choice == '2' or choice == '':
                    print("Manual start mode selected")
                    return 'manual'
                else:
                    print("Invalid choice. Please enter 1, 2, 3, or 4.")
                    
            except (KeyboardInterrupt, EOFError):
                print("\nDefaulting to manual start mode")
                return 'manual'

    def run_enhanced(self) -> int:
        """Main run method"""
        print("=" * 70)
        print("V3 ENHANCED TRADING SYSTEM STARTUP")
        print("=" * 70)
        print(f"System Port: {self.port}")
        print("Enhanced: Cross-communication, Real data only, Better error handling")
        print("=" * 70)
        
        # Set up signal handlers
        signal.signal(signal.SIGINT, self.cleanup_enhanced)
        if hasattr(signal, 'SIGTERM'):
            signal.signal(signal.SIGTERM, self.cleanup_enhanced)
        
        try:
            # Step 1: Environment validation
            if not self.check_environment_enhanced():
                self.logger.error("Environment check failed")
                return 1
            
            # Step 2: Port management
            if not self.free_ports():
                self.logger.warning("Port cleanup had issues, but continuing...")
            
            # Step 3: Startup mode selection
            startup_mode = self.prompt_for_startup_mode()
            
            # Configure environment based on mode
            if startup_mode == 'auto':
                auto_start = True
            elif startup_mode == 'monitor':
                os.environ['MONITOR_ONLY'] = 'true'
                auto_start = False
            elif startup_mode == 'backtest':
                os.environ['RUN_COMPREHENSIVE_BACKTEST'] = 'true'
                auto_start = False
            else:
                auto_start = False
            
            # Step 4: Start main system
            if not self.start_main_system_enhanced(auto_start):
                self.logger.error("Failed to start main system")
                return 1
            
            # Step 5: Health monitoring
            self.monitoring_thread = threading.Thread(target=self.monitor_system_health, daemon=True)
            self.monitoring_thread.start()
            
            # Step 6: Status verification
            time.sleep(3)
            
            if not self.display_system_status():
                self.logger.warning("System status check had issues")
            
            # Step 7: Display final status
            self._display_final_status(startup_mode)
            
            # Step 8: Main monitoring loop
            self.logger.info("System operational - entering monitoring mode")
            
            while self.running:
                try:
                    time.sleep(2)
                    
                    if self.main_proc and self.main_proc.poll() is not None:
                        stdout, stderr = self.main_proc.communicate()
                        self.logger.error("Main system stopped unexpectedly")
                        if stderr:
                            self.logger.error(f"Error output: {stderr}")
                        break
                        
                except KeyboardInterrupt:
                    self.logger.info("Received keyboard interrupt")
                    break
                except Exception as e:
                    self.logger.error(f"Monitoring loop error: {e}")
                    time.sleep(5)
            
            return 0
            
        except Exception as e:
            self.logger.error(f"Startup failed: {e}")
            return 1
        finally:
            self.cleanup_enhanced()
    
    def _display_final_status(self, startup_mode: str):
        """Display final status"""
        print("\n" + "=" * 70)
        
        if startup_mode == 'auto':
            print("V3 SYSTEM STARTED WITH AUTO-TRADING")
            print("Trading will begin automatically after initialization")
        elif startup_mode == 'monitor':
            print("V3 SYSTEM STARTED IN MONITOR-ONLY MODE") 
            print("No trades will be executed")
        elif startup_mode == 'backtest':
            print("V3 SYSTEM STARTED IN BACKTEST MODE")
            print("Comprehensive backtesting will run automatically")
        else:
            print("V3 SYSTEM STARTED IN MANUAL MODE")
            print("Use the dashboard to start trading when ready")
        
        print(f"Dashboard: http://localhost:{self.port}")
        print(f"External: http://185.202.239.125:{self.port}")
        print("Press Ctrl+C to stop the system")
        print("=" * 70)

def main():
    """Main entry point"""
    
    # Handle command line arguments
    if len(sys.argv) > 1:
        arg = sys.argv[1].lower()
        if arg in ['--auto', '-a']:
            os.environ['AUTO_START_TRADING'] = 'true'
            print("Command line auto-start mode enabled")
        elif arg in ['--monitor', '-m']:
            os.environ['MONITOR_ONLY'] = 'true'
            print("Command line monitor-only mode enabled")
        elif arg in ['--backtest', '-b']:
            os.environ['RUN_COMPREHENSIVE_BACKTEST'] = 'true'
            print("Command line backtest mode enabled")
        elif arg in ['--help', '-h']:
            print("V3 Enhanced Trading System")
            print("Usage:")
            print("  python start.py           # Interactive mode")
            print("  python start.py --auto    # Auto-start trading")
            print("  python start.py --monitor # Monitor only (no trading)")
            print("  python start.py --backtest# Run comprehensive backtest")
            print("")
            print("Configuration:")
            print("  Port: Set in .env FLASK_PORT (default: 8102)")
            print("  Trading: Configure in .env (TRADE_AMOUNT_USDT, MIN_CONFIDENCE, etc.)")
            return 0
    
    # Create and run the starter
    starter = V3MainSystemStarter()
    exit_code = starter.run_enhanced()
    sys.exit(exit_code)

if __name__ == "__main__":
    main()