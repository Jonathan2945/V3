#!/usr/bin/env python3
"""
MAIN TRADING SYSTEM STARTER - FIXED VERSION
============================================
Starts only the main trading system (reads port from .env FLASK_PORT)
No more dual system confusion - single unified system
"""

import subprocess
import time
import os
import signal
import sys
import psutil
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class MainSystemStarter:
    def __init__(self):
        self.main_proc = None
        self.running = True
        # FIXED: Get port from .env consistently
        self.port = int(os.getenv('FLASK_PORT', 8107))
        
    def check_port(self, port):
        """Check if port is in use"""
        try:
            for conn in psutil.net_connections():
                if conn.laddr.port == port and conn.status == 'LISTEN':
                    return True
            return False
        except:
            return False

    def kill_port(self, port):
        """Kill process on port with better timeout handling"""
        print(f"Killing process on port {port}...")
        try:
            for proc in psutil.process_iter(['pid', 'connections']):
                try:
                    if proc.info['connections']:
                        for conn in proc.info['connections']:
                            if hasattr(conn, 'laddr') and conn.laddr.port == port:
                                print(f"Terminating process {proc.pid} on port {port}")
                                proc.terminate()
                                try:
                                    proc.wait(timeout=5)
                                    print(f"Successfully terminated process {proc.pid}")
                                    return True
                                except psutil.TimeoutExpired:
                                    print(f"Timeout - force killing process {proc.pid}")
                                    proc.kill()
                                    return True
                except (psutil.NoSuchProcess, psutil.AccessDenied, AttributeError):
                    continue
            
            # Fallback method using lsof
            try:
                result = subprocess.run(['lsof', '-ti', f':{port}'], 
                                      capture_output=True, text=True, timeout=10)
                if result.stdout.strip():
                    pids = result.stdout.strip().split('\n')
                    for pid in pids:
                        if pid.strip():
                            subprocess.run(['kill', '-9', pid.strip()], 
                                         capture_output=True, timeout=5)
                            print(f"Killed PID {pid}")
                    return True
            except Exception as e:
                print(f"lsof method failed: {e}")
            
            return False
        except Exception as e:
            print(f"Error killing port {port}: {e}")
            return False

    def check_environment(self):
        """Check if environment is ready"""
        print("Checking environment...")
        
        # Check .env file
        if not Path('.env').exists():
            print("ERROR: .env file not found!")
            print("Please create a .env file with your configuration")
            return False
        
        # Check required files for main system only
        required_files = ['main.py', 'main_controller.py', 'intelligent_trading_engine.py']
        missing_files = []
        
        for file in required_files:
            if not Path(file).exists():
                missing_files.append(file)
        
        if missing_files:
            print(f"ERROR: Missing required files: {', '.join(missing_files)}")
            return False
        
        # Display .env configuration
        starting_balance = os.getenv('STARTING_BALANCE', '50.0')
        trade_amount = os.getenv('TRADE_AMOUNT_USDT', '25.0')
        testnet_mode = os.getenv('TESTNET', 'true')
        
        print("Environment check passed")
        print(f"Configuration from .env:")
        print(f"  - Port: {self.port}")
        print(f"  - Starting Balance: ${starting_balance}")
        print(f"  - Trade Amount: ${trade_amount}")
        print(f"  - Testnet Mode: {testnet_mode}")
        
        return True

    def free_port(self):
        """Free the main system port only"""
        print(f"Checking and freeing port {self.port}...")
        
        if self.check_port(self.port):
            print(f"Port {self.port} is in use - freeing it...")
            self.kill_port(self.port)
            time.sleep(2)
            
            if self.check_port(self.port):
                print(f"Warning: Port {self.port} still in use")
                # Try one more time with force
                try:
                    subprocess.run(['fuser', '-k', f'{self.port}/tcp'], 
                                 capture_output=True, timeout=10)
                    time.sleep(2)
                except:
                    pass
                
                if self.check_port(self.port):
                    print(f"ERROR: Could not free port {self.port}")
                    return False
            
            print(f"Port {self.port} successfully freed")
        else:
            print(f"Port {self.port} is free")
        
        return True

    def setup_auto_start_environment(self, auto_start=False):
        """Set up environment for auto-start if requested"""
        if auto_start:
            os.environ['AUTO_START_TRADING'] = 'true'
            print("AUTO_START_TRADING enabled - system will start trading automatically")
        else:
            os.environ['AUTO_START_TRADING'] = 'false'
            print("Manual start mode - use dashboard to start trading")

    def start_main_system(self, auto_start=False):
        """Start main trading system"""
        print(f"Starting Main Trading System (Port {self.port})...")
        
        try:
            # Set up environment for auto-start
            self.setup_auto_start_environment(auto_start)
            
            # Start main system
            env = os.environ.copy()
            self.main_proc = subprocess.Popen(
                [sys.executable, 'main.py'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                env=env
            )
            
            # Wait and check if it's running
            time.sleep(3)
            if self.main_proc.poll() is None:
                print("Main system started successfully")
                if auto_start:
                    print("System will automatically start trading after initialization")
                return True
            else:
                stdout, stderr = self.main_proc.communicate()
                print(f"Main system failed to start:")
                print(f"STDOUT: {stdout}")
                print(f"STDERR: {stderr}")
                return False
                
        except Exception as e:
            print(f"Error starting main system: {e}")
            return False

    def check_system_status(self):
        """Check if system is running properly with improved timing"""
        print("\nWaiting for system to fully initialize...")
        
        # Wait longer for Flask to start
        max_wait = 30  # 30 seconds max wait
        wait_interval = 2  # Check every 2 seconds
        
        for attempt in range(max_wait // wait_interval):
            print(f"Checking system status (attempt {attempt + 1})...")
            
            # Check main system
            main_running = self.main_proc and self.main_proc.poll() is None
            
            # Improved port check with HTTP request
            main_port = False
            try:
                import requests
                response = requests.get(f'http://localhost:{self.port}/api/status', timeout=5)
                main_port = response.status_code == 200
                print(f"HTTP check successful: {response.status_code}")
            except:
                # Fallback to port check
                main_port = self.check_port(self.port)
            
            print("SYSTEM STATUS")
            print("=" * 40)
            print(f"Main System Process: {'Running' if main_running else 'Stopped'}")
            print(f"Main System Port {self.port}: {'Active' if main_port else 'Inactive'}")
            print("")
            
            if main_running and main_port:
                print(f"✓ Trading System Dashboard: http://localhost:{self.port}")
                print(f"✓ External Access: http://185.202.239.125:{self.port}")
                return True
            
            if not main_running:
                print("✗ Main process stopped - system failed")
                return False
                
            print(f"Waiting for Flask server to bind to port {self.port}...")
            time.sleep(wait_interval)
        
        # If we get here, Flask didn't start properly
        print("WARNING: Flask server didn't respond to HTTP requests")
        print("But process is running - check for errors in logs")
        return main_running

    def cleanup(self, signum=None, frame=None):
        """Cleanup processes on exit"""
        print("\nShutting down system...")
        self.running = False
        
        if self.main_proc and self.main_proc.poll() is None:
            print("Stopping main system...")
            self.main_proc.terminate()
            try:
                self.main_proc.wait(timeout=10)
                print("Main system stopped gracefully")
            except subprocess.TimeoutExpired:
                print("Force killing main system...")
                self.main_proc.kill()
                try:
                    self.main_proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    print("Warning: Main system may still be running")
        
        print("System stopped")

    def prompt_for_startup_mode(self):
        """Ask user about startup preferences"""
        print("\nStartup Options:")
        print("1. Auto-start trading (system starts trading automatically)")
        print("2. Manual start (use dashboard to start trading)")
        print("3. Monitor only (no trading, just monitoring)")
        
        while True:
            try:
                choice = input("\nChoose startup mode (1/2/3) [default: 2]: ").strip()
                
                if choice == '1':
                    return 'auto'
                elif choice == '3':
                    return 'monitor'
                elif choice == '2' or choice == '':
                    return 'manual'
                else:
                    print("Invalid choice. Please enter 1, 2, or 3.")
                    
            except (KeyboardInterrupt, EOFError):
                print("\nDefaulting to manual start mode")
                return 'manual'

    def run(self):
        """Main run method"""
        print("MAIN TRADING SYSTEM STARTUP")
        print("=" * 30)
        print(f"System Port: {self.port} (from .env FLASK_PORT)")
        print("Unified system - no separate dashboard process")
        print("")
        
        # Set up signal handlers
        signal.signal(signal.SIGINT, self.cleanup)
        signal.signal(signal.SIGTERM, self.cleanup)
        
        try:
            # Step 1: Check environment
            if not self.check_environment():
                return 1
            
            # Step 2: Free port
            if not self.free_port():
                print("Failed to free port, but continuing anyway...")
            
            # Step 3: Ask for startup mode
            startup_mode = self.prompt_for_startup_mode()
            auto_start = (startup_mode == 'auto')
            
            if startup_mode == 'monitor':
                print("\nMonitor-only mode selected")
                print("System will start but not execute any trades")
                os.environ['MONITOR_ONLY'] = 'true'
            
            # Step 4: Start main system
            if not self.start_main_system(auto_start):
                print("Failed to start main system")
                return 1
            
            # Step 5: Wait for system to be ready (improved timing)
            print("Waiting for system to initialize...")
            time.sleep(8)
            
            # Step 6: Check status (with improved checking)
            if not self.check_system_status():
                print("Warning: System not started properly")
            else:
                print("System started successfully!")
            
            # Step 7: Show usage instructions
            print("\n" + "=" * 50)
            if auto_start:
                print("SYSTEM STARTED WITH AUTO-TRADING")
                print("Trading will begin automatically after initialization")
            elif startup_mode == 'monitor':
                print("SYSTEM STARTED IN MONITOR-ONLY MODE")
                print("No trades will be executed")
            else:
                print("SYSTEM STARTED IN MANUAL MODE")
                print("Use the dashboard to start trading when ready")
            
            print(f"Trading Dashboard: http://localhost:{self.port}")
            print("Press Ctrl+C to stop the system")
            print("=" * 50)
            
            # Step 8: Keep running and monitor
            while self.running:
                try:
                    time.sleep(5)
                    
                    # Check if process is still alive
                    if self.main_proc and self.main_proc.poll() is not None:
                        print("Main system stopped unexpectedly")
                        # Show the error output
                        stdout, stderr = self.main_proc.communicate()
                        if stderr:
                            print(f"Error output: {stderr}")
                        break
                        
                except KeyboardInterrupt:
                    break
            
            return 0
            
        except Exception as e:
            print(f"Startup failed: {e}")
            return 1
        finally:
            self.cleanup()

def main():
    """Main entry point"""
    # Handle command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] in ['--auto', '-a']:
            os.environ['AUTO_START_TRADING'] = 'true'
            print("Command line auto-start mode enabled")
        elif sys.argv[1] in ['--monitor', '-m']:
            os.environ['MONITOR_ONLY'] = 'true'
            print("Command line monitor-only mode enabled")
        elif sys.argv[1] in ['--help', '-h']:
            print("Usage:")
            print("  python start.py           # Interactive mode")
            print("  python start.py --auto    # Auto-start trading")
            print("  python start.py --monitor # Monitor only (no trading)")
            print("")
            print("Configuration:")
            print("  Port is read from .env FLASK_PORT (default: 8107)")
            print("  Balance settings read from .env STARTING_BALANCE and TRADE_AMOUNT_USDT")
            return 0
    
    # Create and run the starter
    starter = MainSystemStarter()
    exit_code = starter.run()
    sys.exit(exit_code)

if __name__ == "__main__":
    main()