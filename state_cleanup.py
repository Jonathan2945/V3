#!/usr/bin/env python3
"""
V3 STATE CLEANUP UTILITY
========================
Fixes the "already started" issue by properly cleaning up system state.
Use this when the dashboard says backtesting is already started but no progress shows.

Usage:
    python state_cleanup.py --all          # Clean all state
    python state_cleanup.py --backtest     # Clean only backtest state
    python state_cleanup.py --progress     # Clean only progress tracking
    python state_cleanup.py --check        # Check current state without cleaning
"""

import sqlite3
import os
import sys
from pathlib import Path
from datetime import datetime
import argparse
import json
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class V3StateCleanup:
    """V3 State Cleanup Utility"""
    
    def __init__(self):
        self.data_dir = Path('data')
        self.databases = {
            'backtest_progress': self.data_dir / 'backtest_progress.db',
            'backtest_results': self.data_dir / 'comprehensive_backtest.db',
            'trading_metrics': self.data_dir / 'trading_metrics.db',
            'api_monitor': self.data_dir / 'api_monitor.db'
        }
        
        # Ensure data directory exists
        self.data_dir.mkdir(exist_ok=True)
    
    def check_current_state(self) -> dict:
        """Check current system state"""
        state = {
            'backtest_progress': {},
            'backtest_results_count': 0,
            'trading_metrics': {},
            'databases_exist': {},
            'issues_found': []
        }
        
        logger.info("Checking current V3 system state...")
        
        # Check database existence
        for db_name, db_path in self.databases.items():
            state['databases_exist'][db_name] = db_path.exists()
            if not db_path.exists():
                state['issues_found'].append(f"Database {db_name} does not exist")
        
        # Check backtest progress state
        try:
            if self.databases['backtest_progress'].exists():
                conn = sqlite3.connect(self.databases['backtest_progress'])
                cursor = conn.cursor()
                
                # Get current progress
                cursor.execute("SELECT * FROM backtest_progress WHERE id = 1")
                result = cursor.fetchone()
                
                if result:
                    columns = ['id', 'status', 'current_symbol', 'current_strategy', 'completed', 
                              'total', 'error_count', 'start_time', 'completion_time', 
                              'progress_percent', 'eta_minutes', 'results_count', 'last_update']
                    
                    progress_data = dict(zip(columns, result))
                    state['backtest_progress'] = progress_data
                    
                    # Check for stuck states
                    if progress_data['status'] == 'in_progress':
                        if progress_data['last_update']:
                            from datetime import datetime, timedelta
                            last_update = datetime.fromisoformat(progress_data['last_update'])
                            if datetime.now() - last_update > timedelta(minutes=30):
                                state['issues_found'].append("Backtest stuck in 'in_progress' state for >30 minutes")
                        else:
                            state['issues_found'].append("Backtest in 'in_progress' state with no last_update")
                
                conn.close()
            
        except Exception as e:
            state['issues_found'].append(f"Error checking backtest progress: {e}")
        
        # Check backtest results count
        try:
            if self.databases['backtest_results'].exists():
                conn = sqlite3.connect(self.databases['backtest_results'])
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM historical_backtests")
                state['backtest_results_count'] = cursor.fetchone()[0]
                conn.close()
        except Exception as e:
            state['issues_found'].append(f"Error checking backtest results: {e}")
        
        # Check trading metrics
        try:
            if self.databases['trading_metrics'].exists():
                conn = sqlite3.connect(self.databases['trading_metrics'])
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM system_metrics WHERE id = 1")
                result = cursor.fetchone()
                
                if result:
                    state['trading_metrics'] = {
                        'total_trades': result[1],
                        'winning_trades': result[2],
                        'total_pnl': result[3],
                        'win_rate': result[4],
                        'active_positions': result[5],
                        'last_updated': result[6]
                    }
                
                conn.close()
        except Exception as e:
            state['issues_found'].append(f"Error checking trading metrics: {e}")
        
        return state
    
    def clean_backtest_progress(self) -> bool:
        """Clean backtest progress state - fixes the 'already started' issue"""
        try:
            logger.info("Cleaning backtest progress state...")
            
            db_path = self.databases['backtest_progress']
            if not db_path.exists():
                logger.info("Backtest progress database does not exist, nothing to clean")
                return True
            
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Clear progress table
            cursor.execute("DELETE FROM backtest_progress")
            cursor.execute("DELETE FROM backtest_state")
            
            # Reset sequences if they exist
            cursor.execute("DELETE FROM sqlite_sequence WHERE name IN ('backtest_progress', 'backtest_state')")
            
            conn.commit()
            conn.close()
            
            logger.info("? Backtest progress state cleaned successfully")
            return True
            
        except Exception as e:
            logger.error(f"? Failed to clean backtest progress: {e}")
            return False
    
    def clean_backtest_results(self, keep_results: bool = True) -> bool:
        """Clean backtest results (optionally keep the actual results)"""
        try:
            logger.info(f"Cleaning backtest results database (keep_results={keep_results})...")
            
            db_path = self.databases['backtest_results']
            if not db_path.exists():
                logger.info("Backtest results database does not exist, nothing to clean")
                return True
            
            if not keep_results:
                # Remove entire database
                db_path.unlink()
                logger.info("? Backtest results database removed")
            else:
                # Just clean up any stuck states but keep results
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                
                # Remove any temporary or incomplete results
                cursor.execute("DELETE FROM historical_backtests WHERE total_trades < 5")
                
                conn.commit()
                conn.close()
                logger.info("? Cleaned incomplete backtest results")
            
            return True
            
        except Exception as e:
            logger.error(f"? Failed to clean backtest results: {e}")
            return False
    
    def clean_trading_metrics(self, reset_to_zero: bool = False) -> bool:
        """Clean trading metrics"""
        try:
            logger.info(f"Cleaning trading metrics (reset_to_zero={reset_to_zero})...")
            
            db_path = self.databases['trading_metrics']
            if not db_path.exists():
                logger.info("Trading metrics database does not exist, nothing to clean")
                return True
            
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            if reset_to_zero:
                # Reset metrics to zero
                cursor.execute("""
                    INSERT OR REPLACE INTO system_metrics 
                    (id, total_trades, winning_trades, total_pnl, win_rate, 
                     active_positions, last_updated, session_start)
                    VALUES (1, 0, 0, 0.0, 0.0, 0, ?, ?)
                """, (datetime.now().isoformat(), datetime.now().isoformat()))
                
                logger.info("? Trading metrics reset to zero")
            else:
                # Just update last_updated timestamp
                cursor.execute("""
                    UPDATE system_metrics 
                    SET last_updated = ?, session_start = ?
                    WHERE id = 1
                """, (datetime.now().isoformat(), datetime.now().isoformat()))
                
                logger.info("? Trading metrics timestamps updated")
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"? Failed to clean trading metrics: {e}")
            return False
    
    def clean_all_state(self, preserve_results: bool = True, reset_metrics: bool = False) -> bool:
        """Clean all system state"""
        logger.info("="*60)
        logger.info("?? CLEANING ALL V3 SYSTEM STATE")
        logger.info("="*60)
        
        success = True
        
        # Clean backtest progress (this fixes the main issue)
        if not self.clean_backtest_progress():
            success = False
        
        # Clean backtest results
        if not self.clean_backtest_results(keep_results=preserve_results):
            success = False
        
        # Clean trading metrics
        if not self.clean_trading_metrics(reset_to_zero=reset_metrics):
            success = False
        
        if success:
            logger.info("? All system state cleaned successfully!")
            logger.info("You can now restart the V3 system without 'already started' issues")
        else:
            logger.error("? Some cleanup operations failed")
        
        return success
    
    def backup_state(self) -> str:
        """Create a backup of current state"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_dir = Path(f'backups/state_backup_{timestamp}')
            backup_dir.mkdir(parents=True, exist_ok=True)
            
            logger.info(f"Creating state backup in {backup_dir}...")
            
            # Copy databases
            for db_name, db_path in self.databases.items():
                if db_path.exists():
                    backup_path = backup_dir / db_path.name
                    import shutil
                    shutil.copy2(db_path, backup_path)
                    logger.info(f"Backed up {db_name}")
            
            # Save state summary
            state = self.check_current_state()
            with open(backup_dir / 'state_summary.json', 'w') as f:
                json.dump(state, f, indent=2, default=str)
            
            logger.info(f"? State backup created: {backup_dir}")
            return str(backup_dir)
            
        except Exception as e:
            logger.error(f"? Backup failed: {e}")
            return ""
    
    def print_state_report(self, state: dict):
        """Print a detailed state report"""
        print("\n" + "="*60)
        print("?? V3 SYSTEM STATE REPORT")
        print("="*60)
        
        # Database existence
        print("\n???  Databases:")
        for db_name, exists in state['databases_exist'].items():
            status = "? EXISTS" if exists else "? MISSING"
            print(f"  {db_name}: {status}")
        
        # Backtest progress
        print("\n?? Backtest Progress:")
        if state['backtest_progress']:
            progress = state['backtest_progress']
            print(f"  Status: {progress.get('status', 'Unknown')}")
            print(f"  Progress: {progress.get('completed', 0)}/{progress.get('total', 0)} ({progress.get('progress_percent', 0):.1f}%)")
            print(f"  Current: {progress.get('current_symbol', 'None')} - {progress.get('current_strategy', 'None')}")
            print(f"  Last Update: {progress.get('last_update', 'Never')}")
        else:
            print("  No active backtest progress found")
        
        # Backtest results
        print(f"\n?? Backtest Results: {state['backtest_results_count']} results stored")
        
        # Trading metrics
        print("\n?? Trading Metrics:")
        if state['trading_metrics']:
            metrics = state['trading_metrics']
            print(f"  Total Trades: {metrics.get('total_trades', 0)}")
            print(f"  Win Rate: {metrics.get('win_rate', 0):.1f}%")
            print(f"  Total P&L: ${metrics.get('total_pnl', 0):.2f}")
            print(f"  Active Positions: {metrics.get('active_positions', 0)}")
        else:
            print("  No trading metrics found")
        
        # Issues
        if state['issues_found']:
            print("\n??  Issues Found:")
            for issue in state['issues_found']:
                print(f"  ? {issue}")
        else:
            print("\n? No issues found")
        
        print("="*60)

def main():
    parser = argparse.ArgumentParser(
        description="V3 Trading System State Cleanup Utility",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python state_cleanup.py --check                    # Check current state
  python state_cleanup.py --backtest                 # Fix 'already started' issue
  python state_cleanup.py --all                      # Complete cleanup
  python state_cleanup.py --all --backup             # Cleanup with backup
  python state_cleanup.py --all --reset-metrics      # Cleanup and reset trading metrics
        """
    )
    
    parser.add_argument('--check', action='store_true', 
                       help='Check current system state without cleaning')
    parser.add_argument('--backtest', action='store_true', 
                       help='Clean backtest progress state only (fixes "already started" issue)')
    parser.add_argument('--progress', action='store_true', 
                       help='Clean progress tracking only')
    parser.add_argument('--all', action='store_true', 
                       help='Clean all system state')
    parser.add_argument('--backup', action='store_true', 
                       help='Create backup before cleaning')
    parser.add_argument('--reset-metrics', action='store_true', 
                       help='Reset trading metrics to zero')
    parser.add_argument('--remove-results', action='store_true', 
                       help='Remove backtest results (use with caution)')
    
    args = parser.parse_args()
    
    # If no arguments provided, show help
    if not any(vars(args).values()):
        parser.print_help()
        return
    
    cleanup = V3StateCleanup()
    
    # Check current state
    current_state = cleanup.check_current_state()
    
    if args.check:
        cleanup.print_state_report(current_state)
        return
    
    # Create backup if requested
    if args.backup:
        backup_path = cleanup.backup_state()
        if not backup_path:
            print("? Backup failed, aborting cleanup for safety")
            return
    
    # Perform cleanup operations
    if args.backtest or args.progress:
        # This is the main fix for the "already started" issue
        if cleanup.clean_backtest_progress():
            print("? Backtest progress state cleaned - 'already started' issue should be fixed")
            print("You can now restart the V3 system and start backtesting normally")
        else:
            print("? Failed to clean backtest progress state")
    
    elif args.all:
        cleanup.clean_all_state(
            preserve_results=not args.remove_results,
            reset_metrics=args.reset_metrics
        )
    
    # Show final state
    print("\n?? Final State Check:")
    final_state = cleanup.check_current_state()
    
    if not final_state['issues_found']:
        print("? No issues found - system should work normally now")
    else:
        print("??  Remaining issues:")
        for issue in final_state['issues_found']:
            print(f"  ? {issue}")

if __name__ == "__main__":
    main()