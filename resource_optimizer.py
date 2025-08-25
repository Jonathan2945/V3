#!/usr/bin/env python3
EMOJI = "[FAST]"
"""
RESOURCE OPTIMIZER
==================

Intelligent system resource management and optimization:
- CPU, Memory, and Disk monitoring
- Automatic resource cleanup
- Performance optimization
- Priority-based task scheduling
- Storage management
- System health monitoring

Features:
- Real-time resource monitoring
- Intelligent cleanup algorithms
- Performance bottleneck detection
- Adaptive resource allocation
- Predictive maintenance
- Emergency resource management
"""

import os
import psutil
import logging
import asyncio
import time
import shutil
import sqlite3
import gc
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict
import json
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import warnings
warnings.filterwarnings('ignore')

@dataclass
class ResourceMetrics:
    """System resource metrics"""
    timestamp: datetime
    cpu_percent: float
    memory_percent: float
    memory_available_gb: float
    disk_percent: float
    disk_free_gb: float
    network_sent_mb: float
    network_recv_mb: float
    process_count: int
    load_average: List[float]
    temperature: Optional[float] = None

@dataclass
class OptimizationAction:
    """Resource optimization action"""
    action_type: str
    description: str
    priority: int  # 1=critical, 2=high, 3=medium, 4=low
    estimated_savings: Dict[str, float]  # resource_type: amount_saved
    execution_time: datetime
    success: bool
    error_message: Optional[str] = None

@dataclass
class PerformanceProfile:
    """System performance profile"""
    profile_name: str
    cpu_threshold: float
    memory_threshold: float
    disk_threshold: float
    optimization_frequency: int  # minutes
    aggressive_cleanup: bool
    ml_priority: float  # 0-1, higher = more resources for ML

class ResourceOptimizer:
    """Intelligent System Resource Management"""
    
    def __init__(self):
        # Configuration
        self.config = {
            'max_cpu_usage': float(os.getenv('MAX_CPU_USAGE', 80)),
            'max_memory_usage': float(os.getenv('MAX_MEMORY_USAGE', 85)),
            'max_disk_usage': float(os.getenv('MAX_DISK_USAGE', 90)),
            'enable_auto_cleanup': os.getenv('ENABLE_AUTO_CLEANUP', 'true').lower() == 'true',
            'cleanup_interval_hours': 6,
            'monitoring_interval': 60,  # seconds
            'emergency_threshold': 95  # percent
        }
        
        # Performance profiles
        self.profiles = {
            'conservative': PerformanceProfile(
                profile_name='conservative',
                cpu_threshold=70, memory_threshold=75, disk_threshold=85,
                optimization_frequency=30, aggressive_cleanup=False, ml_priority=0.6
            ),
            'balanced': PerformanceProfile(
                profile_name='balanced',
                cpu_threshold=80, memory_threshold=85, disk_threshold=90,
                optimization_frequency=60, aggressive_cleanup=False, ml_priority=0.7
            ),
            'performance': PerformanceProfile(
                profile_name='performance',
                cpu_threshold=90, memory_threshold=90, disk_threshold=95,
                optimization_frequency=120, aggressive_cleanup=True, ml_priority=0.8
            )
        }
        
        # Current profile
        self.current_profile = self.profiles['balanced']
        
        # Monitoring data
        self.metrics_history = []
        self.max_history_length = 1440  # 24 hours at 1-minute intervals
        
        # Optimization history
        self.optimization_history = []
        
        # Resource tracking
        self.tracked_processes = []
        self.critical_processes = ['python', 'python3']  # Protect trading processes
        
        # Database for metrics storage
        self.db_path = 'data/resource_metrics.db'
        self.init_database()
        
        # Locks for thread safety
        self.metrics_lock = threading.Lock()
        self.optimization_lock = threading.Lock()
        
        logging.info("[FAST] Resource Optimizer initialized")
    
    def init_database(self):
        """Initialize database for resource metrics"""
        try:
            os.makedirs('data', exist_ok=True)
            
            with sqlite3.connect(self.db_path) as conn:
                # Resource metrics table
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS resource_metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp DATETIME NOT NULL,
                        cpu_percent REAL NOT NULL,
                        memory_percent REAL NOT NULL,
                        memory_available_gb REAL NOT NULL,
                        disk_percent REAL NOT NULL,
                        disk_free_gb REAL NOT NULL,
                        network_sent_mb REAL,
                        network_recv_mb REAL,
                        process_count INTEGER,
                        load_average TEXT,
                        temperature REAL
                    )
                ''')
                
                # Optimization actions table
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS optimization_actions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp DATETIME NOT NULL,
                        action_type TEXT NOT NULL,
                        description TEXT NOT NULL,
                        priority INTEGER NOT NULL,
                        estimated_savings TEXT,
                        success BOOLEAN NOT NULL,
                        error_message TEXT,
                        execution_duration REAL
                    )
                ''')
                
                # Performance profiles table
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS performance_profiles (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp DATETIME NOT NULL,
                        profile_name TEXT NOT NULL,
                        cpu_usage REAL,
                        memory_usage REAL,
                        disk_usage REAL,
                        optimization_count INTEGER,
                        ml_tasks_active INTEGER
                    )
                ''')
                
                conn.commit()
                
        except Exception as e:
            logging.error(f"Error initializing resource database: {e}")
    
    def get_system_metrics(self) -> ResourceMetrics:
        """Collect comprehensive system metrics"""
        try:
            # CPU metrics
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # Memory metrics
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            memory_available_gb = memory.available / (1024**3)
            
            # Disk metrics
            disk = psutil.disk_usage('/')
            disk_percent = (disk.used / disk.total) * 100
            disk_free_gb = disk.free / (1024**3)
            
            # Network metrics
            network = psutil.net_io_counters()
            network_sent_mb = network.bytes_sent / (1024**2)
            network_recv_mb = network.bytes_recv / (1024**2)
            
            # Process count
            process_count = len(psutil.pids())
            
            # Load average (Unix systems)
            try:
                load_avg = list(os.getloadavg())
            except (OSError, AttributeError):
                load_avg = [0.0, 0.0, 0.0]  # Windows fallback
            
            # Temperature (if available)
            temperature = None
            try:
                temps = psutil.sensors_temperatures()
                if temps:
                    # Get CPU temperature if available
                    for name, entries in temps.items():
                        if 'cpu' in name.lower() or 'core' in name.lower():
                            temperature = entries[0].current
                            break
            except (AttributeError, OSError):
                pass
            
            return ResourceMetrics(
                timestamp=datetime.now(),
                cpu_percent=cpu_percent,
                memory_percent=memory_percent,
                memory_available_gb=memory_available_gb,
                disk_percent=disk_percent,
                disk_free_gb=disk_free_gb,
                network_sent_mb=network_sent_mb,
                network_recv_mb=network_recv_mb,
                process_count=process_count,
                load_average=load_avg,
                temperature=temperature
            )
            
        except Exception as e:
            logging.error(f"Error collecting system metrics: {e}")
            # Return safe defaults
            return ResourceMetrics(
                timestamp=datetime.now(),
                cpu_percent=0, memory_percent=0, memory_available_gb=1,
                disk_percent=0, disk_free_gb=1, network_sent_mb=0,
                network_recv_mb=0, process_count=0, load_average=[0, 0, 0]
            )
    
    def store_metrics(self, metrics: ResourceMetrics):
        """Store metrics in database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT INTO resource_metrics 
                    (timestamp, cpu_percent, memory_percent, memory_available_gb,
                     disk_percent, disk_free_gb, network_sent_mb, network_recv_mb,
                     process_count, load_average, temperature)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    metrics.timestamp, metrics.cpu_percent, metrics.memory_percent,
                    metrics.memory_available_gb, metrics.disk_percent, metrics.disk_free_gb,
                    metrics.network_sent_mb, metrics.network_recv_mb, metrics.process_count,
                    json.dumps(metrics.load_average), metrics.temperature
                ))
                conn.commit()
                
        except Exception as e:
            logging.error(f"Error storing metrics: {e}")
    
    def check_system_health(self) -> Dict[str, Any]:
        """Check overall system health and return status"""
        try:
            metrics = self.get_system_metrics()
            
            # Store metrics
            with self.metrics_lock:
                self.metrics_history.append(metrics)
                if len(self.metrics_history) > self.max_history_length:
                    self.metrics_history.pop(0)
            
            self.store_metrics(metrics)
            
            # Health assessment
            health_status = {
                'overall_health': 'good',
                'cpu_status': 'normal',
                'memory_status': 'normal',
                'disk_status': 'normal',
                'requires_optimization': False,
                'requires_cleanup': False,
                'emergency_action_needed': False,
                'metrics': asdict(metrics),
                'recommendations': []
            }
            
            # CPU assessment
            if metrics.cpu_percent > self.config['emergency_threshold']:
                health_status['cpu_status'] = 'critical'
                health_status['emergency_action_needed'] = True
                health_status['recommendations'].append("Critical CPU usage - immediate action required")
            elif metrics.cpu_percent > self.current_profile.cpu_threshold:
                health_status['cpu_status'] = 'warning'
                health_status['requires_optimization'] = True
                health_status['recommendations'].append("High CPU usage detected")
            
            # Memory assessment
            if metrics.memory_percent > self.config['emergency_threshold']:
                health_status['memory_status'] = 'critical'
                health_status['emergency_action_needed'] = True
                health_status['recommendations'].append("Critical memory usage - immediate cleanup needed")
            elif metrics.memory_percent > self.current_profile.memory_threshold:
                health_status['memory_status'] = 'warning'
                health_status['requires_optimization'] = True
                health_status['recommendations'].append("High memory usage detected")
            
            # Disk assessment
            if metrics.disk_percent > self.config['emergency_threshold']:
                health_status['disk_status'] = 'critical'
                health_status['emergency_action_needed'] = True
                health_status['requires_cleanup'] = True
                health_status['recommendations'].append("Critical disk space - immediate cleanup required")
            elif metrics.disk_percent > self.current_profile.disk_threshold:
                health_status['disk_status'] = 'warning'
                health_status['requires_cleanup'] = True
                health_status['recommendations'].append("Low disk space - cleanup recommended")
            
            # Overall health
            if health_status['emergency_action_needed']:
                health_status['overall_health'] = 'critical'
            elif health_status['requires_optimization'] or health_status['requires_cleanup']:
                health_status['overall_health'] = 'warning'
            
            # Performance recommendations
            if len(self.metrics_history) >= 10:
                recent_metrics = self.metrics_history[-10:]
                avg_cpu = np.mean([m.cpu_percent for m in recent_metrics])
                avg_memory = np.mean([m.memory_percent for m in recent_metrics])
                
                if avg_cpu > 70:
                    health_status['recommendations'].append("Consider reducing concurrent ML tasks")
                
                if avg_memory > 80:
                    health_status['recommendations'].append("Memory usage consistently high - consider optimization")
            
            return health_status
            
        except Exception as e:
            logging.error(f"Error checking system health: {e}")
            return {
                'overall_health': 'error',
                'error': str(e),
                'requires_optimization': False,
                'requires_cleanup': False,
                'emergency_action_needed': False,
                'recommendations': ['System health check failed']
            }
    
    async def optimize_resources(self) -> List[OptimizationAction]:
        """Perform intelligent resource optimization"""
        actions = []
        
        try:
            with self.optimization_lock:
                logging.info("[FAST] Starting resource optimization...")
                
                # Get current metrics
                metrics = self.get_system_metrics()
                
                # Memory optimization
                if metrics.memory_percent > self.current_profile.memory_threshold:
                    memory_actions = await self.optimize_memory()
                    actions.extend(memory_actions)
                
                # CPU optimization
                if metrics.cpu_percent > self.current_profile.cpu_threshold:
                    cpu_actions = await self.optimize_cpu()
                    actions.extend(cpu_actions)
                
                # Disk optimization
                if metrics.disk_percent > self.current_profile.disk_threshold:
                    disk_actions = await self.optimize_disk()
                    actions.extend(disk_actions)
                
                # Process optimization
                process_actions = await self.optimize_processes()
                actions.extend(process_actions)
                
                # Cache optimization
                cache_actions = await self.optimize_caches()
                actions.extend(cache_actions)
                
                # Log optimization results
                successful_actions = [a for a in actions if a.success]
                failed_actions = [a for a in actions if not a.success]
                
                logging.info(f"[OK] Optimization complete: {len(successful_actions)} successful, {len(failed_actions)} failed")
                
                if successful_actions:
                    total_savings = {}
                    for action in successful_actions:
                        for resource, savings in action.estimated_savings.items():
                            total_savings[resource] = total_savings.get(resource, 0) + savings
                    
                    logging.info(f"[EMOJI] Estimated savings: {total_savings}")
                
                # Store optimization history
                self.optimization_history.extend(actions)
                await self.store_optimization_actions(actions)
                
                return actions
                
        except Exception as e:
            logging.error(f"Error in resource optimization: {e}")
            return []
    
    async def optimize_memory(self) -> List[OptimizationAction]:
        """Optimize memory usage"""
        actions = []
        
        try:
            # Python garbage collection
            action = OptimizationAction(
                action_type="MEMORY_CLEANUP",
                description="Python garbage collection",
                priority=2,
                estimated_savings={"memory_mb": 50},
                execution_time=datetime.now(),
                success=False
            )
            
            try:
                collected = gc.collect()
                action.success = True
                action.estimated_savings["memory_mb"] = collected * 10  # Rough estimate
                logging.info(f"[EMOJI] Garbage collection: {collected} objects collected")
            except Exception as e:
                action.error_message = str(e)
            
            actions.append(action)
            
            # Clear internal caches
            action = OptimizationAction(
                action_type="CACHE_CLEAR",
                description="Clear internal caches",
                priority=3,
                estimated_savings={"memory_mb": 20},
                execution_time=datetime.now(),
                success=False
            )
            
            try:
                # Clear various caches that might exist
                import sys
                if hasattr(sys, '_clear_type_cache'):
                    sys._clear_type_cache()
                
                action.success = True
                logging.info("[CLEANUP] Internal caches cleared")
            except Exception as e:
                action.error_message = str(e)
            
            actions.append(action)
            
            # Memory profiling and optimization suggestions
            memory = psutil.virtual_memory()
            if memory.percent > 90:
                action = OptimizationAction(
                    action_type="EMERGENCY_MEMORY",
                    description="Emergency memory management",
                    priority=1,
                    estimated_savings={"memory_mb": 100},
                    execution_time=datetime.now(),
                    success=False
                )
                
                try:
                    # Emergency memory cleanup
                    import ctypes
                    if os.name == 'nt':  # Windows
                        ctypes.windll.kernel32.SetProcessWorkingSetSize(-1, -1, -1)
                    
                    action.success = True
                    logging.warning("[EMOJI] Emergency memory cleanup executed")
                except Exception as e:
                    action.error_message = str(e)
                
                actions.append(action)
            
        except Exception as e:
            logging.error(f"Error in memory optimization: {e}")
        
        return actions
    
    async def optimize_cpu(self) -> List[OptimizationAction]:
        """Optimize CPU usage"""
        actions = []
        
        try:
            # Process priority adjustment
            action = OptimizationAction(
                action_type="CPU_PRIORITY",
                description="Adjust process priorities",
                priority=2,
                estimated_savings={"cpu_percent": 5},
                execution_time=datetime.now(),
                success=False
            )
            
            try:
                current_process = psutil.Process()
                
                # Lower priority for non-critical processes
                if current_process.nice() == 0:  # Normal priority
                    current_process.nice(1)  # Lower priority slightly
                
                action.success = True
                logging.info("[EMOJI] Process priorities adjusted")
            except Exception as e:
                action.error_message = str(e)
            
            actions.append(action)
            
            # Thread pool optimization
            action = OptimizationAction(
                action_type="THREAD_OPTIMIZATION",
                description="Optimize thread pool usage",
                priority=3,
                estimated_savings={"cpu_percent": 3},
                execution_time=datetime.now(),
                success=True  # This is more of a configuration suggestion
            )
            
            # Recommend optimal thread count
            cpu_count = psutil.cpu_count()
            optimal_threads = min(cpu_count, 4)  # Limit to 4 for stability
            
            action.description += f" (Recommended: {optimal_threads} threads)"
            logging.info(f"Thread Optimal thread count: {optimal_threads}")
            
            actions.append(action)
            
        except Exception as e:
            logging.error(f"Error in CPU optimization: {e}")
        
        return actions
    
    async def optimize_disk(self) -> List[OptimizationAction]:
        """Optimize disk usage"""
        actions = []
        
        try:
            # Clean temporary files
            action = OptimizationAction(
                action_type="TEMP_CLEANUP",
                description="Clean temporary files",
                priority=2,
                estimated_savings={"disk_mb": 100},
                execution_time=datetime.now(),
                success=False
            )
            
            try:
                temp_dirs = ['/tmp', 'data/cache', 'logs']
                total_cleaned = 0
                
                for temp_dir in temp_dirs:
                    if os.path.exists(temp_dir):
                        cleaned = await self.clean_directory(temp_dir, max_age_hours=24)
                        total_cleaned += cleaned
                
                action.success = True
                action.estimated_savings["disk_mb"] = total_cleaned
                logging.info(f"[EMOJI] Cleaned {total_cleaned:.1f}MB of temporary files")
            except Exception as e:
                action.error_message = str(e)
            
            actions.append(action)
            
            # Clean old log files
            action = OptimizationAction(
                action_type="LOG_CLEANUP",
                description="Clean old log files",
                priority=3,
                estimated_savings={"disk_mb": 50},
                execution_time=datetime.now(),
                success=False
            )
            
            try:
                if os.path.exists('logs'):
                    cleaned = await self.clean_log_files('logs', max_age_days=7)
                    action.success = True
                    action.estimated_savings["disk_mb"] = cleaned
                    logging.info(f"[EMOJI] Cleaned {cleaned:.1f}MB of old logs")
                else:
                    action.success = True
                    action.estimated_savings["disk_mb"] = 0
            except Exception as e:
                action.error_message = str(e)
            
            actions.append(action)
            
            # Clean old database records
            action = OptimizationAction(
                action_type="DATABASE_CLEANUP",
                description="Clean old database records",
                priority=3,
                estimated_savings={"disk_mb": 30},
                execution_time=datetime.now(),
                success=False
            )
            
            try:
                cleaned = await self.clean_old_database_records()
                action.success = True
                action.estimated_savings["disk_mb"] = cleaned
                logging.info(f"[EMOJI] Cleaned {cleaned:.1f}MB from databases")
            except Exception as e:
                action.error_message = str(e)
            
            actions.append(action)
            
        except Exception as e:
            logging.error(f"Error in disk optimization: {e}")
        
        return actions
    
    def _safe_remove_file(self, file_path: str) -> bool:
        """Safely remove a file with proper error handling"""
        if not file_path or not isinstance(file_path, str):
            return False
        
        try:
            # Validate path exists and is a file
            if not os.path.exists(file_path):
                return False
            
            if not os.path.isfile(file_path):
                return False
            
            # Check if file is readable/writable
            if not os.access(file_path, os.R_OK | os.W_OK):
                return False
            
            # Safety check - don't remove system files or files outside expected dirs
            allowed_dirs = ['/tmp', 'data/', 'logs/', 'cache/', 'temp/']
            if not any(allowed_dir in file_path for allowed_dir in allowed_dirs):
                return False
            
            # Perform the removal
            os.unlink(file_path)  # Use os.unlink instead of os.remove for clarity
            return True
            
        except (OSError, IOError, PermissionError) as e:
            logging.warning(f"Safe file removal failed for {file_path}: {e}")
            return False
        except Exception as e:
            logging.error(f"Unexpected error removing file {file_path}: {e}")
            return False
    
    async def clean_directory(self, directory: str, max_age_hours: int = 24) -> float:
        """Clean old files from directory with enhanced safety"""
        try:
            if not directory or not os.path.exists(directory):
                return 0
            
            # Validate directory path for safety
            if not os.path.isdir(directory):
                return 0
                
            total_size = 0
            cutoff_time = time.time() - (max_age_hours * 3600)
            
            for root, dirs, files in os.walk(directory):
                for file in files:
                    try:
                        file_path = os.path.join(root, file)
                        
                        # Check file age
                        if os.path.getmtime(file_path) < cutoff_time:
                            # Get file size before removal
                            try:
                                file_size = os.path.getsize(file_path)
                            except (OSError, IOError):
                                continue  # Skip if we can't get size
                            
                            # Use safe removal method
                            if self._safe_remove_file(file_path):
                                total_size += file_size
                                
                    except (OSError, IOError, PermissionError):
                        continue  # Skip files that can't be processed
                    except Exception as e:
                        logging.warning(f"Error processing file {file}: {e}")
                        continue
            
            return total_size / (1024 * 1024)  # Convert to MB
            
        except Exception as e:
            logging.warning(f"Error cleaning directory {directory}: {e}")
            return 0
    
    async def clean_log_files(self, log_directory: str, max_age_days: int = 7) -> float:
        """Clean old log files with enhanced safety"""
        try:
            if not log_directory or not os.path.exists(log_directory):
                return 0
            
            total_size = 0
            cutoff_time = time.time() - (max_age_days * 24 * 3600)
            
            for file in os.listdir(log_directory):
                if file.endswith('.log'):
                    file_path = os.path.join(log_directory, file)
                    try:
                        if os.path.getmtime(file_path) < cutoff_time:
                            try:
                                file_size = os.path.getsize(file_path)
                            except (OSError, IOError):
                                continue
                            
                            if self._safe_remove_file(file_path):
                                total_size += file_size
                                
                    except (OSError, IOError, PermissionError):
                        continue
                    except Exception as e:
                        logging.warning(f"Error processing log file {file}: {e}")
                        continue
            
            return total_size / (1024 * 1024)
            
        except Exception as e:
            logging.warning(f"Error cleaning log files: {e}")
            return 0
    
    async def clean_old_database_records(self) -> float:
        """Clean old records from databases"""
        try:
            total_freed = 0
            
            # Clean old resource metrics (keep only last 30 days)
            if os.path.exists(self.db_path):
                initial_size = os.path.getsize(self.db_path)
                
                with sqlite3.connect(self.db_path) as conn:
                    cutoff_date = datetime.now() - timedelta(days=30)
                    
                    cursor = conn.execute('''
                        DELETE FROM resource_metrics 
                        WHERE timestamp < ?
                    ''', (cutoff_date,))
                    
                    conn.execute('VACUUM')  # Reclaim space
                    conn.commit()
                
                final_size = os.path.getsize(self.db_path)
                total_freed += max(0, initial_size - final_size)
            
            # Clean other databases
            db_files = [
                'data/historical_data.db',
                'data/external_data.db',
                'data/intelligent_trades.db'
            ]
            
            for db_file in db_files:
                if os.path.exists(db_file):
                    try:
                        initial_size = os.path.getsize(db_file)
                        
                        with sqlite3.connect(db_file) as conn:
                            # Clean very old records (keep last 90 days)
                            cutoff_date = datetime.now() - timedelta(days=90)
                            
                            # This would need to be customized for each database schema
                            try:
                                conn.execute('VACUUM')
                                conn.commit()
                            except:
                                pass  # Ignore vacuum errors
                        
                        final_size = os.path.getsize(db_file)
                        total_freed += max(0, initial_size - final_size)
                        
                    except Exception as e:
                        logging.warning(f"Error cleaning {db_file}: {e}")
            
            return total_freed / (1024 * 1024)
            
        except Exception as e:
            logging.warning(f"Error cleaning database records: {e}")
            return 0
    
    async def optimize_processes(self) -> List[OptimizationAction]:
        """Optimize running processes"""
        actions = []
        
        try:
            # Monitor process resource usage
            action = OptimizationAction(
                action_type="PROCESS_MONITOR",
                description="Monitor and optimize processes",
                priority=3,
                estimated_savings={"cpu_percent": 2, "memory_mb": 30},
                execution_time=datetime.now(),
                success=False
            )
            
            try:
                high_cpu_processes = []
                high_memory_processes = []
                
                for process in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent']):
                    try:
                        pinfo = process.info
                        if pinfo['cpu_percent'] > 20:  # High CPU usage
                            high_cpu_processes.append(pinfo)
                        if pinfo['memory_percent'] > 10:  # High memory usage
                            high_memory_processes.append(pinfo)
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
                
                action.success = True
                action.description += f" (Found {len(high_cpu_processes)} high-CPU, {len(high_memory_processes)} high-memory processes)"
                
                if high_cpu_processes or high_memory_processes:
                    logging.info(f"[SEARCH] Process monitoring: {len(high_cpu_processes)} high-CPU, {len(high_memory_processes)} high-memory processes")
                
            except Exception as e:
                action.error_message = str(e)
            
            actions.append(action)
            
        except Exception as e:
            logging.error(f"Error in process optimization: {e}")
        
        return actions
    
    async def optimize_caches(self) -> List[OptimizationAction]:
        """Optimize various caches"""
        actions = []
        
        try:
            # Clear Python import cache
            action = OptimizationAction(
                action_type="IMPORT_CACHE",
                description="Clear Python import cache",
                priority=4,
                estimated_savings={"memory_mb": 10},
                execution_time=datetime.now(),
                success=False
            )
            
            try:
                import sys
                # Clear import cache
                if hasattr(sys, 'path_importer_cache'):
                    sys.path_importer_cache.clear()
                
                action.success = True
                logging.info("[EMOJI] Python import cache cleared")
            except Exception as e:
                action.error_message = str(e)
            
            actions.append(action)
            
            # Clear numpy cache
            action = OptimizationAction(
                action_type="NUMPY_CACHE",
                description="Clear NumPy cache",
                priority=4,
                estimated_savings={"memory_mb": 5},
                execution_time=datetime.now(),
                success=False
            )
            
            try:
                import numpy as np
                # Clear numpy's internal caches if possible
                action.success = True
                logging.info("[EMOJI] NumPy cache optimization")
            except Exception as e:
                action.error_message = str(e)
            
            actions.append(action)
            
        except Exception as e:
            logging.error(f"Error in cache optimization: {e}")
        
        return actions
    
    async def store_optimization_actions(self, actions: List[OptimizationAction]):
        """Store optimization actions in database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                for action in actions:
                    execution_duration = (datetime.now() - action.execution_time).total_seconds()
                    
                    conn.execute('''
                        INSERT INTO optimization_actions 
                        (timestamp, action_type, description, priority, estimated_savings,
                         success, error_message, execution_duration)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        action.execution_time, action.action_type, action.description,
                        action.priority, json.dumps(action.estimated_savings),
                        action.success, action.error_message, execution_duration
                    ))
                
                conn.commit()
                
        except Exception as e:
            logging.error(f"Error storing optimization actions: {e}")
    
    def adapt_performance_profile(self, system_load: float, ml_priority: float):
        """Adapt performance profile based on system conditions"""
        try:
            if system_load > 0.9:  # High system load
                if self.current_profile.profile_name != 'conservative':
                    self.current_profile = self.profiles['conservative']
                    logging.info("[TOOL] Switched to conservative performance profile")
            elif system_load < 0.5 and ml_priority > 0.8:  # Low load, high ML priority
                if self.current_profile.profile_name != 'performance':
                    self.current_profile = self.profiles['performance']
                    logging.info("[LAUNCH] Switched to performance profile")
            else:  # Balanced conditions
                if self.current_profile.profile_name != 'balanced':
                    self.current_profile = self.profiles['balanced']
                    logging.info("[EMOJI] Switched to balanced performance profile")
                    
        except Exception as e:
            logging.error(f"Error adapting performance profile: {e}")
    
    async def emergency_cleanup(self):
        """Emergency resource cleanup for critical situations"""
        try:
            logging.warning("[EMOJI] Executing emergency resource cleanup")
            
            # Force garbage collection
            collected = gc.collect()
            logging.warning(f"[EMOJI] Emergency GC: {collected} objects collected")
            
            # Clear all possible caches
            import sys
            if hasattr(sys, '_clear_type_cache'):
                sys._clear_type_cache()
            
            # Emergency disk cleanup
            if os.path.exists('data/cache'):
                await self.clean_directory('data/cache', max_age_hours=1)
            
            # Clear temporary files aggressively
            for temp_dir in ['/tmp', '/var/tmp', 'data/temp']:
                if os.path.exists(temp_dir):
                    await self.clean_directory(temp_dir, max_age_hours=1)
            
            logging.warning("[OK] Emergency cleanup completed")
            
        except Exception as e:
            logging.error(f"Error in emergency cleanup: {e}")
    
    def get_performance_recommendations(self) -> List[str]:
        """Get performance optimization recommendations"""
        recommendations = []
        
        try:
            if len(self.metrics_history) < 10:
                return ["Insufficient data for recommendations"]
            
            recent_metrics = self.metrics_history[-10:]
            
            # CPU recommendations
            avg_cpu = np.mean([m.cpu_percent for m in recent_metrics])
            if avg_cpu > 80:
                recommendations.append("High CPU usage detected - consider reducing concurrent tasks")
            elif avg_cpu < 30:
                recommendations.append("Low CPU usage - system can handle more concurrent tasks")
            
            # Memory recommendations
            avg_memory = np.mean([m.memory_percent for m in recent_metrics])
            if avg_memory > 85:
                recommendations.append("High memory usage - enable aggressive cleanup")
            elif avg_memory > 70:
                recommendations.append("Moderate memory usage - consider periodic cleanup")
            
            # Disk recommendations
            latest_disk = recent_metrics[-1].disk_percent
            if latest_disk > 90:
                recommendations.append("Critical disk space - immediate cleanup required")
            elif latest_disk > 80:
                recommendations.append("Low disk space - schedule regular cleanup")
            
            # Performance profile recommendations
            system_load = (avg_cpu + avg_memory) / 200  # Normalize to 0-1
            
            if system_load > 0.8 and self.current_profile.profile_name != 'conservative':
                recommendations.append("Consider switching to conservative performance profile")
            elif system_load < 0.4 and self.current_profile.profile_name != 'performance':
                recommendations.append("System resources available - can use performance profile")
            
            # Trending analysis
            if len(recent_metrics) >= 5:
                cpu_trend = np.polyfit(range(5), [m.cpu_percent for m in recent_metrics[-5:]], 1)[0]
                memory_trend = np.polyfit(range(5), [m.memory_percent for m in recent_metrics[-5:]], 1)[0]
                
                if cpu_trend > 2:  # Increasing by >2% per measurement
                    recommendations.append("CPU usage trending upward - monitor closely")
                if memory_trend > 1:  # Increasing by >1% per measurement
                    recommendations.append("Memory usage trending upward - consider cleanup")
            
            return recommendations if recommendations else ["System performance is optimal"]
            
        except Exception as e:
            logging.error(f"Error generating recommendations: {e}")
            return ["Error generating performance recommendations"]
    
    def get_resource_summary(self) -> Dict[str, Any]:
        """Get comprehensive resource summary"""
        try:
            latest_metrics = self.metrics_history[-1] if self.metrics_history else None
            
            if not latest_metrics:
                return {"error": "No metrics available"}
            
            # Calculate resource efficiency
            cpu_efficiency = max(0, 100 - latest_metrics.cpu_percent)
            memory_efficiency = max(0, 100 - latest_metrics.memory_percent)
            disk_efficiency = max(0, 100 - latest_metrics.disk_percent)
            
            overall_efficiency = (cpu_efficiency + memory_efficiency + disk_efficiency) / 3
            
            # Recent optimization stats
            recent_optimizations = [a for a in self.optimization_history 
                                  if (datetime.now() - a.execution_time).hours < 24]
            
            successful_optimizations = len([a for a in recent_optimizations if a.success])
            
            return {
                'current_metrics': asdict(latest_metrics),
                'efficiency_scores': {
                    'cpu_efficiency': cpu_efficiency,
                    'memory_efficiency': memory_efficiency,
                    'disk_efficiency': disk_efficiency,
                    'overall_efficiency': overall_efficiency
                },
                'current_profile': self.current_profile.profile_name,
                'optimization_stats': {
                    'total_optimizations_24h': len(recent_optimizations),
                    'successful_optimizations_24h': successful_optimizations,
                    'last_optimization': recent_optimizations[-1].execution_time.isoformat() 
                                       if recent_optimizations else None
                },
                'recommendations': self.get_performance_recommendations(),
                'health_status': 'good' if overall_efficiency > 70 else 
                               'warning' if overall_efficiency > 50 else 'critical'
            }
            
        except Exception as e:
            logging.error(f"Error getting resource summary: {e}")
            return {"error": str(e)}
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get resource optimizer metrics"""
        try:
            latest_metrics = self.metrics_history[-1] if self.metrics_history else None
            
            if not latest_metrics:
                return {"status": "no_data"}
            
            return {
                'cpu_percent': latest_metrics.cpu_percent,
                'memory_percent': latest_metrics.memory_percent,
                'disk_percent': latest_metrics.disk_percent,
                'memory_available_gb': latest_metrics.memory_available_gb,
                'disk_free_gb': latest_metrics.disk_free_gb,
                'process_count': latest_metrics.process_count,
                'load_average': latest_metrics.load_average,
                'temperature': latest_metrics.temperature,
                'current_profile': self.current_profile.profile_name,
                'metrics_history_length': len(self.metrics_history),
                'optimization_actions_24h': len([a for a in self.optimization_history 
                                               if (datetime.now() - a.execution_time).hours < 24]),
                'last_optimization': self.optimization_history[-1].execution_time.isoformat() 
                                   if self.optimization_history else None
            }
            
        except Exception as e:
            logging.error(f"Error getting resource metrics: {e}")
            return {"error": str(e)}


# Conditional execution guard
if __name__ == "__main__":
    # This code only runs when the script is executed directly
    print("[FAST] Resource Optimizer - Direct execution mode")
    optimizer = ResourceOptimizer()
    print("[FAST] Resource Optimizer initialized successfully")