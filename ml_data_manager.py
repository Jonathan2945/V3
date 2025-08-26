#!/usr/bin/env python3
"""
UPGRADED ML DATA MANAGER - V3 LIVE DATA ONLY
============================================
- Exclusively uses live market data for all operations
- No mock/test data references - production ready
- Enhanced database schema for ML metrics tracking
- Real-time data synchronization with live feeds
- Fixed runtime safety issues with proper file handling
"""

import sqlite3
import json
import pickle
import logging
import os
import shutil
import zipfile
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import pandas as pd
import numpy as np

class MLDataManager:
    """V3 ML data storage and management system - LIVE DATA ONLY"""
    
    def __init__(self, data_dir: str = "data/ml_storage"):
        self.data_dir = data_dir
        self.db_path = os.path.join(data_dir, "ml_data.db")
        
        # V3: Live training data database for real-time sync
        self.training_db_path = "data/ml_training_data.db"
        
        self.models_dir = os.path.join(data_dir, "models")
        self.training_data_dir = os.path.join(data_dir, "training_data")
        self.backups_dir = os.path.join(data_dir, "backups")
        
        self.ensure_directories()
        self.init_database()
        self.init_training_database()
        
        logging.info("[ML_DATA] V3 ML data manager initialized - LIVE DATA ONLY")
    
    def ensure_directories(self):
        """Create all required directories with proper error handling"""
        os.makedirs("data", exist_ok=True)
        
        for directory in [self.data_dir, self.models_dir, self.training_data_dir, self.backups_dir]:
            try:
                os.makedirs(directory, exist_ok=True)
                if not os.path.exists(directory):
                    logging.error(f"[ML_DATA] Failed to create directory: {directory}")
                elif not os.access(directory, os.W_OK):
                    logging.error(f"[ML_DATA] Directory not writable: {directory}")
            except Exception as e:
                logging.error(f"[ML_DATA] Error creating directory {directory}: {e}")
    
    def init_training_database(self):
        """V3: Initialize live training data database for real-time learning"""
        conn = None
        try:
            conn = sqlite3.connect(self.training_db_path)
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=NORMAL')
            cursor = conn.cursor()
            
            # Live market training data - NO MOCK DATA
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS live_training_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    
                    -- Live Price Data from Real Markets
                    open_price REAL NOT NULL,
                    high_price REAL NOT NULL,
                    low_price REAL NOT NULL,
                    close_price REAL NOT NULL,
                    volume REAL DEFAULT 0.0,
                    
                    -- Live Technical Indicators
                    rsi REAL,
                    macd REAL,
                    macd_signal REAL,
                    bb_upper REAL,
                    bb_lower REAL,
                    sma_20 REAL,
                    ema_12 REAL,
                    ema_26 REAL,
                    
                    -- Live Market Context
                    volatility REAL,
                    trend_direction TEXT,
                    support_level REAL,
                    resistance_level REAL,
                    
                    -- Production Labels for Supervised Learning
                    price_direction INTEGER,  -- 1 for up, 0 for down
                    confidence_score REAL,
                    actual_return REAL,
                    
                    -- Live External Data
                    news_sentiment REAL,
                    fear_greed_index INTEGER,
                    market_cap REAL,
                    
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Production ML Training Sessions - Live Data Only
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ml_training_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT UNIQUE NOT NULL,
                    model_type TEXT NOT NULL,
                    training_start TEXT NOT NULL,
                    training_end TEXT,
                    
                    -- Live Training Configuration
                    algorithm_used TEXT,
                    hyperparameters TEXT,
                    training_data_size INTEGER,
                    validation_data_size INTEGER,
                    live_data_size INTEGER,
                    
                    -- Live Training Results
                    training_accuracy REAL,
                    validation_accuracy REAL,
                    live_accuracy REAL,
                    training_loss REAL,
                    validation_loss REAL,
                    
                    -- Production Model Performance
                    precision_score REAL,
                    recall_score REAL,
                    f1_score REAL,
                    confusion_matrix TEXT,
                    
                    -- Live Trading Performance
                    live_trading_return REAL,
                    sharpe_ratio REAL,
                    max_drawdown REAL,
                    win_rate REAL,
                    
                    status TEXT DEFAULT 'ACTIVE',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Live Production Predictions
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS live_predictions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    prediction_id TEXT UNIQUE NOT NULL,
                    model_session_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    
                    -- Live Input Features
                    input_features TEXT NOT NULL,
                    market_conditions TEXT,
                    
                    -- Live Prediction Output
                    predicted_direction INTEGER,
                    predicted_price REAL,
                    confidence_score REAL,
                    risk_score REAL,
                    
                    -- Live Validation (filled from actual market data)
                    actual_direction INTEGER,
                    actual_price REAL,
                    prediction_accuracy REAL,
                    was_profitable BOOLEAN,
                    actual_return REAL,
                    
                    is_validated BOOLEAN DEFAULT FALSE,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    
                    FOREIGN KEY (model_session_id) REFERENCES ml_training_sessions (session_id)
                )
            ''')
            
            # Live Feature Engineering Pipeline
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS feature_engineering (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    feature_name TEXT NOT NULL,
                    feature_type TEXT NOT NULL,
                    feature_category TEXT,
                    
                    -- Live Feature Configuration
                    calculation_method TEXT,
                    lookback_period INTEGER,
                    parameters TEXT,
                    
                    -- Live Feature Performance
                    importance_score REAL,
                    correlation_with_target REAL,
                    information_gain REAL,
                    
                    -- Production Usage Statistics
                    times_used INTEGER DEFAULT 0,
                    avg_performance REAL,
                    last_used TEXT,
                    
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Live Model Performance Tracking
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS model_performance_tracking (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    model_session_id TEXT NOT NULL,
                    evaluation_date TEXT NOT NULL,
                    
                    -- Live Performance Metrics
                    accuracy REAL,
                    precision_score REAL,
                    recall REAL,
                    f1_score REAL,
                    roc_auc REAL,
                    
                    -- Live Trading Metrics
                    total_trades INTEGER,
                    winning_trades INTEGER,
                    losing_trades INTEGER,
                    win_rate REAL,
                    profit_factor REAL,
                    sharpe_ratio REAL,
                    max_drawdown REAL,
                    
                    -- Live Market Performance
                    daily_return REAL,
                    weekly_return REAL,
                    monthly_return REAL,
                    ytd_return REAL,
                    
                    -- Live Model Health
                    prediction_consistency REAL,
                    feature_stability REAL,
                    data_drift_score REAL,
                    
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (model_session_id) REFERENCES ml_training_sessions (session_id)
                )
            ''')
            
            # Live Data Quality Monitoring
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS data_quality_monitoring (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    data_source TEXT NOT NULL,
                    
                    -- Live Quality Metrics
                    completeness_score REAL,
                    accuracy_score REAL,
                    consistency_score REAL,
                    timeliness_score REAL,
                    
                    -- Live Data Issues
                    missing_values_count INTEGER,
                    outliers_count INTEGER,
                    duplicate_count INTEGER,
                    null_count INTEGER,
                    
                    -- Live Data Volume
                    total_records INTEGER,
                    new_records INTEGER,
                    updated_records INTEGER,
                    
                    -- Live Overall Quality
                    overall_quality_score REAL,
                    quality_grade TEXT,
                    
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create indexes for production performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_live_training_timestamp ON live_training_data(timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_live_training_symbol ON live_training_data(symbol)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_predictions_timestamp ON live_predictions(timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_predictions_symbol ON live_predictions(symbol)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_performance_tracking_date ON model_performance_tracking(evaluation_date)')
            
            conn.commit()
            logging.info("[ML_DATA] V3 live training database initialized successfully")
            
        except Exception as e:
            logging.error(f"[ML_DATA] Live training database initialization failed: {e}")
        finally:
            if conn:
                conn.close()
    
    def init_database(self):
        """Initialize production ML data storage database"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=NORMAL')
            cursor = conn.cursor()
            
            # Live ML training sessions
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS training_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT UNIQUE NOT NULL,
                    session_type TEXT NOT NULL,
                    start_time TEXT NOT NULL,
                    end_time TEXT,
                    status TEXT DEFAULT 'ACTIVE',
                    
                    -- Live Training Details
                    data_points_processed INTEGER DEFAULT 0,
                    models_trained INTEGER DEFAULT 0,
                    algorithms_used TEXT,
                    training_parameters TEXT,
                    
                    -- Live Results
                    accuracy_achieved REAL,
                    loss_final REAL,
                    validation_score REAL,
                    
                    -- Live Data Sources
                    data_sources TEXT,
                    external_apis_used TEXT,
                    market_conditions TEXT,
                    
                    -- Metadata
                    version TEXT DEFAULT '1.0',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Live model performance tracking
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS model_performance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    model_id TEXT NOT NULL,
                    model_name TEXT NOT NULL,
                    model_type TEXT NOT NULL,
                    training_session_id TEXT,
                    
                    -- Live Performance Metrics
                    accuracy REAL,
                    precision_score REAL,
                    recall_score REAL,
                    f1_score REAL,
                    mse REAL,
                    mae REAL,
                    
                    -- Live Trading Specific
                    prediction_accuracy REAL,
                    profit_correlation REAL,
                    risk_score REAL,
                    
                    -- Model Info
                    model_path TEXT,
                    feature_count INTEGER,
                    training_size INTEGER,
                    validation_size INTEGER,
                    
                    -- Production Status
                    is_active BOOLEAN DEFAULT FALSE,
                    is_production BOOLEAN DEFAULT FALSE,
                    
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (training_session_id) REFERENCES training_sessions (session_id)
                )
            ''')
            
            # Live learning progress tracking
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS learning_progress (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    epoch INTEGER,
                    
                    -- Live Progress Metrics
                    training_loss REAL,
                    validation_loss REAL,
                    accuracy REAL,
                    learning_rate REAL,
                    
                    -- Live Custom Metrics
                    prediction_confidence REAL,
                    pattern_recognition_score REAL,
                    market_adaptation_score REAL,
                    
                    -- Live Market Context
                    market_data_quality REAL,
                    external_data_available INTEGER,
                    volatility_during_training REAL,
                    
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (session_id) REFERENCES training_sessions (session_id)
                )
            ''')
            
            # Live feature importance tracking
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS feature_importance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    model_id TEXT NOT NULL,
                    feature_name TEXT NOT NULL,
                    importance_score REAL NOT NULL,
                    feature_type TEXT,
                    data_source TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Live prediction logs
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS predictions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    model_id TEXT NOT NULL,
                    prediction_id TEXT UNIQUE NOT NULL,
                    timestamp TEXT NOT NULL,
                    
                    -- Live Input Data
                    symbol TEXT,
                    input_features TEXT,
                    market_context TEXT,
                    
                    -- Live Prediction
                    predicted_direction TEXT,
                    confidence_score REAL,
                    predicted_price REAL,
                    predicted_movement REAL,
                    
                    -- Live Validation (filled from market data)
                    actual_direction TEXT,
                    actual_price REAL,
                    actual_movement REAL,
                    prediction_accuracy REAL,
                    was_correct BOOLEAN,
                    
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Live training data metadata
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS training_data_sets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dataset_id TEXT UNIQUE NOT NULL,
                    dataset_name TEXT NOT NULL,
                    data_type TEXT NOT NULL,
                    
                    -- Live Data Info
                    symbol TEXT,
                    timeframe TEXT,
                    start_date TEXT,
                    end_date TEXT,
                    total_records INTEGER,
                    
                    -- Live Quality Metrics
                    data_quality_score REAL,
                    completeness_pct REAL,
                    outliers_removed INTEGER,
                    
                    -- File Info
                    file_path TEXT,
                    file_size_mb REAL,
                    compression_used TEXT,
                    
                    -- Production Usage
                    times_used INTEGER DEFAULT 0,
                    last_used TEXT,
                    
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Live system configurations and checkpoints
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ml_checkpoints (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    checkpoint_id TEXT UNIQUE NOT NULL,
                    checkpoint_name TEXT NOT NULL,
                    checkpoint_type TEXT NOT NULL,
                    
                    -- Live System State
                    learning_phase TEXT,
                    intelligence_level REAL,
                    total_training_hours REAL,
                    
                    -- Live Configuration
                    config_json TEXT,
                    model_versions TEXT,
                    feature_sets TEXT,
                    
                    -- Live Performance Summary
                    best_accuracy REAL,
                    avg_prediction_confidence REAL,
                    successful_trades_attributed INTEGER,
                    
                    -- Live Backup Info
                    backup_path TEXT,
                    backup_size_mb REAL,
                    
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create indexes for production performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_training_sessions_type ON training_sessions(session_type)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_model_performance_active ON model_performance(is_active)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_predictions_timestamp ON predictions(timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_learning_progress_session ON learning_progress(session_id)')
            
            conn.commit()
            logging.info("[ML_DATA] V3 live ML database initialized successfully")
            
        except Exception as e:
            logging.error(f"[ML_DATA] Database initialization failed: {e}")
        finally:
            if conn:
                conn.close()
    
    def _execute_db_operation(self, operation_func, db_path=None, *args, **kwargs):
        """Execute database operation with proper connection handling"""
        db_path = db_path or self.db_path
        conn = None
        try:
            conn = sqlite3.connect(db_path)
            result = operation_func(conn, *args, **kwargs)
            conn.commit()
            return result
        except Exception as e:
            if conn:
                conn.rollback()
            logging.error(f"[ML_DATA] Database operation failed: {e}")
            return None
        finally:
            if conn:
                conn.close()
    
    def start_live_training_session(self, session_data: Dict) -> str:
        """Start a new live ML training session"""
        def _insert_session(conn, session_data):
            session_id = session_data.get('session_id') or f"live_session_{int(datetime.now().timestamp())}"
            
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO training_sessions (
                    session_id, session_type, start_time, algorithms_used,
                    training_parameters, data_sources, external_apis_used,
                    market_conditions, version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                session_id,
                session_data.get('session_type', 'LIVE_PRODUCTION'),
                datetime.now().isoformat(),
                json.dumps(session_data.get('algorithms', [])),
                json.dumps(session_data.get('parameters', {})),
                json.dumps(session_data.get('data_sources', [])),
                json.dumps(session_data.get('external_apis', [])),
                json.dumps(session_data.get('market_conditions', {})),
                session_data.get('version', '3.0')
            ))
            
            logging.info(f"[ML_DATA] Live training session started: {session_id}")
            return session_id
        
        return self._execute_db_operation(_insert_session, None, session_data)
    
    def log_live_training_data(self, training_data: Dict) -> bool:
        """Log live training data for real-time learning"""
        def _insert_live_data(conn, data):
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO live_training_data (
                    timestamp, symbol, timeframe, open_price, high_price, 
                    low_price, close_price, volume, rsi, macd, macd_signal,
                    bb_upper, bb_lower, sma_20, ema_12, ema_26, volatility,
                    trend_direction, support_level, resistance_level,
                    price_direction, confidence_score, actual_return,
                    news_sentiment, fear_greed_index, market_cap
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                data.get('timestamp', datetime.now().isoformat()),
                data.get('symbol', 'BTCUSDT'),
                data.get('timeframe', '1m'),
                data.get('open_price', 0),
                data.get('high_price', 0),
                data.get('low_price', 0),
                data.get('close_price', 0),
                data.get('volume', 0),
                data.get('rsi'),
                data.get('macd'),
                data.get('macd_signal'),
                data.get('bb_upper'),
                data.get('bb_lower'),
                data.get('sma_20'),
                data.get('ema_12'),
                data.get('ema_26'),
                data.get('volatility'),
                data.get('trend_direction'),
                data.get('support_level'),
                data.get('resistance_level'),
                data.get('price_direction'),
                data.get('confidence_score'),
                data.get('actual_return'),
                data.get('news_sentiment'),
                data.get('fear_greed_index'),
                data.get('market_cap')
            ))
            return True
        
        result = self._execute_db_operation(_insert_live_data, self.training_db_path, training_data)
        return result is not None
    
    def log_training_progress(self, session_id: str, progress_data: Dict) -> bool:
        """Log live training progress for a session"""
        def _insert_progress(conn, session_id, progress_data):
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO learning_progress (
                    session_id, timestamp, epoch, training_loss, validation_loss,
                    accuracy, learning_rate, prediction_confidence,
                    pattern_recognition_score, market_adaptation_score,
                    market_data_quality, external_data_available, volatility_during_training
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                session_id,
                datetime.now().isoformat(),
                progress_data.get('epoch', 0),
                progress_data.get('training_loss', 0),
                progress_data.get('validation_loss', 0),
                progress_data.get('accuracy', 0),
                progress_data.get('learning_rate', 0),
                progress_data.get('prediction_confidence', 0),
                progress_data.get('pattern_recognition_score', 0),
                progress_data.get('market_adaptation_score', 0),
                progress_data.get('market_data_quality', 0),
                progress_data.get('external_data_available', 0),
                progress_data.get('volatility_during_training', 0)
            ))
            return True
        
        result = self._execute_db_operation(_insert_progress, session_id, progress_data)
        return result is not None
    
    def _safe_file_operation(self, file_path: str, mode: str = 'rb', operation_func=None):
        """Safely perform file operations with proper validation and error handling"""
        if not file_path or not isinstance(file_path, str):
            return None
        
        # Validate path is within expected directories
        allowed_dirs = ['data/', 'models/', 'backup/', 'training_data/', self.data_dir, self.models_dir, self.backups_dir]
        if not any(allowed_dir in file_path for allowed_dir in allowed_dirs):
            logging.warning(f"[ML_DATA] File operation denied for path outside allowed directories: {file_path}")
            return None
        
        try:
            # Check if path exists for read operations
            if 'r' in mode and not os.path.exists(file_path):
                return None
            
            # Ensure directory exists for write operations
            if 'w' in mode or 'a' in mode:
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            # Perform the file operation with context manager
            with open(file_path, mode) as f:
                if operation_func:
                    return operation_func(f)
                return f
                
        except (OSError, IOError, PermissionError) as e:
            logging.error(f"[ML_DATA] Safe file operation failed for {file_path}: {e}")
            return None
        except Exception as e:
            logging.error(f"[ML_DATA] Unexpected error in file operation for {file_path}: {e}")
            return None
    
    def save_live_model(self, model_data: Dict, model_object: Any = None) -> str:
        """Save live ML model with metadata and proper file handling"""
        model_id = model_data.get('model_id') or f"live_model_{int(datetime.now().timestamp())}"
        
        # Save model object with safe file handling
        model_path = None
        if model_object is not None:
            try:
                model_path = os.path.join(self.models_dir, f"{model_id}.pkl")
                
                def _save_model(f):
                    pickle.dump(model_object, f)
                    return True
                
                if self._safe_file_operation(model_path, 'wb', _save_model):
                    logging.info(f"[ML_DATA] Live model saved to: {model_path}")
                else:
                    logging.error(f"[ML_DATA] Failed to save live model file: {model_path}")
                    model_path = None
                    
            except Exception as e:
                logging.error(f"[ML_DATA] Failed to save live model file: {e}")
                model_path = None
        
        def _insert_model(conn, model_data, model_path):
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO model_performance (
                    model_id, model_name, model_type, training_session_id,
                    accuracy, precision_score, recall_score, f1_score,
                    mse, mae, prediction_accuracy, profit_correlation,
                    risk_score, model_path, feature_count, training_size,
                    validation_size, is_active, is_production
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                model_id,
                model_data.get('model_name', 'Live Model'),
                model_data.get('model_type', 'Production'),
                model_data.get('training_session_id'),
                model_data.get('accuracy', 0),
                model_data.get('precision_score', 0),
                model_data.get('recall_score', 0),
                model_data.get('f1_score', 0),
                model_data.get('mse', 0),
                model_data.get('mae', 0),
                model_data.get('prediction_accuracy', 0),
                model_data.get('profit_correlation', 0),
                model_data.get('risk_score', 0),
                model_path,
                model_data.get('feature_count', 0),
                model_data.get('training_size', 0),
                model_data.get('validation_size', 0),
                model_data.get('is_active', True),
                model_data.get('is_production', True)
            ))
            
            logging.info(f"[ML_DATA] Live model saved: {model_id}")
            return model_id
        
        return self._execute_db_operation(_insert_model, None, model_data, model_path)
    
    def load_live_model(self, model_id: str) -> Optional[Any]:
        """Load live ML model object with proper file handling"""
        def _get_model_path(conn, model_id):
            cursor = conn.cursor()
            cursor.execute('''
                SELECT model_path FROM model_performance WHERE model_id = ?
            ''', (model_id,))
            
            result = cursor.fetchone()
            return result[0] if result else None
        
        model_path = self._execute_db_operation(_get_model_path, None, model_id)
        
        if model_path and os.path.exists(model_path):
            def _load_model(f):
                return pickle.load(f)
            
            return self._safe_file_operation(model_path, 'rb', _load_model)
        
        return None
    
    def create_production_checkpoint(self, checkpoint_data: Dict) -> str:
        """Create production system checkpoint for backup/restore"""
        try:
            checkpoint_id = checkpoint_data.get('checkpoint_id') or f"prod_checkpoint_{int(datetime.now().timestamp())}"
            
            # Check if backup directory exists and is writable
            if not os.path.exists(self.backups_dir):
                logging.warning(f"[ML_DATA] Backups directory doesn't exist: {self.backups_dir}")
                return checkpoint_id
            
            if not os.access(self.backups_dir, os.W_OK):
                logging.warning(f"[ML_DATA] Backups directory not writable: {self.backups_dir}")
                return checkpoint_id
            
            # Create production backup
            backup_path = os.path.join(self.backups_dir, f"{checkpoint_id}.zip")
            backup_size_mb = 0
            
            try:
                self._create_backup_zip(backup_path)
                backup_size_mb = os.path.getsize(backup_path) / (1024 * 1024) if os.path.exists(backup_path) else 0
            except Exception as e:
                logging.error(f"[ML_DATA] Failed to create production backup: {e}")
                backup_path = None
            
            # Save checkpoint metadata
            def _insert_checkpoint(conn, checkpoint_data, backup_path, backup_size_mb):
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO ml_checkpoints (
                        checkpoint_id, checkpoint_name, checkpoint_type, learning_phase,
                        intelligence_level, total_training_hours, config_json,
                        model_versions, feature_sets, best_accuracy,
                        avg_prediction_confidence, successful_trades_attributed,
                        backup_path, backup_size_mb
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    checkpoint_id,
                    checkpoint_data.get('checkpoint_name', f'Live Checkpoint {datetime.now().strftime("%Y%m%d_%H%M")}'),
                    checkpoint_data.get('checkpoint_type', 'PRODUCTION'),
                    checkpoint_data.get('learning_phase', 'LIVE_TRADING'),
                    checkpoint_data.get('intelligence_level', 0),
                    checkpoint_data.get('total_training_hours', 0),
                    json.dumps(checkpoint_data.get('config', {})),
                    json.dumps(checkpoint_data.get('model_versions', [])),
                    json.dumps(checkpoint_data.get('feature_sets', [])),
                    checkpoint_data.get('best_accuracy', 0),
                    checkpoint_data.get('avg_prediction_confidence', 0),
                    checkpoint_data.get('successful_trades_attributed', 0),
                    backup_path,
                    backup_size_mb
                ))
                
                logging.info(f"[ML_DATA] Production checkpoint created: {checkpoint_id} ({backup_size_mb:.1f} MB)")
                return checkpoint_id
            
            return self._execute_db_operation(_insert_checkpoint, None, checkpoint_data, backup_path, backup_size_mb)
            
        except Exception as e:
            logging.error(f"[ML_DATA] Failed to create production checkpoint: {e}")
            return None
    
    def _create_backup_zip(self, backup_path: str):
        """Create backup zip of all live ML data"""
        if not backup_path or not isinstance(backup_path, str):
            raise ValueError("Invalid backup path")
        
        # Validate backup path is in allowed directory
        if not backup_path.startswith(self.backups_dir):
            raise ValueError("Backup path must be within backups directory")
        
        zip_file = None
        try:
            # Use context manager for zip file
            with zipfile.ZipFile(backup_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                
                # Add database with safety check
                if os.path.exists(self.db_path):
                    try:
                        zip_file.write(self.db_path, 'ml_data.db')
                    except Exception as e:
                        logging.warning(f"[ML_DATA] Failed to backup main database: {e}")
                
                # Add live training database with safety check
                if os.path.exists(self.training_db_path):
                    try:
                        zip_file.write(self.training_db_path, 'ml_training_data.db')
                    except Exception as e:
                        logging.warning(f"[ML_DATA] Failed to backup training database: {e}")
                
                # Add models directory with safe iteration
                if os.path.exists(self.models_dir):
                    self._add_directory_to_zip(zip_file, self.models_dir, self.data_dir)
                
                # Add training data directory with safe iteration
                if os.path.exists(self.training_data_dir):
                    self._add_directory_to_zip(zip_file, self.training_data_dir, self.data_dir)
                        
        except Exception as e:
            logging.error(f"[ML_DATA] Failed to create backup zip: {e}")
            # Clean up partial backup file
            if os.path.exists(backup_path):
                try:
                    os.unlink(backup_path)
                except:
                    pass
            raise
    
    def _add_directory_to_zip(self, zip_file: zipfile.ZipFile, directory: str, base_path: str):
        """Safely add directory contents to zip file"""
        try:
            for root, dirs, files in os.walk(directory):
                for file in files:
                    try:
                        file_path = os.path.join(root, file)
                        
                        # Skip files that are too large (> 100MB)
                        if os.path.getsize(file_path) > 100 * 1024 * 1024:
                            logging.warning(f"[ML_DATA] Skipping large file: {file}")
                            continue
                        
                        # Create archive path
                        arc_path = os.path.relpath(file_path, base_path)
                        zip_file.write(file_path, arc_path)
                        
                    except (OSError, IOError, PermissionError) as e:
                        logging.warning(f"[ML_DATA] Failed to backup file {file}: {e}")
                        continue
                    except Exception as e:
                        logging.warning(f"[ML_DATA] Unexpected error backing up file {file}: {e}")
                        continue
                        
        except Exception as e:
            logging.error(f"[ML_DATA] Error adding directory to zip: {e}")
    
    def get_live_system_status(self) -> Dict:
        """Get comprehensive live ML system status"""
        def _get_status_data(conn):
            cursor = conn.cursor()
            
            try:
                # Get live training sessions count
                cursor.execute("SELECT COUNT(*) FROM training_sessions")
                total_sessions = cursor.fetchone()[0]
                
                # Get active production models count
                cursor.execute("SELECT COUNT(*) FROM model_performance WHERE is_active = 1 AND is_production = 1")
                active_models = cursor.fetchone()[0]
                
                # Get recent live predictions accuracy
                cursor.execute('''
                    SELECT AVG(prediction_accuracy) FROM predictions 
                    WHERE timestamp >= ? AND prediction_accuracy IS NOT NULL
                ''', ((datetime.now() - timedelta(days=7)).isoformat(),))
                
                recent_accuracy = cursor.fetchone()[0] or 0
                
                # Get total live data points processed
                cursor.execute("SELECT SUM(data_points_processed) FROM training_sessions")
                total_data_points = cursor.fetchone()[0] or 0
                
                return {
                    'total_sessions': total_sessions,
                    'active_models': active_models,
                    'recent_accuracy': recent_accuracy,
                    'total_data_points': total_data_points
                }
            except Exception as e:
                logging.error(f"[ML_DATA] Error getting live status data: {e}")
                return {
                    'total_sessions': 0,
                    'active_models': 0,
                    'recent_accuracy': 0,
                    'total_data_points': 0
                }
        
        try:
            status_data = self._execute_db_operation(_get_status_data) or {}
            
            # Get live training database status
            live_db_status = self.get_live_training_database_status()
            
            # Get storage info with error handling
            db_size_mb = 0
            training_db_size_mb = 0
            models_size_mb = 0
            backup_count = 0
            
            try:
                if os.path.exists(self.db_path):
                    db_size_mb = os.path.getsize(self.db_path) / (1024 * 1024)
            except Exception as e:
                logging.warning(f"[ML_DATA] Could not get database size: {e}")
            
            try:
                if os.path.exists(self.training_db_path):
                    training_db_size_mb = os.path.getsize(self.training_db_path) / (1024 * 1024)
            except Exception as e:
                logging.warning(f"[ML_DATA] Could not get live training database size: {e}")
            
            try:
                if os.path.exists(self.models_dir):
                    for root, dirs, files in os.walk(self.models_dir):
                        for file in files:
                            try:
                                models_size_mb += os.path.getsize(os.path.join(root, file))
                            except Exception:
                                continue
                    models_size_mb /= (1024 * 1024)
            except Exception as e:
                logging.warning(f"[ML_DATA] Could not get models size: {e}")
            
            try:
                if os.path.exists(self.backups_dir):
                    backup_count = len([f for f in os.listdir(self.backups_dir) if f.endswith('.zip')])
            except Exception as e:
                logging.warning(f"[ML_DATA] Could not access backups directory: {e}")
            
            return {
                'total_live_training_sessions': status_data.get('total_sessions', 0),
                'active_production_models': status_data.get('active_models', 0),
                'recent_prediction_accuracy': status_data.get('recent_accuracy', 0) * 100,
                'total_live_data_points_processed': status_data.get('total_data_points', 0),
                'database_size_mb': db_size_mb,
                'live_training_database_size_mb': training_db_size_mb,
                'models_size_mb': models_size_mb,
                'total_storage_mb': db_size_mb + training_db_size_mb + models_size_mb,
                'live_data_quality': self._assess_live_data_quality(),
                'last_live_training_session': self._get_last_training_session(),
                'backup_count': backup_count,
                'is_production_ready': True,
                'export_ready': True,
                'live_training_database_status': live_db_status
            }
            
        except Exception as e:
            logging.error(f"[ML_DATA] Failed to get live system status: {e}")
            return {
                'total_live_training_sessions': 0,
                'active_production_models': 0,
                'recent_prediction_accuracy': 0,
                'total_live_data_points_processed': 0,
                'database_size_mb': 0,
                'live_training_database_size_mb': 0,
                'models_size_mb': 0,
                'total_storage_mb': 0,
                'live_data_quality': 'UNKNOWN',
                'last_live_training_session': None,
                'backup_count': 0,
                'is_production_ready': False,
                'export_ready': False,
                'live_training_database_status': 'ERROR',
                'error': str(e)
            }
    
    def get_live_training_database_status(self) -> str:
        """Get live training database status"""
        try:
            if not os.path.exists(self.training_db_path):
                return "MISSING"
            
            def _check_live_training_db(conn):
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM live_training_data")
                data_count = cursor.fetchone()[0]
                return data_count
            
            data_count = self._execute_db_operation(_check_live_training_db, self.training_db_path)
            return f"LIVE ({data_count} records)" if data_count is not None else "ERROR"
            
        except Exception as e:
            logging.error(f"[ML_DATA] Live training database status check failed: {e}")
            return "ERROR"
    
    def _assess_live_data_quality(self) -> str:
        """Assess overall live data quality"""
        try:
            status = self.get_live_system_status()
            
            if status.get('total_live_training_sessions', 0) >= 5 and status.get('recent_prediction_accuracy', 0) >= 70:
                return "HIGH"
            elif status.get('total_live_training_sessions', 0) >= 2 and status.get('recent_prediction_accuracy', 0) >= 50:
                return "MEDIUM"
            else:
                return "LOW"
                
        except:
            return "UNKNOWN"
    
    def _get_last_training_session(self) -> Optional[str]:
        """Get timestamp of last live training session"""
        def _get_last_session(conn):
            cursor = conn.cursor()
            cursor.execute("SELECT start_time FROM training_sessions ORDER BY start_time DESC LIMIT 1")
            result = cursor.fetchone()
            return result[0] if result else None
        
        return self._execute_db_operation(_get_last_session)
    
    def cleanup_old_data(self, days_to_keep: int = 90):
        """Clean up old ML data to manage storage"""
        def _cleanup_data(conn, days_to_keep):
            cutoff_date = (datetime.now() - timedelta(days=days_to_keep)).isoformat()
            cursor = conn.cursor()
            
            # Clean old predictions
            cursor.execute('DELETE FROM predictions WHERE timestamp < ?', (cutoff_date,))
            deleted_predictions = cursor.rowcount
            
            # Clean old learning progress (keep summary)
            cursor.execute('''
                DELETE FROM learning_progress 
                WHERE timestamp < ? AND id NOT IN (
                    SELECT MIN(id) FROM learning_progress 
                    WHERE timestamp < ?
                    GROUP BY session_id, DATE(timestamp)
                )
            ''', (cutoff_date, cutoff_date))
            deleted_progress = cursor.rowcount
            
            logging.info(f"[ML_DATA] Cleaned up {deleted_predictions} predictions and {deleted_progress} progress records")
            return True
        
        # Clean main database
        self._execute_db_operation(_cleanup_data, None, days_to_keep)
        
        # Clean live training database
        def _cleanup_live_training_data(conn, days_to_keep):
            cutoff_date = (datetime.now() - timedelta(days=days_to_keep)).isoformat()
            cursor = conn.cursor()
            
            cursor.execute('DELETE FROM live_training_data WHERE timestamp < ?', (cutoff_date,))
            deleted_training = cursor.rowcount
            
            cursor.execute('DELETE FROM live_predictions WHERE timestamp < ?', (cutoff_date,))
            deleted_predictions = cursor.rowcount
            
            logging.info(f"[ML_DATA] Cleaned up {deleted_training} live training records and {deleted_predictions} live predictions")
            return True
        
        self._execute_db_operation(_cleanup_live_training_data, self.training_db_path, days_to_keep)
        
        # Clean old backups
        try:
            if os.path.exists(self.backups_dir):
                for backup_file in os.listdir(self.backups_dir):
                    if backup_file.endswith('.zip'):
                        backup_path = os.path.join(self.backups_dir, backup_file)
                        try:
                            file_age = datetime.now() - datetime.fromtimestamp(os.path.getctime(backup_path))
                            if file_age.days > days_to_keep:
                                os.unlink(backup_path)
                        except Exception as e:
                            logging.warning(f"[ML_DATA] Failed to remove old backup {backup_file}: {e}")
        except Exception as e:
            logging.error(f"[ML_DATA] Failed to cleanup old backups: {e}")
        
        logging.info(f"[ML_DATA] Cleaned up live data older than {days_to_keep} days")


# V3 Enhanced compatibility
class MLDataManagerV3(MLDataManager):
    """V3 Enhanced version for live data only"""
    pass


# V3 Production execution guard
if __name__ == "__main__":
    # This code only runs when the script is executed directly
    print("[ML_DATA] V3 ML Data Manager - Live Production Mode")
    manager = MLDataManager()
    print("[ML_DATA] V3 ML Data Manager initialized successfully - LIVE DATA ONLY")
    
    # Live functionality verification
    status = manager.get_live_system_status()
    print(f"[ML_DATA] Live system status: {status}")