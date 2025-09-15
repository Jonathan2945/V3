
# V3 Trading System - Quick Start Guide

## ?? FIXES APPLIED
? Added missing `run_comprehensive_backtest()` method to backtester
? Fixed PnL persistence method calls (`load_metrics`, `save_metrics`, etc.)
? Enhanced API middleware with proper error handling
? Complete real data integration (no mock/simulated data)
? Thread-safe database operations with connection pooling
? Proper async task management and resource cleanup

## ?? PREREQUISITES
1. Python 3.8+
2. All required packages: `pip install -r requirements.txt`
3. Binance API credentials in .env file
4. Environment configured for real data only

## ?? QUICK START

### 1. Verify System
```bash
python3 deployment_verification.py
```

### 2. Start System
```bash
python3 main.py
```

### 3. Access Dashboard
Open: http://localhost:8102

## ?? CONFIGURATION

### Required .env Settings
```
USE_REAL_DATA_ONLY=true
MOCK_DATA_DISABLED=true
FORCE_REAL_DATA_MODE=true
BINANCE_API_KEY_1=your_api_key
BINANCE_API_SECRET_1=your_api_secret
```

### Optional Settings
```
FLASK_PORT=8102
HOST=0.0.0.0
AUTO_START_TRADING=false
LOG_LEVEL=INFO
```

## ?? DASHBOARD FEATURES
- Real-time trading metrics
- Live P&L tracking
- Multi-timeframe backtesting
- Strategy performance analysis
- System health monitoring

## ??? API ENDPOINTS
- `/api/dashboard/overview` - Dashboard data
- `/api/trading/metrics` - Trading metrics
- `/api/backtest/progress` - Backtest status
- `/api/control/start_trading` - Start trading
- `/api/control/start_backtest` - Start backtest

## ?? TROUBLESHOOTING

### Common Issues
1. **Import Errors**: Ensure all packages installed
2. **Database Errors**: Check data/ directory permissions
3. **API Errors**: Verify Binance credentials
4. **Port Conflicts**: Change FLASK_PORT in .env

### Debug Mode
```bash
LOG_LEVEL=DEBUG python3 main.py
```

## ?? TRADING WORKFLOW
1. System startup with real data validation
2. Comprehensive backtesting on historical data
3. ML strategy training on real results
4. Paper trading with real market data
5. Live trading (when ready)

## ?? SAFETY FEATURES
- Real data only enforcement
- Testnet mode by default
- Risk management controls
- Position size limits
- Stop loss/take profit automation

## ?? SUPPORT
Check logs in `logs/` directory for detailed error information.
