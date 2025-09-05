# V3 TRADING SYSTEM - ENHANCED REQUIREMENTS
# ==========================================
# Complete dependency list with fixes for Flask integration,
# UTF-8 encoding support, and 8 vCPU / 24GB RAM optimization

# CORE PYTHON UTILITIES - ENHANCED
# =================================
python-dotenv>=1.0.0
psutil>=5.9.5
click>=8.1.6
tqdm>=4.65.0
rich>=13.4.2

# ENHANCED FLASK WEB SERVER INTEGRATION (CRITICAL FIX)
# ====================================================
Flask>=2.3.2
Flask-CORS>=4.0.0
Flask-Limiter>=3.0.0
Flask-SocketIO>=5.3.4
Flask-Compress>=1.13
Werkzeug>=2.3.6

# HTTP REQUESTS AND API INTEGRATION (FIXES "FAILED TO FETCH")
# ===========================================================
requests>=2.31.0
urllib3>=1.26.0,<2.0.0
aiohttp>=3.8.5
httpx>=0.24.0
certifi>=2023.7.22

# ENHANCED ERROR HANDLING AND RETRY MECHANISMS
# ============================================
tenacity>=8.2.0
backoff>=2.2.0
retrying>=1.3.4
timeout-decorator>=0.5.0

# SCIENTIFIC COMPUTING & DATA ANALYSIS - UTF-8 OPTIMIZED
# ======================================================
numpy>=1.24.0,<2.0.0
pandas>=2.0.0,<3.0.0
scipy>=1.10.0
matplotlib>=3.7.0
seaborn>=0.12.0
plotly>=5.15.0
statsmodels>=0.14.0

# MACHINE LEARNING & AI - ENHANCED FOR REAL DATA
# ==============================================
scikit-learn>=1.3.0
tensorflow>=2.13.0,<2.16.0
torch>=2.0.0
xgboost>=1.7.0
lightgbm>=4.0.0
catboost>=1.2.0
joblib>=1.3.0

# FINANCIAL & TRADING APIs - REAL DATA INTEGRATION
# ================================================
python-binance>=1.0.19
ccxt>=4.0.0
yfinance>=0.2.21
pandas-ta>=0.3.14b0
ta>=0.10.2
mplfinance>=0.12.9b0

# EXTERNAL DATA SOURCES - API ROTATION SUPPORT
# ============================================
alpha-vantage>=2.3.1
fredapi>=0.5.1
newsapi-python>=0.2.6
tweepy>=4.14.0
praw>=7.7.1
asyncpraw>=7.7.1

# REAL-TIME & ASYNC - ENHANCED FOR 8 vCPU OPTIMIZATION
# ====================================================
websocket-client>=1.6.1
websockets>=11.0.3
asyncio-mqtt>=0.13.0
aiodns>=3.0.0
aiofiles>=23.1.0
asyncio-throttle>=1.0.2
aiohttp-session>=2.12.0

# DATABASE & STORAGE - UTF-8 AND CONNECTION POOLING
# =================================================
sqlalchemy>=2.0.0
sqlite-utils>=3.34.0
aiosqlite>=0.19.0
sqlite3worker>=1.1.7

# CONFIGURATION & SERIALIZATION - UTF-8 SUPPORT
# ==============================================
pyyaml>=6.0.1
toml>=0.10.2
pydantic>=2.0.0
marshmallow>=3.20.0
jsonschema>=4.19.0
python-box>=7.1.1

# LOGGING & MONITORING - UTF-8 ENHANCED
# =====================================
loguru>=0.7.0
structlog>=23.1.0
coloredlogs>=15.0
python-json-logger>=2.0.7

# TIME & DATE UTILITIES
# ====================
pytz>=2023.3
python-dateutil>=2.8.2
arrow>=1.2.3

# MEMORY & PERFORMANCE - CRITICAL FOR 24GB RAM OPTIMIZATION
# =========================================================
memory-profiler>=0.61.0
line-profiler>=4.1.0
pympler>=0.9
objgraph>=3.6.0
tracemalloc2>=1.0.0

# CONCURRENT PROCESSING - 8 vCPU OPTIMIZATION
# ===========================================
concurrent-futures>=3.1.1
multiprocessing-logging>=0.3.4
billiard>=4.1.0

# PROGRESS TRACKING & UI - DASHBOARD ENHANCEMENT
# ==============================================
alive-progress>=3.1.4
progressbar2>=4.2.0
click-progressbar>=1.3.0
progress>=1.6
enlighten>=1.12.2

# CACHING & PERFORMANCE OPTIMIZATION
# ==================================
diskcache>=5.6.0
cachetools>=5.3.0
lru-dict>=1.2.0

# TEXT PROCESSING & SENTIMENT - UTF-8 SAFE
# ========================================
textblob>=0.17.1
nltk>=3.8.0
beautifulsoup4>=4.12.2
lxml>=4.9.0
vaderSentiment>=3.3.2

# SECURITY & AUTHENTICATION - ENHANCED
# ====================================
cryptography>=41.0.0
bcrypt>=4.0.0
PyJWT>=2.8.0
passlib>=1.7.4

# TESTING & DEVELOPMENT - ENHANCED TESTING SUITE
# ==============================================
pytest>=7.4.0
pytest-asyncio>=0.21.0
mock>=5.1.0
hypothesis>=6.82.0
factory-boy>=3.3.0
freezegun>=1.2.2
faker>=19.3.0

# DATA PROCESSING & ANALYSIS - REAL DATA OPTIMIZED
# ================================================
numba>=0.57.0
bottleneck>=1.3.7
tables>=3.8.0
pyarrow>=12.0.0
fastparquet>=2023.7.0

# API MANAGEMENT & RATE LIMITING - ENHANCED
# =========================================
schedule>=1.2.0
ratelimiter>=1.2.0
async-timeout>=4.0.2
limits>=3.5.0
slowapi>=0.1.9

# BACKGROUND PROCESSING & MONITORING
# ==================================
APScheduler>=3.10.0
watchdog>=3.0.0

# SYSTEM MONITORING AND HEALTH CHECKS
# ===================================
py-healthcheck>=1.10.1

# UNICODE AND ENCODING SUPPORT - UTF-8 FIXES
# ===========================================
chardet>=5.1.0
charset-normalizer>=3.2.0
unicodedata2>=15.0.0

# ERROR TRACKING & DEBUGGING - PRODUCTION READY
# ==============================================
sentry-sdk>=1.29.0

# NETWORKING AND PROTOCOLS
# ========================
dnspython>=2.4.0
idna>=3.4

# COMPRESSION AND ARCHIVING
# =========================
zstandard>=0.21.0
lz4>=4.3.2

# MATHEMATICAL OPERATIONS - ENHANCED
# ==================================
sympy>=1.12
mpmath>=1.3.0

# FILE PROCESSING - UTF-8 SAFE
# ============================
python-magic>=0.4.27
mimetypes-utils>=1.0.0

# DEVELOPMENT TOOLS
# ================
wheel>=0.41.0
setuptools>=68.0.0
pip>=23.2.0

# OPTIONAL JUPYTER SUPPORT (FOR ANALYSIS)
# =======================================
jupyter>=1.0.0
ipython>=8.0.0

# INSTALLATION NOTES FOR ENHANCED V3 SYSTEM:
# ==========================================
# 
# CRITICAL DEPENDENCIES (install first):
#   pip install --upgrade pip setuptools wheel
#   pip install psutil>=5.9.5 numpy>=1.24.0 pandas>=2.0.0
#   pip install Flask>=2.3.2 Flask-CORS>=4.0.0
#
# FLASK INTEGRATION (fixes "failed to fetch" errors):
#   pip install Flask Flask-CORS requests aiohttp
#
# UTF-8 ENCODING SUPPORT:
#   pip install chardet charset-normalizer unicodedata2
#
# PERFORMANCE OPTIMIZATION (8 vCPU / 24GB RAM):
#   pip install memory-profiler pympler concurrent-futures
#
# REAL DATA INTEGRATION:
#   pip install python-binance ccxt yfinance alpha-vantage
#
# API ROTATION SYSTEM:
#   pip install newsapi-python fredapi tweepy praw
#
# TESTING SUITE ENHANCEMENT:
#   pip install pytest pytest-asyncio mock hypothesis
#
# COMPLETE INSTALLATION:
#   pip install -r requirements.txt
#
# SYSTEM REQUIREMENTS:
#   - Python 3.8+ (recommended: 3.11+)
#   - RAM: 8GB minimum, 24GB+ recommended
#   - CPU: 4+ cores (optimized for 8 vCPU)
#   - Storage: 10GB+ free space
#   - Network: Stable internet for real data feeds
#
# TROUBLESHOOTING:
#   - If installation fails, upgrade pip: python -m pip install --upgrade pip
#   - For Windows: pip install --only-binary=all -r requirements.txt
#   - For macOS/Linux with compilation issues: use conda instead of pip
#   - UTF-8 issues: ensure system locale is set to UTF-8
#
# VERIFICATION:
#   After installation, run: python test.py
#   This will verify all dependencies and system integration
#
# DEVELOPMENT ENVIRONMENT:
#   pip install pytest black flake8 mypy
#
# PRODUCTION DEPLOYMENT:
#   pip install gunicorn supervisor sentry-sdk