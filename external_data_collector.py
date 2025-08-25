#!/usr/bin/env python3
"""
ENHANCED EXTERNAL DATA COLLECTOR - FIXED ASYNC VERSION
=====================================================
Fixed issues:
- Removed PRAW async warnings
- Fixed async task handling - removed .get() calls on asyncio Tasks
- Added proper session cleanup for aiohttp
- Enhanced error handling and rate limiting
"""

import warnings
import os
import sqlite3
import json
import time
import asyncio
import schedule
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from collections import defaultdict

# Suppress remaining warnings
warnings.filterwarnings("ignore", category=UserWarning, module="praw")
warnings.filterwarnings("ignore", message=".*asynchronous environment.*")

import requests
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import aiohttp

@dataclass
class APIQuota:
    """Track API usage quotas"""
    api_name: str
    requests_per_hour: int
    requests_per_day: int
    current_hour_count: int = 0
    current_day_count: int = 0
    last_reset_hour: datetime = None
    last_reset_day: datetime = None
    last_request_time: datetime = None

@dataclass
class DataRequest:
    """Queued data request"""
    request_id: str
    api_name: str
    symbol: str
    data_type: str
    timestamp: datetime
    priority: int = 3
    retry_count: int = 0
    max_retries: int = 3

class IntelligentAPIManager:
    """Embedded API management system"""
    
    def __init__(self):
        self.db_path = "data/api_management.db"
        os.makedirs("data", exist_ok=True)
        
        # Enhanced rate limits
        self.api_quotas = {
            'newsapi': APIQuota('newsapi', 50, 500),
            'twitter': APIQuota('twitter', 20, 100),
            'reddit': APIQuota('reddit', 30, 300),
            'alpha_vantage': APIQuota('alpha_vantage', 5, 100),
            'fred': APIQuota('fred', 60, 1000)
        }
        
        self.pending_requests = []
        self.failed_requests = []
        self.data_cache = {}
        self.init_database()
        self.load_quota_state()
        
    def init_database(self):
        """Initialize API management database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS api_quotas (
                        api_name TEXT PRIMARY KEY,
                        current_hour_count INTEGER,
                        current_day_count INTEGER,
                        last_reset_hour TEXT,
                        last_reset_day TEXT,
                        last_request_time TEXT
                    )
                ''')
                
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS pending_requests (
                        request_id TEXT PRIMARY KEY,
                        api_name TEXT,
                        symbol TEXT,
                        data_type TEXT,
                        timestamp TEXT,
                        priority INTEGER,
                        retry_count INTEGER
                    )
                ''')
                
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS collected_data (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        api_name TEXT,
                        symbol TEXT,
                        data_type TEXT,
                        timestamp TEXT,
                        data_json TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                conn.commit()
        except Exception as e:
            print(f"Database init error: {e}")
    
    def can_make_request(self, api_name: str) -> bool:
        """Check if we can make a request without hitting limits"""
        quota = self.api_quotas.get(api_name)
        if not quota:
            return True
        
        now = datetime.now()
        
        # Reset hourly counter if needed
        if quota.last_reset_hour is None or (now - quota.last_reset_hour).seconds >= 3600:
            quota.current_hour_count = 0
            quota.last_reset_hour = now
        
        # Reset daily counter if needed
        if quota.last_reset_day is None or (now - quota.last_reset_day).days >= 1:
            quota.current_day_count = 0
            quota.last_reset_day = now
        
        # Check limits
        if quota.current_hour_count >= quota.requests_per_hour:
            return False
        if quota.current_day_count >= quota.requests_per_day:
            return False
        
        # Minimum time between requests
        min_delay = {
            'newsapi': 5, 'twitter': 10, 'reddit': 3,
            'alpha_vantage': 15, 'fred': 2
        }
        
        if quota.last_request_time:
            elapsed = (now - quota.last_request_time).seconds
            required_delay = min_delay.get(api_name, 3)
            if elapsed < required_delay:
                return False
        
        return True
    
    def record_request(self, api_name: str):
        """Record that a request was made"""
        quota = self.api_quotas.get(api_name)
        if quota:
            quota.current_hour_count += 1
            quota.current_day_count += 1
            quota.last_request_time = datetime.now()
            self.save_quota_state()
    
    def queue_request(self, api_name: str, symbol: str, data_type: str, priority: int = 3):
        """Queue a data request for later processing"""
        request = DataRequest(
            request_id=f"{api_name}_{symbol}_{data_type}_{datetime.now().timestamp()}",
            api_name=api_name,
            symbol=symbol,
            data_type=data_type,
            timestamp=datetime.now(),
            priority=priority
        )
        self.pending_requests.append(request)
        self.save_pending_requests()
        return request.request_id
    
    def process_pending_requests(self) -> int:
        """Process pending requests respecting API limits"""
        processed = 0
        for request in self.pending_requests[:]:
            if self.can_make_request(request.api_name):
                self.pending_requests.remove(request)
                processed += 1
                if processed >= 3:
                    break
        
        if processed > 0:
            self.save_pending_requests()
        
        return processed
    
    def save_quota_state(self):
        """Save API quota state to database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                for quota in self.api_quotas.values():
                    conn.execute('''
                        INSERT OR REPLACE INTO api_quotas VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        quota.api_name, quota.current_hour_count, quota.current_day_count,
                        quota.last_reset_hour.isoformat() if quota.last_reset_hour else None,
                        quota.last_reset_day.isoformat() if quota.last_reset_day else None,
                        quota.last_request_time.isoformat() if quota.last_request_time else None
                    ))
                conn.commit()
        except Exception as e:
            print(f"Error saving quota state: {e}")
    
    def load_quota_state(self):
        """Load API quota state from database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute('SELECT * FROM api_quotas')
                for row in cursor.fetchall():
                    api_name = row[0]
                    if api_name in self.api_quotas:
                        quota = self.api_quotas[api_name]
                        quota.current_hour_count = row[1]
                        quota.current_day_count = row[2]
                        quota.last_reset_hour = datetime.fromisoformat(row[3]) if row[3] else None
                        quota.last_reset_day = datetime.fromisoformat(row[4]) if row[4] else None
                        quota.last_request_time = datetime.fromisoformat(row[5]) if row[5] else None
        except Exception as e:
            print(f"Error loading quota state: {e}")
    
    def save_pending_requests(self):
        """Save pending requests to database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('DELETE FROM pending_requests')
                for req in self.pending_requests:
                    conn.execute('''
                        INSERT INTO pending_requests VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        req.request_id, req.api_name, req.symbol, req.data_type,
                        req.timestamp.isoformat(), req.priority, req.retry_count
                    ))
                conn.commit()
        except Exception as e:
            print(f"Error saving pending requests: {e}")
    
    def get_status(self) -> Dict[str, Any]:
        """Get API manager status"""
        return {
            'pending_requests': len(self.pending_requests),
            'failed_requests': len(self.failed_requests),
            'api_quotas': {
                api_name: {
                    'hourly_used': f"{quota.current_hour_count}/{quota.requests_per_hour}",
                    'daily_used': f"{quota.current_day_count}/{quota.requests_per_day}",
                    'can_request': self.can_make_request(api_name)
                }
                for api_name, quota in self.api_quotas.items()
            }
        }

class ExternalDataCollector:
    """Enhanced external data collector with fixed async support"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Initialize intelligent API manager
        self.api_manager = IntelligentAPIManager()
        
        # Load API credentials
        self.alpha_vantage_key = os.getenv('ALPHA_VANTAGE_API_KEY')
        self.news_api_key = os.getenv('NEWS_API_KEY')
        self.fred_api_key = os.getenv('FRED_API_KEY')
        self.reddit_client_id = os.getenv('REDDIT_CLIENT_ID')
        self.reddit_client_secret = os.getenv('REDDIT_CLIENT_SECRET')
        self.reddit_user_agent = os.getenv('REDDIT_USER_AGENT', 'Trading Bot v1.0')
        self.twitter_bearer = os.getenv('TWITTER_BEARER_TOKEN')
        
        # Session management
        self.session = None
        self.reddit_client = None
        
        # Enhanced data caching
        self.data_cache = {}
        self.cache_duration = int(os.getenv('DEFAULT_CACHE_DURATION', 1800))
        
        # Track API status
        self.api_status = {
            'alpha_vantage': False,
            'news_api': False,
            'fred': False,
            'reddit': False,
            'twitter': False
        }
        
        # Background processing
        self.background_running = False
        self.start_background_processing()
        
        print("Enhanced External Data Collector initialized")
        print(f"Working APIs: {sum(self.api_status.values())}/5")
        print(f"Pending requests: {len(self.api_manager.pending_requests)}")
    
    async def _initialize_async_components(self):
        """Initialize async components"""
        try:
            # Create aiohttp session if not exists
            if not self.session or self.session.closed:
                connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)
                self.session = aiohttp.ClientSession(
                    connector=connector,
                    timeout=aiohttp.ClientTimeout(total=30),
                    headers={
                        'User-Agent': 'Enhanced-Trading-System/1.0',
                        'Accept': 'application/json'
                    }
                )
            
            # Initialize Reddit client (AsyncPRAW)
            await self._initialize_reddit_client()
            
            # Test all APIs
            await self._test_all_apis_async()
            
        except Exception as e:
            self.logger.error(f"Async component initialization failed: {e}")
    
    async def _initialize_reddit_client(self):
        """Initialize AsyncPRAW Reddit client"""
        try:
            if self.reddit_client_id and self.reddit_client_secret:
                # Try to import asyncpraw
                try:
                    import asyncpraw
                    
                    self.reddit_client = asyncpraw.Reddit(
                        client_id=self.reddit_client_id,
                        client_secret=self.reddit_client_secret,
                        user_agent=self.reddit_user_agent
                    )
                    
                    # Test the connection
                    subreddit = await self.reddit_client.subreddit("cryptocurrency")
                    async for submission in subreddit.hot(limit=1):
                        if submission:
                            self.api_status['reddit'] = True
                            break
                            
                except ImportError:
                    self.logger.warning("AsyncPRAW not installed. Install with: pip install asyncpraw")
                    # Fallback to synchronous PRAW in separate thread
                    await self._initialize_sync_reddit_fallback()
                    
        except Exception as e:
            self.logger.error(f"Reddit client initialization failed: {e}")
    
    async def _initialize_sync_reddit_fallback(self):
        """Fallback to sync PRAW in thread pool"""
        try:
            import praw
            import concurrent.futures
            
            def test_reddit_sync():
                reddit = praw.Reddit(
                    client_id=self.reddit_client_id,
                    client_secret=self.reddit_client_secret,
                    user_agent=self.reddit_user_agent
                )
                
                subreddit = reddit.subreddit("cryptocurrency")
                post = next(subreddit.hot(limit=1))
                return bool(post)
            
            # Run in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                result = await loop.run_in_executor(executor, test_reddit_sync)
                if result:
                    self.api_status['reddit'] = True
                    
        except Exception as e:
            self.logger.error(f"Sync Reddit fallback failed: {e}")
    
    def start_background_processing(self):
        """Start background request processing"""
        if self.background_running:
            return
        
        self.background_running = True
        
        def background_worker():
            while self.background_running:
                try:
                    processed = self.api_manager.process_pending_requests()
                    if processed > 0:
                        print(f"Processed {processed} pending API requests")
                    time.sleep(60)
                except Exception as e:
                    print(f"Background processing error: {e}")
                    time.sleep(120)
        
        thread = threading.Thread(target=background_worker, daemon=True)
        thread.start()
    
    async def _safe_request_async(self, url: str, headers: Dict = None, timeout: int = 20, api_name: str = None) -> Optional[Dict]:
        """Enhanced safe HTTP request using aiohttp"""
        try:
            if api_name and not self.api_manager.can_make_request(api_name):
                print(f"Rate limit hit for {api_name} - queuing request")
                return None
            
            if not self.session or self.session.closed:
                await self._initialize_async_components()
            
            async with self.session.get(url, headers=headers or {}) as response:
                if response.status == 429:
                    print(f"Rate limited by {api_name or 'API'}")
                    if api_name:
                        self.api_manager.queue_request(api_name, 'unknown', 'retry', priority=1)
                    return None
                elif response.status == 200:
                    if api_name:
                        self.api_manager.record_request(api_name)
                    return await response.json()
                else:
                    self.logger.warning(f"HTTP {response.status} for {url}")
                    return None
                    
        except asyncio.TimeoutError:
            self.logger.warning(f"Timeout for {url}")
            return None
        except Exception as e:
            self.logger.error(f"Request error for {url}: {e}")
            return None
    
    async def _test_all_apis_async(self):
        """Test all API connections asynchronously"""
        print("Testing ALL your API credentials with async support...")
        
        test_delay = 2  # Reduced delay for async operations
        
        # Test Alpha Vantage
        if self.alpha_vantage_key:
            try:
                url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=MSFT&apikey={self.alpha_vantage_key}"
                data = await self._safe_request_async(url, timeout=20, api_name='alpha_vantage')
                if data and ('Global Quote' in data or 'Information' in data):
                    if 'Information' in data and 'call frequency' in data['Information']:
                        print("  Alpha Vantage: Failed - API call frequency limit")
                    else:
                        self.api_status['alpha_vantage'] = True
                        print("  Alpha Vantage: Working")
                else:
                    print("  Alpha Vantage: Failed")
            except Exception as e:
                print(f"  Alpha Vantage: {e}")
            
            await asyncio.sleep(test_delay)
        
        # Test News API
        if self.news_api_key:
            try:
                url = f"https://newsapi.org/v2/everything?q=bitcoin&apiKey={self.news_api_key}&pageSize=1"
                data = await self._safe_request_async(url, timeout=15, api_name='newsapi')
                if data and 'articles' in data:
                    self.api_status['news_api'] = True
                    print("  News API: Working")
                else:
                    print("  News API: Failed")
            except Exception as e:
                print(f"  News API: {e}")
            
            await asyncio.sleep(test_delay)
        
        # Test FRED
        if self.fred_api_key:
            try:
                url = f"https://api.stlouisfed.org/fred/series/observations?series_id=GDP&api_key={self.fred_api_key}&file_type=json&limit=1"
                data = await self._safe_request_async(url, timeout=15, api_name='fred')
                if data and 'observations' in data:
                    self.api_status['fred'] = True
                    print("  FRED Economic Data: Working")
                else:
                    print("  FRED: Failed")
            except Exception as e:
                print(f"  FRED: {e}")
            
            await asyncio.sleep(test_delay)
        
        # Reddit already tested during initialization
        if self.api_status['reddit']:
            print("  Reddit: Working")
        else:
            print("  Reddit: Failed")
        
        # Test Twitter
        if self.twitter_bearer:
            try:
                headers = {'Authorization': f'Bearer {self.twitter_bearer}'}
                url = "https://api.twitter.com/2/tweets/search/recent?query=bitcoin&max_results=10"
                data = await self._safe_request_async(url, headers=headers, timeout=15, api_name='twitter')
                if data and 'data' in data:
                    self.api_status['twitter'] = True
                    print("  Twitter: Working")
                else:
                    print("  Twitter: Failed")
            except Exception as e:
                print(f"  Twitter: {e}")
    
    def collect_comprehensive_market_data(self, symbol="BTC", force_refresh=False):
        """Main data collection method - FIXED TO RETURN ACTUAL DATA"""
        # Run the async collection and return the result synchronously
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If we're already in an event loop, create a task
                return asyncio.create_task(self._collect_comprehensive_market_data_async(symbol, force_refresh))
            else:
                # If no event loop is running, run it synchronously
                return loop.run_until_complete(self._collect_comprehensive_market_data_async(symbol, force_refresh))
        except RuntimeError:
            # No event loop, create one
            return asyncio.run(self._collect_comprehensive_market_data_async(symbol, force_refresh))
    
    async def _collect_comprehensive_market_data_async(self, symbol="BTC", force_refresh=False):
        """Enhanced async data collection"""
        # Check cache first
        cache_key = f"comprehensive_{symbol}"
        if not force_refresh and cache_key in self.data_cache:
            cache_time, cached_data = self.data_cache[cache_key]
            if (datetime.now() - cache_time).seconds < self.cache_duration:
                print(f"Using cached data for {symbol}")
                return cached_data
        
        # Initialize async components if needed
        if not self.session or self.session.closed:
            await self._initialize_async_components()
        
        collected_data = {
            'timestamp': datetime.now().isoformat(),
            'symbol': symbol,
            'data_sources': [],
            'api_manager_status': self.api_manager.get_status()
        }
        
        api_delay = 2  # Reduced for async
        
        # 1. Alpha Vantage
        if self.api_status['alpha_vantage']:
            if self.api_manager.can_make_request('alpha_vantage'):
                try:
                    av_data = await self._get_alpha_vantage_data_async(symbol)
                    if av_data:
                        collected_data['alpha_vantage'] = av_data
                        collected_data['data_sources'].append('alpha_vantage')
                except Exception as e:
                    self.logger.warning(f"Alpha Vantage collection failed: {e}")
            else:
                self.api_manager.queue_request('alpha_vantage', symbol, 'price_data', priority=2)
                print(f"Alpha Vantage request queued for {symbol}")
            
            await asyncio.sleep(api_delay)
        
        # 2. News sentiment
        if self.api_status['news_api']:
            if self.api_manager.can_make_request('newsapi'):
                try:
                    news_data = await self._get_news_sentiment_async(symbol)
                    if news_data:
                        collected_data['news_sentiment'] = news_data
                        collected_data['data_sources'].append('news_api')
                except Exception as e:
                    self.logger.warning(f"News collection failed: {e}")
            else:
                self.api_manager.queue_request('newsapi', symbol, 'news_sentiment', priority=1)
                print(f"News API request queued for {symbol}")
            
            await asyncio.sleep(api_delay)
        
        # 3. Economic indicators
        if self.api_status['fred']:
            if self.api_manager.can_make_request('fred'):
                try:
                    econ_data = await self._get_economic_indicators_async()
                    if econ_data:
                        collected_data['economic_data'] = econ_data
                        collected_data['data_sources'].append('fred')
                except Exception as e:
                    self.logger.warning(f"FRED collection failed: {e}")
            else:
                self.api_manager.queue_request('fred', 'USD', 'economic_indicators', priority=3)
            
            await asyncio.sleep(api_delay)
        
        # 4. Social media sentiment (Reddit)
        if self.api_status['reddit']:
            if self.api_manager.can_make_request('reddit'):
                try:
                    reddit_data = await self._get_reddit_sentiment_async(symbol)
                    if reddit_data:
                        collected_data['reddit_sentiment'] = reddit_data
                        collected_data['data_sources'].append('reddit')
                except Exception as e:
                    self.logger.warning(f"Reddit collection failed: {e}")
            else:
                self.api_manager.queue_request('reddit', symbol, 'social_sentiment', priority=2)
            
            await asyncio.sleep(api_delay)
        
        # 5. Twitter sentiment
        if self.api_status['twitter']:
            if self.api_manager.can_make_request('twitter'):
                try:
                    twitter_data = await self._get_twitter_sentiment_async(symbol)
                    if twitter_data:
                        collected_data['twitter_sentiment'] = twitter_data
                        collected_data['data_sources'].append('twitter')
                except Exception as e:
                    self.logger.warning(f"Twitter collection failed: {e}")
            else:
                self.api_manager.queue_request('twitter', symbol, 'social_sentiment', priority=2)
        
        # Cache the results
        self.data_cache[cache_key] = (datetime.now(), collected_data)
        
        print(f"Collected data from {len(collected_data['data_sources'])} sources")
        print(f"Pending requests: {len(self.api_manager.pending_requests)}")
        
        return collected_data
    
    async def _get_alpha_vantage_data_async(self, symbol):
        """Enhanced Alpha Vantage data collection with async"""
        try:
            if symbol in ['BTC', 'BITCOIN']:
                url = f"https://www.alphavantage.co/query?function=DIGITAL_CURRENCY_DAILY&symbol=BTC&market=USD&apikey={self.alpha_vantage_key}"
            else:
                url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={self.alpha_vantage_key}"
            
            data = await self._safe_request_async(url, timeout=20, api_name='alpha_vantage')
            if not data:
                return None
            
            # Check for rate limit message
            if 'Information' in data and 'call frequency' in data['Information']:
                print(f"Alpha Vantage rate limit: {data['Information']}")
                return None
            
            if 'Global Quote' in data:
                quote = data['Global Quote']
                return {
                    'price': float(quote.get('05. price', 0)),
                    'change_percent': float(quote.get('10. change percent', '0%').replace('%', '')),
                    'volume': float(quote.get('06. volume', 0))
                }
            elif 'Time Series (Digital Currency Daily)' in data:
                series = data['Time Series (Digital Currency Daily)']
                latest_date = max(series.keys())
                latest = series[latest_date]
                return {
                    'price': float(latest.get('4a. close (USD)', 0)),
                    'volume': float(latest.get('5. volume', 0)),
                    'market_cap': float(latest.get('6. market cap (USD)', 0))
                }
            
            return None
        except Exception as e:
            self.logger.error(f"Alpha Vantage error: {e}")
            return None
    
    async def _get_news_sentiment_async(self, symbol):
        """Enhanced async news sentiment"""
        try:
            search_terms = {
                'BTC': 'bitcoin OR cryptocurrency',
                'ETH': 'ethereum OR crypto',
                'BITCOIN': 'bitcoin OR cryptocurrency'
            }
            
            query = search_terms.get(symbol, symbol if symbol else "bitcoin")
            url = f"https://newsapi.org/v2/everything?q={query}&apiKey={self.news_api_key}&pageSize=20&sortBy=publishedAt"
            
            data = await self._safe_request_async(url, timeout=20, api_name='newsapi')
            if not data or 'articles' not in data:
                return None
            
            articles = data['articles']
            
            # Enhanced sentiment analysis
            positive_words = ['bullish', 'rise', 'gain', 'up', 'positive', 'growth', 'bull', 'surge', 'rally', 'breakthrough']
            negative_words = ['bearish', 'fall', 'drop', 'down', 'negative', 'crash', 'bear', 'decline', 'sell-off', 'plunge']
            
            sentiment_scores = []
            volatility_count = 0
            
            for article in articles[:10]:
                text = (article.get('title', '') + ' ' + article.get('description', '')).lower()
                
                pos_count = sum(1 for word in positive_words if word in text)
                neg_count = sum(1 for word in negative_words if word in text)
                
                # Check for volatility keywords
                volatility_words = ['volatile', 'volatility', 'swing', 'sudden', 'sharp']
                if any(word in text for word in volatility_words):
                    volatility_count += 1
                
                if pos_count > neg_count:
                    sentiment_scores.append(1)
                elif neg_count > pos_count:
                    sentiment_scores.append(-1)
                else:
                    sentiment_scores.append(0)
            
            avg_sentiment = sum(sentiment_scores) / len(sentiment_scores) if sentiment_scores else 0
            
            return {
                'sentiment_score': avg_sentiment,
                'articles_analyzed': len(sentiment_scores),
                'total_articles': len(articles),
                'volatility_mentions': volatility_count,
                'data_freshness': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"News API error: {e}")
            return None
    
    async def _get_economic_indicators_async(self):
        """Enhanced async economic indicators"""
        try:
            indicators = {
                'GDP': 'GDP',
                'unemployment': 'UNRATE',
                'inflation': 'CPIAUCSL',
                'interest_rate': 'FEDFUNDS'
            }
            
            econ_data = {}
            for name, series_id in indicators.items():
                try:
                    url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={self.fred_api_key}&file_type=json&limit=1&sort_order=desc"
                    data = await self._safe_request_async(url, timeout=15, api_name='fred')
                    
                    if data and 'observations' in data and data['observations']:
                        latest = data['observations'][0]
                        if latest['value'] != '.':
                            econ_data[name] = float(latest['value'])
                except Exception as e:
                    self.logger.warning(f"Failed to get {name}: {e}")
                
                await asyncio.sleep(0.5)
            
            return econ_data if econ_data else None
        except Exception as e:
            self.logger.error(f"FRED API error: {e}")
            return None
    
    async def _get_reddit_sentiment_async(self, symbol):
        """Enhanced async Reddit sentiment"""
        try:
            if self.reddit_client:
                # Use AsyncPRAW
                subreddit = await self.reddit_client.subreddit("cryptocurrency")
                search_terms = {
                    'BTC': 'bitcoin',
                    'ETH': 'ethereum',
                    'BITCOIN': 'bitcoin'
                }
                
                search_term = search_terms.get(symbol, symbol.lower())
                sentiment_scores = []
                
                async for submission in subreddit.search(search_term, limit=5):
                    if submission.score > 10:
                        sentiment_scores.append(1)
                    elif submission.score < -5:
                        sentiment_scores.append(-1)
                    else:
                        sentiment_scores.append(0)
                
                if sentiment_scores:
                    avg_sentiment = sum(sentiment_scores) / len(sentiment_scores)
                    return {
                        'sentiment_score': avg_sentiment,
                        'posts_analyzed': len(sentiment_scores),
                        'data_freshness': datetime.now().isoformat()
                    }
            
            return None
        except Exception as e:
            self.logger.error(f"Reddit error: {e}")
            return None
    
    async def _get_twitter_sentiment_async(self, symbol):
        """Enhanced async Twitter sentiment"""
        try:
            search_terms = {
                'BTC': 'bitcoin OR $BTC',
                'ETH': 'ethereum OR $ETH',
                'BITCOIN': 'bitcoin OR cryptocurrency'
            }
            
            query = search_terms.get(symbol, symbol if symbol else "bitcoin")
            headers = {'Authorization': f'Bearer {self.twitter_bearer}'}
            url = f"https://api.twitter.com/2/tweets/search/recent?query={query}&max_results=30"
            
            data = await self._safe_request_async(url, headers=headers, timeout=20, api_name='twitter')
            if not data or 'data' not in data:
                return None
            
            tweets = data['data']
            
            positive_words = ['bullish', 'moon', 'pump', 'buy', 'hodl', 'bull', 'gem', 'rocket']
            negative_words = ['bearish', 'dump', 'sell', 'crash', 'bear', 'rekt', 'rugpull', 'scam']
            
            sentiment_scores = []
            for tweet in tweets:
                text = tweet.get('text', '').lower()
                
                pos_count = sum(1 for word in positive_words if word in text)
                neg_count = sum(1 for word in negative_words if word in text)
                
                if pos_count > neg_count:
                    sentiment_scores.append(1)
                elif neg_count > pos_count:
                    sentiment_scores.append(-1)
                else:
                    sentiment_scores.append(0)
            
            if sentiment_scores:
                avg_sentiment = sum(sentiment_scores) / len(sentiment_scores)
                return {
                    'sentiment_score': avg_sentiment,
                    'tweets_analyzed': len(sentiment_scores),
                    'data_freshness': datetime.now().isoformat()
                }
            
            return None
        except Exception as e:
            self.logger.error(f"Twitter error: {e}")
            return None
    
    # Enhanced public interface methods
    def get_latest_data(self):
        """Get latest comprehensive data - FIXED"""
        # Return the actual result synchronously
        return self.collect_comprehensive_market_data('BTC')
    
    def get_api_status(self):
        """Get enhanced API status including rate limiting info"""
        base_status = {
            'total_apis': len(self.api_status),
            'working_apis': sum(self.api_status.values()),
            'api_details': self.api_status,
            'data_quality': 'HIGH' if sum(self.api_status.values()) >= 3 else 'MEDIUM' if sum(self.api_status.values()) >= 2 else 'LOW'
        }
        
        base_status.update(self.api_manager.get_status())
        return base_status
    
    def get_pending_requests_count(self) -> int:
        """Get number of pending requests"""
        return len(self.api_manager.pending_requests)
    
    def force_process_pending(self) -> int:
        """Force process pending requests"""
        return self.api_manager.process_pending_requests()
    
    def cleanup(self):
        """Enhanced cleanup with proper session management"""
        try:
            self.background_running = False
            self.api_manager.save_quota_state()
            self.api_manager.save_pending_requests()
            
            # Close aiohttp session
            if self.session and not self.session.closed:
                asyncio.create_task(self.session.close())
            
            # Close Reddit client
            if self.reddit_client:
                if hasattr(self.reddit_client, 'close'):
                    asyncio.create_task(self.reddit_client.close())
                    
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
    
    def __del__(self):
        """Ensure cleanup on destruction"""
        self.cleanup()

# Test the enhanced collector
if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    
    async def test_collector():
        collector = ExternalDataCollector()
        
        try:
            print("\nTesting enhanced async data collection...")
            data = await collector._collect_comprehensive_market_data_async('BTC')
            
            print(f"\nData collected from {len(data.get('data_sources', []))} sources:")
            for source in data.get('data_sources', []):
                print(f"  {source}")
            
            print(f"\nAPI Status:")
            status = collector.get_api_status()
            print(f"  Working APIs: {status['working_apis']}/{status['total_apis']}")
            print(f"  Pending requests: {status['pending_requests']}")
            print(f"  Data Quality: {status['data_quality']}")
        
        finally:
            collector.cleanup()
    
    asyncio.run(test_collector())