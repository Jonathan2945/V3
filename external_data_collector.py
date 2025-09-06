#!/usr/bin/env python3
"""
V3 EXTERNAL DATA COLLECTOR - LIVE DATA ONLY - NO FALLBACK DATA
=============================================================
Fixed Issues:
- Fixed News API None value concatenation error
- Removed all fallback/sample data
- Only returns real data from APIs
- Proper error handling without fake data
"""

import os
import json
import time
import logging
import requests
from datetime import datetime
from typing import Dict, List, Optional, Any
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class ExternalDataCollector:
    """V3 Enhanced external data collector - REAL DATA ONLY - NO FALLBACK"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Load API credentials from environment
        self.alpha_vantage_key = os.getenv('ALPHA_VANTAGE_API_KEY_1')
        self.news_api_key = os.getenv('NEWS_API_KEY_1') 
        self.fred_api_key = os.getenv('FRED_API_KEY_1')
        self.reddit_client_id = os.getenv('REDDIT_CLIENT_ID_1')
        self.reddit_client_secret = os.getenv('REDDIT_CLIENT_SECRET_1')
        self.twitter_bearer = os.getenv('TWITTER_BEARER_TOKEN_1')
        
        # Create robust HTTP session
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Track real API status
        self.api_status = {
            'alpha_vantage': False,
            'news_api': False,
            'fred': False,
            'reddit': False,
            'twitter': False
        }
        
        # Test all APIs on initialization
        self._test_all_apis()
        
        print("[V3_EXTERNAL] Enhanced External Data Collector initialized - REAL DATA ONLY")
        print(f"[V3_EXTERNAL] Working APIs: {sum(self.api_status.values())}/5")
        
        # Show which APIs are working
        for api_name, status in self.api_status.items():
            status_text = "Connected" if status else "Failed"
            print(f"  [{status_text.upper()}] {api_name.upper()}: {status_text}")
    
    def _safe_request(self, url: str, headers: Dict = None, timeout: int = 10) -> Optional[Dict]:
        """Make a safe HTTP request with proper error handling"""
        try:
            response = self.session.get(url, headers=headers or {}, timeout=timeout)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                self.logger.warning(f"Rate limited: {url}")
                return None
            else:
                self.logger.warning(f"HTTP {response.status_code} for {url}")
                return None
                
        except requests.exceptions.Timeout:
            self.logger.warning(f"Timeout for {url}")
            return None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request error for {url}: {e}")
            return None
        except json.JSONDecodeError:
            self.logger.error(f"Invalid JSON response from {url}")
            return None
    
    def _test_all_apis(self):
        """Test all API connections to verify they work"""
        print("[V3_EXTERNAL] Testing API connections...")
        
        # Test Alpha Vantage
        if self.alpha_vantage_key:
            try:
                url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=MSFT&apikey={self.alpha_vantage_key}"
                data = self._safe_request(url, timeout=15)
                if data and ('Global Quote' in data or ('Information' in data and 'Thank you' in data['Information'])):
                    if 'Information' in data and 'call frequency' in data.get('Information', ''):
                        print("  [RATE_LIMITED] ALPHA_VANTAGE: Rate limited")
                    else:
                        self.api_status['alpha_vantage'] = True
                        print("  [ACTIVE] ALPHA_VANTAGE: Connected")
                else:
                    print("  [INACTIVE] ALPHA_VANTAGE: Failed")
            except Exception as e:
                print(f"  [INACTIVE] ALPHA_VANTAGE: {e}")
        else:
            print("  [INACTIVE] ALPHA_VANTAGE: No API key")
        
        time.sleep(1)
        
        # Test News API
        if self.news_api_key:
            try:
                url = f"https://newsapi.org/v2/everything?q=bitcoin&apiKey={self.news_api_key}&pageSize=1"
                data = self._safe_request(url, timeout=10)
                if data and 'articles' in data:
                    self.api_status['news_api'] = True
                    print("  [ACTIVE] NEWS_API: Connected")
                else:
                    print("  [INACTIVE] NEWS_API: Failed")
            except Exception as e:
                print(f"  [INACTIVE] NEWS_API: {e}")
        else:
            print("  [INACTIVE] NEWS_API: No API key")
        
        time.sleep(1)
        
        # Test FRED
        if self.fred_api_key:
            try:
                url = f"https://api.stlouisfed.org/fred/series/observations?series_id=GDP&api_key={self.fred_api_key}&file_type=json&limit=1"
                data = self._safe_request(url, timeout=10)
                if data and 'observations' in data:
                    self.api_status['fred'] = True
                    print("  [ACTIVE] FRED: Connected")
                else:
                    print("  [INACTIVE] FRED: Failed")
            except Exception as e:
                print(f"  [INACTIVE] FRED: {e}")
        else:
            print("  [INACTIVE] FRED: No API key")
        
        time.sleep(1)
        
        # Test Reddit
        if self.reddit_client_id and self.reddit_client_secret:
            try:
                # Try to get Reddit access token
                auth = requests.auth.HTTPBasicAuth(self.reddit_client_id, self.reddit_client_secret)
                data = {
                    'grant_type': 'client_credentials'
                }
                headers = {'User-Agent': 'V3-Trading-System/1.0'}
                
                response = self.session.post('https://www.reddit.com/api/v1/access_token',
                                           auth=auth, data=data, headers=headers, timeout=10)
                
                if response.status_code == 200:
                    token_data = response.json()
                    if 'access_token' in token_data:
                        self.api_status['reddit'] = True
                        print("  [ACTIVE] REDDIT: Connected")
                    else:
                        print("  [INACTIVE] REDDIT: No access token")
                else:
                    print("  [INACTIVE] REDDIT: Auth failed")
                    
            except Exception as e:
                print(f"  [INACTIVE] REDDIT: {e}")
        else:
            print("  [INACTIVE] REDDIT: No credentials")
        
        time.sleep(1)
        
        # Test Twitter
        if self.twitter_bearer:
            try:
                headers = {'Authorization': f'Bearer {self.twitter_bearer}'}
                url = "https://api.twitter.com/2/tweets/search/recent?query=bitcoin&max_results=10"
                data = self._safe_request(url, headers=headers, timeout=10)
                if data and 'data' in data:
                    self.api_status['twitter'] = True
                    print("  [ACTIVE] TWITTER: Connected")
                else:
                    print("  [INACTIVE] TWITTER: Failed")
            except Exception as e:
                print(f"  [INACTIVE] TWITTER: {e}")
        else:
            print("  [INACTIVE] TWITTER: No bearer token")
    
    def get_latest_news_sentiment(self, symbol="bitcoin", limit=10):
        """Get real news sentiment from News API - NO FALLBACK DATA"""
        if not self.api_status['news_api']:
            return None
            
        try:
            url = f"https://newsapi.org/v2/everything?q={symbol}&apiKey={self.news_api_key}&pageSize={limit}&sortBy=publishedAt"
            data = self._safe_request(url, timeout=15)
            
            if not data or 'articles' not in data:
                return None
            
            articles = data['articles']
            news_items = []
            
            # Analyze sentiment
            positive_words = ['bullish', 'rise', 'gain', 'up', 'positive', 'growth', 'bull', 'surge', 'rally']
            negative_words = ['bearish', 'fall', 'drop', 'down', 'negative', 'crash', 'bear', 'decline']
            
            for article in articles[:5]:  # Take first 5 for dashboard
                title = article.get('title', '') or ''  # Handle None values
                description = article.get('description', '') or ''  # Handle None values
                
                # Skip articles with no content
                if not title and not description:
                    continue
                    
                text = (title + ' ' + description).lower()
                
                # Determine sentiment
                pos_count = sum(1 for word in positive_words if word in text)
                neg_count = sum(1 for word in negative_words if word in text)
                
                if pos_count > neg_count:
                    sentiment = 'Bullish'
                elif neg_count > pos_count:
                    sentiment = 'Bearish'
                else:
                    sentiment = 'Neutral'
                
                # Get time ago
                pub_date_str = article.get('publishedAt')
                if not pub_date_str:
                    time_ago = 'Unknown time'
                else:
                    try:
                        pub_date = datetime.fromisoformat(pub_date_str.replace('Z', '+00:00'))
                        time_diff = datetime.now(pub_date.tzinfo) - pub_date
                        
                        if time_diff.days > 0:
                            time_ago = f"{time_diff.days} day{'s' if time_diff.days > 1 else ''} ago"
                        elif time_diff.seconds > 3600:
                            hours = time_diff.seconds // 3600
                            time_ago = f"{hours} hour{'s' if hours > 1 else ''} ago"
                        else:
                            minutes = time_diff.seconds // 60
                            time_ago = f"{minutes} minute{'s' if minutes > 1 else ''} ago"
                    except:
                        time_ago = 'Recently'
                
                news_items.append({
                    'title': title,
                    'sentiment': sentiment,
                    'time_ago': time_ago,
                    'url': article.get('url', '')
                })
            
            return news_items if news_items else None
            
        except Exception as e:
            self.logger.error(f"News sentiment error: {e}")
            return None
    
    def get_reddit_posts(self, subreddit="cryptocurrency", limit=5):
        """Get real Reddit posts - NO FALLBACK DATA"""
        if not self.api_status['reddit']:
            return None
            
        try:
            # Get access token
            auth = requests.auth.HTTPBasicAuth(self.reddit_client_id, self.reddit_client_secret)
            data = {'grant_type': 'client_credentials'}
            headers = {'User-Agent': 'V3-Trading-System/1.0'}
            
            response = self.session.post('https://www.reddit.com/api/v1/access_token',
                                       auth=auth, data=data, headers=headers, timeout=10)
            
            if response.status_code != 200:
                return None
                
            token_data = response.json()
            access_token = token_data.get('access_token')
            if not access_token:
                return None
            
            # Get posts
            headers = {
                'User-Agent': 'V3-Trading-System/1.0',
                'Authorization': f'Bearer {access_token}'
            }
            
            url = f"https://oauth.reddit.com/r/{subreddit}/hot?limit={limit}"
            posts_data = self._safe_request(url, headers=headers, timeout=15)
            
            if not posts_data or 'data' not in posts_data:
                return None
            
            posts = []
            for post in posts_data['data']['children'][:limit]:
                post_data = post['data']
                
                # Analyze sentiment based on score and title
                title = post_data.get('title', '') or ''
                score = post_data.get('score', 0)
                
                if score > 50:
                    sentiment = 'bullish'
                elif score < 0:
                    sentiment = 'bearish'
                else:
                    sentiment = 'neutral'
                
                # Calculate time ago
                created_utc = post_data.get('created_utc', 0)
                if created_utc:
                    time_diff = datetime.now() - datetime.fromtimestamp(created_utc)
                    if time_diff.days > 0:
                        time_ago = f"{time_diff.days}d ago"
                    elif time_diff.seconds > 3600:
                        hours = time_diff.seconds // 3600
                        time_ago = f"{hours}h ago"
                    else:
                        minutes = time_diff.seconds // 60
                        time_ago = f"{minutes}m ago"
                else:
                    time_ago = 'Unknown'
                
                posts.append({
                    'platform': 'reddit',
                    'content': title,
                    'sentiment': sentiment,
                    'time': time_ago,
                    'author': f"r/{subreddit}",
                    'score': score
                })
            
            return posts if posts else None
            
        except Exception as e:
            self.logger.error(f"Reddit posts error: {e}")
            return None
    
    def get_twitter_posts(self, query="bitcoin", limit=5):
        """Get real Twitter posts - NO FALLBACK DATA"""
        if not self.api_status['twitter']:
            return None
            
        try:
            headers = {'Authorization': f'Bearer {self.twitter_bearer}'}
            url = f"https://api.twitter.com/2/tweets/search/recent?query={query}&max_results={limit}"
            data = self._safe_request(url, headers=headers, timeout=15)
            
            if not data or 'data' not in data:
                return None
            
            tweets = data['data']
            posts = []
            
            # Analyze sentiment
            positive_words = ['bullish', 'moon', 'pump', 'buy', 'hodl', 'bull', 'gem', 'rocket']
            negative_words = ['bearish', 'dump', 'sell', 'crash', 'bear', 'rekt', 'rugpull', 'scam']
            
            for tweet in tweets:
                text = tweet.get('text', '') or ''
                if not text:
                    continue
                    
                text_lower = text.lower()
                
                pos_count = sum(1 for word in positive_words if word in text_lower)
                neg_count = sum(1 for word in negative_words if word in text_lower)
                
                if pos_count > neg_count:
                    sentiment = 'bullish'
                elif neg_count > pos_count:
                    sentiment = 'bearish'
                else:
                    sentiment = 'neutral'
                
                posts.append({
                    'platform': 'twitter',
                    'content': text[:100] + '...' if len(text) > 100 else text,
                    'sentiment': sentiment,
                    'time': 'Live data',
                    'author': '@CryptoData'
                })
            
            return posts if posts else None
            
        except Exception as e:
            self.logger.error(f"Twitter posts error: {e}")
            return None
    
    def get_fear_greed_index(self):
        """Get Fear & Greed Index from alternative.me"""
        try:
            url = "https://api.alternative.me/fng/"
            data = self._safe_request(url, timeout=10)
            
            if data and 'data' in data and len(data['data']) > 0:
                index_data = data['data'][0]
                value = int(index_data['value'])
                classification = index_data['value_classification']
                
                return {
                    'value': value,
                    'classification': classification
                }
            
        except Exception as e:
            self.logger.error(f"Fear & Greed Index error: {e}")
        
        return None  # Return None instead of fallback
    
    def collect_comprehensive_market_data(self, symbol="BTC", force_refresh=False):
        """Main data collection method - returns real data only, NO FALLBACK"""
        
        data = {
            'timestamp': datetime.now().isoformat(),
            'symbol': symbol,
            'data_sources': [],
            'api_status': self.api_status.copy(),
            'working_apis': sum(self.api_status.values()),
            'total_apis': len(self.api_status)
        }
        
        # Get real news if available - NO FALLBACK
        if self.api_status['news_api']:
            news_data = self.get_latest_news_sentiment(symbol.lower())
            if news_data:
                data['news_sentiment'] = news_data
                data['data_sources'].append('news_api')
        
        # Get real Reddit posts if available - NO FALLBACK
        if self.api_status['reddit']:
            reddit_posts = self.get_reddit_posts()
            if reddit_posts:
                data['reddit_posts'] = reddit_posts
                data['data_sources'].append('reddit')
        
        # Get real Twitter posts if available - NO FALLBACK
        if self.api_status['twitter']:
            twitter_posts = self.get_twitter_posts()
            if twitter_posts:
                data['twitter_posts'] = twitter_posts
                data['data_sources'].append('twitter')
        
        # Get Fear & Greed Index - NO FALLBACK
        fg_data = self.get_fear_greed_index()
        if fg_data:
            data['fear_greed_index'] = fg_data
            data['data_sources'].append('alternative_me')
        
        print(f"[V3_EXTERNAL] Collected data from {len(data['data_sources'])} sources")
        return data
    
    def get_api_status(self):
        """Get current API status"""
        return {
            'total_apis': len(self.api_status),
            'working_apis': sum(self.api_status.values()),
            'api_status': self.api_status.copy(),
            'data_quality': 'HIGH' if sum(self.api_status.values()) >= 3 else 'MEDIUM' if sum(self.api_status.values()) >= 2 else 'LOW'
        }
    
    def get_latest_data(self):
        """Get latest comprehensive data"""
        return self.collect_comprehensive_market_data('BTC')

# Test if run directly
if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    
    collector = ExternalDataCollector()
    data = collector.get_latest_data()
    print(f"\nCollected data from {data.get('working_apis', 0)} APIs")
    print(f"Data sources: {data.get('data_sources', [])}")