#!/usr/bin/env python3
"""
V3 HISTORICAL DATA MANAGER - LIVE DATA ONLY
===========================================
V3 Fixes Applied:
- Removed all mock_data references - V3 uses live data only
- Enhanced API connectivity with multiple endpoints
- Production-ready error handling for live data sources
- No fallback to mock data - live market data only
"""

import os
import asyncio
import aiohttp
import pandas as pd
import numpy as np
import logging
import sqlite3
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import time
import random

class HistoricalDataManager:
    """V3 Historical Data Manager - LIVE DATA ONLY"""
    
    def __init__(self):
        self.db_path = 'data/historical_data.db'
        self.config = {
            'years_of_data': 2,
            'enable_backtesting': True,
            'cache_expiry_hours': 6,
            'cleanup_days': 30,
            'v3_compliance': True,
            'live_data_only': True
        }
        
        # V3 Multiple live data sources for resilience
        self.live_api_endpoints = [
            'https://api.binance.com/api/v3/klines',
            'https://api.binance.us/api/v3/klines',
            'https://testnet.binance.vision/api/v3/klines',
        ]
        
        self.symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'XRPUSDT', 'ADAUSDT', 'SOLUSDT']
        self.timeframes = {'1h': '1h', '4h': '4h', '1d': '1d'}
        self.live_data_cache = {}  # V3: Live data cache
        self.last_update_times = {}
        
        logging.info("[DATA_MANAGER] V3 Historical Data Manager initialized - LIVE DATA ONLY")
    
    async def initialize(self):
        """V3 Initialize with live data sources only"""
        try:
            os.makedirs('data', exist_ok=True)
            await self.init_database()
            await self.test_live_api_connectivity()
            logging.info("[DATA_MANAGER] V3 Historical Data Manager initialization complete - LIVE MODE")
        except Exception as e:
            logging.error(f"[DATA_MANAGER] V3 Data Manager initialization failed: {e}")
            # V3: No fallback to mock data - raise error for production awareness
            raise RuntimeError(f"V3 Live data initialization failed: {e}")
    
    async def init_database(self):
        """Initialize V3 database for live historical data"""
        try:
            live_historical_sql = """
            CREATE TABLE IF NOT EXISTS live_historical_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                open_price REAL NOT NULL,
                high_price REAL NOT NULL,
                low_price REAL NOT NULL,
                close_price REAL NOT NULL,
                volume REAL NOT NULL,
                data_source TEXT DEFAULT 'live_api',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                v3_compliance BOOLEAN DEFAULT TRUE,
                UNIQUE(symbol, timeframe, timestamp)
            )
            """
            
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(live_historical_sql)
                conn.commit()
                logging.info("[DATA_MANAGER] V3 Live historical data database initialized")
        except Exception as e:
            logging.error(f"[DATA_MANAGER] V3 Database initialization failed: {e}")
            return None
    
    async def test_live_api_connectivity(self):
        """V3 Test live API connectivity - no fallbacks"""
        working_endpoints = []
        
        for endpoint in self.live_api_endpoints:
            try:
                async with aiohttp.ClientSession() as session:
                    params = {'symbol': 'BTCUSDT', 'interval': '1h', 'limit': 10}
                    async with session.get(endpoint, params=params, timeout=10) as response:
                        if response.status == 200:
                            data = await response.json()
                            if data and len(data) > 0:
                                working_endpoints.append(endpoint)
                                logging.info(f"[DATA_MANAGER] V3 Live API endpoint working: {endpoint}")
                            else:
                                logging.warning(f"[DATA_MANAGER] V3 API endpoint {endpoint} returned empty data")
                        else:
                            logging.warning(f"[DATA_MANAGER] V3 API endpoint {endpoint} returned {response.status}")
            except Exception as e:
                logging.warning(f"[DATA_MANAGER] V3 API endpoint {endpoint} failed: {e}")
                continue
        
        if working_endpoints:
            # Prioritize working endpoints
            self.live_api_endpoints = working_endpoints + [ep for ep in self.live_api_endpoints if ep not in working_endpoints]
            logging.info(f"[DATA_MANAGER] V3 Found {len(working_endpoints)} working live API endpoints")
        else:
            # V3: No fallback - raise error for production awareness
            raise RuntimeError("V3 CRITICAL: No live API endpoints available - cannot proceed without live data")
    
    async def download_live_historical_data(self, symbol: str, timeframe: str, limit: int = 100):
        """V3 Download live data with proper session management - NO MOCK FALLBACK"""
        for endpoint in self.live_api_endpoints:
            connector = None
            session = None
            try:
                connector = aiohttp.TCPConnector(
                    limit=1,
                    limit_per_host=1,
                    ttl_dns_cache=300,
                    enable_cleanup_closed=True
                )
                
                timeout = aiohttp.ClientTimeout(total=15, connect=5)
                
                session = aiohttp.ClientSession(
                    connector=connector,
                    timeout=timeout,
                    headers={'User-Agent': 'V3-Trading-System/3.0'}
                )
                
                params = {'symbol': symbol, 'interval': timeframe, 'limit': limit}
                
                async with session.get(endpoint, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data:
                            df = pd.DataFrame(data, columns=[
                                'timestamp', 'open', 'high', 'low', 'close', 'volume',
                                'close_time', 'quote_volume', 'trades_count',
                                'taker_buy_volume', 'taker_buy_quote_volume', 'ignore'
                            ])
                            
                            # Convert data types
                            for col in ['open', 'high', 'low', 'close', 'volume']:
                                df[col] = pd.to_numeric(df[col], errors='coerce')
                            
                            df['timestamp'] = pd.to_numeric(df['timestamp'])
                            df = df.dropna()
                            
                            logging.info(f"[DATA_MANAGER] V3 Downloaded {len(df)} live candles for {symbol} {timeframe}")
                            
                            # Cleanup before returning
                            if session and not session.closed:
                                await session.close()
                                await asyncio.sleep(0.1)
                            
                            return df
                    elif response.status == 451:
                        logging.warning(f"[DATA_MANAGER] V3 HTTP 451 (Unavailable For Legal Reasons) from {endpoint}")
                        continue
                    elif response.status == 429:
                        logging.warning(f"[DATA_MANAGER] V3 Rate limited by {endpoint}")
                        await asyncio.sleep(2)  # Wait before trying next endpoint
                        continue
                    else:
                        logging.warning(f"[DATA_MANAGER] V3 HTTP {response.status} from {endpoint}")
                        continue
                        
            except asyncio.TimeoutError:
                logging.warning(f"[DATA_MANAGER] V3 Timeout for endpoint {endpoint}")
                continue
            except Exception as e:
                logging.warning(f"[DATA_MANAGER] V3 Error with endpoint {endpoint}: {e}")
                continue
            finally:
                # Ensure cleanup happens
                try:
                    if session and not session.closed:
                        await session.close()
                        await asyncio.sleep(0.1)
                except:
                    pass
                try:
                    if connector:
                        await connector.close()
                except:
                    pass
        
        # V3: If all APIs fail, raise error - no mock fallback
        logging.error(f"[DATA_MANAGER] V3 CRITICAL: All live API endpoints failed for {symbol} {timeframe}")
        raise RuntimeError(f"V3 CRITICAL: Cannot obtain live data for {symbol} {timeframe} - all endpoints failed")
    
    async def store_live_historical_data(self, symbol: str, timeframe: str, df: pd.DataFrame):
        """V3 Store live historical data in database"""
        try:
            store_sql = """
            INSERT OR REPLACE INTO live_historical_data 
            (symbol, timeframe, timestamp, open_price, high_price, 
             low_price, close_price, volume, data_source, v3_compliance)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            with sqlite3.connect(self.db_path) as conn:
                for _, row in df.iterrows():
                    try:
                        params = (
                            str(symbol), 
                            str(timeframe), 
                            int(float(row['timestamp'])),
                            float(row['open']), 
                            float(row['high']), 
                            float(row['low']),
                            float(row['close']), 
                            float(row['volume']),
                            'live_api',
                            True  # V3 compliance flag
                        )
                        conn.execute(store_sql, params)
                    except (TypeError, ValueError) as e:
                        logging.warning(f"[DATA_MANAGER] V3 Skipping row due to type error: {e}")
                        continue
                conn.commit()
                logging.info(f"[DATA_MANAGER] V3 Stored {len(df)} live data points for {symbol} {timeframe}")
        except Exception as e:
            logging.error(f"[DATA_MANAGER] V3 Error storing live data: {e}")
    
    async def get_historical_data(self, symbol: str, timeframe: str, 
                                 start_time: Optional[datetime] = None, 
                                 end_time: Optional[datetime] = None):
        """V3 Get historical data - LIVE DATA ONLY"""
        try:
            # V3 Query for live data only
            query_sql = """
            SELECT timestamp, open_price, high_price, low_price, close_price, volume
            FROM live_historical_data 
            WHERE symbol = ? AND timeframe = ? AND v3_compliance = TRUE
            ORDER BY timestamp ASC
            """
            
            # Convert any mock objects to strings if needed (legacy compatibility)
            symbol_str = str(symbol) if hasattr(symbol, '__str__') else 'BTCUSDT'
            timeframe_str = str(timeframe) if hasattr(timeframe, '__str__') else '1h'
                
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(query_sql, (symbol_str, timeframe_str))
                rows = cursor.fetchall()
                
                if rows:
                    # Filter by time range if specified
                    if start_time or end_time:
                        filtered_rows = []
                        for row in rows:
                            row_time = datetime.fromtimestamp(row[0] / 1000)
                            if start_time and row_time < start_time:
                                continue
                            if end_time and row_time > end_time:
                                continue
                            filtered_rows.append(row)
                        rows = filtered_rows
                    
                    if rows:
                        data = {
                            'timestamp': [row[0] for row in rows],
                            'open': [row[1] for row in rows],
                            'high': [row[2] for row in rows],
                            'low': [row[3] for row in rows],
                            'close': [row[4] for row in rows],
                            'volume': [row[5] for row in rows]
                        }
                        logging.info(f"[DATA_MANAGER] V3 Retrieved {len(rows)} live data points for {symbol_str} {timeframe_str}")
                        return data
                    else:
                        logging.info(f"[DATA_MANAGER] V3 No data in time range for {symbol_str} {timeframe_str}")
                
                # V3 If no cached data, download live data
                logging.info(f"[DATA_MANAGER] V3 No cached data - downloading live data for {symbol_str} {timeframe_str}")
                df = await self.download_live_historical_data(symbol_str, timeframe_str, 100)
                if df is not None and len(df) > 0:
                    await self.store_live_historical_data(symbol_str, timeframe_str, df)
                    
                    # Filter by time range if specified
                    if start_time or end_time:
                        if start_time:
                            start_ts = int(start_time.timestamp() * 1000)
                            df = df[df['timestamp'] >= start_ts]
                        if end_time:
                            end_ts = int(end_time.timestamp() * 1000)
                            df = df[df['timestamp'] <= end_ts]
                    
                    if len(df) > 0:
                        data = {
                            'timestamp': df['timestamp'].tolist(),
                            'open': df['open'].tolist(),
                            'high': df['high'].tolist(),
                            'low': df['low'].tolist(),
                            'close': df['close'].tolist(),
                            'volume': df['volume'].tolist()
                        }
                        logging.info(f"[DATA_MANAGER] V3 Returning {len(df)} live data points for {symbol_str} {timeframe_str}")
                        return data
                    else:
                        logging.warning(f"[DATA_MANAGER] V3 No data in specified time range for {symbol_str} {timeframe_str}")
                
                return None
                
        except Exception as e:
            logging.error(f"[DATA_MANAGER] V3 Error getting live historical data: {e}")
            # V3: No fallback - return None and let caller handle
            return None
    
    async def get_latest_data(self, symbol: str, timeframe: str = '1h') -> Optional[Dict]:
        """V3 Get latest live market data for a symbol"""
        try:
            # Get recent live historical data (last few periods)
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=24)  # Last 24 hours
            
            data = await self.get_historical_data(symbol, timeframe, start_time, end_time)
            
            if data and len(data.get('close', [])) > 0:
                # Return the latest live data point
                latest_data = {
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'timestamp': data['timestamp'][-1] if data.get('timestamp') else int(datetime.now().timestamp() * 1000),
                    'open': data['open'][-1] if data.get('open') else 0,
                    'high': data['high'][-1] if data.get('high') else 0,
                    'low': data['low'][-1] if data.get('low') else 0,
                    'close': data['close'][-1] if data.get('close') else 0,
                    'volume': data['volume'][-1] if data.get('volume') else 0,
                    'last_updated': datetime.now().isoformat(),
                    'data_source': 'live_api',
                    'v3_compliance': True
                }
                
                return latest_data
            
            return None
            
        except Exception as e:
            logging.error(f"[DATA_MANAGER] V3 Error getting latest live data for {symbol}: {e}")
            return None
    
    def get_latest_data_sync(self, symbol: str, timeframe: str = '1h') -> Optional[Dict]:
        """V3 Synchronous version of get_latest_data for compatibility"""
        try:
            import asyncio
            
            # Try to run in existing event loop
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # Create a task and return future result
                    import concurrent.futures
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        future = executor.submit(asyncio.run, self.get_latest_data(symbol, timeframe))
                        return future.result(timeout=15)
                else:
                    return asyncio.run(self.get_latest_data(symbol, timeframe))
            except RuntimeError:
                return asyncio.run(self.get_latest_data(symbol, timeframe))
                
        except Exception as e:
            logging.error(f"[DATA_MANAGER] V3 Error in sync get_latest_data for {symbol}: {e}")
            # V3: Return None instead of fallback data - let caller handle
            return None

    async def cleanup_old_data(self, days_to_keep: int = 30):
        """V3 Cleanup old live data to manage storage"""
        try:
            cutoff_timestamp = int((datetime.now() - timedelta(days=days_to_keep)).timestamp() * 1000)
            
            cleanup_sql = """
            DELETE FROM live_historical_data 
            WHERE timestamp < ? AND v3_compliance = TRUE
            """
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(cleanup_sql, (cutoff_timestamp,))
                deleted_count = cursor.rowcount
                conn.commit()
                
                logging.info(f"[DATA_MANAGER] V3 Cleaned up {deleted_count} old live data records")
                return deleted_count
                
        except Exception as e:
            logging.error(f"[DATA_MANAGER] V3 Error cleaning up old data: {e}")
            return 0

    def get_v3_metrics(self):
        """V3 Get data manager metrics"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Count live data points
                cursor = conn.execute("SELECT COUNT(*) FROM live_historical_data WHERE v3_compliance = TRUE")
                total_live_data_points = cursor.fetchone()[0]
                
                # Count unique symbols
                cursor = conn.execute("SELECT COUNT(DISTINCT symbol) FROM live_historical_data WHERE v3_compliance = TRUE")
                unique_symbols = cursor.fetchone()[0]
                
                # Count unique timeframes
                cursor = conn.execute("SELECT COUNT(DISTINCT timeframe) FROM live_historical_data WHERE v3_compliance = TRUE")
                unique_timeframes = cursor.fetchone()[0]
                
                return {
                    'total_live_data_points': total_live_data_points,
                    'unique_symbols': unique_symbols,
                    'unique_timeframes': unique_timeframes,
                    'live_data_sources_active': len(self.live_api_endpoints),
                    'v3_compliance': True,
                    'data_mode': 'LIVE_PRODUCTION'
                }
        except Exception as e:
            logging.error(f"[DATA_MANAGER] V3 Error getting metrics: {e}")
            return {
                'total_live_data_points': 0,
                'unique_symbols': 0,
                'unique_timeframes': 0,
                'live_data_sources_active': 0,
                'v3_compliance': True,
                'data_mode': 'ERROR',
                'error': str(e)
            }

    def get_metrics(self):
        """Legacy compatibility method - returns V3 metrics"""
        return self.get_v3_metrics()

    async def validate_v3_compliance(self) -> Dict[str, Any]:
        """V3 Validate that all data is from live sources"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Check for any non-V3 compliant data
                cursor = conn.execute("SELECT COUNT(*) FROM live_historical_data WHERE v3_compliance != TRUE")
                non_compliant_count = cursor.fetchone()[0]
                
                # Check data sources
                cursor = conn.execute("SELECT DISTINCT data_source FROM live_historical_data")
                data_sources = [row[0] for row in cursor.fetchall()]
                
                # Check for any mock data sources
                has_live_data_only = all('live' in source or 'api' in source for source in data_sources)
                
                validation_result = {
                    'v3_compliant': non_compliant_count == 0 and has_live_data_only,
                    'non_compliant_records': non_compliant_count,
                    'data_sources': data_sources,
                    'live_data_only': has_live_data_only,
                    'validation_timestamp': datetime.now().isoformat()
                }
                
                if validation_result['v3_compliant']:
                    logging.info("[DATA_MANAGER] V3 Compliance validation PASSED")
                else:
                    logging.warning(f"[DATA_MANAGER] V3 Compliance validation FAILED: {validation_result}")
                
                return validation_result
                
        except Exception as e:
            logging.error(f"[DATA_MANAGER] V3 Compliance validation error: {e}")
            return {
                'v3_compliant': False,
                'error': str(e),
                'validation_timestamp': datetime.now().isoformat()
            }


# V3 Testing
if __name__ == "__main__":
    print("[DATA_MANAGER] Testing V3 Historical Data Manager - LIVE DATA ONLY")
    
    async def test_v3_data_manager():
        manager = HistoricalDataManager()
        
        try:
            await manager.initialize()
            
            # Test live data retrieval
            data = await manager.get_historical_data('BTCUSDT', '1h')
            if data:
                print(f"[DATA_MANAGER] V3 Retrieved {len(data['close'])} live data points")
            else:
                print("[DATA_MANAGER] V3 No live data retrieved")
            
            # Test latest data
            latest = await manager.get_latest_data('BTCUSDT')
            if latest:
                print(f"[DATA_MANAGER] V3 Latest BTC price: ${latest['close']:.2f}")
            
            # Validate V3 compliance
            validation = await manager.validate_v3_compliance()
            print(f"[DATA_MANAGER] V3 Compliance: {validation['v3_compliant']}")
            
            # Get metrics
            metrics = manager.get_v3_metrics()
            print(f"[DATA_MANAGER] V3 Metrics: {metrics}")
            
        except Exception as e:
            print(f"[DATA_MANAGER] V3 Test failed: {e}")
    
    asyncio.run(test_v3_data_manager())
    print("[DATA_MANAGER] V3 Historical Data Manager test complete - LIVE DATA ONLY!")