import yfinance as yf
from datetime import timezone
from typing import List, Dict, Any
from .database import execute_upsert, fetch_all_symbols

# yfinance limits (approximate safe values)
INTERVAL_LIMITS = {
    "1m": "7d",
    "2m": "60d",
    "5m": "60d",
    "15m": "60d",
    "30m": "60d",
    "60m": "730d",
    "1h": "730d",
    "1d": "10y",
    "5d": "10y",
    "1wk": "10y",
    "1mo": "10y",
    "3mo": "10y",
}

def get_safe_period_for_interval(interval: str, requested_period: str = "1y") -> str:
    limit = INTERVAL_LIMITS.get(interval, "1y")
    if limit == "7d": return "5d"
    if limit == "60d": return "59d"
    return requested_period

def fetch_candles_for_symbol(symbol: str, interval: str, period: str = "1y") -> List[Dict[str, Any]]:
    safe_period = get_safe_period_for_interval(interval, period)
    
    if safe_period != period:
        print(f"  [Info] {interval}: Requested {period}, using limit {safe_period}")
    
    try:
        ticker = yf.Ticker(symbol)
        history = ticker.history(period=safe_period, interval=interval)
        
        if history.empty:
            return []
            
        candles_data = []
        history = history.reset_index()
        
        for _, row in history.iterrows():
            ts = row['Date']
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            else:
                ts = ts.astimezone(timezone.utc)
                
            candle = {
                "symbol": symbol.upper(),
                "timeframe": interval,
                "timestamp": ts, # Keep as datetime object for psycopg2
                "open": float(row['Open']),
                "high": float(row['High']),
                "low": float(row['Low']),
                "close": float(row['Close']),
                "volume": float(row['Volume'])
            }
            candles_data.append(candle)
            
        return candles_data

    except Exception as e:
        print(f"  [Error] {symbol} {interval}: {e}")
        return []

def sync_candles(timeframes: List[str], period: str = "1y"):
    """
    Fetches candles for ALL symbols in the database and updates the database.
    """
    print("--- Starting Candle Sync ---")
    
    # 1. Get symbols from DB
    symbols = fetch_all_symbols()
    if not symbols:
        print("No symbols found in database. Run symbol sync first.")
        return
        
    print(f"Found {len(symbols)} symbols in database.")
    
    # 2. Fetch and Save
    for symbol in symbols:
        print(f"\nProcessing {symbol}...")
        for interval in timeframes:
            candles = fetch_candles_for_symbol(symbol, interval, period)
            
            if not candles:
                continue
                
            print(f"  Saving {len(candles)} candles ({interval})...")
            
            query = """
                INSERT INTO candle (
                    "symbol", "timeframe", "timestamp", 
                    "open", "high", "low", "close", "volume"
                ) VALUES %s
                ON CONFLICT ("symbol", "timeframe", "timestamp") DO UPDATE SET
                    "open" = EXCLUDED."open",
                    "high" = EXCLUDED."high",
                    "low" = EXCLUDED."low",
                    "close" = EXCLUDED."close",
                    "volume" = EXCLUDED."volume";
            """
            
            values = []
            for c in candles:
                values.append((
                    c['symbol'], c['timeframe'], c['timestamp'],
                    c['open'], c['high'], c['low'], c['close'], c['volume']
                ))
                
            execute_upsert(query, values)
            
    print("\nCandle sync complete.")
