import yfinance as yf
import pandas as pd
from typing import List, Dict, Any
from .database import execute_upsert

def fetch_symbol_metadata(ticker_symbol: str) -> Dict[str, Any]:
    """
    Fetches metadata for a single symbol from yfinance.
    """
    print(f"Fetching metadata for {ticker_symbol}...")
    try:
        ticker = yf.Ticker(ticker_symbol)
        info = ticker.info
        
        # Map to Prisma Symbol model
        return {
            "symbol": ticker_symbol.upper(),
            "name": info.get("shortName") or info.get("longName"),
            "baseAsset": ticker_symbol.upper(),
            "quoteAsset": info.get("currency", "USD"),
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "exchange": info.get("exchange"),
            "priceStep": 0.01, # Default
            "quantityStep": 1.0, # Default
            "minQuantity": 1.0 # Default
        }
    except Exception as e:
        print(f"Error fetching metadata for {ticker_symbol}: {e}")
        return None

def sync_symbols(tickers: List[str]):
    """
    Fetches metadata for the provided tickers and updates the database.
    """
    print(f"--- Starting Symbol Sync for {len(tickers)} symbols ---")
    
    symbols_data = []
    for t in tickers:
        data = fetch_symbol_metadata(t)
        if data:
            symbols_data.append(data)
            
    if not symbols_data:
        print("No symbol data found.")
        return

    print(f"Saving {len(symbols_data)} symbols to database...")
    
    query = """
        INSERT INTO symbol (
            "symbol", "name", "baseAsset", "quoteAsset", 
            "sector", "industry", "exchange", 
            "priceStep", "quantityStep", "minQuantity"
        ) VALUES %s
        ON CONFLICT ("symbol") DO UPDATE SET
            "name" = EXCLUDED."name",
            "baseAsset" = EXCLUDED."baseAsset",
            "quoteAsset" = EXCLUDED."quoteAsset",
            "sector" = EXCLUDED."sector",
            "industry" = EXCLUDED."industry",
            "exchange" = EXCLUDED."exchange",
            "priceStep" = EXCLUDED."priceStep",
            "quantityStep" = EXCLUDED."quantityStep",
            "minQuantity" = EXCLUDED."minQuantity";
    """
    
    values = []
    for s in symbols_data:
        values.append((
            s['symbol'], s['name'], s['baseAsset'], s['quoteAsset'],
            s['sector'], s['industry'], s['exchange'],
            s['priceStep'], s['quantityStep'], s['minQuantity']
        ))
        
    execute_upsert(query, values)
    print("Symbol sync complete.")

def export_symbols_to_csv(tickers: List[str], filename: str = "symbols_verification.csv"):
    """
    Fetches metadata for the provided tickers and saves to a CSV file.
    """
    print(f"--- Starting Symbol Export for {len(tickers)} symbols ---")
    
    symbols_data = []
    for t in tickers:
        data = fetch_symbol_metadata(t)
        if data:
            symbols_data.append(data)
            
    if not symbols_data:
        print("No symbol data found.")
        return

    df = pd.DataFrame(symbols_data)
    df.to_csv(filename, index=False)
    print(f"Saved {len(symbols_data)} symbols to {filename}")
