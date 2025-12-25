import config
from modules.symbol_syncer import export_symbols_to_csv, sync_symbols

def main():
    print("--- Market Data Fetcher (Symbols to CSV) ---")
    sync_symbols(config.EQUITIES)

if __name__ == "__main__":
    main()
