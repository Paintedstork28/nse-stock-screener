"""Stock list download, bhavcopy fetching, and SQLite storage."""

import datetime as dt
import io
import sqlite3
import time

import pandas as pd
import requests

from src.utils import DB_PATH, LOOKBACK_MONTHS, trading_days_between

# ---------------------------------------------------------------------------
# SQLite setup
# ---------------------------------------------------------------------------

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    _create_tables(conn)
    return conn


def _create_tables(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS ohlcv (
            symbol TEXT NOT NULL,
            trade_date DATE NOT NULL,
            open REAL, high REAL, low REAL, close REAL,
            volume INTEGER, delivery_qty INTEGER, delivery_pct REAL,
            PRIMARY KEY (symbol, trade_date)
        );
        CREATE TABLE IF NOT EXISTS fii_dii (
            trade_date DATE PRIMARY KEY,
            fii_buy REAL, fii_sell REAL, fii_net REAL,
            dii_buy REAL, dii_sell REAL, dii_net REAL
        );
        CREATE TABLE IF NOT EXISTS bulk_deals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date DATE, symbol TEXT, client_name TEXT,
            deal_type TEXT, quantity INTEGER, price REAL
        );
        CREATE TABLE IF NOT EXISTS promoter_data (
            symbol TEXT, quarter TEXT,
            promoter_holding_pct REAL, pledge_pct REAL,
            fii_holding_pct REAL, dii_holding_pct REAL, public_holding_pct REAL,
            PRIMARY KEY (symbol, quarter)
        );
        CREATE TABLE IF NOT EXISTS sector_indices (
            index_name TEXT NOT NULL,
            trade_date DATE NOT NULL,
            close REAL,
            PRIMARY KEY (index_name, trade_date)
        );
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT
        );
    """)


# ---------------------------------------------------------------------------
# NSE stock list
# ---------------------------------------------------------------------------

_NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*",
}


def fetch_stock_list():
    """Fetch list of all NSE equity symbols."""
    url = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
    try:
        resp = requests.get(url, headers=_NSE_HEADERS, timeout=30)
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(resp.text))
        col = df.columns[0]  # SYMBOL column
        symbols = df[col].dropna().str.strip().tolist()
        return [s for s in symbols if s and len(s) <= 20]
    except Exception:
        # Fallback: try loading from DB
        try:
            conn = get_db()
            df = pd.read_sql("SELECT DISTINCT symbol FROM ohlcv", conn)
            conn.close()
            return df["symbol"].tolist()
        except Exception:
            return []


def fetch_stock_info() -> pd.DataFrame:
    """Fetch company names and sector/industry for all NSE stocks.

    Returns DataFrame with columns: symbol, company_name, industry
    """
    rows = {}

    # 1. Company names from EQUITY_L.csv (covers all ~2200 stocks)
    try:
        url = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
        resp = requests.get(url, headers=_NSE_HEADERS, timeout=15)
        if resp.status_code == 200:
            df = pd.read_csv(io.StringIO(resp.text))
            for _, r in df.iterrows():
                sym = str(r.iloc[0]).strip()
                name = str(r.iloc[1]).strip()
                rows[sym] = {"symbol": sym, "company_name": name, "industry": "—"}
    except Exception:
        pass

    # 2. Industry data from Nifty 500 list (covers top 500)
    try:
        url2 = "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv"
        resp2 = requests.get(url2, headers=_NSE_HEADERS, timeout=15)
        if resp2.status_code == 200 and len(resp2.text) > 200:
            df2 = pd.read_csv(io.StringIO(resp2.text))
            for _, r in df2.iterrows():
                sym = str(r.get("Symbol", "")).strip()
                industry = str(r.get("Industry", "")).strip()
                if sym in rows:
                    rows[sym]["industry"] = industry
                elif sym:
                    name = str(r.get("Company Name", sym)).strip()
                    rows[sym] = {"symbol": sym, "company_name": name, "industry": industry}
    except Exception:
        pass

    if not rows:
        return pd.DataFrame(columns=["symbol", "company_name", "industry"])

    return pd.DataFrame(list(rows.values()))


# ---------------------------------------------------------------------------
# Bhavcopy download (jugaad-data primary, yfinance fallback)
# ---------------------------------------------------------------------------

def _download_bhavcopy_jugaad(trade_date: dt.date):
    """Download a single day's bhavcopy using jugaad-data."""
    try:
        from jugaad_data.nse import bhavcopy_save, bhavcopy_fo_save
        from jugaad_data.nse import NSEHistory
    except ImportError:
        pass

    # Try the direct CSV approach
    url = (
        f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_"
        f"{trade_date.strftime('%d%m%Y')}.csv"
    )
    try:
        resp = requests.get(url, headers=_NSE_HEADERS, timeout=30)
        if resp.status_code == 200 and len(resp.text) > 500:
            df = pd.read_csv(io.StringIO(resp.text))
            df.columns = df.columns.str.strip()
            return _normalize_bhavcopy(df, trade_date)
    except Exception:
        pass

    # Try alternate URL format
    dt_str = trade_date.strftime("%d%b%Y").upper()
    url2 = f"https://nsearchives.nseindia.com/content/historical/EQUITIES/{trade_date.year}/{trade_date.strftime('%b').upper()}/cm{dt_str}bhav.csv.zip"
    try:
        resp = requests.get(url2, headers=_NSE_HEADERS, timeout=30)
        if resp.status_code == 200:
            import zipfile
            zf = zipfile.ZipFile(io.BytesIO(resp.content))
            fname = zf.namelist()[0]
            df = pd.read_csv(zf.open(fname))
            df.columns = df.columns.str.strip()
            return _normalize_bhavcopy(df, trade_date)
    except Exception:
        pass

    # Use jugaad-data library
    try:
        from jugaad_data.nse import bhavcopy_raw
        df = bhavcopy_raw(trade_date)
        if df is not None and len(df) > 0:
            return _normalize_bhavcopy(df, trade_date)
    except Exception:
        pass

    return None


def _normalize_bhavcopy(df: pd.DataFrame, trade_date: dt.date) -> pd.DataFrame:
    """Normalize bhavcopy columns to standard format."""
    df.columns = df.columns.str.strip().str.upper()

    # Map various column names
    col_map = {}
    for c in df.columns:
        cl = c.strip().upper()
        if cl in ("SYMBOL", "TT_SYMBOL"):
            col_map["symbol"] = c
        elif cl in ("OPEN_PRICE", "OPEN"):
            col_map["open"] = c
        elif cl in ("HIGH_PRICE", "HIGH"):
            col_map["high"] = c
        elif cl in ("LOW_PRICE", "LOW"):
            col_map["low"] = c
        elif cl in ("CLOSE_PRICE", "CLOSE", "LAST_PRICE"):
            col_map["close"] = c
        elif cl in ("TTL_TRD_QNTY", "TOTAL_TRADE_QUANTITY", "TOTTRDQTY", "NO_OF_SHARES"):
            col_map["volume"] = c
        elif cl in ("DELIV_QTY", "DELIVERY_QTY", "DELIVERABLE_QTY"):
            col_map["delivery_qty"] = c
        elif cl in ("DELIV_PER", "DELIVERY_PER", "%DELIV"):
            col_map["delivery_pct"] = c
        elif cl == "SERIES":
            col_map["series"] = c

    if "symbol" not in col_map or "close" not in col_map:
        return pd.DataFrame()

    # Filter EQ series only
    if "series" in col_map:
        df = df[df[col_map["series"]].str.strip().isin(["EQ", "BE", "BZ"])].copy()

    result = pd.DataFrame()
    result["symbol"] = df[col_map["symbol"]].str.strip()
    result["trade_date"] = trade_date.isoformat()

    for target, src_key in [("open", "open"), ("high", "high"), ("low", "low"),
                             ("close", "close"), ("volume", "volume"),
                             ("delivery_qty", "delivery_qty"), ("delivery_pct", "delivery_pct")]:
        if src_key in col_map:
            result[target] = pd.to_numeric(df[col_map[src_key]].astype(str).str.strip().str.replace(",", ""), errors="coerce")
        else:
            result[target] = None

    return result.dropna(subset=["symbol", "close"])


def _download_yfinance_fallback(symbols: list[str], start: dt.date, end: dt.date) -> pd.DataFrame:
    """Fallback: use yfinance for a batch of symbols."""
    import yfinance as yf

    tickers = [f"{s}.NS" for s in symbols[:50]]  # Limit batch size
    try:
        data = yf.download(tickers, start=start, end=end, group_by="ticker", progress=False)
        rows = []
        for sym in symbols[:50]:
            ticker = f"{sym}.NS"
            try:
                if len(tickers) == 1:
                    sdf = data
                else:
                    sdf = data[ticker]
                for idx, row in sdf.iterrows():
                    rows.append({
                        "symbol": sym,
                        "trade_date": idx.date().isoformat(),
                        "open": row.get("Open"),
                        "high": row.get("High"),
                        "low": row.get("Low"),
                        "close": row.get("Close"),
                        "volume": row.get("Volume"),
                        "delivery_qty": None,
                        "delivery_pct": None,
                    })
            except Exception:
                continue
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Main data loading orchestration
# ---------------------------------------------------------------------------

def get_dates_to_fetch(conn):
    """Determine which trading days need data."""
    end = dt.date.today()
    start = end - dt.timedelta(days=LOOKBACK_MONTHS * 30 + 15)
    all_trading_days = trading_days_between(start, end)

    # Check what's already in DB
    existing = set()
    try:
        rows = conn.execute(
            "SELECT DISTINCT trade_date FROM ohlcv WHERE trade_date >= ?",
            (start.isoformat(),)
        ).fetchall()
        existing = {r[0] for r in rows}
    except Exception:
        pass

    # Filter to dates not yet in DB (skip today if market still open)
    now = dt.datetime.now()
    if now.hour < 16:  # Market closes at 3:30 PM IST
        all_trading_days = [d for d in all_trading_days if d < end]

    return [d for d in all_trading_days if d.isoformat() not in existing]


def load_all_data(progress_callback=None) -> bool:
    """Main entry: download all missing bhavcopies and store in SQLite."""
    conn = get_db()
    dates_needed = get_dates_to_fetch(conn)

    if not dates_needed:
        conn.close()
        return True

    total = len(dates_needed)
    success_count = 0

    for i, trade_date in enumerate(dates_needed):
        if progress_callback:
            progress_callback(i / total, f"Fetching {trade_date} ({i+1}/{total})")

        df = _download_bhavcopy_jugaad(trade_date)
        if df is not None and len(df) > 0:
            df.to_sql("ohlcv", conn, if_exists="append", index=False,
                       method="multi")
            success_count += 1
        else:
            pass  # Skip silently — could be a holiday or unavailable date

        # Polite delay to avoid rate limiting
        if i < total - 1:
            time.sleep(0.5)

    # Remove duplicates that might have been inserted
    try:
        conn.execute("""
            DELETE FROM ohlcv WHERE rowid NOT IN (
                SELECT MIN(rowid) FROM ohlcv GROUP BY symbol, trade_date
            )
        """)
        conn.commit()
    except Exception:
        pass

    # Clean up data older than the rolling window
    cleanup_cutoff = (dt.date.today() - dt.timedelta(days=LOOKBACK_MONTHS * 30 + 30)).isoformat()
    try:
        conn.execute("DELETE FROM ohlcv WHERE trade_date < ?", (cleanup_cutoff,))
        conn.commit()
    except Exception:
        pass

    # Update metadata
    conn.execute(
        "INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
        ("last_updated", dt.datetime.now().isoformat())
    )
    conn.commit()
    conn.close()

    if progress_callback:
        progress_callback(1.0, "Data loading complete!")

    return success_count > 0


def get_ohlcv_df(conn=None) -> pd.DataFrame:
    """Load rolling 6-month OHLCV data from SQLite into a DataFrame."""
    close_conn = conn is None
    if conn is None:
        conn = get_db()

    cutoff = (dt.date.today() - dt.timedelta(days=LOOKBACK_MONTHS * 30 + 15)).isoformat()

    df = pd.read_sql("""
        SELECT symbol, trade_date, open, high, low, close,
               volume, delivery_qty, delivery_pct
        FROM ohlcv
        WHERE trade_date >= ?
        ORDER BY symbol, trade_date
    """, conn, params=(cutoff,))

    if close_conn:
        conn.close()

    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    return df


def get_data_window():
    """Return the (start_date, end_date) of data currently in the rolling window."""
    try:
        conn = get_db()
        cutoff = (dt.date.today() - dt.timedelta(days=LOOKBACK_MONTHS * 30 + 15)).isoformat()
        row = conn.execute(
            "SELECT MIN(trade_date), MAX(trade_date) FROM ohlcv WHERE trade_date >= ?",
            (cutoff,)
        ).fetchone()
        conn.close()
        if row and row[0] and row[1]:
            start = dt.datetime.strptime(row[0], "%Y-%m-%d").date()
            end = dt.datetime.strptime(row[1], "%Y-%m-%d").date()
            return start, end
    except Exception:
        pass
    return None, None


def get_last_updated():
    try:
        conn = get_db()
        row = conn.execute(
            "SELECT value FROM metadata WHERE key = 'last_updated'"
        ).fetchone()
        conn.close()
        if row:
            ts = dt.datetime.fromisoformat(row[0])
            return ts.strftime("%d %b %Y, %I:%M %p")
    except Exception:
        pass
    return None
