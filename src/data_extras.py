"""Supplementary data fetchers: FII/DII, bulk deals, promoter data, sector indices."""

import datetime as dt
import io
import sqlite3

import pandas as pd
import requests

from src.data_fetcher import get_db

_NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json,text/html,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}


def _nse_session():
    """Create a requests session with NSE cookies."""
    s = requests.Session()
    s.headers.update(_NSE_HEADERS)
    try:
        s.get("https://www.nseindia.com", timeout=10)
    except Exception:
        pass
    return s


# ---------------------------------------------------------------------------
# FII/DII data
# ---------------------------------------------------------------------------

def fetch_fii_dii_data(days: int = 30, progress_callback=None) -> pd.DataFrame:
    """Fetch FII/DII daily activity data."""
    conn = get_db()

    if progress_callback:
        progress_callback(0.1, "Checking cached FII/DII data...")

    # Check cache
    cached = pd.read_sql("SELECT * FROM fii_dii ORDER BY trade_date DESC", conn)
    if len(cached) >= days:
        conn.close()
        if progress_callback:
            progress_callback(1.0, "FII/DII data loaded from cache")
        return cached.head(days)

    # Try NSE API
    try:
        if progress_callback:
            progress_callback(0.3, "Fetching FII/DII data from NSE...")
        session = _nse_session()
        url = "https://www.nseindia.com/api/fiidiiTradeReact"
        resp = session.get(url, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            rows = []
            for item in data:
                try:
                    trade_date = dt.datetime.strptime(item.get("date", ""), "%d-%b-%Y").date()
                    category = item.get("category", "")
                    buy = float(str(item.get("buyValue", 0)).replace(",", ""))
                    sell = float(str(item.get("sellValue", 0)).replace(",", ""))
                    net = buy - sell

                    existing = [r for r in rows if r["trade_date"] == trade_date.isoformat()]
                    if existing:
                        row = existing[0]
                    else:
                        row = {"trade_date": trade_date.isoformat(),
                               "fii_buy": 0, "fii_sell": 0, "fii_net": 0,
                               "dii_buy": 0, "dii_sell": 0, "dii_net": 0}
                        rows.append(row)

                    if "FII" in category.upper() or "FPI" in category.upper():
                        row["fii_buy"] = buy
                        row["fii_sell"] = sell
                        row["fii_net"] = net
                    elif "DII" in category.upper():
                        row["dii_buy"] = buy
                        row["dii_sell"] = sell
                        row["dii_net"] = net
                except Exception:
                    continue

            if rows:
                df = pd.DataFrame(rows)
                for _, r in df.iterrows():
                    conn.execute("""
                        INSERT OR REPLACE INTO fii_dii
                        (trade_date, fii_buy, fii_sell, fii_net, dii_buy, dii_sell, dii_net)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (r["trade_date"], r["fii_buy"], r["fii_sell"], r["fii_net"],
                          r["dii_buy"], r["dii_sell"], r["dii_net"]))
                conn.commit()
    except Exception:
        pass

    if progress_callback:
        progress_callback(0.9, "Loading FII/DII results...")

    result = pd.read_sql(
        "SELECT * FROM fii_dii ORDER BY trade_date DESC LIMIT ?", conn, params=(days,)
    )
    conn.close()
    if progress_callback:
        progress_callback(1.0, "FII/DII data ready")
    return result


# ---------------------------------------------------------------------------
# Bulk / Block deals
# ---------------------------------------------------------------------------

def fetch_bulk_deals(days: int = 30, progress_callback=None) -> pd.DataFrame:
    """Fetch recent bulk and block deals from NSE."""
    conn = get_db()

    if progress_callback:
        progress_callback(0.1, "Checking cached bulk deals...")

    # Check cache
    cutoff = (dt.date.today() - dt.timedelta(days=days)).isoformat()
    cached = pd.read_sql(
        "SELECT * FROM bulk_deals WHERE trade_date >= ? ORDER BY trade_date DESC",
        conn, params=(cutoff,)
    )
    if len(cached) > 0:
        conn.close()
        if progress_callback:
            progress_callback(1.0, "Bulk deals loaded from cache")
        return cached

    # Try NSE API
    try:
        if progress_callback:
            progress_callback(0.3, "Fetching bulk deals from NSE...")
        session = _nse_session()
        for deal_type, url in [
            ("BULK", "https://www.nseindia.com/api/snapshot-capital-market-largedeal"),
        ]:
            try:
                resp = session.get(url, timeout=15)
                if resp.status_code != 200:
                    continue
                data = resp.json()
                items = data if isinstance(data, list) else data.get("data", data.get("BLOCK_DEALS_DATA", []))
                for item in items:
                    try:
                        symbol = item.get("symbol", item.get("sym", "")).strip()
                        client = item.get("clientName", item.get("clientname", "")).strip()
                        qty = int(float(str(item.get("qty", item.get("quantity", 0))).replace(",", "")))
                        price = float(str(item.get("wap", item.get("price", item.get("avgPrice", 0)))).replace(",", ""))
                        trade_date_str = item.get("date", item.get("dealDate", ""))
                        try:
                            td = dt.datetime.strptime(trade_date_str, "%d-%b-%Y").date()
                        except Exception:
                            td = dt.date.today()

                        btype = item.get("buyOrSell", item.get("buySell", deal_type))
                        conn.execute("""
                            INSERT INTO bulk_deals (trade_date, symbol, client_name, deal_type, quantity, price)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (td.isoformat(), symbol, client, btype, qty, price))
                    except Exception:
                        continue
                conn.commit()
            except Exception:
                continue
    except Exception:
        pass

    if progress_callback:
        progress_callback(0.9, "Loading bulk deals results...")

    result = pd.read_sql(
        "SELECT * FROM bulk_deals WHERE trade_date >= ? ORDER BY trade_date DESC",
        conn, params=(cutoff,)
    )
    conn.close()
    if progress_callback:
        progress_callback(1.0, "Bulk deals ready")
    return result


# ---------------------------------------------------------------------------
# Promoter holding + pledge data
# ---------------------------------------------------------------------------

def fetch_promoter_data(force_refresh=False, progress_callback=None) -> pd.DataFrame:
    """Fetch promoter holding and pledge data from NSE shareholding API.

    Args:
        force_refresh: If True, re-fetch even if cached data exists.
        progress_callback: Optional callable(pct, msg) for progress updates.
    """
    import time as _time

    conn = get_db()

    if progress_callback:
        progress_callback(0.05, "Checking cached promoter data...")

    if not force_refresh:
        cached = pd.read_sql("SELECT * FROM promoter_data", conn)
        if len(cached) > 0:
            conn.close()
            if progress_callback:
                progress_callback(1.0, "Promoter data loaded from cache")
            return cached

    # Get Nifty 500 symbols only (not all ~2,200 NSE stocks) for speed
    nifty500_syms = set()
    try:
        n500_url = "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv"
        n500_resp = requests.get(n500_url, headers=_NSE_HEADERS, timeout=15)
        if n500_resp.status_code == 200 and len(n500_resp.text) > 200:
            n500_df = pd.read_csv(io.StringIO(n500_resp.text))
            nifty500_syms = set(n500_df["Symbol"].str.strip().tolist())
    except Exception:
        pass

    symbols_df = pd.read_sql(
        "SELECT DISTINCT symbol FROM ohlcv ORDER BY symbol", conn
    )
    if symbols_df.empty:
        conn.close()
        return pd.DataFrame(columns=[
            "symbol", "quarter", "promoter_holding_pct", "pledge_pct",
            "fii_holding_pct", "dii_holding_pct", "public_holding_pct"
        ])

    all_symbols = symbols_df["symbol"].tolist()
    # Filter to Nifty 500 if available, else fall back to all
    symbols = [s for s in all_symbols if s in nifty500_syms] if nifty500_syms else all_symbols

    # Already-cached symbols (skip unless force_refresh)
    if not force_refresh:
        cached_syms = set(
            pd.read_sql("SELECT DISTINCT symbol FROM promoter_data", conn)["symbol"]
        )
        symbols = [s for s in symbols if s not in cached_syms]

    session = _nse_session()
    total = len(symbols)
    fetched = 0

    for i, symbol in enumerate(symbols):
        try:
            url = (
                "https://www.nseindia.com/api/corporates-shareholding"
                "?index=equities&symbol=" + requests.utils.quote(symbol)
            )
            resp = session.get(url, timeout=15)
            if resp.status_code != 200:
                _time.sleep(0.5)
                continue

            data = resp.json()
            # NSE returns a list of quarterly records
            records = data if isinstance(data, list) else data.get("data", [])

            for rec in records:
                try:
                    quarter = rec.get("date", rec.get("quarter", ""))
                    promoter = float(rec.get("promoterAndPromoterGroup", 0))
                    pledge = float(rec.get("promoterPledge", rec.get("pledgedPercentage", 0)))
                    fii = float(rec.get("foreignInstitutions", rec.get("fiiOrFpi", 0)))
                    dii = float(rec.get("mutualFunds", 0)) + float(rec.get("financialInstitutionsOrBanks", 0))
                    public = float(rec.get("publicShareholding", rec.get("public", 0)))

                    if not quarter or promoter == 0:
                        continue

                    conn.execute("""
                        INSERT OR REPLACE INTO promoter_data
                        (symbol, quarter, promoter_holding_pct, pledge_pct,
                         fii_holding_pct, dii_holding_pct, public_holding_pct)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (symbol, quarter, promoter, pledge, fii, dii, public))
                    fetched += 1
                except (ValueError, TypeError):
                    continue

            conn.commit()
        except Exception:
            pass

        if progress_callback and total > 0:
            progress_callback((i + 1) / total, f"Fetched {i + 1}/{total} symbols")

        _time.sleep(0.5)  # Rate-limit

    result = pd.read_sql("SELECT * FROM promoter_data", conn)
    conn.close()
    return result


# ---------------------------------------------------------------------------
# Sector indices
# ---------------------------------------------------------------------------

def fetch_sector_indices(days: int = 180, progress_callback=None) -> pd.DataFrame:
    """Fetch sectoral index data using yfinance as fallback."""
    conn = get_db()

    if progress_callback:
        progress_callback(0.05, "Checking cached sector data...")

    cutoff = (dt.date.today() - dt.timedelta(days=days)).isoformat()
    cached = pd.read_sql(
        "SELECT * FROM sector_indices WHERE trade_date >= ?",
        conn, params=(cutoff,)
    )
    if len(cached) > 50:
        conn.close()
        if progress_callback:
            progress_callback(1.0, "Sector data loaded from cache")
        return cached

    # Use yfinance for sector indices
    sector_tickers = {
        "Nifty Bank": "^NSEBANK",
        "Nifty IT": "^CNXIT",
        "Nifty Pharma": "^CNXPHARMA",
        "Nifty Metal": "^CNXMETAL",
        "Nifty Realty": "^CNXREALTY",
        "Nifty Auto": "^CNXAUTO",
        "Nifty FMCG": "^CNXFMCG",
        "Nifty Energy": "^CNXENERGY",
        "Nifty PSU Bank": "^CNXPSUBANK",
        "Nifty Financial": "^CNXFINANCE",
        "Nifty 50": "^NSEI",
    }

    try:
        import yfinance as yf
        start = dt.date.today() - dt.timedelta(days=days)
        total_sectors = len(sector_tickers)
        for idx, (name, ticker) in enumerate(sector_tickers.items()):
            if progress_callback:
                progress_callback(0.1 + 0.8 * idx / total_sectors,
                                  f"Downloading {name} ({idx + 1}/{total_sectors})...")
            try:
                data = yf.download(ticker, start=start, progress=False)
                if data is not None and len(data) > 0:
                    for idx, row in data.iterrows():
                        close_val = row["Close"]
                        if hasattr(close_val, 'iloc'):
                            close_val = close_val.iloc[0]
                        td = idx.date() if hasattr(idx, 'date') else idx
                        conn.execute("""
                            INSERT OR REPLACE INTO sector_indices (index_name, trade_date, close)
                            VALUES (?, ?, ?)
                        """, (name, td.isoformat(), float(close_val)))
                conn.commit()
            except Exception:
                continue
    except ImportError:
        pass

    if progress_callback:
        progress_callback(0.95, "Loading sector results...")

    result = pd.read_sql(
        "SELECT * FROM sector_indices WHERE trade_date >= ? ORDER BY index_name, trade_date",
        conn, params=(cutoff,)
    )
    conn.close()
    if progress_callback:
        progress_callback(1.0, "Sector data ready")
    return result


def get_india_vix():
    """Fetch current India VIX value."""
    try:
        import yfinance as yf
        vix = yf.download("^INDIAVIX", period="5d", progress=False)
        if vix is not None and len(vix) > 0:
            val = vix["Close"].iloc[-1]
            if hasattr(val, 'iloc'):
                val = val.iloc[0]
            return round(float(val), 2)
    except Exception:
        pass

    try:
        session = _nse_session()
        resp = session.get("https://www.nseindia.com/api/allIndices", timeout=10)
        if resp.status_code == 200:
            data = resp.json().get("data", [])
            for item in data:
                if "VIX" in item.get("index", "").upper():
                    return round(float(item.get("last", 0)), 2)
    except Exception:
        pass

    return None
