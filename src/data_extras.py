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

def fetch_fii_dii_data(days: int = 30) -> pd.DataFrame:
    """Fetch FII/DII daily activity data."""
    conn = get_db()

    # Check cache
    cached = pd.read_sql("SELECT * FROM fii_dii ORDER BY trade_date DESC", conn)
    if len(cached) >= days:
        conn.close()
        return cached.head(days)

    # Try NSE API
    try:
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

    result = pd.read_sql(
        "SELECT * FROM fii_dii ORDER BY trade_date DESC LIMIT ?", conn, params=(days,)
    )
    conn.close()
    return result


# ---------------------------------------------------------------------------
# Bulk / Block deals
# ---------------------------------------------------------------------------

def fetch_bulk_deals(days: int = 30) -> pd.DataFrame:
    """Fetch recent bulk and block deals from NSE."""
    conn = get_db()

    # Check cache
    cutoff = (dt.date.today() - dt.timedelta(days=days)).isoformat()
    cached = pd.read_sql(
        "SELECT * FROM bulk_deals WHERE trade_date >= ? ORDER BY trade_date DESC",
        conn, params=(cutoff,)
    )
    if len(cached) > 0:
        conn.close()
        return cached

    # Try NSE API
    try:
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

    result = pd.read_sql(
        "SELECT * FROM bulk_deals WHERE trade_date >= ? ORDER BY trade_date DESC",
        conn, params=(cutoff,)
    )
    conn.close()
    return result


# ---------------------------------------------------------------------------
# Promoter holding + pledge data
# ---------------------------------------------------------------------------

def fetch_promoter_data() -> pd.DataFrame:
    """Fetch promoter holding and pledge data."""
    conn = get_db()

    cached = pd.read_sql("SELECT * FROM promoter_data", conn)
    if len(cached) > 0:
        conn.close()
        return cached

    # Generate sample quarterly data structure — real data needs NSDL/BSE API
    # Placeholder: return empty DataFrame with correct schema
    conn.close()
    return pd.DataFrame(columns=[
        "symbol", "quarter", "promoter_holding_pct", "pledge_pct",
        "fii_holding_pct", "dii_holding_pct", "public_holding_pct"
    ])


# ---------------------------------------------------------------------------
# Sector indices
# ---------------------------------------------------------------------------

def fetch_sector_indices(days: int = 180) -> pd.DataFrame:
    """Fetch sectoral index data using yfinance as fallback."""
    conn = get_db()

    cutoff = (dt.date.today() - dt.timedelta(days=days)).isoformat()
    cached = pd.read_sql(
        "SELECT * FROM sector_indices WHERE trade_date >= ?",
        conn, params=(cutoff,)
    )
    if len(cached) > 50:
        conn.close()
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
        for name, ticker in sector_tickers.items():
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

    result = pd.read_sql(
        "SELECT * FROM sector_indices WHERE trade_date >= ? ORDER BY index_name, trade_date",
        conn, params=(cutoff,)
    )
    conn.close()
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
