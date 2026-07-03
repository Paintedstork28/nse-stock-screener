"""Fetch Nifty index constituent lists from niftyindices.com."""

import io
import requests
import pandas as pd
import streamlit as st

_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/csv,text/plain,*/*",
}

# Index name -> CSV filename on niftyindices.com
_INDEX_CSV_MAP = {
    "Nifty 50": "ind_nifty50list.csv",
    "Nifty 100": "ind_nifty100list.csv",
    "Nifty 200": "ind_nifty200list.csv",
    "Nifty 500": "ind_nifty500list.csv",
    "Nifty Midcap 150": "ind_niftymidcap150list.csv",
    "Nifty Smallcap 250": "ind_niftysmallcap250list.csv",
}

UNIVERSE_OPTIONS = list(_INDEX_CSV_MAP.keys())


@st.cache_data(ttl=3600)
def fetch_universe(index_name: str) -> list[str]:
    """Return list of stock symbols for a given Nifty index.

    Args:
        index_name: One of UNIVERSE_OPTIONS (e.g. "Nifty 50").

    Returns:
        List of NSE symbols (e.g. ["RELIANCE", "TCS", ...]).
        Empty list on failure.
    """
    csv_file = _INDEX_CSV_MAP.get(index_name)
    if not csv_file:
        return []

    url = f"https://www.niftyindices.com/IndexConstituent/{csv_file}"
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=15)
        if resp.status_code != 200 or len(resp.text) < 100:
            return []

        df = pd.read_csv(io.StringIO(resp.text))

        # Column name varies across CSVs — try common variants
        sym_col = None
        for candidate in ["Symbol", "symbol", "SYMBOL"]:
            if candidate in df.columns:
                sym_col = candidate
                break

        if sym_col is None:
            # Fallback: first column that looks like symbols
            for col in df.columns:
                sample = df[col].dropna().iloc[:5].tolist()
                if all(isinstance(v, str) and v.isalpha() for v in sample):
                    sym_col = col
                    break

        if sym_col is None:
            return []

        symbols = df[sym_col].str.strip().tolist()
        return [s for s in symbols if isinstance(s, str) and len(s) > 0]

    except Exception:
        return []
