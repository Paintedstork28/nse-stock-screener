"""Date helpers, constants, and formatting utilities."""

import datetime as dt

# ---------------------------------------------------------------------------
# Trading calendar helpers
# ---------------------------------------------------------------------------

MARKET_HOLIDAYS_2024_25 = {
    dt.date(2024, 1, 26), dt.date(2024, 3, 8), dt.date(2024, 3, 25),
    dt.date(2024, 3, 29), dt.date(2024, 4, 11), dt.date(2024, 4, 14),
    dt.date(2024, 4, 17), dt.date(2024, 4, 21), dt.date(2024, 5, 1),
    dt.date(2024, 5, 20), dt.date(2024, 5, 23), dt.date(2024, 6, 17),
    dt.date(2024, 7, 17), dt.date(2024, 8, 15), dt.date(2024, 9, 16),
    dt.date(2024, 10, 1), dt.date(2024, 10, 2), dt.date(2024, 10, 12),
    dt.date(2024, 10, 31), dt.date(2024, 11, 1), dt.date(2024, 11, 15),
    dt.date(2024, 12, 25),
    dt.date(2025, 1, 26), dt.date(2025, 2, 26), dt.date(2025, 3, 14),
    dt.date(2025, 3, 31), dt.date(2025, 4, 10), dt.date(2025, 4, 14),
    dt.date(2025, 4, 18), dt.date(2025, 5, 1), dt.date(2025, 5, 12),
    dt.date(2025, 8, 15), dt.date(2025, 8, 16), dt.date(2025, 8, 27),
    dt.date(2025, 10, 2), dt.date(2025, 10, 20), dt.date(2025, 10, 21),
    dt.date(2025, 10, 22), dt.date(2025, 11, 5), dt.date(2025, 11, 26),
    dt.date(2025, 12, 25),
    dt.date(2026, 1, 26), dt.date(2026, 2, 17), dt.date(2026, 3, 10),
    dt.date(2026, 3, 20), dt.date(2026, 3, 25), dt.date(2026, 4, 3),
    dt.date(2026, 4, 14),
}


def is_trading_day(d: dt.date) -> bool:
    if d.weekday() >= 5:
        return False
    if d in MARKET_HOLIDAYS_2024_25:
        return False
    return True


def trading_days_between(start: dt.date, end: dt.date):
    days = []
    current = start
    while current <= end:
        if is_trading_day(current):
            days.append(current)
        current += dt.timedelta(days=1)
    return days


def last_n_trading_days(n: int, ref=None):
    ref = ref or dt.date.today()
    days = []
    current = ref
    while len(days) < n:
        if is_trading_day(current):
            days.append(current)
        current -= dt.timedelta(days=1)
    return list(reversed(days))


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def fmt_inr(val) -> str:
    """Format number as ₹ value."""
    if val is None:
        return "—"
    try:
        val = float(val)
    except (TypeError, ValueError):
        return "—"
    if abs(val) >= 1_00_00_000:
        return f"₹{val / 1_00_00_000:,.2f} Cr"
    if abs(val) >= 1_00_000:
        return f"₹{val / 1_00_000:,.2f} L"
    return f"₹{val:,.2f}"


def fmt_pct(val, decimals: int = 1) -> str:
    if val is None:
        return "—"
    return f"{float(val):.{decimals}f}%"


def fmt_vol(val) -> str:
    if val is None:
        return "—"
    val = float(val)
    if val >= 1_00_00_000:
        return f"{val / 1_00_00_000:,.2f} Cr"
    if val >= 1_00_000:
        return f"{val / 1_00_000:,.2f} L"
    if val >= 1000:
        return f"{val / 1000:,.1f}K"
    return f"{val:,.0f}"


# ---------------------------------------------------------------------------
# Sector mapping
# ---------------------------------------------------------------------------

SECTOR_INDICES = {
    "NIFTY BANK": "Nifty Bank",
    "NIFTY IT": "Nifty IT",
    "NIFTY PHARMA": "Nifty Pharma",
    "NIFTY METAL": "Nifty Metal",
    "NIFTY REALTY": "Nifty Realty",
    "NIFTY AUTO": "Nifty Auto",
    "NIFTY FMCG": "Nifty FMCG",
    "NIFTY ENERGY": "Nifty Energy",
    "NIFTY INFRA": "Nifty Infra",
    "NIFTY PSE": "Nifty PSE",
    "NIFTY MEDIA": "Nifty Media",
    "NIFTY PRIVATE BANK": "Nifty Pvt Bank",
    "NIFTY PSU BANK": "Nifty PSU Bank",
    "NIFTY HEALTHCARE INDEX": "Nifty Healthcare",
    "NIFTY CONSUMER DURABLES": "Nifty Consumer Dur",
    "NIFTY OIL AND GAS": "Nifty Oil & Gas",
    "NIFTY FINANCIAL SERVICES": "Nifty Financial",
}

import os as _os
_PROJECT_ROOT = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
DB_PATH = _os.path.join(_PROJECT_ROOT, "data", "screener.db")
LOOKBACK_MONTHS = 6
