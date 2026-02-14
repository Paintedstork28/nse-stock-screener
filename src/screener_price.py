"""Tab 1: Big Drops — Tab 2: Range-Bound (Sideways Movers)."""

import pandas as pd
import numpy as np
from src.indicators import rsi, bollinger_bands, sma


def screen_big_drops(ohlcv: pd.DataFrame, threshold_pct: float = 20.0) -> pd.DataFrame:
    """Find stocks that dropped threshold_pct% or more from their 6-month high.

    Args:
        ohlcv: Full OHLCV DataFrame with columns [symbol, trade_date, open, high, low, close, volume]
        threshold_pct: Minimum drop percentage (default 20%)

    Returns:
        DataFrame with columns: Symbol, Current Price, 6M High, Drop %, RSI
    """
    results = []

    for symbol, group in ohlcv.groupby("symbol"):
        group = group.sort_values("trade_date").reset_index(drop=True)
        if len(group) < 20:
            continue

        high_6m = group["high"].max()
        current_price = group["close"].iloc[-1]

        if high_6m <= 0:
            continue

        drop_pct = ((high_6m - current_price) / high_6m) * 100

        if drop_pct >= threshold_pct:
            rsi_val = rsi(group["close"], 14)
            last_rsi = rsi_val.iloc[-1] if not rsi_val.isna().iloc[-1] else None

            results.append({
                "Symbol": symbol,
                "Current Price": round(current_price, 2),
                "6M High": round(high_6m, 2),
                "Drop %": round(drop_pct, 1),
                "RSI": round(last_rsi, 1) if last_rsi is not None else None,
            })

    if not results:
        return pd.DataFrame(columns=["Symbol", "Current Price", "6M High", "Drop %", "RSI"])

    df = pd.DataFrame(results).sort_values("Drop %", ascending=False).reset_index(drop=True)
    return df


def screen_range_bound(ohlcv: pd.DataFrame, range_pct: float = 5.0,
                       min_days: int = 10) -> pd.DataFrame:
    """Find stocks trading in a tight range for consecutive sessions.

    Args:
        ohlcv: Full OHLCV DataFrame
        range_pct: Maximum range width as % of midpoint (default 5%)
        min_days: Minimum consecutive days in range (default 10)

    Returns:
        DataFrame with columns: Symbol, Range Low, Range High, Midpoint, Range Width %, BB Bandwidth, Days in Range
    """
    results = []

    for symbol, group in ohlcv.groupby("symbol"):
        group = group.sort_values("trade_date").reset_index(drop=True)
        if len(group) < min_days + 5:
            continue

        close = group["close"]

        # Check last N days for range-bound behavior
        for window in range(min(len(close), 60), min_days - 1, -1):
            recent = close.iloc[-window:]
            range_high = recent.max()
            range_low = recent.min()
            midpoint = (range_high + range_low) / 2

            if midpoint <= 0:
                continue

            width_pct = ((range_high - range_low) / midpoint) * 100

            if width_pct <= range_pct:
                # Calculate Bollinger bandwidth
                _, _, _, bw, _ = bollinger_bands(close, 20, 2.0)
                last_bw = bw.iloc[-1] if not bw.isna().iloc[-1] else None

                results.append({
                    "Symbol": symbol,
                    "Range Low": round(range_low, 2),
                    "Range High": round(range_high, 2),
                    "Midpoint": round(midpoint, 2),
                    "Range Width %": round(width_pct, 1),
                    "BB Bandwidth": round(last_bw, 2) if last_bw is not None else None,
                    "Days in Range": window,
                })
                break  # Take the longest range found

    if not results:
        return pd.DataFrame(columns=["Symbol", "Range Low", "Range High", "Midpoint",
                                      "Range Width %", "BB Bandwidth", "Days in Range"])

    df = pd.DataFrame(results).sort_values("BB Bandwidth", ascending=True).reset_index(drop=True)
    return df
