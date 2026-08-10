"""Alert Ticker: bullish accumulation pattern screener."""

import pandas as pd
from src.indicators import sma


def screen_alert_ticker(ohlcv: pd.DataFrame) -> pd.DataFrame:
    """Find stocks showing bullish accumulation over the last 3 trading days.

    Criteria (all must be true):
    1. Volume > 150% of 20-day average volume on each of the last 3 days
    2. Close > 50-day moving average on the latest day
    3. Closing price rose day-over-day on each of the last 3 days

    Returns:
        DataFrame with columns: Symbol, Current Price, 3D Change %
        Sorted by 3D Change % descending.
    """
    results = []

    for symbol, group in ohlcv.groupby("symbol"):
        group = group.sort_values("trade_date").reset_index(drop=True)
        if len(group) < 50:
            continue

        close = group["close"]
        volume = group["volume"].fillna(0)

        # 20-day volume SMA and 50-day price SMA
        vol_sma20 = sma(volume, 20)
        price_sma50 = sma(close, 50)

        # Need valid values for the last 3 days
        if vol_sma20.iloc[-4:].isna().any() or pd.isna(price_sma50.iloc[-1]):
            continue

        # Check all 3 criteria over the last 3 days
        last3_close = close.iloc[-3:].values
        last3_vol = volume.iloc[-3:].values
        last3_vol_sma = vol_sma20.iloc[-3:].values

        # 1. Volume > 150% of 20-day avg each day
        if not all(v > 1.5 * avg for v, avg in zip(last3_vol, last3_vol_sma)):
            continue

        # 2. Close > 50 DMA on latest day
        if close.iloc[-1] <= price_sma50.iloc[-1]:
            continue

        # 3. Price rose each of the last 3 days (day-over-day)
        prev_closes = close.iloc[-4:-1].values  # days before the last 3
        if not all(curr > prev for curr, prev in zip(last3_close, prev_closes)):
            continue

        current_price = close.iloc[-1]
        price_3d_ago = close.iloc[-4]
        change_pct = ((current_price - price_3d_ago) / price_3d_ago) * 100

        results.append({
            "Symbol": symbol,
            "Current Price": round(current_price, 2),
            "3D Change %": round(change_pct, 2),
        })

    if not results:
        return pd.DataFrame(columns=["Symbol", "Current Price", "3D Change %"])

    return pd.DataFrame(results).sort_values("3D Change %", ascending=False).reset_index(drop=True)
