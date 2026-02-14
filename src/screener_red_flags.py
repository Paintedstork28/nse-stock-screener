"""Tab 7: Red Flags — pledging, death cross, falling delivery, below all MAs."""

import pandas as pd
import numpy as np
from src.indicators import sma, golden_death_cross


def screen_high_pledge(promoter_df: pd.DataFrame, threshold_pct: float = 20.0) -> pd.DataFrame:
    """Find stocks with high promoter pledging.

    Args:
        promoter_df: DataFrame with columns [symbol, quarter, promoter_holding_pct, pledge_pct, ...]
        threshold_pct: Minimum pledge percentage to flag (default 20%)

    Returns:
        DataFrame with: Symbol, Promoter Holding %, Pledge %, Quarter
    """
    if promoter_df is None or promoter_df.empty:
        return pd.DataFrame(columns=["Symbol", "Promoter Holding %", "Pledge %", "Quarter"])

    df = promoter_df.copy()
    df["pledge_pct"] = pd.to_numeric(df["pledge_pct"], errors="coerce").fillna(0)

    # Get latest quarter per symbol
    latest = df.sort_values("quarter", ascending=False).drop_duplicates("symbol", keep="first")
    flagged = latest[latest["pledge_pct"] >= threshold_pct]

    if flagged.empty:
        return pd.DataFrame(columns=["Symbol", "Promoter Holding %", "Pledge %", "Quarter"])

    result = pd.DataFrame({
        "Symbol": flagged["symbol"],
        "Promoter Holding %": flagged["promoter_holding_pct"].round(1),
        "Pledge %": flagged["pledge_pct"].round(1),
        "Quarter": flagged["quarter"],
    }).sort_values("Pledge %", ascending=False).reset_index(drop=True)

    return result


def screen_death_cross(ohlcv: pd.DataFrame, lookback: int = 10) -> pd.DataFrame:
    """Find stocks where 50 DMA recently crossed below 200 DMA (death cross)."""
    results = []

    for symbol, group in ohlcv.groupby("symbol"):
        group = group.sort_values("trade_date").reset_index(drop=True)
        if len(group) < 210:
            continue

        close = group["close"]
        cross = golden_death_cross(close, lookback=lookback)

        if cross == "DEATH":
            current_price = close.iloc[-1]
            dma50 = sma(close, 50).iloc[-1]
            dma200 = sma(close, 200).iloc[-1]

            results.append({
                "Symbol": symbol,
                "Price": round(current_price, 2),
                "50 DMA": round(dma50, 2) if not np.isnan(dma50) else None,
                "200 DMA": round(dma200, 2) if not np.isnan(dma200) else None,
                "Flag": "Death Cross",
            })

    if not results:
        return pd.DataFrame(columns=["Symbol", "Price", "50 DMA", "200 DMA", "Flag"])

    return pd.DataFrame(results).reset_index(drop=True)


def screen_falling_delivery(ohlcv: pd.DataFrame, lookback: int = 10) -> pd.DataFrame:
    """Find stocks where delivery % is declining while price rises.
    This suggests a speculative rally without genuine buying.
    """
    results = []

    for symbol, group in ohlcv.groupby("symbol"):
        group = group.sort_values("trade_date").reset_index(drop=True)
        if len(group) < lookback + 5:
            continue

        recent = group.iloc[-lookback:]
        close = recent["close"]
        delivery = recent["delivery_pct"].fillna(0)

        if delivery.sum() == 0:
            continue

        # Price rising?
        price_change = ((close.iloc[-1] - close.iloc[0]) / close.iloc[0]) * 100 if close.iloc[0] > 0 else 0
        if price_change <= 3:  # Need meaningful price rise
            continue

        # Delivery declining? Use linear regression slope
        x = np.arange(len(delivery))
        y = delivery.values
        if np.std(y) == 0:
            continue
        slope = np.polyfit(x, y, 1)[0]

        if slope < -0.3:  # Meaningful decline in delivery %
            results.append({
                "Symbol": symbol,
                "Price": round(close.iloc[-1], 2),
                "Price Change %": round(price_change, 1),
                "Delivery % Start": round(delivery.iloc[0], 1),
                "Delivery % End": round(delivery.iloc[-1], 1),
                "Flag": "Speculative Rally",
            })

    if not results:
        return pd.DataFrame(columns=["Symbol", "Price", "Price Change %",
                                      "Delivery % Start", "Delivery % End", "Flag"])

    return pd.DataFrame(results).sort_values("Price Change %", ascending=False).reset_index(drop=True)


def screen_below_all_mas(ohlcv: pd.DataFrame) -> pd.DataFrame:
    """Find stocks trading below ALL major moving averages (20/50/100/200 DMA).
    These are in deep downtrends.
    """
    results = []

    for symbol, group in ohlcv.groupby("symbol"):
        group = group.sort_values("trade_date").reset_index(drop=True)
        if len(group) < 200:
            continue

        close = group["close"]
        current = close.iloc[-1]

        dma20 = sma(close, 20).iloc[-1]
        dma50 = sma(close, 50).iloc[-1]
        dma100 = sma(close, 100).iloc[-1]
        dma200 = sma(close, 200).iloc[-1]

        if any(np.isnan(v) for v in [dma20, dma50, dma100, dma200]):
            continue

        if current < dma20 and current < dma50 and current < dma100 and current < dma200:
            # How far below 200 DMA?
            dist = ((dma200 - current) / dma200) * 100

            results.append({
                "Symbol": symbol,
                "Price": round(current, 2),
                "20 DMA": round(dma20, 2),
                "50 DMA": round(dma50, 2),
                "200 DMA": round(dma200, 2),
                "Below 200 DMA %": round(dist, 1),
                "Flag": "Below All MAs",
            })

    if not results:
        return pd.DataFrame(columns=["Symbol", "Price", "20 DMA", "50 DMA",
                                      "200 DMA", "Below 200 DMA %", "Flag"])

    return pd.DataFrame(results).sort_values("Below 200 DMA %", ascending=False).reset_index(drop=True)
