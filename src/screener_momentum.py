"""Tab 4: Momentum Leaders — stocks in strong uptrends."""

import pandas as pd
import numpy as np
from src.indicators import rsi, supertrend, sma


def screen_momentum_leaders(ohlcv: pd.DataFrame) -> pd.DataFrame:
    """Find stocks showing strong momentum characteristics:
    - At or near 52-week / 6-month high
    - Above 50 AND 200 DMA
    - RSI between 55-75 (strong but not overbought)
    - Supertrend = BUY

    Returns:
        DataFrame with columns: Symbol, Price, 6M High, Dist from High %,
                                RSI, Supertrend, Above 50 DMA, Above 200 DMA
    """
    results = []

    for symbol, group in ohlcv.groupby("symbol"):
        group = group.sort_values("trade_date").reset_index(drop=True)
        if len(group) < 60:
            continue

        close = group["close"]
        high = group["high"]
        low = group["low"]
        current_price = close.iloc[-1]

        if current_price <= 0:
            continue

        # 6M high
        high_6m = high.max()
        dist_from_high = ((high_6m - current_price) / high_6m) * 100

        # Must be within 10% of 6M high
        if dist_from_high > 10:
            continue

        # Moving averages
        dma50 = sma(close, 50)
        dma200 = sma(close, 200)

        dma50_val = dma50.iloc[-1] if not dma50.isna().iloc[-1] else None
        dma200_val = dma200.iloc[-1] if not dma200.isna().iloc[-1] else None

        above_50 = dma50_val is not None and current_price > dma50_val
        above_200 = dma200_val is not None and current_price > dma200_val

        if not (above_50 and above_200):
            continue

        # RSI
        rsi_val = rsi(close, 14)
        last_rsi = rsi_val.iloc[-1] if not rsi_val.isna().iloc[-1] else None
        if last_rsi is None or last_rsi < 55 or last_rsi > 75:
            continue

        # Supertrend
        st = supertrend(high, low, close, 10, 3.0)
        last_st = st.iloc[-1] if st.iloc[-1] in ("BUY", "SELL") else None
        if last_st != "BUY":
            continue

        results.append({
            "Symbol": symbol,
            "Price": round(current_price, 2),
            "6M High": round(high_6m, 2),
            "Dist from High %": round(dist_from_high, 1),
            "RSI": round(last_rsi, 1),
            "Supertrend": last_st,
            "50 DMA": round(dma50_val, 2),
            "200 DMA": round(dma200_val, 2),
        })

    if not results:
        return pd.DataFrame(columns=["Symbol", "Price", "6M High", "Dist from High %",
                                      "RSI", "Supertrend", "50 DMA", "200 DMA"])

    df = pd.DataFrame(results).sort_values("Dist from High %", ascending=True).reset_index(drop=True)
    return df
