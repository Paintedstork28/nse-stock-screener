"""Tab 5: Smart Money — bulk deals, delivery breakouts, OBV divergence."""

import pandas as pd
import numpy as np
from src.indicators import obv, sma


def get_bulk_deals_summary(bulk_deals_df: pd.DataFrame) -> pd.DataFrame:
    """Summarize recent bulk/block deals.

    Args:
        bulk_deals_df: DataFrame from data_extras.fetch_bulk_deals()

    Returns:
        Cleaned DataFrame with deal info
    """
    if bulk_deals_df is None or bulk_deals_df.empty:
        return pd.DataFrame(columns=["Date", "Symbol", "Client", "Type", "Qty", "Price"])

    df = bulk_deals_df.copy()
    rename_map = {}
    for c in df.columns:
        cl = c.lower()
        if "date" in cl:
            rename_map[c] = "Date"
        elif c.lower() == "symbol":
            rename_map[c] = "Symbol"
        elif "client" in cl:
            rename_map[c] = "Client"
        elif "deal" in cl or "type" in cl or "buy" in cl:
            rename_map[c] = "Type"
        elif "quant" in cl or "qty" in cl:
            rename_map[c] = "Qty"
        elif "price" in cl or "wap" in cl:
            rename_map[c] = "Price"

    df = df.rename(columns=rename_map)
    cols = [c for c in ["Date", "Symbol", "Client", "Type", "Qty", "Price"] if c in df.columns]
    return df[cols].reset_index(drop=True) if cols else df


def screen_delivery_breakouts(ohlcv: pd.DataFrame, multiplier: float = 2.0) -> pd.DataFrame:
    """Find stocks where delivery qty is significantly above average.

    Delivery qty > multiplier × 20-day average delivery = accumulation signal.

    Returns:
        DataFrame with columns: Symbol, Price, Avg Delivery Qty, Recent Delivery Qty,
                                Delivery Ratio, Price Change %
    """
    results = []

    for symbol, group in ohlcv.groupby("symbol"):
        group = group.sort_values("trade_date").reset_index(drop=True)
        if len(group) < 25:
            continue

        deliv = group["delivery_qty"].fillna(0)
        if deliv.sum() == 0:
            continue

        avg_delivery = deliv.iloc[-21:-1].mean()
        if avg_delivery <= 0:
            continue

        recent_delivery = deliv.iloc[-1]
        ratio = recent_delivery / avg_delivery

        if ratio >= multiplier:
            current_price = group["close"].iloc[-1]
            prev_price = group["close"].iloc[-2] if len(group) > 1 else current_price
            price_change = ((current_price - prev_price) / prev_price) * 100 if prev_price > 0 else 0

            results.append({
                "Symbol": symbol,
                "Price": round(current_price, 2),
                "Avg Delivery Qty": int(avg_delivery),
                "Recent Delivery Qty": int(recent_delivery),
                "Delivery Ratio": round(ratio, 1),
                "Price Change %": round(price_change, 1),
            })

    if not results:
        return pd.DataFrame(columns=["Symbol", "Price", "Avg Delivery Qty",
                                      "Recent Delivery Qty", "Delivery Ratio", "Price Change %"])

    return pd.DataFrame(results).sort_values("Delivery Ratio", ascending=False).reset_index(drop=True)


def screen_obv_divergence(ohlcv: pd.DataFrame, lookback: int = 20) -> pd.DataFrame:
    """Find stocks where OBV is rising while price is flat or falling.
    This indicates accumulation (smart money buying).

    Returns:
        DataFrame with columns: Symbol, Price, Price Change %, OBV Change %, Signal
    """
    results = []

    for symbol, group in ohlcv.groupby("symbol"):
        group = group.sort_values("trade_date").reset_index(drop=True)
        if len(group) < lookback + 5:
            continue

        close = group["close"]
        vol = group["volume"].fillna(0)

        if vol.sum() == 0:
            continue

        obv_series = obv(close, vol)

        # Price change over lookback period
        price_start = close.iloc[-lookback]
        price_end = close.iloc[-1]
        if price_start <= 0:
            continue
        price_change = ((price_end - price_start) / price_start) * 100

        # OBV change over lookback period
        obv_start = obv_series.iloc[-lookback]
        obv_end = obv_series.iloc[-1]

        # Normalize OBV change relative to average volume
        avg_vol = vol.mean()
        if avg_vol <= 0:
            continue
        obv_change_normalized = ((obv_end - obv_start) / (avg_vol * lookback)) * 100

        # Bullish divergence: price flat/down but OBV rising
        if price_change <= 2 and obv_change_normalized > 10:
            results.append({
                "Symbol": symbol,
                "Price": round(price_end, 2),
                "Price Change %": round(price_change, 1),
                "OBV Trend": "Rising",
                "Signal": "Accumulation (Bullish)",
            })

    if not results:
        return pd.DataFrame(columns=["Symbol", "Price", "Price Change %", "OBV Trend", "Signal"])

    return pd.DataFrame(results).sort_values("Price Change %", ascending=True).reset_index(drop=True)
