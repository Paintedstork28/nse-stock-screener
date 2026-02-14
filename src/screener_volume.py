"""Tab 3: Volume Spikes with delivery % analysis."""

import pandas as pd
import numpy as np


def screen_volume_spikes(ohlcv: pd.DataFrame, vol_threshold_pct: float = 50.0,
                         consecutive_days: int = 3) -> pd.DataFrame:
    """Find stocks with sustained volume spikes.

    Args:
        ohlcv: Full OHLCV DataFrame
        vol_threshold_pct: Volume must be this % above 6M average (default 50%)
        consecutive_days: Number of consecutive high-volume days required (default 3)

    Returns:
        DataFrame with columns: Symbol, Avg Volume, Last 3D Avg Vol, Vol Ratio,
                                Avg Delivery %, Recent Delivery %, Price Change %
    """
    results = []
    vol_multiplier = 1 + vol_threshold_pct / 100

    for symbol, group in ohlcv.groupby("symbol"):
        group = group.sort_values("trade_date").reset_index(drop=True)
        if len(group) < 30:
            continue

        volume = group["volume"].fillna(0)
        avg_vol = volume.iloc[:-consecutive_days].mean()

        if avg_vol <= 0:
            continue

        # Check last N consecutive days
        recent_vols = volume.iloc[-consecutive_days:]
        if (recent_vols <= 0).any():
            continue

        all_above = all(v >= avg_vol * vol_multiplier for v in recent_vols)
        if not all_above:
            continue

        recent_avg_vol = recent_vols.mean()
        vol_ratio = recent_avg_vol / avg_vol

        # Delivery analysis
        delivery_pct = group["delivery_pct"].fillna(0)
        avg_delivery = delivery_pct.iloc[:-consecutive_days].mean()
        recent_delivery = delivery_pct.iloc[-consecutive_days:].mean()

        # Price change over the spike period
        price_start = group["close"].iloc[-consecutive_days - 1] if len(group) > consecutive_days else group["close"].iloc[0]
        price_end = group["close"].iloc[-1]
        price_change = ((price_end - price_start) / price_start) * 100 if price_start > 0 else 0

        results.append({
            "Symbol": symbol,
            "Current Price": round(price_end, 2),
            "Avg Volume": int(avg_vol),
            f"Last {consecutive_days}D Avg Vol": int(recent_avg_vol),
            "Vol Ratio": round(vol_ratio, 1),
            "Avg Delivery %": round(avg_delivery, 1),
            "Recent Delivery %": round(recent_delivery, 1),
            "Delivery Above Avg": "Yes" if recent_delivery > avg_delivery else "No",
            "Price Change %": round(price_change, 1),
        })

    if not results:
        cols = ["Symbol", "Current Price", "Avg Volume", f"Last {consecutive_days}D Avg Vol",
                "Vol Ratio", "Avg Delivery %", "Recent Delivery %", "Delivery Above Avg",
                "Price Change %"]
        return pd.DataFrame(columns=cols)

    df = pd.DataFrame(results).sort_values("Vol Ratio", ascending=False).reset_index(drop=True)
    return df
