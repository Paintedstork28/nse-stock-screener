"""Sharpe Ratio screener with DMA and circuit-hit filters."""

import pandas as pd
from src.indicators import sma, sharpe_ratio, circuit_hits

MIN_DAYS = 60  # Need at least ~3 months for meaningful Sharpe


def screen_sharpe(
    ohlcv: pd.DataFrame,
    universe_symbols: list[str],
    risk_free_rate: float = 0.07,
    dma_period: int = 200,
    circuit_threshold: int = 10,
) -> pd.DataFrame:
    """Screen stocks by Sharpe Ratio with DMA and circuit filters.

    Args:
        ohlcv: Full OHLCV DataFrame (all stocks).
        universe_symbols: List of symbols to screen (from universe.py).
        risk_free_rate: Annual risk-free rate for Sharpe calculation.
        dma_period: DMA period — remove stocks where close < DMA.
        circuit_threshold: Remove stocks with >= this many circuit hits.

    Returns:
        DataFrame sorted by Sharpe Ratio descending with columns:
        Symbol, Sharpe Ratio, Ann. Return, Ann. Volatility, Close,
        DMA Value, Circuit Hits
    """
    # Filter OHLCV to only universe symbols
    universe_set = set(universe_symbols)
    filtered = ohlcv[ohlcv["symbol"].isin(universe_set)]

    if filtered.empty:
        return pd.DataFrame(columns=[
            "Symbol", "Sharpe Ratio", "Ann. Return", "Ann. Volatility",
            "Close", "DMA Value", "Circuit Hits",
        ])

    results = []
    for symbol, group in filtered.groupby("symbol"):
        group = group.sort_values("trade_date").reset_index(drop=True)
        if len(group) < MIN_DAYS:
            continue

        close = group["close"]
        last_close = close.iloc[-1]

        # DMA filter
        dma_val = sma(close, dma_period)
        last_dma = dma_val.iloc[-1]
        if pd.isna(last_dma):
            # Not enough data for this DMA — skip filter but note it
            last_dma = None
        elif last_close < last_dma:
            continue  # Below DMA — filtered out

        # Circuit hits filter
        circuits = circuit_hits(close)
        if circuits >= circuit_threshold:
            continue

        # Sharpe ratio
        sr, ann_ret, ann_vol = sharpe_ratio(close, risk_free_rate)
        if sr is None:
            continue

        results.append({
            "Symbol": symbol,
            "Sharpe Ratio": round(sr, 3),
            "Ann. Return": round(ann_ret * 100, 2),
            "Ann. Volatility": round(ann_vol * 100, 2),
            "Close": round(last_close, 2),
            "DMA Value": round(last_dma, 2) if last_dma is not None else None,
            "Circuit Hits": circuits,
        })

    if not results:
        return pd.DataFrame(columns=[
            "Symbol", "Sharpe Ratio", "Ann. Return", "Ann. Volatility",
            "Close", "DMA Value", "Circuit Hits",
        ])

    df = pd.DataFrame(results)
    df = df.sort_values("Sharpe Ratio", ascending=False).reset_index(drop=True)
    return df
