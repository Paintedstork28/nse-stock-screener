"""Technical indicators: RSI, MACD, Bollinger, Supertrend, OBV, MAs, ATR."""

import numpy as np
import pandas as pd


def sma(series: pd.Series, period: int) -> pd.Series:
    return series.rolling(window=period, min_periods=period).mean()


def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def rsi(close: pd.Series, period: int = 14) -> pd.Series:
    """RSI using Wilder smoothing."""
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)

    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    """Returns (macd_line, signal_line, histogram)."""
    ema_fast = ema(close, fast)
    ema_slow = ema(close, slow)
    macd_line = ema_fast - ema_slow
    signal_line = ema(macd_line, signal)
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram


def bollinger_bands(close: pd.Series, period: int = 20, std_dev: float = 2.0):
    """Returns (upper, middle, lower, bandwidth, pct_b)."""
    middle = sma(close, period)
    std = close.rolling(window=period, min_periods=period).std()
    upper = middle + std_dev * std
    lower = middle - std_dev * std
    bandwidth = ((upper - lower) / middle) * 100
    pct_b = (close - lower) / (upper - lower)
    return upper, middle, lower, bandwidth, pct_b


def atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    """Average True Range."""
    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()


def supertrend(high: pd.Series, low: pd.Series, close: pd.Series,
               period: int = 10, multiplier: float = 3.0) -> pd.Series:
    """Supertrend indicator. Returns Series of 'BUY' or 'SELL'."""
    atr_val = atr(high, low, close, period)
    hl2 = (high + low) / 2
    upper_band = hl2 + multiplier * atr_val
    lower_band = hl2 - multiplier * atr_val

    n = len(close)
    supertrend_dir = pd.Series(index=close.index, dtype="object")
    final_upper = upper_band.copy()
    final_lower = lower_band.copy()

    for i in range(1, n):
        # Final upper band
        if final_upper.iloc[i] < final_upper.iloc[i - 1] or close.iloc[i - 1] > final_upper.iloc[i - 1]:
            final_upper.iloc[i] = final_upper.iloc[i]
        else:
            final_upper.iloc[i] = final_upper.iloc[i - 1]

        # Final lower band
        if final_lower.iloc[i] > final_lower.iloc[i - 1] or close.iloc[i - 1] < final_lower.iloc[i - 1]:
            final_lower.iloc[i] = final_lower.iloc[i]
        else:
            final_lower.iloc[i] = final_lower.iloc[i - 1]

    # Determine direction
    st = pd.Series(np.nan, index=close.index)
    direction = pd.Series(1, index=close.index)

    for i in range(1, n):
        if st.iloc[i - 1] == final_upper.iloc[i - 1]:
            if close.iloc[i] <= final_upper.iloc[i]:
                st.iloc[i] = final_upper.iloc[i]
                direction.iloc[i] = -1
            else:
                st.iloc[i] = final_lower.iloc[i]
                direction.iloc[i] = 1
        else:
            if close.iloc[i] >= final_lower.iloc[i]:
                st.iloc[i] = final_lower.iloc[i]
                direction.iloc[i] = 1
            else:
                st.iloc[i] = final_upper.iloc[i]
                direction.iloc[i] = -1

    return direction.map({1: "BUY", -1: "SELL"})


def obv(close: pd.Series, volume: pd.Series) -> pd.Series:
    """On-Balance Volume."""
    direction = np.where(close > close.shift(1), 1, np.where(close < close.shift(1), -1, 0))
    return (volume * direction).cumsum()


def moving_averages(close: pd.Series):
    """Compute 20/50/100/200 DMA."""
    return {
        "dma_20": sma(close, 20),
        "dma_50": sma(close, 50),
        "dma_100": sma(close, 100),
        "dma_200": sma(close, 200),
    }


def golden_death_cross(close: pd.Series, lookback: int = 5):
    """Detect golden cross or death cross in last `lookback` days.
    Returns 'GOLDEN', 'DEATH', or None.
    """
    dma50 = sma(close, 50)
    dma200 = sma(close, 200)

    if dma50.isna().iloc[-1] or dma200.isna().iloc[-1]:
        return None

    recent = min(lookback, len(close) - 1)
    for i in range(-recent, 0):
        try:
            prev_diff = dma50.iloc[i - 1] - dma200.iloc[i - 1]
            curr_diff = dma50.iloc[i] - dma200.iloc[i]
            if prev_diff <= 0 and curr_diff > 0:
                return "GOLDEN"
            if prev_diff >= 0 and curr_diff < 0:
                return "DEATH"
        except (IndexError, KeyError):
            continue
    return None


def compute_all_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Add all indicator columns to a single-stock DataFrame.
    Expects columns: close, high, low, volume, delivery_qty.
    Returns the DataFrame with new columns added.
    """
    df = df.copy()
    close = df["close"]
    high = df["high"]
    low = df["low"]
    vol = df["volume"].fillna(0)
    deliv = df["delivery_qty"].fillna(0) if "delivery_qty" in df.columns else vol * 0

    df["rsi"] = rsi(close)
    ml, sl, hist = macd(close)
    df["macd"] = ml
    df["macd_signal"] = sl
    df["macd_hist"] = hist
    upper, mid, lower, bw, pctb = bollinger_bands(close)
    df["bb_upper"] = upper
    df["bb_middle"] = mid
    df["bb_lower"] = lower
    df["bb_bandwidth"] = bw
    df["bb_pctb"] = pctb
    df["atr"] = atr(high, low, close)
    df["supertrend"] = supertrend(high, low, close)
    df["obv"] = obv(close, vol)
    mas = moving_averages(close)
    for k, v in mas.items():
        df[k] = v
    df["cross"] = golden_death_cross(close, lookback=10)

    return df
