"""Promoter Holdings screener — quarter-over-quarter trend analysis."""

import pandas as pd


def screen_promoter_holdings(promoter_df):
    """Analyse promoter holding trends over available quarters.

    For each symbol, compares consecutive quarters to determine whether
    promoters have been steadily increasing, steadily decreasing, or
    mixed in their holdings over the last 6 months.

    Args:
        promoter_df: DataFrame from fetch_promoter_data() with columns:
            symbol, quarter, promoter_holding_pct, pledge_pct,
            fii_holding_pct, dii_holding_pct, public_holding_pct

    Returns:
        DataFrame with columns: Symbol, Promoter %, 6M Change %, Trend,
        QoQ Changes, Pledge %, FII %, DII %, Quarters
        Sorted by 6M Change descending.
    """
    empty = pd.DataFrame(columns=[
        "Symbol", "Promoter %", "6M Change %", "Trend",
        "QoQ Changes", "Pledge %", "FII %", "DII %", "Quarters"
    ])

    if promoter_df is None or promoter_df.empty:
        return empty

    df = promoter_df.copy()
    df["promoter_holding_pct"] = pd.to_numeric(df["promoter_holding_pct"], errors="coerce")
    df["pledge_pct"] = pd.to_numeric(df["pledge_pct"], errors="coerce").fillna(0)
    df["fii_holding_pct"] = pd.to_numeric(df["fii_holding_pct"], errors="coerce").fillna(0)
    df["dii_holding_pct"] = pd.to_numeric(df["dii_holding_pct"], errors="coerce").fillna(0)

    # Sort by quarter ascending within each symbol (oldest first)
    df = df.sort_values(["symbol", "quarter"], ascending=[True, True])

    results = []
    for symbol, grp in df.groupby("symbol"):
        grp = grp.dropna(subset=["promoter_holding_pct"]).reset_index(drop=True)
        if len(grp) < 2:
            continue

        holdings = grp["promoter_holding_pct"].tolist()
        quarters = grp["quarter"].tolist()

        # Quarter-over-quarter changes
        qoq = []
        for j in range(1, len(holdings)):
            qoq.append(round(holdings[j] - holdings[j - 1], 2))

        # Classify trend
        ups = sum(1 for c in qoq if c > 0)
        downs = sum(1 for c in qoq if c < 0)
        flat = sum(1 for c in qoq if c == 0)

        if ups > 0 and downs == 0:
            trend = "Steady Increase"
        elif downs > 0 and ups == 0:
            trend = "Steady Decrease"
        elif ups > downs:
            trend = "Mostly Increasing"
        elif downs > ups:
            trend = "Mostly Decreasing"
        else:
            trend = "Mixed"

        latest = grp.iloc[-1]
        oldest = grp.iloc[0]
        total_change = round(holdings[-1] - holdings[0], 2)

        # Format QoQ as readable string
        qoq_str = ", ".join("{:+.1f}".format(c) for c in qoq)

        results.append({
            "Symbol": symbol,
            "Promoter %": round(latest["promoter_holding_pct"], 1),
            "6M Change %": total_change,
            "Trend": trend,
            "QoQ Changes": qoq_str,
            "Pledge %": round(latest["pledge_pct"], 1),
            "FII %": round(latest["fii_holding_pct"], 1),
            "DII %": round(latest["dii_holding_pct"], 1),
            "Quarters": len(quarters),
        })

    if not results:
        return empty

    return pd.DataFrame(results).sort_values(
        "6M Change %", ascending=False
    ).reset_index(drop=True)


def screen_high_pledge(promoter_df, threshold_pct=20.0):
    """Filter stocks with high promoter pledge percentage."""
    empty = pd.DataFrame(columns=["Symbol", "Promoter Holding %", "Pledge %"])

    if promoter_df is None or promoter_df.empty:
        return empty

    df = promoter_df.copy()
    df["promoter_holding_pct"] = pd.to_numeric(df["promoter_holding_pct"], errors="coerce")
    df["pledge_pct"] = pd.to_numeric(df["pledge_pct"], errors="coerce").fillna(0)

    # Get latest quarter per symbol
    df = df.sort_values(["symbol", "quarter"], ascending=[True, False])
    latest = df.groupby("symbol").first().reset_index()

    high = latest[latest["pledge_pct"] >= threshold_pct].copy()
    if high.empty:
        return empty

    result = pd.DataFrame({
        "Symbol": high["symbol"],
        "Promoter Holding %": high["promoter_holding_pct"].round(1),
        "Pledge %": high["pledge_pct"].round(1),
    }).sort_values("Pledge %", ascending=False).reset_index(drop=True)

    return result
