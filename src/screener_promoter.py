"""Promoter Holdings screener — 6-month change in promoter holding %."""

import pandas as pd


def screen_promoter_holdings(promoter_df, min_change=None):
    """Compute 6-month change in promoter holdings.

    Compares the latest quarter with ~2 quarters prior for each symbol.

    Args:
        promoter_df: DataFrame from fetch_promoter_data() with columns:
            symbol, quarter, promoter_holding_pct, pledge_pct,
            fii_holding_pct, dii_holding_pct, public_holding_pct
        min_change: If set, only show symbols where abs(6M change) >= this value.

    Returns:
        DataFrame with columns: Symbol, Promoter Holding %, 6M Change %,
        Pledge %, FII %, DII %, Quarter
        Sorted by 6M Change ascending (biggest drops first).
    """
    empty = pd.DataFrame(columns=[
        "Symbol", "Promoter Holding %", "6M Change %",
        "Pledge %", "FII %", "DII %", "Quarter"
    ])

    if promoter_df is None or promoter_df.empty:
        return empty

    df = promoter_df.copy()
    df["promoter_holding_pct"] = pd.to_numeric(df["promoter_holding_pct"], errors="coerce")
    df["pledge_pct"] = pd.to_numeric(df["pledge_pct"], errors="coerce").fillna(0)
    df["fii_holding_pct"] = pd.to_numeric(df["fii_holding_pct"], errors="coerce").fillna(0)
    df["dii_holding_pct"] = pd.to_numeric(df["dii_holding_pct"], errors="coerce").fillna(0)

    # Sort by quarter descending within each symbol
    df = df.sort_values(["symbol", "quarter"], ascending=[True, False])

    results = []
    for symbol, grp in df.groupby("symbol"):
        grp = grp.reset_index(drop=True)
        if len(grp) < 2:
            continue

        latest = grp.iloc[0]
        # Compare with ~2 quarters back (index 2 if available, else last)
        older_idx = min(2, len(grp) - 1)
        older = grp.iloc[older_idx]

        current_pct = latest["promoter_holding_pct"]
        old_pct = older["promoter_holding_pct"]

        if pd.isna(current_pct) or pd.isna(old_pct):
            continue

        change = current_pct - old_pct

        results.append({
            "Symbol": symbol,
            "Promoter Holding %": round(current_pct, 1),
            "6M Change %": round(change, 2),
            "Pledge %": round(latest["pledge_pct"], 1),
            "FII %": round(latest["fii_holding_pct"], 1),
            "DII %": round(latest["dii_holding_pct"], 1),
            "Quarter": latest["quarter"],
        })

    if not results:
        return empty

    result_df = pd.DataFrame(results)

    if min_change is not None:
        result_df = result_df[result_df["6M Change %"].abs() >= min_change]

    result_df = result_df.sort_values("6M Change %", ascending=True).reset_index(drop=True)
    return result_df
