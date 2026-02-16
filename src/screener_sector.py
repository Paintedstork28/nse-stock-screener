"""Tab 6: Sector Heatmap, FII/DII flow, market breadth."""

import datetime as dt

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px


def compute_sector_performance(sector_df: pd.DataFrame) -> pd.DataFrame:
    """Compute 1W, 1M, 3M, 6M performance for each sector index.

    Args:
        sector_df: DataFrame with columns [index_name, trade_date, close]

    Returns:
        DataFrame with columns: Sector, 1W %, 1M %, 3M %, 6M %
    """
    if sector_df is None or sector_df.empty:
        return pd.DataFrame(columns=["Sector", "1W %", "1M %", "3M %", "6M %"])

    sector_df = sector_df.copy()
    sector_df["trade_date"] = pd.to_datetime(sector_df["trade_date"]).dt.date

    today = sector_df["trade_date"].max()
    periods = {
        "1W %": today - dt.timedelta(days=7),
        "1M %": today - dt.timedelta(days=30),
        "3M %": today - dt.timedelta(days=90),
        "6M %": today - dt.timedelta(days=180),
    }

    results = []
    for name, grp in sector_df.groupby("index_name"):
        grp = grp.sort_values("trade_date")
        if len(grp) < 5:
            continue

        latest_close = grp["close"].iloc[-1]
        row = {"Sector": name}

        for label, cutoff in periods.items():
            past = grp[grp["trade_date"] <= cutoff]
            if len(past) > 0:
                past_close = past["close"].iloc[-1]
                if past_close > 0:
                    row[label] = round(((latest_close - past_close) / past_close) * 100, 1)
                else:
                    row[label] = None
            else:
                row[label] = None

        results.append(row)

    return pd.DataFrame(results).sort_values("1M %", ascending=False).reset_index(drop=True)


def create_sector_heatmap(perf_df: pd.DataFrame) -> go.Figure:
    """Create a plotly heatmap of sector performance."""
    if perf_df.empty:
        fig = go.Figure()
        fig.add_annotation(text="No sector data available", showarrow=False,
                           xref="paper", yref="paper", x=0.5, y=0.5, font=dict(size=16))
        return fig

    sectors = perf_df["Sector"].tolist()
    periods = ["1W %", "1M %", "3M %", "6M %"]
    z_data = perf_df[periods].fillna(0).infer_objects(copy=False).values

    fig = go.Figure(data=go.Heatmap(
        z=z_data,
        x=periods,
        y=sectors,
        colorscale=[
            [0, "#ff4444"],
            [0.35, "#cc3333"],
            [0.5, "#2a2a3e"],
            [0.65, "#008f4d"],
            [1, "#00ff88"],
        ],
        zmid=0,
        text=[[f"{v:.1f}%" if not np.isnan(v) else "" for v in row] for row in z_data],
        texttemplate="%{text}",
        textfont=dict(size=12, color="#e8e8f0"),
        hovertemplate="Sector: %{y}<br>Period: %{x}<br>Return: %{text}<extra></extra>",
    ))

    fig.update_layout(
        title=dict(text="Sector Performance Heatmap", font=dict(color="#e8e8f0")),
        xaxis_title="Period",
        yaxis_title="",
        height=max(400, len(sectors) * 35),
        margin=dict(l=150, r=50, t=60, b=50),
        font=dict(size=12, color="#e8e8f0"),
        paper_bgcolor="#1a1a2e",
        plot_bgcolor="#1a1a2e",
        xaxis=dict(color="#a8a8b8", gridcolor="#2a2a3e"),
        yaxis=dict(color="#a8a8b8", gridcolor="#2a2a3e"),
    )
    return fig


def create_fii_dii_chart(fii_dii_df: pd.DataFrame) -> go.Figure:
    """Create FII/DII net flow bar chart."""
    if fii_dii_df is None or fii_dii_df.empty:
        fig = go.Figure()
        fig.add_annotation(text="No FII/DII data available", showarrow=False,
                           xref="paper", yref="paper", x=0.5, y=0.5, font=dict(size=16))
        return fig

    df = fii_dii_df.copy().sort_values("trade_date")

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["trade_date"], y=df["fii_net"],
        name="FII Net",
        marker_color=df["fii_net"].apply(lambda x: "#00ff88" if x >= 0 else "#ff4444"),
    ))
    fig.add_trace(go.Bar(
        x=df["trade_date"], y=df["dii_net"],
        name="DII Net",
        marker_color=df["dii_net"].apply(lambda x: "#4dabf7" if x >= 0 else "#ff922b"),
    ))

    fig.update_layout(
        title=dict(text="FII / DII Net Flow (\u20b9 Cr)", font=dict(color="#e8e8f0")),
        barmode="group",
        xaxis_title="Date",
        yaxis_title="Net Flow (\u20b9 Cr)",
        height=400,
        margin=dict(l=50, r=50, t=60, b=50),
        paper_bgcolor="#1a1a2e",
        plot_bgcolor="#1a1a2e",
        font=dict(color="#e8e8f0"),
        xaxis=dict(color="#a8a8b8", gridcolor="#2a2a3e"),
        yaxis=dict(color="#a8a8b8", gridcolor="#2a2a3e"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                    bgcolor="#1a1a2e", font=dict(color="#e8e8f0")),
    )
    return fig


def compute_market_breadth(ohlcv: pd.DataFrame) -> dict:
    """Compute market breadth: % of stocks above key DMAs."""
    from src.indicators import sma

    total = 0
    above_200 = 0
    above_50 = 0
    above_20 = 0

    for symbol, group in ohlcv.groupby("symbol"):
        group = group.sort_values("trade_date").reset_index(drop=True)
        if len(group) < 200:
            # Need at least 200 days for 200 DMA
            continue

        close = group["close"]
        current = close.iloc[-1]
        total += 1

        dma200 = sma(close, 200).iloc[-1]
        dma50 = sma(close, 50).iloc[-1]
        dma20 = sma(close, 20).iloc[-1]

        if not np.isnan(dma200) and current > dma200:
            above_200 += 1
        if not np.isnan(dma50) and current > dma50:
            above_50 += 1
        if not np.isnan(dma20) and current > dma20:
            above_20 += 1

    if total == 0:
        return {"total": 0, "above_200_pct": 0, "above_50_pct": 0, "above_20_pct": 0}

    return {
        "total": total,
        "above_200_pct": round(above_200 / total * 100, 1),
        "above_50_pct": round(above_50 / total * 100, 1),
        "above_20_pct": round(above_20 / total * 100, 1),
    }
