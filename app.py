"""NSE Stock Screener — Streamlit Dashboard."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import streamlit as st
import pandas as pd

st.set_page_config(
    page_title="NSE Stock Screener",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

from src.data_fetcher import (load_all_data, get_ohlcv_df, get_last_updated,
                               get_db, get_data_window, fetch_stock_info,
                               restore_from_parquet, save_ohlcv_parquet)
from src.data_extras import (fetch_fii_dii_data, fetch_bulk_deals, fetch_promoter_data,
                              fetch_sector_indices, get_india_vix)

# ---------------------------------------------------------------------------
# Custom CSS for clean professional look
# ---------------------------------------------------------------------------

st.markdown("""
<style>
    /* Info cards row */
    .info-card {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border: 1px solid #2a2a3e;
        border-radius: 8px;
        padding: 0.7rem 1rem;
        height: 100%;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.3);
    }
    .info-card-label {
        font-size: 0.7rem;
        color: #a8a8b8;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        font-weight: 600;
        margin-bottom: 2px;
    }
    .info-card-value {
        font-size: 0.95rem;
        color: #e8e8f0;
        font-weight: 500;
    }
    .info-card-detail {
        font-size: 0.75rem;
        color: #7a7a88;
        margin-top: 2px;
    }

    /* Ensure header isn't cut off */
    .block-container {
        padding-top: 2.5rem;
    }

    /* Button override — green gradient */
    .stButton > button {
        background: linear-gradient(135deg, #00d9a3 0%, #00cc6a 100%) !important;
        color: #0d0d1a !important;
        border: none !important;
        font-weight: 600 !important;
    }
    .stButton > button:hover {
        background: linear-gradient(135deg, #00cc6a 0%, #00b85c 100%) !important;
    }

    /* Dataframe dark background */
    .stDataFrame {
        border: 1px solid #2a2a3e;
        border-radius: 8px;
    }

    /* Expander styling */
    .streamlit-expanderHeader {
        background-color: #1a1a2e !important;
        color: #a8a8b8 !important;
    }

    /* Metric styling */
    [data-testid="stMetricValue"] {
        color: #e8e8f0;
    }
    [data-testid="stMetricLabel"] {
        color: #a8a8b8;
    }

    /* Slider styling */
    .stSlider [data-baseweb="slider"] {
        background-color: #2a2a3e;
    }

    /* Subheader styling */
    .stSubheader, h3 {
        color: #e8e8f0 !important;
    }

    /* Tab styling (sub-tabs only) */
    .stTabs [data-baseweb="tab-list"] {
        background-color: #1a1a2e;
        border-radius: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        color: #a8a8b8;
    }
    .stTabs [aria-selected="true"] {
        color: #00d9a3 !important;
        border-bottom-color: #00d9a3 !important;
    }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Smart startup: restore from parquet cache + fetch only missing days
# ---------------------------------------------------------------------------

if "startup_done" not in st.session_state:
    restored = restore_from_parquet()
    if restored > 0:
        with st.spinner("Updating with latest market data..."):
            load_all_data()
    else:
        conn = get_db()
        row_count = conn.execute("SELECT COUNT(*) FROM ohlcv").fetchone()[0]
        conn.close()
        if row_count > 0:
            from src.data_fetcher import get_dates_to_fetch as _gdf
            conn = get_db()
            missing = _gdf(conn)
            conn.close()
            if missing:
                with st.spinner(f"Fetching {len(missing)} missing day(s)..."):
                    load_all_data()
    st.session_state["startup_done"] = True

# ---------------------------------------------------------------------------
# Cached loaders
# ---------------------------------------------------------------------------

@st.cache_data(ttl=3600, show_spinner=False)
def load_cached_ohlcv():
    try:
        return get_ohlcv_df()
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=600, show_spinner=False)
def load_stock_info():
    try:
        return fetch_stock_info()
    except Exception:
        return pd.DataFrame(columns=["symbol", "company_name", "industry"])

@st.cache_data(ttl=600, show_spinner=False)
def _cached_vix():
    try:
        return get_india_vix()
    except Exception:
        return None

# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------

last_updated = get_last_updated()
win_start, win_end = get_data_window()
vix = _cached_vix()

if win_start and win_end:
    window_text = f"{win_start.strftime('%d %b %Y')} — {win_end.strftime('%d %b %Y')}"
else:
    window_text = "No data yet"

# ---------------------------------------------------------------------------
# Logo path
# ---------------------------------------------------------------------------

_LOGO_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "logo.png")

def _ensure_logo():
    if os.path.exists(_LOGO_PATH):
        return
    try:
        from PIL import Image, ImageDraw
        img = Image.new("RGBA", (120, 120), (0, 0, 0, 0))
        draw = ImageDraw.Draw(img)
        draw.rounded_rectangle([0, 0, 119, 119], radius=20, fill="#1a73e8")
        bar_w, gap, base_y, start_x = 18, 8, 90, 22
        draw.rectangle([start_x, 60, start_x + bar_w, base_y], fill="white")
        draw.rectangle([start_x + bar_w + gap, 42, start_x + 2 * bar_w + gap, base_y], fill="white")
        draw.rectangle([start_x + 2 * (bar_w + gap), 24, start_x + 3 * bar_w + 2 * gap, base_y], fill="white")
        os.makedirs(os.path.dirname(_LOGO_PATH), exist_ok=True)
        img.save(_LOGO_PATH)
    except Exception:
        pass

_ensure_logo()

# ---------------------------------------------------------------------------
# Helper: enrich a results DataFrame with company name and sector
# ---------------------------------------------------------------------------

def enrich_with_info(df, symbol_col="Symbol"):
    """Add Company and Sector columns right after the symbol column."""
    if stock_info.empty or df.empty:
        df.insert(df.columns.get_loc(symbol_col) + 1, "Company", "—")
        df.insert(df.columns.get_loc("Company") + 1, "Sector", "—")
        return df

    info_map = stock_info.set_index("symbol")
    names = df[symbol_col].map(info_map["company_name"]).fillna("—")
    sectors = df[symbol_col].map(info_map["industry"]).fillna("—")

    df.insert(df.columns.get_loc(symbol_col) + 1, "Company", names.values)
    df.insert(df.columns.get_loc("Company") + 1, "Sector", sectors.values)
    return df


# ---------------------------------------------------------------------------
# Number formatting helpers
# ---------------------------------------------------------------------------

def fmt_price_col(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = df[c].apply(lambda v: f"\u20b9{v:,.2f}" if pd.notna(v) else "—")
    return df

def fmt_pct_col(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = df[c].apply(lambda v: f"{v:.2f}%" if pd.notna(v) else "—")
    return df

def fmt_num_col(df, cols, decimals=2):
    for c in cols:
        if c in df.columns:
            df[c] = df[c].apply(lambda v: f"{v:.{decimals}f}" if pd.notna(v) else "—")
    return df

def fmt_vol_col(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = df[c].apply(lambda v: f"{int(v):,}" if pd.notna(v) else "—")
    return df


# ---------------------------------------------------------------------------
# SIDEBAR — all controls, context, and navigation
# ---------------------------------------------------------------------------

with st.sidebar:
    # Logo + title
    _logo_col, _title_col = st.columns([0.15, 0.85], gap="small")
    with _logo_col:
        if os.path.exists(_LOGO_PATH):
            st.image(_LOGO_PATH, width=40)
    with _title_col:
        st.markdown("### Stock Screener")
    st.caption("Smart screening for Indian equities")

    # Refresh button
    if st.button("Refresh Data", use_container_width=True):
        with st.spinner("Downloading market data..."):
            progress_bar = st.progress(0)
            status_text = st.empty()

            def update_progress(pct, msg):
                progress_bar.progress(min(pct, 1.0))
                status_text.text(msg)

            success = load_all_data(progress_callback=update_progress)
            progress_bar.empty()
            status_text.empty()

            if success:
                load_cached_ohlcv.clear()
                load_stock_info.clear()
                st.rerun()
            else:
                st.error("Some data may not have loaded.")
    st.caption(f"Last refreshed: {last_updated or 'Never'}  ·  Data: {window_text}")

    # VIX card
    if vix is not None:
        mood = "Calm" if vix < 15 else ("Normal" if vix < 20 else "Nervous")
        mood_color = "#00ff88" if vix < 15 else ("#ffa502" if vix < 20 else "#ff6b6b")
        st.markdown(f"""<div class="info-card">
            <div class="info-card-label">India VIX (Fear Gauge)</div>
            <div class="info-card-value">{vix:.2f} &nbsp;
                <span style="color:{mood_color}; font-size:0.8rem;">● {mood}</span></div>
            <div class="info-card-detail">Measures expected market swings. Lower = calmer.</div>
        </div>""", unsafe_allow_html=True)
    else:
        st.markdown("""<div class="info-card">
            <div class="info-card-label">India VIX</div>
            <div class="info-card-value">—</div>
        </div>""", unsafe_allow_html=True)

    # Data Sources card
    st.markdown("""<div class="info-card">
        <div class="info-card-label">Data Sources</div>
        <div class="info-card-value" style="font-size:0.82rem;">
            NSE Bhavcopies &bull; Yahoo Finance &bull; NSE Reports
        </div>
        <div class="info-card-detail">All indicators computed locally from price data.</div>
    </div>""", unsafe_allow_html=True)

    st.divider()

    # Screen selector (replaces tabs)
    st.markdown("**Data Cuts**")
    screen = st.radio("Screen", [
        "Price Drops",
        "Sideways Movers",
        "Volume Buzz",
        "Price-Volume Intersection",
        "Top Momentum",
        "Big Player Activity",
        "Sector Map",
        "Warning Signs",
    ], label_visibility="collapsed")

    pass  # Controls and explanations are in the main area


# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

ohlcv = load_cached_ohlcv()
stock_info = load_stock_info()

if ohlcv.empty:
    st.warning(
        "No market data available. Click **Refresh Data** in the sidebar to download "
        "historical data. First load takes 3-5 minutes."
    )
    st.stop()


# ---------------------------------------------------------------------------
# MAIN AREA — data only, based on selected screen
# ---------------------------------------------------------------------------

if screen == "Price Drops":
    from src.screener_price import screen_big_drops

    drop_threshold = st.slider("Minimum drop from 6M high (%)", 10, 50, 20, 5, key="drop_thresh")

    with st.spinner("Screening..."):
        drops_df = screen_big_drops(ohlcv, threshold_pct=drop_threshold)

    st.caption(f"{len(drops_df)} stocks found")

    if not drops_df.empty:
        display_df = enrich_with_info(drops_df.copy())
        display_df = fmt_price_col(display_df, ["Current Price", "6M High"])
        display_df = fmt_pct_col(display_df, ["Drop %"])
        display_df = fmt_num_col(display_df, ["RSI"])

        def highlight_rsi(val):
            if val is None or val == "—":
                return ""
            try:
                v = float(val)
                if v < 30:
                    return "background-color: #008f4d; color: #e8e8f0"
                if v > 70:
                    return "background-color: #d63031; color: #e8e8f0"
            except (ValueError, TypeError):
                pass
            return ""

        styled = display_df.style.map(highlight_rsi, subset=["RSI"])
        st.dataframe(styled, width="stretch", hide_index=True)
    else:
        st.info("No stocks match the current threshold. Try lowering it.")

    with st.expander("How to read this"):
        st.markdown(
            "**Drop %** — How much the stock fell from its 6-month high\n\n"
            "**RSI** — Below 30 (green) = oversold, may bounce. Above 70 (red) = overbought.\n\n"
            "Look for large drop + RSI below 30 for potential value picks."
        )

elif screen == "Sideways Movers":
    from src.screener_price import screen_range_bound

    range_width = st.slider("Max range width (%)", 2, 15, 5, 1, key="range_width")
    min_range_days = st.slider("Minimum days in range", 5, 30, 10, 5, key="range_days")

    with st.spinner("Screening..."):
        range_df = screen_range_bound(ohlcv, range_pct=range_width, min_days=min_range_days)

    st.caption(f"{len(range_df)} stocks found")

    if not range_df.empty:
        display_df = enrich_with_info(range_df.copy())
        display_df = fmt_price_col(display_df, ["Range Low", "Range High", "Midpoint"])
        display_df = fmt_pct_col(display_df, ["Range Width %"])
        display_df = fmt_num_col(display_df, ["BB Bandwidth"])
        st.dataframe(display_df, width="stretch", hide_index=True)
    else:
        st.info("No range-bound stocks found. Try widening the range threshold.")

    with st.expander("How to read this"):
        st.markdown(
            "**Range Width %** — How tight the range is (smaller = more compressed)\n\n"
            "**BB Bandwidth** — Lower = tighter squeeze = breakout more likely\n\n"
            "**Days in Range** — Longer compression often leads to stronger breakouts."
        )

elif screen == "Volume Buzz":
    from src.screener_volume import screen_volume_spikes

    vol_threshold = st.slider("Volume above average (%)", 25, 200, 50, 25, key="vol_thresh")
    consec_days = st.slider("Consecutive high-volume days", 1, 15, 10, 1, key="vol_days")

    with st.spinner("Screening..."):
        vol_df = screen_volume_spikes(ohlcv, vol_threshold_pct=vol_threshold,
                                       consecutive_days=consec_days)

    st.caption(f"{len(vol_df)} stocks found")

    if not vol_df.empty:
        display_df = enrich_with_info(vol_df.copy())
        display_df = fmt_price_col(display_df, ["Current Price"])
        display_df = fmt_vol_col(display_df, ["Avg Volume", f"Last {consec_days}D Avg Vol"])
        display_df = fmt_num_col(display_df, ["Vol Ratio"])
        display_df = fmt_pct_col(display_df, ["Avg Delivery %", "Recent Delivery %", "Price Change %"])

        def highlight_delivery(val):
            if isinstance(val, str) and val == "Yes":
                return "background-color: #008f4d; color: #e8e8f0"
            return ""

        styled = display_df.style.map(highlight_delivery, subset=["Delivery Above Avg"])
        st.dataframe(styled, width="stretch", hide_index=True)
    else:
        st.info("No volume spikes detected. Try lowering the threshold.")

    with st.expander("How to read this"):
        st.markdown(
            "**Vol Ratio** — Current volume vs 6-month average\n\n"
            "**Delivery Above Avg** — Green = genuine buying, not speculation\n\n"
            "Focus on high Vol Ratio + Delivery Above Avg = Yes + positive Price Change."
        )

elif screen == "Price-Volume Intersection":
    from src.screener_price import screen_big_drops
    from src.screener_volume import screen_volume_spikes

    int_drop = st.slider("Minimum drop from 6M high (%)", 10, 50, 20, 5, key="int_drop")
    _pv_col1, _pv_col2 = st.columns(2)
    with _pv_col1:
        int_vol = st.slider("Volume above average (%)", 25, 200, 50, 25, key="int_vol")
    with _pv_col2:
        int_days = st.slider("Consecutive high-volume days", 1, 15, 3, 1, key="int_days")

    with st.spinner("Screening..."):
        int_drops_df = screen_big_drops(ohlcv, threshold_pct=int_drop)
        int_vol_df = screen_volume_spikes(ohlcv, vol_threshold_pct=int_vol,
                                           consecutive_days=int_days)

    if not int_drops_df.empty and not int_vol_df.empty:
        drop_symbols = set(int_drops_df["Symbol"].tolist())
        vol_symbols = set(int_vol_df["Symbol"].tolist())
        common = drop_symbols & vol_symbols

        st.caption(f"{len(common)} stocks found")

        if common:
            drops_sub = int_drops_df[int_drops_df["Symbol"].isin(common)][
                ["Symbol", "Current Price", "6M High", "Drop %", "RSI"]
            ].copy()
            vol_sub = int_vol_df[int_vol_df["Symbol"].isin(common)][
                ["Symbol", "Vol Ratio", "Delivery Above Avg", "Price Change %"]
            ].copy()
            merged = drops_sub.merge(vol_sub, on="Symbol", how="inner")
            merged = merged.sort_values("Drop %")

            display_df = enrich_with_info(merged)
            display_df = fmt_price_col(display_df, ["Current Price", "6M High"])
            display_df = fmt_pct_col(display_df, ["Drop %", "Price Change %"])
            display_df = fmt_num_col(display_df, ["RSI", "Vol Ratio"])

            def highlight_intersection(val):
                if isinstance(val, str) and val == "Yes":
                    return "background-color: #008f4d; color: #e8e8f0"
                return ""

            styled = display_df.style.map(highlight_intersection, subset=["Delivery Above Avg"])
            st.dataframe(styled, width="stretch", hide_index=True)
        else:
            st.info("No stocks currently appear in both Price Drops and Volume Buzz. "
                    "Try adjusting the thresholds.")
    else:
        st.info("Not enough data to compute intersection. Ensure both screens have results.")

    with st.expander("How to read this"):
        st.markdown(
            "Stocks in **both** Price Drops and Volume Buzz.\n\n"
            "Big drop + volume surge + high delivery = strong accumulation signal.\n\n"
            "Always check news to rule out fundamental problems."
        )

elif screen == "Top Momentum":
    from src.screener_momentum import screen_momentum_leaders
    with st.spinner("Screening..."):
        momentum_df = screen_momentum_leaders(ohlcv)

    st.caption(f"{len(momentum_df)} stocks found")

    if not momentum_df.empty:
        display_df = enrich_with_info(momentum_df.copy())
        display_df = fmt_price_col(display_df, ["Price", "6M High", "50 DMA", "200 DMA"])
        display_df = fmt_pct_col(display_df, ["Dist from High %"])
        display_df = fmt_num_col(display_df, ["RSI"])
        st.dataframe(display_df, width="stretch", hide_index=True)
    else:
        st.info("No momentum leaders found matching all criteria.")

    with st.expander("How to read this"):
        st.markdown(
            "Stocks near 6M high, above 50 & 200 DMA, RSI 55-75, Supertrend = BUY.\n\n"
            "\"Ride the wave\" stocks. Enter on dips, stop-loss below 50 DMA."
        )

elif screen == "Big Player Activity":
    from src.screener_smart_money import (get_bulk_deals_summary,
                                           screen_delivery_breakouts, screen_obv_divergence)

    delivery_mult = st.slider("Delivery multiplier (x times average)", 1.5, 5.0, 2.0, 0.5, key="deliv_mult")

    sub1, sub2, sub3 = st.tabs(["Bulk Deals", "Delivery Breakouts", "OBV Accumulation"])

    with sub1:
        with st.spinner("Fetching deals..."):
            bulk_raw = fetch_bulk_deals(days=30)
            bulk_df = get_bulk_deals_summary(bulk_raw)
        if not bulk_df.empty:
            display_df = bulk_df.copy()
            if "Symbol" in display_df.columns:
                display_df = enrich_with_info(display_df)
            if "Price" in display_df.columns:
                display_df = fmt_price_col(display_df, ["Price"])
            if "Qty" in display_df.columns:
                display_df = fmt_vol_col(display_df, ["Qty"])
            st.dataframe(display_df, width="stretch", hide_index=True)
        else:
            st.info("No bulk deals data available. Click Refresh Data to fetch.")

    with sub2:
        with st.spinner("Screening..."):
            delivery_df = screen_delivery_breakouts(ohlcv, multiplier=delivery_mult)
        st.caption(f"{len(delivery_df)} stocks found")
        if not delivery_df.empty:
            display_df = enrich_with_info(delivery_df.copy())
            display_df = fmt_price_col(display_df, ["Price"])
            display_df = fmt_vol_col(display_df, ["Avg Delivery Qty", "Recent Delivery Qty"])
            display_df = fmt_num_col(display_df, ["Delivery Ratio"])
            display_df = fmt_pct_col(display_df, ["Price Change %"])
            st.dataframe(display_df, width="stretch", hide_index=True)
        else:
            st.info("No delivery breakouts found.")

    with sub3:
        with st.spinner("Screening..."):
            obv_df = screen_obv_divergence(ohlcv)
        st.caption(f"{len(obv_df)} stocks found")
        if not obv_df.empty:
            display_df = enrich_with_info(obv_df.copy())
            display_df = fmt_price_col(display_df, ["Price"])
            display_df = fmt_pct_col(display_df, ["Price Change %"])
            st.dataframe(display_df, width="stretch", hide_index=True)
        else:
            st.info("No OBV divergences detected.")

    with st.expander("How to read this"):
        st.markdown(
            "**Bulk Deals** — Large transactions (>0.5% of shares) reported by NSE\n\n"
            "**Delivery Breakouts** — Delivery qty 2x+ above average = real accumulation\n\n"
            "**OBV Accumulation** — Volume flow rising while price flat = silent buying\n\n"
            "Strongest signal: same stock across multiple sub-tabs."
        )

elif screen == "Sector Map":
    from src.screener_sector import (compute_sector_performance, create_sector_heatmap,
                                      create_fii_dii_chart, compute_market_breadth)

    with st.spinner("Loading sector data..."):
        sector_data = fetch_sector_indices(days=180)
        perf_df = compute_sector_performance(sector_data)

    if not perf_df.empty:
        fig = create_sector_heatmap(perf_df)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No sector data available. Click Refresh Data.")

    # Market breadth
    with st.spinner("Computing breadth..."):
        breadth = compute_market_breadth(ohlcv)

    if breadth["total"] > 0:
        b_col1, b_col2, b_col3, b_col4 = st.columns(4)
        with b_col1:
            st.metric("Stocks Analyzed", breadth["total"])
        with b_col2:
            st.metric("Above 200 DMA", f"{breadth['above_200_pct']:.1f}%")
        with b_col3:
            st.metric("Above 50 DMA", f"{breadth['above_50_pct']:.1f}%")
        with b_col4:
            st.metric("Above 20 DMA", f"{breadth['above_20_pct']:.1f}%")

    st.markdown("---")
    st.caption("FII / DII Activity (last 30 days)")
    with st.spinner("Loading FII/DII data..."):
        fii_dii = fetch_fii_dii_data(days=30)
    if not fii_dii.empty:
        fig2 = create_fii_dii_chart(fii_dii)
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No FII/DII data available.")

    with st.expander("How to read this"):
        st.markdown(
            "**Green** = sector up (money flowing in), **Red** = down\n\n"
            "**Market Breadth** — % above 200 DMA: >60% healthy, <40% weak\n\n"
            "**FII** = foreign money, **DII** = domestic institutions.\n\n"
            "Favour stocks from sectors green across all periods."
        )

elif screen == "Warning Signs":
    from src.screener_red_flags import (screen_high_pledge, screen_death_cross,
                                         screen_falling_delivery, screen_below_all_mas)

    pledge_thresh = st.slider("Minimum pledge % (High Pledging tab)", 5, 50, 20, 5, key="pledge_thresh")

    warn1, warn2, warn3, warn4 = st.tabs([
        "Death Cross", "Speculative Rallies", "Below All MAs", "High Pledging"
    ])

    with warn1:
        with st.spinner("Screening..."):
            death_df = screen_death_cross(ohlcv, lookback=10)
        st.caption(f"{len(death_df)} stocks found")
        if not death_df.empty:
            display_df = enrich_with_info(death_df.copy())
            display_df = fmt_price_col(display_df, ["Price", "50 DMA", "200 DMA"])
            st.dataframe(display_df, width="stretch", hide_index=True)
        else:
            st.info("No recent death crosses detected.")

    with warn2:
        with st.spinner("Screening..."):
            spec_df = screen_falling_delivery(ohlcv, lookback=10)
        st.caption(f"{len(spec_df)} stocks found")
        if not spec_df.empty:
            display_df = enrich_with_info(spec_df.copy())
            display_df = fmt_price_col(display_df, ["Price"])
            display_df = fmt_pct_col(display_df, ["Price Change %", "Delivery % Start", "Delivery % End"])
            st.dataframe(display_df, width="stretch", hide_index=True)
        else:
            st.info("No speculative rallies detected.")

    with warn3:
        with st.spinner("Screening..."):
            below_df = screen_below_all_mas(ohlcv)
        st.caption(f"{len(below_df)} stocks found")
        if not below_df.empty:
            display_df = enrich_with_info(below_df.copy())
            display_df = fmt_price_col(display_df, ["Price", "20 DMA", "50 DMA", "200 DMA"])
            display_df = fmt_pct_col(display_df, ["Below 200 DMA %"])
            st.dataframe(display_df, width="stretch", hide_index=True)
        else:
            st.info("No stocks below all moving averages.")

    with warn4:
        with st.spinner("Fetching..."):
            promoter_df = fetch_promoter_data()
            pledge_df = screen_high_pledge(promoter_df, threshold_pct=pledge_thresh)
        st.caption(f"{len(pledge_df)} stocks found")
        if not pledge_df.empty:
            display_df = enrich_with_info(pledge_df.copy())
            display_df = fmt_pct_col(display_df, ["Promoter Holding %", "Pledge %"])
            st.dataframe(display_df, width="stretch", hide_index=True)
        else:
            st.info("No promoter pledging data available.")

    with st.expander("How to read this"):
        st.markdown(
            "**Death Cross** — 50 DMA below 200 DMA = bearish\n\n"
            "**Speculative Rallies** — Price up but delivery % falling\n\n"
            "**Below All MAs** — Deep downtrend, no support\n\n"
            "**High Pledging** — Promoter shares pledged, crash risk"
        )
