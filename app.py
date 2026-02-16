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
    initial_sidebar_state="collapsed",
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
        background: #f8f9fa;
        border: 1px solid #e0e0e0;
        border-radius: 8px;
        padding: 0.7rem 1rem;
        height: 100%;
    }
    .info-card-label {
        font-size: 0.7rem;
        color: #5f6368;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        font-weight: 600;
        margin-bottom: 2px;
    }
    .info-card-value {
        font-size: 0.95rem;
        color: #202124;
        font-weight: 500;
    }
    .info-card-detail {
        font-size: 0.75rem;
        color: #5f6368;
        margin-top: 2px;
    }

    /* Tab separator */
    .tab-separator {
        border-top: 2px solid #e0e0e0;
        margin: 1rem 0 0.5rem 0;
    }

    /* Reduce default streamlit padding */
    .block-container {
        padding-top: 1rem;
    }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Smart startup: restore from parquet cache + fetch only missing days
# ---------------------------------------------------------------------------

if "startup_done" not in st.session_state:
    restored = restore_from_parquet()
    if restored > 0:
        # Parquet had data — now just fetch missing recent days
        with st.spinner("Updating with latest market data..."):
            load_all_data()
    else:
        # Check if SQLite already has data (normal warm restart)
        conn = get_db()
        row_count = conn.execute("SELECT COUNT(*) FROM ohlcv").fetchone()[0]
        conn.close()
        if row_count > 0:
            # SQLite has data, fetch any missing days silently
            from src.data_fetcher import get_dates_to_fetch as _gdf
            conn = get_db()
            missing = _gdf(conn)
            conn.close()
            if missing:
                with st.spinner(f"Fetching {len(missing)} missing day(s)..."):
                    load_all_data()
    st.session_state["startup_done"] = True

# ---------------------------------------------------------------------------
# Header with logo
# ---------------------------------------------------------------------------

_LOGO_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "logo.png")

def _ensure_logo():
    if os.path.exists(_LOGO_PATH):
        return
    try:
        from PIL import Image, ImageDraw, ImageFont
        img = Image.new("RGBA", (120, 120), (0, 0, 0, 0))
        draw = ImageDraw.Draw(img)
        draw.rounded_rectangle([0, 0, 119, 119], radius=20, fill="#1a73e8")
        try:
            font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 44)
        except Exception:
            try:
                font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 44)
            except Exception:
                font = ImageFont.load_default()
        bbox = draw.textbbox((0, 0), "NSE", font=font)
        tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
        draw.text(((120 - tw) / 2, (120 - th) / 2 - 5), "NSE", fill="white", font=font)
        os.makedirs(os.path.dirname(_LOGO_PATH), exist_ok=True)
        img.save(_LOGO_PATH)
    except Exception:
        pass

_ensure_logo()

_logo_col, _title_col = st.columns([0.04, 0.96], gap="small")
with _logo_col:
    if os.path.exists(_LOGO_PATH):
        st.image(_LOGO_PATH, width=46)
with _title_col:
    st.markdown("## Stock Screener")
    st.caption("Smart screening for Indian equities")
st.divider()

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
# Info bar (horizontal cards)
# ---------------------------------------------------------------------------

last_updated = get_last_updated()
win_start, win_end = get_data_window()
vix = _cached_vix()

col1, col2, col3, col4, col5 = st.columns([1.3, 1.5, 1.2, 2.0, 1.0])

with col1:
    st.markdown(f"""<div class="info-card">
        <div class="info-card-label">Last Refreshed</div>
        <div class="info-card-value">{last_updated or 'Never'}</div>
    </div>""", unsafe_allow_html=True)

with col2:
    if win_start and win_end:
        window_text = f"{win_start.strftime('%d %b %Y')} — {win_end.strftime('%d %b %Y')}"
    else:
        window_text = "No data yet"
    st.markdown(f"""<div class="info-card">
        <div class="info-card-label">Data Window (Rolling 6 Months)</div>
        <div class="info-card-value">{window_text}</div>
        <div class="info-card-detail">Auto-shifts forward on each refresh</div>
    </div>""", unsafe_allow_html=True)

with col3:
    if vix is not None:
        mood = "Calm" if vix < 15 else ("Normal" if vix < 20 else "Nervous")
        mood_color = "#2e7d32" if vix < 15 else ("#f57f17" if vix < 20 else "#d32f2f")
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

with col4:
    st.markdown("""<div class="info-card">
        <div class="info-card-label">Data Sources</div>
        <div class="info-card-value" style="font-size:0.82rem;">
            NSE Bhavcopies (prices, volume, delivery) &bull;
            Yahoo Finance (sectors, VIX) &bull;
            NSE Reports (FII/DII, bulk deals)
        </div>
        <div class="info-card-detail">All indicators computed locally from price data.</div>
    </div>""", unsafe_allow_html=True)

with col5:
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
                # Parquet is already saved inside load_all_data
                load_cached_ohlcv.clear()
                load_stock_info.clear()
                st.rerun()
            else:
                st.error("Some data may not have loaded. Check your connection.")

# Spacer
st.markdown('<div class="tab-separator"></div>', unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

ohlcv = load_cached_ohlcv()
stock_info = load_stock_info()

if ohlcv.empty:
    st.warning(
        "No market data available. Click **Refresh Data** above to download "
        "historical data. First load takes 3-5 minutes."
    )
    st.stop()


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
# Tabs
# ---------------------------------------------------------------------------

tab1, tab2, tab3, tab8, tab4, tab5, tab6, tab7 = st.tabs([
    "  Price Drops  ",
    "  Sideways Movers  ",
    "  Volume Buzz  ",
    "  Price-Volume Intersection  ",
    "  Top Momentum  ",
    "  Big Player Activity  ",
    "  Sector Map  ",
    "  Warning Signs  ",
])

# ==================== Tab 1: Price Drops ====================
with tab1:
    st.subheader("Price Drops — Stocks That Fell Significantly")

    with st.expander("What is this & how does it help?", expanded=False):
        st.markdown(
            "**What it tracks:** Stocks that have fallen a certain percentage from their "
            "highest price in the last 6 months.\n\n"
            "**Why it matters:** A big drop can mean two things — either the company has "
            "real problems (avoid), or the market overreacted and the stock is now available "
            "at a discount (opportunity). The **RSI** column helps you tell the difference.\n\n"
            "**How to read the table:**\n"
            "- **Drop %** — How much the stock fell from its 6-month high\n"
            "- **RSI (Relative Strength Index)** — Measures if a stock is oversold or overbought (0-100):\n"
            "  - RSI **below 30** (green) = Oversold — may have fallen too much, could bounce back\n"
            "  - RSI **30-70** = Normal range\n"
            "  - RSI **above 70** (red) = Overbought — ran up too fast\n\n"
            "**Recommendation:** Look for stocks with a large drop AND RSI below 30. "
            "These are potentially good value picks — but always check *why* the stock fell."
        )

    drop_threshold = st.slider("Minimum drop from 6M high (%)", 10, 50, 20, 5, key="drop_thresh")

    from src.screener_price import screen_big_drops
    with st.spinner("Screening..."):
        drops_df = screen_big_drops(ohlcv, threshold_pct=drop_threshold)

    st.metric("Stocks Found", len(drops_df))

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
                    return "background-color: #c8e6c9"
                if v > 70:
                    return "background-color: #ffcdd2"
            except (ValueError, TypeError):
                pass
            return ""

        styled = display_df.style.map(highlight_rsi, subset=["RSI"])
        st.dataframe(styled, width="stretch", hide_index=True)
    else:
        st.info("No stocks match the current threshold. Try lowering it.")

# ==================== Tab 2: Sideways Movers ====================
with tab2:
    st.subheader("Sideways Movers — Stocks Stuck in a Range")

    with st.expander("What is this & how does it help?", expanded=False):
        st.markdown(
            "**What it tracks:** Stocks whose price has barely moved — staying within a "
            "narrow band for many consecutive trading sessions.\n\n"
            "**Why it matters:** When a stock moves sideways for a long time, it's like "
            "a spring being compressed. Eventually it will \"break out\" — either sharply up "
            "or sharply down. Catching the breakout early can be very profitable.\n\n"
            "**How to read the table:**\n"
            "- **Range Width %** — How tight the range is (smaller = more compressed)\n"
            "- **BB Bandwidth (Bollinger Bandwidth)** — Technical measure of how tight "
            "the trading range is. Lower = tighter squeeze = breakout more likely soon.\n"
            "- **Days in Range** — How long the stock has been stuck. Longer compression "
            "often leads to stronger breakouts.\n\n"
            "**Recommendation:** Watch stocks with the lowest BB Bandwidth and longest "
            "days in range. Set price alerts just above the Range High — if the stock breaks "
            "above it with good volume, it's a bullish signal."
        )

    range_width = st.slider("Max range width (%)", 2, 15, 5, 1, key="range_width")
    min_range_days = st.slider("Minimum days in range", 5, 30, 10, 5, key="range_days")

    from src.screener_price import screen_range_bound
    with st.spinner("Screening..."):
        range_df = screen_range_bound(ohlcv, range_pct=range_width, min_days=min_range_days)

    st.metric("Stocks Found", len(range_df))

    if not range_df.empty:
        display_df = enrich_with_info(range_df.copy())
        display_df = fmt_price_col(display_df, ["Range Low", "Range High", "Midpoint"])
        display_df = fmt_pct_col(display_df, ["Range Width %"])
        display_df = fmt_num_col(display_df, ["BB Bandwidth"])
        st.dataframe(display_df, width="stretch", hide_index=True)
    else:
        st.info("No range-bound stocks found. Try widening the range threshold.")

# ==================== Tab 3: Volume Buzz ====================
with tab3:
    st.subheader("Volume Buzz — Unusual Trading Activity")

    with st.expander("What is this & how does it help?", expanded=False):
        st.markdown(
            "**What it tracks:** Stocks where trading volume has suddenly spiked well above "
            "their normal average — and stayed high for multiple days.\n\n"
            "**Why it matters:** Volume is the fuel behind price moves. A sudden, sustained "
            "increase in volume means something is happening — a big buyer is accumulating, "
            "news is coming, or the stock is about to make a big move.\n\n"
            "**The Delivery % edge:**\n"
            "- **High delivery %** alongside high volume = Genuine buying. Investors are "
            "actually taking delivery of shares (holding, not day-trading). Bullish.\n"
            "- **Low delivery %** with high volume = Speculative. Mostly intraday churning — "
            "the move may not sustain.\n\n"
            "**How the average is calculated:** The average volume is computed over the "
            "entire 6-month rolling window (~120 trading days), excluding the most recent "
            "consecutive days being tested. This gives a stable baseline to detect genuine spikes.\n\n"
            "**How to read the table:**\n"
            "- **Vol Ratio** — How many times current volume is vs. the 6-month average (2.5x = 2.5 times normal)\n"
            "- **Delivery Above Avg** — Green \"Yes\" = delivery-backed buying (more reliable)\n"
            "- **Price Change %** — Whether the stock moved up or down during the spike\n\n"
            "**Recommendation:** Focus on stocks where Vol Ratio is high AND Delivery Above "
            "Avg = Yes AND Price Change is positive. Highest conviction buying."
        )

    vol_threshold = st.slider("Volume above average (%)", 25, 200, 50, 25, key="vol_thresh")
    consec_days = st.slider("Consecutive high-volume days", 1, 15, 10, 1, key="vol_days")

    from src.screener_volume import screen_volume_spikes
    with st.spinner("Screening..."):
        vol_df = screen_volume_spikes(ohlcv, vol_threshold_pct=vol_threshold,
                                       consecutive_days=consec_days)

    st.metric("Stocks Found", len(vol_df))

    if not vol_df.empty:
        display_df = enrich_with_info(vol_df.copy())
        display_df = fmt_price_col(display_df, ["Current Price"])
        display_df = fmt_vol_col(display_df, ["Avg Volume", f"Last {consec_days}D Avg Vol"])
        display_df = fmt_num_col(display_df, ["Vol Ratio"])
        display_df = fmt_pct_col(display_df, ["Avg Delivery %", "Recent Delivery %", "Price Change %"])

        def highlight_delivery(val):
            if isinstance(val, str) and val == "Yes":
                return "background-color: #c8e6c9"
            return ""

        styled = display_df.style.map(highlight_delivery, subset=["Delivery Above Avg"])
        st.dataframe(styled, width="stretch", hide_index=True)
    else:
        st.info("No volume spikes detected. Try lowering the threshold.")

# ==================== Tab 8: Price-Volume Intersection ====================
with tab8:
    st.subheader("Price-Volume Intersection — Dropped Stocks With Volume Surge")

    with st.expander("What is this & how does it help?", expanded=False):
        st.markdown(
            "**What it tracks:** Stocks that appear in **both** the Price Drops screen "
            "and the Volume Buzz screen simultaneously.\n\n"
            "**Why it matters:** A stock that has dropped significantly AND is now seeing "
            "a surge in trading volume is showing signs of a potential reversal. The drop "
            "creates value, and the volume surge suggests smart money is stepping in.\n\n"
            "**How to read the table:**\n"
            "- **Drop %** — How much the stock fell from its 6-month high\n"
            "- **RSI** — Below 30 = oversold (more upside potential)\n"
            "- **Vol Ratio** — How many times current volume is vs. average\n"
            "- **Delivery Above Avg** — \"Yes\" = genuine buying, not just speculation\n\n"
            "**Recommendation:** These are the highest-conviction reversal candidates. "
            "A big drop + volume surge + high delivery = strong accumulation signal. "
            "Always check the news to rule out fundamental problems."
        )

    # Controls for this intersection
    int_drop = st.slider("Minimum drop from 6M high (%)", 10, 50, 20, 5, key="int_drop")
    int_col1, int_col2 = st.columns(2)
    with int_col1:
        int_vol = st.slider("Volume above average (%)", 25, 200, 50, 25, key="int_vol")
    with int_col2:
        int_days = st.slider("Consecutive high-volume days", 1, 15, 3, 1, key="int_days")

    from src.screener_price import screen_big_drops
    from src.screener_volume import screen_volume_spikes

    with st.spinner("Screening..."):
        int_drops_df = screen_big_drops(ohlcv, threshold_pct=int_drop)
        int_vol_df = screen_volume_spikes(ohlcv, vol_threshold_pct=int_vol,
                                           consecutive_days=int_days)

    if not int_drops_df.empty and not int_vol_df.empty:
        # Find symbols in both sets
        drop_symbols = set(int_drops_df["Symbol"].tolist())
        vol_symbols = set(int_vol_df["Symbol"].tolist())
        common = drop_symbols & vol_symbols

        st.metric("Stocks Found", len(common))

        if common:
            # Merge key columns from both screens
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
                    return "background-color: #c8e6c9"
                return ""

            styled = display_df.style.map(highlight_intersection, subset=["Delivery Above Avg"])
            st.dataframe(styled, width="stretch", hide_index=True)
        else:
            st.info("No stocks currently appear in both Price Drops and Volume Buzz. "
                    "Try adjusting the thresholds.")
    else:
        st.info("Not enough data to compute intersection. Ensure both screens have results.")

# ==================== Tab 4: Top Momentum ====================
with tab4:
    st.subheader("Top Momentum — Stocks in Strong Uptrends")

    with st.expander("What is this & how does it help?", expanded=False):
        st.markdown(
            "**What it tracks:** Stocks with multiple technical signals confirming a strong uptrend:\n\n"
            "1. **Near 6-month high** — within 10% of their highest price\n"
            "2. **Above 50-day AND 200-day moving average** — trend is up on both timeframes\n"
            "3. **RSI between 55-75** — strong momentum but NOT overbought\n"
            "4. **Supertrend = BUY** — trend-following indicator confirms upward direction\n\n"
            "**Why it matters:** \"The trend is your friend.\" Stocks already in strong uptrends "
            "tend to keep going up. This screen finds stocks with the highest probability of "
            "continuing their run.\n\n"
            "**How to read the table:**\n"
            "- **Dist from High %** — How far below the 6M high (lower = stronger)\n"
            "- **50 DMA / 200 DMA** — Moving average prices. Stock price is above both.\n"
            "- **Supertrend** — BUY = uptrend intact.\n\n"
            "**Recommendation:** These are \"ride the wave\" stocks. Enter on small dips. "
            "Set a stop-loss just below the 50 DMA."
        )

    from src.screener_momentum import screen_momentum_leaders
    with st.spinner("Screening..."):
        momentum_df = screen_momentum_leaders(ohlcv)

    st.metric("Stocks Found", len(momentum_df))

    if not momentum_df.empty:
        display_df = enrich_with_info(momentum_df.copy())
        display_df = fmt_price_col(display_df, ["Price", "6M High", "50 DMA", "200 DMA"])
        display_df = fmt_pct_col(display_df, ["Dist from High %"])
        display_df = fmt_num_col(display_df, ["RSI"])
        st.dataframe(display_df, width="stretch", hide_index=True)
    else:
        st.info("No momentum leaders found matching all criteria.")

# ==================== Tab 5: Big Player Activity ====================
with tab5:
    st.subheader("Big Player Activity — Follow the Smart Money")

    with st.expander("What is this & how does it help?", expanded=False):
        st.markdown(
            "**What it tracks:** Signs that large institutional investors, mutual funds, "
            "or wealthy individuals are quietly buying a stock.\n\n"
            "**Why it matters:** Big players do deep research before committing crores. "
            "Spotting their activity early lets you ride along.\n\n"
            "**Three sub-tabs:**\n\n"
            "**1. Bulk Deals:** When someone buys/sells a very large quantity (>0.5% of "
            "company shares) in a single trade, NSE reports it publicly.\n\n"
            "**2. Delivery Breakouts:** When shares actually delivered jump to 2x+ of the "
            "20-day average — real buyers are accumulating, not just speculators.\n\n"
            "**3. OBV Accumulation:** Detects \"silent buying\" — volume flow increasing even "
            "though price hasn't moved. Someone is quietly buying before a potential move up.\n\n"
            "**Recommendation:** The strongest signal is seeing the same stock across multiple "
            "sub-tabs — e.g., a bulk deal + delivery breakout together is very bullish."
        )

    sub1, sub2, sub3 = st.tabs(["Bulk Deals", "Delivery Breakouts", "OBV Accumulation"])

    from src.screener_smart_money import get_bulk_deals_summary, screen_delivery_breakouts, screen_obv_divergence

    with sub1:
        st.markdown("**Recent Bulk & Block Deals** (last 30 days)")
        st.caption("Large transactions reported by NSE — who bought/sold big quantities.")
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
        st.markdown("**Delivery Qty Breakouts**")
        st.caption("Stocks where actual delivery is far above normal — sign of genuine accumulation.")
        delivery_mult = st.slider("Delivery multiplier (x times average)", 1.5, 5.0, 2.0, 0.5, key="deliv_mult")
        with st.spinner("Screening..."):
            delivery_df = screen_delivery_breakouts(ohlcv, multiplier=delivery_mult)
        st.metric("Stocks Found", len(delivery_df))
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
        st.markdown("**OBV Divergence — Silent Accumulation**")
        st.caption("Volume flow rising even though price is flat — someone is quietly buying.")
        with st.spinner("Screening..."):
            obv_df = screen_obv_divergence(ohlcv)
        st.metric("Stocks Found", len(obv_df))
        if not obv_df.empty:
            display_df = enrich_with_info(obv_df.copy())
            display_df = fmt_price_col(display_df, ["Price"])
            display_df = fmt_pct_col(display_df, ["Price Change %"])
            st.dataframe(display_df, width="stretch", hide_index=True)
        else:
            st.info("No OBV divergences detected.")

# ==================== Tab 6: Sector Map ====================
with tab6:
    st.subheader("Sector Map — Which Sectors Are Hot or Cold")

    with st.expander("What is this & how does it help?", expanded=False):
        st.markdown(
            "**What it tracks:** Performance of major NSE sector indices (Banking, IT, "
            "Pharma, Metal, Auto, etc.) over different time periods.\n\n"
            "**Why it matters:** Markets move in cycles. Some sectors are in favour "
            "(money flowing in) and others are out. Buying stocks in the right sector "
            "gives you a tailwind.\n\n"
            "**How to read the heatmap:**\n"
            "- **Green** = Sector has gone up (money flowing in)\n"
            "- **Red** = Sector has fallen (money flowing out)\n"
            "- Sectors turning red-to-green (1M red but 1W green) = early recovery\n\n"
            "**Market Breadth** = overall market health:\n"
            "- **% above 200 DMA** >60% = healthy market; <40% = weak\n\n"
            "**FII/DII:**\n"
            "- **FII** = Foreign money. Net buying = bullish.\n"
            "- **DII** = Mutual funds, insurance. Often buy when FIIs sell.\n\n"
            "**Recommendation:** Favour stocks from sectors green across all periods."
        )

    from src.screener_sector import (compute_sector_performance, create_sector_heatmap,
                                      create_fii_dii_chart, compute_market_breadth)

    col_a, col_b = st.columns([2, 1])

    with col_a:
        with st.spinner("Loading sector data..."):
            sector_data = fetch_sector_indices(days=180)
            perf_df = compute_sector_performance(sector_data)

        if not perf_df.empty:
            fig = create_sector_heatmap(perf_df)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No sector data available. Click Refresh Data.")

    with col_b:
        st.markdown("**Market Breadth**")
        st.caption("Overall health — what % of stocks are in uptrends")
        with st.spinner("Computing..."):
            breadth = compute_market_breadth(ohlcv)

        if breadth["total"] > 0:
            st.metric("Stocks Analyzed", breadth["total"])
            st.metric("Above 200 DMA", f"{breadth['above_200_pct']:.2f}%",
                      help=">60% = healthy market, <40% = weak")
            st.metric("Above 50 DMA", f"{breadth['above_50_pct']:.2f}%",
                      help="Medium-term trend over ~2 months")
            st.metric("Above 20 DMA", f"{breadth['above_20_pct']:.2f}%",
                      help="Short-term momentum direction")
        else:
            st.info("Insufficient data for breadth analysis.")

    st.markdown("---")
    st.markdown("**FII / DII Activity** (last 30 days)")
    st.caption("Net buying/selling by Foreign (FII) and Domestic (DII) institutions in crores.")
    with st.spinner("Loading FII/DII data..."):
        fii_dii = fetch_fii_dii_data(days=30)
    if not fii_dii.empty:
        fig2 = create_fii_dii_chart(fii_dii)
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No FII/DII data available.")

# ==================== Tab 7: Warning Signs ====================
with tab7:
    st.subheader("Warning Signs — Stocks to Be Cautious About")

    with st.expander("What is this & how does it help?", expanded=False):
        st.markdown(
            "**What it tracks:** Red flags — technical breakdowns, speculative rallies, "
            "and deep downtrends.\n\n"
            "**Why it matters:** Avoiding bad stocks is as important as finding good ones.\n\n"
            "**Four warning types:**\n\n"
            "**1. Death Cross:** 50-day MA crosses BELOW 200-day MA — one of the most "
            "widely-watched bearish signals. Stocks often keep falling after this.\n\n"
            "**2. Speculative Rallies:** Price up BUT delivery % falling — buying is mostly "
            "day-traders, not real investors. These rallies often reverse sharply.\n\n"
            "**3. Below All MAs:** Below 20/50/100/200-day averages simultaneously — "
            "deep downtrend with no support holding.\n\n"
            "**4. High Pledging:** Promoters pledged shares as loan collateral. If price "
            "falls, forced selling creates a vicious crash cycle.\n\n"
            "**Recommendation:** If a stock you hold appears here, review it carefully. "
            "If considering buying — think twice."
        )

    from src.screener_red_flags import (screen_high_pledge, screen_death_cross,
                                         screen_falling_delivery, screen_below_all_mas)

    warn1, warn2, warn3, warn4 = st.tabs([
        "Death Cross", "Speculative Rallies", "Below All MAs", "High Pledging"
    ])

    with warn1:
        st.markdown("**Death Cross** — 50-day average crossed below 200-day average")
        st.caption("Classic bearish signal. Medium-term trend has turned negative.")
        with st.spinner("Screening..."):
            death_df = screen_death_cross(ohlcv, lookback=10)
        st.metric("Stocks Found", len(death_df))
        if not death_df.empty:
            display_df = enrich_with_info(death_df.copy())
            display_df = fmt_price_col(display_df, ["Price", "50 DMA", "200 DMA"])
            st.dataframe(display_df, width="stretch", hide_index=True)
        else:
            st.info("No recent death crosses detected.")

    with warn2:
        st.markdown("**Speculative Rallies** — price rising, but delivery % falling")
        st.caption("Price increase driven by day-traders, not real buying. Often reverses.")
        with st.spinner("Screening..."):
            spec_df = screen_falling_delivery(ohlcv, lookback=10)
        st.metric("Stocks Found", len(spec_df))
        if not spec_df.empty:
            display_df = enrich_with_info(spec_df.copy())
            display_df = fmt_price_col(display_df, ["Price"])
            display_df = fmt_pct_col(display_df, ["Price Change %", "Delivery % Start", "Delivery % End"])
            st.dataframe(display_df, width="stretch", hide_index=True)
        else:
            st.info("No speculative rallies detected.")

    with warn3:
        st.markdown("**Below All Moving Averages** — in deep downtrend")
        st.caption("Below 20/50/100/200-day averages. All timeframes bearish.")
        with st.spinner("Screening..."):
            below_df = screen_below_all_mas(ohlcv)
        st.metric("Stocks Found", len(below_df))
        if not below_df.empty:
            display_df = enrich_with_info(below_df.copy())
            display_df = fmt_price_col(display_df, ["Price", "20 DMA", "50 DMA", "200 DMA"])
            display_df = fmt_pct_col(display_df, ["Below 200 DMA %"])
            st.dataframe(display_df, width="stretch", hide_index=True)
        else:
            st.info("No stocks below all moving averages.")

    with warn4:
        st.markdown("**High Promoter Pledging** — owners have pledged their shares")
        st.caption("Pledged shares as loan collateral. Price drop can trigger forced selling.")
        pledge_thresh = st.slider("Minimum pledge %", 5, 50, 20, 5, key="pledge_thresh")
        with st.spinner("Fetching..."):
            promoter_df = fetch_promoter_data()
            pledge_df = screen_high_pledge(promoter_df, threshold_pct=pledge_thresh)
        st.metric("Stocks Found", len(pledge_df))
        if not pledge_df.empty:
            display_df = enrich_with_info(pledge_df.copy())
            display_df = fmt_pct_col(display_df, ["Promoter Holding %", "Pledge %"])
            st.dataframe(display_df, width="stretch", hide_index=True)
        else:
            st.info("No promoter pledging data available. This requires quarterly filings data.")
