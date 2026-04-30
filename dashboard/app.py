# -*- coding: utf-8 -*-
"""
WMATA Metro Dashboard - Real-time train prediction analytics.
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import timezone, timedelta
from zoneinfo import ZoneInfo
import streamlit.components.v1 as components
from sqlalchemy import create_engine, text

# ----------------------------------------------
# Page config
# ----------------------------------------------
st.set_page_config(
    page_title="WMATA Metro Dashboard",
    page_icon="🚇",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ----------------------------------------------
# CSS - clean, light, editorial
# Fonts: Spectral (headings) + IBM Plex Mono (data)
# Palette: warm off-white bg, near-black text, line colors only
# ----------------------------------------------
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Spectral:wght@600;700&family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@300;400;500&display=swap');

html, body, [class*="css"] {
    font-family: 'IBM Plex Sans', sans-serif;
    background-color: #f7f6f2;
    color: #1c1c1c;
}
.stApp { background-color: #f7f6f2; }

[data-testid="stSidebar"] {
    background-color: #ffffff !important;
    border-right: 1px solid #e8e6e0 !important;
}
[data-testid="stSidebar"] * { color: #1c1c1c; }

.block-container { padding-top: 2rem; }

h1 {
    font-family: 'Spectral', serif !important;
    font-weight: 700 !important;
    font-size: 1.9rem !important;
    letter-spacing: -0.02em;
    color: #1c1c1c;
}
h2, h3 {
    font-family: 'Spectral', serif !important;
    font-weight: 600 !important;
    letter-spacing: -0.01em;
    color: #1c1c1c;
}

[data-testid="stMetric"] {
    background: #ffffff;
    border: 1px solid #e8e6e0;
    border-radius: 4px;
    padding: 1rem 1.2rem;
}
[data-testid="stMetricLabel"] {
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.65rem !important;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: #999 !important;
}
[data-testid="stMetricValue"] {
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 1.55rem !important;
    color: #1c1c1c !important;
}

hr { border: none; border-top: 1px solid #e8e6e0 !important; margin: 1.4rem 0; }

.layer-row {
    display: flex;
    gap: 1px;
    background: #e8e6e0;
    border: 1px solid #e8e6e0;
    border-radius: 4px;
    overflow: hidden;
    margin-bottom: 1.6rem;
}
.layer-cell {
    flex: 1;
    background: #ffffff;
    padding: 1rem 1.1rem;
}
.layer-label {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.6rem;
    text-transform: uppercase;
    letter-spacing: 0.12em;
    color: #aaa;
    margin-bottom: 4px;
}
.layer-value {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 1.35rem;
    font-weight: 500;
    color: #1c1c1c;
    line-height: 1;
}
.layer-sub {
    font-size: 0.72rem;
    color: #999;
    margin-top: 3px;
}

.section-label {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.65rem;
    text-transform: uppercase;
    letter-spacing: 0.12em;
    color: #aaa;
    margin-bottom: 0.5rem;
}

[data-testid="stDataFrame"] {
    border: 1px solid #e8e6e0 !important;
    border-radius: 4px;
    overflow: hidden;
}

/* Line-colored multiselect pills */
[data-testid="stMultiSelect"] span[data-baseweb="tag"] {
    border-radius: 3px !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.72rem !important;
    font-weight: 500 !important;
}
[data-testid="stMultiSelect"] span[data-baseweb="tag"]:has(span[title="Red"]) {
    background-color: #BF0D3E !important; color: #fff !important;
}
[data-testid="stMultiSelect"] span[data-baseweb="tag"]:has(span[title="Blue"]) {
    background-color: #009CDE !important; color: #fff !important;
}
[data-testid="stMultiSelect"] span[data-baseweb="tag"]:has(span[title="Orange"]) {
    background-color: #ED8B00 !important; color: #fff !important;
}
[data-testid="stMultiSelect"] span[data-baseweb="tag"]:has(span[title="Silver"]) {
    background-color: #919D9D !important; color: #fff !important;
}
[data-testid="stMultiSelect"] span[data-baseweb="tag"]:has(span[title="Green"]) {
    background-color: #00B140 !important; color: #fff !important;
}
[data-testid="stMultiSelect"] span[data-baseweb="tag"]:has(span[title="Yellow"]) {
    background-color: #FFD100 !important; color: #1c1c1c !important;
}
[data-testid="stMultiSelect"] span[data-baseweb="tag"] span[role="presentation"] {
    color: inherit !important;
}
</style>
""", unsafe_allow_html=True)

# ----------------------------------------------
# Database
# ----------------------------------------------
@st.cache_resource
def get_engine():
    db_url = os.getenv(
        "DATABASE_URL",
        f"postgresql://{os.getenv('POSTGRES_USER', 'postgres')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'postgres')}@"
        f"{os.getenv('POSTGRES_HOST', 'postgres')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/"
        f"wmata_etl"
    )
    return create_engine(db_url)


def run_query(query: str, params: dict = None) -> pd.DataFrame:
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        return pd.DataFrame(result.fetchall(), columns=result.keys())


# ----------------------------------------------
# Line metadata
# ----------------------------------------------
LINE_COLORS = {
    "RD": "#BF0D3E", "BL": "#009CDE", "OR": "#ED8B00",
    "SV": "#919D9D", "GR": "#00B140", "YL": "#FFD100",
}
LINE_NAMES = {
    "RD": "Red", "BL": "Blue", "OR": "Orange",
    "SV": "Silver", "GR": "Green", "YL": "Yellow",
}

CHART_BASE = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color="#999", family="IBM Plex Mono", size=11),
    xaxis=dict(gridcolor="#efefec", linecolor="#e8e6e0", tickfont=dict(size=10)),
    yaxis=dict(gridcolor="#efefec", linecolor="#e8e6e0", tickfont=dict(size=10)),
    margin=dict(l=8, r=8, t=36, b=8),
)

# ----------------------------------------------
# Guard - no data yet
# ----------------------------------------------
try:
    count_df = run_query("SELECT COUNT(*) as cnt FROM gold.station_wait_times")
    total_records = int(count_df.iloc[0]["cnt"])
except Exception:
    total_records = 0

if total_records == 0:
    st.warning("No data yet. Run the ETL pipeline: `make run`")
    st.stop()

# ----------------------------------------------
# Sidebar
# ----------------------------------------------
try:
    st.logo("WMATA_Metro_Logo.svg")
except Exception:
    pass

st.sidebar.markdown(
    "<p style='font-family:IBM Plex Mono,monospace;font-size:0.65rem;"
    "letter-spacing:0.12em;text-transform:uppercase;color:#aaa;"
    "margin:0 0 1rem 0'>Rail Analytics</p>",
    unsafe_allow_html=True,
)

time_range = st.sidebar.selectbox(
    "Time Range",
    [
        "Last 1 Hour", "Last 6 Hours", "Last 24 Hours",
        "Last 7 Days", "Last 30 Days", "Last 3 Months",
        "Last 6 Months", "Last 1 Year", "All Data",
    ],
)
interval = {
    "Last 1 Hour": "1 hour", "Last 6 Hours": "6 hours",
    "Last 24 Hours": "24 hours", "Last 7 Days": "7 days",
    "Last 30 Days": "30 days", "Last 3 Months": "90 days",
    "Last 6 Months": "180 days", "Last 1 Year": "365 days",
    "All Data": "9999 days",
}[time_range]

selected_lines = st.sidebar.multiselect(
    "Lines",
    options=list(LINE_NAMES.keys()),
    default=list(LINE_NAMES.keys()),
    format_func=lambda x: LINE_NAMES[x],
)
if not selected_lines:
    st.warning("Select at least one metro line.")
    st.stop()

line_list = list(selected_lines)

st.sidebar.markdown("---")
st.sidebar.markdown(
    "<p style='font-size:0.7rem;color:#bbb;font-family:IBM Plex Mono,monospace'>"
    "Airflow · PostgreSQL · Streamlit</p>",
    unsafe_allow_html=True,
)

# ----------------------------------------------
# Header
# ----------------------------------------------
logo_col, title_col = st.columns([1, 11])
with logo_col:
    try:
        st.image("WMATA_Metro_Logo.svg", width=52)
    except Exception:
        st.markdown(
            "<span style='font-size:2rem'>🚇</span>",
            unsafe_allow_html=True,
        )
with title_col:
    st.markdown(
        "<h1 style='margin:4px;padding-top:4px'>WMATA Metro Dashboard</h1>"
        "<p style='font-family:IBM Plex Mono,monospace;font-size:0.68rem;"
        "color:#aaa;margin:2px 0 0 0;letter-spacing:0.06em'>"
        "REAL-TIME RAIL PREDICTIONS · BRONZE → SILVER → GOLD</p>",
        unsafe_allow_html=True,
    )

st.divider()

# ----------------------------------------------
# Data queries for storytelling KPIs
# ----------------------------------------------
kpi = run_query(
    "SELECT ROUND(AVG(avg_wait_minutes)::numeric, 1) AS avg_wait, "
    "COUNT(DISTINCT calculated_at) AS etl_runs, "
    "MAX(calculated_at) AS last_update "
    "FROM gold.station_wait_times "
    "WHERE calculated_at > NOW() - CAST(:interval AS interval) "
    "AND line = ANY(:lines)",
    {"interval": interval, "lines": line_list},
)

# Previous period for delta comparison
prev_kpi = run_query(
    "SELECT ROUND(AVG(avg_wait_minutes)::numeric, 1) AS avg_wait "
    "FROM gold.station_wait_times "
    "WHERE calculated_at BETWEEN "
    "(NOW() - CAST(:interval AS interval) - CAST(:interval AS interval)) "
    "AND (NOW() - CAST(:interval AS interval)) "
    "AND line = ANY(:lines)",
    {"interval": interval, "lines": line_list},
)

# Best and worst line
line_rank = run_query(
    "SELECT line, ROUND(AVG(avg_wait_minutes)::numeric, 1) AS avg_wait "
    "FROM gold.station_wait_times "
    "WHERE calculated_at > NOW() - CAST(:interval AS interval) "
    "AND line = ANY(:lines) "
    "GROUP BY line ORDER BY avg_wait",
    {"interval": interval, "lines": line_list},
)

# Pipeline health
pipeline_health = run_query(
    "SELECT COUNT(*) AS total, "
    "SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS ok "
    "FROM gold.pipeline_runs "
    "WHERE started_at > NOW() - CAST(:interval AS interval)",
    {"interval": interval},
)

# Compute deltas and story
sys_avg = float(kpi.iloc[0]["avg_wait"] or 0)
prev_avg = float(prev_kpi.iloc[0]["avg_wait"] or 0) if not prev_kpi.empty and prev_kpi.iloc[0]["avg_wait"] else 0
delta = round(sys_avg - prev_avg, 1) if prev_avg else None
etl_runs = int(kpi.iloc[0]["etl_runs"] or 0)

worst_line = line_rank.iloc[-1] if not line_rank.empty else None
best_line = line_rank.iloc[0] if not line_rank.empty else None

p_total = int(pipeline_health.iloc[0]["total"] or 0) if not pipeline_health.empty else 0
p_ok = int(pipeline_health.iloc[0]["ok"] or 0) if not pipeline_health.empty else 0

# ----------------------------------------------
# Dynamic headline
# ----------------------------------------------
if worst_line is not None and sys_avg > 0:
    worst_name = LINE_NAMES.get(worst_line["line"], worst_line["line"])
    worst_color = LINE_COLORS.get(worst_line["line"], "#1c1c1c")
    worst_pct = round((float(worst_line["avg_wait"]) - sys_avg) / sys_avg * 100) if sys_avg else 0
    if worst_pct > 15:
        headline = (
            f"<span style='color:{worst_color};font-weight:700'>{worst_name} Line</span>"
            f" averaging {worst_line['avg_wait']} min - "
            f"<span style='color:#BF0D3E'>{worst_pct}% above system average</span>"
        )
    elif delta is not None and delta > 0.3:
        headline = f"System wait times rising - <span style='color:#BF0D3E'>+{delta} min</span> vs. previous period"
    elif delta is not None and delta < -0.3:
        headline = f"Wait times improving - <span style='color:#00B140'>{delta} min</span> vs. previous period"
    else:
        headline = f"All lines within normal range - System avg <strong>{sys_avg} min</strong>"
else:
    headline = "Collecting data…"

st.markdown(
    f"<p style='font-family:IBM Plex Sans,sans-serif;font-size:1.05rem;"
    f"color:#555;margin:0 0 1rem 0;line-height:1.5'>{headline}</p>",
    unsafe_allow_html=True,
)

# ----------------------------------------------
# Story KPIs — 4 cards with meaning
# ----------------------------------------------
c1, c2, c3, c4 = st.columns(4)

delta_str = f"{delta:+.1f}" if delta is not None else None
c1.metric("Avg Wait", f"{sys_avg} min", delta=f"{delta_str} min" if delta_str else None, delta_color="inverse")

if worst_line is not None:
    c2.metric(
        "Longest Wait",
        f"{worst_line['avg_wait']} min",
        delta=LINE_NAMES.get(worst_line["line"], worst_line["line"]),
        delta_color="off",
    )
else:
    c2.metric("Longest Wait", "-")

if best_line is not None:
    c3.metric(
        "Shortest Wait",
        f"{best_line['avg_wait']} min",
        delta=LINE_NAMES.get(best_line["line"], best_line["line"]),
        delta_color="off",
    )
else:
    c3.metric("Shortest Wait", "-")

if p_total > 0:
    health_icon = "✅" if p_ok == p_total else "⚠️"
    c4.metric("Pipeline Health", f"{health_icon} {p_ok}/{p_total} runs")
else:
    lu = kpi.iloc[0]["last_update"]
    if lu:
        eastern = ZoneInfo("America/New_York")
        lu_local = lu.astimezone(eastern) if lu.tzinfo else lu.replace(tzinfo=timezone.utc).astimezone(eastern)
        c4.metric("Last Update", lu_local.strftime("%H:%M:%S"))
    else:
        c4.metric("Last Update", "-")

# ----------------------------------------------
# Line performance — "Which lines need attention?"
# ----------------------------------------------
st.divider()

line_df = run_query(
    "SELECT line, ROUND(AVG(avg_wait_minutes)::numeric, 2) AS avg_wait, "
    "COUNT(DISTINCT station_code) AS stations, SUM(train_count) AS trains "
    "FROM gold.station_wait_times "
    "WHERE calculated_at > NOW() - CAST(:interval AS interval) "
    "AND line = ANY(:lines) GROUP BY line ORDER BY avg_wait DESC",
    {"interval": interval, "lines": line_list},
)
line_df["line_name"] = line_df["line"].map(LINE_NAMES)

# Historical average per line (double the interval for comparison)
hist_df = run_query(
    "SELECT line, ROUND(AVG(avg_wait_minutes)::numeric, 2) AS hist_avg "
    "FROM gold.station_wait_times "
    "WHERE calculated_at BETWEEN "
    "(NOW() - CAST(:interval AS interval) - CAST(:interval AS interval)) "
    "AND (NOW() - CAST(:interval AS interval)) "
    "AND line = ANY(:lines) GROUP BY line",
    {"interval": interval, "lines": line_list},
)

st.markdown("<p class='section-label'>Which lines need attention?</p>", unsafe_allow_html=True)

left, right = st.columns([3, 2])

with left:
    # Horizontal bar with system-average benchmark
    fig_bar = px.bar(
        line_df.sort_values("avg_wait"),
        y="line_name", x="avg_wait",
        color="line", color_discrete_map=LINE_COLORS,
        labels={"avg_wait": "Avg wait (min)", "line_name": ""},
        text_auto=".1f",
        orientation="h",
        title="Average Wait by Line",
    )
    fig_bar.update_traces(marker_line_width=0, textposition="outside")
    # System average benchmark line
    fig_bar.add_vline(
        x=sys_avg, line_dash="dash", line_color="#BF0D3E", line_width=1.5,
        annotation_text=f"System avg {sys_avg}",
        annotation_position="top right",
        annotation_font=dict(size=10, color="#BF0D3E", family="IBM Plex Mono"),
    )
    fig_bar.update_layout(
        **CHART_BASE, height=340, showlegend=False,
        title_font=dict(family="Spectral", size=13, color="#1c1c1c"),
        title_x=0,
    )
    fig_bar.update_yaxes(gridcolor="rgba(0,0,0,0)", linecolor="#e8e6e0", tickfont=dict(size=11))
    st.plotly_chart(fig_bar, use_container_width=True)

with right:
    # Current vs historical comparison
    if not hist_df.empty:
        compare = line_df[["line", "line_name", "avg_wait"]].merge(hist_df, on="line", how="left")
        compare["hist_avg"] = compare["hist_avg"].fillna(compare["avg_wait"])
        compare["change"] = compare["avg_wait"] - compare["hist_avg"]
        compare = compare.sort_values("change", ascending=False)

        fig_comp = go.Figure()
        fig_comp.add_trace(go.Bar(
            y=compare["line_name"], x=compare["hist_avg"],
            name="Previous", orientation="h",
            marker_color="#e0ddd5", text=compare["hist_avg"].apply(lambda v: f"{v:.1f}"),
            textposition="inside",
        ))
        fig_comp.add_trace(go.Bar(
            y=compare["line_name"], x=compare["avg_wait"],
            name="Current", orientation="h",
            marker_color=[LINE_COLORS.get(l, "#999") for l in compare["line"]],
            text=compare["avg_wait"].apply(lambda v: f"{v:.1f}"),
            textposition="inside",
        ))
        fig_comp.update_layout(
            **CHART_BASE, height=340, barmode="overlay",
            title="Current vs. Previous Period",
            title_font=dict(family="Spectral", size=13, color="#1c1c1c"),
            title_x=0,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, font=dict(size=10)),
        )
        fig_comp.update_yaxes(gridcolor="rgba(0,0,0,0)", linecolor="#e8e6e0", tickfont=dict(size=11))
        st.plotly_chart(fig_comp, use_container_width=True)
    else:
        st.info("Not enough historical data for comparison yet.")

st.divider()

# ----------------------------------------------
# Wait time trend — "Is it getting better or worse?"
# ----------------------------------------------
trend_df = run_query(
    "SELECT line, DATE_TRUNC('hour', calculated_at) AS hour, "
    "ROUND(AVG(avg_wait_minutes)::numeric, 2) AS avg_wait "
    "FROM gold.station_wait_times "
    "WHERE calculated_at > NOW() - CAST(:interval AS interval) "
    "AND line = ANY(:lines) "
    "GROUP BY line, DATE_TRUNC('hour', calculated_at) ORDER BY hour",
    {"interval": interval, "lines": line_list},
)

st.markdown("<p class='section-label'>Is it getting better or worse?</p>", unsafe_allow_html=True)

if not trend_df.empty:
    # Convert UTC hours to Eastern for correct rush-hour shading
    eastern = ZoneInfo("America/New_York")
    hours = pd.to_datetime(trend_df["hour"])
    if hours.dt.tz is None:
        hours = hours.dt.tz_localize("UTC")
    trend_df["hour"] = hours.dt.tz_convert(eastern)
    trend_df = trend_df.dropna(subset=["hour"])

    fig_trend = px.line(
        trend_df, x="hour", y="avg_wait",
        color="line", color_discrete_map=LINE_COLORS,
        labels={"avg_wait": "Avg wait (min)", "hour": "", "line": ""},
        markers=True,
    )
    fig_trend.update_traces(line_width=1.8, marker_size=4)

    # Normal range band (2–6 min)
    fig_trend.add_hrect(
        y0=2, y1=6,
        fillcolor="#00B140", opacity=0.06,
        line_width=0,
        annotation_text="Normal range",
        annotation_position="top left",
        annotation_font=dict(size=9, color="#00B140", family="IBM Plex Mono"),
    )

    # Peak hour shading — morning (7-9) and evening (16-19) Eastern
    hours = trend_df["hour"]
    if not hours.empty:
        base_date = hours.min()
        for label, start_h, end_h in [("AM Rush", 7, 9), ("PM Rush", 16, 19)]:
            try:
                t0 = base_date.replace(hour=start_h, minute=0, second=0)
                t1 = base_date.replace(hour=end_h, minute=0, second=0)
                fig_trend.add_vrect(
                    x0=t0, x1=t1,
                    fillcolor="#ED8B00", opacity=0.06,
                    line_width=0,
                    annotation_text=label,
                    annotation_position="top left",
                    annotation_font=dict(size=9, color="#ED8B00", family="IBM Plex Mono"),
                )
            except Exception:
                pass

    fig_trend.update_layout(**CHART_BASE, height=320)
    st.plotly_chart(fig_trend, use_container_width=True)
else:
    st.info("Not enough data for trend analysis.")

st.divider()

# ----------------------------------------------
# Day × Hour heatmap — "When is congestion worst?"
# ----------------------------------------------
heatmap_df = run_query(
    "SELECT EXTRACT(DOW FROM calculated_at) AS dow, "
    "EXTRACT(HOUR FROM calculated_at) AS hour, "
    "ROUND(AVG(avg_wait_minutes)::numeric, 1) AS avg_wait "
    "FROM gold.station_wait_times "
    "WHERE calculated_at > NOW() - CAST(:interval AS interval) "
    "AND line = ANY(:lines) "
    "GROUP BY dow, hour ORDER BY dow, hour",
    {"interval": interval, "lines": line_list},
)

if not heatmap_df.empty:
    st.markdown("<p class='section-label'>When is congestion worst?</p>", unsafe_allow_html=True)

    day_labels = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    heatmap_df["dow"] = pd.to_numeric(heatmap_df["dow"], errors="coerce").astype(int)
    heatmap_df["hour"] = pd.to_numeric(heatmap_df["hour"], errors="coerce").astype(int)
    heatmap_df["avg_wait"] = pd.to_numeric(heatmap_df["avg_wait"], errors="coerce")
    pivot = heatmap_df.pivot(index="dow", columns="hour", values="avg_wait")
    pivot.index = [day_labels[int(i)] for i in pivot.index]
    pivot.columns = [f"{int(h)}:00" for h in pivot.columns]

    fig_heat = px.imshow(
        pivot,
        labels=dict(x="Hour", y="Day", color="Avg Wait (min)"),
        color_continuous_scale=["#e8f5e9", "#fff8e1", "#fce4ec"],
        aspect="auto",
    )
    fig_heat.update_layout(
        **CHART_BASE, height=280,
        coloraxis_colorbar=dict(title="min", tickfont=dict(size=10)),
    )
    st.plotly_chart(fig_heat, use_container_width=True)

st.divider()

# ----------------------------------------------
# Station drill-down — "Zoom in"
# ----------------------------------------------
station_df = run_query(
    "SELECT station_code, station_name, line, "
    "ROUND(AVG(avg_wait_minutes)::numeric, 1) AS avg_wait_minutes, "
    "ROUND(MIN(min_wait_minutes)::numeric, 1) AS min_wait_minutes, "
    "ROUND(MAX(max_wait_minutes)::numeric, 1) AS max_wait_minutes, "
    "SUM(train_count) AS train_count "
    "FROM gold.station_wait_times "
    "WHERE calculated_at > NOW() - CAST(:interval AS interval) "
    "AND line = ANY(:lines) "
    "GROUP BY station_code, station_name, line "
    "ORDER BY avg_wait_minutes DESC",
    {"interval": interval, "lines": line_list},
)

if not station_df.empty:
    for col in ["avg_wait_minutes", "min_wait_minutes", "max_wait_minutes"]:
        station_df[col] = pd.to_numeric(station_df[col], errors="coerce")
    station_df["Line"] = station_df["line"].map(LINE_NAMES)

    st.markdown("<p class='section-label'>Where are riders waiting longest?</p>", unsafe_allow_html=True)

    worst_col, best_col = st.columns(2)

    # Color helper
    def wait_color(val):
        try:
            val = float(val)
        except (ValueError, TypeError):
            return ""
        if val >= 8:
            return "background-color: #fce4ec; color: #BF0D3E"
        elif val >= 5:
            return "background-color: #fff8e1; color: #ED8B00"
        else:
            return "background-color: #e8f5e9; color: #00B140"

    top_worst = station_df.nlargest(10, "avg_wait_minutes")[["station_name", "Line", "avg_wait_minutes", "max_wait_minutes"]].rename(columns={
        "station_name": "Station", "avg_wait_minutes": "Avg Wait", "max_wait_minutes": "Max Wait",
    })
    top_best = station_df.nsmallest(10, "avg_wait_minutes")[["station_name", "Line", "avg_wait_minutes", "min_wait_minutes"]].rename(columns={
        "station_name": "Station", "avg_wait_minutes": "Avg Wait", "min_wait_minutes": "Min Wait",
    })

    with worst_col:
        st.markdown(
            "<p style='font-family:IBM Plex Mono,monospace;font-size:0.7rem;"
            "color:#BF0D3E;margin-bottom:4px'>TOP 10 · LONGEST WAITS</p>",
            unsafe_allow_html=True,
        )
        styled_worst = top_worst.style.format({"Avg Wait": "{:.1f}", "Max Wait": "{:.1f}"}).map(wait_color, subset=["Avg Wait"])
        st.dataframe(styled_worst, use_container_width=True, hide_index=True, height=380)

    with best_col:
        st.markdown(
            "<p style='font-family:IBM Plex Mono,monospace;font-size:0.7rem;"
            "color:#00B140;margin-bottom:4px'>TOP 10 · SHORTEST WAITS</p>",
            unsafe_allow_html=True,
        )
        styled_best = top_best.style.format({"Avg Wait": "{:.1f}", "Min Wait": "{:.1f}"}).map(wait_color, subset=["Avg Wait"])
        st.dataframe(styled_best, use_container_width=True, hide_index=True, height=380)

st.divider()

# ----------------------------------------------
# Pipeline observability — collapsed (engine room)
# ----------------------------------------------
with st.expander("Pipeline Observability", expanded=False):
    obs_left, obs_right = st.columns(2)

    with obs_left:
        st.markdown("<p class='section-label'>Pipeline Layer Health</p>", unsafe_allow_html=True)
        try:
            b = run_query("SELECT COUNT(*) AS cnt, MAX(extracted_at) AS ts FROM bronze.raw_predictions")
            s = run_query("SELECT COUNT(*) AS cnt, MAX(cleaned_at) AS ts FROM silver.cleaned_predictions")
            g = run_query("SELECT COUNT(*) AS cnt, MAX(calculated_at) AS ts FROM gold.station_wait_times")

            b_cnt = int(b.iloc[0]["cnt"] or 0)
            s_cnt = int(s.iloc[0]["cnt"] or 0)
            g_cnt = int(g.iloc[0]["cnt"] or 0)
            yield_ = (s_cnt / b_cnt * 100) if b_cnt else 0
            fmt = lambda ts: ts.strftime("%H:%M") if ts else "-"

            st.markdown(f"""
            <div class="layer-row">
              <div class="layer-cell">
                <div class="layer-label">Bronze · Raw</div>
                <div class="layer-value">{b_cnt:,}</div>
                <div class="layer-sub">API responses · {fmt(b.iloc[0]["ts"])}</div>
              </div>
              <div class="layer-cell">
                <div class="layer-label">Silver · Cleaned</div>
                <div class="layer-value">{s_cnt:,}</div>
                <div class="layer-sub">Yield {yield_:.1f}%</div>
              </div>
              <div class="layer-cell">
                <div class="layer-label">Gold · Aggregated</div>
                <div class="layer-value">{g_cnt:,}</div>
                <div class="layer-sub">{fmt(g.iloc[0]["ts"])}</div>
              </div>
            </div>
            """, unsafe_allow_html=True)
        except Exception:
            st.info("Pipeline layer data not yet available.")

    with obs_right:
        st.markdown("<p class='section-label'>Recent Pipeline Runs</p>", unsafe_allow_html=True)
        try:
            runs_df = run_query(
                "SELECT run_id, started_at, completed_at, status, "
                "records_extracted, records_cleaned, records_loaded "
                "FROM gold.pipeline_runs "
                "WHERE started_at > NOW() - CAST(:interval AS interval) "
                "ORDER BY started_at DESC LIMIT 10",
                {"interval": interval},
            )
            if not runs_df.empty:
                eastern = ZoneInfo("America/New_York")
                for col in ["started_at", "completed_at"]:
                    ts = pd.to_datetime(runs_df[col], errors="coerce")
                    ts = ts.dt.tz_convert(eastern) if ts.dt.tz else ts.dt.tz_localize("UTC").dt.tz_convert(eastern)
                    runs_df[col] = ts.dt.strftime("%H:%M:%S").fillna("-")
                status_map = {"success": "✅", "failed": "❌", "running": "🔄"}
                runs_df["status"] = runs_df["status"].map(lambda s: f'{status_map.get(s, "⚠️")} {s}')
                st.dataframe(
                    runs_df.rename(columns={
                        "run_id": "Run", "started_at": "Start", "completed_at": "End",
                        "status": "Status", "records_extracted": "Ext",
                        "records_cleaned": "Clean", "records_loaded": "Load",
                    }),
                    use_container_width=True, hide_index=True, height=320,
                )
            else:
                st.info("No pipeline runs recorded yet.")
        except Exception as e:
            st.info(f"Pipeline activity: {e}")

# ----------------------------------------------
# Auto-refresh every 5 minutes (300 000 ms)
# ----------------------------------------------
components.html("""
<script>
setTimeout(function() {
    window.parent.location.reload();
}, 300000);
</script>
""", height=0)

# ----------------------------------------------
# Footer
# ----------------------------------------------
st.divider()
st.markdown(
    "<p style='font-family:IBM Plex Mono,monospace;font-size:0.65rem;"
    "color:#bbb;text-align:center;letter-spacing:0.06em'>"
    "WMATA ETL · Bronze → Silver → Gold · "
    "Apache Airflow · PostgreSQL · Streamlit · Auto-refresh 5 min</p>",
    unsafe_allow_html=True,
)