"""Streamlit dashboard. Reads the small Parquet aggregates produced by PySpark."""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.config import REGION_BY_MODE, processed_dir  # noqa: E402
from src.regions import REGIONS  # noqa: E402

st.set_page_config(page_title="Taxi Trip Analytics", page_icon="🚕", layout="wide")

DOW_ORDER = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
KM_PER_MILE = 1.609344
DIST_LABELS = {"0-2 miles": "0-3 km", "2-5 miles": "3-8 km", "5-10 miles": "8-16 km",
               "10-20 miles": "16-32 km", "20+ miles": "32+ km"}
DIST_ORDER = list(DIST_LABELS.values())
MONEY_COLS = {"revenue", "avg_fare", "avg_tip", "avg_revenue_per_trip", "total_amount", "fare_amount",
              "tip_amount", "total_revenue", "avg_revenue", "fare_sum", "tip_sum", "card_fare_sum", "card_tip_sum"}
DIST_COLS = {"trip_distance", "avg_distance", "distance_sum", "median_distance"}
USD_TO_INR = 85.0  # overwritten below (1.0 for Mumbai data, which is already in INR/km)
DIST_FACTOR = KM_PER_MILE
SUM_COLS = ["trips", "revenue", "fare_sum", "distance_sum", "duration_sum", "tip_sum",
            "card_fare_sum", "card_tip_sum", "passenger_sum"]
COLOR = "#1f6feb"
st.markdown(
    "<style>"
    "[data-testid='stMetric']{background:rgba(128,128,128,0.12);"
    "border:1px solid rgba(128,128,128,0.35);padding:12px 16px;border-radius:8px}"
    "[data-testid='stMetricValue']{font-size:1.6rem}"
    "</style>",
    unsafe_allow_html=True,
)


def _group(whole: str) -> str:
    """Indian digit grouping: 1234567 -> 12,34,567."""
    if len(whole) <= 3:
        return whole
    head = re.sub(r"(\d)(?=(\d\d)+$)", r"\1,", whole[:-3])
    return f"{head},{whole[-3:]}"


def num(x: float, decimals: int = 0) -> str:
    whole, _, frac = f"{abs(x):.{decimals}f}".partition(".")
    return ("-" if x < 0 else "") + _group(whole) + (f".{frac}" if frac else "")


def inr(x: float, decimals: int = 0) -> str:
    return "₹" + num(x, decimals)


# ---------- data loading ----------
@st.cache_data(show_spinner=False)
def _read(mode: str, name: str) -> pd.DataFrame | None:
    path = processed_dir(mode) / name
    return pd.read_parquet(path) if path.exists() else None


def load_table(mode: str, name: str) -> pd.DataFrame | None:
    """Read a processed table and convert USD -> INR and miles -> km for display."""
    df = _read(mode, name)
    if df is None:
        return None
    df = df.copy()
    for col in MONEY_COLS & set(df.columns):
        df[col] = df[col] * USD_TO_INR
    for col in DIST_COLS & set(df.columns):
        df[col] = df[col] * DIST_FACTOR
    if "fare_per_mile" in df.columns:  # now rupees per km
        df["fare_per_mile"] = df["fare_per_mile"] * USD_TO_INR / DIST_FACTOR
    if "trip_distance_category" in df.columns:
        df["trip_distance_category"] = df["trip_distance_category"].replace(DIST_LABELS)
    if name == "distance_hist":
        df["km"] = (df["distance_bucket"] * DIST_FACTOR).round(1)
    if name == "anomaly_summary":
        factor = {"trip_distance": DIST_FACTOR, "fare_amount": USD_TO_INR, "fare_per_mile": USD_TO_INR / DIST_FACTOR}
        scale = df["metric"].map(factor).fillna(1.0)
        df["mean"], df["std_dev"] = df["mean"] * scale, df["std_dev"] * scale
    return df


def fig_show(fig, key: str) -> None:
    fig.update_layout(template="plotly_white", margin=dict(l=10, r=10, t=40, b=10))
    st.plotly_chart(fig, use_container_width=True, key=key)


def download(df: pd.DataFrame, label: str, filename: str, key: str) -> None:
    st.download_button(label, df.to_csv(index=False).encode("utf-8"), filename, "text/csv", key=key)


def add_ratios(g: pd.DataFrame) -> pd.DataFrame:
    nz = lambda s: s.replace(0, np.nan)  # noqa: E731  (avoid divide-by-zero)
    g["avg_fare"] = g.fare_sum / nz(g.trips)
    g["avg_revenue"] = g.revenue / nz(g.trips)
    g["avg_distance"] = g.distance_sum / nz(g.trips)
    g["avg_duration"] = g.duration_sum / nz(g.trips)
    g["avg_tip"] = g.tip_sum / nz(g.trips)
    g["avg_passengers"] = g.passenger_sum / nz(g.trips)
    g["avg_speed_mph"] = g.distance_sum / nz(g.duration_sum) * 60
    g["fare_per_mile"] = g.fare_sum / nz(g.distance_sum)
    g["tip_pct"] = 100 * g.card_tip_sum / nz(g.card_fare_sum)
    return g


def summarize(df: pd.DataFrame, by: str | list[str]) -> pd.DataFrame:
    return add_ratios(df.groupby(by, as_index=False, observed=True)[SUM_COLS].sum())


# ---------- sidebar: data source ----------
st.sidebar.title("🚕 Taxi Analytics")
SOURCES = {"Mumbai taxi (synthetic, INR/km)": "mumbai", "NYC sample dataset": "sample", "NYC processed full dataset": "full"}
source = st.sidebar.radio("Data source", list(SOURCES))
mode = SOURCES[source]
region = REGIONS[REGION_BY_MODE[mode]]
if region.key == "mumbai":
    USD_TO_INR, DIST_FACTOR = 1.0, 1.0  # data is already in rupees and km
else:
    USD_TO_INR = st.sidebar.number_input("USD → INR rate", min_value=1.0, value=float(os.getenv("USD_TO_INR", 85.0)),
                                         step=0.5, help="NYC data is in US dollars; amounts are converted for display.")
    DIST_FACTOR = KM_PER_MILE

cube = load_table(mode, "trip_cube")
if cube is None or cube.empty:
    cmd = "python scripts/process_data.py --mode " + mode
    st.error(f"No processed data found for **{mode}** mode.")
    prep = {"sample": "python scripts/generate_sample_data.py\n", "mumbai": "python scripts/generate_mumbai_data.py\n",
            "full": "python scripts/download_data.py --year 2024 --months 1 2 3\n"}[mode]
    st.code(prep + cmd, language="bash")
    st.stop()

required = {"pickup_date", "pickup_hour", "trips", "revenue", "payment_name", "vendor_name"}
if not required.issubset(cube.columns):
    st.error(f"Processed data is missing columns: {sorted(required - set(cube.columns))}. Re-run the pipeline.")
    st.stop()

cube["pickup_date"] = pd.to_datetime(cube["pickup_date"])
cube["year"] = cube.pickup_date.dt.year
cube["month"] = cube.pickup_date.dt.to_period("M").astype(str)
cube["weekday"] = pd.Categorical(cube.pickup_date.dt.day_name(), DOW_ORDER, ordered=True)
cube["passenger_sum"] = cube.trips * cube.passenger_count
routes = load_table(mode, "route_metrics")

# ---------- sidebar: filters ----------
st.sidebar.header("Filters")
d_min, d_max = cube.pickup_date.min().date(), cube.pickup_date.max().date()
date_range = st.sidebar.date_input("Date range", (d_min, d_max), min_value=d_min, max_value=d_max)
if len(date_range) != 2:
    st.info("Select both a start and an end date.")
    st.stop()


def multiselect(label: str, options: list) -> list:
    return st.sidebar.multiselect(label, options, default=options)


years = multiselect("Year", sorted(cube.year.unique()))
months = multiselect("Month", sorted(cube.month.unique()))
weekdays = multiselect("Day of week", [d for d in DOW_ORDER if d in set(cube.weekday)])
hours = st.sidebar.slider("Hour of day", 0, 23, (0, 23))
vendors = multiselect("Vendor", sorted(cube.vendor_name.unique()))
payments = multiselect("Payment type", sorted(cube.payment_name.unique()))
dist_cats = multiselect("Trip distance range", [c for c in DIST_ORDER if c in set(cube.trip_distance_category)])
zone_options = sorted(set(routes.pickup_zone) | set(routes.dropoff_zone)) if routes is not None else []
pu_zones = st.sidebar.multiselect("Pickup zone (Locations tab)", zone_options)
do_zones = st.sidebar.multiselect("Dropoff zone (Locations tab)", zone_options)

mask = (
    cube.pickup_date.between(pd.Timestamp(date_range[0]), pd.Timestamp(date_range[1]))
    & cube.year.isin(years) & cube.month.isin(months) & cube.weekday.isin(weekdays)
    & cube.pickup_hour.between(*hours) & cube.vendor_name.isin(vendors)
    & cube.payment_name.isin(payments) & cube.trip_distance_category.isin(dist_cats)
)
f = cube[mask]
if f.empty:
    st.warning("No trips match the current filters. Widen the filters in the sidebar.")
    st.stop()

# ---------- header + KPIs ----------
st.title("Taxi Trip Big Data Analytics & Intelligence Dashboard")
st.caption(f"{region.label}  |  Large-Scale Taxi Trip Analysis Using PySpark, Apache Spark SQL and Streamlit  |  "
           f"{num(len(cube))} pre-aggregated rows from {num(cube.trips.sum())} cleaned trips")

kpi = add_ratios(f[SUM_COLS].sum().to_frame().T)
c = st.columns(6)
c[0].metric("Total trips", num(kpi.trips[0]))
c[1].metric("Total revenue", inr(kpi.revenue[0]))
c[2].metric("Avg fare", inr(kpi.avg_fare[0], 2))
c[3].metric("Avg distance", f"{num(kpi.avg_distance[0], 2)} km")
c[4].metric("Avg duration", f"{kpi.avg_duration[0]:,.1f} min")
c[5].metric("Avg tip", inr(kpi.avg_tip[0], 2))
c = st.columns(4)
c[0].metric("Avg passengers", f"{kpi.avg_passengers[0]:.2f}")
c[1].metric("Avg revenue / trip", inr(kpi.avg_revenue[0], 2))
c[2].metric("Avg fare / km", inr(kpi.fare_per_mile[0], 2))
c[3].metric("Tip % (digital)", f"{kpi.tip_pct[0]:.1f}%", help="Cash tips are not recorded, so tip % uses card/digital payments only.")

tabs = st.tabs(["Executive Overview", "Trip Analytics", "Revenue", "Passengers & Tips",
                "Locations", "Vendor & Payment", "Data Quality & SQL"])

hourly = summarize(f, "pickup_hour")
daily = summarize(f, "pickup_date")
weekday = summarize(f, "weekday")
by_payment = summarize(f, "payment_name")

# ---------- 1. overview ----------
with tabs[0]:
    a, b = st.columns(2)
    with a:
        fig_show(px.line(daily, x="pickup_date", y="revenue", title="Daily revenue",
                         color_discrete_sequence=[COLOR]), "ov_rev")
    with b:
        fig_show(px.bar(daily, x="pickup_date", y="trips", title="Daily trip volume",
                        color_discrete_sequence=[COLOR]), "ov_trips")
    a, b = st.columns(2)
    with a:
        fig_show(px.bar(hourly, x="pickup_hour", y="trips", title="Trips by hour",
                        color_discrete_sequence=[COLOR]), "ov_hour")
    with b:
        fig_show(px.pie(by_payment, names="payment_name", values="trips", hole=0.45,
                        title="Payment distribution"), "ov_pay")
    with st.expander("Peak hours analysis", expanded=True):
        top5 = hourly.nlargest(5, "trips")
        m = st.columns(4)
        m[0].metric("Busiest hour", f"{int(hourly.loc[hourly.trips.idxmax(), 'pickup_hour']):02d}:00")
        m[1].metric("Highest revenue hour", f"{int(hourly.loc[hourly.revenue.idxmax(), 'pickup_hour']):02d}:00")
        m[2].metric("Quietest hour", f"{int(hourly.loc[hourly.trips.idxmin(), 'pickup_hour']):02d}:00")
        m[3].metric("Trips in top-5 hours", f"{100 * top5.trips.sum() / hourly.trips.sum():.1f}%")
        fig_show(px.bar(weekday, x="weekday", y="trips", title="Trips by weekday",
                        color_discrete_sequence=[COLOR]), "ov_wd")
        st.caption("Weekend = Saturday + Sunday. Compare the weekday bars: busiest day is "
                   f"**{weekday.loc[weekday.trips.idxmax(), 'weekday']}**.")
    download(daily, "Download daily metrics", "daily_metrics.csv", "dl_daily")

# ---------- 2. trips ----------
with tabs[1]:
    dist_hist, dur_hist = load_table(mode, "distance_hist"), load_table(mode, "duration_hist")
    a, b = st.columns(2)
    if dist_hist is not None:
        with a:
            fig_show(px.bar(dist_hist, x="km", y="trips", title=f"Trip distance distribution (km; last bar = {region.hist_cap * DIST_FACTOR:.0f}+ km)",
                            color_discrete_sequence=[COLOR]), "tr_dist")
    if dur_hist is not None:
        with b:
            fig_show(px.bar(dur_hist, x="minutes", y="trips", title="Trip duration distribution (min, 5-min bins)",
                            color_discrete_sequence=[COLOR]), "tr_dur")
    st.caption("Distribution charts cover all cleaned trips (they are not affected by the sidebar filters).")
    a, b = st.columns(2)
    cats = summarize(f, "trip_distance_category")
    cats["trip_distance_category"] = pd.Categorical(cats.trip_distance_category, DIST_ORDER, ordered=True)
    cats = cats.sort_values("trip_distance_category")
    with a:
        fig_show(px.bar(cats, x="trip_distance_category", y="trips", title="Trips by distance category",
                        color_discrete_sequence=[COLOR]), "tr_cat")
    with b:
        fig_show(px.line(hourly, x="pickup_hour", y="avg_speed_mph", markers=True, title="Average speed by hour (km/h)",
                         color_discrete_sequence=[COLOR]), "tr_speed")
    scatter = load_table(mode, "scatter_sample")
    if scatter is not None:
        fig_show(px.scatter(scatter, x="trip_distance", y="total_amount", opacity=0.4, color="payment_name",
                            title="Trip distance vs total fare (random sample of 5,000 trips)"), "tr_scatter")
    st.dataframe(cats[["trip_distance_category", "trips", "revenue", "avg_fare", "avg_duration", "avg_speed_mph"]]
                 .round(2), use_container_width=True, hide_index=True)

# ---------- 3. revenue ----------
with tabs[2]:
    grain = st.radio("Trend granularity", ["Daily", "Monthly"], horizontal=True)
    trend = daily if grain == "Daily" else summarize(f, "month")
    fig_show(px.line(trend, x="pickup_date" if grain == "Daily" else "month", y="revenue",
                     markers=True, title=f"{grain} revenue", color_discrete_sequence=[COLOR]), "rv_trend")
    a, b = st.columns(2)
    with a:
        fig_show(px.bar(hourly, x="pickup_hour", y="revenue", title="Revenue by hour",
                        color_discrete_sequence=[COLOR]), "rv_hour")
    with b:
        fig_show(px.bar(weekday, x="weekday", y="revenue", title="Revenue by weekday",
                        color_discrete_sequence=[COLOR]), "rv_wd")
    a, b = st.columns(2)
    with a:
        fig_show(px.bar(by_payment, x="payment_name", y="revenue", title="Revenue by payment type",
                        color_discrete_sequence=[COLOR]), "rv_pay")
    with b:
        fig_show(px.line(hourly, x="pickup_hour", y="avg_fare", markers=True, title="Average fare by hour (₹)",
                         color_discrete_sequence=[COLOR]), "rv_fare")
    fig_show(px.bar(cats, x="trip_distance_category", y="avg_fare", title="Average fare by distance category (₹)",
                    color_discrete_sequence=[COLOR]), "rv_dist")

# ---------- 4. passengers & tips ----------
with tabs[3]:
    pax = summarize(f, "passenger_count")
    a, b = st.columns(2)
    with a:
        fig_show(px.bar(pax, x="passenger_count", y="trips", title="Trips by passenger count",
                        color_discrete_sequence=[COLOR]), "px_trips")
    with b:
        fig_show(px.bar(pax, x="passenger_count", y="avg_fare", title="Average fare by passenger count (₹)",
                        color_discrete_sequence=[COLOR]), "px_fare")
    a, b = st.columns(2)
    with a:
        fig_show(px.bar(pax, x="passenger_count", y="avg_distance", title="Average distance by passenger count (km)",
                        color_discrete_sequence=[COLOR]), "px_dist")
    with b:
        fig_show(px.bar(pax, x="passenger_count", y="tip_pct", title="Tip % (digital payments) by passenger count",
                        color_discrete_sequence=[COLOR]), "px_tip")
    st.subheader("Tip behaviour")
    a, b = st.columns(2)
    with a:
        fig_show(px.line(hourly, x="pickup_hour", y="tip_pct", markers=True, title="Average tip % (digital payments) by hour",
                         color_discrete_sequence=[COLOR]), "tp_hour")
    with b:
        if scatter is not None:
            card = scatter[scatter.payment_name.isin(["Credit card", "Digital (UPI/Card)"])]
            fig_show(px.scatter(card, x="fare_amount", y="tip_amount", opacity=0.4,
                                title="Tip amount vs fare amount (digital payments, sample)",
                                color_discrete_sequence=[COLOR]), "tp_scatter")
    st.caption("Passenger count is driver-entered; more passengers does not automatically mean higher revenue - read the bars.")

# ---------- 5. locations ----------
with tabs[4]:
    loc = load_table(mode, "location_metrics")
    top_n = st.slider("Show top N", 5, 25, 10)
    if loc is not None:
        a, b = st.columns(2)
        for col, role in ((a, "pickup"), (b, "dropoff")):
            part = loc[loc.role == role].nlargest(top_n, "trips").sort_values("trips")
            with col:
                fig_show(px.bar(part, x="trips", y="zone", orientation="h", title=f"Top {role} zones",
                                color_discrete_sequence=[COLOR]), f"lc_{role}")
        rev = loc[loc.role == "pickup"].nlargest(top_n, "revenue").sort_values("revenue")
        fig_show(px.bar(rev, x="revenue", y="zone", orientation="h", title="Revenue by pickup zone",
                        color_discrete_sequence=[COLOR]), "lc_rev")
    if routes is not None:
        r = routes
        if pu_zones:
            r = r[r.pickup_zone.isin(pu_zones)]
        if do_zones:
            r = r[r.dropoff_zone.isin(do_zones)]
        if r.empty:
            st.warning("No routes match the selected zones.")
        else:
            cols = ["route", "trips", "revenue", "avg_fare", "avg_distance"]
            a, b = st.columns(2)
            a.subheader("Most popular routes")
            a.dataframe(r.nlargest(top_n, "trips")[cols], hide_index=True, use_container_width=True)
            b.subheader("Highest revenue routes")
            b.dataframe(r.nlargest(top_n, "revenue")[cols], hide_index=True, use_container_width=True)
            min_trips = st.slider("Minimum trips for longest/shortest routes", 1, 100, 10)
            reliable = r[r.trips >= min_trips]
            a, b = st.columns(2)
            a.subheader("Longest routes (avg distance)")
            a.dataframe(reliable.nlargest(top_n, "avg_distance")[cols], hide_index=True, use_container_width=True)
            b.subheader("Shortest routes (avg distance)")
            b.dataframe(reliable.nsmallest(top_n, "avg_distance")[cols], hide_index=True, use_container_width=True)
            download(r[cols], "Download route metrics", "route_metrics.csv", "dl_routes")
    st.caption("Location tables are all-time aggregates; only the two zone filters apply here.")

# ---------- 6. vendor & payment ----------
with tabs[5]:
    vend = summarize(f, "vendor_name")
    a, b = st.columns(2)
    with a:
        fig_show(px.bar(vend, x="vendor_name", y="trips", title="Vendor trip count",
                        color_discrete_sequence=[COLOR]), "vp_trips")
    with b:
        fig_show(px.bar(vend, x="vendor_name", y="revenue", title="Vendor revenue",
                        color_discrete_sequence=[COLOR]), "vp_rev")
    show = ["vendor_name", "trips", "revenue", "avg_fare", "avg_distance", "avg_duration", "avg_tip"]
    st.dataframe(vend[show].round(2), hide_index=True, use_container_width=True)
    st.caption("Metrics are shown side by side; no vendor is ranked 'best' because that depends on the metric chosen.")
    st.subheader("Payment methods")
    show_p = ["payment_name", "trips", "revenue", "avg_fare", "avg_tip", "tip_pct"]
    st.dataframe(by_payment[show_p].round(2), hide_index=True, use_container_width=True)
    trend = summarize(f, ["month", "payment_name"])
    fig_show(px.bar(trend, x="month", y="trips", color="payment_name", barmode="stack",
                    title="Payment method trend (trips per month)"), "vp_trend")
    vm = load_table(mode, "vendor_monthly")
    if vm is not None:
        with st.expander("Monthly vendor revenue ranking (Spark window function)"):
            st.dataframe(vm, hide_index=True, use_container_width=True)
    a, b, c3 = st.columns(3)
    with a:
        download(vend[show], "Download vendor metrics", "vendor_metrics.csv", "dl_vendor")
    with b:
        download(by_payment[show_p], "Download payment metrics", "payment_metrics.csv", "dl_pay")
    with c3:
        download(f.drop(columns=["passenger_sum"]), "Download filtered data", "filtered_trip_cube.csv", "dl_filtered")

# ---------- 7. data quality & SQL ----------
with tabs[6]:
    dq = load_table(mode, "dq_report")
    if dq is not None:
        st.subheader("Data quality report (raw data)")
        total = int(dq.loc[dq.metric == "total_rows", "value"].iloc[0])
        dq = dq.assign(pct_of_rows=(100 * dq.value / total).round(2))
        st.dataframe(dq, hide_index=True, use_container_width=True)
    summ, sample = load_table(mode, "anomaly_summary"), load_table(mode, "anomaly_sample")
    if summ is not None:
        st.subheader("Anomaly analysis (z-score rule on cleaned data)")
        st.dataframe(summ.round(2), hide_index=True, use_container_width=True)
    if sample is not None:
        with st.expander("Most extreme trips"):
            st.dataframe(sample, hide_index=True, use_container_width=True)
    sql_dir = processed_dir(mode) / "sql_results"
    if sql_dir.exists():
        st.subheader("Spark SQL query results")
        for q in sorted(p.name for p in sql_dir.iterdir() if p.is_dir()):
            with st.expander(q):
                st.dataframe(load_table(mode, f"sql_results/{q}"), hide_index=True, use_container_width=True)