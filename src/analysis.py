"""Stage 6 - Exploratory data analysis and visualisation (reads from SQLite).

Research questions
  RQ1  How do temperature, humidity and wind differ between the Kazakhstan cities?
  RQ2  How are temperature and relative humidity related?
  RQ3  Which weather conditions dominate, and how does temperature vary through the day?
"""
from __future__ import annotations

import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from src import config
from src.cleaner import PART_ORDER
from src.loader import connect
from src.utils import get_logger, update_metrics

log = get_logger("analysis")
sns.set_theme(style="whitegrid", context="notebook")

QUERY = """
SELECT o.*, c.country, c.latitude, c.longitude
FROM weather_observations o JOIN cities c ON c.city = o.city
ORDER BY o.city, o.observation_time
"""
NUMERIC = ["temperature_c", "humidity_pct", "wind_speed_kmh", "dew_point_c", "wind_chill_c"]


def save(fig, name: str) -> str:
    config.FIGURES_DIR.mkdir(exist_ok=True)
    path = config.FIGURES_DIR / name
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    log.info("figure saved: figures/%s", name)
    return name


def make_figures(df: pd.DataFrame) -> list[str]:
    figures = []

    fig, ax = plt.subplots(figsize=(11, 5))
    sns.lineplot(data=df, x="time", y="temperature_c", hue="city", linewidth=1.3, ax=ax)
    ax.set(title="Hourly air temperature by city", xlabel="Time", ylabel="Temperature (°C)")
    ax.legend(title="City", ncol=2, fontsize=8)
    fig.autofmt_xdate()
    figures.append(save(fig, "fig01_temperature_timeseries.png"))

    fig, ax = plt.subplots(figsize=(9, 5))
    order = df.groupby("city")["humidity_pct"].median().sort_values().index
    sns.boxplot(data=df, x="city", y="humidity_pct", order=order, hue="city", legend=False, ax=ax)
    ax.set(title="Relative humidity distribution by city", xlabel="City", ylabel="Relative humidity (%)")
    plt.setp(ax.get_xticklabels(), rotation=30, ha="right")
    figures.append(save(fig, "fig02_humidity_boxplot_by_city.png"))

    fig, ax = plt.subplots(figsize=(8, 5))
    counts = df["weather_category"].value_counts()
    sns.barplot(x=counts.index, y=counts.values, hue=counts.index, legend=False, ax=ax)
    for i, v in enumerate(counts.values):
        ax.text(i, v, str(v), ha="center", va="bottom", fontsize=9)
    ax.set(title="Number of hours per weather category", xlabel="Weather category (from WMO code)",
           ylabel="Hours")
    figures.append(save(fig, "fig03_weather_category_counts.png"))

    fig, ax = plt.subplots(figsize=(8, 5.5))
    sns.scatterplot(data=df, x="temperature_c", y="humidity_pct", hue="city", s=18, alpha=0.7, ax=ax)
    slope, intercept = np.polyfit(df["temperature_c"], df["humidity_pct"], 1)
    xs = np.linspace(df["temperature_c"].min(), df["temperature_c"].max(), 50)
    ax.plot(xs, slope * xs + intercept, color="black", linestyle="--", label=f"trend (slope {slope:.2f} %/°C)")
    ax.set(title="Temperature vs relative humidity", xlabel="Temperature (°C)", ylabel="Relative humidity (%)")
    ax.legend(fontsize=8, ncol=2)
    figures.append(save(fig, "fig04_temperature_vs_humidity.png"))

    fig, ax = plt.subplots(figsize=(7, 5.5))
    sns.heatmap(df[NUMERIC].corr(), annot=True, fmt=".2f", cmap="coolwarm", vmin=-1, vmax=1, ax=ax)
    ax.set_title("Correlation matrix of numerical variables")
    figures.append(save(fig, "fig05_correlation_heatmap.png"))

    pivot = pd.pivot_table(df, index="city", columns="part_of_day", values="temperature_c",
                           aggfunc="mean").reindex(columns=PART_ORDER)
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.heatmap(pivot, annot=True, fmt=".1f", cmap="YlOrRd", cbar_kws={"label": "Mean temperature (°C)"}, ax=ax)
    ax.set(title="Mean temperature by city and part of day", xlabel="Part of day", ylabel="City")
    figures.append(save(fig, "fig06_city_part_of_day_heatmap.png"))
    return figures


def findings(df: pd.DataFrame) -> list[str]:
    """Findings are computed from the data - never typed by hand."""
    by_city = df.groupby("city").agg(t=("temperature_c", "mean"), h=("humidity_pct", "mean"),
                                     w=("wind_speed_kmh", "mean"), wmax=("wind_speed_kmh", "max"))
    warm, cold = by_city["t"].idxmax(), by_city["t"].idxmin()
    corr = df["temperature_c"].corr(df["humidity_pct"])
    cat = df["weather_category"].value_counts(normalize=True)
    part = df.groupby("part_of_day")["temperature_c"].mean().reindex(PART_ORDER)
    strength = "strong" if abs(corr) >= 0.6 else "moderate" if abs(corr) >= 0.3 else "weak"
    direction = "negative" if corr < 0 else "positive"
    return [
        f"RQ1 - The warmest city over the period is {warm} ({by_city.loc[warm, 't']:.1f} °C mean) and the coldest is "
        f"{cold} ({by_city.loc[cold, 't']:.1f} °C); the spread of city means is {by_city['t'].max() - by_city['t'].min():.1f} °C.",
        f"RQ1 - The most humid city is {by_city['h'].idxmax()} ({by_city['h'].max():.0f} % mean) and the windiest is "
        f"{by_city['w'].idxmax()} (mean {by_city['w'].max():.1f} km/h, peak {by_city['wmax'].max():.0f} km/h).",
        f"RQ2 - Temperature and humidity show a {strength} {direction} correlation (Pearson r = {corr:.2f}).",
        f"RQ3 - The most frequent weather category is '{cat.index[0]}' ({cat.iloc[0] * 100:.0f} % of hours), "
        f"followed by '{cat.index[1] if len(cat) > 1 else '-'}'.",
        f"RQ3 - Over the whole sample the warmest part of the day is {part.idxmax()} ({part.max():.1f} °C) "
        f"and the coldest is {part.idxmin()} ({part.min():.1f} °C).",
    ]


def main() -> int:
    if not config.DB_PATH.exists():
        log.error("data/output.db not found - run the loader first")
        return 1
    conn = connect()
    try:
        df = pd.read_sql_query(QUERY, conn)
    finally:
        conn.close()
    df["time"] = pd.to_datetime(df["observation_time"])

    log.info("Dataset shape: %s", df.shape)
    info = {
        "shape": list(df.shape),
        "dtypes": {c: str(t) for c, t in df.dtypes.items()},
        "describe": df[NUMERIC].describe().round(2).to_dict(),
        "categorical_counts": {c: df[c].value_counts().to_dict()
                               for c in ["city", "weather_category", "part_of_day", "comfort_level"]},
        "correlations": df[NUMERIC].corr().round(3).to_dict(),
        "grouped_mean_by_city": df.groupby("city")[NUMERIC].mean().round(2).to_dict("index"),
        "grouped_mean_by_category": df.groupby("weather_category")[NUMERIC].mean().round(2).to_dict("index"),
    }
    print(df[NUMERIC].describe().round(2).to_string())

    figures = make_figures(df)
    notes = findings(df)
    for line in notes:
        log.info("FINDING: %s", line)
    config.FINDINGS_PATH.write_text("# EDA findings (computed from data/output.db)\n\n"
                                    + "\n".join(f"- {n}" for n in notes) + "\n", encoding="utf-8")
    update_metrics("eda", {**info, "figures": figures, "findings": notes})
    return 0


if __name__ == "__main__":
    sys.exit(main())
