import pandas as pd

MONTH_LABELS = ["Jan", "Feb", "Mär", "Apr", "Mai", "Jun", "Jul", "Aug", "Sep", "Okt", "Nov", "Dez"]

df = pd.read_csv("data/monthly_means.csv")


def get_monthly_overlay_data(start_year, end_year):
    filtered = df[(df["year"] >= start_year) & (df["year"] <= end_year)]

    if filtered.empty:
        raise ValueError("Keine Daten für den gewählten Zeitraum gefunden.")

    monthly = (
        filtered.groupby("month", as_index=False)[["germany", "north", "south"]]
        .mean()
        .sort_values("month")
    )

    yearly_avg = filtered.groupby("year")["germany"].sum().mean() / 12.0

    return {
        "unit": "kWh/m²",
        "months": MONTH_LABELS,
        "germany_values": monthly["germany"].round(2).tolist(),
        "north_values": monthly["north"].round(2).tolist(),
        "south_values": monthly["south"].round(2).tolist(),
        "yearly_average_line": [round(yearly_avg, 2)] * 12,
    }


def get_seasonal_heatmap_data(start_year, end_year):
    filtered = df[(df["year"] >= start_year) & (df["year"] <= end_year)]

    if filtered.empty:
        raise ValueError("Keine Daten für den gewählten Zeitraum gefunden.")

    pivot = (
        filtered.pivot(index="year", columns="month", values="germany")
        .sort_index()
        .reindex(columns=range(1, 13))
    )

    values = []
    for year in pivot.index:
        row = []
        for month in pivot.columns:
            value = pivot.loc[year, month]
            row.append(None if pd.isna(value) else round(float(value), 2))
        values.append(row)

    return {
        "unit": "kWh/m²",
        "years": [int(year) for year in pivot.index.tolist()],
        "months": MONTH_LABELS,
        "values": values,
    }