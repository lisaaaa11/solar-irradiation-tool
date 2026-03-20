import pandas as pd

MONTH_LABELS = ["Jan", "Feb", "Mär", "Apr", "Mai", "Jun", "Jul", "Aug", "Sep", "Okt", "Nov", "Dez"]
VALID_REGIONS = {"germany", "north", "south"}

df = pd.read_csv("data/monthly_means.csv")


def validate_inputs(region, analysis_year, reference_start, reference_end):
    if region not in VALID_REGIONS:
        raise ValueError("Ungültiger Standort. Erlaubt sind: germany, north, south.")

    if reference_start > reference_end:
        raise ValueError("Der Referenzzeitraum ist ungültig.")

    if reference_start <= analysis_year <= reference_end:
        raise ValueError("Das Analysejahr sollte nicht im Referenzzeitraum liegen.")


def get_monthly_anomaly_data(region, analysis_year, reference_start, reference_end):
    validate_inputs(region, analysis_year, reference_start, reference_end)

    analysis = df[df["year"] == analysis_year][["month", region]].sort_values("month")
    if analysis.empty:
        raise ValueError("Keine Daten für das gewählte Analysejahr gefunden.")

    baseline = (
        df[(df["year"] >= reference_start) & (df["year"] <= reference_end)]
        .groupby("month", as_index=False)[region]
        .mean()
        .sort_values("month")
    )
    if baseline.empty:
        raise ValueError("Keine Daten für den gewählten Referenzzeitraum gefunden.")

    merged = analysis.merge(baseline, on="month", suffixes=("_actual", "_baseline"))
    merged["anomaly"] = merged[f"{region}_actual"] - merged[f"{region}_baseline"]

    return {
        "unit": "kWh/m²",
        "months": MONTH_LABELS,
        "region": region,
        "analysis_year": analysis_year,
        "reference_period": f"{reference_start}-{reference_end}",
        "actual_values": merged[f"{region}_actual"].round(2).tolist(),
        "baseline_values": merged[f"{region}_baseline"].round(2).tolist(),
        "anomaly_values": merged["anomaly"].round(2).tolist(),
    }


def get_anomaly_heatmap_data(region, analysis_year, reference_start, reference_end):
    validate_inputs(region, analysis_year, reference_start, reference_end)

    analysis = df[df["year"] == analysis_year][["month", region]].sort_values("month")
    if analysis.empty:
        raise ValueError("Keine Daten für das gewählte Analysejahr gefunden.")

    baseline = (
        df[(df["year"] >= reference_start) & (df["year"] <= reference_end)]
        .groupby("month", as_index=False)[region]
        .mean()
        .sort_values("month")
    )
    if baseline.empty:
        raise ValueError("Keine Daten für den gewählten Referenzzeitraum gefunden.")

    merged = analysis.merge(baseline, on="month", suffixes=("_actual", "_baseline"))
    merged["anomaly"] = merged[f"{region}_actual"] - merged[f"{region}_baseline"]

    return {
        "unit": "kWh/m²",
        "months": MONTH_LABELS,
        "year": analysis_year,
        "region": region,
        "reference_period": f"{reference_start}-{reference_end}",
        "values": [round(value, 2) for value in merged["anomaly"].tolist()],
    }