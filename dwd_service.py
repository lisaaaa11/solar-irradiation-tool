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


def classify_deviation(percentage):
    abs_percentage = abs(percentage)

    if abs_percentage < 5:
        return "normal"
    if abs_percentage < 10:
        return "merklich"
    if abs_percentage < 20:
        return "deutlich"
    return "stark"


def get_monthly_deviation_data(region, analysis_year, reference_start, reference_end):
    validate_inputs(region, analysis_year, reference_start, reference_end)

    analysis = df[df["year"] == analysis_year][["month", region]].sort_values("month") #Daten für Analysejahr filtern, nur Monat und Region behalten, nach Monat sortieren
    if analysis.empty:
        raise ValueError("Keine Daten für das gewählte Analysejahr gefunden.")

    reference_period = (
        df[(df["year"] >= reference_start) & (df["year"] <= reference_end)] #Jahre im Referenzzeitraum
        .groupby("month", as_index=False)[region] #Nach Monat gruppieren, Nur Werte der Region behalten
        .mean()
        .sort_values("month")
    )
    if reference_period.empty:
        raise ValueError("Keine Daten für den gewählten Referenzzeitraum gefunden.")

    merged = analysis.merge(reference_period, on="month", suffixes=("_actual", "_reference")) #Analyse- und Referenzwerte anhand des Monats zusammenführen (Suffixe weil gleiche Spaltennamen)
    merged["deviation"] = merged[f"{region}_actual"] - merged[f"{region}_reference"]
    merged["deviation_percentage"] = merged.apply(
        lambda row: (row["deviation"] / row[f"{region}_reference"] * 100) #Abweichung total / Referenzwert * 100
        if row[f"{region}_reference"] != 0 else 0, #Division durch 0 vermeiden
        axis=1
    )

    return {
        "unit": "kWh/m²",
        "months": MONTH_LABELS,
        "region": region,
        "analysis_year": analysis_year,
        "reference_period": f"{reference_start}-{reference_end}",
        "actual_values": merged[f"{region}_actual"].round(2).tolist(),
        "reference_values": merged[f"{region}_reference"].round(2).tolist(),
        "deviation_values": merged["deviation"].round(2).tolist(),
        "deviation_percentage_values": merged["deviation_percentage"].round(1).tolist(),
        "deviation_levels": [classify_deviation(v) for v in merged["deviation_percentage"]], #Reihenfolge entspricht Monaten
    }