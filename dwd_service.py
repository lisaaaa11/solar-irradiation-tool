import io
import json
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import requests

BASE_URL = "https://opendata.dwd.de/climate_environment/CDC/grids_germany/monthly/radiation_global/"
CACHE_FOLDER = Path("data/cache")
CACHE_FOLDER.mkdir(parents=True, exist_ok=True)

MONTH_LABELS = ["Jan", "Feb", "Mär", "Apr", "Mai", "Jun", "Jul", "Aug", "Sep", "Okt", "Nov", "Dez"]


def month_url(year, month):
    filename = f"grids_germany_monthly_radiation_global_{year}{month:02d}.zip"
    return BASE_URL + filename


def parse_asc_from_zip(content: bytes):
    archive = zipfile.ZipFile(io.BytesIO(content))
    asc_name = next(name for name in archive.namelist() if name.endswith(".asc"))

    with archive.open(asc_name) as file:
        lines = file.readlines()

    ncols = nrows = None
    nodata_value = -999.0
    data_start = None

    for i, raw_line in enumerate(lines):
        text = raw_line.decode("latin-1").strip()

        if text.startswith("NCOLS"):
            ncols = int(text.split()[1])
        elif text.startswith("NROWS"):
            nrows = int(text.split()[1])
        elif text.startswith("NODATA_VALUE"):
            nodata_value = float(text.split()[1])
            data_start = i + 1
            break

    if data_start is None or ncols is None or nrows is None:
        raise ValueError("ASC-Datei konnte nicht gelesen werden.")

    values = []
    for raw_line in lines[data_start:]:
        values.extend(raw_line.decode("latin-1").split())

    grid = np.array(pd.to_numeric(values, errors="coerce"), dtype=float).reshape((nrows, ncols))
    grid[grid == nodata_value] = np.nan
    return grid


def load_month(year, month):
    cache_file = CACHE_FOLDER / f"{year}{month:02d}.json"

    if cache_file.exists():
        with open(cache_file, encoding="utf-8") as file:
            return json.load(file)

    response = requests.get(month_url(year, month), headers={"User-Agent": "Mozilla/5.0"})
    if response.status_code != 200:
        raise ValueError(f"Datei nicht verfügbar: {year}-{month:02d}")

    grid = parse_asc_from_zip(response.content)

    mid = grid.shape[0] // 2
    north = grid[:mid, :]
    south = grid[mid:, :]

    result = {
        "year": year,
        "month": month,
        "avg_radiation": float(np.nanmean(grid)),
        "north_avg": float(np.nanmean(north)),
        "south_avg": float(np.nanmean(south)),
    }

    with open(cache_file, "w", encoding="utf-8") as file:
        json.dump(result, file)

    return result


def build_monthly_mean_table(start_year, end_year):
    rows = []

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            try:
                month_data = load_month(year, month)
                rows.append(
                    {
                        "year": year,
                        "month": month,
                        "value": month_data["avg_radiation"],
                        "north": month_data["north_avg"],
                        "south": month_data["south_avg"],
                    }
                )
            except Exception:
                continue

    if not rows:
        raise ValueError("Keine Daten gefunden.")

    return pd.DataFrame(rows)


def get_monthly_overlay_data(start_year, end_year):
    df = build_monthly_mean_table(start_year, end_year)

    monthly_mean = df.groupby("month", as_index=False)["value"].mean().sort_values("month")
    north_mean = df.groupby("month", as_index=False)["north"].mean().sort_values("month")
    south_mean = df.groupby("month", as_index=False)["south"].mean().sort_values("month")

    yearly_avg = float(df.groupby("year")["value"].sum().mean() / 12.0)

    return {
        "unit": "kWh/m²",
        "months": MONTH_LABELS,
        "germany_values": [round(v, 2) for v in monthly_mean["value"].tolist()],
        "north_values": [round(v, 2) for v in north_mean["north"].tolist()],
        "south_values": [round(v, 2) for v in south_mean["south"].tolist()],
        "yearly_average_line": [round(yearly_avg, 2)] * 12,
    }


def get_seasonal_heatmap_data(start_year, end_year):
    df = build_monthly_mean_table(start_year, end_year)

    pivot = (
        df.pivot(index="year", columns="month", values="value")
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