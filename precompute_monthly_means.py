import io
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import requests

BASE_URL = "https://opendata.dwd.de/climate_environment/CDC/grids_germany/monthly/radiation_global/"
OUTPUT_FILE = Path("data/monthly_means.csv")
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)


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
    response = requests.get(month_url(year, month), headers={"User-Agent": "Mozilla/5.0"})
    if response.status_code != 200:
        return None

    grid = parse_asc_from_zip(response.content)

    mid = grid.shape[0] // 2
    north = grid[:mid, :]
    south = grid[mid:, :]

    return {
        "year": year,
        "month": month,
        "germany": float(np.nanmean(grid)),
        "north": float(np.nanmean(north)),
        "south": float(np.nanmean(south)),
    }


def main():
    rows = []

    for year in range(1991, 2025):
        for month in range(1, 13):
            print(f"Lade {year}-{month:02d} ...")
            result = load_month(year, month)
            if result is not None:
                rows.append(result)

    if not rows:
        raise ValueError("Keine Daten gefunden.")

    df = pd.DataFrame(rows)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"Gespeichert: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()