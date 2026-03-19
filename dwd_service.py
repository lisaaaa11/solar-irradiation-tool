import io
import json
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://opendata.dwd.de/climate_environment/CDC/grids_germany/monthly/radiation_global/"

DATA_FOLDER = Path("data")
CACHE_FOLDER = DATA_FOLDER / "cache"
CATALOG_FILE = DATA_FOLDER / "catalog.json"

DATA_FOLDER.mkdir(exist_ok=True)
CACHE_FOLDER.mkdir(exist_ok=True)


def build_catalog():
    print("Hole verfügbare Dateien vom DWD...")

    response = requests.get(BASE_URL, headers={"User-Agent": "Mozilla/5.0"})
    response.raise_for_status()

    files = []
    soup = BeautifulSoup(response.text, "html.parser")
    for link in soup.find_all("a"):
        href = link.get("href")
        if not href or not href.endswith(".zip"):
            continue

        yyyymm = href.split("_")[-1].replace(".zip", "")
        files.append(
            {
                "file": href,
                "year": int(yyyymm[:4]),
                "month": int(yyyymm[4:6]),
                "url": BASE_URL + href,
            }
        )

    with open(CATALOG_FILE, "w", encoding="utf-8") as file:
        json.dump(files, file, indent=2)

    return files


def load_catalog():
    if not CATALOG_FILE.exists():
        return build_catalog()

    with open(CATALOG_FILE, encoding="utf-8") as file:
        return json.load(file)


def find_entry(year, month):
    return next(
        (
            entry
            for entry in load_catalog()
            if entry["year"] == year and entry["month"] == month
        ),
        None,
    )


def load_month(year, month):
    cache_file = CACHE_FOLDER / f"{year}{month:02d}.json"

    if cache_file.exists():
        with open(cache_file, encoding="utf-8") as file:
            cached_data = json.load(file)
        if "grid" in cached_data:
            return cached_data

    entry = find_entry(year, month)
    if entry is None:
        raise ValueError(f"Monat nicht gefunden: {year}-{month:02d}")

    print(f"Lade {year}-{month:02d} vom DWD...")
    response = requests.get(entry["url"], headers={"User-Agent": "Mozilla/5.0"})
    response.raise_for_status()

    archive = zipfile.ZipFile(io.BytesIO(response.content))
    asc_file = next(name for name in archive.namelist() if name.endswith(".asc"))

    with archive.open(asc_file) as file:
        lines = file.readlines()

    metadata = {}
    for line in lines[:40]:
        text = line.decode("latin-1")
        if "=" in text:
            key, value = text.split("=", 1)
            metadata[key.strip()] = value.strip()

    ncols = nrows = None
    xllcorner = yllcorner = cellsize = None
    nodata_value = -999.0
    data_start = 0

    for index, line in enumerate(lines):
        text = line.decode("latin-1").strip()

        if text.startswith("NCOLS"):
            ncols = int(text.split()[1])
        elif text.startswith("NROWS"):
            nrows = int(text.split()[1])
        elif text.startswith("XLLCORNER"):
            xllcorner = float(text.split()[1])
        elif text.startswith("YLLCORNER"):
            yllcorner = float(text.split()[1])
        elif text.startswith("CELLSIZE"):
            cellsize = float(text.split()[1])
        elif text.startswith("NODATA_VALUE"):
            nodata_value = float(text.split()[1])
            data_start = index + 1
            break

    numbers = [
        value
        for line in lines[data_start:]
        for value in line.decode("latin-1").split()
    ]

    values = pd.to_numeric(numbers, errors="coerce")
    grid = values.reshape((nrows, ncols))
    grid[grid == nodata_value] = np.nan

    result = {
        "year": metadata.get("Jahr"),
        "month": metadata.get("Monat"),
        "avg_radiation": float(grid[~np.isnan(grid)].mean()),
        "unit": metadata.get("Werte_Dimension"),
        "ncols": ncols,
        "nrows": nrows,
        "xllcorner": xllcorner,
        "yllcorner": yllcorner,
        "cellsize": cellsize,
        "grid": grid.tolist(),
    }

    with open(cache_file, "w", encoding="utf-8") as file:
        json.dump(result, file)

    return result


def load_year_grid(year):
    month_grids = []
    metadata = None

    for month in range(1, 13):
        try:
            month_data = load_month(year, month)
            month_grids.append(np.array(month_data["grid"], dtype=float))

            if metadata is None:
                metadata = {
                    "ncols": month_data["ncols"],
                    "nrows": month_data["nrows"],
                    "xllcorner": month_data["xllcorner"],
                    "yllcorner": month_data["yllcorner"],
                    "cellsize": month_data["cellsize"],
                }
        except Exception:
            continue

    if not month_grids or metadata is None:
        return None

    return {
        "grid": np.nansum(month_grids, axis=0),
        "meta": metadata,
    }

def load_block_grid(start_year, end_year):
    yearly_grids = []
    metadata = None

    for year in range(start_year, end_year + 1):
        year_data = load_year_grid(year)
        if year_data is None:
            continue

        yearly_grids.append(year_data["grid"])

        if metadata is None:
            metadata = year_data["meta"]

    if not yearly_grids or metadata is None:
        raise ValueError(f"Keine Daten für Block {start_year}-{end_year} gefunden.")

    mean_grid = np.nanmean(yearly_grids, axis=0)

    return {
        "start_year": start_year,
        "end_year": end_year,
        "unit": "kWh/m2",
        "ncols": metadata["ncols"],
        "nrows": metadata["nrows"],
        "xllcorner": metadata["xllcorner"],
        "yllcorner": metadata["yllcorner"],
        "cellsize": metadata["cellsize"],
        "grid": mean_grid.tolist(),
    }


def load_cell_block_timeseries(start_year, end_year, x, y):
    years = []
    values = []
    global_values = []  # Neu: Liste für den globalen Durchschnitt

    for year in range(start_year, end_year + 1):
        year_data = load_year_grid(year)
        if year_data is None:
            continue

        year_grid = year_data["grid"]

        if y < 0 or y >= year_grid.shape[0] or x < 0 or x >= year_grid.shape[1]:
            raise ValueError(f"Ungültige Zelle: x={x}, y={y}")

        # Wert der spezifischen Zelle
        cell_value = year_grid[y, x]
        
        # NEU: Globalen Mittelwert für dieses Jahr berechnen
        # Wir nutzen np.nanmean, um NODATA-Werte zu ignorieren
        avg_value = load_year_global_avg_from_cache(year)

        years.append(year)
        values.append(None if np.isnan(cell_value) else float(cell_value))
        global_values.append(None if np.isnan(avg_value) else float(avg_value))

    if not years:
        raise ValueError(f"Keine Zeitreihe für Block {start_year}-{end_year} gefunden.")

    return {
        "start_year": start_year,
        "end_year": end_year,
        "x": x,
        "y": y,
        "unit": "kWh/m2",
        "years": years,
        "values": values,
        "global_values": global_values,  # Jetzt wird das Feld mitgeliefert
    }
    
def load_year_global_avg_from_cache(year):
    monthly_avgs = []

    for month in range(1, 13):
        try:
            month_data = load_month(year, month)  # lädt aus Cache, falls vorhanden
            avg_value = month_data.get("avg_radiation")

            if avg_value is not None:
                monthly_avgs.append(float(avg_value))
        except Exception:
            continue

    if not monthly_avgs:
        return None

    return float(sum(monthly_avgs))