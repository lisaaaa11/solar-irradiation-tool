import requests
import zipfile
import io
import json
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from pathlib import Path

BASE_URL = "https://opendata.dwd.de/climate_environment/CDC/grids_germany/monthly/radiation_global/"

DATA_FOLDER = Path("data")
CACHE_FOLDER = DATA_FOLDER / "cache"
CATALOG_FILE = DATA_FOLDER / "catalog.json"

DATA_FOLDER.mkdir(exist_ok=True)
CACHE_FOLDER.mkdir(exist_ok=True)


def build_catalog():
    print("Hole verfügbare Dateien vom DWD...")

    r = requests.get(BASE_URL, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    files = []
    for link in soup.find_all("a"):
        href = link.get("href")
        if href and href.endswith(".zip"):
            yyyymm = href.split("_")[-1].replace(".zip", "")
            year = int(yyyymm[:4])
            month = int(yyyymm[4:6])

            files.append(
                {
                    "file": href,
                    "year": year,
                    "month": month,
                    "url": BASE_URL + href,
                }
            )

    with open(CATALOG_FILE, "w", encoding="utf-8") as f:
        json.dump(files, f, indent=2)

    return files


def load_catalog():
    if CATALOG_FILE.exists():
        with open(CATALOG_FILE, encoding="utf-8") as f:
            return json.load(f)
    return build_catalog()


def find_entry(year, month):
    catalog = load_catalog()

    for entry in catalog:
        if entry["year"] == year and entry["month"] == month:
            return entry

    raise ValueError(f"Monat nicht gefunden: {year}-{month:02d}")


def load_month(year, month):
    cache_file = CACHE_FOLDER / f"{year}{month:02d}.json"

    if cache_file.exists():
        with open(cache_file, encoding="utf-8") as f:
            cached_data = json.load(f)

        if "grid" in cached_data:
            return cached_data

    entry = find_entry(year, month)

    print(f"Lade {year}-{month:02d} vom DWD...")
    r = requests.get(entry["url"], headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()

    z = zipfile.ZipFile(io.BytesIO(r.content))
    asc_file = [name for name in z.namelist() if name.endswith(".asc")][0]

    with z.open(asc_file) as f:
        lines = f.readlines()

    metadata = {}
    for line in lines[:40]:
        text = line.decode("latin-1")
        if "=" in text:
            k, v = text.split("=", 1)
            metadata[k.strip()] = v.strip()

    start = 0
    ncols = None
    nrows = None
    xllcorner = None
    yllcorner = None
    cellsize = None
    nodata_value = -999

    for i, line in enumerate(lines):
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
            start = i + 1
            break

    numbers = []
    for line in lines[start:]:
        numbers += line.decode("latin-1").split()

    values = pd.to_numeric(numbers, errors="coerce")
    grid = values.reshape((nrows, ncols))
    grid[grid == nodata_value] = np.nan

    valid_values = grid[~np.isnan(grid)]
    avg = float(valid_values.mean())

    result = {
        "year": metadata.get("Jahr"),
        "month": metadata.get("Monat"),
        "avg_radiation": avg,
        "unit": metadata.get("Werte_Dimension"),
        "ncols": ncols,
        "nrows": nrows,
        "xllcorner": xllcorner,
        "yllcorner": yllcorner,
        "cellsize": cellsize,
        "grid": grid.tolist(),
    }

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f)

    return result


def load_block_grid(start_year, end_year):
    yearly_grids = []

    for year in range(start_year, end_year + 1):
        months = []

        for month in range(1, 13):
            try:
                month_data = load_month(year, month)
                months.append(np.array(month_data["grid"], dtype=float))
            except Exception as e:
                print(f"Fehler bei {year}-{month:02d}: {e}")

        if months:
            year_grid = np.nansum(months, axis=0)
            yearly_grids.append(year_grid)

    if not yearly_grids:
        raise ValueError(f"Keine Daten für Block {start_year}-{end_year} gefunden.")

    mean_grid = np.nanmean(yearly_grids, axis=0)

    first_month = load_month(start_year, 1)

    return {
        "start_year": start_year,
        "end_year": end_year,
        "unit": "kWh/m2",
        "ncols": first_month["ncols"],
        "nrows": first_month["nrows"],
        "xllcorner": first_month["xllcorner"],
        "yllcorner": first_month["yllcorner"],
        "cellsize": first_month["cellsize"],
        "grid": mean_grid.tolist(),
    }


def load_cell_block_timeseries(start_year, end_year, x, y):
    years = []
    values = []

    for year in range(start_year, end_year + 1):
        months = []

        for month in range(1, 13):
            try:
                month_data = load_month(year, month)
                grid = np.array(month_data["grid"], dtype=float)
                months.append(grid)
            except Exception as e:
                print(f"Fehler bei {year}-{month:02d}: {e}")

        if months:
            year_grid = np.nansum(months, axis=0)
            value = year_grid[y][x]

            years.append(year)
            values.append(value)

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
    }