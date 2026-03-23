import io, zipfile #ZIPs lesen
from pathlib import Path
from datetime import date
import numpy as np
import pandas as pd
import requests #http Downloads

BASE_URL = "https://opendata.dwd.de/climate_environment/CDC/grids_germany/monthly/radiation_global/"
OUTPUT_FILE = Path("data/monthly_means.csv")
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

#Passende URL für Monat und Jahr finden
def month_url(year, month):
    filename = f"grids_germany_monthly_radiation_global_{year}{month:02d}.zip"
    return BASE_URL + filename


def parse_asc_from_zip(content: bytes): #Übergeben werden die Bytes der ZIP-Datei
    zip_file = zipfile.ZipFile(io.BytesIO(content)) #ZIP-Datei im Speicher öffnen
    asc_name = next(name for name in zip_file.namelist() if name.endswith(".asc")) #ASC-Datei im ZIP finden, next() gibt erstes Element zurück, das Bedingung erfüllt

    with zip_file.open(asc_name) as file:
        lines = file.readlines()

    number_columns = number_rows = None
    nodata_value = None
    data_start = None

    #Durch die Zeilen iterieren für die Meta-Daten
    for i, byte_line in enumerate(lines):
        text = byte_line.decode("latin-1").strip() #Bytes in String umwandeln und Leerzeichen entfernen

        if text.startswith("NCOLS"):
            number_columns = int(text.split()[1]) #NCOLS 100 -> ["NCOLS", "100"] -> 100
        elif text.startswith("NROWS"):
            number_rows = int(text.split()[1])
        elif text.startswith("NODATA_VALUE"):
            nodata_value = float(text.split()[1])
            data_start = i + 1
            break

    if data_start is None or number_columns is None or number_rows is None:
        raise ValueError("ASC-Datei konnte nicht gelesen werden.")

    values = []
    for byte_line in lines[data_start:]:
        values.extend(byte_line.decode("latin-1").split()) #Zeile in String umwandeln, in Werte aufteilen und zur Liste hinzufügen

    grid = np.array(pd.to_numeric(values, errors="coerce"), dtype=float).reshape((number_rows, number_columns)) #Von 1D zu 2D (Weiß reshape durch Anzahl Spalten)
    grid[grid == nodata_value] = np.nan
    return grid


def load_month(year, month): #Von der Website die Daten für einen Monat laden und in ein 2D-Array umwandeln
    response = requests.get(month_url(year, month), headers={"User-Agent": "Mozilla/5.0"})
    if response.status_code != 200:
        return None

    grid = parse_asc_from_zip(response.content)

    mid = grid.shape[0] // 2 #grid.shape[0] gibt Anzahl Zeilen zurück, durch 2 teilen für Mitte
    north = grid[:mid, :] #Alle Zeilen von Anfang bis Mitte, alle Spalten
    south = grid[mid:, :]

    return {
        "year": year,
        "month": month,
        "germany": float(np.nanmean(grid)), #np.nanmean berechnet den Mittelwert und ignoriert dabei NaN-Werte
        "north": float(np.nanmean(north)),
        "south": float(np.nanmean(south)),
    }


def main():
    rows = []
    
    current_year = date.today().year

    for year in range(1991, current_year + 1):
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