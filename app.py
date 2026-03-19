from flask import Flask, render_template, request, jsonify
from dwd_service import load_catalog, load_month, load_block_grid, load_cell_block_timeseries

app = Flask(__name__)


@app.route("/")
def index():
    catalog = load_catalog()

    months = []
    all_years = set()

    for entry in catalog:
        months.append(
            {
                "label": f"{entry['year']}-{entry['month']:02d}",
                "year": entry["year"],
                "month": entry["month"],
            }
        )
        all_years.add(entry["year"])

    all_years = sorted(list(all_years))

    blocks = []
    if all_years:
        min_year = all_years[0]
        max_year = all_years[-1]

        start = min_year
        while start <= max_year:
            end = min(start + 4, max_year)
            blocks.append(
                {
                    "label": f"{start}-{end}",
                    "start_year": start,
                    "end_year": end,
                }
            )
            start += 5

    return render_template("index.html", months=months, blocks=blocks)


@app.route("/api/month-data")
def month_data():
    year = request.args.get("year", type=int)
    month = request.args.get("month", type=int)

    if year is None or month is None:
        return jsonify({"error": "year und month sind erforderlich"}), 400

    try:
        data = load_month(year, month)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/block-grid")
def block_grid():
    start_year = request.args.get("start_year", type=int)
    end_year = request.args.get("end_year", type=int)

    if start_year is None or end_year is None:
        return jsonify({"error": "start_year und end_year sind erforderlich"}), 400

    try:
        data = load_block_grid(start_year, end_year)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/cell-timeseries")
def cell_timeseries():
    start_year = request.args.get("start_year", type=int)
    end_year = request.args.get("end_year", type=int)
    x = request.args.get("x", type=int)
    y = request.args.get("y", type=int)

    if start_year is None or end_year is None or x is None or y is None:
        return jsonify({"error": "start_year, end_year, x und y sind erforderlich"}), 400

    try:
        data = load_cell_block_timeseries(start_year, end_year, x, y)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True)