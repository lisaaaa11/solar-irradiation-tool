from flask import Flask, jsonify, render_template, request
from dwd_service import load_block_grid, load_catalog, load_cell_block_timeseries

app = Flask(__name__)


def build_blocks(years):
    if not years:
        return []

    first_year = min(years)
    last_year = max(years)

    return [
        {
            "label": f"{start}-{min(start + 4, last_year)}",
            "start_year": start,
            "end_year": min(start + 4, last_year),
        }
        for start in range(first_year, last_year + 1, 5)
    ]


@app.route("/")
def index():
    years = sorted({entry["year"] for entry in load_catalog()})
    return render_template("index.html", blocks=build_blocks(years))


@app.route("/api/block-grid")
def block_grid():
    start_year = request.args.get("start_year", type=int)
    end_year = request.args.get("end_year", type=int)

    if None in (start_year, end_year):
        return jsonify({"error": "start_year und end_year sind erforderlich"}), 400

    try:
        return jsonify(load_block_grid(start_year, end_year))
    except Exception as error:
        return jsonify({"error": str(error)}), 500


@app.route("/api/cell-timeseries")
def cell_timeseries():
    start_year = request.args.get("start_year", type=int)
    end_year = request.args.get("end_year", type=int)
    x = request.args.get("x", type=int)
    y = request.args.get("y", type=int)

    if None in (start_year, end_year, x, y):
        return jsonify({"error": "start_year, end_year, x und y sind erforderlich"}), 400

    try:
        return jsonify(load_cell_block_timeseries(start_year, end_year, x, y))
    except Exception as error:
        return jsonify({"error": str(error)}), 500


if __name__ == "__main__":
    app.run(debug=True)
