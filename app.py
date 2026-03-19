from flask import Flask, render_template, request, jsonify
from dwd_service import load_block_grid, load_cell_block_timeseries

app = Flask(__name__)


@app.route("/")
def index():
    all_years = list(range(1991, 2025))  # oder dynamischer, falls du es anders willst

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

    return render_template("index.html", blocks=blocks)


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