from flask import Flask, jsonify, render_template, request
from dwd_service import get_monthly_overlay_data, get_seasonal_heatmap_data

app = Flask(__name__)


@app.route("/")
def index():
    return render_template("index.html", default_start=1991, default_end=2024)


@app.route("/api/monthly-overlay")
def monthly_overlay():
    start_year = request.args.get("start_year", type=int)
    end_year = request.args.get("end_year", type=int)

    if None in (start_year, end_year):
        return jsonify({"error": "start_year und end_year sind erforderlich"}), 400

    try:
        return jsonify(get_monthly_overlay_data(start_year, end_year))
    except Exception as error:
        return jsonify({"error": str(error)}), 500


@app.route("/api/seasonal-heatmap")
def seasonal_heatmap():
    start_year = request.args.get("start_year", type=int)
    end_year = request.args.get("end_year", type=int)

    if None in (start_year, end_year):
        return jsonify({"error": "start_year und end_year sind erforderlich"}), 400

    try:
        return jsonify(get_seasonal_heatmap_data(start_year, end_year))
    except Exception as error:
        return jsonify({"error": str(error)}), 500


if __name__ == "__main__":
    app.run(debug=True)