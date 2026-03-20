from flask import Flask, jsonify, render_template, request
from dwd_service import get_monthly_anomaly_data, get_anomaly_heatmap_data

app = Flask(__name__)


@app.route("/")
def index():
    return render_template(
        "index.html",
        default_analysis_year=2024,
        default_reference_start=1991,
        default_reference_end=2020,
    )


@app.route("/api/monthly-anomaly")
def monthly_anomaly():
    region = request.args.get("region", default="germany", type=str)
    analysis_year = request.args.get("analysis_year", type=int)
    reference_start = request.args.get("reference_start", type=int)
    reference_end = request.args.get("reference_end", type=int)

    if None in (analysis_year, reference_start, reference_end):
        return jsonify({"error": "analysis_year, reference_start und reference_end sind erforderlich"}), 400

    try:
        return jsonify(
            get_monthly_anomaly_data(
                region=region,
                analysis_year=analysis_year,
                reference_start=reference_start,
                reference_end=reference_end,
            )
        )
    except Exception as error:
        return jsonify({"error": str(error)}), 500


@app.route("/api/anomaly-heatmap")
def anomaly_heatmap():
    region = request.args.get("region", default="germany", type=str)
    analysis_year = request.args.get("analysis_year", type=int)
    reference_start = request.args.get("reference_start", type=int)
    reference_end = request.args.get("reference_end", type=int)

    if None in (analysis_year, reference_start, reference_end):
        return jsonify({"error": "analysis_year, reference_start und reference_end sind erforderlich"}), 400

    try:
        return jsonify(
            get_anomaly_heatmap_data(
                region=region,
                analysis_year=analysis_year,
                reference_start=reference_start,
                reference_end=reference_end,
            )
        )
    except Exception as error:
        return jsonify({"error": str(error)}), 500


if __name__ == "__main__":
    app.run(debug=True)