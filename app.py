from flask import Flask, jsonify, render_template, request #Webserver, JSON-Antworten aus Python-Daten, rendert HTML, HTTP-Anfragen
from dwd_service import get_monthly_deviation_data, get_monthly_deviation_data

app = Flask(__name__) #


@app.route("/") #Startseite
def index():
    return render_template("index.html")


@app.route("/api/monthly-deviation") #Wenn ein http-Request an diesen Endpunkt gesendet wird, führe monthly_deviation aus.  region, analysis_year, reference_start, reference_end
def monthly_deviation():
    region = request.args.get("region", type=str)
    analysis_year = request.args.get("analysis_year", type=int)
    reference_start = request.args.get("reference_start", type=int)
    reference_end = request.args.get("reference_end", type=int)

    if None in (analysis_year, reference_start, reference_end):
        return jsonify({"error": "analysis_year, reference_start und reference_end sind erforderlich"}), 400

    try:
        return jsonify(
            get_monthly_deviation_data(
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