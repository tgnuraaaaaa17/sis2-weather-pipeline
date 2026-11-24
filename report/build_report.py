"""Build report/project_report.pdf from the REAL results of the pipeline.

Run after `python main.py`:   python report/build_report.py
Numbers, tables, log excerpts, figures and code excerpts are all read from the
files produced by the pipeline and from the source code, so the report always
matches the repository. Edit report/report_config.json (group members etc.) first.
"""
from __future__ import annotations

import ast
import json
import sys
import textwrap
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (Image, PageBreak, Paragraph, Preformatted, SimpleDocTemplate,
                                Spacer, Table, TableStyle)
from xml.sax.saxutils import escape

from src import config
from src.cleaner import WHY
from src.loader import SCHEMA_SQL
from src.utils import load_metrics
from src.validator import RULES

M = load_metrics()
CFG = json.loads((ROOT / "report" / "report_config.json").read_text(encoding="utf-8"))
OUT = ROOT / "report" / "project_report.pdf"

ss = getSampleStyleSheet()
BODY = ParagraphStyle("body", parent=ss["BodyText"], fontSize=10, leading=14, spaceAfter=6)
H1 = ParagraphStyle("h1", parent=ss["Heading1"], fontSize=16, spaceBefore=6, spaceAfter=8)
H2 = ParagraphStyle("h2", parent=ss["Heading2"], fontSize=12, spaceBefore=8, spaceAfter=4)
CAP = ParagraphStyle("cap", parent=BODY, fontSize=8.5, leading=11, textColor=colors.HexColor("#444444"), alignment=1)
CELL = ParagraphStyle("cell", parent=BODY, fontSize=8, leading=10, spaceAfter=0)
CODE = ParagraphStyle("code", fontName="Courier", fontSize=7, leading=8.5, backColor=colors.HexColor("#F3F3F3"),
                      borderPadding=4, spaceAfter=8)
story: list = []
_tab, _fig = [0], [0]


def g(stage: str, key: str, default="n/a"):
    return M.get(stage, {}).get(key, default)


def P(text: str, style=BODY):
    story.append(Paragraph(text, style))


def h1(text: str):
    story.append(Paragraph(text, H1))


def h2(text: str):
    story.append(Paragraph(text, H2))


def bullets(items):
    for it in items:
        story.append(Paragraph("&bull; " + it, BODY))


def table(title: str, rows: list[list], widths=None, header=True):
    _tab[0] += 1
    story.append(Paragraph(f"<b>Table {_tab[0]}.</b> {escape(title)}", CAP))
    data = [[Paragraph(escape(str(c)), CELL) for c in r] for r in rows]
    t = Table(data, colWidths=widths, repeatRows=1 if header else 0)
    style = [("GRID", (0, 0), (-1, -1), 0.4, colors.grey), ("VALIGN", (0, 0), (-1, -1), "TOP")]
    if header:
        style.append(("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#DDE6F0")))
    t.setStyle(TableStyle(style))
    story.extend([t, Spacer(1, 8)])


def df_table(title: str, frame: pd.DataFrame, widths=None):
    table(title, [list(frame.columns)] + frame.astype(str).values.tolist(), widths)


def figure(path: Path, caption: str, width=14 * cm):
    if not path.exists():
        P(f"<i>[figure {path.name} not found - run the pipeline]</i>")
        return
    _fig[0] += 1
    from PIL import Image as PILImage
    w, h = PILImage.open(path).size
    story.append(Image(str(path), width=width, height=width * h / w))
    story.append(Paragraph(f"<b>Figure {_fig[0]}.</b> {escape(caption)}", CAP))
    story.append(Spacer(1, 8))


def code(title: str, text: str):
    story.append(Paragraph(f"<i>{escape(title)}</i>", CAP))
    story.append(Preformatted(text.rstrip(), CODE))


def func_source(relpath: str, name: str, max_lines=40) -> str:
    path = ROOT / relpath
    src = path.read_text(encoding="utf-8")
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name == name:
            lines = ast.get_source_segment(src, node).splitlines()
            if len(lines) > max_lines:
                lines = lines[:max_lines] + ["    ... (shortened)"]
            return "\n".join(lines)
    return f"# {name} not found in {relpath}"


def log_lines(*needles: str) -> str:
    log = ROOT / "logs" / "pipeline.log"
    if not log.exists():
        return "(logs/pipeline.log not found)"
    lines = log.read_text(encoding="utf-8").splitlines()
    picked = []
    for needle in needles:
        hits = [ln for ln in lines if needle in ln]
        if hits:
            picked.extend(textwrap.wrap(hits[-1], width=105, subsequent_indent='    ', break_long_words=True))
    return "\n".join(picked) or "(no matching log lines)"


def csv_head(path: Path, n=5, cols=None) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame({"info": ["file not found"]})
    frame = pd.read_csv(path)
    return frame[cols].head(n) if cols else frame.head(n)


# ------------------------------------------------------------------ diagram
def draw_architecture(path: Path) -> None:
    fig, ax = plt.subplots(figsize=(11, 4.2))
    ax.set_xlim(0, 11)
    ax.set_ylim(0, 4.2)
    ax.axis("off")
    boxes = [
        (0.1, 2.9, "Open-Meteo\nForecast API", "#FFF2CC"), (2.3, 2.9, "api_client.py\nraw_data.csv", "#DAE8FC"),
        (4.5, 2.9, "producer.py\n(JSON)", "#D5E8D4"), (6.7, 2.9, "Apache Kafka\ntopic: weather_raw", "#F8CECC"),
        (8.9, 2.9, "consumer.py\nconsumed_data.csv", "#D5E8D4"),
        (8.9, 0.9, "validator.py\n(Pandera)", "#E1D5E7"), (6.7, 0.9, "cleaner.py\npandas + NumPy", "#DAE8FC"),
        (4.5, 0.9, "loader.py\nSQLite3 (upsert)", "#DAE8FC"), (2.3, 0.9, "analysis.py\nEDA + figures", "#DAE8FC"),
        (0.1, 0.9, "verify.py\ncount checks", "#FFF2CC"),
    ]
    for x, y, label, color in boxes:
        ax.add_patch(FancyBboxPatch((x, y), 1.9, 0.9, boxstyle="round,pad=0.03", fc=color, ec="black"))
        ax.text(x + 0.95, y + 0.45, label, ha="center", va="center", fontsize=8.5)
    arrows = [((2.0, 3.35), (2.3, 3.35)), ((4.2, 3.35), (4.5, 3.35)), ((6.4, 3.35), (6.7, 3.35)),
              ((8.6, 3.35), (8.9, 3.35)), ((9.85, 2.9), (9.85, 1.8)), ((8.9, 1.35), (8.6, 1.35)),
              ((6.7, 1.35), (6.4, 1.35)), ((4.5, 1.35), (4.2, 1.35)), ((2.3, 1.35), (2.0, 1.35))]
    for a, b in arrows:
        ax.add_patch(FancyArrowPatch(a, b, arrowstyle="-|>", mutation_scale=14, color="black"))
    ax.text(10.0, 2.35, "valid / quarantine", fontsize=7, ha="left")
    ax.set_title("End-to-end pipeline architecture", fontsize=11)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)


# =================================================================== CONTENT
def title_page():
    story.append(Spacer(1, 4 * cm))
    story.append(Paragraph(CFG["project_title"], ParagraphStyle("t", parent=H1, fontSize=24, leading=30, alignment=1)))
    story.append(Spacer(1, 1 * cm))
    for label, value in [("Course", CFG["course"]), ("Assignment", "SIS2 - Data Streaming and Validation Pipeline"),
                         ("Submission date", CFG["submission_date"])]:
        story.append(Paragraph(f"<b>{label}:</b> {escape(str(value))}", ParagraphStyle("c", parent=BODY, fontSize=12, alignment=1)))
    story.append(Spacer(1, 1 * cm))
    story.append(Paragraph("<b>Group members</b>", ParagraphStyle("c2", parent=BODY, fontSize=12, alignment=1)))
    for member in CFG["members"]:
        story.append(Paragraph(escape(member["name"]), ParagraphStyle("c3", parent=BODY, fontSize=12, alignment=1)))
    story.append(PageBreak())


def introduction():
    h1("1. Introduction")
    h2("1.1 Project background")
    P("Weather conditions influence transport, energy use and daily planning. Public weather APIs deliver data in "
      "small JSON batches, which is a typical input for modern data engineering pipelines: collect, stream, "
      "validate, clean, store and analyse. This project builds such a pipeline for hourly weather data of "
      f"{len(config.CITIES)} cities of Kazakhstan ({', '.join(c['name'] for c in config.CITIES)}).")
    h2("1.2 Problem description")
    P("Raw API data cannot be trusted blindly: values can be missing, out of range or duplicated, and messages can be "
      "lost or repeated while travelling between systems. The problem is to build a reproducible pipeline that moves "
      "the data through Apache Kafka, proves its quality with Pandera, and stores a clean, duplicate-free result in "
      "a relational database for analysis.")
    h2("1.3 Objectives")
    bullets(["Collect at least 200 real records from a public API with error handling and keep the original data.",
             "Publish and consume every record through a real Apache Kafka broker (JSON messages, separate scripts).",
             "Validate data with a Pandera schema (at least five rules) and quarantine invalid records.",
             "Clean and transform data with pandas and NumPy (missing values, duplicates, types, text, GroupBy, reshaping).",
             "Store data in SQLite3 with keys and constraints, and prove idempotent loading.",
             "Explore the data (EDA) and present at least three visualisations with interpretation.",
             "Verify the whole pipeline by comparing record counts at every stage."])
    P("<b>Research questions answered by the exploratory analysis:</b>")
    bullets(["<b>RQ1</b> - How do the eight cities differ in temperature, humidity and wind speed?",
             "<b>RQ2</b> - How are air temperature and relative humidity related?",
             "<b>RQ3</b> - Which weather conditions dominate, and how does temperature change during the day?"])
    h2("1.4 Expected outcomes")
    P("A runnable repository (python main.py), a SQLite database data/output.db, figures in figures/, and this report; "
      "all numbers in this report are produced by the code in the repository.")


def data_source():
    h1("2. Data Source and Collection")
    h2("2.1 API description and endpoint")
    P(f"<b>Open-Meteo Weather Forecast API</b> (free, no API key). Endpoint: <font face='Courier'>{config.API_URL}</font>; "
      f"documentation: {config.API_DOCS_URL}. The API returns hourly forecasts as JSON where every variable is an array "
      "aligned with the 'time' array.")
    h2("2.2 Request parameters")
    params = g("api", "request_parameters", {})
    table("Request parameters (one GET request per city)", [["Parameter", "Value"]] + [[k, v] for k, v in params.items()],
          [5 * cm, 11 * cm])
    P("The table shows the parameters of one request; latitude and longitude are different for each of the "
      f"{len(config.CITIES)} cities (taken from the city list in src/config.py), all other parameters are the same.")
    h2("2.3 Data collection procedure")
    P("api_client.py sends one request per city using a requests Session with automatic retries (HTTP 429/5xx, "
      "exponential back-off) and a timeout. Network errors, non-200 status codes, invalid JSON, API error objects and "
      "incomplete 'hourly' blocks raise ApiError; the failing city is logged and skipped, the others continue. The untouched "
      "responses are saved to data/raw_api_responses.json and the flattened table to data/raw_data.csv BEFORE any cleaning.")
    code("api_client.py - request + error handling", func_source("src/api_client.py", "fetch_city", 30))
    h2("2.4 Dataset size and structure")
    table("Collection summary", [["Item", "Value"],
          ["Cities requested / successful", f"{g('api','cities_requested')} / {g('api','cities_ok')}"],
          ["Failed cities", ", ".join(g("api", "cities_failed", [])) or "none"],
          ["Records collected", g("api", "records_collected")],
          ["First / last hour", f"{g('api','time_min')} / {g('api','time_max')}"],
          ["Missing values in raw data", g("api", "missing_values_in_raw", {}) or "none"],
          ["Collected at (UTC)", g("api", "collected_at")]], [6 * cm, 10 * cm])
    h2("2.5 Fields")
    table("Description of numerical, categorical and date/time fields", [
        ["Field", "Type", "Kind", "Description"],
        ["record_id", "text", "identifier", "city|time, unique key"],
        ["city, country", "text", "categorical", "City name and ISO country code"],
        ["latitude, longitude", "float", "numerical", "Grid-cell coordinates returned by API"],
        ["time", "text (ISO 8601)", "date/time", "Hour in Asia/Almaty time zone"],
        ["temperature_2m", "float", "numerical", "Air temperature at 2 m, degC"],
        ["relative_humidity_2m", "float", "numerical", "Relative humidity, %"],
        ["wind_speed_10m", "float", "numerical", "Wind speed at 10 m, km/h"],
        ["weather_code", "int", "categorical", "WMO weather interpretation code"],
        ["source", "text", "categorical", "open-meteo or synthetic_test"]], [3.4 * cm, 2.6 * cm, 2.4 * cm, 7.6 * cm])
    h2("2.6 Examples of collected records")
    df_table("First rows of data/raw_data.csv", csv_head(config.RAW_CSV_PATH, 5, ["record_id", "time", "temperature_2m",
             "relative_humidity_2m", "wind_speed_10m", "weather_code"]))


def architecture():
    h1("3. Pipeline Architecture")
    diagram = ROOT / "figures" / "architecture.png"
    draw_architecture(diagram)
    figure(diagram, "Complete pipeline architecture", 16 * cm)
    table("Components and data flow", [["Component", "Input -> Output", "Role"],
        ["api_client.py", "API -> data/raw_data.csv, raw_api_responses.json", "Collect and preserve original data"],
        ["producer.py", "raw_data.csv -> Kafka topic", "Serialise to JSON and publish; adds 5 invalid test records"],
        ["Kafka broker", "topic weather_raw", "Durable, ordered message log between producer and consumer"],
        ["consumer.py", "Kafka -> data/consumed_data.csv", "Deserialise JSON; stops at expected count or idle timeout"],
        ["validator.py", "consumed_data.csv -> validated_valid.csv / quarantine_invalid.csv", "Pandera schema, quarantine invalid rows"],
        ["cleaner.py", "validated_valid.csv -> clean_data.csv + summaries", "pandas / NumPy cleaning and transformation"],
        ["loader.py", "clean_data.csv -> data/output.db", "Idempotent SQLite upsert, SQL verification"],
        ["analysis.py", "output.db -> figures/, eda_findings.md", "EDA and visualisation (reads from the database)"],
        ["verify.py", "pipeline_metrics.json -> pipeline_summary.md", "Count comparison and PASS/FAIL checks"]],
        [3 * cm, 6.2 * cm, 6.8 * cm])
    P("Every stage reads the output file of the previous stage; no manually prepared dataset is used. main.py starts the "
      "stages one after another as separate processes, and each stage appends its results to data/pipeline_metrics.json "
      "which feeds both the verification stage and this report.")


def kafka():
    h1("4. Apache Kafka Implementation")
    h2("4.1 Broker configuration")
    P("A real single-node Apache Kafka 3.8.0 broker runs in Docker (KRaft mode, no ZooKeeper) using docker-compose.yml. "
      f"Clients connect to <font face='Courier'>{escape(str(g('producer','broker', config.KAFKA_BOOTSTRAP)))}</font> "
      "through the confluent-kafka Python client.")
    h2("4.2 Topic configuration")
    table("Topic", [["Setting", "Value"], ["Name", g("producer", "topic")], ["Partitions", g("producer", "partitions")],
                    ["Replication factor", config.KAFKA_REPLICATION],
                    ["Message key", "record_id"], ["Message value", "UTF-8 JSON object"], ["Header", "run_id (separates pipeline runs)"]],
          [5 * cm, 11 * cm])
    P("One partition keeps strict ordering, which makes duplicate detection deterministic. The topic is created by the "
      "producer (AdminClient) if it does not exist. The run_id header lets the consumer ignore messages of older runs.")
    h2("4.3 Producer implementation and JSON serialisation")
    code("producer.py - delivery tracking", func_source("src/producer.py", "DeliveryTracker", 20))
    P("Each record is serialised with json.dumps (allow_nan=False, so a NaN cannot silently enter the stream), encoded as UTF-8 "
      "and sent with acks=all and idempotent delivery. Delivery callbacks count confirmations from the broker; BufferError "
      "(full local queue) is handled by polling and retrying; flush() waits for outstanding messages.")
    h2("4.4 Consumer implementation and JSON deserialisation")
    P("consumer.py joins a new consumer group per run (auto.offset.reset=earliest, manual commit), decodes each message with "
      "json.loads and stores it. Malformed messages are counted and written to data/consumer_errors.jsonl instead of stopping "
      "the pipeline. <b>Termination condition:</b> the consumer stops when the expected number of messages of the run has been "
      f"consumed or when no message arrives for {config.CONSUMER_IDLE_TIMEOUT} s ({config.CONSUMER_FIRST_MSG_TIMEOUT} s for the first message).")
    h2("4.5 Message transmission results")
    table("Kafka results", [["Metric", "Value"],
          ["Messages published (confirmed by broker)", g("producer", "published")],
          ["API records / deliberate test records sent", f"{g('producer','api_records_sent')} / {g('producer','test_records_sent')}"],
          ["Delivery failures / serialisation errors", f"{g('producer','delivery_failed')} / {g('producer','serialization_errors')}"],
          ["Messages consumed", g("consumer", "consumed")],
          ["Deserialisation errors / skipped (older runs)", f"{g('consumer','deserialization_errors')} / {g('consumer','skipped_other_runs')}"],
          ["Consumer stopped by", g("consumer", "terminated_by")],
          ["Offsets consumed (partition: [first, last])", g("consumer", "offsets_per_partition")],
          ["Producer / consumer duration (s)", f"{g('producer','duration_s')} / {g('consumer','duration_s')}"]], [8 * cm, 8 * cm])
    h2("4.6 Error handling")
    bullets(["Broker unreachable: AdminClient.list_topics() raises KafkaException -> clear message, exit code 2.",
             "Transmission: delivery callback counts failures; BufferError retry; flush timeout is reported.",
             "Processing: JSON/UTF-8 errors are quarantined in consumer_errors.jsonl; broker errors are logged and counted."])
    h2("4.7 Evidence of successful publishing and consumption")
    code("logs/pipeline.log (excerpt)", log_lines("Topic '", "PUBLISHED", "Consuming topic", "CONSUMED"))


def pandera_section():
    h1("5. Pandera Validation")
    h2("5.1 Validation schema")
    P("validator.py defines a DataFrameSchema (strict: no unexpected columns) and validates the consumed dataset with "
      "lazy=True, so that all errors of all rows are collected instead of stopping at the first one. Validation runs "
      "BEFORE cleaning.")
    code("validator.py - schema (excerpt)", func_source("src/validator.py", "build_schema", 28))
    h2("5.2 Validation rules")
    table(f"{len(RULES)} validation rules", [["ID", "Column(s)", "Rule", "Type"]] + [list(r) for r in RULES],
          [1.1 * cm, 4.4 * cm, 8.5 * cm, 2 * cm])
    h2("5.3 Handling of invalid data")
    P("<b>Strategy: quarantine.</b> Invalid rows are removed from the main flow and saved with their error messages to "
      "data/quarantine_invalid.csv (details per failed check in data/validation_errors.csv); valid rows continue to cleaning. "
      "A structurally broken dataset (missing columns) stops the pipeline.")
    h2("5.4 Results")
    table("Validation summary", [["Metric", "Value"], ["Records received", g("validation", "received")],
          ["Valid records", g("validation", "valid")], ["Invalid records", g("validation", "invalid")],
          ["Invalid real API records", g("validation", "real_records_invalid")],
          ["Deliberate test records / detected", f"{g('validation','test_records_total')} / {g('validation','test_records_detected')}"]],
          [8 * cm, 8 * cm])
    errs = g("validation", "errors_by_rule", {})
    if errs:
        table("Failed checks (number of failure cases)", [["Column: check", "Count"]] + [[k, v] for k, v in errs.items()], [12 * cm, 4 * cm])
    df_table("Examples of valid records", csv_head(config.VALID_PATH, 3, ["record_id", "temperature_2m", "relative_humidity_2m", "weather_code"]))
    df_table("Examples of invalid records (quarantine)", csv_head(config.QUARANTINE_PATH, 5, ["record_id", "validation_errors"]),
             [4.5 * cm, 11.5 * cm])
    h2("5.5 Intentionally invalid test records")
    P("Open-Meteo normally returns clean data, so the producer appends five clearly marked test records "
      "(source = synthetic_test): humidity 135 %, missing temperature, unknown weather code 999, negative wind speed and a duplicated "
      "record_id. Pandera must detect all of them; the cleaner removes any test record from the final dataset. "
      "The same cases are covered by automated tests in tests/test_validation.py.")
    code("validator.py - turning Pandera failure cases into quarantine rows", func_source("src/validator.py", "validate_dataframe", 20))


def cleaning():
    h1("6. Data Cleaning and Transformation")
    c = M.get("cleaning", {})
    table("Cleaning results", [["Step", "Result"],
          ["Input records (valid)", c.get("input_records", "n/a")],
          ["Test records removed", c.get("test_records_removed", "n/a")],
          ["Missing values before cleaning", c.get("missing_before") or "none"],
          ["Wind values imputed (city median)", c.get("wind_values_imputed", "n/a")],
          ["Missing values after cleaning", c.get("missing_after", "n/a")],
          ["Duplicates removed", c.get("duplicates_removed", "n/a")],
          ["Filter: temperature <= 0 and wind >= 20 km/h", c.get("filter_windy_cold", "n/a")],
          ["Filter: humidity >= 80, 08-20 h, rain/snow/fog", c.get("filter_humid_daytime_precipitation_or_fog", "n/a")],
          ["Records after cleaning", c.get("records_after_cleaning", "n/a")]], [9 * cm, 7 * cm])
    P("<i>Note:</i> the five deliberately invalid test records were already placed in quarantine by the Pandera stage, so the "
      "cleaner receives only valid records and finds no test records or duplicates to remove (the corresponding counters are 0); "
      "the cleaner still contains these checks as a second safety net.")
    h2("6.1 Missing values, duplicates, types and text")
    P("Missing values are analysed with isna().sum(); wind speed (the only optional field) is imputed with the per-city median "
      "and flagged in wind_imputed. Duplicates are found with duplicated(subset='record_id'). time is converted to datetime64, "
      "weather_code to int64 and measurements to float64. City names are stripped and title-cased, country codes upper-cased, and "
      "weather_code is mapped to a readable category and description.")
    h2("6.2 NumPy operations")
    code("cleaner.py - NumPy feature engineering", func_source("src/cleaner.py", "add_numpy_features", 30))
    P("Vectorised NumPy functions (np.log, np.power, np.where, np.select, np.std, np.clip) compute dew point (Magnus formula), "
      "wind chill, wind speed in m/s, per-city temperature z-score, comfort level and part of day.")
    h2("6.3 Filtering, GroupBy and reshaping")
    summary = ROOT / "data" / "summary_by_city.csv"
    if summary.exists():
        df_table("GroupBy per city (named aggregations)", pd.read_csv(summary))
    pivot = ROOT / "data" / "pivot_city_part_of_day.csv"
    if pivot.exists():
        df_table("pivot_table: mean temperature, city x part of day", pd.read_csv(pivot))
    P(f"melt() converts the city summary into long format ({g('cleaning','long_format_shape')} rows x columns) "
      "(data/long_city_metrics.csv).")
    h2("6.4 Why these operations were necessary")
    table("Justification", [["Operation", "Reason"]] + [[k, v] for k, v in WHY.items()], [3.5 * cm, 12.5 * cm])


def sqlite_section():
    h1("7. SQLite3 Database Implementation")
    h2("7.1 Schema, keys and constraints")
    code("loader.py - table schema", SCHEMA_SQL.strip())
    P("Primary keys: cities.city and weather_observations.record_id. weather_observations.city is a FOREIGN KEY to cities, "
      "UNIQUE(city, observation_time) is an additional uniqueness constraint, and CHECK constraints repeat the physical "
      "limits (humidity 0-100, temperature range, non-negative wind).")
    h2("7.2 Insertion process")
    P("All rows are written in one transaction with an UPSERT statement (INSERT ... ON CONFLICT(record_id) DO UPDATE). "
      "If the same record_id arrives again, the row is overwritten with identical values instead of being duplicated.")
    code("loader.py - load step", func_source("src/loader.py", "load_dataframe", 20))
    h2("7.3 SQL verification queries and results")
    ver = M.get("sqlite", {}).get("verification", {})
    rows = [["Query (sql/verification_queries.sql)", "Result (first rows)"]]
    for name, res in ver.items():
        if isinstance(res, dict):
            rows.append([name, str(res["rows"][:4]) if res["rows"] else "no rows"])
    rows.append(["PRAGMA integrity_check", ver.get("pragma_integrity_check", "n/a")])
    rows.append(["PRAGMA foreign_key_check (violations)", ver.get("pragma_foreign_key_violations", "n/a")])
    table("SQL verification", rows, [5 * cm, 11 * cm])
    h2("7.4 Idempotency test")
    s = M.get("sqlite", {})
    table("Loading the same dataset twice", [["Run", "Rows before", "Attempted", "Inserted", "Rows after", "Table checksum"],
          ["Load #1", s.get("load_1", {}).get("rows_before"), s.get("load_1", {}).get("rows_attempted"),
           s.get("load_1", {}).get("rows_inserted"), s.get("load_1", {}).get("rows_after"), str(s.get("checksum_1", ""))[:16]],
          ["Load #2", s.get("load_2", {}).get("rows_before"), s.get("load_2", {}).get("rows_attempted"),
           s.get("load_2", {}).get("rows_inserted"), s.get("load_2", {}).get("rows_after"), str(s.get("checksum_2", ""))[:16]]],
          [2 * cm, 2.4 * cm, 2.4 * cm, 2.4 * cm, 2.4 * cm, 4.4 * cm])
    P(f"Result: <b>{'PASSED' if s.get('idempotency_passed') else 'FAILED'}</b> - the second run inserted no rows and the SHA-256 "
      "checksum of the whole table is identical, so stored data were not changed. Records processed: "
      f"{s.get('records_processed','n/a')}; records stored: {s.get('records_stored','n/a')}.")
    code("logs/pipeline.log (excerpt)", log_lines("LOAD #1", "LOAD #2", "IDEMPOTENCY", "Processed="))


def fig_notes() -> dict:
    """Interpretations of the figures computed from data/clean_data.csv (always match the current run)."""
    try:
        df = pd.read_csv(ROOT / "data" / "clean_data.csv")
    except Exception:
        return {}
    notes: dict = {}

    def safe(key, fn):
        try:
            notes[key] = fn()
        except Exception:
            pass

    def f1():
        t = df.groupby("city")["temperature_c"].agg(["mean", "min", "max"])
        hot, cold = t["mean"].idxmax(), t["mean"].idxmin()
        return (f"Every curve rises during the day and falls at night, so the daily cycle is clearly visible. "
                f"{hot} is the warmest city (mean {t.loc[hot, 'mean']:.1f} C, peak {t.loc[hot, 'max']:.1f} C) and {cold} "
                f"is the coldest (mean {t.loc[cold, 'mean']:.1f} C, minimum {t.loc[cold, 'min']:.1f} C); the difference of "
                f"city means is {t['mean'].max() - t['mean'].min():.1f} C.")

    def f2():
        h = df.groupby("city")["humidity_pct"]
        med = h.median()
        iqr = h.quantile(0.75) - h.quantile(0.25)
        return (f"{med.idxmax()} has the highest median humidity ({med.max():.0f} %) and {med.idxmin()} the lowest "
                f"({med.min():.0f} %). {iqr.idxmax()} has the widest box (interquartile range {iqr.max():.0f} points), "
                f"i.e. its humidity changes most; {iqr.idxmin()} is the most stable ({iqr.min():.0f} points). "
                "Points outside the whiskers are outliers (hours that differ strongly from the typical level of the city).")

    def f3():
        c = df["weather_category"].value_counts()
        total = c.sum()
        rare = ", ".join(f"{k} ({v})" for k, v in c.tail(3).items())
        return (f"'{c.index[0]}' dominates with {c.iloc[0]} hours ({100 * c.iloc[0] / total:.0f} % of all hours), followed by "
                f"'{c.index[1]}' ({c.iloc[1]} hours). The rarest conditions are {rare}.")

    def f4():
        x, y = df["temperature_c"].to_numpy(float), df["humidity_pct"].to_numpy(float)
        r = float(np.corrcoef(x, y)[0, 1])
        slope = float(np.polyfit(x, y, 1)[0])
        strength = "strong" if abs(r) >= 0.7 else "moderate" if abs(r) >= 0.4 else "weak"
        direction = "negative" if r < 0 else "positive"
        return (f"Pearson r = {r:.2f} is a {strength} {direction} relation; the trend line changes humidity by {slope:.2f} "
                "percentage points per degree. Each point is one hour, and the spread around the line shows that temperature "
                "alone does not determine humidity (city climate, cloud cover and precipitation also matter).")

    def f5():
        cols = [c for c in ["temperature_c", "humidity_pct", "wind_speed_kmh", "dew_point_c", "wind_chill_c"] if c in df]
        m = df[cols].corr()
        return (f"Temperature and wind chill are almost identical (r = {m.loc['temperature_c', 'wind_chill_c']:.2f}) because "
                f"wind chill is calculated from temperature. Temperature and humidity are negatively correlated "
                f"(r = {m.loc['temperature_c', 'humidity_pct']:.2f}). Wind speed is almost independent of temperature "
                f"(r = {m.loc['temperature_c', 'wind_speed_kmh']:.2f}) and humidity "
                f"(r = {m.loc['humidity_pct', 'wind_speed_kmh']:.2f}). Derived variables are strongly correlated with their source "
                "and should not be used together in a model.")

    def f6():
        pv = df.pivot_table(index="city", columns="part_of_day", values="temperature_c", aggfunc="mean")
        stacked = pv.stack()
        (wc, wp), wv = stacked.idxmax(), stacked.max()
        (cc, cp), cv = stacked.idxmin(), stacked.min()
        amp = pv.max(axis=1) - pv.min(axis=1)
        return (f"The warmest cell is {wc} in the {wp.lower()} ({wv:.1f} C) and the coldest is {cc} at {cp.lower()} ({cv:.1f} C). "
                f"The day-night amplitude of the mean temperature is largest in {amp.idxmax()} ({amp.max():.1f} C) and smallest in "
                f"{amp.idxmin()} ({amp.min():.1f} C).")

    for key, fn in [("fig01", f1), ("fig02", f2), ("fig03", f3), ("fig04", f4), ("fig05", f5), ("fig06", f6)]:
        safe(key, fn)
    return notes


def eda():
    h1("8. Exploratory Data Analysis")
    e = M.get("eda", {})
    h2("8.1 Dimensions, types and descriptive statistics")
    P(f"The analysed table (joined from SQLite) has shape {e.get('shape', 'n/a')}.")
    desc = e.get("describe", {})
    if desc:
        frame = pd.DataFrame(desc).T.reset_index().rename(columns={"index": "variable"})
        df_table("Descriptive statistics of numerical variables", frame)
    cats = e.get("categorical_counts", {})
    if cats:
        table("Categorical variables (value counts)", [["Variable", "Counts"]] + [[k, str(v)] for k, v in cats.items()],
              [3 * cm, 13 * cm])
    cs = ROOT / "data" / "summary_by_weather_category.csv"
    if cs.exists():
        df_table("Grouped analysis: weather category", pd.read_csv(cs))
    h2("8.2 Visualisations and interpretation")
    notes = e.get("findings", [])
    figs = [("fig01_temperature_timeseries.png", "Hourly temperature by city (RQ1)",
             "Shows the daily cycle and the level differences between cities."),
            ("fig02_humidity_boxplot_by_city.png", "Humidity distribution by city (RQ1)",
             "Compares median, spread and outliers of humidity across cities."),
            ("fig03_weather_category_counts.png", "Hours per weather category (RQ3)",
             "Shows which weather conditions dominate the period."),
            ("fig04_temperature_vs_humidity.png", "Temperature vs humidity (RQ2)",
             "Each point is one hour; the dashed line is a linear trend."),
            ("fig05_correlation_heatmap.png", "Correlation matrix",
             "Pearson correlations between numerical variables."),
            ("fig06_city_part_of_day_heatmap.png", "Mean temperature by city and part of day (RQ3)",
             "Visualises the pivot table from the cleaning stage.")]
    computed = fig_notes()
    for file, caption, what in figs:
        figure(ROOT / "figures" / file, caption)
        text = computed.get(file[:5])
        P(f"<b>Interpretation.</b> {escape(text) if text else what + ' See the computed findings below.'}")
    h2("8.3 Main findings")
    bullets([escape(n) for n in notes] or ["(run the analysis stage)"])
    P("<i>Note:</i> the data are short-term hourly forecasts for a few days, so conclusions describe this period only "
      "and cannot be generalised to the climate of the cities.")


def verification():
    h1("9. Pipeline Testing and Verification")
    v = M.get("verification", {})
    counts = v.get("counts", {})
    table("Record counts at every stage", [["Stage", "Records"]] + [[k, str(x)] for k, x in counts.items()], [11 * cm, 5 * cm])
    table("Automated checks", [["Check", "Result"]] + [[k, "PASS" if ok else "FAIL"] for k, ok in v.get("checks", {}).items()],
          [12 * cm, 4 * cm])
    h2("9.1 Explanation of count differences")
    bullets([escape(n) for n in v.get("notes", [])] or ["(run verify.py)"])
    h2("9.2 Validation testing, database consistency, repeated execution")
    P("Validation testing: the five deliberately invalid records are detected (section 5.5) and pytest tests in tests/ "
      "check additional invalid cases. Database consistency: SQL queries show zero duplicates, zero NULLs in required "
      "columns, zero orphan rows and integrity_check = ok. Repeated execution: section 7.4.")
    h2("9.3 Encountered errors and how they were handled")
    bullets(["<b>Docker engine not running.</b> The first <font face='Courier'>docker compose up -d</font> failed with "
             "'failed to connect to the docker API'. Docker Desktop had not been started; after it showed 'Engine running' "
             "the command succeeded and the broker container sis2-kafka started.",
             "<b>Wrong activation command on Windows.</b> The Linux command <font face='Courier'>source .venv/bin/activate</font> "
             "is not recognised in PowerShell; the correct command is <font face='Courier'>.venv\\Scripts\\Activate.ps1</font>.",
             "<b>requirements.txt not found.</b> pip was started in the wrong (outer) folder. The commands must be run in the "
             "project folder that contains main.py and requirements.txt.",
             "<b>Idempotence PID warning.</b> Right after the topic was created the producer printed 'Failed to acquire idempotence "
             "PID ... Coordinator load in progress'. The client retried automatically and all messages were delivered (failed=0).",
             "<b>Messages of older runs in the topic.</b> Repeated runs leave old messages in weather_raw. The run_id header lets "
             "the consumer skip them (counter 'skipped'), so the counts of the current run stay exact.",
             "<b>Deliberately invalid records.</b> The five test records are rejected by Pandera and quarantined, which is the "
             "expected behaviour and not an error of the pipeline."])
    P("The code handles broker, transmission and processing errors as described in section 4.6; the log file "
      "logs/pipeline.log keeps every warning and error of the last run.")
    code("logs/pipeline.log (excerpt)", log_lines("VALID=", "PIPELINE", "Pipeline finished"))
    shots = sorted((ROOT / "report" / "screenshots").glob("*.png")) if (ROOT / "report" / "screenshots").exists() else []
    if shots:
        h2("9.4 Screenshots of the execution")
        for shot in shots:
            figure(shot, "Screenshot: " + shot.stem.replace("_", " ").replace("-", " "), 15 * cm)


def conclusions():
    h1("10. Conclusions")
    bullets(["Implemented: API collection with error handling, real Kafka producer/consumer with JSON, Pandera schema with "
             f"{len(RULES)} rules, pandas/NumPy cleaning, SQLite with keys and idempotent upserts, EDA with six figures, automatic verification.",
             "Main analytical findings: " + " ".join(escape(n) for n in M.get("eda", {}).get("findings", [])[:5]),
             "Technical challenges: keeping message order and run separation in Kafka (one partition + run_id header), "
             "detecting duplicates deterministically, making database loading repeatable (upsert + checksum).",
             "Limitations: only hourly forecast data for a few days; a single-node broker; one partition; imputation of wind speed "
             "is simple (city median); conclusions are not climatological."])


def contributions():
    h1("11. Individual Contributions")
    table("Contribution of each member", [["Member", "Components and responsibilities"]] +
          [[m["name"], m["contribution"]] for m in CFG["members"]], [4.5 * cm, 11.5 * cm])


def references():
    h1("12. References")
    bullets(["Open-Meteo Weather Forecast API documentation - https://open-meteo.com/en/docs",
             "Apache Kafka documentation - https://kafka.apache.org/documentation/",
             "confluent-kafka Python client - https://docs.confluent.io/kafka-clients/python/current/overview.html",
             "Pandera documentation - https://pandera.readthedocs.io/",
             "pandas documentation - https://pandas.pydata.org/docs/",
             "NumPy documentation - https://numpy.org/doc/",
             "SQLite documentation (UPSERT) - https://www.sqlite.org/lang_upsert.html",
             "Matplotlib - https://matplotlib.org/stable/ and Seaborn - https://seaborn.pydata.org/",
             "WMO weather interpretation codes (as listed in the Open-Meteo documentation)"])


def footer(canvas, doc):
    canvas.setFont("Helvetica", 8)
    canvas.drawCentredString(A4[0] / 2, 1.2 * cm, f"Page {doc.page}")


def main() -> int:
    if not M:
        print("data/pipeline_metrics.json not found. Run  python main.py  first.")
        return 1
    title_page()
    for section in (introduction, data_source, architecture, kafka, pandera_section, cleaning,
                    sqlite_section, eda, verification, conclusions, contributions, references):
        section()
        story.append(PageBreak())
    doc = SimpleDocTemplate(str(OUT), pagesize=A4, leftMargin=2 * cm, rightMargin=2 * cm,
                            topMargin=2 * cm, bottomMargin=2 * cm, title=CFG["project_title"])
    doc.build(story, onFirstPage=footer, onLaterPages=footer)
    print(f"Report written: {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
