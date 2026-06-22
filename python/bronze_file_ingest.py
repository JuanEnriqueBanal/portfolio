# Databricks notebook source
# MAGIC %md
# MAGIC ##STEP 1: IMPORTS + LIBRARIES

# COMMAND ----------

# =============================================================
# REQUIRED LIBRARIES
# =============================================================

try:
    import pandas as pd
    import openpyxl
except ImportError:
    %pip install pandas openpyxl
    dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ###Global Constants

# COMMAND ----------

from datetime import datetime
from collections import defaultdict
import io
import re

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
)

CONFIG_TABLE = "cw_ucc_services_archer_amr_cop_mckesson_d.control.file_ingestion_config"
LOG_TABLE = "cw_ucc_services_archer_amr_cop_mckesson_d.control.file_ingestion_log"
DEFAULT_CATALOG = "cw_ucc_services_archer_amr_cop_mckesson_d"
DEFAULT_SCHEMA = "bronze_core"
DEFAULT_PIPELINE_NAME = "bronze_file_ingest"

RUN_ID = f"INGEST_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

LOG_SCHEMA = StructType([
    StructField("run_id", StringType(), True),
    StructField("config_id", IntegerType(), True),
    StructField("source_file_name", StringType(), True),
    StructField("sheet_name", StringType(), True),
    StructField("target_catalog", StringType(), True),
    StructField("target_schema", StringType(), True),
    StructField("target_table", StringType(), True),
    StructField("load_frequency", StringType(), True),
    StructField("status", StringType(), True),
    StructField("row_count", IntegerType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("error_message", StringType(), True),
    StructField("created_at", TimestampType(), True),
])


# COMMAND ----------

# MAGIC %md ##2) Runtime parameter cell

# COMMAND ----------


# Choose one of: daily / weekly / monthly
# If you do not want frequency filtering for ad hoc runs, set RUN_FREQUENCY = None.

RUN_FREQUENCY = None

# Leave blank to run all files
FILE_TO_RUN = ""

# Example:
# FILE_TO_RUN = "CoStar_FF_Green_Building_Certifications.CSV"


# COMMAND ----------

dbutils.widgets.text("run_frequency", "")
RUN_FREQUENCY = dbutils.widgets.get("run_frequency").strip().lower()

if RUN_FREQUENCY == "":
    RUN_FREQUENCY = None

print(f"RUN_FREQUENCY = {RUN_FREQUENCY}")


# COMMAND ----------

# MAGIC %md
# MAGIC ##3) Load active config

# COMMAND ----------

config_df = (
    spark.table(CONFIG_TABLE)
    .filter(F.col("is_active") == True)
    .orderBy(F.col("load_order"))
)

configs = [row.asDict() for row in config_df.collect()]
print(f"Loaded active config rows: {len(configs)}")

# COMMAND ----------

required_fields = [
    "id",
    "source_file_name",
    "source_path",
    "file_type",
    "target_table",
    "target_schema",
    "target_catalog",
    "ingestion_strategy",
    "source_system",
    "pipeline_name"
]

invalid_configs = []

for cfg in configs:
    missing = [field for field in required_fields if not cfg.get(field)]
    if cfg.get("file_type", "").lower() in ["xlsx", "xlsm"] and not cfg.get("sheet_name"):
        missing.append("sheet_name")

    if missing:
        invalid_configs.append({
            "id": cfg.get("id"),
            "missing_fields": ", ".join(missing)
        })

if invalid_configs:
    for item in invalid_configs:
        print(f"❌ Invalid config ID {item['id']} → Missing: {item['missing_fields']}")
    raise Exception("Config validation failed. Fix invalid rows before execution.")
else:
    print("✅ Config validation passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##4) Helper: clean column names

# COMMAND ----------


def clean_columns(cols):
    clean_names = []
    seen = {}

    for col in cols:
        new_col = str(col).strip()

        # Replace special characters with underscore
        new_col = re.sub(r'[^A-Za-z0-9]', '_', new_col)

        # Remove duplicate underscores
        new_col = re.sub(r'_+', '_', new_col)

        # Remove leading/trailing underscores
        new_col = new_col.strip('_')

        # Lowercase
        new_col = new_col.lower()

        # Handle duplicate column names
        if new_col in seen:
            seen[new_col] += 1
            new_col = f"{new_col}_{seen[new_col]}"
        else:
            seen[new_col] = 0

        clean_names.append(new_col)

    return clean_names


# COMMAND ----------

# MAGIC %md
# MAGIC ##5) Helper: detect header row

# COMMAND ----------

def detect_header_row(df, max_scan_rows=15):
    """
    Dynamically detect the most likely header row by scoring the first N rows.
    Returns the zero-based row index of the best header candidate.
    """

    skip_phrases = [
        "extract from",
        "placeholder",
        "file path",
        "add note",
        "tbd",
        "note:",
        "as of",
        "filtered",
        "tab therein",
        "sharepoint",
        "budget file"
    ]

    def normalize_cell(v):
        if pd.isna(v):
            return ""
        return str(v).strip()

    def is_numeric_like(v):
        s = normalize_cell(v)
        if s == "":
            return False
        s = s.replace(",", "")
        s = s.replace("$", "")
        s = s.replace("%", "")
        s = s.replace("(", "-").replace(")", "")
        return bool(re.fullmatch(r"-?\d+(\.\d+)?", s))

    def is_text_like(v):
        s = normalize_cell(v)
        return any(ch.isalpha() for ch in s)

    def non_empty_values(row):
        return [normalize_cell(v) for v in row.tolist() if normalize_cell(v) != ""]

    def score_row(row_idx):
        row = df.iloc[row_idx]
        vals = non_empty_values(row)

        # Hard reject: completely blank
        if not vals:
            return float("-inf")

        row_text = " ".join(vals).lower()

        # Strong penalty for note/title/instruction rows
        penalty = 0
        if any(p in row_text for p in skip_phrases):
            penalty -= 50

        non_empty_count = len(vals)
        text_count = sum(is_text_like(v) for v in vals)
        numeric_count = sum(is_numeric_like(v) for v in vals)
        unique_count = len(set(v.lower() for v in vals if v != ""))

        # Header-like features
        score = 0
        score += non_empty_count * 4                  # more populated cells
        score += text_count * 3                       # headers are usually text
        score += (unique_count / max(non_empty_count, 1)) * 10

        # Penalize rows that look too numeric-heavy
        if numeric_count > text_count:
            score -= 8

        # Penalize very sparse rows (common for title/note rows)
        if non_empty_count <= 1:
            score -= 15
        elif non_empty_count == 2:
            score -= 5

        # Look ahead: next row should look more like data than header
        if row_idx + 1 < len(df):
            next_vals = non_empty_values(df.iloc[row_idx + 1])
            if next_vals:
                next_numeric = sum(is_numeric_like(v) for v in next_vals)
                next_text = sum(is_text_like(v) for v in next_vals)

                # Good signal: header row is text-ish, next row is mixed/data-ish
                if text_count >= 2 and next_numeric >= 1:
                    score += 8

                # Another good signal: next row has similar number of populated cells
                if abs(len(next_vals) - non_empty_count) <= 2:
                    score += 4

                # If next row is also pure note-like text, reduce confidence
                next_text_blob = " ".join(next_vals).lower()
                if any(p in next_text_blob for p in skip_phrases):
                    score -= 6

        score += penalty
        return score

    scan_limit = min(max_scan_rows, len(df))
    candidate_scores = {i: score_row(i) for i in range(scan_limit)}

    best_row = max(candidate_scores, key=candidate_scores.get)
    best_score = candidate_scores[best_row]

    # Fallback: if all scores are poor, use original first plausible row logic
    if best_score < 0:
        for idx, row in df.iterrows():
            vals = non_empty_values(row)
            if not vals:
                continue
            row_text = " ".join(vals).lower()
            if any(word in row_text for word in skip_phrases):
                continue
            return idx
        return 0

    return best_row


# COMMAND ----------

# MAGIC %md ##6) Helper: should_run

# COMMAND ----------

def should_run(cfg, run_frequency=None):
    """Filter configs by load_frequency when a runtime frequency is provided."""
    if not run_frequency:
        return True

    return (cfg.get("load_frequency") or "").lower() == str(run_frequency).lower()

# COMMAND ----------

# MAGIC %md ##7) Helper: write log

# COMMAND ----------

def write_log(cfg, start_time, end_time, status, row_count, error_message):
    """Write one execution record to the control log table."""
    log_row = [(
        RUN_ID,
        int(cfg.get("id") or 0),
        cfg.get("source_file_name"),
        cfg.get("sheet_name"),
        cfg.get("target_catalog") or DEFAULT_CATALOG,
        cfg.get("target_schema") or DEFAULT_SCHEMA,
        cfg.get("target_table"),
        cfg.get("load_frequency"),
        status,
        int(row_count or 0),
        start_time,
        end_time,
        error_message,
        datetime.now(),
    )]

    log_df = spark.createDataFrame(log_row, schema=LOG_SCHEMA)
    log_df.write.mode("append").saveAsTable(LOG_TABLE)

# COMMAND ----------

# MAGIC %md ##8) Helper: enrich metadata

# COMMAND ----------

def enrich_metadata(df, source_system, pipeline_name, sheet_name, file_name):
    """Append bronze metadata columns."""
    return (
        df
        .withColumn("bronze_ingest_ts_utc", F.current_timestamp())
        .withColumn("bronze_load_date", F.current_date())
        .withColumn("bronze_source_system", F.lit(source_system))
        .withColumn("bronze_pipeline_name", F.lit(pipeline_name))
        .withColumn("bronze_source_sheet", F.lit(sheet_name))
        .withColumn("bronze_source_file", F.lit(file_name))
    )

# COMMAND ----------

# MAGIC %md ##9) Helper: read CSV to Spark

# COMMAND ----------

def read_csv_to_spark(base_path, file_name, cfg):
    return (
        spark.read.format("csv")
        .option("header", cfg.get("header", True))
        .option("inferSchema", cfg.get("infer_schema", True))
        .option("delimiter", cfg.get("delimiter", ","))
        .load(base_path + file_name)
    )


# COMMAND ----------

# MAGIC %md ##10) Helper: read Excel to Spark

# COMMAND ----------

def read_excel_to_spark(spark_path, sheet_name):
    """Read Excel from UC Volume, detect header, preserve NA strings, keep blank cells as NULL."""

    binary_content = (
        spark.read.format("binaryFile")
        .load(spark_path)
        .select("content")
        .head()[0]
    )

    raw_pdf = pd.read_excel(
        io.BytesIO(binary_content),
        sheet_name=sheet_name,
        dtype=object,
        header=None,
        engine="openpyxl",
        keep_default_na=False,
        na_values=[]
    )

    # Drop only fully blank rows
    raw_pdf = raw_pdf.dropna(how="all").reset_index(drop=True)

    if raw_pdf.shape[0] == 0:
        raise Exception(f"No rows found in sheet -> {sheet_name}")

    header_row = detect_header_row(raw_pdf)
    print(f"Detected Header Row: Excel Row {header_row + 1}")

    headers = raw_pdf.iloc[header_row].tolist()

    pdf = raw_pdf.iloc[header_row + 1:].copy()
    pdf.columns = headers
    pdf = pdf.reset_index(drop=True)

    # Drop only fully blank data rows
    pdf = pdf.dropna(how="all").reset_index(drop=True)

    # Keep blank columns
    pdf.columns = clean_columns(pdf.columns)

    if pdf.shape[1] == 0:
        raise Exception(f"No columns found after cleaning -> {sheet_name}")

    if pdf.shape[0] == 0:
        raise Exception(f"No data found after cleaning -> {sheet_name}")

    # Convert whitespace-only strings to None
    pdf = pdf.replace(r'^\s*$', None, regex=True)

    # Handle NaN, NaT, None correctly
    for col in pdf.columns:
        pdf[col] = pdf[col].apply(
            lambda x: None if pd.isna(x) else str(x).strip()
        )

    # Force all columns to StringType
    spark_schema = StructType([
        StructField(col, StringType(), True)
        for col in pdf.columns
    ])

    return spark.createDataFrame(
        pdf.to_dict(orient="records"),
        schema=spark_schema
    )

# COMMAND ----------

# MAGIC %md ##11) Main ingestion function

# COMMAND ----------

def ingest_file(cfg):
    start_time = datetime.now()
    end_time = None
    row_count = 0
    status = "SUCCESS"
    error_message = None

    # ----- Config values -----
    base_path = cfg["source_path"]
    file_name = cfg["source_file_name"]
    file_type = (cfg.get("file_type") or "").lower()
    sheet_name = cfg.get("sheet_name")
    source_system = cfg.get("source_system")
    pipeline_name = cfg.get("pipeline_name") or DEFAULT_PIPELINE_NAME

    catalog = cfg.get("target_catalog") or DEFAULT_CATALOG
    target_schema = cfg.get("target_schema") or DEFAULT_SCHEMA
    target_table_name = cfg["target_table"]

    spark_path = "dbfs:" + base_path + file_name
    full_target_table = f"{catalog}.{target_schema}.{target_table_name}"

    print(f"\n📂 Processing: {file_name} | Sheet: {sheet_name}")
    print(f"🎯 Target: {full_target_table}")

    try:
        # ----- Read source -----
        if file_type == "csv":
            df = read_csv_to_spark(base_path, file_name, cfg)

        elif file_type in ["xlsx", "xlsm"]:
            df = read_excel_to_spark(spark_path, sheet_name)

        else:
            raise Exception(f"Unsupported file type: {file_type}")

        # =====================================================
        # CLEAN COLUMN NAMES FOR DELTA COMPATIBILITY
        # =====================================================
        old_columns = df.columns
        new_columns = clean_columns(df.columns)

        if old_columns != new_columns:
            print("🔄 Renaming columns:")
            for old, new in zip(old_columns, new_columns):
                if old != new:
                    print(f"   {old} -> {new}")

        df = df.toDF(*new_columns)

        # ----- Count rows before enrich/write -----
        row_count = df.count()

        # ----- Metadata -----
        df = enrich_metadata(
            df,
            source_system,
            pipeline_name,
            sheet_name,
            file_name
        )

        # ----- Write -----
        strategy = (cfg.get("ingestion_strategy") or "overwrite").lower()

        if strategy == "overwrite":
            (
                df.write
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(full_target_table)
            )

        elif strategy == "append":
            (
                df.write
                .mode("append")
                .saveAsTable(full_target_table)
            )

        else:
            raise Exception(f"Invalid ingestion_strategy: {strategy}")

        print(f"✅ SUCCESS → {full_target_table} | Rows: {row_count}")

    except Exception as e:
        status = "FAILED"
        error_message = str(e)
        print(f"❌ ERROR → {file_name} | Sheet: {sheet_name}")
        print(error_message)

    finally:
        end_time = datetime.now()
        write_log(cfg, start_time, end_time, status, row_count, error_message)

# COMMAND ----------

# MAGIC %md ##STEP 5: EXECUTE

# COMMAND ----------



selected_configs = [
    cfg
    for cfg in configs
    if should_run(cfg, RUN_FREQUENCY)
    and (
        not FILE_TO_RUN
        or cfg["source_file_name"] == FILE_TO_RUN
    )
]

print(f"Configs selected for this run: {len(selected_configs)}")

for cfg in selected_configs:
    ingest_file(cfg)