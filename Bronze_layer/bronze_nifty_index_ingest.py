# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Bronze Nifty 50 Index Ingest
# MAGIC %md
# MAGIC # Bronze — NSE Nifty 50 Index Ingest
# MAGIC
# MAGIC Downloads daily Nifty 50 index OHLCV data from UC Volume (uploaded by GitHub Actions at 6 PM IST) and writes to `workspace.bronze_swing.nifty_index`. Idempotent — safe to re-run for any date. Includes smart date detection, data validation, audit trail, and quality checks.

# COMMAND ----------

# DBTITLE 1,Imports + Shared Config
# ── IMPORTS + SHARED CONFIG ──────────────────────────────────
import pathlib

_config_path = pathlib.Path("/Workspace/Users/chandrakanthab@gmail.com/Swing-Institutional-Alpha/shared/config.py")
_exec_ns = globals()
exec(compile(_config_path.read_text(), str(_config_path), "exec"), _exec_ns)

import pandas as pd
import io
from datetime import datetime, date, timedelta, timezone
from typing import Optional, Tuple, Set
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, LongType, StringType, BooleanType, TimestampType
from delta.tables import DeltaTable
from pyspark.sql import Row

print(f"\u2705 Shared config loaded")
print(f"   Table: {T_NIFTY_INDEX}")

# COMMAND ----------

# DBTITLE 1,Schema Setup
# ── SCHEMA SETUP ──────────────────────────────────────────────

def setup_schema():
    """Create bronze_swing.nifty_index table if it doesn't exist."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_BRONZE}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {T_NIFTY_INDEX} (
            trade_date      DATE        NOT NULL COMMENT 'NSE trading date (partition key)',
            index_name      STRING      NOT NULL COMMENT 'Index name (e.g. NIFTY 50)',
            open            DOUBLE      NOT NULL COMMENT 'Opening index value',
            high            DOUBLE      NOT NULL COMMENT 'Intraday high',
            low             DOUBLE      NOT NULL COMMENT 'Intraday low',
            close           DOUBLE      NOT NULL COMMENT 'Closing index value',
            prev_close      DOUBLE               COMMENT 'Previous trading day close',
            change          DOUBLE               COMMENT 'Absolute change',
            change_pct      DOUBLE               COMMENT 'Percentage change',
            volume          BIGINT               COMMENT 'Total volume',
            turnover_lacs   DOUBLE               COMMENT 'Turnover in lakhs',
            day_range_pct   DOUBLE               COMMENT 'Intraday range as % of close',
            is_bullish      BOOLEAN              COMMENT 'Close > Open',
            _source_file    STRING               COMMENT 'Source CSV filename',
            _load_ts        TIMESTAMP           COMMENT 'Load timestamp',
            _batch_id       STRING               COMMENT 'Batch identifier'
        )
        USING DELTA
        PARTITIONED BY (trade_date)
        COMMENT 'Bronze — NSE Nifty 50 daily index OHLCV'
    """)
    print(f"\u2705 Schema: {SCHEMA_BRONZE}")
    print(f"\u2705 Table: {T_NIFTY_INDEX}")

# COMMAND ----------

# DBTITLE 1,UC Volume Loader + Transform
# ── UC VOLUME LOADER + TRANSFORM ──────────────────────────────
# Reads nifty50_DDMMYYYY.csv from UC Volume (uploaded by GitHub Actions)

VOLUME_NIFTY_PATH = "/Volumes/workspace/bronze_swing/deals_uploads"


def load_nifty_from_volume(trade_date: date) -> Tuple[Optional[pd.DataFrame], str]:
    """Load Nifty 50 CSV from UC Volume.

    File naming: nifty50_DDMMYYYY.csv
    Returns: (DataFrame, source_path) or (None, source_path) if not found.
    """
    date_str = trade_date.strftime("%d%m%Y")
    vol_path = f"{VOLUME_NIFTY_PATH}/nifty50_{date_str}.csv"

    try:
        sdf = spark.read.csv(vol_path, header=True, inferSchema=False)
        pdf = sdf.toPandas()

        if len(pdf) == 0:
            print(f"   [NIFTY] Volume: empty file at {vol_path}")
            return None, vol_path

        pdf.columns = [str(c).strip().upper() for c in pdf.columns]
        print(f"   [NIFTY] Volume loaded: {len(pdf)} rows from {vol_path}")
        return pdf, vol_path

    except Exception:
        print(f"   [NIFTY] Volume file not found: {vol_path}")
        return None, vol_path


def transform_nifty(raw_df: pd.DataFrame, trade_date: date, batch_id: str, source_url: str) -> pd.DataFrame:
    """Clean and enrich raw Nifty 50 index data.

    Expected CSV columns: TRADE_DATE, OPEN, HIGH, LOW, CLOSE, PREV_CLOSE, VOLUME, TURNOVER
    """
    df = raw_df.copy()
    print(f"   Transforming {len(df)} raw rows...")

    # Normalize column names (handle variants)
    COL_MAP = {
        "TRADE_DATE": "trade_date_raw",
        "TIMESTAMP": "trade_date_raw",
        "EOD_OPEN_INDEX_VAL": "open",
        "OPEN": "open",
        "EOD_HIGH_INDEX_VAL": "high",
        "HIGH": "high",
        "EOD_LOW_INDEX_VAL": "low",
        "LOW": "low",
        "EOD_CLOSE_INDEX_VAL": "close",
        "CLOSE": "close",
        "PREV_CLOSE": "prev_close",
        "PREVCLOSE": "prev_close",
        "VOLUME": "volume",
        "TURNOVER": "turnover_lacs",
        "TURNOVER_LACS": "turnover_lacs",
    }
    df = df.rename(columns={k: v for k, v in COL_MAP.items() if k in df.columns})

    # Filter to the target date if multiple rows
    if "trade_date_raw" in df.columns and len(df) > 1:
        target_str = trade_date.strftime("%d-%b-%Y")
        df_match = df[df["trade_date_raw"].astype(str).str.strip() == target_str]
        if len(df_match) > 0:
            df = df_match
        else:
            # Try DD-MM-YYYY format
            target_str2 = trade_date.strftime("%d-%m-%Y")
            df_match = df[df["trade_date_raw"].astype(str).str.strip() == target_str2]
            if len(df_match) > 0:
                df = df_match

    # Keep only the latest row if still multiple
    df = df.tail(1).copy()

    # Numeric conversion
    float_cols = ["open", "high", "low", "close", "prev_close", "turnover_lacs"]
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.strip().replace(["-", "NA", "N/A", ""], None), errors="coerce")
    if "volume" in df.columns:
        df["volume"] = pd.to_numeric(df["volume"].astype(str).str.strip().replace(["-", "NA", "N/A", ""], None), errors="coerce")

    # Fill missing values
    for col in float_cols + ["volume"]:
        if col not in df.columns:
            df[col] = None

    # Derived columns
    df["index_name"] = "NIFTY 50"
    df["change"] = (df["close"] - df["prev_close"]).round(2) if df["prev_close"].notna().iloc[0] else None
    df["change_pct"] = ((df["close"] - df["prev_close"]) / df["prev_close"] * 100).round(4) if df["prev_close"].notna().iloc[0] and df["prev_close"].iloc[0] != 0 else None
    df["day_range_pct"] = ((df["high"] - df["low"]) / df["close"] * 100).round(4) if df["close"].iloc[0] != 0 else None
    df["is_bullish"] = df["close"] > df["open"]
    df["trade_date"] = trade_date
    df["_source_file"] = source_url.rsplit("/", 1)[-1]
    df["_load_ts"] = datetime.now(timezone.utc)
    df["_batch_id"] = batch_id

    FINAL_COLS = [
        "trade_date", "index_name",
        "open", "high", "low", "close", "prev_close",
        "change", "change_pct", "volume", "turnover_lacs",
        "day_range_pct", "is_bullish",
        "_source_file", "_load_ts", "_batch_id",
    ]
    df = df[[c for c in FINAL_COLS if c in df.columns]]
    print(f"   \u2705 Transform: {len(df)} rows | close={df['close'].iloc[0]:.2f}" if len(df) > 0 and df['close'].notna().iloc[0] else f"   \u2705 Transform: {len(df)} rows")
    return df

# COMMAND ----------

# DBTITLE 1,Delta Writer + Audit
# ── DELTA WRITER + AUDIT ──────────────────────────────────────
# Idempotent: DELETE partition → INSERT (replaces, never duplicates)

def write_to_delta_nifty(pdf: pd.DataFrame, trade_date: date) -> int:
    """Write pandas DF to Bronze Delta table. Returns rows written."""
    sdf = spark.createDataFrame(pdf)
    sdf = sdf.withColumn("trade_date", F.to_date(F.col("trade_date").cast("string")))
    rows = sdf.count()

    if not spark.catalog.tableExists(T_NIFTY_INDEX):
        (sdf.write.format("delta").mode("overwrite")
            .partitionBy("trade_date").option("overwriteSchema", "true")
            .saveAsTable(T_NIFTY_INDEX))
        print(f"   \u2705 Delta table created. Written {rows} rows.")
        return rows

    existing = spark.sql(
        f"SELECT COUNT(*) AS n FROM {T_NIFTY_INDEX} WHERE trade_date = '{trade_date}'"
    ).collect()[0]["n"]
    if existing > 0:
        print(f"   \U0001f504 Re-run: deleting {existing} existing rows for {trade_date}")
        dt = DeltaTable.forName(spark, T_NIFTY_INDEX)
        dt.delete(F.col("trade_date") == F.lit(str(trade_date)).cast(DateType()))

    (sdf.write.format("delta").mode("append").saveAsTable(T_NIFTY_INDEX))
    print(f"   \u2705 Written {rows} rows for {trade_date}")
    return rows


def write_nifty_audit(trade_date: date, status: str, batch_id: str,
                       source_url: str = "", rows_loaded: int = 0, error_msg: str = ""):
    """Append one row to the audit table. Best-effort."""
    try:
        row = Row(
            trade_date=trade_date, load_ts=datetime.now(timezone.utc), status=status,
            source_url=source_url, rows_raw=0, rows_series_filter=0,
            rows_loaded=rows_loaded, download_secs=0.0,
            error_msg=error_msg[:1000] if error_msg else "", batch_id=batch_id,
        )
        adf = spark.createDataFrame([row])
        adf = adf.withColumn("trade_date", F.to_date(F.col("trade_date").cast("string")))
        adf.write.format("delta").mode("append").saveAsTable(T_LOAD_AUDIT)
    except Exception as e:
        print(f"   \u274c Failed to write audit: {e}")

# COMMAND ----------

# DBTITLE 1,Load Orchestration
# ── LOAD ORCHESTRATION ────────────────────────────────
# Smart date detection, force=True reload, idempotent writes

def ingest_nifty_for_date(trade_date: date, batch_id: str, force: bool = True) -> str:
    """Load Nifty 50 index data for one date from UC Volume.

    Returns: SUCCESS / SKIP_WEEKEND / HOLIDAY / FAILED
    """
    ds = trade_date.isoformat()

    if trade_date.weekday() >= 5:
        print(f"   \u23ed  {ds} Saturday/Sunday — skip")
        return "SKIP_WEEKEND"

    print(f"\n{'\u2500'*55}")
    print(f"\U0001f4c5 Nifty index ingestion: {ds}")
    print(f"{'\u2500'*55}")

    try:
        raw_df, source_path = load_nifty_from_volume(trade_date)
        if raw_df is None or len(raw_df) == 0:
            print(f"\U0001f4c6 {ds} — no data in Volume (holiday or not yet uploaded)")
            write_nifty_audit(trade_date, "HOLIDAY", batch_id, source_path)
            return "HOLIDAY"

        clean_df = transform_nifty(raw_df, trade_date, batch_id, source_path)

        # Validate critical fields
        if clean_df["close"].isna().iloc[0] or clean_df["close"].iloc[0] <= 0:
            raise ValueError(f"Invalid close price: {clean_df['close'].iloc[0]}")

        rows_loaded = write_to_delta_nifty(clean_df, trade_date)
        write_nifty_audit(trade_date, "SUCCESS", batch_id, source_path, rows_loaded)
        print(f"\u2705 {ds} — SUCCESS ({rows_loaded} rows)")
        return "SUCCESS"

    except Exception as e:
        print(f"\u274c {ds} — FAILED: {e}")
        write_nifty_audit(trade_date, "FAILED", batch_id, "", 0, str(e))
        return "FAILED"


def get_effective_trading_date_nifty() -> date:
    """Find the most recent COMPLETED trading day whose data is available.

    Rules:
      - Trading day + after 4:00 PM IST → today (data published)
      - Trading day + before 4:00 PM IST → last trading day
      - Weekend / holiday → last trading day
    """
    import pytz
    ist = pytz.timezone("Asia/Kolkata")
    now_ist = datetime.now(ist)
    today = now_ist.date()

    market_data_available = now_ist.hour >= 16  # 4:00 PM IST

    if is_trading_day(today) and market_data_available:
        return today

    effective = today - timedelta(days=1)
    while not is_trading_day(effective):
        effective -= timedelta(days=1)
    return effective


def run_daily_nifty_load():
    """Daily entry point. Loads the most recent COMPLETED trading day's Nifty data.

    Always reloads (force=True) — deletes existing data and rewrites. No duplicates.
    """
    import pytz
    ist = pytz.timezone("Asia/Kolkata")
    now_ist = datetime.now(ist)
    today = now_ist.date()
    effective_date = get_effective_trading_date_nifty()

    print(f"\U0001f504 Daily Nifty index load — {today} ({now_ist.strftime('%H:%M IST')})")

    if effective_date == today:
        print(f"   Today is a trading day, market closed → loading today's data")
    else:
        if not is_trading_day(today):
            reason = "weekend" if today.weekday() >= 5 else f"holiday ({NSE_HOLIDAYS.get(today, '?')})"
            print(f"   Today is {reason} → loading last trading day's data")
        else:
            print(f"   Today is a trading day but market still open → loading last completed trading day")

    print(f"   \U0001f4c5 Effective date: {effective_date}")

    batch_id = f"nifty_daily_{today.strftime('%Y%m%d')}_{now_ist.strftime('%H%M%S')}"
    status = ingest_nifty_for_date(effective_date, batch_id, force=True)

    if status == "SUCCESS":
        print(f"\n\U0001f527 Optimizing partition {effective_date}...")
        spark.sql(f"OPTIMIZE {T_NIFTY_INDEX} WHERE trade_date = '{effective_date}'")
        run_nifty_quality_check(effective_date)

# COMMAND ----------

# DBTITLE 1,Data Quality Checks
# ── DATA QUALITY CHECKS ───────────────────────────────

def run_nifty_quality_check(trade_date: date = None) -> bool:
    """Validate Nifty 50 data for a given date."""
    if trade_date is None:
        row = spark.sql(f"SELECT MAX(trade_date) AS d FROM {T_NIFTY_INDEX}").collect()[0]
        if row["d"] is None:
            print("\u274c Table is empty")
            return False
        trade_date = row["d"]

    print(f"\n\U0001f50d Nifty Quality Check — {trade_date}")
    print(f"{'\u2500'*50}")

    row = spark.sql(f"""
        SELECT
            index_name,
            open,
            high,
            low,
            close,
            prev_close,
            change_pct,
            volume,
            turnover_lacs,
            CASE WHEN high < low THEN 1 ELSE 0 END AS inverted_hl,
            CASE WHEN close <= 0 THEN 1 ELSE 0 END AS zero_close
        FROM {T_NIFTY_INDEX}
        WHERE trade_date = '{trade_date}'
    """).collect()[0]

    all_pass = True
    checks = [
        ("Index name",       row["index_name"] == "NIFTY 50", True),
        ("Close > 0",        row["zero_close"] == 0, True),
        ("High >= Low",      row["inverted_hl"] == 0, True),
        ("Open not null",    row["open"] is not None, True),
        ("High not null",    row["high"] is not None, True),
        ("Low not null",     row["low"] is not None, True),
    ]

    for name, ok, critical in checks:
        icon = "\u2705" if ok else ("\u274c" if critical else "\u26a0")
        print(f"   {icon} {name:<25}")
        if critical and not ok:
            all_pass = False

    print(f"\n   Close  : {row['close']}")
    print(f"   Change : {row['change_pct']}%")
    print(f"   Volume : {row['volume']}")
    print(f"{'\u2500'*50}")
    print("\u2705 All critical checks passed." if all_pass else "\u274c Critical check(s) FAILED.")
    return all_pass

# COMMAND ----------

# DBTITLE 1,Main Execution
# ── MAIN EXECUTION ────────────────────────────────────────────
# Choose the mode for this run. Uncomment exactly ONE block.

# \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
# MODE A — FIRST TIME SETUP (run ONCE)
# setup_schema()
#run_daily_nifty_load()
# \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550

# \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
# MODE B — DAILY SCHEDULED JOB
run_daily_nifty_load()
# \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550

# \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
# MODE C — MANUAL RECOVERY (specific date re-load)
# recover_date = date(2026, 9, 18)
# batch_id = f"nifty_recovery_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
# ingest_nifty_for_date(recover_date, batch_id, force=True)
# run_nifty_quality_check(recover_date)
# \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550

# \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
# MODE D — INSPECT BRONZE TABLE
# \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
print("=" * 60)
print("\U0001f4ca Nifty 50 Index — Bronze Table Status")
print("=" * 60)

# Check if table exists
if spark.catalog.tableExists(T_NIFTY_INDEX):
    count = spark.sql(f"SELECT COUNT(*) AS n FROM {T_NIFTY_INDEX}").collect()[0]["n"]
    print(f"  Total rows: {count}")
    if count > 0:
        spark.sql(f"""
            SELECT trade_date, index_name, open, high, low, close,
                   change_pct, volume, turnover_lacs
            FROM {T_NIFTY_INDEX}
            ORDER BY trade_date DESC
            LIMIT 15
        """).show(truncate=False)
        run_nifty_quality_check()
    else:
        print("  Table exists but is empty. Run MODE A to load data.")
else:
    print("  Table does not exist. Run MODE A (setup_schema()) to create it.")