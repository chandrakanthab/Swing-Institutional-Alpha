# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Bronze NSE Bulk Block Deals Ingest
# MAGIC %md
# MAGIC # Bronze — NSE Bulk & Block Deals Ingest
# MAGIC
# MAGIC Downloads daily NSE Bulk Deals + Block Deals and writes to `workspace.bronze_swing.nse_bulk_block_deals`. Classifies each client name as MF/FII_FPI/DII/PROMOTER/HNI/ALGO/OTHER using keyword matching (LLM enhancement via Gemini Flash stored in `client_name_registry` — future enhancement). Idempotent, with audit trail and quality checks.

# COMMAND ----------

# DBTITLE 1,Imports + Shared Config
# ── IMPORTS + SHARED CONFIG ──────────────────────────────────
import pathlib

_config_path = pathlib.Path("/Workspace/Users/chandrakanthab@gmail.com/Swing-Institutional-Alpha/shared/config.py")
_exec_ns = globals()
exec(compile(_config_path.read_text(), str(_config_path), "exec"), _exec_ns)

import requests
import pandas as pd
import io
import re
import time
import random
import logging
from datetime import datetime, date, timedelta
from typing import Optional, Tuple, Set, List

from pyspark.sql import functions as F, Row
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, DateType, TimestampType, BooleanType
)
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bronze.deals")

print("✅ Imports + shared config loaded")

# COMMAND ----------

# DBTITLE 1,Unity Catalog Schema Setup
# ── SCHEMA SETUP ──────────────────────────────────────────────
# Creates deals table, audit table, and client_name_registry. Safe to re-run.

def setup_schemas():
    """Create UC tables for bulk/block deals + audit + client registry."""

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_BRONZE}")
    print(f"✅ Schema: {SCHEMA_BRONZE}")

    # ── Main deals table ───────────────────────────────────────────────────
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {T_BULK_BLOCK_DEALS} (
            trade_date          DATE        NOT NULL COMMENT 'NSE trading date (partition key)',
            deal_type           STRING      NOT NULL COMMENT 'BULK or BLOCK',
            symbol              STRING      NOT NULL COMMENT 'NSE ticker symbol',
            security_name       STRING      COMMENT 'Full company name from NSE',
            client_name         STRING      COMMENT 'Entity that traded (raw from NSE)',
            client_type         STRING      COMMENT 'MF / FII_FPI / DII / PROMOTER / HNI / ALGO / OTHER',
            is_institutional    BOOLEAN     COMMENT 'True if MF / FII_FPI / DII',
            buy_sell            STRING      NOT NULL COMMENT 'BUY or SELL',
            quantity            LONG        COMMENT 'Number of shares in deal',
            trade_price         DOUBLE      COMMENT 'Deal execution price (INR)',
            remarks             STRING      COMMENT 'Additional NSE notes',
            turnover_cr         DOUBLE      COMMENT 'Deal value in crores = qty * price / 1e7',
            _source_file        STRING      COMMENT 'CSV filename downloaded',
            _load_ts            TIMESTAMP   COMMENT 'UTC load timestamp',
            _batch_id           STRING      COMMENT 'Batch run UUID for lineage'
        )
        USING DELTA
        PARTITIONED BY (trade_date)
        COMMENT 'NSE Bulk + Block Deals — Bronze. Institutional footprint scoring in Silver.'
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact'   = 'true',
            'delta.enableChangeDataFeed'        = 'true'
        )
    """)
    print(f"✅ Table: {T_BULK_BLOCK_DEALS}")

    # ── Audit table ─────────────────────────────────────────────────────────
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {T_DEALS_AUDIT} (
            trade_date          DATE        NOT NULL,
            load_ts             TIMESTAMP   NOT NULL,
            status              STRING      NOT NULL COMMENT 'SUCCESS / NO_DEALS / HOLIDAY / FAILED / SKIP',
            deal_type           STRING      COMMENT 'BULK / BLOCK / BOTH',
            bulk_rows           LONG        COMMENT 'Rows from bulk deals file',
            block_rows          LONG        COMMENT 'Rows from block deals file',
            total_rows          LONG        COMMENT 'Total rows written to Delta',
            error_msg           STRING,
            batch_id            STRING
        )
        USING DELTA
        COMMENT 'Load audit for bulk/block deals ingestion'
        TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
    """)
    print(f"✅ Table: {T_DEALS_AUDIT}")

    # ── Client name registry (for LLM classification caching) ────────────────
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {T_CLIENT_REGISTRY} (
            client_name         STRING      NOT NULL COMMENT 'Raw client name from NSE (normalized upper)',
            client_type         STRING      NOT NULL COMMENT 'MF / FII_FPI / DII / PROMOTER / HNI / ALGO / OTHER',
            is_institutional    BOOLEAN     NOT NULL COMMENT 'True if MF / FII_FPI / DII',
            classification_method STRING    COMMENT 'KEYWORD / LLM / MANUAL',
            confidence          DOUBLE      COMMENT 'LLM confidence score (0-1)',
            reasoning           STRING      COMMENT 'LLM reasoning text',
            first_seen_date     DATE        COMMENT 'First date this name appeared in deals',
            last_updated        TIMESTAMP   COMMENT 'UTC timestamp of last classification',
            _batch_id           STRING      COMMENT 'Batch that classified this entry'
        )
        USING DELTA
        COMMENT 'Client name classification registry — keyword + LLM classified'
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.enableChangeDataFeed'        = 'true'
        )
    """)
    print(f"✅ Table: {T_CLIENT_REGISTRY}")
    print("\n✅ Schema setup complete. Safe to re-run.")

setup_schemas()

# COMMAND ----------

# DBTITLE 1,Client Type Classifier
# ── CLIENT TYPE CLASSIFIER ────────────────────────────────────
# Keyword-based classification. LLM (Gemini Flash) enhancement will be added
# for names not caught by keywords — stored in client_name_registry table.

_MF_KEYWORDS = [
    "MUTUAL FUND", "MF-", "-MF-", " MF ", "SCHEME",
    "PARAG PARIKH", "MIRAE ASSET", "EDELWEISS MF", "QUANT MF",
    "GROWW MF", "MOTILAL OSWAL MF", "DSP MF", "NIPPON MF",
    "ADITYA BIRLA SL", "FRANKLIN TEMPLETON", "INVESCO MF",
    "KOTAK MF", "AXIS MF", "HDFC MF", "ICICI PRU MF",
    "SBI MF", "TATA MF", "UTI MF", "PPFAS",
    "BANDHAN MF", "NAVI MF", "WHITE OAK",
    "ASSET MANAGEMENT", "TRUSTEESHIP", "PGIM", "JM FINANCIAL MF",
    "BARODA BNP", "ITI MUTUAL", "QUANTUM MF", "MAHINDRA MANULIFE",
    "SAMCO MF", "ZERODHA MF", "360 ONE",
]

_FII_FPI_KEYWORDS = [
    "MORGAN STANLEY", "GOLDMAN SACHS", "MERRILL LYNCH", "JP MORGAN",
    "CITIGROUP", "CITI BANK", "DEUTSCHE BANK", "NOMURA", "MACQUARIE",
    "CLSA", "UBS AG", "HSBC", "BARCLAYS", "CREDIT SUISSE",
    "BNP PARIBAS", "SOCIETE GENERALE", "ABU DHABI", "GIC PTE",
    "TEMASEK", "VANGUARD", "BLACKROCK", "FIDELITY", "T. ROWE",
    "ABERDEEN", "MATTHEWS ASIA", "ARTISAN PARTNERS",
    "CANADA PENSION", "CALIFORNIA STF",
    "FII", "FPI", "FOREIGN PORTFOLIO",
    "MAURITIUS", "SINGAPORE PTE", "CAYMAN ISLANDS",
    "AIF", "GOVERNMENT OF", "SOVEREIGN",
    "TRUST COMPANY AS TRUSTEE", "WASATCH", "SANDS CAPITAL",
    "NORGES BANK", "OVERSEAS", "METZLER", "CARNELIAN",
    "STATE STREET", "NORTHERN TRUST", "BROWN BROTHERS",
    "PUBLIC SECTOR PENSION",
]

_DII_KEYWORDS = [
    "LIFE INSURANCE CORPORATION", "LIC OF INDIA",
    "SBI LIFE INSURANCE", "HDFC LIFE", "ICICI PRUDENTIAL LIFE",
    "BAJAJ ALLIANZ LIFE", "MAX LIFE", "TATA AIA",
    "NEW INDIA ASSURANCE", "NATIONAL INSURANCE",
    "UNITED INDIA INSURANCE", "ORIENTAL INSURANCE",
    "GENERAL INSURANCE", "STAR UNION DAI-ICHI",
    "NATIONAL PENSION", "NPS TRUST", "EPFO",
    "EMPLOYEES PROVIDENT", "GRATUITY FUND",
    "INSURANCE COMPANY", "LIFE INSURANCE", "INSURANCE",
    "KOTAK MAHINDRA LIFE", "RELIANCE NIPPON LIFE",
    "PNB METLIFE", "STAR HEALTH", "HDFC ERGO",
    "ICICI LOMBARD", "SBI GENERAL", "TATA AIG",
    "IFFCO TOKIO", "ROYAL SUNDARAM", "EDELWEISS LIFE",
    "PENSION FUND", "PENSION", "PROVIDENT",
    "SOCIAL SECURITY", "SOCIAL INSURANCE", "SUPER ANNUATION",
]

_PROMOTER_KEYWORDS = [
    "PROMOTER", "PROMOTER GROUP", "FOUNDER",
    "MANAGING DIRECTOR", " MD ", "CHAIRMAN",
    "PROMOTER HOLDING", "FAMILY TRUST", "FAMILY OFFICE",
]

_HNI_KEYWORDS = [
    "JHUNJHUNWALA", "REKHA JHUNJHUNWALA", "DAMANI",
    "RADHAKISHAN DAMANI", "KACHOLIA", "ASHISH KACHOLIA",
    "DOLLY KHANNA", "VIJAY KEDIA", "PORINJU VELIYATH",
    "NIKHIL KAMATH",
]

_ALGO_KEYWORDS = [
    "PROPRIETARY", "PROP ", "ALGORITHMIC",
    "JANE STREET", "CITADEL", "RENAISSANCE", "TWO SIGMA",
    "OPTIVER", "SUSQUEHANNA",
]


def classify_client(raw_name: str) -> Tuple[str, bool]:
    """Classify a client name into a type category.

    Returns: (client_type, is_institutional)
    Priority: MF → FII_FPI → DII → PROMOTER → HNI → ALGO → OTHER
    """
    if not raw_name or pd.isna(raw_name) or str(raw_name).strip() in ("", "-", "NA"):
        return "UNKNOWN", False

    name = str(raw_name).strip().upper()

    for kw in _MF_KEYWORDS:
        if kw.upper() in name:
            return "MF", True
    for kw in _FII_FPI_KEYWORDS:
        if kw.upper() in name:
            return "FII_FPI", True
    for kw in _DII_KEYWORDS:
        if kw.upper() in name:
            return "DII", True
    for kw in _PROMOTER_KEYWORDS:
        if kw.upper() in name:
            return "PROMOTER", False
    for kw in _HNI_KEYWORDS:
        if kw.upper() in name:
            return "HNI", False
    for kw in _ALGO_KEYWORDS:
        if kw.upper() in name:
            return "ALGO", False
    return "OTHER", False


# ── Unit tests ──────────────────────────────────────────────────
_test_cases = [
    ("HDFC MUTUAL FUND - GROWTH SCHEME",        "MF",       True),
    ("MORGAN STANLEY MAURITIUS COMPANY LIMITED", "FII_FPI",  True),
    ("LIFE INSURANCE CORPORATION OF INDIA",      "DII",      True),
    ("PROMOTER GROUP HOLDINGS LIMITED",          "PROMOTER", False),
    ("ASHISH KACHOLIA HUF",                       "HNI",      False),
    ("",                                         "UNKNOWN",  False),
    ("-",                                        "UNKNOWN",  False),
    ("SOME RANDOM RETAIL INVESTOR",              "OTHER",    False),
    ("MIRAE ASSET LARGE CAP FUND",               "MF",       True),
    ("ABU DHABI INVESTMENT AUTHORITY",           "FII_FPI",  True),
]
_all_pass = True
for _name, _exp_type, _exp_inst in _test_cases:
    _got_type, _got_inst = classify_client(_name)
    if _got_type != _exp_type or _got_inst != _exp_inst:
        _all_pass = False
        print(f"❌ CLASSIFY FAIL: '{_name}' → got ({_got_type},{_got_inst}), expected ({_exp_type},{_exp_inst})")
if _all_pass:
    print(f"✅ All {len(_test_cases)} classifier tests passed")

# COMMAND ----------

# DBTITLE 1,NSE Downloader (Bulk + Block)
# ── DBFS LOADER (BULK + BLOCK) ───────────────────────────────
# Reads CSV files uploaded by GitHub Actions at 6 PM IST
# NSE HTTP download removed — NSE blocks serverless IPs.
# GitHub Actions script: scripts/nse_bulk_deals_downloader.py

# NSE bulk/block CSV column name variants → our schema
BULK_COL_MAP = {
    "SYMBOL": "symbol", "SECURITY NAME": "security_name", "CLIENT NAME": "client_name",
    "BUY / SELL": "buy_sell", "BUY/SELL": "buy_sell", "BUYSELL": "buy_sell",
    "QUANTITY TRADED": "quantity", "QUANTITY_TRADED": "quantity", "QUANTITY": "quantity",
    "TRADE PRICE": "trade_price", "TRADE_PRICE": "trade_price",
    "WGTD. AVG. PRICE": "trade_price", "REMARKS": "remarks",
    "SCRIP NAME": "security_name", "SCRIP_NAME": "security_name",
    "CLIENT_NAME": "client_name", "SYMBOL": "symbol",
}

# NSE HTTP download functions removed — NSE blocks serverless IPs.
# Download is handled by GitHub Actions: scripts/nse_bulk_deals_downloader.py
# which uploads CSVs to UC Volume. This notebook reads from the volume only.


# ── UC VOLUME LOADER (files uploaded by GitHub Actions) ─────────
VOLUME_DEALS_PATH = "/Volumes/workspace/bronze_swing/deals_uploads"


def load_deals_from_volume(trade_date: date, deal_type: str) -> Tuple[Optional[pd.DataFrame], str]:
    """Load bulk/block deals CSV from UC Volume (uploaded by GitHub Actions).

    File naming: bulk_DDMMYYYY.csv / block_DDMMYYYY.csv
    Returns: (DataFrame, source_path) or (None, source_path) if file not found.
    """
    date_str = trade_date.strftime("%d%m%Y")
    prefix = "bulk" if deal_type == "BULK" else "block"
    vol_path = f"{VOLUME_DEALS_PATH}/{prefix}_{date_str}.csv"

    try:
        sdf = spark.read.csv(vol_path, header=True, inferSchema=False)
        pdf = sdf.toPandas()

        if len(pdf) == 0:
            print(f"   [{deal_type}] Volume: empty file at {vol_path}")
            return None, vol_path

        # Check for NSE "NO RECORDS" response
        if len(pdf) == 1 and str(pdf.iloc[0, 0]).strip().upper() in ("NO RECORDS", "NO RECORD"):
            print(f"   [{deal_type}] Volume: NO RECORDS at {vol_path}")
            return None, vol_path

        pdf.columns = [str(c).strip().replace("\n", " ").upper() for c in pdf.columns]
        print(f"   [{deal_type}] Volume loaded: {len(pdf)} rows from {vol_path}")
        return pdf, vol_path

    except Exception:
        print(f"   [{deal_type}] Volume file not found: {vol_path}")
        return None, vol_path

# COMMAND ----------

# DBTITLE 1,Data Transformation
# ── DATA TRANSFORMATION ────────────────────────────────────────

def clean_quantity(series: pd.Series) -> pd.Series:
    """Strip Indian number formatting: '1,50,000' → 150000."""
    return (series.astype(str).str.strip().str.replace(",", "", regex=False)
            .replace({"NA": None, "-": None, "": None})
            .pipe(pd.to_numeric, errors="coerce").astype("Int64"))

def clean_price(series: pd.Series) -> pd.Series:
    """Price may have commas: '2,850.50' → 2850.50."""
    return (series.astype(str).str.strip().str.replace(",", "", regex=False)
            .replace({"NA": None, "-": None, "": None})
            .pipe(pd.to_numeric, errors="coerce"))

def normalize_buy_sell(series: pd.Series) -> pd.Series:
    """Normalize 'B'/'Buy'/'BUY' → 'BUY', 'S'/'Sell'/'SELL' → 'SELL'."""
    mapping = {"B": "BUY", "BUY": "BUY", "Buy": "BUY",
               "S": "SELL", "SELL": "SELL", "Sell": "SELL"}
    return series.astype(str).str.strip().map(lambda x: mapping.get(x, x.upper()))

def map_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Flexible column mapper for NSE bulk/block deal CSV variants."""
    col_map = {}
    for raw_col in df.columns:
        clean = raw_col.strip().upper()
        if clean in BULK_COL_MAP:
            col_map[raw_col] = BULK_COL_MAP[clean]
            continue
        if "PRICE" in clean or "WGTD" in clean:
            col_map[raw_col] = "trade_price"
        elif "QUANTITY" in clean or "QTY" in clean:
            col_map[raw_col] = "quantity"
        elif "CLIENT" in clean or "ENTITY" in clean:
            col_map[raw_col] = "client_name"
        elif "BUY" in clean or "SELL" in clean or "SIDE" in clean:
            col_map[raw_col] = "buy_sell"
        elif "SYMBOL" in clean or "TICKER" in clean:
            col_map[raw_col] = "symbol"
        elif "SECURITY" in clean or "COMPANY" in clean or "NAME" in clean:
            col_map[raw_col] = "security_name"
        elif "REMARK" in clean or "NOTE" in clean:
            col_map[raw_col] = "remarks"
    return df.rename(columns=col_map)

def transform_deals(
    raw_df: pd.DataFrame, deal_type: str, trade_date: date, batch_id: str, source_url: str
) -> pd.DataFrame:
    """Transform raw NSE bulk/block deal CSV into clean schema."""
    df = raw_df.copy()
    df = map_columns(df)

    for col in ["symbol", "client_name", "buy_sell", "quantity", "trade_price"]:
        if col not in df.columns:
            df[col] = None

    df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    df["client_name"] = df["client_name"].astype(str).str.strip()
    df["security_name"] = df.get("security_name", pd.Series()).astype(str).str.strip()
    df["remarks"] = df.get("remarks", pd.Series()).astype(str).str.strip()

    df = df[df["symbol"].str.len() > 0].copy()
    df = df[~df["symbol"].isin(["NAN", "NONE", "NULL", ""])].copy()

    df["quantity"] = clean_quantity(df["quantity"])
    df["trade_price"] = clean_price(df["trade_price"])

    invalid = df["quantity"].isna() | (df["quantity"] <= 0)
    if invalid.sum() > 0:
        print(f"   ⚠️  Dropping {invalid.sum()} rows with zero/null quantity")
        df = df[~invalid].copy()

    df["buy_sell"] = normalize_buy_sell(df["buy_sell"])
    df = df[df["buy_sell"].isin(["BUY", "SELL"])].copy()

    classifications = df["client_name"].apply(classify_client)
    df["client_type"] = classifications.apply(lambda x: x[0])
    df["is_institutional"] = classifications.apply(lambda x: x[1])

    df["turnover_cr"] = (df["quantity"].astype(float) * df["trade_price"].fillna(0) / 1_00_00_000).round(2)

    n_before = len(df)
    df = df.drop_duplicates(subset=["symbol", "client_name", "buy_sell", "quantity", "trade_price"], keep="last")
    if len(df) < n_before:
        print(f"   ⚠️  Removed {n_before - len(df)} duplicate deal rows")

    df["trade_date"] = trade_date
    df["deal_type"] = deal_type
    df["_source_file"] = source_url.rsplit("/", 1)[-1]
    df["_load_ts"] = datetime.utcnow()
    df["_batch_id"] = batch_id

    FINAL_COLS = [
        "trade_date", "deal_type", "symbol", "security_name",
        "client_name", "client_type", "is_institutional",
        "buy_sell", "quantity", "trade_price", "turnover_cr",
        "remarks", "_source_file", "_load_ts", "_batch_id",
    ]
    return df[[c for c in FINAL_COLS if c in df.columns]]

# COMMAND ----------

# DBTITLE 1,Delta Writer + Audit
# ── DELTA WRITER + AUDIT ──────────────────────────────────────

def write_deals_to_delta(df: pd.DataFrame, trade_date: date) -> int:
    """Idempotent write: delete existing date partition → insert. Returns rows written."""
    if len(df) == 0:
        print(f"   ℹ️  Zero rows for {trade_date} — OK (no deals today)")
        return 0

    sdf = spark.createDataFrame(df)
    sdf = sdf.withColumn("trade_date", F.to_date(F.col("trade_date").cast("string")))
    rows = sdf.count()

    if not spark.catalog.tableExists(T_BULK_BLOCK_DEALS):
        (sdf.write.format("delta").mode("overwrite")
            .partitionBy("trade_date").option("overwriteSchema", "true")
            .saveAsTable(T_BULK_BLOCK_DEALS))
        print(f"   ✅ Delta table created. Written {rows} rows.")
        return rows

    dt = DeltaTable.forName(spark, T_BULK_BLOCK_DEALS)
    existing = spark.sql(f"SELECT COUNT(*) AS n FROM {T_BULK_BLOCK_DEALS} WHERE trade_date = '{trade_date}'").collect()[0]["n"]
    if existing > 0:
        print(f"   🔄 Re-run: deleting {existing} existing rows for {trade_date}")
        dt.delete(F.col("trade_date") == F.lit(str(trade_date)).cast(DateType()))

    sdf.write.format("delta").mode("append").saveAsTable(T_BULK_BLOCK_DEALS)
    print(f"   ✅ Written {rows} rows for {trade_date}")
    return rows


def write_deals_audit(
    trade_date: date, status: str, batch_id: str,
    bulk_rows: int = 0, block_rows: int = 0, total_rows: int = 0, error_msg: str = "",
):
    """Append one row to the deals audit table."""
    try:
        row = Row(
            trade_date=trade_date, load_ts=datetime.utcnow(), status=status,
            deal_type="BOTH", bulk_rows=bulk_rows, block_rows=block_rows,
            total_rows=total_rows, error_msg=error_msg[:500] if error_msg else "",
            batch_id=batch_id,
        )
        adf = spark.createDataFrame([row])
        adf = adf.withColumn("trade_date", F.to_date(F.col("trade_date").cast("string")))
        adf.write.format("delta").mode("append").saveAsTable(T_DEALS_AUDIT)
    except Exception as e:
        print(f"⚠️  Audit write failed (non-fatal): {e}")

# COMMAND ----------

# DBTITLE 1,Load Orchestration
# ── LOAD ORCHESTRATION ────────────────────────────────────────

def get_loaded_deals_dates() -> Set[date]:
    """Dates already successfully loaded (from audit table)."""
    try:
        rows = spark.sql(f"SELECT DISTINCT trade_date FROM {T_DEALS_AUDIT} WHERE status IN ('SUCCESS', 'NO_DEALS')").collect()
        return {r["trade_date"] for r in rows}
    except Exception:
        return set()

def is_confirmed_holiday(trade_date: date) -> bool:
    """Check bhavcopy audit for confirmed holidays."""
    try:
        result = spark.sql(f"SELECT COUNT(*) AS n FROM {T_LOAD_AUDIT} WHERE trade_date = '{trade_date}' AND status = 'HOLIDAY'").collect()[0]["n"]
        return result > 0
    except Exception:
        return False

def ingest_deals_for_date(
    trade_date: date, session: requests.Session, batch_id: str, force: bool = False
) -> str:
    """Pipeline for one date: load bulk + block from UC Volume → transform → write → audit.

    Returns: SUCCESS / NO_DEALS / HOLIDAY / SKIP / FAILED
    """
    ds = trade_date.isoformat()

    if trade_date.weekday() >= 5:
        print(f"   ⏭  {ds} weekend")
        return "SKIP"

    if not force and is_confirmed_holiday(trade_date):
        print(f"   📆 {ds} confirmed holiday (bhavcopy audit) — skip")
        write_deals_audit(trade_date, "HOLIDAY", batch_id)
        return "HOLIDAY"

    if not force and trade_date in get_loaded_deals_dates():
        print(f"   ⏭  {ds} already loaded")
        return "SKIP"

    print(f"\n{'─'*55}")
    print(f"📅 Deals ingestion: {ds}")
    print(f"{'─'*55}")

    bulk_rows = block_rows = 0
    all_dfs = []

    try:
        # Load from UC Volume (files uploaded by GitHub Actions at 6 PM IST)
        bulk_raw, bulk_url = load_deals_from_volume(trade_date, "BULK")
        if bulk_raw is not None and len(bulk_raw) > 0:
            bulk_df = transform_deals(bulk_raw, "BULK", trade_date, batch_id, bulk_url)
            bulk_rows = len(bulk_df)
            if bulk_rows > 0:
                all_dfs.append(bulk_df)
                print(f"   ✅ BULK: {bulk_rows} deals ({bulk_df['is_institutional'].sum()} institutional)")

        # Load from UC Volume (files uploaded by GitHub Actions at 6 PM IST)
        block_raw, block_url = load_deals_from_volume(trade_date, "BLOCK")
        if block_raw is not None and len(block_raw) > 0:
            block_df = transform_deals(block_raw, "BLOCK", trade_date, batch_id, block_url)
            block_rows = len(block_df)
            if block_rows > 0:
                all_dfs.append(block_df)
                print(f"   ✅ BLOCK: {block_rows} deals ({block_df['is_institutional'].sum()} institutional)")

        total_rows = 0
        if all_dfs:
            combined = pd.concat(all_dfs, ignore_index=True)
            total_rows = write_deals_to_delta(combined, trade_date)
            status = "SUCCESS"
        else:
            print(f"   ℹ️  No bulk/block deals on {ds} (low activity day)")
            status = "NO_DEALS"

        write_deals_audit(trade_date, status, batch_id, bulk_rows, block_rows, total_rows)
        print(f"{'─'*55}")
        print(f"✅ {ds} — {status} | bulk={bulk_rows} block={block_rows} total={total_rows}")
        return status

    except Exception as e:
        print(f"❌ {ds} — FAILED: {e}")
        write_deals_audit(trade_date, "FAILED", batch_id, error_msg=str(e))
        return "FAILED"

# def run_deals_backfill(calendar_days: int = 90, force: bool = False) -> dict:
#     """Historical backfill — disabled (NSE API blocked from serverless).
#     Use GitHub Actions script to backfill: run manually with date range.
#     """
#     pass

def get_effective_trading_date() -> date:
    """Find the most recent COMPLETED trading day whose data is available.

    Rules:
      - Trading day + after 4:30 PM IST → today (market closed, data published)
      - Trading day + before 4:30 PM IST → last trading day (market in progress)
      - Weekend / holiday → last trading day
    """
    import pytz
    ist = pytz.timezone("Asia/Kolkata")
    now_ist = datetime.now(ist)
    today = now_ist.date()

    market_data_available = now_ist.hour >= 16  # 4:00 PM IST — NSE publishes data after market close (3:30 PM)

    if is_trading_day(today) and market_data_available:
        return today

    # Walk backwards to find the most recent trading day
    effective = today - timedelta(days=1)
    while not is_trading_day(effective):
        effective -= timedelta(days=1)
    return effective


def run_daily_deals_load():
    """Daily job entry point. Downloads the most recent COMPLETED trading day's data.

    Smart date selection:
      - Mon 11 AM → downloads Friday's data (market still open)
      - Mon 4:01 PM → downloads Monday's data (market closed, data available)
      - Sat/Sun → downloads Friday's data
      - Holiday → downloads last trading day's data
    Always reloads (force=True) — deletes existing data and rewrites. No duplicates.
    """
    import pytz
    ist = pytz.timezone("Asia/Kolkata")
    now_ist = datetime.now(ist)
    today = now_ist.date()
    effective_date = get_effective_trading_date()

    print(f"🔄 Daily deals load — {today} ({now_ist.strftime('%H:%M IST')})")

    if effective_date == today:
        print(f"   Today is a trading day, market closed → downloading today's data")
    else:
        if not is_trading_day(today):
            reason = "weekend" if today.weekday() >= 5 else f"holiday ({NSE_HOLIDAYS.get(today, '?')})"
            print(f"   Today is {reason} → downloading last trading day's data")
        else:
            print(f"   Today is a trading day but market still open → downloading last completed trading day")

    print(f"   📅 Effective date: {effective_date}")

    batch_id = f"deals_daily_{today.strftime('%Y%m%d')}_{now_ist.strftime('%H%M')}"
    session = None  # Volume-only mode (NSE download handled by GitHub Actions)
    status = ingest_deals_for_date(effective_date, session, batch_id, force=True)

    if status in ("SUCCESS", "NO_DEALS"):
        print(f"\n🔧 Optimizing partition {effective_date}...")
        spark.sql(f"OPTIMIZE {T_BULK_BLOCK_DEALS} WHERE trade_date = '{effective_date}'")
        print_deals_summary(effective_date)

def print_deals_summary(trade_date: date):
    """Quick summary of today's institutional activity."""
    print(f"\n🏦 Institutional deals — {trade_date}")
    spark.sql(f"""
        SELECT deal_type, client_type, buy_sell,
               COUNT(*) AS deals, COUNT(DISTINCT symbol) AS symbols,
               ROUND(SUM(turnover_cr), 1) AS total_turnover_cr
        FROM {T_BULK_BLOCK_DEALS}
        WHERE trade_date = '{trade_date}' AND is_institutional = true
        GROUP BY deal_type, client_type, buy_sell
        ORDER BY total_turnover_cr DESC
    """).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Data Quality Checks
# ── DATA QUALITY CHECKS ───────────────────────────────────────

def run_deals_quality_check(trade_date: date = None) -> bool:
    """Validate deals data. Less strict than bhavcopy (zero-deal days are normal)."""
    if trade_date is None:
        trade_date = spark.sql(f"SELECT MAX(trade_date) AS d FROM {T_BULK_BLOCK_DEALS}").collect()[0]["d"]

    print(f"\n🔍 Deals Quality Check — {trade_date}")
    print(f"{'─'*50}")

    stats = spark.sql(f"""
        SELECT
            COUNT(*) AS total_rows,
            SUM(CASE WHEN buy_sell NOT IN ('BUY','SELL') THEN 1 ELSE 0 END) AS bad_direction,
            SUM(CASE WHEN quantity IS NULL OR quantity <= 0 THEN 1 ELSE 0 END) AS bad_qty,
            SUM(CASE WHEN trade_price <= 0 THEN 1 ELSE 0 END) AS bad_price,
            SUM(CASE WHEN is_institutional = true AND buy_sell = 'BUY' THEN 1 ELSE 0 END) AS inst_buys,
            SUM(CASE WHEN is_institutional = true AND buy_sell = 'SELL' THEN 1 ELSE 0 END) AS inst_sells,
            COUNT(DISTINCT symbol) AS unique_symbols,
            ROUND(SUM(turnover_cr), 1) AS total_turnover_cr
        FROM {T_BULK_BLOCK_DEALS}
        WHERE trade_date = '{trade_date}'
    """).collect()[0]

    all_ok = True
    for name, val, ok_fn, critical in [
        ("Total rows (0 is OK)", stats["total_rows"], lambda x: x >= 0, False),
        ("Invalid buy/sell", stats["bad_direction"], lambda x: x == 0, True),
        ("Zero/null quantity", stats["bad_qty"], lambda x: x == 0, True),
        ("Zero/neg price", stats["bad_price"], lambda x: x == 0, True),
        ("Unique symbols", stats["unique_symbols"], lambda x: x >= 0, False),
        ("Inst buys", stats["inst_buys"], lambda x: x >= 0, False),
        ("Total turnover (Cr)", stats["total_turnover_cr"], lambda x: True, False),
    ]:
        ok = ok_fn(val) if val is not None else True
        icon = "✅" if ok else ("❌" if critical else "⚠️ ")
        if critical and not ok:
            all_ok = False
        print(f"   {icon} {name:<30}: {val}")

    print(f"{'─'*50}")
    print("✅ Deals quality checks passed." if all_ok else "❌ Quality issue detected.")
    return all_ok

# COMMAND ----------

# DBTITLE 1,Main Execution
# ── MAIN EXECUTION ────────────────────────────────────────────

# ═══════════════════════════════════════════════════════
# TEST — Upload sample CSVs to DBFS + test DBFS loading path
# Simulates what GitHub Actions would do at 6 PM IST
# ═══════════════════════════════════════════════════════
print("=" * 60)
print("TEST: UC Volume loading path (sample CSV upload + ingest)")
print("=" * 60)

_test_date = date(2026, 9, 18)
_date_str = _test_date.strftime("%d%m%Y")

# Sample NSE-format bulk deals CSV
_bulk_csv = 'SYMBOL,SECURITY NAME,CLIENT NAME,BUY/SELL,QUANTITY TRADED,TRADE PRICE,REMARKS\nRELIANCE,Reliance Industries Ltd,MUTUAL FUND SCHEME A,BUY,"1,00,000",2500.50,\nTCS,Tata Consultancy Services Ltd,MORGAN STANLEY MAURITIUS COMPANY LTD,SELL,"50,000",3500.00,\nINFY,Infosys Ltd,LIFE INSURANCE CORPORATION OF INDIA,BUY,"2,00,000",1500.75,\nHDFCBANK,HDFC Bank Ltd,GOLDMAN SACHS (SINGAPORE) PTE LTD,SELL,"75,000",1600.00,\nSBIN,State Bank of India,PROMOTER GROUP HOLDINGS,BUY,"3,00,000",550.25,\n'

# Sample NSE-format block deals CSV
_block_csv = 'SYMBOL,SECURITY NAME,CLIENT NAME,BUY/SELL,QUANTITY TRADED,TRADE PRICE,REMARKS\nICICIBANK,ICICI Bank Ltd,MIRAE ASSET LARGE CAP FUND,BUY,"5,00,000",900.50,\nWIPRO,Wipro Ltd,ABU DHABI INVESTMENT AUTHORITY,SELL,"3,00,000",450.00,\nLT,Larsen & Toubro Ltd,NATIONAL PENSION SYSTEM TRUST,BUY,"2,50,000",3500.00,\n'

# Upload to UC Volume (simulating GitHub Actions)
# NOTE: Files already uploaded via executeCode test. Uncomment to re-upload.
# import io
# from databricks.sdk import WorkspaceClient
# _w = WorkspaceClient()
# _bulk_vol = f"{VOLUME_DEALS_PATH}/bulk_{_date_str}.csv"
# _block_vol = f"{VOLUME_DEALS_PATH}/block_{_date_str}.csv"
# _w.files.upload(file_path=_bulk_vol, contents=io.BytesIO(_bulk_csv.encode()), overwrite=True)
# print(f"Uploaded bulk CSV to: {_bulk_vol}")
# _w.files.upload(file_path=_block_vol, contents=io.BytesIO(_block_csv.encode()), overwrite=True)
# print(f"Uploaded block CSV to: {_block_vol}")
print(f"Sample CSVs already in volume: {VOLUME_DEALS_PATH}/[bulk|block]_{_date_str}.csv")

# Test load_deals_from_volume()
print("\n--- Testing load_deals_from_volume() ---")
_bdf, _burl = load_deals_from_volume(_test_date, "BULK")
print(f"  Bulk: {len(_bdf) if _bdf is not None else 'None'} rows")
_bldf, _blurl = load_deals_from_volume(_test_date, "BLOCK")
print(f"  Block: {len(_bldf) if _bldf is not None else 'None'} rows")

# Full ingestion test (force=True to overwrite existing Sep 18 data)
print("\n--- Testing ingest_deals_for_date() via Volume ---")
_batch_id = f"test_dbfs_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
_status = ingest_deals_for_date(_test_date, None, _batch_id, force=True)
print(f"\nIngestion status: {_status}")

# Quality check
run_deals_quality_check(_test_date)

# Show the loaded data
print("\n--- Sep 18 data after Volume test ---")
spark.sql(f"""
    SELECT deal_type, client_type, buy_sell, symbol, quantity, trade_price,
           ROUND(turnover_cr, 2) AS turnover_cr
    FROM {T_BULK_BLOCK_DEALS}
    WHERE trade_date = '{_test_date}'
    ORDER BY deal_type, client_type, turnover_cr DESC
""").show(truncate=False)

# ═══════════════════════════════════════════════════════
# TEST 2 — run_daily_deals_load() smart date + force reload
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("TEST 2: run_daily_deals_load() — smart date + force reload")
print("=" * 60)
run_daily_deals_load()

# Verify no duplicates after reload
print("\n--- Duplicate check ---")
_dups = spark.sql(f"""
    SELECT symbol, client_name, buy_sell, quantity, COUNT(*) as cnt
    FROM {T_BULK_BLOCK_DEALS}
    WHERE trade_date = '2026-09-18'
    GROUP BY symbol, client_name, buy_sell, quantity
    HAVING COUNT(*) > 1
""").collect()
print(f"  Duplicate rows: {len(_dups)}")
_total = spark.sql(f"SELECT COUNT(*) as n FROM {T_BULK_BLOCK_DEALS} WHERE trade_date = '2026-09-18'").collect()[0]["n"]
print(f"  Total rows for Sep 18: {_total}")

# ═══════════════════════════════════════════════════════
# MODE A — FIRST TIME SETUP (run ONCE, requires NSE access)
# ═══════════════════════════════════════════════════════
# setup_schemas()
# run_deals_backfill(calendar_days=90, force=False)
# run_deals_quality_check()

# ═══════════════════════════════════════════════════════
# MODE B — DAILY SCHEDULED JOB (17:15 IST, after bhavcopy)
# ═══════════════════════════════════════════════════════
# run_daily_deals_load()

# ═══════════════════════════════════════════════════════
# MODE C — MANUAL RECOVERY (specific date re-load)
# ═══════════════════════════════════════════════════════
# recover_date = date(2026, 9, 18)
# session = get_nse_session()
# batch_id = f"recovery_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
# ingest_deals_for_date(recover_date, session, batch_id, force=True)
# run_deals_quality_check(recover_date)

# ═══════════════════════════════════════════════════════
# MODE D — INSPECT EXISTING DATA
# ═══════════════════════════════════════════════════════
# print("=" * 60)
# print("EXISTING BRONZE DEALS DATA")
# print("=" * 60)
# run_deals_quality_check()
# spark.sql(f"SELECT COUNT(*) AS total_rows, COUNT(DISTINCT trade_date) AS trading_dates, MIN(trade_date) AS first_date, MAX(trade_date) AS last_date FROM {T_BULK_BLOCK_DEALS}").show(truncate=False)