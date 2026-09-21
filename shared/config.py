# Databricks notebook source
# DBTITLE 1,Swing Institutional Alpha — Shared Config
# MAGIC %md
# MAGIC # Swing Institutional Alpha — Shared Configuration
# MAGIC
# MAGIC Single source of truth for catalog names, table identifiers, NSE parameters, strategy thresholds, and LLM config. Every notebook starts with `%run /Users/chandrakanthab@gmail.com/Swing-Institutional-Alpha/shared/config` to import these variables.

# COMMAND ----------

# DBTITLE 1,All Configuration Variables
# ============================================================
# SHARED CONFIG — Swing Institutional Alpha
# All notebooks %run this to get parameters.
# Change a value here → every notebook picks it up.
# ============================================================

from datetime import date, timedelta

# ── 1. UNITY CATALOG (3-level naming) ───────────────────────────
CATALOG          = "workspace"
SCHEMA_BRONZE    = f"{CATALOG}.bronze_swing"
SCHEMA_SILVER    = f"{CATALOG}.silver_swing"
SCHEMA_GOLD      = f"{CATALOG}.gold_swing"

# ── Bronze tables (raw ingestion) ───────────────────────────────
T_BHAVCOPY          = f"{SCHEMA_BRONZE}.nse_bhavcopy"
T_BULK_BLOCK_DEALS  = f"{SCHEMA_BRONZE}.nse_bulk_block_deals"
T_NIFTY_INDEX       = f"{SCHEMA_BRONZE}.nifty_index"
T_FII_DII_FLOW      = f"{SCHEMA_BRONZE}.fii_dii_daily_flow"
T_CORP_ACTIONS     = f"{SCHEMA_BRONZE}.corporate_actions"
T_NIFTY500_HIST    = f"{SCHEMA_BRONZE}.nifty500_history"
T_CLIENT_REGISTRY  = f"{SCHEMA_BRONZE}.client_name_registry"
T_LOAD_AUDIT       = f"{SCHEMA_BRONZE}.load_audit"
T_DEALS_AUDIT      = f"{SCHEMA_BRONZE}.bulk_deals_audit"

# ── Silver tables (processed) ──────────────────────────────────
T_ADJUSTED_PRICES    = f"{SCHEMA_SILVER}.adjusted_prices"
T_TECH_INDICATORS    = f"{SCHEMA_SILVER}.technical_indicators"
T_REL_STRENGTH       = f"{SCHEMA_SILVER}.relative_strength"
T_INST_SCORES        = f"{SCHEMA_SILVER}.institutional_scores"
T_MARKET_REGIME      = f"{SCHEMA_SILVER}.market_regime"
T_VOLUME_METRICS     = f"{SCHEMA_SILVER}.volume_metrics"
T_FUNDAMENTAL_FLAGS  = f"{SCHEMA_SILVER}.fundamental_flags"

# ── Gold tables (decisions) ────────────────────────────────────
T_DAILY_SIGNALS   = f"{SCHEMA_GOLD}.daily_signals"
T_BACKTEST_RESULTS = f"{SCHEMA_GOLD}.backtest_results"

# ── 2. NSE SOURCE URLs ──────────────────────────────────────────
NSE_HOME_URL = "https://www.nseindia.com"

NSE_BHAVCOPY_PRIMARY = (
    "https://nsearchives.nseindia.com/products/content/"
    "sec_bhavdata_full_{ddmmyyyy}.csv"
)
NSE_BHAVCOPY_FALLBACK = (
    "https://archives.nseindia.com/products/content/"
    "sec_bhavdata_full_{ddmmyyyy}.csv"
)

# Bulk / Block deals — live CSV (today's data) + historical API (backfill)
NSE_BULK_LIVE_URLS = [
    "https://nsearchives.nseindia.com/archives/equities/bulkdeals/bulk.csv",
    "https://www.nseindia.com/archives/equities/bulkdeals/bulk.csv",
]
NSE_BLOCK_LIVE_URLS = [
    "https://nsearchives.nseindia.com/archives/equities/blockdeals/block.csv",
    "https://www.nseindia.com/archives/equities/blockdeals/block.csv",
]
NSE_BULK_HISTORY_API  = "https://www.nseindia.com/api/historical/bulk-deals"
NSE_BLOCK_HISTORY_API = "https://www.nseindia.com/api/historical/block-deals"

# Nifty 50 index — historical API (returns JSON, not CSV)
NSE_NIFTY50_HISTORY_API = "https://www.nseindia.com/api/historical/indices"
NSE_NIFTY50_INDEX_TYPE  = "NIFTY 50"

# ── 3. NSE HTTP HEADERS ─────────────────────────────────────────
NSE_REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer":         "https://www.nseindia.com/",
    "Connection":      "keep-alive",
    "Cache-Control":   "no-cache, no-store",
    "Pragma":          "no-cache",
}

# ── 4. DOWNLOAD SETTINGS ────────────────────────────────────────
MAX_RETRIES           = 3
RETRY_BASE_DELAY      = 5        # seconds × attempt number
REQUEST_TIMEOUT       = 45       # seconds per HTTP request
MIN_EXPECTED_EQ_ROWS  = 200      # sanity floor for bhavcopy
HISTORICAL_CALENDAR_DAYS = 360   # backfill window (~60 trading days)
INTER_REQUEST_DELAY   = (2.0, 4.0)  # random.uniform range between downloads
SESSION_REFRESH_EVERY = 10       # refresh NSE session every N downloads

# ── 5. VALID SERIES (NSE equity segments) ──────────────────────
VALID_SERIES = {"EQ", "BE", "BZ"}

# ── 6. NSE TRADING HOLIDAY CALENDAR ─────────────────────────────
# Source: https://www.nseindia.com/trade/market-timings-holidays
# Update each year when NSE publishes the official list.
NSE_HOLIDAYS: dict = {
    # ── 2025 ──────────────────────────────────────────────────────
    date(2025, 1, 26):  "Republic Day",
    date(2025, 2, 26):  "Mahashivratri",
    date(2025, 3, 14):  "Holi",
    date(2025, 4, 10):  "Shree Ram Navami",
    date(2025, 4, 14):  "Dr. Ambedkar Jayanti",
    date(2025, 4, 18):  "Good Friday",
    date(2025, 5, 1):   "Maharashtra Day",
    date(2025, 8, 15):  "Independence Day",
    date(2025, 8, 27):  "Ganesh Chaturthi",
    date(2025, 10, 2):  "Gandhi Jayanti / Dussehra",
    date(2025, 10, 20): "Diwali Laxmi Puja",
    date(2025, 10, 21): "Diwali Balipratipada",
    date(2025, 11, 5):  "Guru Nanak Jayanti",
    date(2025, 12, 25): "Christmas",

    # ── 2026 ──────────────────────────────────────────────────────
    date(2026, 1, 26):  "Republic Day",
    date(2026, 3, 3):   "Holi",
    date(2026, 3, 30):  "Gudi Padwa",
    date(2026, 4, 3):   "Good Friday",
    date(2026, 4, 14):  "Dr. Ambedkar Jayanti",
    date(2026, 4, 29):  "Buddha Purnima",
    date(2026, 5, 1):   "Maharashtra Day",
    date(2026, 6, 17):  "Bakri Id",
    date(2026, 8, 15):  "Independence Day",
    date(2026, 8, 27):  "Ganesh Chaturthi",
    date(2026, 10, 2):  "Gandhi Jayanti",
    date(2026, 10, 20): "Dussehra",
    date(2026, 11, 3):  "Diwali Laxmi Puja",
    date(2026, 11, 4):  "Diwali Balipratipada",
    date(2026, 11, 23): "Guru Nanak Jayanti",
    date(2026, 12, 25): "Christmas",
}

def is_trading_day(d: date) -> bool:
    """True if NSE is open (not weekend, not in NSE_HOLIDAYS)."""
    return d.weekday() < 5 and d not in NSE_HOLIDAYS

def get_trading_days(start: date, end: date) -> list:
    """Sorted list of NSE trading days between start and end (inclusive)."""
    result, current = [], start
    while current <= end:
        if is_trading_day(current):
            result.append(current)
        current += timedelta(days=1)
    return result

# ── 7. DATA QUALITY THRESHOLDS ──────────────────────────────────
DQ_MIN_EQ_SYMBOLS    = 1500     # min distinct EQ symbols per day
DQ_MAX_NULL_DELIVERY = 500      # max rows with null delivery % (non-critical)
DQ_AVG_DELIVERY_LO    = 20       # expected avg delivery % range (non-critical)
DQ_AVG_DELIVERY_HI    = 80

# ── 8. STRATEGY PARAMETERS (Silver + Gold layers) ───────────────
# Relative Strength (RS) — PRIMARY FILTER
RS_THRESHOLDS = {
    "rs_1m":  1.5,   # 1-month RS vs Nifty — hard filter
    "rs_3m":  1.3,   # 3-month RS
    "rs_6m":  1.2,   # 6-month RS
}

# ATR-based risk parameters (not fixed percentages)
ATR_PARAMS = {
    "atr_period":     14,     # ATR lookback
    "sl_multiplier":  1.5,    # stop-loss = entry - ATR × multiplier
    "target_1_mult":  2.0,    # first target = entry + ATR × multiplier
    "target_2_mult":  3.0,    # second target
    "target_3_mult":  5.0,    # third target (runner)
}

# Market Regime (Nifty EMA + RSI)
REGIME_PARAMS = {
    "nifty_ema_period":  50,    # EMA period for regime classification
    "nifty_rsi_period":   14,   # RSI period
    "bull_rsi_min":       50,   # RSI above this in BULL
    "bear_rsi_max":       40,   # RSI below this in BEAR
    "adx_threshold":      25,   # ADX above this = trending
}

# Scoring weights (base quality + institutional + technical)
SCORING_WEIGHTS = {
    "rs_rank":          0.30,   # relative strength rank
    "institutional":    0.25,  # FII/DII + bulk deal flow
    "delivery_trend":    0.15,  # rising delivery %
    "price_momentum":     0.15,  # close above EMA50/EMA200
    "volume_quality":    0.10,  # volume ratio vs 20-day avg
    "base_structure":     0.05,  # tight consolidation / VCP-lite
}

# Fundamental exclusion thresholds (hard filters, not scoring)
FUNDAMENTAL_EXCLUSIONS = {
    "max_pe":              100,   # exclude if PE > 100
    "min_roe":             5,    # exclude if ROE < 5%
    "max_debt_equity":     3.0,   # exclude if D/E > 3
    "min_market_cap_cr":   500,   # exclude if mktcap < ₹500 Cr
}

# Signal generation gates (execution order matters)
SIGNAL_GATES = {
    "market_regime_gate": True,   # no signals in BEAR regime
    "rs_filter":           True,   # RS > threshold required
    "fundamental_filter":  True,   # exclude flagged stocks
    "min_score":           60,     # minimum composite score (0-100)
}

# ── 9. LLM CONFIG (Gemini Flash — entity classification only) ───
LLM_CONFIG = {
    "provider":       "gemini",
    "model":          "gemini-1.5-flash",
    "api_key_secret": "swing_trading",   # dbutils.secrets scope
    "api_key_key":     "gemini_api_key",   # key within scope
    "max_tokens":     200,
    "temperature":    0.1,               # deterministic classification
    "batch_size":     50,                # names per API call
    "categories": [
        "MF",          # Mutual Fund
        "FII_FPI",     # Foreign Institutional Investor / FPI
        "DII",         # Domestic Institutional Investor (insurance, pension)
        "PROMOTER",    # Promoter / insider
        "ALGO_PROP",   # Algorithmic / proprietary trading firm
        "INDIVIDUAL",   # Individual investor (HNI/retail)
        "OTHER",
    ],
}

# ── 10. BACKTEST PARAMETERS ─────────────────────────────────────
BACKTEST_PARAMS = {
    "initial_capital":   1000000,   # ₹10 lakh
    "max_positions":     10,        # max concurrent holdings
    "position_size":     0.10,      # 10% of capital per position
    "max_hold_days":     20,        # max holding period (swing)
    "min_hold_days":     3,         # min holding before exit
    "trailing_sl":       True,      # trail stop-loss after target-1
    "benchmark":         "NIFTY 50",
}

print("✅ Shared config loaded — Swing Institutional Alpha")
print(f"   Catalog : {CATALOG}")
print(f"   Bronze  : {SCHEMA_BRONZE}")
print(f"   Silver  : {SCHEMA_SILVER}")
print(f"   Gold    : {SCHEMA_GOLD}")

# COMMAND ----------

