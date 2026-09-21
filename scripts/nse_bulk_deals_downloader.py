#!/usr/bin/env python3
# ============================================================
# SCRIPT  : scripts/nse_bulk_deals_downloader.py
# PURPOSE : Download NSE bulk + block deals CSV files
#           and Nifty 50 index data (via historical API)
#           and upload them to Databricks UC Volume.
#
# RUN BY  : GitHub Actions at 6:00 PM IST every weekday.
#           Can also be run manually from any machine.
#
# REQUIRES env vars (set as GitHub Secrets):
#   DATABRICKS_HOST   → e.g. https://adb-xxx.databricks.com
#   DATABRICKS_TOKEN  → your Databricks personal access token
#
# NO OTHER DEPENDENCIES — only stdlib + requests
# ============================================================

import os
import sys
import time
import random
import io
import urllib.parse
import requests
from datetime import date, datetime, timezone, timedelta

# ── Config ────────────────────────────────────────────────────────────────────
DATABRICKS_HOST  = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
# UC Volume path (not DBFS — DBFS is disabled on this workspace)
VOLUME_UPLOAD_PATH = "/Volumes/workspace/bronze_swing/deals_uploads"

# IST timezone (UTC+5:30)
IST = timezone(timedelta(hours=5, minutes=30))

# ── NSE URLs ─────────────────────────────────────────────────────────────────
# Live CSV URLs — ONLY work for today's trading data (no date in filename)
NSE_BULK_URLS = [
    "https://nsearchives.nseindia.com/archives/equities/bulkdeals/bulk.csv",
    "https://www.nseindia.com/archives/equities/bulkdeals/bulk.csv",
]
NSE_BLOCK_URLS = [
    "https://nsearchives.nseindia.com/archives/equities/blockdeals/block.csv",
    "https://www.nseindia.com/archives/equities/blockdeals/block.csv",
]

# Historical API endpoints — for past trading dates (from/to format: DD-MM-YYYY)
NSE_BULK_HISTORY_API  = "https://www.nseindia.com/api/historical/bulk-deals"
NSE_BLOCK_HISTORY_API = "https://www.nseindia.com/api/historical/block-deals"

NSE_NIFTY50_API = "https://www.nseindia.com/api/historical/indices"
NSE_NIFTY50_INDEX_TYPE = "NIFTY 50"

# Yahoo Finance for Nifty 50 (reliable — not blocked by anti-bot protection)
YAHOO_NIFTY50_URL = "https://query1.finance.yahoo.com/v8/finance/chart/%5ENSEI"
YAHOO_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
}

# Headers for browsing NSE pages (session priming)
NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer":         "https://www.nseindia.com/",
    "Connection":      "keep-alive",
}

# Headers for NSE API calls (JSON responses)
NSE_API_HEADERS = {
    **NSE_HEADERS,
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.nseindia.com/reports/bulk-deals",
}


def get_ist_date() -> date:
    return datetime.now(IST).date()


# -- NSE Trading Holiday Calendar -------------------------------------------
# Must match the calendar in shared/config notebook.
# Update each year when NSE publishes the official list.
NSE_HOLIDAYS = {
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
    """True if NSE is open (not weekend, not holiday)."""
    return d.weekday() < 5 and d not in NSE_HOLIDAYS


def get_effective_trading_date() -> date:
    """Smart date detection — same logic as the notebook.

    Rules:
      - Trading day + after 4:00 PM IST -> today (data published)
      - Trading day + before 4:00 PM IST -> last trading day
      - Weekend / holiday -> last trading day
    """
    now_ist = datetime.now(IST)
    today = now_ist.date()

    if is_trading_day(today) and now_ist.hour >= 16:
        return today

    effective = today - timedelta(days=1)
    while not is_trading_day(effective):
        effective -= timedelta(days=1)
    return effective


def prime_nse_session(session: requests.Session):
    pages = [
        "https://www.nseindia.com",
        "https://www.nseindia.com/market-data/bulk-deals",
    ]
    for url in pages:
        try:
            resp = session.get(url, timeout=15, allow_redirects=True)
            if resp.status_code == 200:
                print(f"   Session primed from: {url.split('/')[2]}")
                time.sleep(random.uniform(1.5, 2.5))
                break
        except Exception as e:
            print(f"   Prime attempt failed ({url.split('/')[2]}): {e}")


def download_csv(session: requests.Session, urls: list, label: str) -> bytes:
    for url in urls:
        for attempt in range(1, 4):
            try:
                resp = session.get(url, timeout=30, stream=False)
                if resp.status_code == 403:
                    print(f"   [{label}] 403 on attempt {attempt} — retrying...")
                    time.sleep(random.uniform(3, 5))
                    continue
                if resp.status_code == 404:
                    print(f"   [{label}] 404 — no file at {url}")
                    break
                resp.raise_for_status()
                content = resp.content
                if content.strip().startswith(b"<"):
                    print(f"   [{label}] Got HTML (blocked), trying next URL")
                    break
                if len(content) < 50:
                    print(f"   [{label}] Empty response, trying next URL")
                    break
                print(f"   [{label}] Downloaded {len(content):,} bytes from {url.split('/')[-1]}")
                return content
            except requests.exceptions.Timeout:
                print(f"   [{label}] Timeout attempt {attempt}")
                time.sleep(2)
            except Exception as e:
                print(f"   [{label}] Error attempt {attempt}: {e}")
                time.sleep(2)
    raise RuntimeError(f"All download attempts failed for {label}")


def download_deals_historical(session: requests.Session, api_url: str, target_date: date, label: str) -> bytes:
    """Download bulk/block deals from NSE historical API for a past trading date.

    Uses https://www.nseindia.com/api/historical/bulk-deals (or block-deals)
    with from/to date params. Returns CSV bytes.
    """
    import csv as csv_mod

    date_str = target_date.strftime("%d-%m-%Y")
    params = {"from": date_str, "to": date_str}

    # Use API headers for JSON responses
    api_session = requests.Session()
    api_session.headers.update(NSE_API_HEADERS)
    api_session.cookies.update(session.cookies)

    for attempt in range(1, 4):
        try:
            resp = api_session.get(api_url, params=params, timeout=30)
            if resp.status_code == 403:
                print(f"   [{label}] 403 on attempt {attempt} — refreshing session...")
                prime_nse_session(api_session)
                time.sleep(random.uniform(3, 5))
                continue
            if resp.status_code == 404:
                print(f"   [{label}] 404 — no records for {date_str}")
                raise RuntimeError(f"No {label} records for {target_date} (404)")
            resp.raise_for_status()

            # Check if response is actually JSON
            content_type = resp.headers.get("Content-Type", "")
            if "json" not in content_type and not resp.text.strip().startswith("{"):
                print(f"   [{label}] Non-JSON response (Content-Type: {content_type}), retrying...")
                prime_nse_session(api_session)
                time.sleep(random.uniform(3, 5))
                continue

            data = resp.json()

            # Extract records from NSE API response
            records = None
            if isinstance(data, list):
                records = data if len(data) > 0 else None
            elif isinstance(data, dict):
                for key in ["data", "Data", "DATA", label, label.lower()]:
                    if key in data:
                        val = data[key]
                        if val and isinstance(val, list):
                            records = val
                            break

            if not records:
                print(f"   [{label}] No records returned for {target_date}")
                raise RuntimeError(f"No {label} records for {target_date}")

            # Convert JSON records to CSV
            df_data = records
            if isinstance(df_data[0], dict):
                output = io.StringIO()
                writer = csv_mod.DictWriter(output, fieldnames=df_data[0].keys())
                writer.writeheader()
                writer.writerows(df_data)
                csv_bytes = output.getvalue().encode("utf-8")
                print(f"   [{label}] API returned {len(records)} records, CSV {len(csv_bytes)} bytes")
                return csv_bytes
            else:
                raise RuntimeError(f"Unexpected record format from {label} API")

        except requests.exceptions.Timeout:
            print(f"   [{label}] Timeout attempt {attempt}")
            time.sleep(2)
        except RuntimeError:
            raise
        except Exception as e:
            if attempt < 3:
                print(f"   [{label}] Error attempt {attempt}: {e}")
                time.sleep(2)
            else:
                raise

    raise RuntimeError(f"All {label} historical API attempts failed")


def download_nifty50(session: requests.Session, target_date: date) -> bytes:
    """Download Nifty 50 index data from Yahoo Finance API.

    Uses Yahoo Finance chart API for ^NSEI (Nifty 50 index).
    Returns CSV bytes with columns: TRADE_DATE, OPEN, HIGH, LOW, CLOSE, VOLUME.
    """
    import csv as csv_mod

    # Yahoo Finance uses Unix timestamps (UTC)
    dt_start = datetime(target_date.year, target_date.month, target_date.day, tzinfo=timezone.utc)
    period1 = int(dt_start.timestamp())
    period2 = int((dt_start + timedelta(days=1)).timestamp())

    params = {
        "period1": period1,
        "period2": period2,
        "interval": "1d",
    }

    for attempt in range(1, 4):
        try:
            resp = requests.get(YAHOO_NIFTY50_URL, params=params, timeout=15, headers=YAHOO_HEADERS)
            resp.raise_for_status()

            data = resp.json()
            result = data.get("chart", {}).get("result", [])

            if not result:
                print(f"   [NIFTY50] No result from Yahoo Finance for {target_date}")
                raise RuntimeError(f"No Nifty 50 data for {target_date}")

            chart_data = result[0]
            timestamps = chart_data.get("timestamp", [])
            quote = chart_data.get("indicators", {}).get("quote", [{}])[0]

            if not timestamps:
                print(f"   [NIFTY50] No timestamps for {target_date}")
                raise RuntimeError(f"No Nifty 50 data for {target_date}")

            # Build CSV from Yahoo Finance response
            CSV_HEADERS = ["TRADE_DATE", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]
            output = io.StringIO()
            writer = csv_mod.DictWriter(output, fieldnames=CSV_HEADERS)
            writer.writeheader()

            for i, ts in enumerate(timestamps):
                trade_date = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
                writer.writerow({
                    "TRADE_DATE": trade_date,
                    "OPEN":   quote.get("open", [None])[i] or "",
                    "HIGH":   quote.get("high", [None])[i] or "",
                    "LOW":    quote.get("low", [None])[i] or "",
                    "CLOSE":  quote.get("close", [None])[i] or "",
                    "VOLUME": quote.get("volume", [None])[i] or "",
                })

            csv_bytes = output.getvalue().encode("utf-8")
            print(f"   [NIFTY50] Yahoo Finance returned {len(timestamps)} record(s), CSV {len(csv_bytes)} bytes")
            return csv_bytes

        except requests.exceptions.Timeout:
            print(f"   [NIFTY50] Timeout attempt {attempt}")
            time.sleep(2)
        except Exception as e:
            if attempt < 3:
                print(f"   [NIFTY50] Error attempt {attempt}: {e}")
                time.sleep(2)
            else:
                raise

    raise RuntimeError("All Nifty 50 download attempts failed")


def upload_to_volume(content: bytes, volume_path: str):
    """Upload file to Databricks UC Volume via Files API.
    Uses PUT /api/2.0/fs/files/{path}?overwrite=true with raw bytes in body.
    """
    if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
        raise ValueError(
            "DATABRICKS_HOST and DATABRICKS_TOKEN must be set as env vars or GitHub Secrets"
        )

    encoded_path = urllib.parse.quote(volume_path, safe="")
    url = f"{DATABRICKS_HOST}/api/2.0/fs/files/{encoded_path}"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/octet-stream",
    }
    params = {"overwrite": "true"}

    resp = requests.put(url, headers=headers, params=params, data=content, timeout=30)

    if resp.status_code != 200:
        raise RuntimeError(
            f"Volume upload failed: {resp.status_code} — {resp.text}"
        )

    print(f"   Uploaded to UC Volume: {volume_path}")


def trigger_databricks_job(job_name: str = "NSE_BulkDeals_Daily"):
    if os.environ.get("TRIGGER_JOB", "false").lower() != "true":
        return
    list_url = f"{DATABRICKS_HOST}/api/2.1/jobs/list"
    headers  = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    resp     = requests.get(list_url, headers=headers, timeout=15)
    jobs     = resp.json().get("jobs", [])
    job      = next((j for j in jobs if j["settings"]["name"] == job_name), None)
    if not job:
        print(f"   Job '{job_name}' not found — skipping trigger")
        return
    job_id   = job["job_id"]
    run_url  = f"{DATABRICKS_HOST}/api/2.1/jobs/run-now"
    run_resp = requests.post(run_url, headers=headers,
                             json={"job_id": job_id}, timeout=15)
    run_resp.raise_for_status()
    run_id   = run_resp.json().get("run_id")
    print(f"   Triggered Databricks job '{job_name}' — run_id={run_id}")


def main():
    today          = get_ist_date()
    effective_date = get_effective_trading_date()
    ddmmyyyy       = effective_date.strftime("%d%m%Y")

    print(f"{'='*55}")
    print(f"NSE DEALS DOWNLOADER — {today} (effective: {effective_date})")
    print(f"   Host     : {DATABRICKS_HOST or 'NOT SET'}")
    print(f"   Volume   : {VOLUME_UPLOAD_PATH}")
    print(f"{'='*55}\n")

    if today != effective_date:
        if not is_trading_day(today):
            reason = "weekend" if today.weekday() >= 5 else f"holiday ({NSE_HOLIDAYS.get(today, '?')})"
            print(f"Today is {reason} -> downloading last trading day data ({effective_date})")
        else:
            print(f"Today is a trading day but market still open -> downloading last completed trading day ({effective_date})")
    else:
        print(f"Today is a trading day, market closed -> downloading today data ({effective_date})")

    print("Establishing NSE session...")
    session = requests.Session()
    session.headers.update(NSE_HEADERS)
    prime_nse_session(session)

    is_historical = (today != effective_date)
    if is_historical:
        print("   (Historical date — will use NSE historical API endpoints)")

    results  = {}
    failures = []

    print(f"\nBulk deals ({ddmmyyyy}):")
    try:
        if is_historical:
            print("   [BULK] Historical date — NSE API is blocked from GitHub Actions.")
            print("   [BULK] For historical bulk deals, download manually from:")
            print("   [BULK]   https://www.nseindia.com/reports/bulk-deals (browser only)")
            print("   [BULK]   Upload CSV to UC Volume as bulk_" + ddmmyyyy + ".csv")
            raise RuntimeError("Historical bulk deals not available via API (NSE anti-bot block)")
        else:
            bulk_content = download_csv(session, NSE_BULK_URLS, "BULK")
        bulk_vol_path  = f"{VOLUME_UPLOAD_PATH}/bulk_{ddmmyyyy}.csv"
        upload_to_volume(bulk_content, bulk_vol_path)
        results["bulk"] = "SUCCESS"
    except Exception as e:
        print(f"   BULK failed: {e}")
        results["bulk"] = "FAILED"
        failures.append("BULK")

    time.sleep(random.uniform(2, 4))

    print(f"\nBlock deals ({ddmmyyyy}):")
    try:
        if is_historical:
            print("   [BLOCK] Historical date — NSE API is blocked from GitHub Actions.")
            print("   [BLOCK] For historical block deals, download manually from:")
            print("   [BLOCK]   https://www.nseindia.com/reports/block-deals (browser only)")
            print("   [BLOCK]   Upload CSV to UC Volume as block_" + ddmmyyyy + ".csv")
            raise RuntimeError("Historical block deals not available via API (NSE anti-bot block)")
        else:
            block_content = download_csv(session, NSE_BLOCK_URLS, "BLOCK")
        block_vol_path  = f"{VOLUME_UPLOAD_PATH}/block_{ddmmyyyy}.csv"
        upload_to_volume(block_content, block_vol_path)
        results["block"] = "SUCCESS"
    except Exception as e:
        print(f"   BLOCK failed: {e}")
        results["block"] = "FAILED"
        failures.append("BLOCK")

    print(f"\nNifty 50 index ({ddmmyyyy}):")
    try:
        nifty_csv      = download_nifty50(session, effective_date)
        nifty_vol_path = f"{VOLUME_UPLOAD_PATH}/nifty50_{ddmmyyyy}.csv"
        upload_to_volume(nifty_csv, nifty_vol_path)
        results["nifty50"] = "SUCCESS"
    except Exception as e:
        print(f"   NIFTY50 failed: {e}")
        results["nifty50"] = "FAILED"
        failures.append("NIFTY50")

    print()
    trigger_databricks_job()

    print(f"\n{'='*55}")
    print(f"SUMMARY — {today} (effective: {effective_date})")
    for k, v in results.items():
        icon = "OK" if v == "SUCCESS" else "FAIL"
        print(f"   [{icon}] {k.upper():<10} : {v}")
    print(f"{'='*55}")

    if failures:
        print(f"\nFailed: {failures}. Check NSE availability and retry.")
        sys.exit(1)

    print("\nAll files uploaded to UC Volume. Databricks Bronze job will pick them up.")


if __name__ == "__main__":
    main()