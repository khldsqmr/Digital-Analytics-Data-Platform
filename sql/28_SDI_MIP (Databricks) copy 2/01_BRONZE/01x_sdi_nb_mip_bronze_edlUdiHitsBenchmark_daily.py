# Databricks notebook source
# ============================================================================
# FILE   : 01x_sdi_nb_mip_bronze_edlUdiHitsBenchmark_daily.py new
# NAME   : sdi_nb_mip_bronze_edlUdiHitsBenchmark_daily
# OBJECT : B01
# PURPOSE:
#   TEMPORARY benchmark only.
#
#   Compares:
#     1. one multi-day SP call
#     2. one-day SP calls sequentially
#     3. one-day SP calls concurrently
#
#   The same date scope is intentionally rewritten between scenarios.
#   Do not use this notebook as the production Runner.
# ============================================================================

# COMMAND ----------
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

dbutils.widgets.text("asOfDate", "")
dbutils.widgets.text("testDays", "2")
dbutils.widgets.text("concurrencyLevel", "2")

as_of_raw = dbutils.widgets.get("asOfDate").strip()
test_days = int(dbutils.widgets.get("testDays"))
concurrency_level = int(dbutils.widgets.get("concurrencyLevel"))

if test_days < 1:
    raise ValueError("testDays must be >= 1.")
if concurrency_level < 1:
    raise ValueError("concurrencyLevel must be >= 1.")

if as_of_raw:
    as_of_date = datetime.strptime(as_of_raw, "%Y-%m-%d").date()
else:
    as_of_date = (
        datetime.now(ZoneInfo("America/Los_Angeles")).date()
        - timedelta(days=1)
    )

dates = [
    as_of_date - timedelta(days=offset)
    for offset in reversed(range(test_days))
]

PROC = "prdrzranalytics.lab42.sdi_sp_mip_bronze_edlUdiHits_daily"

def call_proc(session, day, window_days):
    started = time.perf_counter()
    try:
        # Same CALL rule as the production Runner: render only the already-typed
        # Python date/int as SQL literals because CALL requires foldable args.
        call_sql = f"""
            CALL {PROC}(
                p_asOfDate        => DATE '{day.isoformat()}',
                p_eventWindowDays => {int(window_days)},
                p_validateOnly    => FALSE
            )
        """
        session.sql(call_sql).collect()
        return {
            "day": str(day),
            "windowDays": window_days,
            "status": "SUCCEEDED",
            "seconds": time.perf_counter() - started,
            "error": None,
        }
    except Exception as exc:
        return {
            "day": str(day),
            "windowDays": window_days,
            "status": "FAILED",
            "seconds": time.perf_counter() - started,
            "error": str(exc),
        }

results = []

# Scenario 1: one N-day window
started = time.perf_counter()
window_detail = [call_proc(spark, as_of_date, test_days)]
window_wall = time.perf_counter() - started
results.append(("WINDOW", window_wall, window_detail))

# Scenario 2: one day at a time, sequentially
started = time.perf_counter()
seq_detail = [call_proc(spark, day, 1) for day in dates]
seq_wall = time.perf_counter() - started
results.append(("DAILY_SEQUENTIAL", seq_wall, seq_detail))

# Scenario 3: one day at a time, concurrently
started = time.perf_counter()
parallel_detail = []
effective_concurrency = min(concurrency_level, len(dates))

with ThreadPoolExecutor(max_workers=effective_concurrency) as pool:
    future_map = {
        pool.submit(call_proc, spark.newSession(), day, 1): day
        for day in dates
    }
    for future in as_completed(future_map):
        parallel_detail.append(future.result())

parallel_wall = time.perf_counter() - started
results.append(
    (f"DAILY_PARALLEL_C{effective_concurrency}", parallel_wall, parallel_detail)
)

window_seconds = results[0][1]

rows = []
for scenario, wall_seconds, detail in results:
    failed_calls = sum(1 for item in detail if item["status"] != "SUCCEEDED")
    sum_call_seconds = sum(item["seconds"] for item in detail)
    rows.append(
        {
            "scenario": scenario,
            "wallMinutes": round(wall_seconds / 60, 3),
            "sumCallMinutes": round(sum_call_seconds / 60, 3),
            "speedupVsWindow": (
                round(window_seconds / wall_seconds, 3)
                if wall_seconds > 0
                else None
            ),
            "failedCalls": failed_calls,
            "detail": detail,
        }
    )

display(spark.createDataFrame(rows))
