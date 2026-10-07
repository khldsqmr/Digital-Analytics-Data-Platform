# Databricks notebook source
# ============================================================================
# FILE   : 01x_sdi_nb_mip_bronze_edlUdiHitsBenchmark_daily.py
# NAME   : sdi_nb_mip_bronze_edlUdiHitsBenchmark_daily
# OBJECT : B01
# LAYER  : BRONZE
# PURPOSE:
#   TEMPORARY performance benchmark used to select the B01 execution strategy.
#
# THIS IS NOT A PRODUCTION RUNNER.
#
# TESTED STRATEGIES:
#   A. WINDOW
#      One stored-procedure call for N days.
#
#   B. DAILY_SEQUENTIAL
#      N one-day stored-procedure calls executed sequentially.
#
#   C. DAILY_PARALLEL
#      N one-day calls with configurable notebook-level concurrency.
#
# DEFAULT TEST:
#   testDays = 2
#   concurrencyLevel = 2
#
# IMPORTANT FOR A TWO-DAY TEST:
#   With only two dates, effective concurrency cannot exceed 2. To compare
#   concurrency 2 vs 3 vs 4, increase testDays accordingly.
#
# WRITE BEHAVIOR:
#   Each scenario rewrites the SAME requested dates using REPLACE WHERE. This is
#   intentional so all strategies process identical data. Run only on completed
#   dates that are safe to refresh.
#
# VALIDATION:
#   The Validator is NOT executed between benchmark scenarios. The goal is to
#   measure transformation runtime and detect any concurrent Delta conflicts.
#
# PRODUCTION DECISION:
#   If daily parallel execution materially wins without conflicts, implement
#   concurrency in Lakeflow/For Each rather than keeping Python threads here.
# ============================================================================

# COMMAND ----------
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

# COMMAND ----------
dbutils.widgets.text("asOfDate", "")
dbutils.widgets.text("testDays", "2")
dbutils.widgets.dropdown("concurrencyLevel", "2", ["1", "2", "3", "4"])

as_of_date_raw = dbutils.widgets.get("asOfDate").strip()
test_days = int(dbutils.widgets.get("testDays"))
requested_concurrency = int(dbutils.widgets.get("concurrencyLevel"))

if not as_of_date_raw:
    raise ValueError(
        "Provide asOfDate as the latest completed test date, e.g. 2026-10-02."
    )
if test_days < 2:
    raise ValueError("testDays must be >= 2 for the benchmark.")
if requested_concurrency < 1:
    raise ValueError("concurrencyLevel must be >= 1.")

as_of_date = datetime.strptime(as_of_date_raw, "%Y-%m-%d").date()

dates = [
    as_of_date - timedelta(days=offset)
    for offset in range(test_days - 1, -1, -1)
]

effective_concurrency = min(requested_concurrency, test_days)

PROC = "prdrzranalytics.lab42.sdi_sp_mip_bronze_edlUdiHits_daily"

print("=" * 96)
print("B01 BENCHMARK")
print(f"dates                 : {[str(d) for d in dates]}")
print(f"testDays              : {test_days}")
print(f"requestedConcurrency  : {requested_concurrency}")
print(f"effectiveConcurrency  : {effective_concurrency}")
print("=" * 96)

def call_proc(run_date, window_days):
    """
    Execute one procedure call in an isolated SparkSession wrapper.

    Separate sessions do not guarantee separate compute resources; this is a
    first-pass concurrency experiment on the current compute. Lakeflow should be
    used for the final production concurrency implementation.
    """
    session = spark.newSession()
    started = time.perf_counter()

    try:
        session.sql(f"""
            CALL {PROC}(
                p_asOfDate        => DATE '{run_date}',
                p_eventWindowDays => {window_days},
                p_validateOnly    => FALSE
            )
        """).collect()

        return {
            "date": str(run_date),
            "windowDays": int(window_days),
            "status": "SUCCEEDED",
            "durationSeconds": float(time.perf_counter() - started),
            "error": None,
        }

    except Exception as exc:
        return {
            "date": str(run_date),
            "windowDays": int(window_days),
            "status": "FAILED",
            "durationSeconds": float(time.perf_counter() - started),
            "error": str(exc)[:4000],
        }

# COMMAND ----------
# Scenario A: one N-day procedure call.
scenario_started = time.perf_counter()
window_detail = call_proc(as_of_date, test_days)
window_wall = time.perf_counter() - scenario_started

results = [{
    "scenario": f"WINDOW_{test_days}D",
    "wallSeconds": float(window_wall),
    "sumCallSeconds": float(window_detail["durationSeconds"]),
    "failedCalls": 0 if window_detail["status"] == "SUCCEEDED" else 1,
    "detail": str(window_detail),
}]

# COMMAND ----------
# Scenario B: N one-day calls, sequential.
scenario_started = time.perf_counter()
sequential_details = [call_proc(d, 1) for d in dates]
sequential_wall = time.perf_counter() - scenario_started

results.append({
    "scenario": "DAILY_SEQUENTIAL",
    "wallSeconds": float(sequential_wall),
    "sumCallSeconds": float(sum(x["durationSeconds"] for x in sequential_details)),
    "failedCalls": int(sum(1 for x in sequential_details if x["status"] != "SUCCEEDED")),
    "detail": str(sequential_details),
})

# COMMAND ----------
# Scenario C: N one-day calls, concurrent.
scenario_started = time.perf_counter()
parallel_details = []

with ThreadPoolExecutor(max_workers=effective_concurrency) as pool:
    futures = {
        pool.submit(call_proc, d, 1): d
        for d in dates
    }

    for future in as_completed(futures):
        parallel_details.append(future.result())

parallel_wall = time.perf_counter() - scenario_started

results.append({
    "scenario": f"DAILY_PARALLEL_C{effective_concurrency}",
    "wallSeconds": float(parallel_wall),
    "sumCallSeconds": float(sum(x["durationSeconds"] for x in parallel_details)),
    "failedCalls": int(sum(1 for x in parallel_details if x["status"] != "SUCCEEDED")),
    "detail": str(sorted(parallel_details, key=lambda x: x["date"])),
})

# COMMAND ----------
# Summary.
baseline = results[0]["wallSeconds"]

summary_rows = []
for r in results:
    speedup = (
        baseline / r["wallSeconds"]
        if r["wallSeconds"] > 0
        else None
    )

    summary_rows.append({
        "scenario": r["scenario"],
        "wallMinutes": round(r["wallSeconds"] / 60.0, 3),
        "sumCallMinutes": round(r["sumCallSeconds"] / 60.0, 3),
        "speedupVsWindow": round(speedup, 3) if speedup is not None else None,
        "failedCalls": r["failedCalls"],
        "detail": r["detail"],
    })

summary_df = spark.createDataFrame(summary_rows).orderBy("wallMinutes")
display(summary_df)

print("=" * 96)
print("DECISION GUIDE")
print("""
1. DAILY_SEQUENTIAL faster than WINDOW:
   Daily chunking reduces the working set even without concurrency.

2. DAILY_PARALLEL materially faster with failedCalls=0:
   Daily chunking + concurrency is promising. Re-test that pattern in Lakeflow
   before adopting it for production.

3. DAILY_PARALLEL reports concurrent-write/Delta conflicts:
   Keep concurrency low or use sequential daily chunks.

4. WINDOW remains fastest:
   Keep multi-day window execution for B01; concurrency is not helping here.

For the default two-day test, concurrency above 2 cannot be evaluated.
""")
