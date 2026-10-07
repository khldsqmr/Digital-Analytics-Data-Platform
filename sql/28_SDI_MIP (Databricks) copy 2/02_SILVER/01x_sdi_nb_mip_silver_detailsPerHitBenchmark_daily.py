# Databricks notebook source
# ============================================================================
# FILE   : 01x_sdi_nb_mip_silver_detailsPerHitBenchmark_daily.py
# NAME   : sdi_nb_mip_silver_detailsPerHitBenchmark_daily
# OBJECT : S01
# LAYER  : SILVER
# PURPOSE:
#   TEMPORARY transformation-runtime benchmark only.
#
#   Compares the same S01 date scope using:
#     1. one N-day stored-procedure call
#     2. one-day stored-procedure calls sequentially
#     3. one-day calls concurrently at C2..Cmax
#
#   The same dates are intentionally rewritten between scenarios.
#   Do not use this notebook as the production Runner.
#   Do not run the Validator between benchmark scenarios.
#
# WIDGETS:
#   asOfDate        : last date in the benchmark scope; blank = yesterday PST
#   testDays        : number of dates; use >= 4 when testing C4
#   concurrencyLevel: maximum parallel level to test, 2-4; default 4 tests C2/C3/C4
#
# CALL NOTE:
#   CALL arguments are rendered as validated Python DATE/INT literals because
#   the Databricks runtime requires stored-procedure CALL args to be foldable.
# ============================================================================

# COMMAND ----------
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# COMMAND ----------
dbutils.widgets.text("asOfDate", "")
dbutils.widgets.text("testDays", "4")
dbutils.widgets.text("concurrencyLevel", "4")

as_of_raw = dbutils.widgets.get("asOfDate").strip()

try:
    test_days = int(dbutils.widgets.get("testDays").strip())
except Exception as exc:
    raise ValueError("testDays must be an integer >= 1.") from exc

try:
    max_concurrency = int(dbutils.widgets.get("concurrencyLevel").strip())
except Exception as exc:
    raise ValueError("concurrencyLevel must be an integer from 2 through 4.") from exc

if test_days < 1:
    raise ValueError("testDays must be >= 1.")
if max_concurrency < 2 or max_concurrency > 4:
    raise ValueError("concurrencyLevel must be between 2 and 4.")
if test_days < max_concurrency:
    raise ValueError(
        "testDays must be >= concurrencyLevel so every requested concurrency "
        "level can be exercised. For C2/C3/C4 use testDays >= 4."
    )

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

PROC = "prdrzranalytics.lab42.sdi_sp_mip_silver_detailsPerHit_daily"

# COMMAND ----------
def call_proc(session, day, window_days):
    """Call S01 once and return transformation-only timing/error detail."""
    started = time.perf_counter()
    try:
        call_sql = f"""
            CALL {PROC}(
                p_asOfDate        => DATE '{day.isoformat()}',
                p_eventWindowDays => {int(window_days)},
                p_validateOnly    => FALSE
            )
        """
        session.sql(call_sql).collect()
        return {
            "day": day.isoformat(),
            "windowDays": int(window_days),
            "status": "SUCCEEDED",
            "seconds": time.perf_counter() - started,
            "error": None,
        }
    except Exception as exc:
        return {
            "day": day.isoformat(),
            "windowDays": int(window_days),
            "status": "FAILED",
            "seconds": time.perf_counter() - started,
            "error": str(exc),
        }


def run_window():
    started = time.perf_counter()
    detail = [call_proc(spark, as_of_date, test_days)]
    return time.perf_counter() - started, detail


def run_sequential():
    started = time.perf_counter()
    detail = [call_proc(spark, day, 1) for day in dates]
    return time.perf_counter() - started, detail


def run_parallel(concurrency):
    started = time.perf_counter()
    detail = []
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        future_map = {
            pool.submit(call_proc, spark.newSession(), day, 1): day
            for day in dates
        }
        for future in as_completed(future_map):
            detail.append(future.result())
    detail.sort(key=lambda x: x["day"])
    return time.perf_counter() - started, detail

# COMMAND ----------
print("=" * 100)
print("MIP S01 SILVER benchmark | detailsPerHit")
print(f"procedure          : {PROC}")
print(f"asOfDate           : {as_of_date}")
print(f"testDays           : {test_days}")
print(f"dates              : {', '.join(d.isoformat() for d in dates)}")
print(f"maxConcurrency     : {max_concurrency}")
print("NOTE               : the same target dates are intentionally rewritten.")
print("=" * 100)

scenario_results = []

window_wall, window_detail = run_window()
scenario_results.append(
    {
        "scenario": f"WINDOW_{test_days}D",
        "wallSeconds": window_wall,
        "detail": window_detail,
    }
)

seq_wall, seq_detail = run_sequential()
scenario_results.append(
    {
        "scenario": "DAILY_SEQUENTIAL",
        "wallSeconds": seq_wall,
        "detail": seq_detail,
    }
)

for concurrency in range(2, max_concurrency + 1):
    parallel_wall, parallel_detail = run_parallel(concurrency)
    scenario_results.append(
        {
            "scenario": f"DAILY_PARALLEL_C{concurrency}",
            "wallSeconds": parallel_wall,
            "detail": parallel_detail,
        }
    )

# COMMAND ----------
window_seconds = scenario_results[0]["wallSeconds"]
seq_success_times = [
    item["seconds"]
    for item in seq_detail
    if item["status"] == "SUCCEEDED"
]
seq_avg_seconds = (
    sum(seq_success_times) / len(seq_success_times)
    if seq_success_times
    else None
)

summary_rows = []
detail_rows = []

for result in scenario_results:
    scenario = result["scenario"]
    wall_seconds = result["wallSeconds"]
    detail = result["detail"]
    failed_calls = sum(1 for item in detail if item["status"] != "SUCCEEDED")
    sum_call_seconds = sum(item["seconds"] for item in detail)
    success_times = [
        item["seconds"]
        for item in detail
        if item["status"] == "SUCCEEDED"
    ]
    avg_call_seconds = (
        sum(success_times) / len(success_times)
        if success_times
        else None
    )

    summary_rows.append(
        {
            "scenario": scenario,
            "wallMinutes": round(wall_seconds / 60, 3),
            "sumCallMinutes": round(sum_call_seconds / 60, 3),
            "avgCallMinutes": (
                round(avg_call_seconds / 60, 3)
                if avg_call_seconds is not None
                else None
            ),
            "failedCalls": failed_calls,
            "speedupVsWindow": (
                round(window_seconds / wall_seconds, 3)
                if wall_seconds > 0
                else None
            ),
            "avgCallSlowdownVsSequential": (
                round(avg_call_seconds / seq_avg_seconds, 3)
                if avg_call_seconds is not None
                and seq_avg_seconds is not None
                and seq_avg_seconds > 0
                and scenario.startswith("DAILY_PARALLEL_C")
                else None
            ),
            "detailJson": json.dumps(detail, default=str),
        }
    )

    for item in detail:
        detail_rows.append(
            {
                "scenario": scenario,
                "day": item["day"],
                "windowDays": int(item["windowDays"]),
                "status": item["status"],
                "callMinutes": round(item["seconds"] / 60, 3),
                "error": item["error"],
            }
        )

summary_df = spark.createDataFrame(summary_rows)
detail_df = spark.createDataFrame(detail_rows)

print("\nSUMMARY")
display(summary_df.orderBy("wallMinutes"))

print("\nPER-CALL DETAIL")
display(detail_df.orderBy("scenario", "day"))

# COMMAND ----------
# A recommendation is printed only when a scenario completed with zero failed
# calls. Lower wall time alone is not sufficient; per-call slowdown is surfaced
# above so resource contention remains visible.
valid = [row for row in summary_rows if row["failedCalls"] == 0]
if valid:
    fastest = min(valid, key=lambda row: row["wallMinutes"])
    print("\nFASTEST ZERO-FAILURE SCENARIO")
    print(json.dumps(fastest, indent=2))
else:
    print("\nNo benchmark scenario completed with zero failed calls.")

print(
    "\nInterpretation: choose concurrency using wallMinutes together with "
    "sumCallMinutes and avgCallSlowdownVsSequential. A lower wall time with a "
    "large per-call slowdown means the cluster is experiencing contention."
)
