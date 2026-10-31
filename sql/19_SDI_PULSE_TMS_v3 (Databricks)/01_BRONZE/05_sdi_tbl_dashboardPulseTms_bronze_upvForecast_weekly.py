# Databricks notebook source
# MAGIC %md
# MAGIC # PulseTMS — UPV Forecast Bronze Upload
# MAGIC
# MAGIC **Purpose:** Upload a quarterly UPV forecast CSV to the Bronze table in Databricks.
# MAGIC **Target table:** `prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_upvForecast_weekly`
# MAGIC **Upsert logic:** New weeks are inserted; existing weeks are overwritten with the latest
# MAGIC forecast values; weeks not in the upload are untouched.
# MAGIC
# MAGIC **Table name corrected from the GCP version** (`sdi_pulseTms_bronze_upvForecast_weekly`) to
# MAGIC match this pipeline's established 6-segment taxonomy
# MAGIC (`teamName_tableOrView_projectName_layer_purpose_cadence`) — this is also the exact name
# MAGIC already wired into `gold_unified_long`'s `UpvForecast` CTE.
# MAGIC
# MAGIC **Expected CSV columns:**
# MAGIC
# MAGIC | Column | Description |
# MAGIC |--------|-------------|
# MAGIC | `Week Ending` | Saturday date (or corrected quarter-end date for boundary stubs) |
# MAGIC | `2026 Forecast` | UPV forecast (maps to `upvTotalAdobe`) |
# MAGIC | `2026 Web +App Forecast` | UPV Web + App forecast |
# MAGIC
# MAGIC > **Before uploading:** Ensure boundary stub weeks use the correct quarter-end date
# MAGIC > (e.g. `9/30/2026`), not the following Saturday.
# MAGIC
# MAGIC **How this differs from the GCP notebook, and why:**
# MAGIC - Upload happens via an in-cell `ipywidgets.FileUpload` widget + a **Process File** button,
# MAGIC   same in-notebook convenience as the original. One click reads, cleans, validates, and
# MAGIC   previews the CSV, including the boundary-date check — no need to run several cells in
# MAGIC   sequence for that part. ⚠ ipywidgets support in Databricks notebooks varies by runtime;
# MAGIC   this is worth testing once in your workspace before relying on it. If it doesn't render or
# MAGIC   the button doesn't fire, the reliable fallback is uploading the CSV to a Unity Catalog
# MAGIC   Volume (Catalog Explorer → Volumes → Upload) and reading it with a plain
# MAGIC   `pd.read_csv('/Volumes/.../file.csv')` in place of the widget cell below — worth knowing
# MAGIC   about even if you don't need it today, since a scheduled/automated version of this
# MAGIC   notebook would need that approach regardless (ipywidgets only works in interactive runs).
# MAGIC - The staging step uses a Spark temp view instead of a physical BigQuery staging table —
# MAGIC   Delta's `MERGE` can read directly from a temp view, so there's no staging table to create
# MAGIC   or drop afterward (a temp view is session-scoped and disappears on its own).
# MAGIC - `spark` and `dbutils` are already initialized in every Databricks notebook session — there's
# MAGIC   no equivalent of the GCP version's explicit `bigquery.Client()` connection step needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

import io
import calendar
from datetime import date

import pandas as pd
import ipywidgets as widgets
from IPython.display import display, HTML

# ── Configuration ────────────────────────────────────────────────────────────
CATALOG    = 'prdrzranalytics'
SCHEMA     = 'lab42'
TABLE_NAME = 'sdi_tbl_dashboardPulseTms_bronze_upvForecast_weekly'

FULL_TABLE = f'{CATALOG}.{SCHEMA}.{TABLE_NAME}'

# Column name mapping: CSV -> Bronze (unchanged from the GCP version)
COL_MAP = {
    'Week Ending': 'week_sun_sat',
    '2026 Forecast': 'upv_forecast',
    '2026 Web +App Forecast': 'upv_webapp_forecast',
}

# Delta schema used for the Bronze table (FLOAT64 -> DOUBLE, matching this pipeline's
# established BQ -> Databricks type-porting convention throughout every other file)
DELTA_SCHEMA = """
  week_sun_sat        DATE,
  upv_forecast        DOUBLE,
  upv_webapp_forecast DOUBLE,
  file_load_date      DATE,
  source_filename     STRING
"""

# Quarter-end dates are Mar 31, Jun 30, Sep 30, Dec 31 -- used by the boundary-date check below
QUARTER_END_MONTHS_DAYS = {
    (3, 31),
    (6, 30),
    (9, 30),
    (12, 31)
}

# Confirm the catalog/schema exist and are reachable (Databricks equivalent of the GCP
# version's client.get_dataset() existence check)
schema_check = spark.sql(f"SHOW SCHEMAS IN {CATALOG} LIKE '{SCHEMA}'").count()
if schema_check == 0:
    raise ValueError(f'Schema not found: {CATALOG}.{SCHEMA}')

print(f'✅ Connected to catalog: {CATALOG}')
print(f'   Schema: {CATALOG}.{SCHEMA}')
print(f'   Target table: {FULL_TABLE}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Upload and process the CSV
# MAGIC
# MAGIC Select the quarterly CSV with the widget, then click **Process File**. This reads, cleans,
# MAGIC validates columns, parses dates/numbers, and checks boundary dates -- all in one click, with
# MAGIC the results (preview + any validation issues) shown right below the button.
# MAGIC
# MAGIC The parsed result is stored in the notebook's `df` variable (and any date issues in `issues`)
# MAGIC for the next cell to use -- you don't need to re-run this cell if you just want to re-check
# MAGIC the output, only if you're uploading a different file.

# COMMAND ----------

uploader = widgets.FileUpload(
    accept='.csv',
    multiple=False,
    description='Select CSV'
)
process_button = widgets.Button(
    description='Process File',
    button_style='primary',
    icon='check'
)
output = widgets.Output()

# Populated by process_uploaded_file() below once the button is clicked; the next cell
# (the MERGE/upsert) reads these as module-level globals.
df = None
issues = None
filename = None


def process_uploaded_file(_button):
    global df, issues, filename

    with output:
        output.clear_output()

        # ── Guard: ensure a file was uploaded ────────────────────────────────
        if not uploader.value:
            print('❌ No file uploaded. Please select a CSV using the widget above first.')
            return

        # ── Read uploaded bytes into a DataFrame ─────────────────────────────
        # Handles both ipywidgets FileUpload value formats -- older versions return a dict
        # keyed by filename, newer versions (8.x+) return a tuple of dicts. Which one your
        # workspace's ipywidgets version uses isn't something I can confirm from here, so
        # both are handled rather than assumed.
        if isinstance(uploader.value, dict):
            uploaded_file = list(uploader.value.values())[0]
            fname = list(uploader.value.keys())[0]
        else:
            uploaded_file = uploader.value[0]
            fname = uploaded_file['name']

        raw_bytes = uploaded_file['content']
        df_raw = pd.read_csv(io.BytesIO(raw_bytes))

        # Clean column names:
        # - remove leading/trailing spaces
        # - normalize multiple spaces into a single space
        df_raw.columns = (
            df_raw.columns
                .str.strip()
                .str.replace(r'\s+', ' ', regex=True)
        )

        print(f'📄 File loaded: {fname}')
        print(f'   Raw shape: {df_raw.shape[0]} rows × {df_raw.shape[1]} columns')
        print(f'   Columns found: {list(df_raw.columns)}')

        # ── Validate expected columns exist ──────────────────────────────────
        missing_cols = [c for c in COL_MAP.keys() if c not in df_raw.columns]

        if missing_cols:
            print(f'❌ Missing expected columns: {missing_cols}')
            print(f'   Columns in file: {list(df_raw.columns)}')
            df, issues, filename = None, None, None
            return

        # ── Select and rename columns ────────────────────────────────────────
        parsed = df_raw[list(COL_MAP.keys())].copy()
        parsed.rename(columns=COL_MAP, inplace=True)

        # ── Parse week_sun_sat as DATE ───────────────────────────────────────
        parsed['week_sun_sat'] = pd.to_datetime(
            parsed['week_sun_sat'],
            errors='coerce'
        ).dt.date

        # ── Parse metric columns — strip commas/spaces, cast to float ────────
        for col in ['upv_forecast', 'upv_webapp_forecast']:
            parsed[col] = (
                parsed[col]
                .astype(str)
                .str.replace(',', '', regex=False)
                .str.strip()
                .pipe(pd.to_numeric, errors='coerce')
            )

        # ── Add metadata columns ─────────────────────────────────────────────
        parsed['file_load_date'] = date.today()
        parsed['source_filename'] = fname

        # ── Drop rows where week_sun_sat is null ─────────────────────────────
        null_dates = parsed['week_sun_sat'].isna().sum()

        if null_dates > 0:
            print(f'⚠️ Dropping {null_dates} rows with null week_sun_sat')
            parsed = parsed.dropna(subset=['week_sun_sat'])

        print(f'\n✅ Parsed successfully — {len(parsed)} rows')

        if len(parsed) > 0:
            print(
                f'   Date range: '
                f'{parsed["week_sun_sat"].min()} → {parsed["week_sun_sat"].max()}'
            )

        print()
        display(parsed.head(10))

        # ── Boundary date validation (Saturday or exact quarter-end) ─────────
        row_issues = []

        for _, row in parsed.iterrows():
            d = row['week_sun_sat']
            is_saturday = d.weekday() == 5  # Monday=0, Saturday=5
            is_quarter_end = (d.month, d.day) in QUARTER_END_MONTHS_DAYS

            if not is_saturday and not is_quarter_end:
                row_issues.append({
                    'week_sun_sat': d,
                    'day_of_week': calendar.day_name[d.weekday()],
                    'issue': 'Not a Saturday or quarter-end date'
                })

        print()
        if row_issues:
            print(f'⚠️ {len(row_issues)} date(s) failed validation:')
            display(pd.DataFrame(row_issues))
            print('\nPlease fix these dates in the CSV, then re-upload and click Process File again.')
            print('Boundary stub weeks should use the quarter-end date, e.g. 9/30/2026, not the following Saturday.')
        else:
            print(f'✅ All {len(parsed)} dates are valid, Saturday or quarter-end.')

        # ── Set the globals the next cell (MERGE) reads ──────────────────────
        df, issues, filename = parsed, row_issues, fname


process_button.on_click(process_uploaded_file)

display(
    HTML('<h4 style="font-family:sans-serif; margin-bottom:8px">Select and process quarterly UPV forecast CSV</h4>'),
    uploader,
    process_button,
    output
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Upsert to the Bronze Delta table
# MAGIC
# MAGIC **Logic (unchanged from the GCP version):**
# MAGIC 1. Register the validated rows as a temp view (Delta equivalent of writing to a staging table)
# MAGIC 2. `MERGE` the temp view -> Bronze on `week_sun_sat`
# MAGIC    - **MATCH:** overwrite forecast values + update `file_load_date` and `source_filename`
# MAGIC    - **NO MATCH:** insert new row
# MAGIC    - Weeks not in the upload are untouched
# MAGIC
# MAGIC > Only run this cell once you're happy with the preview and validation above.

# COMMAND ----------

# ── Abort if the upload/process step above hasn't run yet, or failed ─────────
if df is None:
    raise ValueError(
        'No processed data available. Select a CSV and click Process File in the cell above first.'
    )

# ── Abort if date validation found issues ─────────────────────────────────────
if issues:
    raise ValueError(
        f'{len(issues)} date validation issue(s) found. '
        'Fix the CSV, re-upload, and click Process File again before writing to the Bronze table.'
    )

# ── Abort if parsed dataframe is empty ────────────────────────────────────────
if df.empty:
    raise ValueError('Parsed dataframe is empty. Nothing to write to the Bronze table.')

# ── Abort if duplicate week_sun_sat values exist in upload ───────────────────
duplicate_weeks = df[df.duplicated(subset=['week_sun_sat'], keep=False)]

if not duplicate_weeks.empty:
    print('❌ Duplicate week_sun_sat values found in upload:')
    display(
        duplicate_weeks
        .sort_values('week_sun_sat')
        [['week_sun_sat', 'upv_forecast', 'upv_webapp_forecast', 'source_filename']]
    )

    raise ValueError(
        'Duplicate week_sun_sat values found in the CSV. '
        'Fix duplicates, re-upload, and click Process File again.'
    )

# ── Step 1: Ensure Bronze target table exists ─────────────────────────────────
print(f'🛠️ Checking target Bronze table: {FULL_TABLE}')

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {FULL_TABLE} (
    {DELTA_SCHEMA}
  )
  USING DELTA
  CLUSTER BY (week_sun_sat)
  COMMENT 'PulseTMS Bronze — quarterly UPV forecast, uploaded manually via this notebook. One row per week_sun_sat. Upserted (not appended) on every upload: existing weeks are overwritten with the latest forecast, weeks not in the current upload are left untouched. Refreshed ad hoc via this notebook, not on the weekly orchestration schedule.'
""")

print(f'   ✅ Target table ready: {FULL_TABLE}')

# ── Step 2: Register validated rows as a temp view (staging, no physical table needed) ───────
print('\n📤 Registering staging temp view...')

df_spark = spark.createDataFrame(df)
df_spark.createOrReplaceTempView('upv_forecast_staging')

print(f'   ✅ Staging view registered: upv_forecast_staging ({df_spark.count()} rows)')

# ── Step 3: MERGE staging -> Bronze ───────────────────────────────────────────
print('\n🔀 Running MERGE into Bronze table...')

merge_result = spark.sql(f"""
  MERGE INTO {FULL_TABLE} AS target
  USING upv_forecast_staging AS source
    ON target.week_sun_sat = source.week_sun_sat

  -- Existing week: overwrite forecast values and update metadata
  WHEN MATCHED THEN UPDATE SET
    target.upv_forecast        = source.upv_forecast,
    target.upv_webapp_forecast = source.upv_webapp_forecast,
    target.file_load_date      = source.file_load_date,
    target.source_filename     = source.source_filename

  -- New week: insert full row
  WHEN NOT MATCHED THEN INSERT (
    week_sun_sat,
    upv_forecast,
    upv_webapp_forecast,
    file_load_date,
    source_filename
  ) VALUES (
    source.week_sun_sat,
    source.upv_forecast,
    source.upv_webapp_forecast,
    source.file_load_date,
    source.source_filename
  )
""")

print('   ✅ MERGE complete')
display(merge_result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verify

# COMMAND ----------

df_verify = spark.sql(f"""
  SELECT
    week_sun_sat,
    upv_forecast,
    upv_webapp_forecast,
    file_load_date,
    source_filename
  FROM {FULL_TABLE}
  ORDER BY week_sun_sat DESC
  LIMIT 20
""")

total_rows = spark.sql(f'SELECT COUNT(*) AS n FROM {FULL_TABLE}').collect()[0]['n']

range_row = spark.sql(f"""
  SELECT
    MIN(week_sun_sat) AS min_dt,
    MAX(week_sun_sat) AS max_dt
  FROM {FULL_TABLE}
""").collect()[0]

print('📊 Bronze table preview, most recent 20 rows:')
print(f'   Total rows in table: {total_rows}')
print(f'   Date range in table: {range_row["min_dt"]} → {range_row["max_dt"]}')

display(df_verify)