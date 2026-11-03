# Databricks notebook source
# ============================================================================
# PROJECT      : Weekly Narrative | Databricks UC | v7.1
# FILE         : 02_sdi_nb_weeklyNarrative_bronze_uploadHistorical_perRun.py
# LAYER        : Track B / Bronze
# PURPOSE      : ONE-TIME manual JSON / JSONL import without a UC Volume.
# INPUT        : Browser file upload via ipywidgets; small files <= 2 MiB.
# OUTPUT       : Immutable Bronze rows using a deterministic Delta MERGE.
# NEXT STEP    : CALL Track B Silver SP, then Gold SP REFRESH; optional AI notebook later.
# DEPENDENCIES : Bronze SP deployed; UC table CREATE/MODIFY permission.
# OWNERSHIP    : Human-operated, restricted to authorized ingestion operators.
# SAFETY       : No LLM trust or automatic publication. Source content immutable.
# IDEMPOTENCY  : Same source ID + same content -> no-op; changed content -> fail.
# NOTE         : This notebook is interactive; do NOT invoke in scheduled Jobs.
# ============================================================================

# COMMAND ----------
import json, hashlib, re
from datetime import date, datetime, timezone
from pyspark.sql import functions as F

BASE = 'prdrzranalytics.lab42'
BRONZE = f'{BASE}.sdi_tbl_weeklyNarrative_bronze_narrativeBank_weekly'
BRONZE_SP = f'{BASE}.sdi_sp_weeklyNarrative_bronze_narrativeBank_perRun'
SILVER_SP = f'{BASE}.sdi_sp_weeklyNarrative_silver_narrativeChunks_perRun'
MAX_BYTES = 2 * 1024 * 1024

# Creates Bronze via the layer-owned SP. No Volume is involved.
spark.sql(f"CALL {BRONZE_SP}('ENSURE_TABLE', NULL)")

# COMMAND ----------
# Databricks notebook widgets do NOT have a native laptop-file-upload control.
# ipywidgets.FileUpload works on compatible Databricks notebook compute.
# If ipywidgets is unavailable, arrange the initial upload via workspace UI
# or an approved file-transfer route instead of assuming a Volume exists.
import ipywidgets as widgets
from IPython.display import display

upload = widgets.FileUpload(accept='.json,.jsonl', multiple=False,
                            description='Choose JSON/JSONL')
commit_button = widgets.Button(description='Import to Bronze',button_style='primary',icon='upload')
output = widgets.Output()

def _get_file(w):
    value = w.value
    if isinstance(value, dict):  # ipywidgets 7
        if len(value) != 1: raise ValueError('Select exactly one file')
        name, data = next(iter(value.items()))
        return str(data.get('name',name)), bytes(data['content'])
    if len(value) != 1: raise ValueError('Select exactly one file')
    data = value[0]  # ipywidgets 8
    return str(data['name']), bytes(data['content'])

def _decode_records(filename, contents):
    if not filename.lower().endswith(('.json','.jsonl')) or '/' in filename or '\\' in filename:
        raise ValueError('Upload a plain .json or .jsonl filename')
    if not contents or len(contents) > MAX_BYTES:
        raise ValueError('File must be 1 byte to 2 MiB')
    txt = contents.decode('utf-8-sig')
    if filename.lower().endswith('.jsonl'):
        records = [json.loads(line) for line in txt.splitlines() if line.strip()]
        raw_strings = [line for line in txt.splitlines() if line.strip()]
    else:
        doc = json.loads(txt)
        records = doc if isinstance(doc,list) else [doc]
        raw_strings = [json.dumps(row,ensure_ascii=False,separators=(',',':')) for row in records]
    if not records or not all(isinstance(x,dict) for x in records):
        raise ValueError('Expected JSON object(s), or one JSON object per JSONL line')
    return records,raw_strings

def _prepare_rows(filename, contents):
    records,raw_strings=_decode_records(filename,contents)
    now=datetime.now(timezone.utc).replace(tzinfo=None)
    rows=[]
    keys=set()
    for idx,(record,raw) in enumerate(zip(records,raw_strings),1):
        native_id=str(record.get('id') or '').strip()
        date_str=str(record.get('week_end') or '')
        try: week=date.fromisoformat(date_str)
        except ValueError: raise ValueError(f'Record {idx}: invalid week_end')
        if not native_id or week.weekday()!=5:
            raise ValueError(f'Record {idx}: nonempty id and Saturday week_end required')
        if record.get('decision','').lower() != 'published':
            raise ValueError(f'Record {idx}: only published historical reports are accepted')
        steps=record.get('steps')
        if not isinstance(steps,list) or not steps:
            raise ValueError(f'Record {idx}: steps must be a nonempty array')
        for step in steps:
            if not isinstance(step,dict) or not isinstance(step.get('items'),list):
                raise ValueError(f'Record {idx}: every step needs an items array')
            if not all(isinstance(item,str) and item.strip() for item in step['items']):
                raise ValueError(f'Record {idx}: empty/non-text items not allowed')
        source_key=f'hist:{native_id}'
        if source_key in keys: raise ValueError('Duplicate historical id in upload')
        keys.add(source_key)
        content_hash=hashlib.sha256(raw.encode('utf-8')).hexdigest()
        source_id='SRC_'+hashlib.sha256(f'HISTORICAL_JSONL|{source_key}|{content_hash}'.encode()).hexdigest()
        rows.append(dict(
            source_id=source_id,source_system='HISTORICAL_JSONL',
            source_type='HISTORICAL_IMPORT',ingestion_mode='ONE_TIME_BACKFILL',
            source_key=source_key,original_source_id=native_id,
            report_id=f'WN_{week:%Y%m%d}_ALL',version_id=None,week_end=week,
            title=record.get('title'),raw_json=raw,content_hash=content_hash,
            source_status_at_ingest='HISTORICAL_PUBLISHED',
            created_by_user_id=None,created_by_name=None,
            source_uri=f'widget-upload://{filename}#{source_key}',
            original_available_at=None,ingested_at=now
        ))
    return rows

def _import_clicked(_):
    with output:
        output.clear_output()
        try:
            filename,contents=_get_file(upload)
            rows=_prepare_rows(filename,contents)
            # The browser upload is a one-time transfer. The Bronze raw_json is
            # the durable raw copy; a separate Volume object is NOT required.
            target=spark.table(BRONZE)
            batch=spark.createDataFrame(rows,schema=target.schema)
            # Detect upstream ID reuse with different payload BEFORE insert.
            existing=(batch.alias('s').join(target.alias('t'),
                (F.col('s.source_system')==F.col('t.source_system')) &
                (F.col('s.source_key')==F.col('t.source_key')),'inner')
                .where(F.col('s.content_hash')!=F.col('t.content_hash')))
            if existing.limit(1).count():
                raise ValueError('Existing historical ID has different raw content; import cancelled')
            tmp='wn_widget_bronze_import'
            batch.createOrReplaceTempView(tmp)
            spark.sql(f"MERGE INTO {BRONZE} t USING {tmp} s "
                      "ON t.source_id=s.source_id WHEN NOT MATCHED THEN INSERT *")
            present=spark.table(BRONZE).where(F.col('source_id').isin([r['source_id'] for r in rows])).count()
            print(f'Bronze import complete: {present} / {len(rows)} source records present.')
            print('Next: run the Silver processing procedure in the next cell.')
        except Exception as exc:
            print(f'IMPORT FAILED ({type(exc).__name__}): {exc}')
            raise

commit_button.on_click(_import_clicked)
display(widgets.VBox([widgets.HTML('<b>One-time historical upload — direct to Bronze (no Volume)</b>'),
                      upload,commit_button,output]))

# COMMAND ----------
# Run this separately AFTER a successful import message. Do not run before.
# A fully scoped Saturday can be passed instead of NULL if necessary.
# spark.sql(f"CALL {SILVER_SP}(NULL, FALSE)")
# display(spark.sql(f"SELECT count(*) AS bronze_reports FROM {BRONZE}"))
