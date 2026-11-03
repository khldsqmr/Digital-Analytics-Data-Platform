# Databricks notebook source
# ======================================================================================
# TRACK B - SINGLE OPERATIONAL NOTEBOOK
# FILE: 14_sdi_nb_weeklyNarrative_orchestration_processNarratives_perRun.py
#
# Modes:
#   HISTORICAL_IMPORT = seed one-time JSONL from governed Volume -> Bronze -> Silver.
#   APP_SYNC          = sync every saved Gold report version -> Bronze -> Silver;
#                       re-derive per-section review classification and publish status.
#   REPROCESS         = no new Bronze ingestion, rebuild missing Silver and refresh metadata.
#
# Dependency rule: HISTORICAL_IMPORT runs BEFORE Agent objects exist; only APP_SYNC
# requires Gold Report Versions, Review Events and the Report Status view.
#
# Operational contracts:
#   - Serialize this notebook as a Databricks Job with max_concurrent_runs=1.
#   - Use restricted service principal; no direct writes from the frontend.
#   - MERGE on deterministic IDs is retry-safe for a single writer; Delta does
#     not enforce uniqueness if unrelated writers bypass that contract.
#   - Bronze raw_json is IMMUTABLE. Reviews only change Silver classifications.
#   - Original passages are copied EXACTLY to Silver. AI must not rewrite them.
#   - Historical original publication time is unknown; ingestion timestamp is
#     not a safe historical as-of time for backtests.
#   - INTERACTIVE IMPORT: optional ipywidgets FileUpload for SMALL JSONL files.
#     Databricks native File > Upload files to volume is the safer large-file path.
#     ipywidgets is not used for automated Databricks Jobs.
# ======================================================================================
import json, re, uuid
from datetime import datetime
from pyspark.sql import functions as F, types as T

BASE = 'prdrzranalytics.lab42'
BRONZE = f'{BASE}.sdi_tbl_weeklyNarrative_bronze_narrativeBank_weekly'
SILVER = f'{BASE}.sdi_tbl_weeklyNarrative_silver_narrativeChunks_weekly'
REGISTRY = f'{BASE}.sdi_vw_weeklyNarrative_control_metricRegistry_static'
SOURCE_REGISTRY = f'{BASE}.sdi_vw_weeklyNarrative_control_narrativeSources_static'
VERSIONS = f'{BASE}.sdi_tbl_weeklyNarrative_gold_reportVersions_wide'
REVIEWS = f'{BASE}.sdi_tbl_weeklyNarrative_gold_reviewEvents_long'
STATUS = f'{BASE}.sdi_vw_weeklyNarrative_gold_reportStatus_wide'
SECTIONS = {
 'what happened': 'WHAT_HAPPENED',
 'why it happened': 'WHY_IT_HAPPENED',
 'is it meaningful?': 'IS_IT_MEANINGFUL',
 'recommendations & risks to watch': 'RECOMMENDATIONS_AND_RISKS'
}

# COMMAND ----------
dbutils.widgets.dropdown('run_mode','HISTORICAL_IMPORT',['HISTORICAL_IMPORT','APP_SYNC','REPROCESS'])
dbutils.widgets.text('historical_path','/Volumes/prdrzranalytics/lab42/weeklyNarrative_inputs/narrative_bank.jsonl')
mode=dbutils.widgets.get('run_mode').strip().upper()
path=dbutils.widgets.get('historical_path').strip()

# COMMAND ----------
# OPTIONAL INTERACTIVE UPLOAD — HISTORICAL_IMPORT only.
#
# INSTRUCTIONS:
#   A. Attach this notebook to a running Databricks cluster supporting ipywidgets.
#   B. Set run_mode = HISTORICAL_IMPORT.
#   C. Run the widget cell below. Click "Choose JSONL file" and choose the file
#      on your laptop, then click "Save upload to Unity Catalog Volume".
#   D. Wait until you see the saved /Volumes/... path in green.
#   E. Run the remaining notebook cells BELOW this cell; they use the updated
#      `path` variable and ingest the selected file into Bronze -> Silver.
#
# NOTE: The browse/upload button moves bytes through the notebook widget
# communication channel. Databricks documents a 5 MB message payload limit.
# This implementation caps uploads at 2 MiB (suitable for the 11-record seed).
# For larger files, use Databricks notebook menu:
#   File > Upload files to volume
# then set the `historical_path` notebook parameter and run the notebook.
# Scheduled jobs should always use historical_path and skip this optional UI.

import os
import uuid as _upload_uuid
from pathlib import Path

WIDGET_UPLOAD_MAX_BYTES = 2 * 1024 * 1024
UPLOADED_HISTORICAL_PATH = None

if mode == 'HISTORICAL_IMPORT':
    try:
        import ipywidgets as ipw
        from IPython.display import display as show_widget
    except ImportError:
        print('Interactive upload unavailable: ipywidgets is not installed.')
        print('Use File > Upload files to volume, then set historical_path.')
    else:
        upload_dir = ipw.Text(
            value='/Volumes/prdrzranalytics/lab42/weeklyNarrative_inputs',
            description='Volume folder:',
            layout=ipw.Layout(width='96%'),
            style={'description_width': 'initial'}
        )
        file_upload = ipw.FileUpload(accept='.jsonl', multiple=False,
                                     description='Choose JSONL file')
        save_upload_button = ipw.Button(
            description='Save upload to Unity Catalog Volume',
            button_style='primary', icon='upload',
            layout=ipw.Layout(width='320px')
        )
        upload_output = ipw.Output()

        def _selected_file(widget):
            # Supports ipywidgets 7 (dict) and 8 (tuple) FileUpload values.
            files = widget.value
            if isinstance(files, dict):
                if len(files) != 1:
                    raise ValueError('Choose exactly one .jsonl file.')
                name, info = next(iter(files.items()))
                return str(info.get('name', name)), bytes(info['content'])
            if not files or len(files) != 1:
                raise ValueError('Choose exactly one .jsonl file.')
            info = files[0]
            return str(info['name']), bytes(info['content'])

        def _save_uploaded_file(_):
            global path, UPLOADED_HISTORICAL_PATH
            with upload_output:
                upload_output.clear_output()
                try:
                    filename, contents = _selected_file(file_upload)
                    if Path(filename).name != filename or not filename.lower().endswith('.jsonl'):
                        raise ValueError('Only a plain .jsonl filename is allowed.')
                    if not contents or len(contents) > WIDGET_UPLOAD_MAX_BYTES:
                        raise ValueError('File must be nonempty and <= 2 MiB. Use native Volume upload for larger files.')
                    # Validate the entire JSONL BEFORE creating a file.
                    lines = [line for line in contents.decode('utf-8-sig').splitlines() if line.strip()]
                    if not lines:
                        raise ValueError('No JSONL records found.')
                    for line_no, line in enumerate(lines, start=1):
                        doc = json.loads(line)
                        if not isinstance(doc, dict) or not doc.get('id') or not doc.get('week_end'):
                            raise ValueError(f'JSONL line {line_no} must contain id and week_end.')
                    folder = Path(upload_dir.value.strip())
                    if not str(folder).startswith('/Volumes/') or '..' in folder.parts:
                        raise ValueError('Destination must be a valid /Volumes/... folder.')
                    if not folder.is_dir():
                        raise ValueError(f'Volume folder does not exist or is inaccessible: {folder}')
                    suffix = _upload_uuid.uuid4().hex[:12]
                    safe_name = f'wn_historical_{suffix}.jsonl'
                    dest = folder / safe_name
                    # Exclusive creation ensures an upload never overwrites another file.
                    with open(dest, 'xb') as handle:
                        handle.write(contents)
                    UPLOADED_HISTORICAL_PATH = str(dest)
                    path = str(dest)  # Used by HISTORICAL_IMPORT code in subsequent cells.
                    print(f'Saved {len(lines)} JSONL records to: {path}')
                    print('Next: run the remaining notebook cells to ingest Bronze and Silver.')
                except Exception as exc:
                    print(f'Upload failed: {type(exc).__name__}: {exc}')

        save_upload_button.on_click(_save_uploaded_file)
        show_widget(ipw.VBox([
            ipw.HTML('<b>Historical narrative upload (optional)</b><p>For small JSONL files only. Upload does not ingest until you run the cells below.</p>'),
            upload_dir, file_upload, save_upload_button, upload_output
        ]))
else:
    print(f'run_mode={mode}: interactive upload skipped; using existing sources.')

def enabled_source(source_system):
    r=spark.table(SOURCE_REGISTRY).where((F.col('source_system')==source_system)&F.col('is_enabled')).collect()
    if len(r)!=1: raise RuntimeError(f'Source disabled or duplicated: {source_system}')

def table_exists(table_name):
    try: return spark.catalog.tableExists(table_name)
    except Exception: return False

def merge_new_rows(target,rows_df, temp_view):
    # NOTE: this assumes one controlled writer and unique source IDs in incoming batch.
    fields=spark.table(target).columns
    rows_df.select(*fields).createOrReplaceTempView(temp_view)
    spark.sql(f'''MERGE INTO {target} t USING {temp_view} s ON t.{fields[0]}=s.{fields[0]}
      WHEN NOT MATCHED THEN INSERT *''')

def validate_bronze_batch(df):
    invalid=df.filter(F.col('report_id').isNull()|F.col('source_id').isNull()|
                      F.col('week_end').isNull()|(F.dayofweek('week_end')!=7))
    if invalid.limit(1).count(): raise ValueError('Invalid Bronze report ID or Saturday date')
    if df.groupBy('source_id').count().where('count>1').limit(1).count():
        raise ValueError('Duplicate source ID in incoming batch')
    # Reject the same native source key being reused with different content.
    conflicting=(df.select('source_system','source_key','content_hash').alias('s')
         .join(spark.table(BRONZE).select('source_system','source_key','content_hash').alias('t'),
               ['source_system','source_key'],'inner')
         .where(F.col('s.content_hash')!=F.col('t.content_hash')).limit(1).count())
    if conflicting: raise ValueError('Same source_key now has different raw content; source is immutable')

# COMMAND ----------
if mode=='HISTORICAL_IMPORT':
    enabled_source('HISTORICAL_JSONL')
    if not (path.startswith('/Volumes/') and path.endswith('.jsonl')):
        raise ValueError('Use a governed JSONL path under /Volumes/...')
    raw=spark.read.text(path).select(F.col('value').alias('raw_json')).where(F.length(F.trim('raw_json'))>0)
    parsed=(raw.withColumn('original_source_id',F.get_json_object('raw_json','$.id'))
      .withColumn('week_end',F.to_date(F.get_json_object('raw_json','$.week_end')))
      .withColumn('title',F.get_json_object('raw_json','$.title'))
      .withColumn('decision',F.get_json_object('raw_json','$.decision')))
    if parsed.where(F.col('original_source_id').isNull()|F.col('week_end').isNull()|
                    (F.dayofweek('week_end')!=7)|(F.col('decision')!='published')).limit(1).count():
        raise ValueError('Historical data needs id, Saturday week_end, published decision')
    parsed=(parsed.withColumn('source_system',F.lit('HISTORICAL_JSONL'))
      .withColumn('source_type',F.lit('HISTORICAL_IMPORT'))
      .withColumn('ingestion_mode',F.lit('ONE_TIME_BACKFILL'))
      .withColumn('source_key',F.concat(F.lit('hist:'),F.col('original_source_id')))
      .withColumn('report_id',F.concat(F.lit('WN_'),F.date_format('week_end','yyyyMMdd'),F.lit('_ALL')))
      .withColumn('version_id',F.lit(None).cast('string'))
      .withColumn('content_hash',F.sha2('raw_json',256))
      .withColumn('source_id',F.concat(F.lit('SRC_'),F.sha2(F.concat_ws('|','source_system','source_key','content_hash'),256)))
      .withColumn('source_status_at_ingest',F.lit('HISTORICAL_PUBLISHED'))
      .withColumn('created_by_user_id',F.lit(None).cast('string'))
      .withColumn('created_by_name',F.lit(None).cast('string'))
      .withColumn('source_uri',F.lit(path))
      .withColumn('original_available_at',F.lit(None).cast('timestamp'))
      .withColumn('ingested_at',F.current_timestamp()))
    validate_bronze_batch(parsed)
    merge_new_rows(BRONZE,parsed,'wn_hist_bronze_staging')
    print('Historical rows ingested',parsed.count())

elif mode=='APP_SYNC':
    enabled_source('WEEKLY_NARRATIVE_APP')
    if not all(table_exists(t) for t in [VERSIONS,REVIEWS,STATUS]):
        raise RuntimeError('APP_SYNC requires Agent Gold Report Versions, Review Events and Report Status view')
    versions=spark.table(VERSIONS)
    # Bronze schema expects the same 4-section shape as historical JSONL.
    # Convert the app section dictionary/array into source `steps` without
    # modifying authored text. `sections_json` is preserved in payload for audit.
    def app_payload(section_json, week_end, version_id):
        original=json.loads(section_json)
        canonical={'WHAT_HAPPENED':'What happened',
                   'WHY_IT_HAPPENED':'Why it happened',
                   'IS_IT_MEANINGFUL':'Is it meaningful?',
                   'RECOMMENDATIONS_AND_RISKS':'Recommendations & risks to watch'}
        if isinstance(original,dict) and 'sections' in original:original=original['sections']
        steps=[]
        if isinstance(original,list):
            for x in original:
                key=x.get('section_key') or x.get('key')
                v=x.get('text') or x.get('items') or ''
                items=v if isinstance(v,list) else [v]
                if key in canonical:steps.append({'title':canonical[key],'items':[str(z) for z in items if str(z).strip()]})
        elif isinstance(original,dict):
            for key,title in canonical.items():
                v=original.get(key,'')
                if isinstance(v,dict):v=v.get('text') or v.get('items') or ''
                items=v if isinstance(v,list) else [v]
                steps.append({'title':title,'items':[str(z) for z in items if str(z).strip()]})
        if len(steps)!=4 or not all(s['items'] for s in steps):
            raise ValueError('Saved app report must have four nonempty supported sections')
        return json.dumps({'id':version_id,'week_end':str(week_end),'steps':steps},ensure_ascii=False)
    payload_udf=F.udf(app_payload,T.StringType())
    app=(versions.withColumn('raw_json',payload_udf('sections_json','week_end','version_id'))
      .withColumn('source_system',F.lit('WEEKLY_NARRATIVE_APP'))
      .withColumn('source_type',F.lit('APP_VERSION'))
      .withColumn('ingestion_mode',F.lit('APP_SYNC'))
      .withColumn('source_key',F.concat(F.lit('app:'),F.col('version_id')))
      .withColumn('original_source_id',F.col('version_id'))
      .withColumn('title',F.concat(F.lit('Weekly narrative '),F.col('report_id')))
      .withColumn('content_hash',F.sha2('raw_json',256))
      .withColumn('source_id',F.concat(F.lit('SRC_'),F.sha2(F.concat_ws('|','source_system','source_key','content_hash'),256)))
      .withColumn('source_status_at_ingest',F.lit('DRAFT'))
      .withColumn('created_by_user_id',F.col('created_by_user_id'))
      .withColumn('created_by_name',F.col('created_by_display_name'))
      .withColumn('source_uri',F.concat(F.lit('app://'),F.col('version_id')))
      .withColumn('original_available_at',F.col('created_at'))
      .withColumn('ingested_at',F.current_timestamp()))
    validate_bronze_batch(app)
    merge_new_rows(BRONZE,app,'wn_app_bronze_staging')
    print('App source versions available:',app.count())

# COMMAND ----------
# Bronze -> Silver. All modes use this SAME parser. It has no Agent dependency
# during HISTORICAL_IMPORT, and APP_SYNC uses Agent status only if deployed.
section_schema='STRUCT<steps:ARRAY<STRUCT<title:STRING,items:ARRAY<STRING>>>>'
bronze=spark.table(BRONZE)
bronze=(bronze.withColumn('parsed',F.from_json('raw_json',section_schema))
  .select('*',F.posexplode_outer('parsed.steps').alias('section_index','section'))
  .select('*',F.posexplode_outer('section.items').alias('item_index','chunk_text')))
# SQL expression maps the four canonical titles; all other sections get safe generated keys.
section_expr='''CASE lower(trim(section.title))
 WHEN 'what happened' THEN 'WHAT_HAPPENED'
 WHEN 'why it happened' THEN 'WHY_IT_HAPPENED'
 WHEN 'is it meaningful?' THEN 'IS_IT_MEANINGFUL'
 WHEN 'recommendations & risks to watch' THEN 'RECOMMENDATIONS_AND_RISKS'
 ELSE upper(regexp_replace(trim(section.title),'[^A-Za-z0-9]+','_')) END'''
chunks=(bronze.where(F.col('chunk_text').isNotNull()&(F.length(F.trim('chunk_text'))>0))
  .withColumn('section_key',F.expr(section_expr))
  .withColumn('chunk_id',F.concat(F.lit('CHK_'),F.sha2(F.concat_ws('|',F.col('source_id'),
         F.col('section_index').cast('string'),F.col('item_index').cast('string')),256)))
  .withColumn('search_text',F.concat_ws(' ',F.col('section.title'),F.col('chunk_text')))
  .withColumnRenamed('week_end','report_week_end'))

# Metric tags: generate CONTROLLED CANDIDATES from current registry aliases.
# No LLM needed. Aliases like 'upv' can match multiple metrics; these remain
# candidates, NEVER assertions of verified scope. Add reviewed AI tagging later.
registry=(spark.table(REGISTRY).where(F.col('is_active')==True)
  .select('metric_key','aliases').collect())
patterns=[]
for r in registry:
    for alias in (r.aliases or []):
        a=alias.strip().lower()
        if len(a)>=3:patterns.append((r.metric_key,a))
# Bound keyword count to prevent accidental uncontrolled giant SQL expressions.
if len(patterns)>3000:raise RuntimeError('Registry alias list requires scalable matching implementation')
def detect_keys(text):
    normalized=' '+re.sub(r'[^a-z0-9]+',' ',(text or '').lower())+' '
    out=set()
    for key,alias in patterns:
        a=' '+re.sub(r'[^a-z0-9]+',' ',alias)+' '
        if a in normalized:out.add(key)
    return sorted(out)
tag_udf=F.udf(detect_keys,T.ArrayType(T.StringType()))
chunks=(chunks.withColumn('metric_keys',tag_udf('chunk_text'))
  .withColumn('tag_status',F.when(F.size('metric_keys')>0,'RULE_CANDIDATE').otherwise('NOT_TAGGED')))

# PHASE 1 uses the imported publication marker, without Gold Review dependencies.
# PHASE 3+ uses version status from Agent and the per-reviewer latest vote per section.
if mode=='HISTORICAL_IMPORT' and not table_exists(STATUS):
    chunks=chunks.withColumn('live_status',F.when(F.col('source_type')=='HISTORICAL_IMPORT',
        F.lit('HISTORICAL_PUBLISHED')).otherwise(F.lit('DRAFT')))
    chunks=chunks.withColumn('last_review_event_id',F.lit(None).cast('string'))
    chunks=chunks.withColumn('negative_vote',F.lit(False))
elif table_exists(STATUS) and table_exists(REVIEWS):
    status=spark.table(STATUS).select('version_id','report_status')
    chunks=(chunks.join(status,'version_id','left')
      .withColumn('live_status',F.when(F.col('source_type')=='HISTORICAL_IMPORT',
         F.lit('HISTORICAL_PUBLISHED')).otherwise(F.coalesce(F.col('report_status'),F.lit('DRAFT')))))
    # Latest vote PER REVIEWER and section. A rejected vote remains active until
    # THAT reviewer changes their vote. Other reviewers cannot silently erase it.
    review=spark.table(REVIEWS).where(F.col('section_key').isin(list(SECTIONS.values())))
    from pyspark.sql.window import Window
    w=Window.partitionBy('version_id','section_key','reviewer_id').orderBy(
        F.col('reviewed_at').desc(),F.col('review_event_id').desc())
    latest=(review.withColumn('rn',F.row_number().over(w)).where('rn=1')
      .groupBy('version_id','section_key').agg(
          F.max(F.when(F.col('review_action').isin('REJECT','REQUEST_CHANGES'),F.lit(1)).otherwise(F.lit(0))).alias('any_negative'),
          F.max_by('review_event_id','reviewed_at').alias('last_review_event_id')))
    chunks=(chunks.join(latest,['version_id','section_key'],'left')
      .withColumn('negative_vote',F.coalesce(F.col('any_negative'),F.lit(0))>0))
else:
    chunks=chunks.withColumn('live_status',F.when(F.col('source_type')=='HISTORICAL_IMPORT',
        F.lit('HISTORICAL_PUBLISHED')).otherwise(F.lit('DRAFT')))
    chunks=chunks.withColumn('last_review_event_id',F.lit(None).cast('string'))
    chunks=chunks.withColumn('negative_vote',F.lit(False))

# Only CURRENTLY PUBLISHED report versions are positive evidence. A superseded
# publication is retained for lineage but not selected as current evidence.
chunks=(chunks.withColumn('is_evidence_eligible',F.col('live_status').isin('PUBLISHED','HISTORICAL_PUBLISHED'))
 .withColumn('is_feedback_eligible',F.col('negative_vote'))
 .withColumn('knowledge_role',
   F.when(F.col('is_evidence_eligible'),F.lit('APPROVED_EVIDENCE'))
    .when(F.col('negative_vote'),F.lit('NEGATIVE_EXAMPLE'))
    .when(F.col('live_status')=='SUPERSEDED',F.lit('SUPERSEDED_EVIDENCE'))
    .otherwise(F.lit('PENDING_REVIEW')))
 .withColumn('review_status',F.col('live_status'))
 .withColumn('created_at',F.current_timestamp())
 .withColumn('classified_at',F.current_timestamp()))
needed=spark.table(SILVER).columns
chunks.select(*needed).createOrReplaceTempView('wn_silver_incoming')
spark.sql(f'''MERGE INTO {SILVER} t USING wn_silver_incoming s ON t.chunk_id=s.chunk_id
 WHEN MATCHED THEN UPDATE SET
  t.metric_keys=s.metric_keys,t.tag_status=s.tag_status,t.review_status=s.review_status,
  t.knowledge_role=s.knowledge_role,t.is_evidence_eligible=s.is_evidence_eligible,
  t.is_feedback_eligible=s.is_feedback_eligible,t.last_review_event_id=s.last_review_event_id,
  t.classified_at=current_timestamp()
 WHEN NOT MATCHED THEN INSERT *''')
print('Bronze:',spark.table(BRONZE).count(),'Silver:',spark.table(SILVER).count())
display(spark.table(SILVER).groupBy('source_type','knowledge_role').count())
