# Databricks notebook source
# ============================================================================
# FILE: 01_sdi_nb_weeklyNarrative_gold_assembleContext_perRun.py
# LAYER: Context / Gold | PURPOSE: inspect pinned facts + eligible knowledge bundle
# READ-ONLY; generation notebook owns immutable investigations in Agent Gold.
# ============================================================================
from pyspark.sql import functions as F
import json
dbutils.widgets.text('week_end','2026-10-24')
dbutils.widgets.text('scope','ALL')
week=dbutils.widgets.get('week_end');scope=dbutils.widgets.get('scope').upper()
BASE='prdrzranalytics.lab42'
facts=spark.table(f'{BASE}.sdi_vw_weeklyNarrative_gold_factsLatest_long').where(
 (F.col('week_end')==F.lit(week).cast('date'))&(F.col('row_role')=='ACTUAL'))
if scope!='ALL':facts=facts.where(F.col('metric_lob')==scope)
ids=[r.snapshot_id for r in facts.select('snapshot_id').distinct().collect()]
if len(ids)!=1:raise RuntimeError('Context must pin one unique snapshot')
selected=facts.select('metric_key','channel_group','week_value','wow_pct','target_week_value',
 'source_validation_status','snapshot_id').limit(200)
passages=spark.table(f'{BASE}.sdi_tbl_weeklyNarrative_gold_approvedEvidence_long').where(
 F.col('report_week_end')<F.lit(week).cast('date')).orderBy(F.desc('report_week_end')).limit(20)
print(json.dumps({'snapshot_id':ids[0],'fact_count':selected.count(),
 'evidence_count':passages.count(),'note':'Read-only inspection; generated reports use Agent Gold investigation records.'}))
display(selected);display(passages)
