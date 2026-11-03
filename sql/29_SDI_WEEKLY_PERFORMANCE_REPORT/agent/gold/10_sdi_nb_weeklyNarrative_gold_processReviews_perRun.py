# Databricks notebook source
# ==================================================================================================
# 10_sdi_nb_weeklyNarrative_gold_processReviews_perRun.py
# PHASE 3 | Review, Edit, Publish | one SERIALIZED writer job (max_concurrent_runs = 1)
#
# Design:
#   1. Many employees can generate different versions of ONE week (same report_id).
#   2. Many employees may review the same version. All decisions append; do not use last-wins.
#   2a. Latest WORKING is for display; most recent PUBLISH is the official report.
#   2b. Edits and publishing require an optimistic latest-working version token.
#   3. A reviewer can change THEIR own decision. Other reviewers' decisions remain active.
#   4. Conflicting active votes BLOCK publication. The status view shows REVIEW_CONFLICT.
#   5. No role/email fields; reviewer_id and reviewer_name from signed OIDC-backed API.
#   6. PUBLISH requires four fully accepted sections on its selected version, no outstanding
#      conflicting/negative votes, validated investigation, and an unchanged publication head.
#   7. Editing creates a NEW immutable version; never edits parent; review votes do not carry over.
#   8. Browser request idempotency; signed jobs + one worker prevent simultaneous writes.
#   9. Other Databricks users must not be able to launch this Job or edit audit tables directly.
# ==================================================================================================
import json, uuid, hmac, hashlib
from datetime import datetime, timezone
from pyspark.sql import functions as F

BASE = 'prdrzranalytics.lab42'
VERSIONS = f'{BASE}.sdi_tbl_weeklyNarrative_gold_reportVersions_wide'
REVIEWS = f'{BASE}.sdi_tbl_weeklyNarrative_gold_reviewEvents_long'
INVESTIGATIONS = f'{BASE}.sdi_tbl_weeklyNarrative_gold_investigations_wide'
SECTIONS = {'WHAT_HAPPENED', 'WHY_IT_HAPPENED', 'IS_IT_MEANINGFUL', 'RECOMMENDATIONS_AND_RISKS'}

# A signed command prevents a caller with Job RUN permission from impersonating a reviewer.
# Sign the exact UTF-8 command JSON string in the authenticated backend.
dbutils.widgets.text('command_json', '')
dbutils.widgets.text('command_signature', '')
command_json = dbutils.widgets.get('command_json')
signature = dbutils.widgets.get('command_signature')
secret = dbutils.secrets.get('weeklyNarrative', 'command_hmac_key').encode()
computed = hmac.new(secret, command_json.encode(), hashlib.sha256).hexdigest()
if not signature or not hmac.compare_digest(signature, computed):
    raise PermissionError('The review command is not API-signed')
command = json.loads(command_json)
operation = command.get('operation')
if operation not in ('REVIEW', 'EDIT', 'PUBLISH'):
    raise ValueError('Unsupported operation')
identity = command.get('identity') or {}
reviewer_id = str(identity.get('user_id') or '').strip()
reviewer_name = str(identity.get('display_name') or '').strip()
if not reviewer_id or not reviewer_name:
    raise ValueError('Authenticated reviewer ID and name are mandatory')
request_id = str(command.get('client_request_id') or '').strip()
if len(request_id) < 12 or len(request_id) > 128:
    raise ValueError('client_request_id must be a stable 12-128 character token')
version_id = command.get('version_id') or command.get('parent_version_id')
parent = spark.table(VERSIONS).where(F.col('version_id') == version_id).limit(2).collect()
if len(parent) != 1:
    raise ValueError('Exactly one parent version must exist')
v = parent[0]

# Shared helpers: one worker only; Delta tables' informational PKs alone do not enforce uniqueness.
def records(table, predicate, count_limit=2):
    return spark.table(table).where(predicate).limit(count_limit).collect()

def insert_one(table, row):
    schema = spark.table(table).schema
    data = [tuple(row.get(field.name) for field in schema.fields)]
    spark.createDataFrame(data, schema).write.format('delta').mode('append').saveAsTable(table)

def stop_if_published():
    if records(REVIEWS, (F.col('version_id') == version_id) & (F.col('review_action') == 'PUBLISH'), 1):
        raise RuntimeError('Published versions cannot receive further edits/reviews')

# ---- EDIT: editing an older tab must not silently replace newer work. -------------------------
# Allow editing a published version: that creates an UNPUBLISHED new revision; the published
# version remains the official report until a subsequent explicit PUBLISH.
# The UI keeps the user's unsaved text if this job reports STALE_WORKING_VERSION.
def current_latest_working(report_id):
    rows=(spark.table(VERSIONS).where(F.col('report_id')==report_id)
          .orderBy(F.desc('created_at'),F.desc('version_id')).limit(1).collect())
    return rows[0].version_id if rows else None

if operation == 'EDIT':
    expected = str(command.get('expected_latest_working_version_id') or '').strip()
    actual = current_latest_working(v.report_id)
    if not expected or expected != actual or version_id != actual:
        raise RuntimeError('STALE_WORKING_VERSION: newer saved version exists; reload, compare and retry')
    key = str(command.get('section_key') or '')
    text = str(command.get('corrected_text') or '').strip()
    if key not in SECTIONS or not text or len(text) > 14000:
        raise ValueError('Provide one valid section and 1-14000 characters')
    prior = records(VERSIONS, F.col('generation_request_id') == request_id)
    if prior:
        if len(prior) != 1 or prior[0].created_by_user_id != reviewer_id:
            raise RuntimeError('Idempotency key collision')
        dbutils.notebook.exit(json.dumps({'version_id':prior[0].version_id,'status':'ALREADY_CREATED'}))
    doc = json.loads(v.sections_json)
    sections = doc.get('sections') or []
    if len(sections) != 4 or {x['section_key'] for x in sections} != SECTIONS:
        raise ValueError('Parent has an invalid four-section document')
    for section in sections:
        if section['section_key'] == key:
            section['text'] = text
            section['evidence_ids'] = []  # Human edits require renewed evidence review.
    new_version = 'VER_' + str(uuid.uuid5(uuid.NAMESPACE_URL,'WN|EDIT|'+request_id))
    insert_one(VERSIONS, {
       'version_id':new_version, 'report_id':v.report_id, 'week_end':v.week_end,
       'report_scope':v.report_scope, 'parent_version_id':version_id,
       'investigation_id':v.investigation_id, 'sections_json':json.dumps(doc,ensure_ascii=False),
       'author_type':'HUMAN_EDIT', 'generation_mode':'MANUAL_EDIT',
       'model_id':None, 'prompt_version':None,
       'created_by_user_id':reviewer_id,'created_by_display_name':reviewer_name,
       'generation_request_id':request_id,'created_at':datetime.now(timezone.utc)
    })
    dbutils.notebook.exit(json.dumps({'version_id':new_version,'status':'DRAFT'}))

# All review/publish commands are version-scoped; never force one global 'latest draft'.
if operation == 'REVIEW':
    stop_if_published()
    section = str(command.get('section_key') or '')
    action = str(command.get('review_action') or '')
    if section not in SECTIONS or action not in ('ACCEPT','REJECT','REQUEST_CHANGES'):
        raise ValueError('Invalid section or review action')
else:
    section, action = 'REPORT', 'PUBLISH'

comment = str(command.get('review_comment') or '').strip() or None
if action in ('REJECT', 'REQUEST_CHANGES') and not comment:
    raise ValueError('Rejection/changes requested require a meaningful comment')

# Same idempotency token, same logical action. Different payload is an error.
event_id = 'REV_' + str(uuid.uuid5(uuid.NAMESPACE_URL,'WN|REVIEW|'+request_id))
prior = records(REVIEWS,F.col('client_request_id') == request_id)
if prior:
    p = prior[0]
    if len(prior)!=1 or (p.version_id,p.section_key,p.review_action,p.reviewer_id) != (version_id,section,action,reviewer_id):
        raise RuntimeError('Idempotency key already used for a different action')
    dbutils.notebook.exit(json.dumps({'review_event_id':p.review_event_id,'status':'ALREADY_RECORDED'}))

# Optimistic check applies ONLY to this user's most recent vote on this section.
# Two different employees can vote concurrently without rejecting one another's action.
if operation == 'REVIEW':
    latest = (spark.table(REVIEWS).where(
       (F.col('version_id')==version_id) & (F.col('section_key')==section) &
       (F.col('reviewer_id')==reviewer_id) &
       F.col('review_action').isin('ACCEPT','REJECT','REQUEST_CHANGES'))
       .orderBy(F.desc('reviewed_at'), F.desc('review_event_id')).limit(1).collect())
    actual = latest[0].review_event_id if latest else None
    seen = command.get('seen_own_event_id') or None
    if actual != seen:
        raise RuntimeError('STALE_OWN_REVIEW: refresh before changing your own decision')
else:
    seen = None

metric_keys = command.get('related_metric_keys') or []
if not isinstance(metric_keys,list) or len(metric_keys)>12 or any(not isinstance(x,str) for x in metric_keys):
    raise ValueError('related_metric_keys must be at most 12 strings')
if metric_keys:
    investigations=records(INVESTIGATIONS,F.col('investigation_id')==v.investigation_id)
    if len(investigations)!=1: raise RuntimeError('Missing investigation')
    allowed={f['metric_key'] for f in json.loads(investigations[0].facts_json)}
    if not set(metric_keys).issubset(allowed):
        raise ValueError('related_metric_keys must occur in the version investigation')

# Publication must be atomic from the reader's perspective:
#  - One serialized writer for all PUBLISH operations
#  - Optimistic expected publication head AND latest-working ID prevent stale tabs replacing a report.
#  - A later deliberate re-publication requires explicit current head token.
if operation == 'PUBLISH':
    expected_working = str(command.get('expected_latest_working_version_id') or '').strip()
    actual_working = current_latest_working(v.report_id)
    if not expected_working or expected_working != actual_working or version_id != actual_working:
        raise RuntimeError('STALE_WORKING_VERSION: cannot publish a draft older than the latest saved version')
    expected = command.get('expected_published_version_id') or None
    current = (spark.table(REVIEWS).where(
        (F.col('report_id')==v.report_id) & (F.col('review_action')=='PUBLISH') &
        (F.col('section_key')=='REPORT'))
        .orderBy(F.desc('reviewed_at'), F.desc('review_event_id')).limit(1).collect())
    current_head = current[0].version_id if current else None
    if current_head != expected:
        raise RuntimeError('PUBLICATION_CONFLICT: another version was published; refresh')
    if current_head == version_id:
        raise RuntimeError('This version is already published')
    investigations = records(INVESTIGATIONS, F.col('investigation_id')==v.investigation_id)
    if len(investigations)!=1 or investigations[0].investigation_status!='VALIDATED':
        raise RuntimeError('Only versions backed by validated investigations can publish')
    # Current opinion of EACH person on EACH section.
    votes=(spark.table(REVIEWS).where(
       (F.col('version_id')==version_id) & F.col('section_key').isin(list(SECTIONS)) &
       F.col('review_action').isin('ACCEPT','REJECT','REQUEST_CHANGES'))
       .orderBy(F.desc('reviewed_at'),F.desc('review_event_id')).collect())
    latest_by_person={}
    for vote in votes:
        token=(vote.section_key,vote.reviewer_id)
        if token not in latest_by_person:
            latest_by_person[token]=vote.review_action
    for key in SECTIONS:
        actions=[action for (sec,reviewer),action in latest_by_person.items() if sec==key]
        if not actions or 'ACCEPT' not in actions or any(a!='ACCEPT' for a in actions):
            raise RuntimeError('PUBLISH_BLOCKED: every section needs >=1 ACCEPT and no negative/conflicting votes')

insert_one(REVIEWS, {
   'review_event_id':event_id,'client_request_id':request_id,
   'report_id':v.report_id,'version_id':version_id,'week_end':v.week_end,
   'section_key':section,'review_action':action,
   'related_metric_keys':metric_keys,'feedback_category':command.get('feedback_category'),
   'review_comment':comment,'suggested_text':command.get('suggested_text'),
   'reviewer_id':reviewer_id,'reviewer_name':reviewer_name,
   'reviewed_at':datetime.now(timezone.utc),'seen_own_event_id':seen
})
print(json.dumps({'review_event_id':event_id, 'version_id':version_id,
                  'review_action':action, 'reviewer_name':reviewer_name, 'status':'RECORDED'}))
