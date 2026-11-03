# Databricks notebook source
# FILE: 01_app.py
# LAYER: agent/api
# PURPOSE: Layer-local Python notebook; see documentation for external dependencies
# INPUT/OUTPUT: Governed Databricks objects and user-approved job parameters
# OWNERSHIP: Service principal / version-controlled review; no frontend table writes
# Weekly Narrative multi-user API. Reference implementation for Databricks Workflows.
# Requirements: OIDC issuer/audience/JWKS, rate limits, service principal.
# Review/edit/publish worker must be serialized. Job requests are ASYNCHRONOUS.
# A queued response is not proof that the requested action has committed successfully.
# Only backend sends trusted identity/role params; job RUN permission is restricted.
import json,os,uuid,hmac,hashlib
from datetime import date
from functools import lru_cache
from typing import Literal,Optional
import jwt
from jwt import PyJWKClient
from fastapi import FastAPI,Depends,Header,HTTPException
from pydantic import BaseModel,Field
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config,oauth_service_principal

app=FastAPI(title='Weekly Narrative API',version='5.0')
BASE='prdrzranalytics.lab42'
SECTIONS={'WHAT_HAPPENED','WHY_IT_HAPPENED','IS_IT_MEANINGFUL','RECOMMENDATIONS_AND_RISKS'}

class Principal(BaseModel):
    user_id:str
    display_name:str


@lru_cache
# Config evaluated once on startup, environment must be secured.
def jwks_client():return PyJWKClient(os.environ['OIDC_JWKS_URL'])

def principal(authorization:str=Header(...,alias='Authorization')) -> Principal:
    if not authorization.startswith('Bearer '):raise HTTPException(401,'Bearer required')
    try:
        token=authorization[7:]
        key=jwks_client().get_signing_key_from_jwt(token).key
        claims=jwt.decode(token,key,algorithms=['RS256'],issuer=os.environ['OIDC_ISSUER'],
                          audience=os.environ['OIDC_AUDIENCE'],options={'require':['sub','exp']})
    except Exception as exc:raise HTTPException(401,'Invalid authentication') from exc
    groups=claims.get('groups') or []
    if isinstance(groups,str): groups=[groups]
    # Identity and app access are separate. No reviewer profile or role is persisted.
    allowed=set(filter(None,os.getenv('WN_ALLOWED_GROUPS','weeklyNarrative.users').split(',')))
    if not allowed.intersection(groups):raise HTTPException(403,'Weekly Narrative access required')
    return Principal(user_id=str(claims['sub']),
        display_name=str(claims.get('name') or claims.get('preferred_username') or claims['sub']))

def client():
    return WorkspaceClient(host='https://'+os.environ['DATABRICKS_SERVER_HOSTNAME'],
       client_id=os.environ['DATABRICKS_CLIENT_ID'],client_secret=os.environ['DATABRICKS_CLIENT_SECRET'])

def creds():
    return oauth_service_principal(Config(host='https://'+os.environ['DATABRICKS_SERVER_HOSTNAME'],
       client_id=os.environ['DATABRICKS_CLIENT_ID'],client_secret=os.environ['DATABRICKS_CLIENT_SECRET']))

def query(sql_text,params):
    with sql.connect(server_hostname=os.environ['DATABRICKS_SERVER_HOSTNAME'],
                     http_path=os.environ['DATABRICKS_HTTP_PATH'],credentials_provider=creds) as cn:
        with cn.cursor() as cur:
            cur.execute(sql_text,params)
            names=[c[0] for c in cur.description]
            return [dict(zip(names,row)) for row in cur.fetchall()]

def enqueue(job_env,parameters):
    try:
        run=client().jobs.run_now(job_id=int(os.environ[job_env]),notebook_params=parameters)
        return {'status':'QUEUED','job_run_id':run.run_id}
    except Exception as exc:raise HTTPException(503,'Could not queue job') from exc

class GenerateBody(BaseModel):
    week_end:date
    scope:Literal['ALL','POSTPAID','BROADBAND']='ALL'
    mode:Literal['HUMAN_FIRST','AGENT_ASSISTED']='HUMAN_FIRST'
    parent_version_id:Optional[str]=None
    use_vector:bool=False
    use_graph:bool=False
    client_request_id:str=Field(min_length=12,max_length=128)

class ReviewBody(BaseModel):
    version_id:str
    section_key:Literal['WHAT_HAPPENED','WHY_IT_HAPPENED','IS_IT_MEANINGFUL','RECOMMENDATIONS_AND_RISKS']
    review_action:Literal['ACCEPT','REJECT','REQUEST_CHANGES']
    client_request_id:str=Field(min_length=12,max_length=128)
    seen_own_event_id:Optional[str]=None
    related_metric_keys:list[str]=Field(default_factory=list,max_length=12)
    feedback_category:Optional[str]=Field(default=None,max_length=80)
    review_comment:Optional[str]=Field(default=None,max_length=4000)
    suggested_text:Optional[str]=Field(default=None,max_length=14000)

class PublishBody(BaseModel):
    version_id:str
    # Publish only the exact draft that the user is seeing, unless deliberately refreshed.
    expected_latest_working_version_id:str
    expected_published_version_id:Optional[str]=None
    client_request_id:str=Field(min_length=12,max_length=128)
    seen_own_event_id:Optional[str]=None

class EditBody(BaseModel):
    parent_version_id:str
    # Mandatory optimistic UI token: the latest working version displayed when editing began.
    expected_latest_working_version_id:str
    section_key:Literal['WHAT_HAPPENED','WHY_IT_HAPPENED','IS_IT_MEANINGFUL','RECOMMENDATIONS_AND_RISKS']
    corrected_text:str=Field(min_length=1,max_length=14000)
    client_request_id:str=Field(min_length=12,max_length=128)

@app.post('/narratives/generate',status_code=202)
def generate(req:GenerateBody,who:Principal=Depends(principal)):
    if req.week_end.weekday()!=5 or req.week_end>=date.today():
        raise HTTPException(400,'week_end must be a completed Saturday')
    # The frontend supplies desired week and scope, not SQL or model privileges.
    return enqueue('DATABRICKS_GENERATION_JOB_ID',{
       'week_end':req.week_end.isoformat(),'scope':req.scope,'mode':req.mode,
       'llm_endpoint':os.environ['LLM_ENDPOINT'],
       'generation_request_id':req.client_request_id,
       'requested_by_user_id':who.user_id,'requested_by_display_name':who.display_name,
       'parent_version_id':req.parent_version_id or '',
       'use_vector':str(req.use_vector).lower(),'use_graph':str(req.use_graph).lower(),
       'ai_search_endpoint':os.getenv('AI_SEARCH_ENDPOINT','REPLACE_WITH_SEARCH_ENDPOINT')})

@app.get('/narratives/heads/{week_end}')
def report_heads(week_end:date,scope:Literal['ALL','POSTPAID','BROADBAND']='ALL',
                 who:Principal=Depends(principal)):
    """Default UI loader: latest working version and current published version side by side.
    The GET endpoint is read-only; no client can choose which draft counts as latest.
    """
    if week_end.weekday()!=5:raise HTTPException(400,'Saturday required')
    report_id=f'WN_{week_end:%Y%m%d}_{scope}'
    rows=query(f'''SELECT * FROM {BASE}.sdi_vw_weeklyNarrative_gold_reportHeads_wide
                    WHERE report_id=?''',[report_id])
    return rows[0] if rows else {'report_id':report_id,'latest_working_version_id':None,
                                 'published_version_id':None,'publication_sync_status':'NO_DRAFT'}

@app.get('/narratives/{week_end}')
def versions(week_end:date,scope:Literal['ALL','POSTPAID','BROADBAND']='ALL',
             who:Principal=Depends(principal)):
    if week_end.weekday()!=5:raise HTTPException(400,'Saturday required')
    rows=query(f'''SELECT v.version_id,v.report_id,v.week_end,v.report_scope,v.sections_json,
      s.version_number,s.report_status,v.created_by_display_name,v.created_at,
      s.accepted_sections,s.rejected_sections,s.flagged_sections
      FROM {BASE}.sdi_tbl_weeklyNarrative_gold_reportVersions_wide v
      JOIN {BASE}.sdi_vw_weeklyNarrative_gold_reportStatus_wide s ON v.version_id=s.version_id
      WHERE v.week_end=? AND v.report_scope=? ORDER BY v.created_at DESC LIMIT 30''',
      [week_end,scope])
    return {'week_end':week_end,'scope':scope,'versions':rows}

@app.get('/narratives/versions/{version_id}/reviews')
def version_reviews(version_id:str,who:Principal=Depends(principal)):
    if len(version_id)>128:raise HTTPException(400,'Invalid version ID')
    rows=query(f'''SELECT review_event_id,section_key,review_action,related_metric_keys,feedback_category,
     review_comment,reviewer_id,reviewer_name,reviewed_at
     FROM {BASE}.sdi_tbl_weeklyNarrative_gold_reviewEvents_long
     WHERE version_id=? ORDER BY reviewed_at,review_event_id''',[version_id])
    return {'version_id':version_id,'events':rows}

def submit_writer(payload,who):
    # All identity fields below come from verified OIDC, never from browser input.
    payload['identity']=who.model_dump()
    raw=json.dumps(payload,sort_keys=True,separators=(',',':'))
    secret=os.environ['WN_WORKER_HMAC_KEY'].encode('utf-8')
    signature=hmac.new(secret,raw.encode('utf-8'),hashlib.sha256).hexdigest()
    return enqueue('DATABRICKS_REVIEW_WORKER_JOB_ID',
                   {'command_json':raw,'command_signature':signature})

@app.post('/narratives/review',status_code=202)
def review(req:ReviewBody,who:Principal=Depends(principal)):
    if req.review_action!='ACCEPT' and not (req.review_comment or '').strip():
        raise HTTPException(400,'A reason is required for rejection/changes')
    return submit_writer({'operation':'REVIEW',**req.model_dump()},who)

@app.post('/narratives/publish',status_code=202)
def publish(req:PublishBody,who:Principal=Depends(principal)):
    return submit_writer({'operation':'PUBLISH',**req.model_dump()},who)

@app.post('/narratives/edit',status_code=202)
def edit(req:EditBody,who:Principal=Depends(principal)):
    return submit_writer({'operation':'EDIT',**req.model_dump()},who)

@app.get('/jobs/{job_run_id}')
def run_status(job_run_id:int,who:Principal=Depends(principal)):
    try:
        run=client().jobs.get_run(run_id=job_run_id)
        return {'job_run_id':job_run_id,'life_cycle_state':str(run.state.life_cycle_state),
                'result_state':str(run.state.result_state)}
    except Exception as exc:raise HTTPException(404,'Run unavailable') from exc
