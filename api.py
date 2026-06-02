"""
LeadScan AI — FastAPI Web Service
Render deployment entrypoint.

Endpoints:
    GET  /              — web dashboard (trigger runs, view jobs)
    GET  /health        — uptime / health check
    POST /run           — start a pipeline run, returns job_id
    GET  /jobs/{id}     — job status + live log
    GET  /results/{id}  — completed leads JSON
"""

import os
import uuid
import json
import time
import logging
import threading
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, BackgroundTasks, HTTPException, Security, Depends, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel

from pipeline import run_pipeline

# ─────────────────────────────────────────
# Auth
# ─────────────────────────────────────────
_bearer = HTTPBearer(auto_error=False)

def require_api_key(
    creds: HTTPAuthorizationCredentials = Security(_bearer),
    key: str = None,   # ?key= query parameter (for direct browser access)
):
    """
    Validates the API key against API_SECRET_KEY env var.
    Accepts:
      Authorization: Bearer <key>   (API clients, dashboard)
      ?key=<key>                    (direct browser URLs)
    If API_SECRET_KEY is not set, all access is open.
    """
    secret = os.environ.get("API_SECRET_KEY", "").strip()
    if not secret:
        return  # No key configured — open access

    token = (creds.credentials if creds else None) or key
    if token != secret:
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing API key. Pass it as: Authorization: Bearer <key>",
        )

# ─────────────────────────────────────────
# Logging
# ─────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger("leadscan.api")

# ─────────────────────────────────────────
# App
# ─────────────────────────────────────────
app = FastAPI(
    title       = "LeadScan AI",
    description = "Automated lead generation for digital marketing agencies.",
    version     = "1.0.0",
)

@app.on_event("startup")
async def _startup():
    if not os.environ.get("API_SECRET_KEY", "").strip():
        log.warning("API_SECRET_KEY not set — endpoints are UNPROTECTED")
    log.info(f"JobStore mode: {JOBS.mode}")


# ─────────────────────────────────────────
# Job store — Redis-backed with memory fallback
# ─────────────────────────────────────────
class JobStore:
    """
    Stores jobs in Redis (persistent across redeploys) when UPSTASH_REDIS_URL
    is set, otherwise falls back to an in-memory dict.

    Each job is stored as JSON at key  leadscan:job:{job_id}  with a 7-day TTL.
    A sorted set  leadscan:jobs  tracks job IDs ordered by creation time.
    """
    _PREFIX  = "leadscan:job:"
    _SET_KEY = "leadscan:jobs"
    _TTL     = 7 * 24 * 3600   # 7 days

    def __init__(self):
        self._r    = None
        self._mem  = {}
        self.mode  = "memory"
        url = os.environ.get("UPSTASH_REDIS_URL", "").strip()
        if url:
            try:
                import redis as _redis
                self._r   = _redis.from_url(url, decode_responses=True, socket_timeout=5)
                self._r.ping()
                self.mode = "redis"
                log.info("JobStore: connected to Redis ✅")
            except Exception as e:
                log.warning(f"JobStore: Redis unavailable ({e}) — using memory fallback")

    # ── write ──────────────────────────────────────────────────────────────
    def create(self, job_id: str, data: dict):
        data = dict(data)
        if self._r:
            self._r.set(f"{self._PREFIX}{job_id}", json.dumps(data, ensure_ascii=False), ex=self._TTL)
            self._r.zadd(self._SET_KEY, {job_id: time.time()})
            self._r.expire(self._SET_KEY, self._TTL)
        else:
            self._mem[job_id] = data

    def _save(self, job_id: str, data: dict):
        if self._r:
            self._r.set(f"{self._PREFIX}{job_id}", json.dumps(data, ensure_ascii=False), ex=self._TTL)
        else:
            self._mem[job_id] = data

    def update(self, job_id: str, **fields):
        """Merge fields into an existing job and persist."""
        job = self.get(job_id)
        if job is None:
            return
        job.update(fields)
        self._save(job_id, job)

    def append_log(self, job_id: str, msg: str):
        """Append a log line to the job's log list."""
        job = self.get(job_id)
        if job is None:
            return
        job.setdefault("log", []).append(msg)
        self._save(job_id, job)

    # ── read ───────────────────────────────────────────────────────────────
    def get(self, job_id: str) -> Optional[dict]:
        if self._r:
            raw = self._r.get(f"{self._PREFIX}{job_id}")
            return json.loads(raw) if raw else None
        return self._mem.get(job_id)

    def all(self) -> list:
        """All jobs, newest first (up to 200)."""
        if self._r:
            ids = self._r.zrevrange(self._SET_KEY, 0, 199)
            if not ids:
                return []
            vals = self._r.mget(*[f"{self._PREFIX}{i}" for i in ids])
            return [json.loads(v) for v in vals if v]
        return sorted(self._mem.values(), key=lambda j: j.get("started_at", ""), reverse=True)


JOBS = JobStore()

RESULTS_DIR = "/tmp/leadscan_results"
os.makedirs(RESULTS_DIR, exist_ok=True)


# ─────────────────────────────────────────
# Models
# ─────────────────────────────────────────
class RunRequest(BaseModel):
    industry:   str
    location:   str
    suburb:     str  = ""    # Optional: refine to a specific suburb (e.g. "Surry Hills")
    postcode:   str  = ""    # Optional: refine to a postcode (e.g. "2010")
    max_pages:  int  = 1     # 1 page = up to 20 businesses (~$0.34 in Places API)
    region:     str  = "AU"
    skip_dedup: bool = True  # Set False once GHL is configured
    skip_crm:   bool = False # Set True to skip GHL write (audit-only mode)


# ─────────────────────────────────────────
# Background pipeline runner
# ─────────────────────────────────────────
def _run_job(job_id: str, req: RunRequest):
    """Runs the full pipeline in a background thread, persisting progress to JobStore."""
    JOBS.update(job_id, status="running")

    def on_progress(msg: str):
        JOBS.append_log(job_id, msg)
        log.info(f"[{job_id[:8]}] {msg}")

    try:
        result = run_pipeline(
            industry    = req.industry,
            location    = req.location,
            max_pages   = req.max_pages,
            skip_dedup  = req.skip_dedup,
            skip_crm    = req.skip_crm,
            region      = req.region,
            suburb      = req.suburb,
            postcode    = req.postcode,
            on_progress = on_progress,
        )

        # Store results JSON in Redis (as leads_json field) AND /tmp for fallback
        results_path = f"{RESULTS_DIR}/{job_id}.json"
        leads_json   = json.dumps(result["leads"], ensure_ascii=False)
        with open(results_path, "w", encoding="utf-8") as f:
            f.write(leads_json)

        JOBS.update(job_id,
            status       = "completed",
            completed_at = datetime.now(timezone.utc).isoformat(),
            lead_count   = result["lead_count"],
            stages       = result["stages"],
            duration_s   = result["duration_s"],
            errors       = result["errors"],
            results_path = results_path,
            leads_json   = leads_json,   # stored in Redis so results survive redeploys
        )

    except Exception as e:
        log.exception(f"[{job_id[:8]}] Pipeline error")
        JOBS.update(job_id,
            status       = "failed",
            completed_at = datetime.now(timezone.utc).isoformat(),
            error        = str(e),
        )
        JOBS.append_log(job_id, f"ERROR: {e}")


# ─────────────────────────────────────────
# Routes
# ─────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "service": "LeadScan AI", "store": JOBS.mode}


@app.get("/debug")
def debug():
    """
    Returns startup diagnostics. Open in browser to verify agents loaded.
    No auth required so you can check it even if credentials aren't set yet.
    """
    import sys, importlib
    _agents_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "agents")
    results = {}
    for mod in ("scraper", "auditor", "summariser", "crm_writer"):
        try:
            spec = importlib.util.find_spec(mod)
            results[mod] = "found" if spec else "not found"
        except Exception as e:
            results[mod] = f"error: {e}"
    return {
        "python": sys.version,
        "agents_dir": _agents_dir,
        "agents_dir_exists": os.path.isdir(_agents_dir),
        "agent_modules": results,
        "env_keys_set": {
            "GOOGLE_PLACES_API_KEY": bool(os.environ.get("GOOGLE_PLACES_API_KEY")),
            "PAGESPEED_API_KEY":     bool(os.environ.get("PAGESPEED_API_KEY")),
            "ANTHROPIC_API_KEY":     bool(os.environ.get("ANTHROPIC_API_KEY")),
            "GHL_API_KEY":           bool(os.environ.get("GHL_API_KEY")),
            "API_SECRET_KEY":        bool(os.environ.get("API_SECRET_KEY")),
        },
    }


@app.post("/run", status_code=202)
def start_run(req: RunRequest, background_tasks: BackgroundTasks, _=Depends(require_api_key)):
    """
    Starts a pipeline run. Returns immediately with a job_id.
    Poll GET /jobs/{job_id} for status.
    """
    job_id = str(uuid.uuid4())
    JOBS.create(job_id, {
        "job_id":       job_id,
        "industry":     req.industry,
        "location":     req.location,
        "max_pages":    req.max_pages,
        "status":       "queued",
        "started_at":   datetime.now(timezone.utc).isoformat(),
        "completed_at": None,
        "lead_count":   None,
        "stages":       None,
        "duration_s":   None,
        "errors":       [],
        "log":          [f"Job queued: {req.industry} in {req.location}"],
        "results_path": None,
        "leads_json":   None,
    })

    # Run in a background thread so we can return immediately
    t = threading.Thread(target=_run_job, args=(job_id, req), daemon=True)
    t.start()

    log.info(f"Started job {job_id[:8]} — {req.industry} in {req.location}")
    return {
        "job_id":    job_id,
        "status":    "queued",
        "status_url": f"/jobs/{job_id}",
    }


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    """Returns job status + progress log."""
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {k: v for k, v in job.items() if k not in ("results_path", "leads_json")}


@app.get("/results/{job_id}")
def get_results(job_id: str, key: str = Query(default=None), _=Depends(require_api_key)):
    """Returns the completed leads array as JSON."""
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job["status"] != "completed":
        raise HTTPException(status_code=409, detail=f"Job status is '{job['status']}' — not completed yet")

    # Prefer the Redis-stored JSON (survives redeploys), fall back to /tmp file
    if job.get("leads_json"):
        return JSONResponse(content=json.loads(job["leads_json"]))

    results_path = job.get("results_path")
    if results_path and os.path.exists(results_path):
        with open(results_path, encoding="utf-8") as f:
            return JSONResponse(content=json.load(f))

    raise HTTPException(status_code=404, detail="Results not found — job may be from a previous deployment")


@app.get("/jobs")
def list_jobs(_=Depends(require_api_key)):
    """Lists all jobs in reverse-chronological order."""
    jobs_list = [
        {k: v for k, v in j.items() if k not in ("results_path", "log", "leads_json")}
        for j in JOBS.all()
    ]
    return jobs_list


# ─────────────────────────────────────────
# Dashboard JS  (raw string — Python never processes escape sequences)
# ─────────────────────────────────────────
DASHBOARD_JS = r"""
'use strict';
var pollers = {};

function saveKey(val) { localStorage.setItem('ls_api_key', val); }
function getKey()     { return document.getElementById('apiKey').value.trim(); }

function authHeaders() {
  var key = getKey();
  return key
    ? { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + key }
    : { 'Content-Type': 'application/json' };
}

async function startRun() {
  var btn = document.getElementById('runBtn');
  btn.disabled = true; btn.textContent = 'Starting...';
  var body = {
    industry:   document.getElementById('industry').value.trim(),
    location:   document.getElementById('location').value.trim(),
    suburb:     document.getElementById('suburb').value.trim(),
    postcode:   document.getElementById('postcode').value.trim(),
    max_pages:  parseInt(document.getElementById('max_pages').value),
    region:     document.getElementById('region').value.trim(),
    skip_dedup: true,
  };
  try {
    var r = await fetch('/run', { method: 'POST', headers: authHeaders(), body: JSON.stringify(body) });
    if (r.status === 401) {
      alert('Invalid API key. Check the key field at the top.');
      btn.disabled = false; btn.textContent = 'Run'; return;
    }
    if (!r.ok) {
      var txt = await r.text();
      alert('Server error ' + r.status + ': ' + txt.slice(0, 300) + '\n\nCheck the Render Logs tab.');
      btn.disabled = false; btn.textContent = 'Run'; return;
    }
    var data = await r.json();
    showToast('Job started: ' + data.job_id.slice(0, 8));
    pollJob(data.job_id);
    await refreshJobs();
  } catch (e) {
    alert('Network error: ' + e);
  }
  btn.disabled = false; btn.textContent = 'Run';
}

async function refreshJobs() {
  var r;
  try { r = await fetch('/jobs', { headers: authHeaders() }); } catch (e) { return; }
  var container = document.getElementById('jobs-container');
  if (r.status === 401) {
    container.innerHTML = '<div class="empty-state" style="color:#f87171">Enter your API key above to view jobs.</div>';
    return;
  }
  if (!r.ok) {
    container.innerHTML = '<div class="empty-state" style="color:#f87171">Server error ' + r.status + ' — check Render Logs.</div>';
    return;
  }
  var jobs = await r.json();
  if (!jobs.length) {
    container.innerHTML = '<div class="empty-state">No jobs yet. Start a run above.</div>';
    return;
  }
  var rows = '';
  for (var i = 0; i < jobs.length; i++) {
    var j = jobs[i];
    var jid  = j.job_id;
    var jid8 = jid.slice(0, 8);
    var ts   = j.started_at ? j.started_at.slice(0, 19).replace('T', ' ') : '';
    var leads = j.lead_count != null ? j.lead_count : '-';
    var dur   = j.duration_s != null ? j.duration_s + 's' : '-';
    var resLink = j.status === 'completed'
      ? ' &nbsp;<a class="link" href="/results/' + jid + '" target="_blank">Results</a>' : '';
    rows += '<tr id="row-' + jid + '">';
    rows += '<td style="font-family:monospace;color:#64748b">' + jid8 + '</td>';
    rows += '<td>' + j.industry + ' - ' + j.location;
    rows += '<br><span style="color:#64748b;font-size:12px">' + ts + '</span></td>';
    rows += '<td><span class="status ' + j.status + '"><span class="dot"></span>' + j.status + '</span></td>';
    rows += '<td>' + leads + '</td>';
    rows += '<td>' + dur + '</td>';
    rows += '<td><a class="link" href="#" onclick="toggleLog(\'' + jid + '\');return false">Log</a>' + resLink + '</td>';
    rows += '</tr>';
    rows += '<tr><td colspan="6" style="padding:0"><div class="log-box" id="log-' + jid + '">Loading...</div></td></tr>';
  }
  container.innerHTML = '<table class="jobs-table"><thead><tr>'
    + '<th>ID</th><th>Query</th><th>Status</th><th>Leads</th><th>Dur</th><th>Actions</th>'
    + '</tr></thead><tbody>' + rows + '</tbody></table>';

  jobs.forEach(function(j) {
    if ((j.status === 'running' || j.status === 'queued') && !pollers[j.job_id]) {
      pollJob(j.job_id);
    }
  });
}

async function toggleLog(jobId) {
  var box = document.getElementById('log-' + jobId);
  if (box.style.display === 'block') { box.style.display = 'none'; return; }
  box.style.display = 'block';
  var r = await fetch('/jobs/' + jobId, { headers: authHeaders() });
  var j = await r.json();
  box.textContent = (j.log || []).join('\n');
  box.scrollTop = box.scrollHeight;
}

function pollJob(jobId) {
  if (pollers[jobId]) return;
  pollers[jobId] = setInterval(async function() {
    var r = await fetch('/jobs/' + jobId, { headers: authHeaders() });
    var j = await r.json();
    var box = document.getElementById('log-' + jobId);
    if (box && box.style.display === 'block') {
      box.textContent = (j.log || []).join('\n');
      box.scrollTop = box.scrollHeight;
    }
    if (j.status !== 'running' && j.status !== 'queued') {
      clearInterval(pollers[jobId]); delete pollers[jobId];
      refreshJobs();
    }
  }, 2000);
}

function showToast(msg) {
  var t = document.getElementById('toast');
  t.textContent = msg; t.style.display = 'block';
  setTimeout(function() { t.style.display = 'none'; }, 3000);
}

// Init
var savedKey = localStorage.getItem('ls_api_key');
if (savedKey) document.getElementById('apiKey').value = savedKey;
refreshJobs();
setInterval(refreshJobs, 10000);
"""

# ─────────────────────────────────────────
# Dashboard HTML
# ─────────────────────────────────────────
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>LeadScan AI</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         background: #0f172a; color: #e2e8f0; min-height: 100vh; }
  .header { background: #1e293b; border-bottom: 1px solid #334155;
            padding: 20px 32px; display: flex; align-items: center; gap: 12px; }
  .header h1 { font-size: 20px; font-weight: 700; color: #f8fafc; }
  .header span { font-size: 13px; color: #64748b; }
  .badge { background: #10b981; color: #fff; font-size: 11px; font-weight: 600;
           padding: 2px 8px; border-radius: 99px; letter-spacing: 0.5px; }
  .container { max-width: 900px; margin: 40px auto; padding: 0 24px; }
  .card { background: #1e293b; border: 1px solid #334155; border-radius: 12px;
          padding: 28px; margin-bottom: 24px; }
  .card h2 { font-size: 15px; font-weight: 600; color: #94a3b8;
             text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 20px; }
  .form-row { display: flex; gap: 12px; flex-wrap: wrap; }
  .form-group { display: flex; flex-direction: column; gap: 6px; flex: 1; min-width: 160px; }
  label { font-size: 12px; color: #94a3b8; font-weight: 500; text-transform: uppercase; letter-spacing: 0.5px; }
  input, select { background: #0f172a; border: 1px solid #334155; color: #f1f5f9;
                  padding: 10px 14px; border-radius: 8px; font-size: 14px;
                  transition: border-color 0.2s; outline: none; }
  input:focus, select:focus { border-color: #6366f1; }
  .btn { background: #6366f1; color: #fff; border: none; padding: 11px 24px;
         border-radius: 8px; font-size: 14px; font-weight: 600; cursor: pointer;
         transition: background 0.2s; white-space: nowrap; align-self: flex-end; }
  .btn:hover { background: #4f46e5; }
  .btn:disabled { background: #334155; color: #64748b; cursor: not-allowed; }
  .jobs-table { width: 100%; border-collapse: collapse; }
  .jobs-table th { font-size: 11px; color: #64748b; text-transform: uppercase;
                   letter-spacing: 0.5px; padding: 8px 12px; text-align: left;
                   border-bottom: 1px solid #334155; }
  .jobs-table td { padding: 12px; border-bottom: 1px solid #1e293b;
                   font-size: 14px; vertical-align: middle; }
  .jobs-table tr:last-child td { border-bottom: none; }
  .status { display: inline-flex; align-items: center; gap: 5px;
            padding: 3px 10px; border-radius: 99px; font-size: 12px; font-weight: 600; }
  .status.completed { background: #064e3b; color: #10b981; }
  .status.running   { background: #1e3a5f; color: #60a5fa; }
  .status.queued    { background: #2d1b69; color: #a78bfa; }
  .status.failed    { background: #4c1d1d; color: #f87171; }
  .dot { width: 6px; height: 6px; border-radius: 50%; background: currentColor;
         animation: pulse 1.5s ease-in-out infinite; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }
  .running .dot, .queued .dot { display: inline-block; } .completed .dot, .failed .dot { display: none; }
  .link { color: #6366f1; text-decoration: none; font-size: 13px; }
  .link:hover { text-decoration: underline; }
  .log-box { background: #0f172a; border: 1px solid #334155; border-radius: 8px;
             padding: 16px; font-family: monospace; font-size: 12px;
             line-height: 1.7; color: #94a3b8; max-height: 300px; overflow-y: auto;
             display: none; margin-top: 12px; }
  .empty-state { text-align: center; padding: 40px; color: #475569; font-size: 14px; }
  #toast { position: fixed; bottom: 24px; right: 24px; background: #10b981;
           color: #fff; padding: 12px 20px; border-radius: 8px; font-size: 14px;
           font-weight: 600; display: none; z-index: 100; }
</style>
</head>
<body>
<div class="header">
  <h1>LeadScan AI</h1>
  <span class="badge">LIVE</span>
  <span style="margin-left:auto; font-size:13px; color:#475569">
    <a href="/docs" class="link">API Docs</a> &nbsp;·&nbsp;
    <a href="/jobs" class="link">All Jobs (JSON)</a>
  </span>
</div>

<div class="container">
  <!-- API Key -->
  <div class="card" style="margin-bottom:16px; padding:18px 28px;">
    <div class="form-row" style="align-items:flex-end">
      <div class="form-group" style="flex:2">
        <label>API Key</label>
        <input id="apiKey" type="password" placeholder="Bearer token — set as API_SECRET_KEY on Render"
               style="font-family:monospace"
               oninput="saveKey(this.value)">
      </div>
      <div style="font-size:12px; color:#475569; padding-bottom:12px; white-space:nowrap">
        Stored locally in your browser. Never sent to anyone else.
      </div>
    </div>
  </div>

  <!-- Run form -->
  <div class="card">
    <h2>New Run</h2>
    <div class="form-row">
      <div class="form-group">
        <label>Industry</label>
        <input id="industry" type="text" value="plumbers" placeholder="e.g. plumbers">
      </div>
      <div class="form-group">
        <label>Location</label>
        <input id="location" type="text" value="Sydney" placeholder="e.g. Sydney">
      </div>
      <div class="form-group">
        <label>Suburb <span style="color:#475569;font-weight:400">(optional)</span></label>
        <input id="suburb" type="text" placeholder="e.g. Surry Hills">
      </div>
      <div class="form-group" style="max-width:110px">
        <label>Postcode <span style="color:#475569;font-weight:400">(opt)</span></label>
        <input id="postcode" type="text" placeholder="e.g. 2010">
      </div>
      <div class="form-group" style="max-width:110px">
        <label>Pages</label>
        <select id="max_pages">
          <option value="1">1 (~20)</option>
          <option value="2">2 (~40)</option>
          <option value="3">3 (~60)</option>
        </select>
      </div>
      <div class="form-group" style="max-width:80px">
        <label>Region</label>
        <input id="region" type="text" value="AU">
      </div>
      <button class="btn" id="runBtn" onclick="startRun()">Run</button>
    </div>
  </div>

  <!-- Recent jobs -->
  <div class="card">
    <h2>Recent Jobs</h2>
    <div id="jobs-container">
      <div class="empty-state">No jobs yet. Start a run above.</div>
    </div>
  </div>
</div>

<div id="toast">Job started!</div>

<script src="/app.js"></script>
</body>
</html>"""


@app.get("/app.js")
def dashboard_js():
    from fastapi.responses import Response
    return Response(content=DASHBOARD_JS, media_type="application/javascript")


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return DASHBOARD_HTML
