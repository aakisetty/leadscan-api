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
var pollers     = {};
var expanded    = {};   // jobId -> 'log' | 'results'
var resultsCache = {};  // jobId -> leads array

function saveKey(val) { localStorage.setItem('ls_api_key', val); }
function getKey()     { return document.getElementById('apiKey').value.trim(); }

function authHeaders(json) {
  var h = json !== false ? { 'Content-Type': 'application/json' } : {};
  var k = getKey();
  if (k) h['Authorization'] = 'Bearer ' + k;
  return h;
}

// ── Run ──────────────────────────────────────────────────────────────────────
async function startRun() {
  var btn = document.getElementById('runBtn');
  btn.disabled = true; btn.innerHTML = '<span class="spin">&#9696;</span> Running...';
  var body = {
    industry:   document.getElementById('industry').value.trim(),
    location:   document.getElementById('location').value.trim(),
    suburb:     document.getElementById('suburb').value.trim(),
    postcode:   document.getElementById('postcode').value.trim(),
    max_pages:  parseInt(document.getElementById('max_pages').value),
    region:     document.getElementById('region').value.trim(),
    skip_dedup: false,
  };
  try {
    var r = await fetch('/run', { method: 'POST', headers: authHeaders(), body: JSON.stringify(body) });
    if (r.status === 401) { showToast('Invalid API key', true); btn.disabled=false; btn.textContent='Run'; return; }
    if (!r.ok) { var t = await r.text(); showToast('Error ' + r.status, true); btn.disabled=false; btn.textContent='Run'; return; }
    var data = await r.json();
    showToast('Job started');
    expanded[data.job_id] = 'log';
    await refreshJobs();
    pollJob(data.job_id);
  } catch (e) { showToast('Network error', true); }
  btn.disabled = false; btn.textContent = 'Run';
}

// ── Jobs list ────────────────────────────────────────────────────────────────
async function refreshJobs() {
  var r;
  try { r = await fetch('/jobs', { headers: authHeaders(false) }); } catch (e) { return; }
  var container = document.getElementById('jobs-container');
  if (r.status === 401) {
    container.innerHTML = '<div class="empty">Enter your API key above to view jobs.</div>';
    return;
  }
  if (!r.ok) { return; }
  var jobs = await r.json();
  if (!jobs.length) {
    container.innerHTML = '<div class="empty">No jobs yet — start a run above.</div>';
    return;
  }
  var html = '';
  for (var i = 0; i < jobs.length; i++) {
    var j   = jobs[i];
    var jid = j.job_id;
    var q   = j.industry + ' · ' + j.location + (j.suburb ? ' · ' + j.suburb : '') + (j.postcode ? ' ' + j.postcode : '');
    var ts  = j.started_at ? j.started_at.slice(0,16).replace('T',' ') : '';
    var dur = j.duration_s ? j.duration_s + 's' : '';
    var lc  = j.lead_count != null ? j.lead_count + ' leads' : '';
    var open = expanded[jid] ? ' open' : '';
    html += '<div class="job-card' + open + '" id="jcard-' + jid + '">';
    html += '<div class="job-header" onclick="toggleJob(\'' + jid + '\')">';
    html += '  <span class="st-badge ' + j.status + '"><span class="dot"></span>' + j.status + '</span>';
    html += '  <span class="job-query">' + q + '</span>';
    html += '  <span class="job-meta">' + (lc || '') + (dur ? ' &nbsp;' + dur : '') + (ts ? ' &nbsp;' + ts : '') + '</span>';
    html += '  <span class="job-chevron">&#x25BE;</span>';
    html += '</div>';
    html += '<div class="job-body" id="jbody-' + jid + '" style="display:' + (expanded[jid] ? 'block' : 'none') + '">';
    html += '  <div class="tab-bar">';
    html += '    <button class="tab-btn' + (expanded[jid]==='log' ? ' active' : '') + '" onclick="showTab(\'' + jid + '\',\'log\')">Log</button>';
    if (j.status === 'completed') {
      html += '    <button class="tab-btn' + (expanded[jid]==='results' ? ' active' : '') + '" onclick="showTab(\'' + jid + '\',\'results\')">Results (' + (j.lead_count || 0) + ')</button>';
    }
    html += '  </div>';
    html += '  <div id="tab-log-' + jid + '" class="tab-pane" style="display:' + (expanded[jid]==='log' ? 'block' : 'none') + '">';
    html += '    <div class="log-box" id="log-' + jid + '">Loading...</div>';
    html += '  </div>';
    html += '  <div id="tab-results-' + jid + '" class="tab-pane" style="display:' + (expanded[jid]==='results' ? 'block' : 'none') + '">';
    html += '    <div id="results-' + jid + '"><div class="loading-results">Loading results...</div></div>';
    html += '  </div>';
    html += '</div>';
    html += '</div>';
  }
  container.innerHTML = html;

  // Populate open panels
  for (var i = 0; i < jobs.length; i++) {
    var j = jobs[i];
    if (expanded[j.job_id]) {
      loadLog(j.job_id);
      if (expanded[j.job_id] === 'results' && j.status === 'completed') loadResults(j.job_id);
      if (j.status === 'running' || j.status === 'queued') pollJob(j.job_id);
    }
  }
}

// ── Toggle job expand ────────────────────────────────────────────────────────
function toggleJob(jid) {
  if (expanded[jid]) {
    delete expanded[jid];
    var body = document.getElementById('jbody-' + jid);
    if (body) body.style.display = 'none';
    var card = document.getElementById('jcard-' + jid);
    if (card) card.classList.remove('open');
  } else {
    expanded[jid] = 'log';
    var body = document.getElementById('jbody-' + jid);
    if (body) body.style.display = 'block';
    var card = document.getElementById('jcard-' + jid);
    if (card) card.classList.add('open');
    loadLog(jid);
  }
}

function showTab(jid, tab) {
  expanded[jid] = tab;
  var logPane  = document.getElementById('tab-log-' + jid);
  var resPane  = document.getElementById('tab-results-' + jid);
  var btns     = document.querySelectorAll('#jcard-' + jid + ' .tab-btn');
  if (logPane) logPane.style.display = tab === 'log' ? 'block' : 'none';
  if (resPane) resPane.style.display = tab === 'results' ? 'block' : 'none';
  btns.forEach(function(b, i) { b.classList.toggle('active', (i===0&&tab==='log')||(i===1&&tab==='results')); });
  if (tab === 'results') loadResults(jid);
  if (tab === 'log')     loadLog(jid);
}

// ── Log ──────────────────────────────────────────────────────────────────────
async function loadLog(jid) {
  var box = document.getElementById('log-' + jid);
  if (!box) return;
  var r = await fetch('/jobs/' + jid, { headers: authHeaders(false) });
  if (!r.ok) return;
  var j = await r.json();
  box.textContent = (j.log || []).join('\n');
  box.scrollTop = box.scrollHeight;
}

// ── Results ───────────────────────────────────────────────────────────────────
async function loadResults(jid) {
  var pane = document.getElementById('results-' + jid);
  if (!pane) return;
  if (resultsCache[jid]) { pane.innerHTML = renderLeads(resultsCache[jid]); return; }
  pane.innerHTML = '<div class="loading-results">Fetching results...</div>';
  try {
    var r = await fetch('/results/' + jid, { headers: authHeaders(false) });
    if (!r.ok) { pane.innerHTML = '<div class="loading-results" style="color:#f87171">Failed to load results (status ' + r.status + ')</div>'; return; }
    var leads = await r.json();
    resultsCache[jid] = leads;
    pane.innerHTML = renderLeads(leads);
  } catch(e) { pane.innerHTML = '<div class="loading-results" style="color:#f87171">Error: ' + e + '</div>'; }
}

function stageLabel(s) {
  var m = { no_website:'No Website', website_no_ads:'No Ads', needs_gbp_ads:'Needs GBP', nurture:'Nurture', unknown:'Unknown' };
  return m[s] || s;
}
function stageClass(s) {
  var m = { no_website:'stage-1', website_no_ads:'stage-2', needs_gbp_ads:'stage-3', nurture:'stage-4' };
  return m[s] || 'stage-4';
}
function psColor(s) {
  if (!s || s < 0) return '#64748b';
  if (s < 50) return '#f87171';
  if (s < 75) return '#fb923c';
  return '#34d399';
}

function renderLeads(leads) {
  if (!leads || !leads.length) return '<div class="loading-results">No leads found.</div>';
  var html = '<div class="leads-summary">' + leads.length + ' lead' + (leads.length!==1?'s':'') + ' found</div>';
  html += '<div class="leads-grid">';
  for (var i = 0; i < leads.length; i++) {
    var l   = leads[i];
    var seo = l.seo_results || {};
    var ps  = seo.performance_score;
    var stg = l.ghl_pipeline_stage || (l.has_website === false ? 'no_website' : 'nurture');
    var tel = l.phone ? '<a href="tel:' + l.phone + '" class="phone-link">' + l.phone + '</a>' : '<span style="color:#475569">No phone</span>';
    html += '<div class="lead-card">';
    html += '  <div class="lead-top">';
    html += '    <div class="lead-name">' + (l.name || 'Unknown') + '</div>';
    html += '    <span class="stage-pill ' + stageClass(stg) + '">' + stageLabel(stg) + '</span>';
    html += '  </div>';
    html += '  <div class="lead-addr">' + (l.address || '') + '</div>';
    html += '  <div class="lead-row">' + tel;
    if (l.website) html += ' &nbsp;<a href="' + l.website + '" target="_blank" rel="noopener" class="link">Site</a>';
    if (l.maps_url) html += ' &nbsp;<a href="' + l.maps_url + '" target="_blank" rel="noopener" class="link">Maps</a>';
    html += '</div>';
    if (ps != null) {
      html += '<div class="lead-ps"><span style="color:' + psColor(ps) + ';font-weight:700">' + ps + '/100</span> <span style="color:#64748b;font-size:11px">PageSpeed mobile</span></div>';
    }
    if (l.gap_summary && l.gap_summary !== 'No significant digital gaps detected.') {
      html += '<div class="lead-gap">' + l.gap_summary + '</div>';
    } else if (l.gap_summary === 'No significant digital gaps detected.') {
      html += '<div class="lead-gap" style="color:#34d399">No significant gaps detected</div>';
    }
    html += '</div>';
  }
  html += '</div>';
  return html;
}

// ── Polling ───────────────────────────────────────────────────────────────────
function pollJob(jid) {
  if (pollers[jid]) return;
  pollers[jid] = setInterval(async function() {
    var r = await fetch('/jobs/' + jid, { headers: authHeaders(false) });
    if (!r.ok) return;
    var j = await r.json();
    var box = document.getElementById('log-' + jid);
    if (box) { box.textContent = (j.log || []).join('\n'); box.scrollTop = box.scrollHeight; }
    if (j.status !== 'running' && j.status !== 'queued') {
      clearInterval(pollers[jid]); delete pollers[jid];
      refreshJobs();
    }
  }, 2500);
}

// ── Toast ─────────────────────────────────────────────────────────────────────
function showToast(msg, err) {
  var t = document.getElementById('toast');
  t.textContent = msg;
  t.style.background = err ? '#ef4444' : '#10b981';
  t.style.display = 'block';
  setTimeout(function() { t.style.display = 'none'; }, 3000);
}

// ── Init ──────────────────────────────────────────────────────────────────────
var savedKey = localStorage.getItem('ls_api_key');
if (savedKey) document.getElementById('apiKey').value = savedKey;
refreshJobs();
setInterval(refreshJobs, 12000);
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
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Inter',sans-serif;background:#080f1a;color:#e2e8f0;min-height:100vh;font-size:14px}

/* ── Header ── */
.header{background:#0f172a;border-bottom:1px solid #1e2d40;padding:0 32px;height:56px;display:flex;align-items:center;gap:14px;position:sticky;top:0;z-index:50}
.logo{font-size:17px;font-weight:700;color:#f8fafc;letter-spacing:-0.3px}
.logo span{color:#6366f1}
.live-dot{width:8px;height:8px;border-radius:50%;background:#10b981;box-shadow:0 0 6px #10b981;animation:pulse 2s ease-in-out infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}
.header-right{margin-left:auto;display:flex;align-items:center;gap:8px}
.key-wrap{display:flex;align-items:center;gap:8px;background:#1e293b;border:1px solid #334155;border-radius:8px;padding:4px 10px}
.key-wrap label{font-size:11px;font-weight:500;color:#64748b;white-space:nowrap}
.key-input{background:none;border:none;outline:none;color:#94a3b8;font-size:12px;font-family:monospace;width:220px;padding:4px 0}
.key-input::placeholder{color:#334155}

/* ── Layout ── */
.wrap{max-width:1100px;margin:0 auto;padding:28px 24px}

/* ── Run card ── */
.run-card{background:#0f172a;border:1px solid #1e2d40;border-radius:14px;padding:22px 24px;margin-bottom:24px}
.run-card h2{font-size:11px;font-weight:600;color:#475569;text-transform:uppercase;letter-spacing:.8px;margin-bottom:16px}
.form-grid{display:grid;grid-template-columns:1fr 1fr 1fr 1fr 100px 70px auto;gap:10px;align-items:end}
@media(max-width:900px){.form-grid{grid-template-columns:1fr 1fr 1fr;}}
.fg{display:flex;flex-direction:column;gap:5px}
.fg label{font-size:11px;font-weight:500;color:#64748b;letter-spacing:.3px}
.fg label small{font-weight:400;color:#334155}
input,select{background:#161f2e;border:1px solid #1e2d40;color:#e2e8f0;padding:9px 12px;border-radius:8px;font-size:13px;font-family:'Inter',sans-serif;outline:none;transition:border-color .15s}
input:focus,select:focus{border-color:#6366f1}
input::placeholder{color:#334155}
.run-btn{background:linear-gradient(135deg,#6366f1,#4f46e5);color:#fff;border:none;padding:10px 22px;border-radius:8px;font-size:13px;font-weight:600;cursor:pointer;white-space:nowrap;transition:opacity .15s;display:flex;align-items:center;gap:6px}
.run-btn:hover{opacity:.9}
.run-btn:disabled{opacity:.4;cursor:not-allowed}
.spin{display:inline-block;animation:rotate 1s linear infinite}
@keyframes rotate{to{transform:rotate(360deg)}}

/* ── Jobs section ── */
.section-title{font-size:11px;font-weight:600;color:#475569;text-transform:uppercase;letter-spacing:.8px;margin-bottom:12px}
.empty{text-align:center;padding:48px;color:#334155;font-size:13px}

/* ── Job cards ── */
.job-card{background:#0f172a;border:1px solid #1e2d40;border-radius:12px;margin-bottom:10px;overflow:hidden;transition:border-color .15s}
.job-card.open{border-color:#334155}
.job-header{display:flex;align-items:center;gap:12px;padding:14px 18px;cursor:pointer;user-select:none}
.job-header:hover{background:#111827}
.job-query{font-size:13px;font-weight:500;color:#cbd5e1;flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.job-meta{font-size:12px;color:#475569;white-space:nowrap}
.job-chevron{font-size:12px;color:#475569;transition:transform .2s}
.job-card.open .job-chevron{transform:rotate(180deg)}

/* ── Status badges ── */
.st-badge{display:inline-flex;align-items:center;gap:5px;padding:3px 9px;border-radius:99px;font-size:11px;font-weight:600;white-space:nowrap}
.st-badge .dot{width:5px;height:5px;border-radius:50%;background:currentColor;flex-shrink:0}
.st-badge.completed{background:#052e16;color:#4ade80}
.st-badge.running{background:#0c1a2e;color:#60a5fa}
.st-badge.running .dot,.st-badge.queued .dot{animation:pulse 1.2s ease-in-out infinite}
.st-badge.queued{background:#1a0e2e;color:#a78bfa}
.st-badge.failed{background:#1c0a0a;color:#f87171}
.st-badge.completed .dot,.st-badge.failed .dot{display:none}

/* ── Tab bar ── */
.job-body{border-top:1px solid #1e2d40}
.tab-bar{display:flex;gap:0;padding:0 18px;background:#080f1a;border-bottom:1px solid #1e2d40}
.tab-btn{background:none;border:none;border-bottom:2px solid transparent;color:#64748b;font-size:12px;font-weight:500;padding:10px 14px;cursor:pointer;font-family:'Inter',sans-serif;margin-bottom:-1px}
.tab-btn:hover{color:#94a3b8}
.tab-btn.active{color:#6366f1;border-bottom-color:#6366f1}

/* ── Log ── */
.log-box{padding:16px 20px;font-family:monospace;font-size:11.5px;line-height:1.75;color:#64748b;max-height:280px;overflow-y:auto;white-space:pre-wrap;word-break:break-all}
.log-box:empty::before{content:'Waiting for output...';color:#334155}

/* ── Results ── */
.leads-summary{padding:12px 20px;font-size:12px;font-weight:600;color:#64748b;border-bottom:1px solid #1e2d40;text-transform:uppercase;letter-spacing:.5px}
.leads-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:1px;background:#1e2d40}
.lead-card{background:#0a1120;padding:16px 18px;display:flex;flex-direction:column;gap:8px}
.lead-top{display:flex;align-items:flex-start;justify-content:space-between;gap:8px}
.lead-name{font-size:13px;font-weight:600;color:#f1f5f9;line-height:1.3}
.lead-addr{font-size:12px;color:#475569;line-height:1.4}
.lead-row{display:flex;align-items:center;gap:10px;font-size:12px}
.phone-link{color:#60a5fa;text-decoration:none;font-weight:500}
.phone-link:hover{text-decoration:underline}
.link{color:#6366f1;text-decoration:none;font-size:12px}
.link:hover{text-decoration:underline}
.lead-ps{font-size:12px}
.lead-gap{font-size:12px;color:#94a3b8;line-height:1.5;padding:8px 10px;background:#0f172a;border-radius:6px;border-left:2px solid #334155}
.loading-results{padding:32px;text-align:center;color:#334155;font-size:13px}

/* ── Stage pills ── */
.stage-pill{display:inline-block;padding:2px 8px;border-radius:99px;font-size:10px;font-weight:700;letter-spacing:.3px;white-space:nowrap;flex-shrink:0}
.stage-1{background:#3f0f0f;color:#f87171}
.stage-2{background:#3a1f06;color:#fb923c}
.stage-3{background:#2d2500;color:#fbbf24}
.stage-4{background:#0f1f2e;color:#60a5fa}

/* ── Toast ── */
#toast{position:fixed;bottom:24px;right:24px;padding:11px 18px;border-radius:8px;font-size:13px;font-weight:600;color:#fff;display:none;z-index:200;animation:fadein .2s}
@keyframes fadein{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:none}}
</style>
</head>
<body>

<header class="header">
  <div class="logo">Lead<span>Scan</span> AI</div>
  <div class="live-dot"></div>
  <div class="header-right">
    <div class="key-wrap">
      <label>API KEY</label>
      <input class="key-input" id="apiKey" type="password" placeholder="paste API_SECRET_KEY here" oninput="saveKey(this.value)">
    </div>
  </div>
</header>

<div class="wrap">

  <div class="run-card">
    <h2>New Run</h2>
    <div class="form-grid">
      <div class="fg">
        <label>Industry</label>
        <input id="industry" type="text" value="plumbers" placeholder="plumbers">
      </div>
      <div class="fg">
        <label>City / Region</label>
        <input id="location" type="text" value="Sydney" placeholder="Sydney">
      </div>
      <div class="fg">
        <label>Suburb <small>(optional)</small></label>
        <input id="suburb" type="text" placeholder="Surry Hills">
      </div>
      <div class="fg">
        <label>Postcode <small>(optional)</small></label>
        <input id="postcode" type="text" placeholder="2010">
      </div>
      <div class="fg">
        <label>Pages</label>
        <select id="max_pages">
          <option value="1">1 (~20)</option>
          <option value="2">2 (~40)</option>
          <option value="3">3 (~60)</option>
        </select>
      </div>
      <div class="fg">
        <label>Region</label>
        <input id="region" type="text" value="AU">
      </div>
      <button class="run-btn" id="runBtn" onclick="startRun()">&#9654; Run</button>
    </div>
  </div>

  <div class="section-title">Recent Jobs</div>
  <div id="jobs-container">
    <div class="empty">No jobs yet. Start a run above.</div>
  </div>

</div>

<div id="toast"></div>
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
