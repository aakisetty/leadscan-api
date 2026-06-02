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
import logging
import threading
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

from pipeline import run_pipeline

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

# In-memory job store (persists within a single server session).
# On Render free tier, this resets when the service spins down.
JOBS: dict[str, dict] = {}

RESULTS_DIR = "/tmp/leadscan_results"
os.makedirs(RESULTS_DIR, exist_ok=True)


# ─────────────────────────────────────────
# Models
# ─────────────────────────────────────────
class RunRequest(BaseModel):
    industry:   str
    location:   str
    max_pages:  int  = 1     # 1 page = up to 20 businesses (~$0.34 in Places API)
    region:     str  = "AU"
    skip_dedup: bool = True  # Set False once GHL is configured
    skip_crm:   bool = False # Set True to skip GHL write (audit-only mode)


# ─────────────────────────────────────────
# Background pipeline runner
# ─────────────────────────────────────────
def _run_job(job_id: str, req: RunRequest):
    """
    Runs the full pipeline in a background thread.
    Updates JOBS[job_id] as it progresses.
    """
    job = JOBS[job_id]
    job["status"] = "running"

    def on_progress(msg: str):
        job["log"].append(msg)
        log.info(f"[{job_id[:8]}] {msg}")

    try:
        result = run_pipeline(
            industry    = req.industry,
            location    = req.location,
            max_pages   = req.max_pages,
            skip_dedup  = req.skip_dedup,
            skip_crm    = req.skip_crm,
            region      = req.region,
            on_progress = on_progress,
        )

        # Save results to /tmp for retrieval
        results_path = f"{RESULTS_DIR}/{job_id}.json"
        with open(results_path, "w", encoding="utf-8") as f:
            json.dump(result["leads"], f, indent=2, ensure_ascii=False)

        job.update({
            "status":       "completed",
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "lead_count":   result["lead_count"],
            "stages":       result["stages"],
            "duration_s":   result["duration_s"],
            "errors":       result["errors"],
            "results_path": results_path,
        })

    except Exception as e:
        log.exception(f"[{job_id[:8]}] Pipeline error")
        job.update({
            "status":     "failed",
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "error":      str(e),
        })
        job["log"].append(f"ERROR: {e}")


# ─────────────────────────────────────────
# Routes
# ─────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "service": "LeadScan AI", "jobs_in_memory": len(JOBS)}


@app.post("/run", status_code=202)
def start_run(req: RunRequest, background_tasks: BackgroundTasks):
    """
    Starts a pipeline run. Returns immediately with a job_id.
    Poll GET /jobs/{job_id} for status.
    """
    job_id = str(uuid.uuid4())
    JOBS[job_id] = {
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
    }

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
    # Don't expose the internal file path
    return {k: v for k, v in job.items() if k != "results_path"}


@app.get("/results/{job_id}")
def get_results(job_id: str):
    """Returns the completed leads array as JSON."""
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job["status"] != "completed":
        raise HTTPException(status_code=409, detail=f"Job status is '{job['status']}' — not completed yet")
    results_path = job.get("results_path")
    if not results_path or not os.path.exists(results_path):
        raise HTTPException(status_code=404, detail="Results file not found")
    with open(results_path, encoding="utf-8") as f:
        return JSONResponse(content=json.load(f))


@app.get("/jobs")
def list_jobs():
    """Lists all jobs in reverse-chronological order."""
    jobs_list = [
        {k: v for k, v in j.items() if k not in ("results_path", "log")}
        for j in JOBS.values()
    ]
    return sorted(jobs_list, key=lambda j: j.get("started_at", ""), reverse=True)


# ─────────────────────────────────────────
# Dashboard
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
      <div class="form-group" style="max-width:120px">
        <label>Pages</label>
        <select id="max_pages">
          <option value="1">1 (~20)</option>
          <option value="2">2 (~40)</option>
          <option value="3">3 (~60)</option>
        </select>
      </div>
      <div class="form-group" style="max-width:110px">
        <label>Region</label>
        <input id="region" type="text" value="AU">
      </div>
      <button class="btn" id="runBtn" onclick="startRun()">▶ Run</button>
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

<div id="toast">✅ Job started!</div>

<script>
  let pollers = {};

  async function startRun() {
    const btn = document.getElementById('runBtn');
    btn.disabled = true; btn.textContent = 'Starting…';
    const body = {
      industry:   document.getElementById('industry').value.trim(),
      location:   document.getElementById('location').value.trim(),
      max_pages:  parseInt(document.getElementById('max_pages').value),
      region:     document.getElementById('region').value.trim(),
      skip_dedup: true,
    };
    try {
      const r = await fetch('/run', { method:'POST',
        headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
      const data = await r.json();
      showToast('Job started — ' + data.job_id.slice(0,8));
      pollJob(data.job_id);
      await refreshJobs();
    } catch(e) { alert('Error: ' + e); }
    btn.disabled = false; btn.textContent = '▶ Run';
  }

  async function refreshJobs() {
    const r = await fetch('/jobs');
    const jobs = await r.json();
    const container = document.getElementById('jobs-container');
    if (!jobs.length) {
      container.innerHTML = '<div class="empty-state">No jobs yet. Start a run above.</div>';
      return;
    }
    container.innerHTML = `
      <table class="jobs-table">
        <thead><tr>
          <th>ID</th><th>Query</th><th>Status</th><th>Leads</th>
          <th>Duration</th><th>Actions</th>
        </tr></thead>
        <tbody>
        ${jobs.map(j => `
          <tr id="row-${j.job_id}">
            <td style="font-family:monospace;color:#64748b">${j.job_id.slice(0,8)}</td>
            <td>${j.industry} · ${j.location}<br>
                <span style="color:#64748b;font-size:12px">${j.started_at ? j.started_at.slice(0,19).replace('T',' ') : ''}</span>
            </td>
            <td><span class="status ${j.status}"><span class="dot"></span>${j.status}</span></td>
            <td>${j.lead_count != null ? j.lead_count : '—'}</td>
            <td>${j.duration_s != null ? j.duration_s + 's' : '—'}</td>
            <td>
              <a class="link" href="#" onclick="toggleLog('${j.job_id}');return false">Log</a>
              ${j.status === 'completed' ? ` &nbsp;<a class="link" href="/results/${j.job_id}" target="_blank">Results</a>` : ''}
            </td>
          </tr>
          <tr><td colspan="6" style="padding:0">
            <div class="log-box" id="log-${j.job_id}">Loading log…</div>
          </td></tr>
        `).join('')}
        </tbody>
      </table>`;

    // Resume polling for running/queued jobs
    jobs.forEach(j => {
      if ((j.status === 'running' || j.status === 'queued') && !pollers[j.job_id]) {
        pollJob(j.job_id);
      }
    });
  }

  async function toggleLog(jobId) {
    const box = document.getElementById('log-' + jobId);
    if (box.style.display === 'block') { box.style.display = 'none'; return; }
    box.style.display = 'block';
    const r = await fetch('/jobs/' + jobId);
    const j = await r.json();
    box.textContent = j.log.join('\\n');
    box.scrollTop = box.scrollHeight;
  }

  function pollJob(jobId) {
    if (pollers[jobId]) return;
    pollers[jobId] = setInterval(async () => {
      const r = await fetch('/jobs/' + jobId);
      const j = await r.json();
      // Update log if open
      const box = document.getElementById('log-' + jobId);
      if (box && box.style.display === 'block') {
        box.textContent = j.log.join('\\n');
        box.scrollTop = box.scrollHeight;
      }
      if (j.status !== 'running' && j.status !== 'queued') {
        clearInterval(pollers[jobId]); delete pollers[jobId];
        refreshJobs();
      }
    }, 2000);
  }

  function showToast(msg) {
    const t = document.getElementById('toast');
    t.textContent = '✅ ' + msg; t.style.display = 'block';
    setTimeout(() => t.style.display = 'none', 3000);
  }

  // Load jobs on page load
  refreshJobs();
  setInterval(refreshJobs, 10000);
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return DASHBOARD_HTML
