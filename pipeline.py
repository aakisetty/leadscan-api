"""
LeadScan AI — Pipeline Orchestrator
Chains Agent 1 → 2 → 3 → 4 and streams progress updates.
"""

import os
import sys
import json
import time
import logging
from datetime import datetime, timezone
from typing import Callable, Optional

# Add agents directory to path so imports work both locally and on Render
_here = os.path.dirname(os.path.abspath(__file__))
_agents_dir = os.path.join(_here, "agents")
if _agents_dir not in sys.path:
    sys.path.insert(0, _agents_dir)

log = logging.getLogger("leadscan.pipeline")

# Lazy imports — each agent is only imported when run_pipeline is called.
# This prevents a single bad import from crashing the whole web server.
def _import_agents():
    try:
        from scraper    import run_scraper    as _scraper
        from auditor    import run_auditor    as _auditor
        from summariser import run_summariser as _summariser
        from crm_writer import run_writer     as _writer
        return _scraper, _auditor, _summariser, _writer
    except ImportError as e:
        log.error(f"Agent import failed: {e}")
        raise RuntimeError(f"Failed to import agent scripts: {e}") from e


def run_pipeline(
    industry:    str,
    location:    str,
    max_pages:   int  = 1,
    skip_dedup:  bool = True,
    skip_crm:    bool = False,
    region:      str  = "AU",
    suburb:      str  = "",
    postcode:    str  = "",
    on_progress: Optional[Callable[[str], None]] = None,
) -> dict:
    """
    Full LeadScan pipeline: Scraper → Auditor → Summariser → CRM Writer.

    on_progress(msg) is called after each major step so callers can
    stream log lines to the client.

    Returns a result dict with keys:
        leads       — final enriched list
        lead_count  — number of leads processed
        stages      — breakdown of pipeline stage assignments
        errors      — list of per-lead errors (if any)
        duration_s  — total wall-clock seconds
    """
    def emit(msg: str):
        log.info(msg)
        if on_progress:
            on_progress(msg)

    started = time.time()
    run_scraper, run_auditor, run_summariser, run_writer = _import_agents()

    # ── Agent 1: Scraper ─────────────────────────────────────────────────────
    emit(f"[1/4] Scraper — querying '{industry} in {location}' (max_pages={max_pages})...")
    try:
        raw_leads = run_scraper(
            industry    = industry,
            location    = location,
            skip_dedup  = skip_dedup,
            region_code = region,
            max_pages   = max_pages,
            suburb      = suburb,
            postcode    = postcode,
        )
        emit(f"[1/4] Scraper — {len(raw_leads)} new leads found.")
    except Exception as e:
        emit(f"[1/4] Scraper — FAILED: {e}")
        raise RuntimeError(f"Scraper failed: {e}") from e

    if not raw_leads:
        emit("[1/4] No new leads — pipeline complete (0 records).")
        return _result([], started)

    # ── Agent 2: Auditor ─────────────────────────────────────────────────────
    emit(f"[2/4] Auditor — running 5 checks on {len(raw_leads)} businesses...")
    try:
        audited = run_auditor(raw_leads, country=region)
        emit(f"[2/4] Auditor — complete ({len(audited)} leads enriched).")
    except Exception as e:
        emit(f"[2/4] Auditor — FAILED: {e}")
        raise RuntimeError(f"Auditor failed: {e}") from e

    # ── Agent 3: Gap Summariser ───────────────────────────────────────────────
    emit(f"[3/4] Summariser — generating gap summaries...")
    try:
        summarised = run_summariser(audited)
        emit(f"[3/4] Summariser — complete ({len(summarised)} summaries generated).")
    except Exception as e:
        emit(f"[3/4] Summariser — FAILED: {e}")
        raise RuntimeError(f"Summariser failed: {e}") from e

    # ── Agent 4: CRM Writer ───────────────────────────────────────────────────
    if skip_crm:
        emit("[4/4] CRM Writer — skipped (SKIP_CRM=true or GHL not configured).")
        final = summarised
    else:
        ghl_configured = bool(
            os.environ.get("GHL_API_KEY") and
            os.environ.get("GHL_LOCATION_ID") and
            os.environ.get("GHL_PIPELINE_ID")
        )
        if not ghl_configured:
            emit("[4/4] CRM Writer — skipped (GHL_API_KEY / GHL_LOCATION_ID / GHL_PIPELINE_ID not set).")
            final = summarised
        else:
            emit(f"[4/4] CRM Writer — pushing {len(summarised)} leads to GHL...")
            try:
                final = run_writer(summarised)
                created  = sum(1 for l in final if l.get("ghl_contact_action") == "created")
                updated  = sum(1 for l in final if l.get("ghl_contact_action") == "updated")
                emit(f"[4/4] CRM Writer — complete ({created} created, {updated} updated in GHL).")
            except Exception as e:
                emit(f"[4/4] CRM Writer — FAILED: {e}")
                raise RuntimeError(f"CRM Writer failed: {e}") from e

    result = _result(final, started)
    emit(
        f"Pipeline complete — {result['lead_count']} leads in "
        f"{result['duration_s']:.1f}s | stages: {result['stages']}"
    )
    return result


def _result(leads: list, started: float) -> dict:
    """Builds the standard result dict."""
    from summariser import build_gap_snapshot

    stages: dict = {"no_website": 0, "needs_gbp_ads": 0, "website_no_ads": 0, "nurture": 0, "unknown": 0}
    for l in leads:
        s = l.get("ghl_pipeline_stage") or _infer_stage(l)
        stages[s] = stages.get(s, 0) + 1

    errors = [
        {"name": l.get("name", "?"), "error": l.get("ghl_error") or l.get("audit_error") or l.get("summariser_error")}
        for l in leads
        if any(l.get(k) for k in ("ghl_error", "audit_error", "summariser_error"))
    ]

    return {
        "leads":      leads,
        "lead_count": len(leads),
        "stages":     stages,
        "errors":     errors,
        "duration_s": round(time.time() - started, 1),
    }


def _infer_stage(lead: dict) -> str:
    """Infers pipeline stage from lead data (for dry-run results)."""
    snap = lead.get("gap_snapshot") or {}
    if not lead.get("has_website"):
        return "no_website"
    if lead.get("gbp_claimed") is False:
        return "needs_gbp_ads"
    if snap.get("meta_ads_status") == "ok_no_ads":
        return "website_no_ads"
    return "nurture"
