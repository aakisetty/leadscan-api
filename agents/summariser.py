#!/usr/bin/env python3
"""
LeadScan AI — Agent 3: Gap Summariser
=======================================
Takes a JSON array of audited lead objects (from Agent 2: Auditor) and
produces two new fields for each lead:

  gap_snapshot   Structured pass/fail dict — used by Agent 5 (Scheduler)
                 to diff results across re-audit runs and detect changes.

  gap_summary    1–2 sentence plain-English summary of what the business
                 is missing digitally, written for a sales caller to read
                 before they dial. Specific, no filler, no hallucination.

Usage:
    cat audited.json | python3 summariser.py
    python3 summariser.py --input audited.json --output summarised.json
    python3 summariser.py --single '{"name":"...", "has_website":false, ...}'
    python3 summariser.py --input audited.json --dry-run   # snapshot only, no API call

Required environment variables:
    ANTHROPIC_API_KEY    Anthropic API key for Claude

Optional environment variables:
    CLAUDE_MODEL         Model to use (default: claude-haiku-4-5 — cheap, fast, accurate)
    SUMMARISE_DELAY_S    Seconds between API calls (default: 0.5)
"""

import os
import sys
import json
import time
import argparse
from datetime import datetime, timezone

import anthropic
from typing import Optional


# ─────────────────────────────────────────
# Config
# ─────────────────────────────────────────
DEFAULT_MODEL = "claude-haiku-4-5"   # Fast and cheap for this structured task
MAX_TOKENS    = 120                   # 1-2 sentences needs < 100 tokens
SYSTEM_PROMPT = """You write lead briefs for a digital marketing sales agency. Your ONLY job is to report what the audit found — nothing more.

HARD RULES — violating any of these produces wrong output:

1. You may ONLY mention gaps that are explicitly labelled MISSING, NOT INSTALLED, or UNCLAIMED in the Audit Results section. Nothing else.
2. NEVER invent, infer, or suggest any gap not directly listed in the audit. Do not mention structured data, email capture, schema markup, ads accounts, or anything else not in the audit fields.
3. NEVER mention anything shown as UNKNOWN — treat it as if it doesn't exist.
4. NEVER mention Google Ads under any circumstances.
5. NEVER mention Meta Ads unless it shows "NOT RUNNING". If it shows UNKNOWN, ignore it completely.
6. If Google Tag Manager is INSTALLED, Google Analytics is covered — do not mention it.
7. If PageSpeed score is below 50 (confirmed number, not UNKNOWN), mention the slow site with the actual score. If UNKNOWN, say nothing about speed.
8. If only one gap exists, write one sentence. If no gaps exist, write exactly: "No significant digital gaps detected." Do NOT pad with a second sentence.
9. Write naturally for a caller to read aloud. No bullet points, no "Additionally".
10. Always use "they" and "their" — never "you" or "your". The caller is reading about the business, not to it."""


# ─────────────────────────────────────────
# Gap snapshot builder
# ─────────────────────────────────────────
def build_gap_snapshot(lead: dict) -> dict:
    """
    Extracts a flat, diffable pass/fail dict from the enriched lead object.
    All values are booleans, ints, or short strings — nothing nested.
    This is stored in GHL as a JSON string and used by the Scheduler
    to detect changes between re-audit runs.
    """
    seo = lead.get("seo_results") or {}
    ads = lead.get("ads_results") or {}

    return {
        # Website
        "has_website":              lead.get("has_website"),
        # SEO
        "title_present":            seo.get("title_present"),
        "meta_description_present": seo.get("meta_description_present"),
        "viewport_present":         seo.get("viewport_present"),
        "analytics_installed":      seo.get("analytics_installed"),
        "gtm_installed":            seo.get("gtm_installed"),
        "performance_score":        seo.get("performance_score"),
        # Google Business Profile
        "gbp_claimed":              lead.get("gbp_claimed"),
        "gbp_claimed_status":       lead.get("gbp_claimed_status"),
        # Ads
        "meta_ads_active":          ads.get("meta_ads_active"),
        "meta_ads_status":          ads.get("meta_ads_status"),
        "google_ads_status":        ads.get("google_ads_status", "manual_check_required"),
        # Meta
        "snapshotted_at":           datetime.now(timezone.utc).isoformat(),
    }


def snapshot_diff(old: dict, new: dict) -> dict:
    """
    Returns only the keys that changed between two gap_snapshot dicts.
    Used by Agent 5 (Scheduler) to determine if a GHL update is needed.
    Ignores 'snapshotted_at'.
    """
    IGNORE = {"snapshotted_at"}
    changed = {}
    all_keys = set(old.keys()) | set(new.keys()) - IGNORE
    for k in all_keys:
        if k in IGNORE:
            continue
        if old.get(k) != new.get(k):
            changed[k] = {"was": old.get(k), "now": new.get(k)}
    return changed


# ─────────────────────────────────────────
# Prompt builder
# ─────────────────────────────────────────
def _fmt_bool(val, true_label="YES", false_label="NO", unknown_label="UNKNOWN"):
    if val is True:
        return true_label
    if val is False:
        return false_label
    return unknown_label


def build_prompt(lead: dict, snapshot: dict) -> str:
    """
    Builds the user message for the Claude API call.
    Only exposes values we actually know — unknown/failed checks are
    labelled UNKNOWN so Claude doesn't hallucinate gaps.
    """
    name    = lead.get("name", "Unknown Business")
    address = lead.get("address", "")
    # Extract a useful location hint: skip generic country-level endings
    # e.g. "45 Parramatta Rd, Homebush NSW 2140, Australia" → "Homebush NSW 2140"
    SKIP_PARTS = {"australia", "au"}
    parts = [p.strip() for p in address.split(",")]
    location_hint = ""
    for part in reversed(parts):
        if part.lower() not in SKIP_PARTS and len(part) > 3:
            location_hint = part
            break

    perf = snapshot.get("performance_score")
    perf_str = (
        f"{perf}/100 (mobile)"
        if isinstance(perf, (int, float))
        else "UNKNOWN (check failed or skipped)"
    )

    # Meta Ads — only report if check actually succeeded
    meta_status = snapshot.get("meta_ads_status", "")
    if meta_status in ("ok_no_ads", "ok_ads_found"):
        meta_str = _fmt_bool(snapshot.get("meta_ads_active"), "RUNNING", "NOT RUNNING")
    else:
        meta_str = "UNKNOWN (JS-required, could not verify)"

    # GBP — only report if check gave a clear answer
    gbp_status = snapshot.get("gbp_claimed_status", "")
    if gbp_status in ("unclaimed", "claimed_inferred"):
        gbp_str = _fmt_bool(snapshot.get("gbp_claimed"), "CLAIMED", "UNCLAIMED")
    else:
        gbp_str = "UNKNOWN (could not verify)"

    # If GTM is installed, suppress GA from the prompt entirely — Claude
    # must not flag GA as missing when GTM is present (GA4 fires through GTM).
    ga_str = _fmt_bool(snapshot.get("analytics_installed"), "INSTALLED", "NOT INSTALLED")
    if snapshot.get("gtm_installed"):
        ga_str = "INSTALLED (via GTM — do not flag as missing)"

    return f"""Business: {name}{f", {location_hint}" if location_hint else ""}

=== Audit Results ===
Website:              {_fmt_bool(snapshot.get("has_website"), "EXISTS", "MISSING")}
Title tag:            {_fmt_bool(snapshot.get("title_present"), "PRESENT", "MISSING")}
Meta description:     {_fmt_bool(snapshot.get("meta_description_present"), "PRESENT", "MISSING")}
Mobile responsive:    {_fmt_bool(snapshot.get("viewport_present"), "YES", "NO")}
Google Analytics:     {ga_str}
Google Tag Manager:   {_fmt_bool(snapshot.get("gtm_installed"), "INSTALLED", "NOT INSTALLED")}
PageSpeed score:      {perf_str}
Google Business Profile (GBP): {gbp_str}
Meta Ads:             {meta_str}

Write the gap summary now (1–2 sentences, gaps only, no filler):"""


# ─────────────────────────────────────────
# Claude API call
# ─────────────────────────────────────────
def generate_gap_summary(
    lead: dict,
    snapshot: dict,
    client: anthropic.Anthropic,
    model: str = DEFAULT_MODEL,
) -> str:
    """
    Calls Claude to generate the gap_summary string.
    Returns the summary text, or a fallback string on error.
    """
    prompt = build_prompt(lead, snapshot)
    try:
        msg = client.messages.create(
            model=model,
            max_tokens=MAX_TOKENS,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        return msg.content[0].text.strip()
    except anthropic.APIError as e:
        print(f"  [Summariser][WARN] Claude API error: {e}", file=sys.stderr)
        return f"[summary_error: {str(e)[:80]}]"
    except Exception as e:
        print(f"  [Summariser][WARN] Unexpected error: {e}", file=sys.stderr)
        return f"[summary_error: {str(e)[:80]}]"


# ─────────────────────────────────────────
# Per-lead summariser
# ─────────────────────────────────────────
def summarise_lead(
    lead: dict,
    client: Optional[anthropic.Anthropic],
    model: str = DEFAULT_MODEL,
    dry_run: bool = False,
) -> dict:
    """
    Builds gap_snapshot and gap_summary, appends both to the lead, returns enriched lead.
    dry_run=True skips the Claude API call and leaves gap_summary as None.
    """
    snapshot = build_gap_snapshot(lead)

    if dry_run or client is None:
        summary = None
    else:
        summary = generate_gap_summary(lead, snapshot, client, model)

    return {
        **lead,
        "gap_snapshot": snapshot,
        "gap_summary":  summary,
    }


# ─────────────────────────────────────────
# Batch runner
# ─────────────────────────────────────────
def run_summariser(
    leads: list,
    dry_run: bool = False,
    model: str = DEFAULT_MODEL,
) -> list:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    delay   = float(os.environ.get("SUMMARISE_DELAY_S", "0.5"))
    model   = os.environ.get("CLAUDE_MODEL", model)

    if not dry_run and not api_key:
        print("[Summariser][ERROR] ANTHROPIC_API_KEY not set. Use --dry-run to skip API calls.", file=sys.stderr)
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key) if not dry_run else None

    print(f"[Summariser] ─── Starting ──────────────────────────────────────────────", file=sys.stderr)
    print(f"[Summariser] Leads:    {len(leads)}", file=sys.stderr)
    print(f"[Summariser] Model:    {model}", file=sys.stderr)
    print(f"[Summariser] Dry run:  {dry_run}", file=sys.stderr)

    results = []
    for i, lead in enumerate(leads, start=1):
        name = lead.get("name", f"Lead #{i}")
        print(f"[Summariser] [{i}/{len(leads)}] {name}", file=sys.stderr)
        try:
            enriched = summarise_lead(lead, client, model, dry_run)
            if not dry_run:
                print(f"  → {enriched['gap_summary']}", file=sys.stderr)
            results.append(enriched)
        except Exception as e:
            print(f"[Summariser][ERROR] Failed on '{name}': {e}", file=sys.stderr)
            lead["summariser_error"] = str(e)
            results.append(lead)

        if not dry_run and i < len(leads):
            time.sleep(delay)

    print(f"[Summariser] ─── Done ──────────────────────────────────────────────────", file=sys.stderr)
    return results


# ─────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="LeadScan AI — Agent 3: Gap Summariser",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--input",    help="JSON file of audited leads from Agent 2 (default: stdin)")
    parser.add_argument("--output",   help="Write enriched JSON to this file (default: stdout)")
    parser.add_argument("--single",   help="Process a single lead JSON string (for testing)")
    parser.add_argument("--dry-run",  action="store_true", help="Build gap_snapshot only, skip Claude API call")
    parser.add_argument("--model",    default=DEFAULT_MODEL, help=f"Claude model to use (default: {DEFAULT_MODEL})")
    args = parser.parse_args()

    if args.single:
        leads = [json.loads(args.single)]
    elif args.input:
        with open(args.input, encoding="utf-8") as f:
            leads = json.load(f)
    else:
        raw = sys.stdin.read().strip()
        if not raw:
            print("[Summariser][ERROR] No input. Use --input, --single, or pipe JSON via stdin.", file=sys.stderr)
            sys.exit(1)
        leads = json.loads(raw)

    if not isinstance(leads, list):
        leads = [leads]

    results = run_summariser(leads, dry_run=args.dry_run, model=args.model)
    out     = json.dumps(results, indent=2, ensure_ascii=False)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(out)
        print(f"[Summariser] Written {len(results)} leads to {args.output}", file=sys.stderr)
    else:
        print(out)
