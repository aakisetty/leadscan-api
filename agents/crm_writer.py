#!/usr/bin/env python3
"""
LeadScan AI — Agent 4: CRM Writer
====================================
Takes a JSON array of fully enriched lead objects (from Agent 3: Gap Summariser)
and upserts each one into GoHighLevel as a Contact + Opportunity.

Per lead it:
  1. Formats and normalises contact data (phone → international, address parse)
  2. Assigns the correct pipeline stage based on the gap profile
  3. Searches GHL for an existing contact (phone or name match)
  4. Creates or updates the GHL contact with all fields and custom fields
  5. Creates or updates an Opportunity in the LeadScan pipeline
  6. Triggers the outreach workflow for new contacts (optional)

Usage:
    cat summarised.json | python3 crm_writer.py
    python3 crm_writer.py --input summarised.json --output written.json
    python3 crm_writer.py --input summarised.json --dry-run          # no API calls
    python3 crm_writer.py --single '{"name":"...", ...}'

Required environment variables (set as skill credentials):
    GHL_API_KEY              Location API key (or Private Integration Token)
    GHL_LOCATION_ID          Sub-account / Location ID
    GHL_PIPELINE_ID          LeadScan AI pipeline ID
    GHL_STAGE_NO_WEBSITE     Stage ID — "No website — highest priority"
    GHL_STAGE_WEBSITE_NO_ADS Stage ID — "Website only — ads opportunity"
    GHL_STAGE_NEEDS_GBP_ADS  Stage ID — "Needs GBP + Google Ads"
    GHL_STAGE_NURTURE         Stage ID — "Nurture — monitor only"

Optional:
    GHL_WORKFLOW_ID          Workflow ID to trigger for new contacts
    WRITE_DELAY_S            Seconds between writes (default: 0.5)
    COUNTRY_CODE             Default country code (default: AU)

Custom fields — auto-discovered from GHL by key name.
Create the following custom fields in GHL → Settings → Custom Fields:
    Key: leadscan_maps_url        Label: LeadScan Maps URL       Type: Text
    Key: leadscan_gap_summary     Label: LeadScan Gap Summary    Type: Text Area
    Key: leadscan_gap_snapshot    Label: LeadScan Gap Snapshot   Type: Text Area
    Key: leadscan_source_query    Label: LeadScan Source Query   Type: Text
    Key: leadscan_last_scraped    Label: LeadScan Last Scraped   Type: Text
    Key: leadscan_last_audited    Label: LeadScan Last Audited   Type: Text
"""

import os
import re
import sys
import json
import time
import argparse
import requests
from datetime import datetime, timezone
from typing import Optional

# ─────────────────────────────────────────
# Constants
# ─────────────────────────────────────────
GHL_BASE      = "https://services.leadconnectorhq.com"
GHL_VERSION   = "2021-07-28"
WRITE_DELAY   = 0.5   # seconds between API calls

# The keys we look for when discovering custom fields from GHL
CF_KEY_MAP = {
    "maps_url":      "leadscan_maps_url",
    "gap_summary":   "leadscan_gap_summary",
    "gap_snapshot":  "leadscan_gap_snapshot",
    "source_query":  "leadscan_source_query",
    "last_scraped":  "leadscan_last_scraped",
    "last_audited":  "leadscan_last_audited",
}

AU_STATES = {"ACT", "NSW", "NT", "QLD", "SA", "TAS", "VIC", "WA"}


# ─────────────────────────────────────────
# GHL HTTP helpers
# ─────────────────────────────────────────
def _headers(api_key: str) -> dict:
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type":  "application/json",
        "Version":       GHL_VERSION,
    }


def _get(path: str, api_key: str, params: dict = None) -> dict:
    r = requests.get(f"{GHL_BASE}{path}", headers=_headers(api_key), params=params, timeout=15)
    r.raise_for_status()
    return r.json()


def _post(path: str, api_key: str, body: dict) -> dict:
    r = requests.post(f"{GHL_BASE}{path}", headers=_headers(api_key), json=body, timeout=15)
    r.raise_for_status()
    return r.json()


def _put(path: str, api_key: str, body: dict) -> dict:
    r = requests.put(f"{GHL_BASE}{path}", headers=_headers(api_key), json=body, timeout=15)
    r.raise_for_status()
    return r.json()


# ─────────────────────────────────────────
# Config & custom field discovery
# ─────────────────────────────────────────
def load_config() -> dict:
    required = [
        "GHL_API_KEY", "GHL_LOCATION_ID", "GHL_PIPELINE_ID",
        "GHL_STAGE_NO_WEBSITE", "GHL_STAGE_WEBSITE_NO_ADS",
        "GHL_STAGE_NEEDS_GBP_ADS", "GHL_STAGE_NURTURE",
    ]
    missing = [k for k in required if not os.environ.get(k, "").strip()]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    return {
        "api_key":     os.environ["GHL_API_KEY"].strip(),
        "location_id": os.environ["GHL_LOCATION_ID"].strip(),
        "pipeline_id": os.environ["GHL_PIPELINE_ID"].strip(),
        "stages": {
            "no_website":     os.environ["GHL_STAGE_NO_WEBSITE"].strip(),
            "website_no_ads": os.environ["GHL_STAGE_WEBSITE_NO_ADS"].strip(),
            "needs_gbp_ads":  os.environ["GHL_STAGE_NEEDS_GBP_ADS"].strip(),
            "nurture":        os.environ["GHL_STAGE_NURTURE"].strip(),
        },
        "workflow_id":  os.environ.get("GHL_WORKFLOW_ID", "").strip() or None,
        "write_delay":  float(os.environ.get("WRITE_DELAY_S", WRITE_DELAY)),
        "country_code": os.environ.get("COUNTRY_CODE", "AU").strip(),
    }


def discover_custom_fields(location_id: str, api_key: str) -> dict:
    """
    Fetches all custom fields for the location from GHL and returns
    a mapping of our logical names → GHL field IDs.
    e.g. {"maps_url": "abc123", "gap_summary": "def456", ...}

    Matches by checking if the field key contains our expected key string.
    GHL field keys typically look like: contact.leadscan_maps_url
    """
    try:
        data = _get(f"/locations/{location_id}/customFields", api_key)
        fields = data.get("customFields", [])
    except Exception as e:
        print(f"[Writer][WARN] Could not fetch custom fields: {e}", file=sys.stderr)
        return {}

    cf_ids = {}
    for logical_name, key_fragment in CF_KEY_MAP.items():
        for f in fields:
            field_key = f.get("fieldKey", "") or f.get("key", "")
            if key_fragment in field_key:
                cf_ids[logical_name] = f.get("id", "")
                break

    missing = [k for k in CF_KEY_MAP if k not in cf_ids]
    if missing:
        print(
            f"[Writer][WARN] Could not find custom fields for: {missing}. "
            f"Create them in GHL → Settings → Custom Fields with keys: "
            f"{[CF_KEY_MAP[k] for k in missing]}",
            file=sys.stderr,
        )

    found = len(cf_ids)
    print(f"[Writer] Custom fields discovered: {found}/{len(CF_KEY_MAP)}", file=sys.stderr)
    return cf_ids


# ─────────────────────────────────────────
# Data formatting
# ─────────────────────────────────────────
def format_phone(phone: str, country: str = "AU") -> str:
    """
    Attempts to normalise a phone number to international format.
    Falls back to returning the raw string if it can't be parsed.
    """
    if not phone:
        return ""
    clean = re.sub(r"[\s\-\(\)\.]", "", phone)
    if country == "AU":
        # Local landline: 02/03/07/08 + 8 digits → +61 X XXXX XXXX
        if re.match(r"^0([2378])\d{8}$", clean):
            area = clean[1]
            num  = clean[2:]
            return f"+61 {area} {num[:4]} {num[4:]}"
        # Local mobile: 04XX + 7 digits → +61 4XX XXX XXX
        if re.match(r"^04\d{8}$", clean):
            return f"+61 {clean[1:4]} {clean[4:7]} {clean[7:]}"
        # Already has 61 country code (no +)
        if re.match(r"^61[2-9]\d{8}$", clean):
            return f"+{clean[:2]} {clean[2]} {clean[3:7]} {clean[7:]}"
    # Already internationalised — return as-is (GHL normalises internally)
    if clean.startswith("+"):
        return clean
    return phone   # Return original if we can't normalise


def parse_address(address: str) -> dict:
    """
    Attempts to split a formatted address into GHL fields.
    Falls back to putting the full address in address1.
    Handles Australian format: "Street, Suburb STATE PostCode, Australia"
    """
    # Strip country suffix
    addr = re.sub(r",?\s*Australia\s*$", "", address.strip(), flags=re.I)

    # Try: "Street, Suburb STATE PostCode"
    pattern = r"^(.*?),\s*(.+?)\s+(" + "|".join(AU_STATES) + r")\s+(\d{4})\s*$"
    m = re.match(pattern, addr, flags=re.I)
    if m:
        return {
            "address1":   m.group(1).strip(),
            "city":       m.group(2).strip(),
            "state":      m.group(3).upper(),
            "postalCode": m.group(4),
            "country":    "AU",
        }

    return {"address1": addr, "city": "", "state": "", "postalCode": "", "country": "AU"}


def make_tags(lead: dict) -> list:
    """
    Returns a list of GHL tags: source_query slug + "leadscan".
    e.g. ["plumbers__sydney", "leadscan"]
    """
    tags = ["leadscan"]
    sq = lead.get("source_query", "")
    if sq:
        tags.append(sq)
    return tags


def build_custom_fields(lead: dict, cf_ids: dict) -> list:
    """
    Builds the customFields array for the GHL contact/opportunity payload.
    Only includes fields for which we have a discovered ID.
    """
    snap = lead.get("gap_snapshot") or {}
    values = {
        "maps_url":     lead.get("maps_url", ""),
        "gap_summary":  lead.get("gap_summary", ""),
        "gap_snapshot": json.dumps(snap, ensure_ascii=False) if snap else "",
        "source_query": lead.get("source_query", ""),
        "last_scraped": lead.get("last_scraped_at", ""),
        "last_audited": lead.get("last_audited_at", ""),
    }
    result = []
    for logical, field_id in cf_ids.items():
        val = values.get(logical, "")
        if field_id and val is not None:
            result.append({"id": field_id, "field_value": str(val)})
    return result


# ─────────────────────────────────────────
# Pipeline stage assignment
# ─────────────────────────────────────────
def assign_stage(lead: dict, stages: dict) -> str:
    """
    Returns the GHL pipeline stage ID based on the lead's gap profile.

    Priority order:
      1. No website         → no_website stage
      2. GBP unclaimed      → needs_gbp_ads stage (high-confidence, always actable)
      3. Confirmed no Meta Ads (meta_ads_status == "ok_no_ads")
                            → website_no_ads stage
      4. Everything else    → nurture stage
    """
    snap = lead.get("gap_snapshot") or {}

    if not lead.get("has_website"):
        return stages["no_website"]

    if lead.get("gbp_claimed") is False:
        return stages["needs_gbp_ads"]

    if snap.get("meta_ads_status") == "ok_no_ads":
        return stages["website_no_ads"]

    return stages["nurture"]


# ─────────────────────────────────────────
# GHL contact operations
# ─────────────────────────────────────────
def search_contact(lead: dict, location_id: str, api_key: str) -> Optional[dict]:
    """
    Searches GHL for an existing contact by phone (primary) then business name.
    Returns the first matching contact dict, or None.
    """
    def _search(query: str):
        try:
            data = _get("/contacts/", api_key, {"locationId": location_id, "query": query, "limit": 5})
            return data.get("contacts", [])
        except Exception:
            return []

    # 1. Phone search
    phone = lead.get("phone", "")
    if phone:
        contacts = _search(phone)
        if contacts:
            return contacts[0]

    # 2. Name search — normalised compare
    name = lead.get("name", "").lower().strip()
    if name:
        contacts = _search(lead.get("name", ""))
        for c in contacts:
            existing = (c.get("companyName") or c.get("contactName") or "").lower().strip()
            if existing == name:
                return c

    return None


def build_contact_body(lead: dict, location_id: str, cf_ids: dict, country: str = "AU") -> dict:
    name     = lead.get("name", "")
    phone    = format_phone(lead.get("phone", ""), country)
    addr     = parse_address(lead.get("address", ""))
    tags     = make_tags(lead)
    cf_list  = build_custom_fields(lead, cf_ids)

    return {
        "locationId":  location_id,
        "firstName":   name,           # Business name in firstName for correct display
        "companyName": name,
        "phone":       phone,
        "website":     lead.get("website", ""),
        "address1":    addr["address1"],
        "city":        addr["city"],
        "state":       addr["state"],
        "postalCode":  addr["postalCode"],
        "country":     addr["country"],
        "tags":        tags,
        "source":      "LeadScan AI",
        **({"customFields": cf_list} if cf_list else {}),
    }


def upsert_contact(lead: dict, cfg: dict, cf_ids: dict, existing: Optional[dict]) -> tuple:
    """
    Creates or updates a GHL contact. Returns (contact_id, action).
    action is "created" or "updated".
    """
    body = build_contact_body(lead, cfg["location_id"], cf_ids, cfg["country_code"])

    if existing:
        contact_id = existing.get("id")
        _put(f"/contacts/{contact_id}", cfg["api_key"], body)
        return contact_id, "updated"
    else:
        resp = _post("/contacts/", cfg["api_key"], body)
        contact_id = resp.get("contact", {}).get("id") or resp.get("id", "")
        return contact_id, "created"


# ─────────────────────────────────────────
# GHL opportunity operations
# ─────────────────────────────────────────
def find_opportunity(contact_id: str, pipeline_id: str, location_id: str, api_key: str) -> Optional[dict]:
    """
    Searches for an existing opportunity for this contact in the LeadScan pipeline.
    Returns the first match, or None.
    """
    try:
        data = _get(
            "/opportunities/search",
            api_key,
            {"location_id": location_id, "contact_id": contact_id, "pipeline_id": pipeline_id},
        )
        opps = data.get("opportunities", [])
        return opps[0] if opps else None
    except Exception as e:
        print(f"[Writer][WARN] Opportunity search failed: {e}", file=sys.stderr)
        return None


def upsert_opportunity(
    contact_id: str,
    lead: dict,
    cfg: dict,
    existing_opp: Optional[dict],
) -> tuple:
    """
    Creates or updates a GHL opportunity. Returns (opportunity_id, action).
    """
    stage_id = assign_stage(lead, cfg["stages"])
    name     = lead.get("name", "Unknown")
    sq       = lead.get("source_query", "leadscan")
    title    = f"{name} — {sq}"

    if existing_opp:
        opp_id = existing_opp.get("id", "")
        _put(
            f"/opportunities/{opp_id}",
            cfg["api_key"],
            {"pipelineStageId": stage_id},
        )
        return opp_id, "updated"
    else:
        body = {
            "locationId":      cfg["location_id"],
            "pipelineId":      cfg["pipeline_id"],
            "pipelineStageId": stage_id,
            "contactId":       contact_id,
            "name":            title,
            "status":          "open",
            "monetaryValue":   0,
        }
        resp = _post("/opportunities/", cfg["api_key"], body)
        opp_id = resp.get("opportunity", {}).get("id") or resp.get("id", "")
        return opp_id, "created"


# ─────────────────────────────────────────
# Workflow trigger
# ─────────────────────────────────────────
def trigger_workflow(contact_id: str, workflow_id: str, api_key: str) -> bool:
    """
    Subscribes a contact to a GHL workflow. Returns True on success.
    """
    try:
        _post(f"/contacts/{contact_id}/workflow/{workflow_id}", api_key, {})
        return True
    except Exception as e:
        print(f"[Writer][WARN] Workflow trigger failed: {e}", file=sys.stderr)
        return False


# ─────────────────────────────────────────
# Per-lead writer
# ─────────────────────────────────────────
def write_lead(lead: dict, cfg: dict, cf_ids: dict, dry_run: bool = False) -> dict:
    """
    Full write cycle for one lead. Returns the lead enriched with GHL IDs.
    """
    name  = lead.get("name", "")
    stage = assign_stage(lead, cfg["stages"])

    # Stage label for logging
    stage_label = {v: k for k, v in cfg["stages"].items()}.get(stage, stage)

    if dry_run:
        print(f"  [DRY RUN] Would write '{name}' → stage: {stage_label}", file=sys.stderr)
        return {
            **lead,
            "ghl_contact_id":         None,
            "ghl_opportunity_id":     None,
            "ghl_contact_action":     "dry_run",
            "ghl_opp_action":         "dry_run",
            "ghl_pipeline_stage":     stage_label,
            "ghl_workflow_triggered": False,
            "ghl_written_at":         datetime.now(timezone.utc).isoformat(),
        }

    # 1. Check for existing contact
    existing_contact = search_contact(lead, cfg["location_id"], cfg["api_key"])
    time.sleep(0.2)

    # 2. Upsert contact
    contact_id, contact_action = upsert_contact(lead, cfg, cf_ids, existing_contact)
    time.sleep(0.2)

    # 3. Find existing opportunity
    existing_opp = find_opportunity(contact_id, cfg["pipeline_id"], cfg["location_id"], cfg["api_key"])
    time.sleep(0.2)

    # 4. Upsert opportunity
    opp_id, opp_action = upsert_opportunity(contact_id, lead, cfg, existing_opp)
    time.sleep(0.2)

    # 5. Trigger workflow (new contacts only)
    workflow_triggered = False
    if contact_action == "created" and cfg.get("workflow_id"):
        workflow_triggered = trigger_workflow(contact_id, cfg["workflow_id"], cfg["api_key"])

    print(
        f"  contact {contact_action} ({contact_id[:8]}…) | "
        f"opp {opp_action} → {stage_label}"
        + (" | workflow triggered" if workflow_triggered else ""),
        file=sys.stderr,
    )

    return {
        **lead,
        "ghl_contact_id":       contact_id,
        "ghl_opportunity_id":   opp_id,
        "ghl_contact_action":   contact_action,
        "ghl_opp_action":       opp_action,
        "ghl_pipeline_stage":   stage_label,
        "ghl_workflow_triggered": workflow_triggered,
        "ghl_written_at":       datetime.now(timezone.utc).isoformat(),
    }


# ─────────────────────────────────────────
# Batch runner
# ─────────────────────────────────────────
def run_writer(leads: list, dry_run: bool = False) -> list:
    cfg    = load_config()
    cf_ids = {} if dry_run else discover_custom_fields(cfg["location_id"], cfg["api_key"])

    print(f"[Writer] ─── Starting ──────────────────────────────────────────────────", file=sys.stderr)
    print(f"[Writer] Leads:    {len(leads)}", file=sys.stderr)
    print(f"[Writer] Dry run:  {dry_run}", file=sys.stderr)

    results = []
    for i, lead in enumerate(leads, start=1):
        name = lead.get("name", f"Lead #{i}")
        print(f"[Writer] [{i}/{len(leads)}] {name}", file=sys.stderr)
        try:
            written = write_lead(lead, cfg, cf_ids, dry_run)
            results.append(written)
        except requests.exceptions.HTTPError as e:
            code = e.response.status_code if e.response else "?"
            body = e.response.text[:200] if e.response else ""
            print(f"[Writer][ERROR] HTTP {code} on '{name}': {body}", file=sys.stderr)
            lead["ghl_error"] = f"HTTP {code}: {body}"
            results.append(lead)
        except Exception as e:
            print(f"[Writer][ERROR] Failed on '{name}': {e}", file=sys.stderr)
            lead["ghl_error"] = str(e)
            results.append(lead)

        if i < len(leads):
            time.sleep(cfg["write_delay"])

    ok      = sum(1 for r in results if "ghl_contact_id" in r and r.get("ghl_contact_id"))
    errored = sum(1 for r in results if "ghl_error" in r)
    print(f"[Writer] ─── Done ─────── {ok} written, {errored} errors ──────────────", file=sys.stderr)
    return results


# ─────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="LeadScan AI — Agent 4: CRM Writer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--input",    help="JSON file of leads from Agent 3 (default: stdin)")
    parser.add_argument("--output",   help="Write results to this file (default: stdout)")
    parser.add_argument("--single",   help="Write a single lead JSON string")
    parser.add_argument("--dry-run",  action="store_true", help="Print stage assignments without calling GHL")
    args = parser.parse_args()

    if args.single:
        leads = [json.loads(args.single)]
    elif args.input:
        with open(args.input, encoding="utf-8") as f:
            leads = json.load(f)
    else:
        raw = sys.stdin.read().strip()
        if not raw:
            print("[Writer][ERROR] No input. Use --input, --single, or pipe JSON.", file=sys.stderr)
            sys.exit(1)
        leads = json.loads(raw)

    if not isinstance(leads, list):
        leads = [leads]

    results  = run_writer(leads, dry_run=args.dry_run)
    out_json = json.dumps(results, indent=2, ensure_ascii=False)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(out_json)
        print(f"[Writer] Written {len(results)} leads to {args.output}", file=sys.stderr)
    else:
        print(out_json)
