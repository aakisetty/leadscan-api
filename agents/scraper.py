#!/usr/bin/env python3
"""
LeadScan AI — Agent 1: Scraper
===============================
Turns { industry, location } into a list of raw lead objects, each
deduplicated against existing GoHighLevel contacts.

Usage:
    python3 scraper.py "plumbers" "Sydney"
    python3 scraper.py "plumbers" "Sydney" --skip-dedup
    python3 scraper.py "plumbers" "Sydney" --output leads.json

Writes JSON array to stdout (or --output file).
Progress/debug messages go to stderr.

Required environment variables:
    GOOGLE_PLACES_API_KEY   Google Places API (New) key
    GHL_API_KEY             GoHighLevel Location API key
    GHL_LOCATION_ID         GoHighLevel sub-account / Location ID

Optional:
    REGION_CODE             Two-letter country code (default: AU)
    MAX_PAGES               Max pages of Places results (default: 3 → up to 60 results)
"""

import os
import sys
import json
import time
import argparse
import requests
from datetime import datetime, timezone

# ─────────────────────────────────────────
# Constants
# ─────────────────────────────────────────
PLACES_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"

# Only request the fields we actually use — keeps billing minimal.
# Priced fields: displayName, formattedAddress, nationalPhoneNumber,
#                internationalPhoneNumber, websiteUri, googleMapsUri → "Basic" tier
# businessStatus, id, types → free
PLACES_FIELD_MASK = ",".join([
    "places.id",
    "places.displayName",
    "places.formattedAddress",
    "places.nationalPhoneNumber",
    "places.internationalPhoneNumber",
    "places.websiteUri",
    "places.googleMapsUri",
    "places.businessStatus",
    "places.types",
])
# Note: nextPageToken is returned at the response root level,
# NOT as a place field — do not include it in the FieldMask.

GHL_BASE_URL      = "https://services.leadconnectorhq.com"
GHL_API_VERSION   = "2021-07-28"

DEFAULT_MAX_PAGES  = 3     # 3 pages × 20 = up to 60 results per query
DEFAULT_PAGE_SIZE  = 20    # Places API max per page
GHL_RATE_LIMIT_MS  = 0.3   # seconds between GHL dedup calls


# ─────────────────────────────────────────
# Google Places scraping
# ─────────────────────────────────────────
def scrape_google_places(
    query: str,
    api_key: str,
    region_code: str = "AU",
    max_pages: int = DEFAULT_MAX_PAGES,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> list:
    """
    Calls Google Places Text Search (New) API.
    Paginates up to max_pages, returns list of raw Place dicts.
    """
    results   = []
    page_token = None
    headers   = {
        "X-Goog-Api-Key":   api_key,
        "X-Goog-FieldMask": PLACES_FIELD_MASK,
        "Content-Type":     "application/json",
    }

    for page_num in range(max_pages):
        body = {
            "textQuery":    query,
            "pageSize":     page_size,
            "languageCode": "en",
            "regionCode":   region_code,
        }
        if page_token:
            body["pageToken"] = page_token

        try:
            resp = requests.post(
                PLACES_SEARCH_URL,
                headers=headers,
                json=body,
                timeout=15,
            )
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"[Scraper][ERROR] Places API HTTP error on page {page_num + 1}: {e}", file=sys.stderr)
            print(f"[Scraper][ERROR] Response body: {resp.text[:500]}", file=sys.stderr)
            break
        except requests.exceptions.RequestException as e:
            print(f"[Scraper][ERROR] Places API request failed on page {page_num + 1}: {e}", file=sys.stderr)
            break

        data   = resp.json()
        places = data.get("places", [])
        results.extend(places)
        print(f"[Scraper] Page {page_num + 1}: got {len(places)} results (total so far: {len(results)})", file=sys.stderr)

        page_token = data.get("nextPageToken")
        if not page_token:
            break

        # Google recommends a brief pause between paginated calls
        time.sleep(1.5)

    return results


# ─────────────────────────────────────────
# Lead object construction
# ─────────────────────────────────────────
def parse_place(place: dict, source_query: str, scraped_at: str) -> dict:
    """
    Maps a raw Google Places object into the LeadScan lead object schema.
    """
    name    = place.get("displayName", {}).get("text", "").strip()
    phone   = (
        place.get("nationalPhoneNumber")
        or place.get("internationalPhoneNumber", "")
        or ""
    ).strip()
    website = place.get("websiteUri", "").rstrip("/").strip()

    return {
        "name":              name,
        "address":           place.get("formattedAddress", "").strip(),
        "phone":             phone,
        "website":           website,
        "maps_url":          place.get("googleMapsUri", ""),
        "google_place_id":   place.get("id", ""),
        "business_status":   place.get("businessStatus", "OPERATIONAL"),
        "source_query":      source_query,
        "last_scraped_at":   scraped_at,
        # Downstream agents will append:
        # has_website, seo_results, ads_results, gap_summary,
        # gap_snapshot, last_audited_at
    }


def make_source_slug(industry: str, location: str) -> str:
    """Normalised slug used as GHL tag and for Scheduler queries."""
    def slugify(s):
        return s.lower().strip().replace(" ", "_").replace(",", "")
    return f"{slugify(industry)}__{slugify(location)}"


# ─────────────────────────────────────────
# GHL deduplication
# ─────────────────────────────────────────
def _ghl_headers(api_key: str) -> dict:
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type":  "application/json",
        "Version":       GHL_API_VERSION,
    }


def _ghl_search(query_str: str, location_id: str, api_key: str) -> list:
    """
    Searches GHL contacts by a free-text query (matches name, phone, email).
    Returns list of contact dicts (empty list on error).
    """
    url    = f"{GHL_BASE_URL}/contacts/"
    params = {
        "locationId": location_id,
        "query":      query_str,
        "limit":      5,
    }
    try:
        resp = requests.get(url, headers=_ghl_headers(api_key), params=params, timeout=10)
        if resp.status_code == 200:
            return resp.json().get("contacts", [])
        else:
            print(f"[Scraper][WARN] GHL search returned {resp.status_code} for query '{query_str}'", file=sys.stderr)
            return []
    except requests.exceptions.RequestException as e:
        print(f"[Scraper][WARN] GHL search request failed: {e}", file=sys.stderr)
        return []


def check_ghl_duplicate(lead: dict, location_id: str, api_key: str) -> bool:
    """
    Returns True if this business already exists in GHL.
    Checks phone first (most precise), falls back to business name.
    """
    # 1. Phone match — most reliable dedup signal
    if lead["phone"]:
        contacts = _ghl_search(lead["phone"], location_id, api_key)
        if contacts:
            existing = contacts[0]
            existing_name = existing.get("companyName") or existing.get("contactName", "")
            print(
                f"  [DEDUP] Phone match → '{existing_name}' "
                f"(id: {existing.get('id', 'n/a')})",
                file=sys.stderr,
            )
            return True

    # 2. Business name match — normalised compare
    if lead["name"]:
        contacts = _ghl_search(lead["name"], location_id, api_key)
        for contact in contacts:
            existing_name = (
                contact.get("companyName", "")
                or contact.get("contactName", "")
            ).lower().strip()
            if existing_name == lead["name"].lower().strip():
                print(
                    f"  [DEDUP] Name match → '{existing_name}' "
                    f"(id: {contact.get('id', 'n/a')})",
                    file=sys.stderr,
                )
                return True

    return False


# ─────────────────────────────────────────
# Main orchestration
# ─────────────────────────────────────────
def run_scraper(
    industry: str,
    location: str,
    skip_dedup: bool = False,
    region_code: str = "AU",
    max_pages: int = DEFAULT_MAX_PAGES,
) -> list:
    """
    Full scraper pipeline:
      1. Google Places Text Search (paginated)
      2. Filter out permanently-closed businesses
      3. Parse into lead objects
      4. GHL dedup check (unless skip_dedup)
    Returns list of new lead objects ready to pass to Agent 2.
    """
    places_key     = os.environ.get("GOOGLE_PLACES_API_KEY", "").strip()
    ghl_key        = os.environ.get("GHL_API_KEY", "").strip()
    ghl_location   = os.environ.get("GHL_LOCATION_ID", "").strip()
    region_code    = os.environ.get("REGION_CODE", region_code)
    max_pages      = int(os.environ.get("MAX_PAGES", max_pages))

    if not places_key:
        raise ValueError("GOOGLE_PLACES_API_KEY environment variable is required")
    if not skip_dedup and (not ghl_key or not ghl_location):
        raise ValueError(
            "GHL_API_KEY and GHL_LOCATION_ID are required for dedup check "
            "(set them or pass --skip-dedup)"
        )

    query        = f"{industry} in {location}"
    source_query = make_source_slug(industry, location)
    scraped_at   = datetime.now(timezone.utc).isoformat()

    print(f"[Scraper] ─── Starting ───────────────────────────────────────", file=sys.stderr)
    print(f"[Scraper] Query:        '{query}'", file=sys.stderr)
    print(f"[Scraper] Source slug:  {source_query}", file=sys.stderr)
    print(f"[Scraper] Region:       {region_code}", file=sys.stderr)
    print(f"[Scraper] Max pages:    {max_pages}", file=sys.stderr)

    # Step 1 — Fetch from Google Places
    raw_places = scrape_google_places(query, places_key, region_code, max_pages)
    print(f"[Scraper] Total raw results: {len(raw_places)}", file=sys.stderr)

    # Step 2 — Filter permanently closed
    active = [
        p for p in raw_places
        if p.get("businessStatus", "OPERATIONAL") != "CLOSED_PERMANENTLY"
    ]
    print(f"[Scraper] Active businesses: {len(active)} ({len(raw_places) - len(active)} closed filtered)", file=sys.stderr)

    # Step 3 — Parse into lead objects
    leads = [parse_place(p, source_query, scraped_at) for p in active]

    # Step 4 — Dedup against GHL
    if skip_dedup:
        print(f"[Scraper] Dedup check skipped.", file=sys.stderr)
        new_leads = leads
    else:
        print(f"[Scraper] Running GHL dedup check on {len(leads)} leads...", file=sys.stderr)
        new_leads = []
        skipped   = 0
        for i, lead in enumerate(leads, start=1):
            print(f"[Scraper] [{i}/{len(leads)}] {lead['name']}", file=sys.stderr)
            if check_ghl_duplicate(lead, ghl_location, ghl_key):
                skipped += 1
            else:
                new_leads.append(lead)
            time.sleep(GHL_RATE_LIMIT_MS)

        print(
            f"[Scraper] Dedup complete: {len(new_leads)} new, {skipped} existing (skipped).",
            file=sys.stderr,
        )

    print(f"[Scraper] ─── Done ─────────────────────────────────────────────", file=sys.stderr)
    return new_leads


# ─────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="LeadScan AI — Agent 1: Scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("industry",     help='Industry to search, e.g. "plumbers"')
    parser.add_argument("location",     help='Location to search, e.g. "Sydney"')
    parser.add_argument("--skip-dedup", action="store_true", help="Skip GHL dedup check (useful for testing)")
    parser.add_argument("--region",     default="AU",        help="Two-letter country/region code (default: AU)")
    parser.add_argument("--max-pages",  type=int, default=3, help="Max pages of Places results (default: 3 = up to 60)")
    parser.add_argument("--output",     help="Write JSON output to this file instead of stdout")
    args = parser.parse_args()

    try:
        leads = run_scraper(
            args.industry,
            args.location,
            skip_dedup=args.skip_dedup,
            region_code=args.region,
            max_pages=args.max_pages,
        )
    except ValueError as e:
        print(f"[Scraper][ERROR] {e}", file=sys.stderr)
        sys.exit(1)

    output_json = json.dumps(leads, indent=2, ensure_ascii=False)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as fh:
            fh.write(output_json)
        print(f"[Scraper] Written {len(leads)} leads to {args.output}", file=sys.stderr)
    else:
        print(output_json)
