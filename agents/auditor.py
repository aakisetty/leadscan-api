#!/usr/bin/env python3
"""
LeadScan AI — Agent 2: Auditor
================================
Takes a JSON array of lead objects (from Agent 1: Scraper) and runs
five checks against each one, appending structured results.

  Check 1: Website     — HTTP HEAD/GET; flags has_website: false if none/dead
  Check 2: SEO DOM     — Parses homepage for title, meta description, viewport,
                         Google Analytics, and Google Tag Manager
  Check 3: PageSpeed   — Google PageSpeed Insights API (mobile, free tier)
                         returns performance_score (0–100) + FCP in ms
  Check 4: GBP Claimed — Fetches the Google Maps listing page and looks for
                         "Claim this business" / "Own this business?" text
  Check 5: Meta Ads    — Requests-based scan of Meta Ad Library public search;
                         graceful fallback to "check_failed" when JS is required
  Google Ads           — Phase 1: always "manual_check_required"

Usage:
    cat leads.json | python3 auditor.py
    python3 auditor.py --input leads.json --output audited.json
    python3 auditor.py --single '{"name":"Blue Sky Plumbing","website":"https://..."}'
    python3 auditor.py --input leads.json --no-meta --no-pagespeed  # fast mode

Required environment variables:
    PAGESPEED_API_KEY    Google PageSpeed Insights API key (free — enable at
                         console.cloud.google.com → APIs & Services → PageSpeed Insights API)

Optional environment variables:
    AUDIT_DELAY_S        Seconds to wait between audits (default: 1.5)
    SKIP_META_ADS        Set to "1" to skip Meta Ads check
    SKIP_PAGESPEED       Set to "1" to skip PageSpeed check
"""

import os
import sys
import json
import time
import re
import argparse
import requests
from datetime import datetime, timezone

try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False
    print(
        "[Auditor][WARN] BeautifulSoup4 not installed. "
        "Run: pip install beautifulsoup4 lxml",
        file=sys.stderr,
    )


# ─────────────────────────────────────────
# Constants
# ─────────────────────────────────────────
PAGESPEED_URL   = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
META_ADS_URL    = "https://www.facebook.com/ads/library/"
REQUEST_TIMEOUT = 15
PS_TIMEOUT      = 35
META_TIMEOUT    = 12

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
HEADERS = {
    "User-Agent":      UA,
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.5",
}


# ─────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────
def normalise_url(url: str) -> str:
    if not url or not url.strip():
        return ""
    url = url.strip().rstrip("/")
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url


# ─────────────────────────────────────────
# Check 1: Website reachability
# ─────────────────────────────────────────
def check_website(website_url: str) -> dict:
    """
    Returns:
        has_website (bool)
        website_status ("ok" | "no_url" | "connection_error" | "timeout" | "ssl_error" | "error" | HTTP code)
        website_status_code (int | None)
    """
    url = normalise_url(website_url)
    if not url:
        return {"has_website": False, "website_status": "no_url", "website_status_code": None}

    def _try(method_fn, label):
        try:
            resp = method_fn(url, timeout=REQUEST_TIMEOUT, allow_redirects=True, headers=HEADERS)
            code = resp.status_code
            if hasattr(resp, "close"):
                resp.close()
            ok = code < 400
            return {"has_website": ok, "website_status": "ok" if ok else f"http_{code}", "website_status_code": code}
        except requests.exceptions.SSLError:
            return None  # signal to try HTTP fallback
        except requests.exceptions.ConnectionError:
            return {"has_website": False, "website_status": "connection_error", "website_status_code": None}
        except requests.exceptions.Timeout:
            return {"has_website": False, "website_status": "timeout", "website_status_code": None}
        except Exception as e:
            return {"has_website": False, "website_status": f"error:{str(e)[:60]}", "website_status_code": None}

    # 1. HEAD request (fast, minimal data)
    result = _try(requests.head, "HEAD")
    # Some servers reject HEAD with 405 — fall back to GET
    if result and result.get("website_status_code") == 405:
        result = _try(lambda u, **kw: requests.get(u, stream=True, **kw), "GET")

    # SSL errors → try http://
    if result is None:
        http_url = url.replace("https://", "http://", 1)
        try:
            resp = requests.head(http_url, timeout=REQUEST_TIMEOUT, allow_redirects=True, headers=HEADERS)
            code = resp.status_code
            ok   = code < 400
            result = {
                "has_website":         ok,
                "website_status":      "ssl_fallback_ok" if ok else f"ssl_fallback_http_{code}",
                "website_status_code": code,
            }
        except Exception as e:
            result = {"has_website": False, "website_status": f"ssl_error:{str(e)[:60]}", "website_status_code": None}

    return result


# ─────────────────────────────────────────
# Check 2: SEO DOM audit
# ─────────────────────────────────────────
def check_seo_dom(url: str) -> dict:
    """
    Fetches the homepage and parses:
    - <title> (presence + text)
    - <meta name="description"> (presence + text)
    - <meta name="viewport"> (mobile responsiveness signal)
    - Google Analytics / GA4 (script detection)
    - Google Tag Manager (script + noscript detection)
    """
    base = {
        "title_present":            False,
        "title_text":               None,
        "meta_description_present": False,
        "meta_description_text":    None,
        "viewport_present":         False,
        "analytics_installed":      False,
        "gtm_installed":            False,
        "seo_dom_status":           "not_run",
    }

    if not BS4_AVAILABLE:
        base["seo_dom_status"] = "skipped_no_bs4"
        return base

    try:
        resp = requests.get(
            url,
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
            headers=HEADERS,
        )
        resp.raise_for_status()
        html = resp.text
    except Exception as e:
        base["seo_dom_status"] = f"fetch_error:{str(e)[:80]}"
        return base

    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")

    # Title
    t = soup.find("title")
    if t:
        text = (t.string or t.get_text("", strip=True)).strip()
        if text:
            base["title_present"] = True
            base["title_text"]    = text[:120]

    # Meta description (standard + og:description fallback)
    md = (
        soup.find("meta", attrs={"name": re.compile(r"^description$", re.I)})
        or soup.find("meta", attrs={"property": re.compile(r"^og:description$", re.I)})
    )
    if md:
        c = (md.get("content") or "").strip()
        if c:
            base["meta_description_present"] = True
            base["meta_description_text"]    = c[:200]

    # Viewport
    if soup.find("meta", attrs={"name": re.compile(r"^viewport$", re.I)}):
        base["viewport_present"] = True

    # Analytics / GTM
    for s in soup.find_all("script"):
        src  = s.get("src") or ""
        body = s.string or ""

        if (
            "google-analytics.com/analytics.js" in src
            or "googletagmanager.com/gtag/js" in src
            or re.search(r"\bgtag\s*\(", body)
            or "GoogleAnalyticsObject" in body
        ):
            base["analytics_installed"] = True

        if (
            "googletagmanager.com/gtm.js" in src
            or re.search(r"GTM-[A-Z0-9]{4,}", body)
        ):
            base["gtm_installed"] = True

    # GTM also appears in <noscript> iframes
    if not base["gtm_installed"]:
        for ns in soup.find_all("noscript"):
            if "googletagmanager.com" in str(ns):
                base["gtm_installed"] = True
                break

    base["seo_dom_status"] = "ok"
    return base


# ─────────────────────────────────────────
# Check 3: PageSpeed Insights
# ─────────────────────────────────────────
def check_pagespeed(url: str, api_key: str) -> dict:
    """
    Calls Google PageSpeed Insights v5 (mobile strategy).
    Free tier: 25,000 calls/day.
    Returns performance_score (0–100), first_contentful_paint_ms.
    """
    no_key = {"performance_score": None, "page_speed_fcp_ms": None, "pagespeed_status": "no_api_key"}
    if not api_key:
        return no_key

    try:
        resp = requests.get(
            PAGESPEED_URL,
            params={"url": url, "key": api_key, "strategy": "mobile", "category": "performance"},
            timeout=PS_TIMEOUT,
        )
        if resp.status_code != 200:
            return {"performance_score": None, "page_speed_fcp_ms": None, "pagespeed_status": f"api_error_{resp.status_code}"}

        lhr = resp.json().get("lighthouseResult", {})

        raw   = lhr.get("categories", {}).get("performance", {}).get("score")
        score = round(raw * 100) if raw is not None else None

        fcp_raw = lhr.get("audits", {}).get("first-contentful-paint", {}).get("numericValue")
        fcp_ms  = int(fcp_raw) if fcp_raw is not None else None

        return {"performance_score": score, "page_speed_fcp_ms": fcp_ms, "pagespeed_status": "ok"}
    except Exception as e:
        return {"performance_score": None, "page_speed_fcp_ms": None, "pagespeed_status": f"error:{str(e)[:80]}"}


# ─────────────────────────────────────────
# Check 4: GBP claimed status
# ─────────────────────────────────────────
def check_gbp_claimed(maps_url: str) -> dict:
    """
    Fetches the Google Maps listing and scans for "Claim this business" /
    "Own this business?" text. Returns gbp_claimed (bool | None).
    """
    base = {"gbp_claimed": None, "gbp_claimed_status": "unknown"}
    if not maps_url:
        base["gbp_claimed_status"] = "no_maps_url"
        return base

    try:
        resp = requests.get(maps_url, timeout=REQUEST_TIMEOUT, headers=HEADERS)
        text = resp.text.lower()

        if "claim this business" in text or "own this business" in text:
            return {"gbp_claimed": False, "gbp_claimed_status": "unclaimed"}

        if resp.status_code == 200 and len(text) > 5_000:
            # Page loaded with content — claim text absent → likely claimed
            return {"gbp_claimed": True, "gbp_claimed_status": "claimed_inferred"}

        base["gbp_claimed_status"] = "check_failed_js_required"
    except Exception as e:
        base["gbp_claimed_status"] = f"error:{str(e)[:80]}"

    return base


# ─────────────────────────────────────────
# Check 5: Meta Ads
# ─────────────────────────────────────────
def check_meta_ads(business_name: str, country: str = "AU") -> dict:
    """
    Requests-based scan of the Meta Ad Library public search page.
    Returns:
        meta_ads_active  (bool | None)
        meta_ads_count   (int | None)
        meta_ads_status  ("ok_no_ads" | "ok_ads_found" | "check_failed_js_required" |
                          "check_failed_inconclusive" | "timeout" | "error")

    Meta's Ad Library requires JavaScript to fully render. This check catches
    the cases where the server-side HTML is sufficient to determine the result.
    When it can't determine it, it returns "check_failed_js_required" rather
    than crashing or producing a false result.
    """
    base = {"meta_ads_active": None, "meta_ads_count": None, "meta_ads_status": "not_run"}
    if not business_name:
        base["meta_ads_status"] = "no_business_name"
        return base

    try:
        resp = requests.get(
            META_ADS_URL,
            params={
                "active_status": "all",
                "ad_type":       "all",
                "country":       country,
                "q":             business_name,
                "search_type":   "keyword_unordered",
            },
            headers={**HEADERS, "Accept": "text/html,application/xhtml+xml"},
            timeout=META_TIMEOUT,
        )
        if resp.status_code != 200:
            base["meta_ads_status"] = f"http_{resp.status_code}"
            return base

        text = resp.text
        tl   = text.lower()

        # If page is mostly a JS shell we can't parse meaningfully
        if len(text) < 3_000:
            base["meta_ads_status"] = "check_failed_js_required"
            return base

        # Positive: zero results
        if (
            "no ads match" in tl
            or "no results found" in tl
            or '"total_count":0' in text
            or '"resultCount":0' in text
        ):
            return {"meta_ads_active": False, "meta_ads_count": 0, "meta_ads_status": "ok_no_ads"}

        # Positive: ads present
        if (
            "ad_archive_id" in text
            or "page_id" in text
            or "adresult" in tl
        ):
            count = None
            m = re.search(r'"total_count"\s*:\s*(\d+)', text)
            if m:
                count = int(m.group(1))
            return {"meta_ads_active": True, "meta_ads_count": count, "meta_ads_status": "ok_ads_found"}

        base["meta_ads_status"] = "check_failed_js_required"

    except requests.exceptions.Timeout:
        base["meta_ads_status"] = "timeout"
    except Exception as e:
        base["meta_ads_status"] = f"error:{str(e)[:80]}"

    return base


# ─────────────────────────────────────────
# Per-lead audit orchestrator
# ─────────────────────────────────────────
def audit_lead(lead: dict, pagespeed_key: str, country: str = "AU") -> dict:
    website    = normalise_url(lead.get("website", ""))
    maps_url   = lead.get("maps_url", "")
    name       = lead.get("name", "")
    skip_ps    = os.environ.get("SKIP_PAGESPEED", "0") == "1"
    skip_meta  = os.environ.get("SKIP_META_ADS",  "0") == "1"

    # Check 1
    print("  [1/5] Website check", file=sys.stderr)
    w = check_website(website)

    seo = {}
    ps  = {}
    gbp = {}

    if w["has_website"] and website:
        # Check 2
        print("  [2/5] SEO DOM parse", file=sys.stderr)
        seo = check_seo_dom(website)

        # Check 3
        if skip_ps:
            ps = {"performance_score": None, "page_speed_fcp_ms": None, "pagespeed_status": "skipped"}
        else:
            print("  [3/5] PageSpeed Insights", file=sys.stderr)
            ps = check_pagespeed(website, pagespeed_key)
    else:
        seo = {
            "title_present": None, "title_text": None,
            "meta_description_present": None, "meta_description_text": None,
            "viewport_present": None, "analytics_installed": None,
            "gtm_installed": None, "seo_dom_status": "skipped_no_website",
        }
        ps  = {"performance_score": None, "page_speed_fcp_ms": None, "pagespeed_status": "skipped_no_website"}

    # Check 4 — GBP runs for ALL leads regardless of website status
    # (a business without a website can still have an unclaimed GBP)
    print("  [4/5] GBP claim status", file=sys.stderr)
    gbp = check_gbp_claimed(maps_url)

    # Check 5
    if skip_meta:
        meta = {"meta_ads_active": None, "meta_ads_count": None, "meta_ads_status": "skipped"}
    else:
        print("  [5/5] Meta Ads scan", file=sys.stderr)
        meta = check_meta_ads(name, country)

    return {
        **lead,
        "has_website":          w["has_website"],
        "website_status":       w.get("website_status"),
        "seo_results": {
            **seo,
            **ps,
        },
        "ads_results": {
            **meta,
            "google_ads_status": "manual_check_required",
        },
        "gbp_claimed":          gbp.get("gbp_claimed"),
        "gbp_claimed_status":   gbp.get("gbp_claimed_status"),
        "last_audited_at":      datetime.now(timezone.utc).isoformat(),
    }


# ─────────────────────────────────────────
# Batch runner
# ─────────────────────────────────────────
def run_auditor(leads: list, country: str = "AU") -> list:
    pagespeed_key = os.environ.get("PAGESPEED_API_KEY", "").strip()
    audit_delay   = float(os.environ.get("AUDIT_DELAY_S", "1.5"))

    if not pagespeed_key:
        print("[Auditor][WARN] PAGESPEED_API_KEY not set — PageSpeed checks will be skipped.", file=sys.stderr)

    print(f"[Auditor] ─── Starting ─────────────────────────────────────────────────", file=sys.stderr)
    print(f"[Auditor] Leads to audit: {len(leads)}", file=sys.stderr)

    results = []
    for i, lead in enumerate(leads, start=1):
        name = lead.get("name", f"Lead #{i}")
        print(f"[Auditor] [{i}/{len(leads)}] {name}", file=sys.stderr)
        try:
            enriched = audit_lead(lead, pagespeed_key, country)
            results.append(enriched)
        except Exception as e:
            print(f"[Auditor][ERROR] Failed auditing '{name}': {e}", file=sys.stderr)
            lead["audit_error"] = str(e)
            results.append(lead)

        if i < len(leads):
            time.sleep(audit_delay)

    print(f"[Auditor] ─── Done ─────────────────────────────────────────────────────", file=sys.stderr)
    return results


# ─────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="LeadScan AI — Agent 2: Auditor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--input",        help="JSON file of leads from Agent 1 (default: stdin)")
    parser.add_argument("--output",       help="Write enriched JSON to this file (default: stdout)")
    parser.add_argument("--single",       help="Audit a single lead JSON string (for testing)")
    parser.add_argument("--country",      default="AU", help="Country code for Meta Ads search (default: AU)")
    parser.add_argument("--no-meta",      action="store_true", help="Skip Meta Ads check")
    parser.add_argument("--no-pagespeed", action="store_true", help="Skip PageSpeed API check")
    args = parser.parse_args()

    if args.no_meta:
        os.environ["SKIP_META_ADS"] = "1"
    if args.no_pagespeed:
        os.environ["SKIP_PAGESPEED"] = "1"

    if args.single:
        leads = [json.loads(args.single)]
    elif args.input:
        with open(args.input, encoding="utf-8") as f:
            leads = json.load(f)
    else:
        raw = sys.stdin.read().strip()
        if not raw:
            print("[Auditor][ERROR] No input. Use --input, --single, or pipe JSON via stdin.", file=sys.stderr)
            sys.exit(1)
        leads = json.loads(raw)

    if not isinstance(leads, list):
        leads = [leads]

    enriched = run_auditor(leads, country=args.country)
    out_json  = json.dumps(enriched, indent=2, ensure_ascii=False)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(out_json)
        print(f"[Auditor] Written {len(enriched)} audited leads to {args.output}", file=sys.stderr)
    else:
        print(out_json)
