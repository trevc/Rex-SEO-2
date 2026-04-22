#!/home/trev/.local/share/pipx/venvs/analytics-mcp/bin/python
import asyncio
import html
import json
import math
import os
import re
import sys
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import date
from pathlib import Path

from analytics_mcp.tools.reporting.core import run_report

PROPERTY_ID = int(os.environ.get("GA4_PROPERTY_ID", "311312437"))
SITE_ROOT = os.environ.get("SITE_ROOT", "https://omegaforums.net")
OUTDIR = Path(os.environ.get("OUTDIR", "/home/trev/.openclaw/workspace/reports"))
OUTDIR.mkdir(parents=True, exist_ok=True)
TODAY = date.today().isoformat()
JSON_PATH = OUTDIR / f"seo-thread-opportunities-{TODAY}.json"
MD_PATH = OUTDIR / f"seo-thread-opportunities-{TODAY}.md"
CSV_PATH = OUTDIR / f"seo-thread-opportunities-{TODAY}.csv"

DATE_RANGES = {
    "90d": {"start_date": "90daysAgo", "end_date": "yesterday"},
    "28d": {"start_date": "28daysAgo", "end_date": "yesterday"},
}

DIMENSION_FILTER = {
    "filter": {
        "field_name": "landingPagePlusQueryString",
        "string_filter": {"match_type": "CONTAINS", "value": "/threads/"},
    }
}

ENGAGEMENT_METRICS = [
    "sessions",
    "engagedSessions",
    "activeUsers",
    "screenPageViews",
    "averageSessionDuration",
    "bounceRate",
]

SEARCH_METRICS = [
    "organicGoogleSearchClicks",
    "organicGoogleSearchImpressions",
    "organicGoogleSearchClickThroughRate",
    "organicGoogleSearchAveragePosition",
]


def to_number(value):
    if value in (None, "", "(not set)"):
        return 0.0
    try:
        return float(value)
    except Exception:
        return 0.0


def clamp(value, low=0.0, high=1.0):
    return max(low, min(high, value))


def normalize_thread_path(path: str | None) -> str | None:
    if not path:
        return None
    if "://" in path:
        parsed = urllib.parse.urlsplit(path)
        path = parsed.path
    else:
        path = urllib.parse.urlsplit(f"https://dummy{path}").path
    if not path.startswith("/threads/"):
        return None
    path = re.sub(r"/page-\d+$", "", path.rstrip("/"))
    return path + "/"


def expected_ctr(position: float) -> float:
    if position <= 0:
        return 0.01
    if position <= 3:
        return 0.08
    if position <= 5:
        return 0.05
    if position <= 8:
        return 0.03
    if position <= 12:
        return 0.02
    if position <= 20:
        return 0.01
    if position <= 30:
        return 0.005
    return 0.003


def position_factor(position: float) -> float:
    if position <= 0:
        return 0.2
    if 4 <= position <= 15:
        return 1.0
    if 15 < position <= 25:
        return 0.75
    if position < 4:
        return 0.55
    if 25 < position <= 40:
        return 0.35
    return 0.15


def slug_to_title(path: str) -> str:
    bits = path.strip("/").split("/")
    if len(bits) < 2:
        return path
    slug = bits[1]
    slug = re.sub(r"\.\d+$", "", slug)
    slug = slug.replace("-", " ")
    return slug.title()


def fetch_title(path: str) -> str:
    url = urllib.parse.urljoin(SITE_ROOT, path)
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/123 Safari/537.36"
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            body = resp.read(250000).decode("utf-8", errors="replace")
        match = re.search(r"<title>(.*?)</title>", body, flags=re.I | re.S)
        if not match:
            return slug_to_title(path)
        title = html.unescape(re.sub(r"\s+", " ", match.group(1))).strip()
        title = re.sub(r"\s*\|\s*Omega Forums\s*$", "", title, flags=re.I)
        return title or slug_to_title(path)
    except Exception:
        return slug_to_title(path)


def heuristic_type(title: str, path: str) -> str:
    text = f"{title} {path}".lower()
    if any(k in text for k in ["guide", "history", "explained", "review", "serial number", "serial numbers", "difference between", "worth it", "too big", "pictorial", "special dials"]):
        return "evergreen"
    if any(k in text for k in ["for sale", "price", "reduced", "shipping", "sold", "serviced", "euro", "bracelet", "dial and hands"]):
        return "listing-like"
    return "mixed"


def opportunity_bucket(d: dict) -> str:
    pos = d.get("avgPosition90", 0.0)
    ctr = d.get("ctr90", 0.0)
    imp = d.get("organicImpressions90", 0.0)
    ctr_gap = d.get("ctrGap90", 0.0)
    if imp >= 3000 and pos <= 12 and ctr_gap >= 0.45:
        return "CTR fix"
    if imp >= 1000 and 8 < pos <= 20:
        return "Near-page-1 fix"
    if d.get("engagementScore", 0.0) >= 70 and imp >= 300:
        return "Hidden gem"
    return "Monitor"


def recommendation(d: dict) -> str:
    bucket = d["bucket"]
    if bucket == "CTR fix":
        return "Rewrite title for search intent, tighten the intro, and add FAQ-style subheads to win more clicks at the current rank."
    if bucket == "Near-page-1 fix":
        return "Expand the intro, add stronger query-matching headings, and push internal links from forum hubs and related evergreen threads."
    if bucket == "Hidden gem":
        return "Treat as a sleeper winner: improve title clarity, add an SEO summary up top, and link it harder from related threads."
    return "Keep watching; lower priority than the main shortlist."


async def fetch_report(date_range: dict, metrics: list[str], offset: int = 0, limit: int = 10000):
    return await run_report(
        property_id=PROPERTY_ID,
        date_ranges=[date_range],
        dimensions=["landingPagePlusQueryString"],
        metrics=metrics,
        dimension_filter=DIMENSION_FILTER,
        order_bys=[{"dimension": {"dimension_name": "landingPagePlusQueryString"}}],
        limit=limit,
        offset=offset,
    )


async def fetch_all_rows(label: str, metrics: list[str]) -> list[dict]:
    date_range = DATE_RANGES[label]
    offset = 0
    limit = 10000
    rows_out = []
    total = None
    while True:
        report = await fetch_report(date_range, metrics=metrics, offset=offset, limit=limit)
        if total is None:
            total = int(report.get("row_count", 0))
        dim_headers = [d["name"] for d in report.get("dimension_headers", [])]
        metric_headers = [m["name"] for m in report.get("metric_headers", [])]
        rows = report.get("rows", [])
        for row in rows:
            item = {}
            for i, h in enumerate(dim_headers):
                item[h] = row.get("dimension_values", [])[i].get("value", "") if i < len(row.get("dimension_values", [])) else ""
            for i, h in enumerate(metric_headers):
                item[h] = row.get("metric_values", [])[i].get("value", "0") if i < len(row.get("metric_values", [])) else "0"
            rows_out.append(item)
        offset += len(rows)
        if not rows or offset >= total:
            break
    return rows_out


def merge_raw_rows(*row_sets: list[dict]) -> list[dict]:
    merged = {}
    for row_set in row_sets:
        for row in row_set:
            key = row.get("landingPagePlusQueryString", "")
            if key not in merged:
                merged[key] = {"landingPagePlusQueryString": key}
            merged[key].update(row)
    return list(merged.values())


def aggregate_rows(rows: list[dict]) -> dict[str, dict]:
    agg: dict[str, dict] = defaultdict(lambda: {
        "rawPaths": set(),
        "sessions": 0.0,
        "engagedSessions": 0.0,
        "activeUsers": 0.0,
        "screenPageViews": 0.0,
        "organicGoogleSearchClicks": 0.0,
        "organicGoogleSearchImpressions": 0.0,
        "_duration_weight": 0.0,
        "_duration_sum": 0.0,
        "_bounce_weight": 0.0,
        "_bounce_sum": 0.0,
        "_position_weight": 0.0,
        "_position_sum": 0.0,
    })

    for row in rows:
        raw_path = row.get("landingPagePlusQueryString", "")
        path = normalize_thread_path(raw_path)
        if not path:
            continue
        a = agg[path]
        sessions = to_number(row.get("sessions"))
        impressions = to_number(row.get("organicGoogleSearchImpressions"))
        avg_duration = to_number(row.get("averageSessionDuration"))
        bounce_rate = to_number(row.get("bounceRate"))
        avg_position = to_number(row.get("organicGoogleSearchAveragePosition"))

        a["rawPaths"].add(raw_path)
        a["sessions"] += sessions
        a["engagedSessions"] += to_number(row.get("engagedSessions"))
        a["activeUsers"] += to_number(row.get("activeUsers"))
        a["screenPageViews"] += to_number(row.get("screenPageViews"))
        a["organicGoogleSearchClicks"] += to_number(row.get("organicGoogleSearchClicks"))
        a["organicGoogleSearchImpressions"] += impressions
        a["_duration_sum"] += avg_duration * sessions
        a["_duration_weight"] += sessions
        a["_bounce_sum"] += bounce_rate * sessions
        a["_bounce_weight"] += sessions
        a["_position_sum"] += avg_position * impressions
        a["_position_weight"] += impressions

    out = {}
    for path, a in agg.items():
        sessions = a["sessions"]
        impressions = a["organicGoogleSearchImpressions"]
        out[path] = {
            "path": path,
            "rawPathVariants": sorted(a["rawPaths"]),
            "variantCount": len(a["rawPaths"]),
            "sessions": sessions,
            "engagedSessions": a["engagedSessions"],
            "activeUsers": a["activeUsers"],
            "screenPageViews": a["screenPageViews"],
            "averageSessionDuration": (a["_duration_sum"] / a["_duration_weight"]) if a["_duration_weight"] else 0.0,
            "bounceRate": (a["_bounce_sum"] / a["_bounce_weight"]) if a["_bounce_weight"] else 0.0,
            "organicGoogleSearchClicks": a["organicGoogleSearchClicks"],
            "organicGoogleSearchImpressions": impressions,
            "organicGoogleSearchClickThroughRate": (a["organicGoogleSearchClicks"] / impressions) if impressions else 0.0,
            "organicGoogleSearchAveragePosition": (a["_position_sum"] / a["_position_weight"]) if a["_position_weight"] else 0.0,
        }
    return out


def build_scored_dataset(data90: dict[str, dict], data28: dict[str, dict]) -> list[dict]:
    paths = sorted(set(data90) | set(data28))
    out = []
    for path in paths:
        d90 = data90.get(path, {})
        d28 = data28.get(path, {})
        sessions90 = d90.get("sessions", 0.0)
        engaged90 = d90.get("engagedSessions", 0.0)
        pv90 = d90.get("screenPageViews", 0.0)
        imp90 = d90.get("organicGoogleSearchImpressions", 0.0)
        clicks90 = d90.get("organicGoogleSearchClicks", 0.0)
        ctr90 = d90.get("organicGoogleSearchClickThroughRate", 0.0)
        pos90 = d90.get("organicGoogleSearchAveragePosition", 0.0)
        dur90 = d90.get("averageSessionDuration", 0.0)
        bounce90 = d90.get("bounceRate", 0.0)

        if sessions90 < 20 or engaged90 < 10 or imp90 < 200:
            continue

        engagement_rate = engaged90 / sessions90 if sessions90 else 0.0
        views_per_session = pv90 / sessions90 if sessions90 else 0.0
        expected = expected_ctr(pos90)
        ctr_gap = max(expected - ctr90, 0.0) / expected if expected else 0.0

        eng_score = 100 * (
            0.45 * clamp(engagement_rate / 0.68) +
            0.35 * clamp(dur90 / 300.0) +
            0.20 * clamp(views_per_session / 2.5)
        )
        neglect_score = 100 * (
            0.50 * clamp(ctr_gap) +
            0.25 * clamp(position_factor(pos90)) +
            0.25 * clamp(math.log1p(imp90) / math.log(20000))
        )
        revenue_score = 100 * (
            0.70 * clamp(math.log1p(pv90) / math.log(15000)) +
            0.30 * clamp(math.log1p(sessions90) / math.log(5000))
        )
        freshness_score = 100 * clamp(math.log1p(d28.get("screenPageViews", 0.0)) / math.log(5000))
        final_score = (0.40 * eng_score) + (0.35 * neglect_score) + (0.20 * revenue_score) + (0.05 * freshness_score)

        row = {
            "path": path,
            "url": urllib.parse.urljoin(SITE_ROOT, path),
            "sessions90": sessions90,
            "engagedSessions90": engaged90,
            "engagementRate90": engagement_rate,
            "screenPageViews90": pv90,
            "viewsPerSession90": views_per_session,
            "avgDuration90": dur90,
            "bounceRate90": bounce90,
            "organicClicks90": clicks90,
            "organicImpressions90": imp90,
            "ctr90": ctr90,
            "avgPosition90": pos90,
            "sessions28": d28.get("sessions", 0.0),
            "engagedSessions28": d28.get("engagedSessions", 0.0),
            "screenPageViews28": d28.get("screenPageViews", 0.0),
            "organicClicks28": d28.get("organicGoogleSearchClicks", 0.0),
            "organicImpressions28": d28.get("organicGoogleSearchImpressions", 0.0),
            "ctr28": d28.get("organicGoogleSearchClickThroughRate", 0.0),
            "avgPosition28": d28.get("organicGoogleSearchAveragePosition", 0.0),
            "variantCount": d90.get("variantCount", 0),
            "engagementScore": eng_score,
            "searchNeglectScore": neglect_score,
            "revenueScore": revenue_score,
            "freshnessScore": freshness_score,
            "ctrGap90": ctr_gap,
            "expectedCtr90": expected,
            "finalOpportunityScore": final_score,
        }
        row["bucket"] = opportunity_bucket(row)
        out.append(row)
    out.sort(key=lambda x: x["finalOpportunityScore"], reverse=True)
    return out


def attach_titles(rows: list[dict], top_n: int = 30):
    for row in rows[:top_n]:
        title = fetch_title(row["path"])
        row["title"] = title
        row["threadType"] = heuristic_type(title, row["path"])
        row["recommendation"] = recommendation(row)
    for row in rows[top_n:]:
        row["title"] = slug_to_title(row["path"])
        row["threadType"] = heuristic_type(row["title"], row["path"])
        row["recommendation"] = recommendation(row)


def fmt_pct(value: float) -> str:
    return f"{value * 100:.1f}%"


def fmt_num(value: float) -> str:
    if value >= 1000:
        return f"{value:,.0f}"
    if value == int(value):
        return str(int(value))
    return f"{value:.1f}"


def make_markdown(rows: list[dict], stats: dict) -> str:
    top = rows[:12]
    evergreen = [r for r in rows if r.get("threadType") == "evergreen"][:8]
    ctr_fixes = [r for r in rows if r.get("bucket") == "CTR fix"][:8]
    near_page_one = [r for r in rows if r.get("bucket") == "Near-page-1 fix"][:8]

    def bullet_row(r: dict) -> str:
        return (
            f"- **[{r['title']}]({r['url']})** — score `{r['finalOpportunityScore']:.1f}` · {r['bucket']}\n"
            f"  - 90d: {fmt_num(r['sessions90'])} sessions, {fmt_pct(r['engagementRate90'])} engagement rate, {fmt_num(r['screenPageViews90'])} page views\n"
            f"  - Search: {fmt_num(r['organicImpressions90'])} impressions, {fmt_num(r['organicClicks90'])} clicks, {fmt_pct(r['ctr90'])} CTR, avg position {r['avgPosition90']:.1f}\n"
            f"  - Fix: {r['recommendation']}"
        )

    lines = []
    lines.append(f"# Omega Forums SEO Thread Opportunity Report\n")
    lines.append(f"Generated: {TODAY}\n")
    lines.append("## Mission\n")
    lines.append("Find thread pages with strong on-site engagement but weak Google organic performance, then prioritise title/content work that should lift search clicks and ad revenue.\n")
    lines.append("## Data used\n")
    lines.append("- Source: GA4 via analytics-mcp\n- Search metrics: linked Search Console data exposed in GA4\n- Windows: last 90 days and last 28 days\n- Scope: landing pages containing `/threads/`, collapsed to canonical thread root URLs\n")
    lines.append("## Harvest summary\n")
    lines.append(
        f"- Raw 90d thread landing rows: **{stats['raw90']}**\n"
        f"- Raw 28d thread landing rows: **{stats['raw28']}**\n"
        f"- Canonical thread URLs after collapsing paginated variants: **{stats['canon90']}**\n"
        f"- Candidate URLs after signal thresholds: **{stats['candidates']}**\n"
    )
    lines.append("## How scoring works\n")
    lines.append("- **Engagement score**: engagement rate + average session duration + views/session\n- **Search neglect score**: impression volume + CTR gap vs rough position benchmark + current ranking window\n- **Revenue score**: page view and session scale\n- **Final opportunity score**: weighted blend of engagement, neglect, revenue, and freshness\n")
    lines.append("## Top opportunities\n")
    for r in top:
        lines.append(bullet_row(r) + "\n")
    lines.append("## Best evergreen bets\n")
    for r in evergreen:
        lines.append(bullet_row(r) + "\n")
    lines.append("## Best CTR-fix bets\n")
    for r in ctr_fixes:
        lines.append(bullet_row(r) + "\n")
    lines.append("## Best near-page-1 bets\n")
    for r in near_page_one:
        lines.append(bullet_row(r) + "\n")
    lines.append("## Immediate execution plan\n")
    lines.append(
        "1. Rewrite titles on the CTR-fix set first.\n"
        "2. Add a tight SEO summary/introduction block at the top of each target thread.\n"
        "3. Add query-matching subheads/FAQ blocks where natural.\n"
        "4. Add internal links from high-traffic hubs such as the homepage, forum indexes, and related reference threads.\n"
        "5. Re-check 28d organic clicks / CTR after edits land.\n"
    )
    lines.append("## Notes / caveats\n")
    lines.append(
        "- This is page-level organic data from GA4/Search Console integration, not query-level Search Console.\n"
        "- Scores are prioritisation heuristics, not truth from the gods.\n"
        "- Some listing-like threads may score well but be poor long-term SEO bets; favour evergreen threads first.\n"
    )
    return "\n".join(lines)


def make_csv(rows: list[dict]) -> str:
    cols = [
        "finalOpportunityScore",
        "bucket",
        "threadType",
        "title",
        "url",
        "sessions90",
        "engagedSessions90",
        "engagementRate90",
        "screenPageViews90",
        "avgDuration90",
        "bounceRate90",
        "organicImpressions90",
        "organicClicks90",
        "ctr90",
        "avgPosition90",
        "sessions28",
        "screenPageViews28",
        "organicImpressions28",
        "organicClicks28",
        "ctr28",
        "avgPosition28",
        "recommendation",
    ]
    def esc(v):
        s = str(v)
        if any(ch in s for ch in [',', '"', '\n']):
            s = '"' + s.replace('"', '""') + '"'
        return s
    lines = [",".join(cols)]
    for r in rows:
        lines.append(",".join(esc(r.get(c, "")) for c in cols))
    return "\n".join(lines) + "\n"


async def main():
    rows90_eng = await fetch_all_rows("90d", ENGAGEMENT_METRICS)
    rows90_search = await fetch_all_rows("90d", SEARCH_METRICS)
    rows28_eng = await fetch_all_rows("28d", ENGAGEMENT_METRICS)
    rows28_search = await fetch_all_rows("28d", SEARCH_METRICS)

    rows90 = merge_raw_rows(rows90_eng, rows90_search)
    rows28 = merge_raw_rows(rows28_eng, rows28_search)

    data90 = aggregate_rows(rows90)
    data28 = aggregate_rows(rows28)
    scored = build_scored_dataset(data90, data28)
    attach_titles(scored, top_n=35)

    stats = {
        "raw90": len(rows90),
        "raw28": len(rows28),
        "canon90": len(data90),
        "canon28": len(data28),
        "candidates": len(scored),
    }

    payload = {
        "generated": TODAY,
        "property_id": PROPERTY_ID,
        "site_root": SITE_ROOT,
        "stats": stats,
        "top_opportunities": scored[:25],
        "all_candidates": scored,
    }
    JSON_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    MD_PATH.write_text(make_markdown(scored, stats), encoding="utf-8")
    CSV_PATH.write_text(make_csv(scored[:250]), encoding="utf-8")

    summary = {
        "json": str(JSON_PATH),
        "markdown": str(MD_PATH),
        "csv": str(CSV_PATH),
        "stats": stats,
        "top": scored[:12],
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    asyncio.run(main())

