#!/home/trev/.local/share/pipx/venvs/analytics-mcp/bin/python
import asyncio
import html
import json
import math
import os
import re
import statistics
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date
from html.parser import HTMLParser
from pathlib import Path

from analytics_mcp.tools.reporting.core import run_report

PROPERTY_ID = int(os.environ.get("GA4_PROPERTY_ID", "311312437"))
SITE_ROOT = os.environ.get("SITE_ROOT", "https://omegaforums.net")
OUTDIR = Path(os.environ.get("OUTDIR", "/home/trev/.openclaw/workspace/reports"))
OUTDIR.mkdir(parents=True, exist_ok=True)
TODAY = date.today().isoformat()
MD_PATH = OUTDIR / f"seo-best-performers-{TODAY}.md"
JSON_PATH = OUTDIR / f"seo-best-performers-{TODAY}.json"

DATE_RANGES = {
    "180d": {"start_date": "180daysAgo", "end_date": "yesterday"},
    "30d": {"start_date": "30daysAgo", "end_date": "yesterday"},
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

THREAD_FILTER = {
    "filter": {
        "field_name": "landingPagePlusQueryString",
        "string_filter": {"match_type": "CONTAINS", "value": "/threads/"},
    }
}

FORUM_FILTER = {
    "filter": {
        "field_name": "landingPagePlusQueryString",
        "string_filter": {"match_type": "CONTAINS", "value": "/forums/"},
    }
}

STOPWORDS = {
    "the", "and", "for", "that", "with", "this", "from", "your", "you", "are", "but", "was", "have", "has", "had", "its",
    "about", "into", "onto", "only", "than", "then", "they", "them", "their", "there", "here", "what", "when", "where", "which",
    "will", "would", "could", "should", "can", "does", "doing", "done", "just", "like", "more", "most", "some", "many", "over",
    "under", "very", "been", "being", "because", "also", "after", "before", "while", "through", "using", "used", "user", "users",
    "watch", "watches", "omega", "forum", "forums", "thread", "threads", "help", "show", "tell", "new", "first", "one", "two",
    "three", "review", "guide", "full", "mini", "year", "years", "today", "best", "maybe", "good", "great", "question", "questions",
    "work", "works", "working", "worth", "want", "need", "time", "times", "anyone", "please", "thanks", "thank",
}

TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9\-]{1,}")


class XFPageParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.title_parts = []
        self.capture_title = False
        self.title_depth = 0
        self.breadcrumbs = []
        self.capture_breadcrumb = False
        self.breadcrumb_buf = []
        self.wrapper_parts = []
        self.capture_wrapper = False
        self.wrapper_depth = 0
        self.got_first_wrapper = False
        self.in_h2 = False
        self.h2_buf = []
        self.headings = []
        self.in_h3 = False
        self.h3_buf = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        cls = attrs.get("class", "")

        if tag == "h1" and "p-title-value" in cls and not self.title_parts:
            self.capture_title = True
            self.title_depth = 1
        elif self.capture_title:
            self.title_depth += 1

        if tag == "span" and attrs.get("itemprop") == "name":
            self.capture_breadcrumb = True
            self.breadcrumb_buf = []

        if tag == "div" and (not self.got_first_wrapper) and "bbWrapper" in cls:
            self.capture_wrapper = True
            self.wrapper_depth = 1
        elif self.capture_wrapper:
            self.wrapper_depth += 1

        if self.capture_wrapper and tag == "br":
            self.wrapper_parts.append("\n")

        if tag == "h2":
            self.in_h2 = True
            self.h2_buf = []
        if tag == "h3":
            self.in_h3 = True
            self.h3_buf = []

    def handle_endtag(self, tag):
        if self.capture_title:
            self.title_depth -= 1
            if self.title_depth == 0:
                self.capture_title = False

        if self.capture_breadcrumb and tag == "span":
            txt = " ".join("".join(self.breadcrumb_buf).split())
            if txt:
                self.breadcrumbs.append(txt)
            self.capture_breadcrumb = False

        if self.capture_wrapper:
            self.wrapper_depth -= 1
            if self.wrapper_depth == 0:
                self.capture_wrapper = False
                self.got_first_wrapper = True

        if tag == "h2" and self.in_h2:
            txt = " ".join("".join(self.h2_buf).split())
            if txt:
                self.headings.append(txt)
            self.in_h2 = False
        if tag == "h3" and self.in_h3:
            txt = " ".join("".join(self.h3_buf).split())
            if txt:
                self.headings.append(txt)
            self.in_h3 = False

    def handle_data(self, data):
        if self.capture_title:
            self.title_parts.append(data)
        if self.capture_breadcrumb:
            self.breadcrumb_buf.append(data)
        if self.capture_wrapper:
            self.wrapper_parts.append(data)
        if self.in_h2:
            self.h2_buf.append(data)
        if self.in_h3:
            self.h3_buf.append(data)


@dataclass
class ScrapeData:
    title: str
    breadcrumbs: list
    first_post_text: str
    first_post_words: int
    title_words: int
    title_chars: int
    headings: list
    heading_count: int
    tag_count: int
    page_count: int
    meta_description: str
    thread_kind: str


def clamp(v, low=0.0, high=1.0):
    return max(low, min(high, v))


def to_num(v):
    if v in (None, "", "(not set)"):
        return 0.0
    try:
        return float(v)
    except Exception:
        return 0.0


def normalize_path(path: str, kind: str):
    if not path:
        return None
    parsed = urllib.parse.urlsplit(path if "://" in path else f"https://dummy{path}")
    path = parsed.path
    prefix = "/threads/" if kind == "thread" else "/forums/"
    if not path.startswith(prefix):
        return None
    path = re.sub(r"/page-\d+$", "", path.rstrip("/"))
    return path + "/"


def base_for_pages(path: str):
    return path.rstrip("/")


def fetch_html(path: str):
    url = urllib.parse.urljoin(SITE_ROOT, path)
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/123 Safari/537.36"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8", errors="replace")


def classify_title(title: str):
    t = title.lower()
    if any(x in t for x in ["guide", "review", "history", "explained", "full guide", "pictorial", "serial", "reference", "mini-review"]):
        return "guide/reference"
    if "?" in t or any(x in t for x in ["how ", "how does", "what ", "why ", "is ", "can ", "should ", "vs.", " vs ", "worth it"]):
        return "question/problem"
    if any(x in t for x in ["show your", "show and tell", "wruw"]):
        return "showcase/gallery"
    if any(x in t for x in ["for sale", "reduced", "shipping", "bracelet", "dial and hands", "serviced"]):
        return "listing-like"
    return "discussion/reference"


def scrape_page(path: str):
    html_text = fetch_html(path)
    parser = XFPageParser()
    parser.feed(html_text)
    title = " ".join("".join(parser.title_parts).split())
    if not title:
        m = re.search(r"<title>(.*?)</title>", html_text, flags=re.I | re.S)
        title = html.unescape(" ".join(m.group(1).split())) if m else path
        title = re.sub(r"\s*\|\s*Omega.*$", "", title, flags=re.I)
    body = " ".join("".join(parser.wrapper_parts).split())
    meta = ""
    mm = re.search(r'<meta[^>]+name=["\']description["\'][^>]+content=["\'](.*?)["\']', html_text, flags=re.I | re.S)
    if mm:
        meta = html.unescape(" ".join(mm.group(1).split()))
    tag_count = len(set(re.findall(r'href=["\'](/tags/[^"\']+)', html_text, flags=re.I)))
    nums = [int(x) for x in re.findall(re.escape(base_for_pages(path)) + r'/page-(\d+)', html_text)]
    page_count = max(nums) if nums else 1
    return ScrapeData(
        title=title,
        breadcrumbs=parser.breadcrumbs,
        first_post_text=body,
        first_post_words=len(body.split()),
        title_words=len(title.split()),
        title_chars=len(title),
        headings=parser.headings,
        heading_count=len(parser.headings),
        tag_count=tag_count,
        page_count=page_count,
        meta_description=meta,
        thread_kind=classify_title(title),
    )


async def fetch_report(date_range, dimension_filter, metrics, offset=0, limit=10000):
    return await run_report(
        property_id=PROPERTY_ID,
        date_ranges=[date_range],
        dimensions=["landingPagePlusQueryString"],
        metrics=metrics,
        dimension_filter=dimension_filter,
        order_bys=[{"dimension": {"dimension_name": "landingPagePlusQueryString"}}],
        limit=limit,
        offset=offset,
    )


async def fetch_all_rows(date_label, dimension_filter, metrics):
    rows_out = []
    offset = 0
    limit = 10000
    total = None
    while True:
        report = await fetch_report(DATE_RANGES[date_label], dimension_filter, metrics, offset=offset, limit=limit)
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


def merge_raw_rows(*row_sets):
    merged = {}
    for row_set in row_sets:
        for row in row_set:
            key = row.get("landingPagePlusQueryString", "")
            if key not in merged:
                merged[key] = {"landingPagePlusQueryString": key}
            merged[key].update(row)
    return list(merged.values())


def aggregate_rows(rows, kind):
    agg = defaultdict(lambda: {
        "rawPaths": set(),
        "sessions": 0.0,
        "engagedSessions": 0.0,
        "activeUsers": 0.0,
        "screenPageViews": 0.0,
        "organicGoogleSearchClicks": 0.0,
        "organicGoogleSearchImpressions": 0.0,
        "_duration_sum": 0.0,
        "_duration_weight": 0.0,
        "_bounce_sum": 0.0,
        "_bounce_weight": 0.0,
        "_position_sum": 0.0,
        "_position_weight": 0.0,
    })
    for row in rows:
        raw = row.get("landingPagePlusQueryString", "")
        path = normalize_path(raw, kind)
        if not path:
            continue
        a = agg[path]
        sessions = to_num(row.get("sessions"))
        impressions = to_num(row.get("organicGoogleSearchImpressions"))
        duration = to_num(row.get("averageSessionDuration"))
        bounce = to_num(row.get("bounceRate"))
        position = to_num(row.get("organicGoogleSearchAveragePosition"))
        a["rawPaths"].add(raw)
        a["sessions"] += sessions
        a["engagedSessions"] += to_num(row.get("engagedSessions"))
        a["activeUsers"] += to_num(row.get("activeUsers"))
        a["screenPageViews"] += to_num(row.get("screenPageViews"))
        a["organicGoogleSearchClicks"] += to_num(row.get("organicGoogleSearchClicks"))
        a["organicGoogleSearchImpressions"] += impressions
        a["_duration_sum"] += duration * sessions
        a["_duration_weight"] += sessions
        a["_bounce_sum"] += bounce * sessions
        a["_bounce_weight"] += sessions
        a["_position_sum"] += position * impressions
        a["_position_weight"] += impressions
    out = {}
    for path, a in agg.items():
        impressions = a["organicGoogleSearchImpressions"]
        clicks = a["organicGoogleSearchClicks"]
        out[path] = {
            "path": path,
            "rawPathVariants": sorted(a["rawPaths"]),
            "variantCount": len(a["rawPaths"]),
            "sessions": a["sessions"],
            "engagedSessions": a["engagedSessions"],
            "activeUsers": a["activeUsers"],
            "screenPageViews": a["screenPageViews"],
            "averageSessionDuration": (a["_duration_sum"] / a["_duration_weight"]) if a["_duration_weight"] else 0.0,
            "bounceRate": (a["_bounce_sum"] / a["_bounce_weight"]) if a["_bounce_weight"] else 0.0,
            "organicGoogleSearchClicks": clicks,
            "organicGoogleSearchImpressions": impressions,
            "organicGoogleSearchClickThroughRate": (clicks / impressions) if impressions else 0.0,
            "organicGoogleSearchAveragePosition": (a["_position_sum"] / a["_position_weight"]) if a["_position_weight"] else 0.0,
        }
    return out


def build_dataset(records_long, records_recent, kind):
    paths = sorted(set(records_long) | set(records_recent))
    rows = []
    max_clicks = max((records_long[p].get("organicGoogleSearchClicks", 0.0) for p in paths), default=1.0) or 1.0
    max_impressions = max((records_long[p].get("organicGoogleSearchImpressions", 0.0) for p in paths), default=1.0) or 1.0
    max_sessions = max((records_long[p].get("sessions", 0.0) for p in paths), default=1.0) or 1.0
    max_pageviews = max((records_long[p].get("screenPageViews", 0.0) for p in paths), default=1.0) or 1.0
    max_recent_clicks = max((records_recent.get(p, {}).get("organicGoogleSearchClicks", 0.0) for p in paths), default=1.0) or 1.0
    max_recent_pageviews = max((records_recent.get(p, {}).get("screenPageViews", 0.0) for p in paths), default=1.0) or 1.0
    max_duration = max((records_long[p].get("averageSessionDuration", 0.0) for p in paths), default=1.0) or 1.0
    max_views_per_session = max(((records_long[p].get("screenPageViews", 0.0) / max(records_long[p].get("sessions", 0.0), 1.0)) for p in paths), default=1.0) or 1.0

    for p in paths:
        long = records_long.get(p, {})
        recent = records_recent.get(p, {})
        sessions = long.get("sessions", 0.0)
        engaged = long.get("engagedSessions", 0.0)
        impressions = long.get("organicGoogleSearchImpressions", 0.0)
        clicks = long.get("organicGoogleSearchClicks", 0.0)
        if sessions < 15 or impressions < 100:
            continue
        engagement_rate = engaged / sessions if sessions else 0.0
        views_per_session = long.get("screenPageViews", 0.0) / sessions if sessions else 0.0
        ctr = long.get("organicGoogleSearchClickThroughRate", 0.0)
        pos = long.get("organicGoogleSearchAveragePosition", 0.0)
        position_score = 1.0 if 0 < pos <= 3 else 0.9 if pos <= 5 else 0.8 if pos <= 10 else 0.55 if pos <= 20 else 0.25
        traffic_score = 100 * (
            0.45 * (math.log1p(clicks) / math.log1p(max_clicks)) +
            0.30 * (math.log1p(sessions) / math.log1p(max_sessions)) +
            0.25 * (math.log1p(long.get("screenPageViews", 0.0)) / math.log1p(max_pageviews))
        )
        quality_score = 100 * (
            0.35 * clamp(engagement_rate / 0.8) +
            0.25 * clamp(long.get("averageSessionDuration", 0.0) / max_duration) +
            0.20 * clamp(views_per_session / max_views_per_session) +
            0.20 * clamp(1.0 - long.get("bounceRate", 0.0))
        )
        search_score = 100 * (
            0.40 * (math.log1p(impressions) / math.log1p(max_impressions)) +
            0.35 * clamp(ctr / 0.08) +
            0.25 * position_score
        )
        freshness_score = 100 * (
            0.50 * (math.log1p(recent.get("organicGoogleSearchClicks", 0.0)) / math.log1p(max_recent_clicks)) +
            0.50 * (math.log1p(recent.get("screenPageViews", 0.0)) / math.log1p(max_recent_pageviews))
        )
        perf = 0.35 * traffic_score + 0.25 * quality_score + 0.25 * search_score + 0.15 * freshness_score
        rows.append({
            "kind": kind,
            "path": p,
            "url": urllib.parse.urljoin(SITE_ROOT, p),
            "variantCount": long.get("variantCount", 0),
            "sessions180": sessions,
            "engagedSessions180": engaged,
            "engagementRate180": engagement_rate,
            "activeUsers180": long.get("activeUsers", 0.0),
            "screenPageViews180": long.get("screenPageViews", 0.0),
            "viewsPerSession180": views_per_session,
            "avgDuration180": long.get("averageSessionDuration", 0.0),
            "bounceRate180": long.get("bounceRate", 0.0),
            "organicClicks180": clicks,
            "organicImpressions180": impressions,
            "ctr180": ctr,
            "avgPosition180": pos,
            "sessions30": recent.get("sessions", 0.0),
            "screenPageViews30": recent.get("screenPageViews", 0.0),
            "organicClicks30": recent.get("organicGoogleSearchClicks", 0.0),
            "organicImpressions30": recent.get("organicGoogleSearchImpressions", 0.0),
            "ctr30": recent.get("organicGoogleSearchClickThroughRate", 0.0),
            "avgPosition30": recent.get("organicGoogleSearchAveragePosition", 0.0),
            "trafficScore": traffic_score,
            "qualityScore": quality_score,
            "searchScore": search_score,
            "freshnessScore": freshness_score,
            "performanceScore": perf,
        })
    rows.sort(key=lambda x: x["performanceScore"], reverse=True)
    return rows


def tokenise(text):
    toks = []
    for tok in TOKEN_RE.findall(text.lower()):
        if tok in STOPWORDS:
            continue
        if tok.isdigit() and len(tok) < 3:
            continue
        toks.append(tok)
    return toks


def top_terms(rows, key, n=12):
    c = Counter()
    for r in rows:
        c.update(tokenise(r.get(key, "")))
    return c.most_common(n)


def top_bigrams(rows, key, n=12):
    c = Counter()
    for r in rows:
        toks = tokenise(r.get(key, ""))
        for a, b in zip(toks, toks[1:]):
            c[f"{a} {b}"] += 1
    return c.most_common(n)


def fmt_num(v):
    if isinstance(v, int):
        return f"{v:,}"
    if abs(v - round(v)) < 1e-9:
        return f"{int(round(v)):,}"
    return f"{v:,.1f}"


def fmt_pct(v):
    return f"{v * 100:.1f}%"


def table(headers, rows):
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]
    for row in rows:
        out.append("| " + " | ".join(row) + " |")
    return "\n".join(out)


def render_markdown(thread_rows, forum_rows, insights, stats):
    top_threads = thread_rows[:25]
    top_forums = forum_rows[:12]
    lines = []
    lines.append("# Omega Forums GA4/GSC Best Performers Report\n")
    lines.append(f"Generated: {TODAY}\n")
    lines.append("## Scope\n")
    lines.append("- Data windows: last 180 days + last 30 days freshness check\n- Sources: GA4 engagement metrics + linked Search Console metrics in GA4\n- Page groups: thread landing pages (`/threads/`) and forum landing pages (`/forums/`)\n")
    lines.append("## Harvest stats\n")
    lines.append(
        f"- Thread raw rows 180d: **{stats['thread_raw_180']}**\n"
        f"- Thread raw rows 30d: **{stats['thread_raw_30']}**\n"
        f"- Canonical thread URLs: **{stats['thread_canonical']}**\n"
        f"- Forum raw rows 180d: **{stats['forum_raw_180']}**\n"
        f"- Forum raw rows 30d: **{stats['forum_raw_30']}**\n"
        f"- Canonical forum URLs: **{stats['forum_canonical']}**\n"
    )
    lines.append("## Top forum landing pages\n")
    lines.append(table(
        ["#", "Forum", "180d Sessions", "180d Organic Clicks", "Impr.", "CTR", "Avg Pos", "Score"],
        [[str(i+1), r.get("title", r["path"]), fmt_num(r["sessions180"]), fmt_num(r["organicClicks180"]), fmt_num(r["organicImpressions180"]), fmt_pct(r["ctr180"]), f"{r['avgPosition180']:.1f}", f"{r['performanceScore']:.1f}"] for i, r in enumerate(top_forums)]
    ))
    lines.append("\n## Top 25 thread winners\n")
    lines.append(table(
        ["#", "Thread", "Forum", "180d Sessions", "180d Organic Clicks", "Impr.", "CTR", "Avg Pos", "Words", "On-site Pages", "Score"],
        [[
            str(i+1),
            r["title"],
            r.get("forumName", ""),
            fmt_num(r["sessions180"]),
            fmt_num(r["organicClicks180"]),
            fmt_num(r["organicImpressions180"]),
            fmt_pct(r["ctr180"]),
            f"{r['avgPosition180']:.1f}",
            fmt_num(r.get("firstPostWords", 0)),
            fmt_num(r.get("pageCount", 1)),
            f"{r['performanceScore']:.1f}",
        ] for i, r in enumerate(top_threads)]
    ))
    lines.append("\n## What the winners have in common\n")
    lines.append(f"- **Google page 1 presence:** {insights['page1_threads']} of top 25 average on page 1 (`avg position <= 10`). {insights['page2plus_threads']} sit beyond page 1 on average.\n")
    lines.append(f"- **On-site pagination:** {insights['multi_page_threads']} of top 25 are multi-page threads. Median thread depth is **{insights['median_page_count']}** pages. This is not a page-1-only site pattern; long-running threads can rank and keep accumulating traffic.\n")
    lines.append(f"- **Title length:** median **{insights['median_title_words']}** words / **{insights['median_title_chars']}** chars. Winners are usually specific, not cute.\n")
    lines.append(f"- **Opening text length:** median first-post length is **{insights['median_body_words']}** words; mean **{insights['mean_body_words']}**. Strong winners include both short intent-match Q&A pages and long evergreen reference posts.\n")
    lines.append(f"- **Tags:** visible tag count across the sampled winners is **{insights['tagged_threads']} / 25**. Tags do not look like a meaningful visible driver on the current winners.\n")
    lines.append(f"- **Dominant title archetypes:** {', '.join([f"{k} ({v})" for k, v in insights['title_type_counts']])}.\n")
    lines.append(f"- **Top winning forums:** {', '.join([f"{k} ({v})" for k, v in insights['forum_counts'][:6]])}.\n")
    lines.append("\n## Recurring title keywords\n")
    lines.append("- Terms: " + ", ".join([f"`{k}` ({v})" for k, v in insights['title_terms']]) + "\n")
    lines.append("- Bigrams: " + ", ".join([f"`{k}` ({v})" for k, v in insights['title_bigrams']]) + "\n")
    lines.append("\n## Recurring body keywords from first posts\n")
    lines.append("- Terms: " + ", ".join([f"`{k}` ({v})" for k, v in insights['body_terms']]) + "\n")
    lines.append("\n## Read on why they win\n")
    lines.append("1. **Specific search intent in titles.** Winners usually name the exact model, problem, comparison, reference family, or terminology people actually search.\n")
    lines.append("2. **Evergreen reference content matters.** Guides, reviews, serial-number/reference threads, and fitment/comparison threads punch above their weight.\n")
    lines.append("3. **Community accretion helps.** Multi-page threads often keep ranking because replies add breadth, long-tail terms, and fresh activity.\n")
    lines.append("4. **Most winners are already near or on page 1.** That means the biggest upside is often packaging: title, intro, headings, and internal linking — not inventing whole new topics.\n")
    lines.append("5. **Forum hubs matter as feeders.** The best forum landing pages are discovery hubs that should link harder to evergreen winner threads.\n")
    lines.append("\n## Actionable patterns to copy\n")
    lines.append("- Use titles with **brand + model + exact problem / comparison / reference term**.\n")
    lines.append("- For evergreen threads, add a **tight intro summary** near the top so Google gets the answer faster.\n")
    lines.append("- Add **clear subheads** for variants, fitment, reference numbers, pros/cons, FAQ-style questions.\n")
    lines.append("- Build or refresh **reference/guide threads** inside the winning forums, then link them from forum indexes and related threads.\n")
    lines.append("- Do not rely on tags to do the heavy lifting. The crawlable winners do not show a strong visible tag pattern.\n")
    lines.append("\n## Best threads to model future SEO work on\n")
    model_rows = [r for r in top_threads if r.get("threadKind") in {"guide/reference", "question/problem", "discussion/reference"}][:10]
    for r in model_rows:
        lines.append(
            f"- **[{r['title']}]({r['url']})** — {r.get('forumName','')} · {fmt_num(r['organicClicks180'])} clicks / {fmt_num(r['organicImpressions180'])} impressions · {fmt_pct(r['ctr180'])} CTR · avg pos {r['avgPosition180']:.1f} · {fmt_num(r.get('firstPostWords',0))} words · {r.get('pageCount',1)} on-site pages."
        )
    lines.append("\n## Raw notes on page-1 vs more\n")
    lines.append("- **Google:** page-1 dominance is real, but not everything is top-3. Many winners live in positions 4–10 and still print traffic.\n")
    lines.append("- **On-site:** multi-page threads are common among winners, so depth/community accumulation can help instead of hurt.\n")
    return "\n".join(lines) + "\n"


async def main():
    thread_180_eng = await fetch_all_rows("180d", THREAD_FILTER, ENGAGEMENT_METRICS)
    thread_180_search = await fetch_all_rows("180d", THREAD_FILTER, SEARCH_METRICS)
    thread_30_eng = await fetch_all_rows("30d", THREAD_FILTER, ENGAGEMENT_METRICS)
    thread_30_search = await fetch_all_rows("30d", THREAD_FILTER, SEARCH_METRICS)

    forum_180_eng = await fetch_all_rows("180d", FORUM_FILTER, ENGAGEMENT_METRICS)
    forum_180_search = await fetch_all_rows("180d", FORUM_FILTER, SEARCH_METRICS)
    forum_30_eng = await fetch_all_rows("30d", FORUM_FILTER, ENGAGEMENT_METRICS)
    forum_30_search = await fetch_all_rows("30d", FORUM_FILTER, SEARCH_METRICS)

    thread_180 = aggregate_rows(merge_raw_rows(thread_180_eng, thread_180_search), "thread")
    thread_30 = aggregate_rows(merge_raw_rows(thread_30_eng, thread_30_search), "thread")
    forum_180 = aggregate_rows(merge_raw_rows(forum_180_eng, forum_180_search), "forum")
    forum_30 = aggregate_rows(merge_raw_rows(forum_30_eng, forum_30_search), "forum")

    thread_rows = build_dataset(thread_180, thread_30, "thread")
    forum_rows = build_dataset(forum_180, forum_30, "forum")

    for row in thread_rows[:25]:
        s = scrape_page(row["path"])
        row["title"] = s.title
        row["breadcrumbs"] = s.breadcrumbs
        row["forumName"] = s.breadcrumbs[-1] if s.breadcrumbs else ""
        row["firstPostWords"] = s.first_post_words
        row["titleWords"] = s.title_words
        row["titleChars"] = s.title_chars
        row["headingCount"] = s.heading_count
        row["tagCount"] = s.tag_count
        row["pageCount"] = s.page_count
        row["metaDescription"] = s.meta_description
        row["threadKind"] = s.thread_kind
        row["firstPostText"] = s.first_post_text

    for row in thread_rows[25:60]:
        # light scrape for extra comparative stats if needed later
        s = scrape_page(row["path"])
        row["title"] = s.title
        row["breadcrumbs"] = s.breadcrumbs
        row["forumName"] = s.breadcrumbs[-1] if s.breadcrumbs else ""
        row["firstPostWords"] = s.first_post_words
        row["titleWords"] = s.title_words
        row["titleChars"] = s.title_chars
        row["headingCount"] = s.heading_count
        row["tagCount"] = s.tag_count
        row["pageCount"] = s.page_count
        row["metaDescription"] = s.meta_description
        row["threadKind"] = s.thread_kind
        row["firstPostText"] = s.first_post_text

    for row in forum_rows[:12]:
        s = scrape_page(row["path"])
        row["title"] = s.title
        row["breadcrumbs"] = s.breadcrumbs

    top25 = thread_rows[:25]
    page1_threads = sum(1 for r in top25 if 0 < r["avgPosition180"] <= 10)
    multi_page_threads = sum(1 for r in top25 if r.get("pageCount", 1) > 1)
    title_type_counts = Counter(r.get("threadKind", "unknown") for r in top25).most_common()
    forum_counts = Counter(r.get("forumName", "") or "(unknown)" for r in top25).most_common()
    title_terms = top_terms(top25, "title")
    title_bigrams = top_bigrams(top25, "title")
    body_terms = top_terms(top25, "firstPostText")
    tagged_threads = sum(1 for r in top25 if r.get("tagCount", 0) > 0)

    insights = {
        "page1_threads": page1_threads,
        "page2plus_threads": 25 - page1_threads,
        "multi_page_threads": multi_page_threads,
        "median_page_count": statistics.median([r.get("pageCount", 1) for r in top25]) if top25 else 1,
        "median_title_words": statistics.median([r.get("titleWords", 0) for r in top25]) if top25 else 0,
        "median_title_chars": statistics.median([r.get("titleChars", 0) for r in top25]) if top25 else 0,
        "median_body_words": statistics.median([r.get("firstPostWords", 0) for r in top25]) if top25 else 0,
        "mean_body_words": round(statistics.mean([r.get("firstPostWords", 0) for r in top25])) if top25 else 0,
        "tagged_threads": tagged_threads,
        "title_type_counts": title_type_counts,
        "forum_counts": forum_counts,
        "title_terms": title_terms,
        "title_bigrams": title_bigrams,
        "body_terms": body_terms,
    }

    stats = {
        "thread_raw_180": len(thread_180_eng),
        "thread_raw_30": len(thread_30_eng),
        "thread_canonical": len(thread_180),
        "forum_raw_180": len(forum_180_eng),
        "forum_raw_30": len(forum_30_eng),
        "forum_canonical": len(forum_180),
    }

    markdown = render_markdown(thread_rows, forum_rows, insights, stats)
    MD_PATH.write_text(markdown, encoding="utf-8")
    payload = {
        "generated": TODAY,
        "stats": stats,
        "insights": insights,
        "top_threads": top25,
        "top_forums": forum_rows[:12],
        "markdown": str(MD_PATH),
    }
    JSON_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps({
        "markdown": str(MD_PATH),
        "json": str(JSON_PATH),
        "stats": stats,
        "top_threads": [{
            "title": r.get("title", r["path"]),
            "forum": r.get("forumName", ""),
            "clicks180": r["organicClicks180"],
            "impressions180": r["organicImpressions180"],
            "ctr180": r["ctr180"],
            "avgPosition180": r["avgPosition180"],
            "performanceScore": r["performanceScore"],
        } for r in top25[:10]],
        "top_forums": [{
            "title": r.get("title", r["path"]),
            "clicks180": r["organicClicks180"],
            "impressions180": r["organicImpressions180"],
            "ctr180": r["ctr180"],
            "avgPosition180": r["avgPosition180"],
            "performanceScore": r["performanceScore"],
        } for r in forum_rows[:8]],
        "insights": insights,
    }, indent=2))


if __name__ == "__main__":
    asyncio.run(main())

