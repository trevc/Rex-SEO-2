# Omega Forums SEO Toolkit (Rex-SEO-2)

Agent-facing companion to the Rex-SEO-2 repo. Contains workflow rules,
credentials layout, and Chrome MCP recovery instructions.

## Repository

- Path: `/home/trev/Rex-SEO-2/`
- GitHub: `https://github.com/trevc/Rex-SEO-2`
- `.env` is **gitignored** and lives at repo root
- `.env-example` shows the required variables without secret values

## Verified tool status

As of the latest setup session, all tools are installed and confirmed working:

| Tool | Status | Details |
|------|--------|---------|
| `analytics-mcp` (pipx) | ✅ | Python venv at `~/.local/share/pipx/venvs/analytics-mcp/`, shebang interpreter working |
| `google-analytics-mcp` | ✅ | Global opencode MCP server, responds to JSON-RPC `initialize` |
| `chrome-devtools-mcp` | ✅ | Global npm install, navigated `omegaforums.net`, snapshot + screenshot captured |
| XenForo API | ✅ | `~/.config/xenforo-api.env` loaded, `GET /threads/` returns 200 with live data |

Global opencode MCP config: `~/.config/opencode/opencode.json` wires both `google-analytics` and `chrome-devtools` as local stdio servers.

## Secrets layout

Local sensitive files:
- Repo `.env` — GA4 property ID, project ID, site root, outdir, XenForo API key
- `/home/trev/ga4-mcp-key.json` — Google service account JSON for GA4
- `~/.config/xenforo-api.env` — XenForo API base URL and key

See `.env-example` for variable names. Never commit secrets.
Never print credentials to chat, logs, or reports.

## Required interpreter

```bash
pipx install analytics-mcp
~/.local/share/pipx/venvs/analytics-mcp/bin/python --version
```

Preferred interpreter for embedded scripts:
```bash
~/.local/share/pipx/venvs/analytics-mcp/bin/python
```

## Running reports

Load env then execute scripts from repo root:

```bash
export $(grep -v '^#' /home/trev/Rex-SEO-2/.env | xargs)
~/.local/share/pipx/venvs/analytics-mcp/bin/python scripts/ga4_thread_opportunity_report.py
~/.local/share/pipx/venvs/analytics-mcp/bin/python scripts/ga4_best_performers_report.py
```

## Targets

- Site root: `https://omegaforums.net`
- XenForo API base: `https://omegaforums.net/api/`
- GA4 property ID: `311312437`
- Google project ID: `omega-forums`
- Preferred DNS resolver if GA4 fails due to poisoned DNS: `1.1.1.1`

## Hard rules for Omega Forums SEO

- Prioritise high-engagement pages underperforming in Google search.
- Exclude threads whose first post primarily leads off-site.
- Always consult Raptive data too; primary sources are GA4/GSC, XenForo API, and the Raptive spreadsheet.
- For any proposed Omega Forums post revisions, output only XenForo-compatible BBCode.
- Do not wrap BBCode in fenced code blocks unless explicitly asked for literal/raw BBCode.
- Use best-performer analysis to learn patterns, but prioritise underperforming page-1 / near-page-1 threads with upside rather than already-big winners.
- Do not assume a candidate edit pack equals the actually implemented live-edit set unless separately verified.

## Verified GA4/GSC behaviour

Working Search Console-linked metrics through GA4:
- `organicGoogleSearchClicks`
- `organicGoogleSearchImpressions`
- `organicGoogleSearchClickThroughRate`
- `organicGoogleSearchAveragePosition`

Primary page dimension: `landingPagePlusQueryString`
Primary thread filter: contains `/threads/`

Known-safe minimal metric set for before/after comparison work:
- `sessions`
- `organicGoogleSearchClicks`
- `organicGoogleSearchImpressions`
- `organicGoogleSearchClickThroughRate`
- `organicGoogleSearchAveragePosition`

If a report 400s on compatibility, strip back metrics until the request succeeds.
`screenPageViews` previously caused failures when included with `landingPagePlusQueryString`.

## DNS gotchas

If GA4 calls fail with connection errors:
```bash
cat /etc/resolv.conf
getent ahosts analyticsdata.googleapis.com
```
If you see LAN/private IPs, fix DNS before blaming auth or code.

## XenForo API usage

When posting or editing content on XenForo, generate real XenForo BBCode, not Markdown.
Only use fenced/raw blocks if another agent explicitly needs literal BBCode.

## Chrome MCP Setup

**Purpose**: Restore Chrome DevTools MCP reliably when it reports `Could not find DevToolsActivePort`.

**Observed failure mode**:
- A stale `~/.config/google-chrome/DevToolsActivePort` symlink can exist even when no live debug Chrome is reachable.
- Launching Chrome from inside the sandbox fails with `crashpad ... setsockopt: Operation not permitted`.
- Launching Chrome from an exec session without `setsid`/detaching can print `DevTools listening ...` and then die as soon as the session exits.

**Reliable recovery pattern**:
- Use host execution, not sandboxed execution.
- Launch an isolated headless Chrome with a temp profile and detach it with `setsid`.
- Copy the generated `DevToolsActivePort` file into `~/.config/google-chrome/DevToolsActivePort` as a real file, not a symlink.
- Verify the port before using MCP.

**Working recovery command**:
```bash
profile=$(mktemp -d /tmp/codex-chrome-profile-XXXXXX); setsid -f /usr/bin/google-chrome-stable --headless=new --disable-gpu --remote-debugging-port=0 --user-data-dir="$profile" --no-first-run --no-default-browser-check about:blank >/tmp/codex-chrome-launch.log 2>&1; for i in $(seq 1 20); do if [ -f "$profile/DevToolsActivePort" ]; then break; fi; sleep 0.5; done; cp "$profile/DevToolsActivePort" /home/trev/.config/google-chrome/DevToolsActivePort; port=$(head -n1 "$profile/DevToolsActivePort"); curl -sS "http://127.0.0.1:$port/json/version"
```

**Verification**:
- The `curl` above must return Chrome version JSON.
- After that, `chrome_devtools.list_pages` should succeed.

**Do not rely on**:
- The old symlink pattern from `~/.config/google-chrome/DevToolsActivePort` into `/tmp/...`
- A non-detached `google-chrome-stable ...` launch from a normal exec session
- The stale long-running Chrome process if its debug port does not answer locally

## Files in repo

- `scripts/ga4_thread_opportunity_report.py` — thread-level GA4 + GSC opportunity scoring
- `scripts/ga4_best_performers_report.py` — analyse winning threads/forums to extract patterns
- `.env-example` — template for required environment variables
- `.gitignore` — excludes `.env`, reports, and JSON keys

## Report outputs

Scripts write to `OUTDIR` (default `/home/trev/.openclaw/workspace/reports`):
- `seo-thread-opportunities-{DATE}.json`
- `seo-thread-opportunities-{DATE}.md`
- `seo-thread-opportunities-{DATE}.csv`
- `seo-best-performers-{DATE}.md`
- `seo-best-performers-{DATE}.json`

## Benchmark: Top 20 Thread Title Rewrites

**Baseline date:** 2026-04-22  
**Benchmark thread IDs:** `1790,166398,176728,125224,190100,145939,117608,135883,151225,8930,96762,159035,19435,93110,175065,154032,42644,154922,83475,179617`

These 20 threads were selected by `scripts/ga4_thread_opportunity_report.py` as having the highest ROI potential from title rewrites. Baseline metrics are stored in:
- `/home/trev/.openclaw/workspace/reports/seo-thread-opportunities-2026-04-22.json`

### How to rerun the benchmark

When the user asks for a "rerun" or "check benchmark" or "compare title rewrite results":

1. Fix DNS if needed (`resolvectl dns enp7s0 1.1.1.1 8.8.8.8 && resolvectl flush-caches` or use `getent ahosts analyticsdata.googleapis.com` to verify).
2. Run the opportunity report:
   ```bash
   export $(grep -v '^#' /home/trev/Rex-SEO-2/.env | xargs)
   ~/.local/share/pipx/venvs/analytics-mcp/bin/python /home/trev/Rex-SEO-2/scripts/ga4_thread_opportunity_report.py
   ```
3. Read the latest JSON report from `OUTDIR`.
4. Filter to the benchmark thread IDs above.
5. Output a comparison table with: current title, position, CTR, clicks, impressions, sessions — plus the delta vs the baseline date (2026-04-22).
6. Also provide the comma-separated thread ID list on its own line for copy/paste.

## Absolute path hygiene

When passing `filePath` to Read, Write, or Edit tools, always provide a **literal absolute path starting with `/`**. Do **not** prefix paths with stray characters (especially `:`) — when a `filePath` does not begin with `/`, the read tool treats it as relative to the working directory and prepends the CWD, producing bogus doubled paths like `/home/trev/rex-seo/:/home/trev/.config/opencode/tui.json`.

- **Wrong:** `"filePath":":/home/trev/.config/opencode/tui.json"`
- **Correct:** `"filePath":"/home/trev/.config/opencode/tui.json"`

Verify the path before calling Read/Write/Edit if unsure.
