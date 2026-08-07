# Simplify ETF Dashboard

Tracks the 42-ETF lineup from [Simplify Asset Management](https://simplify.us): performance, relative strength ranking, volume flow, expense ratios, TTM yield, and 1-year Z-Score positioning. Forked from the KCM ETF Dashboard.

**Live:** simplify.market-dashboards.com
**Stack:** Flask (Python) + Tiingo API (price data) + Upstash Redis (cache) + Chart.js (Z-Score tab) + Gunicorn, deployed as a Dokku app on the DigitalOcean droplet.

---

## How it works

1. `app.py` pulls 3 years of daily prices per symbol from Tiingo, computes returns (1D/1W/1M/3M/6M/YTD/1Y), a Relative Strength score, volume flow, SMA flags, and 1-year Z-Scores.
2. Results are cached in Upstash Redis (`simplify_dashboard_cache`) so repeat visits load instantly instead of re-hitting Tiingo.
3. A weekly Z-Score snapshot is also stored (`simplify_weekly_history_v1`) to power the "13-Week Trail" view on the Z-Score Chart tab.
4. The page has two tabs: **Table** (the main ranked list) and **Z-Score Chart** (scatter plot of every fund's Z-Score vs. 4 benchmark ETFs: SPY, VGK, IEF, DBC).
5. The whole dataset is rebuilt automatically once a day; visiting `/refresh` manually forces an immediate rebuild and wipes the cache + weekly history.

## Fixing the Z-Score Chart tab bug

The Z-Score Chart was appearing directly below the table on the same page instead of being hidden behind its own tab (see your screenshot). The cause: the stylesheet only ever defined `.price-modal.hidden { display: none; }` — there was no generic `.hidden { display: none; }` rule, so adding the `hidden` class to `#panel-zchart` via JavaScript had no visual effect outside of print mode.

**Fix applied** in `templates/index.html`: added a generic
```css
.hidden { display: none !important; }
```
rule (scoped outside the print media query). The tab-switching JavaScript (`switchTab()`) was already correct — it just needed the CSS to back it up. No other files were touched.

## Project files

| File | Purpose |
|---|---|
| `app.py` | Flask app — data fetching, scoring, caching, API routes |
| `templates/index.html` | The dashboard page (HTML + CSS + JS, single file) |
| `funds.json` | The 42-fund roster: symbol, name, category, type, expense ratio, TTM yield |
| `requirements.txt` | Python dependencies |
| `Procfile` | Tells Dokku/Gunicorn how to run the app |

## Environment variables (set on the Dokku app, not in code)

| Variable | Purpose |
|---|---|
| `TIINGO_TOKEN` | Tiingo API key for price data |
| `UPSTASH_REDIS_REST_URL` | Redis cache endpoint |
| `UPSTASH_REDIS_REST_TOKEN` | Redis cache auth token |
| `PORT` | Set automatically by Dokku |

## Updating fund data

- **Expense ratios** (`exp_ratio` in `funds.json`): update annually when Simplify publishes new prospectuses.
- **TTM Yield** (`ttm_yield` in `funds.json`): update monthly by hand — this isn't pulled from Tiingo. The "Last TTM update" date shown in the table's toggle bar is a separate field set directly in `templates/index.html` (search for `ttm-update-date`).
- **Adding/removing a fund**: add or delete its entry in `funds.json`, then hit `/refresh` (or wait for the next daily auto-refresh) so Redis picks up the change.

## Running locally

You'd need Python 3 and the three environment variables above set, then:
```
pip install -r requirements.txt
python app.py
```
This starts the app at `http://localhost:5000`. In practice, local runs are mainly useful for previewing HTML/CSS tweaks — the real data fetch needs a valid Tiingo token and Redis credentials, which live on the server, not in this repo.

## Deploying changes

Same flow as the other dashboards on this server: commit the change (e.g. via GitHub Desktop), then push to the Dokku remote for this app. Dokku rebuilds and restarts the app automatically on push.
