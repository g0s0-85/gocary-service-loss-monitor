# GoCary Service Loss Monitor

Cross-checks GoCary's GTFS-RT feed against the static GTFS schedule to
automatically flag the kinds of service loss that ops has historically had
to infer by hand from daily dispatch logs — hosted entirely on GitHub, no
server to run or pay for. Same shape as
[gocary-transit-dashboard](https://github.com/g0s0-85/gocary-transit-dashboard)
and [gocary-news-monitor](https://github.com/g0s0-85/gocary-news-monitor): a
GitHub Actions poller commits JSON under `docs/data/`, and a static
`docs/index.html` on GitHub Pages reads it.

## What it detects

- **Canceled** — a trip GoCary's own RT feed flags `CANCELED`
  (`schedule_relationship`). Direct signal, but only catches what dispatch
  chooses to report through the feed.
- **No-show** — GoCary's RT feed assigns each trip a random UUID with no
  relationship to the static GTFS's `trip_id`, and neither feed populates
  `start_time`/`start_date` either, so there's no field to match a live trip
  back to one specific scheduled trip (confirmed by checking both raw feeds
  directly). Instead this compares, per route, how many scheduled trips
  should have finished by now (15 min past their window, `NO_SHOW_GRACE_S`)
  against how many distinct trips actually showed up on that route today —
  a shortfall is flagged against the earliest unmatched scheduled slot(s),
  assuming trips run in schedule order. The count is reliable; which exact
  slot it points at can be wrong if a vehicle skips a trip and runs a later
  one on time instead.
- **Severe delay** — a trip that did run, but fell more than 20 minutes
  behind its predicted schedule — a rider-facing miss even though the trip
  technically operated. (This is a different, stricter threshold than
  gocary-transit-dashboard's on-time/late split, which is about day-to-day
  schedule adherence, not "did the rider effectively lose the trip.")
- **Route gap** — a route that should have live coverage right now (it has a
  scheduled trip in progress) but has gone 20+ minutes with no vehicle or
  trip-update activity at all. Opens when the gap starts, closes
  automatically the moment coverage resumes.

All four thresholds are constants at the top of `scripts/poll_service_loss.py`
(`SEVERE_DELAY_S`, `NO_SHOW_GRACE_S`, `ROUTE_GAP_S`) — adjust them there if
GoCary's operational definition of "lost service" differs from these
defaults.

## How it works

- **`scripts/poll_service_loss.py`** — fetches `GTFS_TripUpdates.pb` and
  `GTFS_VehiclePositions.pb`, tracks which scheduled trips have shown up
  today (`docs/data/live/seen_trips.json`) and which routes have had recent
  activity (`docs/data/live/route_activity.json`), and writes:
  - `docs/data/events/YYYY-MM-DD.json` — every event detected that service
    day, each with a type, route, trip (where applicable), detection time,
    and resolution time (route gaps only). This file *is* the record —
    there's no other archive of this.
  - `docs/data/events/index.json` — list of dates with an events file, the
    same "poller writes the listing itself" workaround
    gocary-transit-dashboard uses, since GitHub Pages has no directory
    listing and the Contents API's listing endpoint has no static fallback.
  - `docs/data/live/active_events.json` — today's events plus any
    currently-open route gaps, for the dashboard banner and for the alerting
    step to check against.
  - `docs/data/schedule.json` / `docs/data/routes.json` — the static
    schedule (trips, calendar, calendar_dates) and route metadata, refreshed
    weekly from GoCary's static GTFS zip. This is what "no-show" and "route
    gap" are measured against — without it there's no way to know what
    *should* be running right now.
  - `docs/data/status.json` — last poll time, error (if any), poll count,
    today's event count, currently-open gap count.

  **On dedup:** each event gets a stable id (`{type}:{trip_id}`, or
  `{type}:{route_id}:{opened_at}` for route gaps) so re-running detection
  every poll doesn't create duplicate events for the same underlying trip or
  gap — a route gap's record is instead updated in place with a
  `resolved_at` timestamp once coverage resumes.

  **Alerting:** if the `SERVICE_LOSS_WEBHOOK_URL` repo secret is set, the
  poller POSTs a Slack-compatible `{"text": "..."}` payload for every newly
  detected event and every route-gap resolution. Slack's own "Incoming
  Webhooks" app produces a URL that works as-is; most other chat tools that
  accept a generic incoming webhook (Discord, via its
  `/slack`-compatible endpoint suffix; Microsoft Teams via a connector) will
  also accept this shape. Leave the secret unset to run dashboard-only with
  no notifications.

- **`.github/workflows/poll-service-loss.yml`** — runs the script (passing
  the webhook secret through as an env var) and commits `docs/data` if
  anything changed. Only triggered by `workflow_dispatch`, same reasoning as
  the sibling projects: GitHub's own `schedule:` trigger is unreliable.
- **A [cron-job.org](https://cron-job.org) job** (set up separately, not
  part of this repo) calls GitHub's API on an interval to fire
  `workflow_dispatch`. Every 1–2 minutes is reasonable — no point polling
  faster than GoCary's feeds actually update, and the no-show / route-gap
  checks only need to notice something within their grace periods, not
  instantly.
- **`docs/index.html`** — a static dashboard (no backend): a banner for any
  currently-open route gaps, a filterable event table (defaults to today,
  pick any past date from the dropdown), and a 14-day trend table of event
  counts by type. Same Contents-API-first-then-Pages-fallback data loading
  as gocary-transit-dashboard.

## One-time setup

1. **Create a GitHub repo** and push this folder to it:
   ```bash
   git init
   git add .
   git commit -m "Set up GoCary service loss monitor"
   git branch -M main
   git remote add origin https://github.com/<you>/gocary-service-loss-monitor.git
   git push -u origin main
   ```
   If you use a different GitHub username/org or repo name than
   `g0s0-85/gocary-service-loss-monitor`, update the `REPO` constant near
   the top of the `<script>` block in `docs/index.html` to match.

2. **Let the workflow push commits.**
   `Settings → Actions → General → Workflow permissions` → select
   **"Read and write permissions"** → Save.

3. **(Optional) Wire up alerts.** Create a Slack Incoming Webhook (or
   equivalent) URL, then `Settings → Secrets and variables → Actions → New
   repository secret` → name it `SERVICE_LOSS_WEBHOOK_URL` → paste the URL.
   Skip this step to run dashboard-only.

4. **Turn on GitHub Pages.**
   `Settings → Pages` → **Source**: "Deploy from a branch", **Branch**:
   `main`, folder **`/docs`** → Save.

5. **Kick off the first poll**: Actions tab → "Poll GoCary Service Loss" →
   **Run workflow**. No-show and route-gap detection need at least one full
   scheduled trip window to pass before they can flag anything meaningful;
   cancellations and severe delays can show up as soon as GoCary's feed
   reports one.

6. **Set up the external trigger**: a fine-grained GitHub token scoped to
   this repo with "Actions: Read and write" permission, and a free
   cron-job.org job that POSTs to
   `https://api.github.com/repos/<you>/gocary-service-loss-monitor/actions/workflows/poll-service-loss.yml/dispatches`
   with that token in an `Authorization: Bearer <token>` header and body
   `{"ref":"main"}`.

## Adjusting things

- **Poll frequency**: change the cron-job.org schedule.
- **Detection thresholds**: `SEVERE_DELAY_S` / `NO_SHOW_GRACE_S` /
  `ROUTE_GAP_S` in `scripts/poll_service_loss.py`.
- **Alerts**: add/remove/rotate the `SERVICE_LOSS_WEBHOOK_URL` secret.
