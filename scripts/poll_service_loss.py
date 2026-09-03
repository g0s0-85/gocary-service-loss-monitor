"""
Poll GoCary's GTFS-RT feeds (TripUpdates, VehiclePositions) and cross-check
them against GoCary's static GTFS schedule to automatically detect the kinds
of service loss that ops has historically had to infer by hand from daily
dispatch logs:

  - canceled   -- a trip the RT feed itself flags CANCELED (schedule_relationship)
  - no_show    -- fewer distinct trips have been observed on a route today than
                  the number of that route's scheduled trips whose window has
                  fully closed -- see detect_events()'s no_show section for why
                  this is a route-level headcount rather than matching specific
                  trip_ids (GoCary's RT and static feeds don't share an id).
                  Suppressed entirely on the monitor's first day of operation
                  (status.json's first_run_date) since a partial day always
                  looks like a deficit for reasons that have nothing to do
                  with real service loss -- see main()/detect_events().
  - severe_delay -- a trip that did run, but fell so far behind (>20 min) that
                  it's effectively a missed connection for anyone relying on it
  - route_gap  -- a route that should have live coverage right now (it has a
                  scheduled trip in progress) but no vehicle/trip activity has
                  been seen for it in a while -- open while ongoing, closed the
                  moment coverage resumes

Meant to be run on a schedule by .github/workflows/poll-service-loss.yml,
which commits whatever this script writes under docs/data/ and, if
SERVICE_LOSS_WEBHOOK_URL is set, posts a Slack-compatible webhook message for
every newly detected (or newly resolved) event.

Same overall shape as the sibling gocary-transit-dashboard project: this
script only classifies things, it never decides polling frequency (that's an
external cron-job.org trigger hitting workflow_dispatch, same reasoning as
that project -- GitHub's own `schedule:` trigger is unreliable).

Known limitation: "today" is the real calendar date in America/New_York, not
a GTFS service-day, so a trip scheduled past midnight (stop_times.txt times
>= 24:00:00) would have its in-progress tracking reset at the real midnight
rollover along with everything else that day. GoCary doesn't run overnight
service as of this writing, so this hasn't mattered in practice -- but if
that ever changes, seen_trips.json / the events file would need to key off a
GTFS service-day boundary instead of the calendar date.
"""

import csv
import io
import json
import os
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from google.transit import gtfs_realtime_pb2

VEHICLE_POSITIONS_URL = "https://www.gocarylive.org/GTFS/Realtime/GTFS_VehiclePositions.pb"
TRIP_UPDATES_URL = "https://www.gocarylive.org/GTFS/Realtime/GTFS_TripUpdates.pb"
STATIC_GTFS_URL = "http://data.trilliumtransit.com/gtfs/cary-transit-nc-us/cary-transit-nc-us.zip"

SERVICE_TZ = ZoneInfo("America/New_York")

# A trip counts as severely delayed once its current predicted/observed delay
# (seconds, positive = late) crosses this -- distinct from gocary-transit-
# dashboard's on-time/late split (5 min), which is about schedule adherence.
# This is about whether the trip is still useful to a rider at all.
SEVERE_DELAY_S = 20 * 60

# How long past a scheduled trip's *entire* window (start through last stop)
# we wait before calling it a no-show rather than "just running very late".
# A trip that's 25 minutes late but eventually shows up is a severe_delay,
# not a no_show -- this grace period is what tells the two apart.
NO_SHOW_GRACE_S = 15 * 60

# How long a route can go without any TripUpdate/VehiclePosition activity
# before we treat it as a coverage gap, counted only while the route has at
# least one scheduled trip that should currently be running.
ROUTE_GAP_S = 20 * 60

ROUTES_MAX_AGE_S = 7 * 24 * 3600

# Routes still present in the static GTFS mirror (RT-feed short_name form)
# that GoCary no longer actually operates -- the mirror is a stale snapshot
# and has no "discontinued" flag, so there's no way to detect this from feed
# data alone. Maintained manually; confirmed directly by Mark: ACX
# (2026-09-02) and route 8 (2026-09-03) were both flagged as false
# route_gaps because the schedule still expects coverage that no longer
# exists. Anything listed here is excluded entirely from no_show and
# route_gap detection.
DISCONTINUED_ROUTES = {"ACX", "8"}

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "docs" / "data"
LIVE_DIR = DATA_DIR / "live"
EVENTS_DIR = DATA_DIR / "events"
SCHEDULE_FILE = DATA_DIR / "schedule.json"
ROUTES_FILE = DATA_DIR / "routes.json"
ROUTE_STOPS_FILE = DATA_DIR / "route_stops.json"
STATUS_FILE = DATA_DIR / "status.json"
SEEN_FILE = LIVE_DIR / "seen_trips.json"
ROUTE_ACTIVITY_FILE = LIVE_DIR / "route_activity.json"
ACTIVE_EVENTS_FILE = LIVE_DIR / "active_events.json"

WEBHOOK_URL = os.environ.get("SERVICE_LOSS_WEBHOOK_URL")

DAY_KEYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def now_service_dt():
    return datetime.now(SERVICE_TZ)


def seconds_since_service_midnight(dt):
    midnight = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return (dt - midnight).total_seconds()


def load_json(path, default):
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return default


def save_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def fetch_feed(url):
    resp = requests.get(url, timeout=20, headers={"User-Agent": "gocary-service-loss-monitor/1.0 (+github actions)"})
    resp.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(resp.content)
    return feed


def hms_to_seconds(hms):
    h, m, s = hms.split(":")
    return int(h) * 3600 + int(m) * 60 + int(s)


def ensure_static_data():
    """Refresh routes.json / schedule.json / route_stops.json from GoCary's
    static GTFS if schedule.json is missing or older than a week -- same
    once-a-week cadence as gocary-transit-dashboard, since route/trip/stop
    metadata barely ever changes. schedule.json caches, per trip_id, the
    (route_id, service_id, start_s, end_s) needed to know which trips
    *should* be running right now without re-downloading the zip on every
    poll. route_stops.json caches each route's stop coordinates (RT-style
    route_id keys) so the dashboard can approximate "where" for event types
    that have no real vehicle position (no_show, route_gap) by highlighting
    the route's stops rather than a fake precise pin. If a refresh fails,
    keep whatever was already cached rather than failing the whole run."""
    existing_schedule = load_json(SCHEDULE_FILE, None)
    # Also require route_stops.json to already exist before trusting the
    # cache -- otherwise a schedule.json from before route_stops.json
    # existed (still within the 7-day window) would make this return early
    # forever and route_stops.json would never actually get generated.
    if existing_schedule is not None and ROUTE_STOPS_FILE.exists():
        fetched_at = existing_schedule.get("_fetched_at")
        if fetched_at:
            age = time.time() - datetime.fromisoformat(fetched_at).timestamp()
            if age < ROUTES_MAX_AGE_S:
                return existing_schedule, load_json(ROUTES_FILE, {})

    try:
        resp = requests.get(STATIC_GTFS_URL, timeout=60)
        resp.raise_for_status()
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
        fetched_at = now_iso()

        routes = {"_fetched_at": fetched_at}
        with zf.open("routes.txt") as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
            for row in reader:
                routes[row["route_id"]] = {
                    "short_name": row.get("route_short_name") or row.get("route_long_name") or row["route_id"],
                    "long_name": row.get("route_long_name") or "",
                }

        with zf.open("trips.txt") as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
            trips = {
                row["trip_id"]: {"route_id": row["route_id"], "service_id": row["service_id"]}
                for row in reader
            }

        stops_by_id = {}
        with zf.open("stops.txt") as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
            for row in reader:
                if not row.get("stop_lat") or not row.get("stop_lon"):
                    continue
                stops_by_id[row["stop_id"]] = {
                    "name": row.get("stop_name") or row["stop_id"],
                    "lat": float(row["stop_lat"]),
                    "lon": float(row["stop_lon"]),
                }

        # stop_times.txt isn't guaranteed sorted by stop_sequence, so instead
        # of trusting "first row = start / last row = end", track the min and
        # max of each row's own arrival/departure across every row for that
        # trip -- that gives the same envelope regardless of row order.
        # Also collect, per (internal) route_id, the set of stops it serves
        # -- this is what lets the dashboard highlight "roughly where" for
        # no_show/route_gap events, which have no real vehicle position.
        route_stop_ids = {}
        with zf.open("stop_times.txt") as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
            for row in reader:
                trip = trips.get(row["trip_id"])
                if trip is None:
                    continue
                arr = row.get("arrival_time") or row.get("departure_time")
                dep = row.get("departure_time") or row.get("arrival_time")
                if not arr or not dep:
                    continue
                try:
                    arr_s, dep_s = hms_to_seconds(arr), hms_to_seconds(dep)
                except ValueError:
                    continue
                lo, hi = min(arr_s, dep_s), max(arr_s, dep_s)
                if "start_s" not in trip or lo < trip["start_s"]:
                    trip["start_s"] = lo
                if "end_s" not in trip or hi > trip["end_s"]:
                    trip["end_s"] = hi
                if row.get("stop_id"):
                    route_stop_ids.setdefault(trip["route_id"], set()).add(row["stop_id"])

        trips = {tid: t for tid, t in trips.items() if "start_s" in t and "end_s" in t}

        # route_stop_ids is keyed by the static feed's internal route_id;
        # translate to the RT feed's short_name here (same mismatch
        # documented throughout this file) so the dashboard can key
        # route_stops.json the same way it already keys everything else
        # from the live feed.
        route_stops = {}
        for internal_route_id, stop_ids in route_stop_ids.items():
            short_name = routes.get(internal_route_id, {}).get("short_name", internal_route_id)
            coords = [stops_by_id[sid] for sid in stop_ids if sid in stops_by_id]
            if coords:
                route_stops.setdefault(short_name, []).extend(coords)

        calendar = {}
        try:
            with zf.open("calendar.txt") as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
                for row in reader:
                    calendar[row["service_id"]] = {
                        **{day: row[day] == "1" for day in DAY_KEYS},
                        "start_date": row["start_date"],
                        "end_date": row["end_date"],
                    }
        except KeyError:
            pass  # calendar.txt is optional if the feed is calendar_dates-only

        calendar_dates = {}
        try:
            with zf.open("calendar_dates.txt") as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
                for row in reader:
                    calendar_dates[f"{row['service_id']}|{row['date']}"] = int(row["exception_type"])
        except KeyError:
            pass

        schedule = {
            "_fetched_at": fetched_at,
            "trips": trips,
            "calendar": calendar,
            "calendar_dates": calendar_dates,
        }
        save_json(SCHEDULE_FILE, schedule)
        save_json(ROUTES_FILE, routes)
        save_json(ROUTE_STOPS_FILE, route_stops)
        return schedule, routes
    except Exception as exc:
        print(f"Warning: failed to refresh schedule/routes/route_stops ({exc}); keeping existing copy")
        return (
            existing_schedule or {"_fetched_at": None, "trips": {}, "calendar": {}, "calendar_dates": {}},
            load_json(ROUTES_FILE, {}),
        )


def active_service_ids(calendar, calendar_dates, date_obj):
    """GoCary's mirrored static GTFS calendar.txt only has start_date/
    end_date ranges through 2024-12-31 (confirmed by inspecting the
    downloaded zip directly) -- every service_id's date range has already
    expired relative to any date in 2025+, staler than the sibling
    dashboard project's already-documented stop-roster staleness. Worse,
    the feed contains two full back-to-back *generations* of the same
    calendar under different service_ids ("October 2023" and "2024") whose
    trips.txt row counts match exactly per weekday pattern (confirmed:
    310/310, 168/168, 69/69, 59/59, 16/16) -- GoCary re-published an
    unchanged schedule under new service_ids rather than editing the old
    ones. Matching on day-of-week alone (ignoring the expired date range)
    would therefore double-count every trip, once per generation. The fix:
    only consider the single most recent generation (the one with the
    latest end_date) and match day-of-week within it -- that both survives
    the expired date range and avoids the duplicate-generation double
    count. calendar_dates.txt's exceptions are also all 2024-dated in this
    mirror, so holiday-specific overrides are effectively inert until
    GoCary/Trillium publish a fresher feed -- not fixable from this side,
    but the matching logic below is left in place so it starts working
    again automatically whenever that happens."""
    if not calendar:
        return set()
    latest_end = max(c["end_date"] for c in calendar.values())
    date_str = date_obj.strftime("%Y%m%d")
    day_key = DAY_KEYS[date_obj.weekday()]
    active = {sid for sid, c in calendar.items() if c["end_date"] == latest_end and c.get(day_key)}
    for key, extype in calendar_dates.items():
        sid, d = key.split("|", 1)
        if d != date_str:
            continue
        if extype == 1:
            active.add(sid)
        elif extype == 2:
            active.discard(sid)
    return active


def trip_delay(tu):
    """Same convention as gocary-transit-dashboard's vehicle display: the
    soonest upcoming stop's delay, as the best available read on 'how late
    is this trip right now'."""
    best = None
    for stu in tu.stop_time_update:
        d = None
        if stu.HasField("departure") and stu.departure.HasField("delay"):
            d = stu.departure.delay
        elif stu.HasField("arrival") and stu.arrival.HasField("delay"):
            d = stu.arrival.delay
        if d is not None:
            best = (stu.stop_sequence, d)
            break
    return best[1] if best else None


CANCELED = gtfs_realtime_pb2.TripDescriptor.CANCELED


def poll_seen(tu_feed, vp_feed, date):
    seen = load_json(SEEN_FILE, {"date": date, "trips": {}})
    if seen["date"] != date:
        seen = {"date": date, "trips": {}}

    route_activity = load_json(ROUTE_ACTIVITY_FILE, {})
    ts = now_iso()

    def touch(trip_id, route_id, delay=None, canceled=False, vehicle_id=None, lat=None, lon=None):
        rec = seen["trips"].setdefault(trip_id, {
            "route_id": route_id, "first_seen": ts, "last_seen": ts,
            "max_delay": None, "canceled": False,
            "vehicle_id": None, "lat": None, "lon": None,
        })
        rec["last_seen"] = ts
        if delay is not None:
            rec["max_delay"] = delay if rec["max_delay"] is None else max(rec["max_delay"], delay)
        if canceled:
            rec["canceled"] = True
        if vehicle_id is not None:
            rec["vehicle_id"] = vehicle_id
        if lat is not None and lon is not None:
            rec["lat"], rec["lon"] = lat, lon
        if route_id:
            route_activity[route_id] = ts

    for entity in tu_feed.entity:
        if not entity.HasField("trip_update"):
            continue
        tu = entity.trip_update
        touch(
            tu.trip.trip_id, tu.trip.route_id,
            delay=trip_delay(tu),
            canceled=(tu.trip.schedule_relationship == CANCELED),
        )

    # VehiclePositions is the only feed that carries an actual bus number
    # (vehicle.id, GoCary's own fleet numbering) and a live lat/lon -- this
    # is what lets the dashboard plot "approximately where" a canceled or
    # severe_delay event happened; TripUpdates alone has neither.
    for entity in vp_feed.entity:
        if not entity.HasField("vehicle") or not entity.vehicle.HasField("trip"):
            continue
        v = entity.vehicle
        touch(
            v.trip.trip_id, v.trip.route_id,
            canceled=(v.trip.schedule_relationship == CANCELED),
            vehicle_id=(v.vehicle.id if v.HasField("vehicle") and v.vehicle.id else None),
            lat=(v.position.latitude if v.HasField("position") else None),
            lon=(v.position.longitude if v.HasField("position") else None),
        )

    save_json(SEEN_FILE, seen)
    save_json(ROUTE_ACTIVITY_FILE, route_activity)
    return seen, route_activity


def route_name(routes, route_id):
    return routes.get(route_id, {}).get("short_name", route_id)


def detect_events(date, seen, route_activity, schedule, routes, now_dt, now_s, first_run_date):
    path = EVENTS_DIR / f"{date}.json"
    day = load_json(path, {"date": date, "events": []})
    existing_ids = {e["id"] for e in day["events"]}
    new_events = []

    def add(event):
        if event["id"] in existing_ids:
            return
        day["events"].append(event)
        existing_ids.add(event["id"])
        new_events.append(event)

    # canceled + severe_delay: driven by what actually showed up in the feed
    for trip_id, rec in seen["trips"].items():
        route_id = rec["route_id"]
        if rec["canceled"]:
            add({
                "id": f"canceled:{trip_id}", "type": "canceled", "trip_id": trip_id,
                "route_id": route_id, "route_short_name": route_name(routes, route_id),
                "detected_at": now_iso(), "resolved_at": None,
                "delay_seconds": rec["max_delay"], "alerted": False,
                "vehicle_id": rec.get("vehicle_id"), "lat": rec.get("lat"), "lon": rec.get("lon"),
            })
        elif rec["max_delay"] is not None and rec["max_delay"] > SEVERE_DELAY_S:
            add({
                "id": f"severe_delay:{trip_id}", "type": "severe_delay", "trip_id": trip_id,
                "route_id": route_id, "route_short_name": route_name(routes, route_id),
                "detected_at": now_iso(), "resolved_at": None,
                "delay_seconds": rec["max_delay"], "alerted": False,
                "vehicle_id": rec.get("vehicle_id"), "lat": rec.get("lat"), "lon": rec.get("lon"),
            })

    # no_show: GoCary's RT feed assigns its own random trip_id (a UUID) that
    # has no relationship to the static GTFS's trip_id, and neither feed
    # populates start_time/start_date on the trip descriptor either -- so
    # there is no field anywhere to match a live trip back to one specific
    # scheduled trip (confirmed by grepping both raw feeds for either id
    # format and finding no overlap). Individual trip-level no-show
    # detection is therefore not possible; instead, per route, compare how
    # many of that route's scheduled trips should have finished by now
    # (their window fully closed, past NO_SHOW_GRACE_S) against how many
    # distinct trip_ids have actually been seen for that route today at all.
    # A shortfall means that many scheduled runs likely never happened.
    # Which specific slot(s) is attributed by pairing scheduled and observed
    # trips in chronological order (earliest scheduled <-> earliest
    # observed) and flagging whatever's left over at the tail of the
    # schedule -- this assumes a route's trips run in schedule order, which
    # holds for GoCary's fixed routes but means a vehicle that skips an
    # earlier trip and later runs a later one on time can point at the
    # wrong slot even though the deficit count itself is still accurate.
    # schedule.json's trips are keyed by the static GTFS's *internal* route_id
    # (e.g. "1717"), but seen["trips"]/route_activity are keyed by whatever
    # the RT feed calls route_id -- which is already the short rider-facing
    # code (e.g. "7"), the same mismatch gocary-transit-dashboard documented
    # for its own routes.json lookups. Translate every scheduled trip's
    # route_id through routes.json's short_name up front so every comparison
    # below is in the RT feed's namespace; a route the RT feed reports that
    # isn't in the static snapshot at all (confirmed to happen for routes
    # "2" and "9" -- GoCary has added routes since routes.txt was last
    # mirrored) simply won't have any scheduled_today entries, so no_show
    # and route_gap silently can't be evaluated for it -- no false positives,
    # just no coverage, same degradation already accepted for route display.
    active_sids = active_service_ids(schedule.get("calendar", {}), schedule.get("calendar_dates", {}), now_dt.date())
    scheduled_today = {
        tid: {**t, "route_id": route_name(routes, t["route_id"])}
        for tid, t in schedule.get("trips", {}).items()
        if t["service_id"] in active_sids
    }
    scheduled_today = {
        tid: t for tid, t in scheduled_today.items()
        if t["route_id"] not in DISCONTINUED_ROUTES
    }
    scheduled_by_route = {}
    for info in scheduled_today.values():
        scheduled_by_route.setdefault(info["route_id"], []).append(info)

    observed_by_route = {}
    for trip_id, rec in seen["trips"].items():
        observed_by_route.setdefault(rec["route_id"], []).append(rec["first_seen"])

    # The monitor's first day of operation starts partway through the
    # service day (whenever it's first deployed), so "scheduled" always
    # covers the whole day from midnight while "observed" only covers
    # however much of the day has been polled so far -- guaranteeing a
    # fake deficit on every route for reasons that have nothing to do with
    # real service loss. Skip no_show scoring entirely for that first day;
    # it activates automatically starting the next service day, once
    # seen_trips.json has been built up from midnight by the poller itself.
    if date != first_run_date:
        for route_id, trips in scheduled_by_route.items():
            trips = sorted(trips, key=lambda t: t["start_s"])
            closed = [t for t in trips if t["end_s"] + NO_SHOW_GRACE_S < now_s]
            observed_count = len(observed_by_route.get(route_id, []))
            if observed_count >= len(closed):
                continue
            for info in closed[observed_count:]:
                add({
                    "id": f"no_show:{route_id}:{info['start_s']}", "type": "no_show", "trip_id": None,
                    "route_id": route_id, "route_short_name": route_name(routes, route_id),
                    "scheduled_start_s": info["start_s"], "scheduled_end_s": info["end_s"],
                    "detected_at": now_iso(), "resolved_at": None,
                    "delay_seconds": None, "alerted": False,
                })

    # route_gap: routes that should have coverage right now but don't
    routes_active_now = {t["route_id"] for t in scheduled_today.values() if t["start_s"] <= now_s <= t["end_s"]}
    open_gaps = {e["route_id"]: e for e in day["events"] if e["type"] == "route_gap" and e["resolved_at"] is None}
    resolved_events = []

    for route_id in routes_active_now:
        last_seen = route_activity.get(route_id)
        last_seen_age = (now_dt - datetime.fromisoformat(last_seen)).total_seconds() if last_seen else None
        gapped = last_seen_age is None or last_seen_age > ROUTE_GAP_S
        if gapped and route_id not in open_gaps:
            add({
                "id": f"route_gap:{route_id}:{now_iso()}", "type": "route_gap",
                "trip_id": None, "route_id": route_id, "route_short_name": route_name(routes, route_id),
                "detected_at": now_iso(), "resolved_at": None,
                "delay_seconds": None, "alerted": False, "alerted_resolved": False,
            })
        elif not gapped and route_id in open_gaps:
            open_gaps[route_id]["resolved_at"] = now_iso()
            resolved_events.append(open_gaps[route_id])

    # a route that no longer has any scheduled coverage at all (service day
    # winding down) but still has an open gap should also close out, rather
    # than staying "open" forever once nothing is scheduled on it anymore
    for route_id, ev in open_gaps.items():
        if route_id not in routes_active_now and ev["resolved_at"] is None:
            ev["resolved_at"] = now_iso()
            resolved_events.append(ev)

    save_json(path, day)
    return day, new_events, resolved_events


def send_webhook(text):
    if not WEBHOOK_URL:
        return
    try:
        requests.post(WEBHOOK_URL, json={"text": text}, timeout=10).raise_for_status()
    except Exception as exc:
        print(f"Warning: webhook post failed ({exc})")


def hms_label(seconds):
    h, m = int(seconds) // 3600, (int(seconds) // 60) % 60
    return f"{h:02d}:{m:02d}"


def describe(event):
    route = event["route_short_name"]
    vehicle_suffix = f" (bus {event['vehicle_id']})" if event.get("vehicle_id") else ""
    if event["type"] == "canceled":
        return f":no_entry: Route {route} trip canceled{vehicle_suffix}"
    if event["type"] == "no_show":
        window = hms_label(event["scheduled_start_s"])
        return f":ghost: Route {route}'s trip scheduled around {window} likely never ran (fewer trips observed than scheduled)"
    if event["type"] == "severe_delay":
        mins = round(event["delay_seconds"] / 60)
        return f":turtle: Route {route} trip running {mins} min late{vehicle_suffix}"
    if event["type"] == "route_gap":
        return f":warning: Route {route} has had no vehicle activity for over {ROUTE_GAP_S // 60} min despite scheduled service"
    return f"Service loss event: {event}"


def alert_and_mark(day, new_events, resolved_events, date):
    path = EVENTS_DIR / f"{date}.json"
    changed = False
    for event in new_events:
        send_webhook(describe(event))
        event["alerted"] = True
        changed = True
    for event in resolved_events:
        route = event["route_short_name"]
        send_webhook(f":white_check_mark: Route {route} coverage gap resolved")
        event["alerted_resolved"] = True
        changed = True
    if changed:
        save_json(path, day)


def write_events_index():
    dates = sorted(p.stem for p in EVENTS_DIR.glob("*.json") if p.stem != "index")
    save_json(EVENTS_DIR / "index.json", {"dates": dates})


def write_active_events(date, day):
    open_gaps = [e for e in day["events"] if e["type"] == "route_gap" and e["resolved_at"] is None]
    save_json(ACTIVE_EVENTS_FILE, {
        "updated_at": now_iso(),
        "date": date,
        "open_route_gaps": open_gaps,
        "today_events": day["events"],
    })


def main():
    status = load_json(STATUS_FILE, {"last_polled": None, "last_error": None, "polls_run": 0})
    if "first_run_date" not in status:
        status["first_run_date"] = now_service_dt().date().isoformat()

    try:
        schedule, routes = ensure_static_data()
        tu_feed = fetch_feed(TRIP_UPDATES_URL)
        vp_feed = fetch_feed(VEHICLE_POSITIONS_URL)

        now_dt = now_service_dt()
        date = now_dt.date().isoformat()
        now_s = seconds_since_service_midnight(now_dt)

        seen, route_activity = poll_seen(tu_feed, vp_feed, date)
        day, new_events, resolved_events = detect_events(
            date, seen, route_activity, schedule, routes, now_dt, now_s, status["first_run_date"])
        alert_and_mark(day, new_events, resolved_events, date)
        write_events_index()
        write_active_events(date, day)

        status["last_error"] = None
        status["events_today"] = len(day["events"])
        status["open_route_gaps"] = sum(1 for e in day["events"] if e["type"] == "route_gap" and e["resolved_at"] is None)
        status["new_events_this_poll"] = len(new_events)
        print(f"Polled OK: {len(day['events'])} event(s) today, "
              f"{status['open_route_gaps']} open route gap(s), "
              f"{len(new_events)} new this poll.")
    except Exception as exc:
        status["last_error"] = f"{now_iso()}: {exc}"
        status["last_polled"] = now_iso()
        status["polls_run"] += 1
        save_json(STATUS_FILE, status)
        raise

    status["last_polled"] = now_iso()
    status["polls_run"] += 1
    save_json(STATUS_FILE, status)


if __name__ == "__main__":
    main()
