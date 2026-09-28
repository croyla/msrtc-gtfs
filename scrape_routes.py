"""
Builds msrtc-routes.json (route code -> ordered stop_id skeleton) and
msrtc-trips.json (trip id -> route code) from mobileapi.msrtcvtspis.com.
Second step of the new-API GTFS-generation endeavor, after scrape_stops.py.
Timings are NOT collected here -- this is purely stop-sequence/route-code/
trip-id skeleton discovery; a later step layers schedule times on top.

Three endpoints, chained:

1. getCurrentTripStatusAtStop.json (POST, {"stopId": ...}) -- run once per
   stop in msrtc-stops.geojson (every stop scrape_stops.py already found is
   either termini-matched or a nearby-sweep result, so this covers both).
   Seeds an initial set of route codes (+ trip ids) from whatever is
   currently passing each stop. Confirmed live: this only looks ~24h
   FORWARD from query time, not a full calendar day -- so it's cached per
   WEEKDAY with a smart-refresh rule (see _weekday_cache_is_stale()): a
   cache entry expires after WEEKDAY_CACHE_TTL_DAYS, OR earlier if a fresh
   query for that weekday happens at an earlier time-of-day than the cached
   one (that query sees strictly more of the day, so it's worth overwriting
   even if not calendar-stale yet).

2. getStopByRoute (POST, {"routeCode": ...}) -- gives the route's full
   ordered stop sequence (stopSequence field). This alone only covers
   routes already seeded from step 1's stops, which undercounts: a route
   that never passes any of our seed stops (or does, but wasn't "current"
   at scrape time) is invisible so far. This one's response is NOT
   weekday-partitioned -- route topology (which stops a route visits)
   doesn't vary by day, unlike steps 1 and 3.

3. suggestTripsBetweenStopsV2 (POST, {"startStopId":..,"endStopId":..}) --
   run once per CONSECUTIVE stop pair in every route discovered so far.
   Confirmed live: this returns every trip (any route) currently crossing
   that specific stop pair, not just trips for the route that pair came
   from (one segment returned 169 trips across dozens of distinct routes).
   That surfaces route codes step 1 never seeded, which get queued back
   into step 2 -- so this is a BFS over "routes", not a flat crawl: each
   round fetches stop sequences for the current frontier, queries every
   resulting stop pair, and any route codes seen for the first time become
   next round's frontier. Continues until a round finds nothing new. Like
   step 1, --discover's own pair queries are cached per WEEKDAY with the
   same TTL/early-time-of-day refresh rule (raw/trips_between_stops/
   <weekday>/<a>_<b>.json) -- running --discover again on a different
   weekday queries for real rather than silently reusing whatever weekday
   got crawled first. (--requery's pairs are separate: see
   RAW_TRIPS_BETWEEN_DAILY_DIR, an exact-date, no-TTL cache purpose-built
   for "give me a fresh read right now.")

   Confirmed live on a full crawl: 18,542 routes needed 120,695 pair
   queries to discover via this BFS. But most of that is redundant for
   COVERAGE purposes -- a handful of busy junction-to-junction pairs each
   return 100+ distinct routes at once, so the same 18,431 pair-discoverable
   routes can be covered by a much smaller pair set (see --min-cover below).
   The route *network* (which stops each route visits) doesn't change
   day-to-day, but which trips/routes are actually running does -- so once
   a minimal cover is known, --requery re-queries just that small pair set
   on a new day to see what's currently running, instead of repeating the
   full BFS (which mostly just re-derives the same route-to-stop-sequence
   topology already on disk). --min-cover itself is CUMULATIVE across every
   weekday --discover has ever crawled (see _load_pair_routes()) -- a route
   only visible via a Tuesday pair response and never a Sunday one still
   ends up covered, since each pair's route-set is unioned across whatever
   weekday buckets exist before the cover is computed.

Usage:
  python3 scrape_routes.py --discover          # full seed + BFS expansion for TODAY's weekday
  python3 scrape_routes.py --build             # aggregate raw/ into msrtc-routes.json + msrtc-trips.json
  python3 scrape_routes.py --min-cover          # compute a cumulative (all-weekdays) minimal pair cover
  python3 scrape_routes.py --requery [--date D] # re-run seed + minimal-cover pairs only, for a specific day
"""

import argparse
import datetime as dt
import heapq
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from scrape_stops import BASE, HEADERS, DEFAULT_WORKERS, _request_with_retry

# Both getCurrentTripStatusAtStop AND suggestTripsBetweenStopsV2 (when used
# by --discover's BFS, NOT --requery -- see RAW_TRIPS_BETWEEN_DAILY_DIR
# below) are cached per WEEKDAY (not per exact date):
# raw/trip_status/<monday|tuesday|...>/<stop_id>.json,
# raw/trips_between_stops/<monday|tuesday|...>/<a>_<b>.json. MSRTC schedules
# repeat weekly, so a given weekday's cache is a reasonable stand-in for any
# future occurrence of the same weekday -- but "reasonable stand-in" decays
# over time (seasonal/holiday schedule changes), hence the TTL below: a
# cached file older than WEEKDAY_CACHE_TTL_DAYS is treated as stale and
# refetched rather than trusted forever. Critically, this also means running
# --discover again on a DIFFERENT weekday doesn't just cache-hit on
# whatever's already on disk for a totally different day -- each weekday
# gets queried for real (a Monday --discover cannot silently reuse Sunday's
# answers, since they live in different directories entirely).
WEEKDAY_CACHE_TTL_DAYS = 90  # ~3 months

RAW_TRIP_STATUS_DIR = "raw/trip_status"
RAW_STOP_BY_ROUTE_DIR = "raw/stop_by_route"
RAW_TRIPS_BETWEEN_DIR = "raw/trips_between_stops"

# --requery's pair queries write here instead, date-partitioned (an actual
# calendar date, not just a weekday bucket) -- this is the part that varies
# hour-to-hour, not just week-to-week, and no TTL/reuse makes sense for it:
# the whole point of --requery is a fresh read of what's running right now.
RAW_TRIPS_BETWEEN_DAILY_DIR = "raw/trips_between_stops_daily"

STOPS_GEOJSON = "msrtc-stops.geojson"
OUT_ROUTES_JSON = "msrtc-routes.json"
OUT_TRIPS_JSON = "msrtc-trips.json"
OUT_TRIP_TIMES_JSON = "msrtc-trip-times.json"
OUT_PAIR_COVER_JSON = "msrtc-pair-cover.json"
OUT_SOURCE_STOPS_JSON = "msrtc-source-stops.json"

JSON_HEADERS = {**HEADERS, "Content-Type": "application/json"}


def get_current_trip_status(stop_id):
    return _request_with_retry(
        "POST", f"{BASE}/app/travel/getCurrentTripStatusAtStop.json",
        headers=JSON_HEADERS, json={"stopId": stop_id},
    )


def get_stop_by_route(route_code):
    return _request_with_retry(
        "POST", f"{BASE}/stop/getStopByRoute",
        headers=JSON_HEADERS, json={"routeCode": route_code},
    )


def suggest_trips_between_stops(start_stop_id, end_stop_id):
    return _request_with_retry(
        "POST", f"{BASE}/trip/suggestTripsBetweenStopsV2",
        headers=JSON_HEADERS, json={"startStopId": start_stop_id, "endStopId": end_stop_id},
    )


def _cached(path, fetch_fn, is_stale=None):
    """is_stale(path) -> bool decides whether an existing file is trusted;
    None means "never stale" (the permanent cache used by getStopByRoute /
    suggestTripsBetweenStopsV2, where a response doesn't go stale on its
    own)."""
    if os.path.exists(path) and not (is_stale and is_stale(path)):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    data = fetch_fn()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Atomic write (temp file + rename): a killed/overlapping run left a
    # truncated 0-byte cache file behind once already (in-place open("w")
    # is visible-and-empty for a window before content lands), which then
    # poisoned every subsequent read of that path with a JSONDecodeError.
    tmp = f"{path}.tmp.{os.getpid()}"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)
    return data


def _weekday_label(date=None):
    """'monday'..'sunday' for the given date (ISO string or datetime.date),
    or today's if not given."""
    if isinstance(date, str):
        date = dt.date.fromisoformat(date)
    return (date or dt.date.today()).strftime("%A").lower()


def _weekday_cache_is_stale(now):
    """Shared staleness rule for anything cached per-WEEKDAY (both
    getCurrentTripStatusAtStop and, in --discover's BFS, also
    suggestTripsBetweenStopsV2 -- confirmed live both only look ~24h FORWARD
    from query time, not a full-day snapshot, so a cache entry fetched at
    3pm never saw that weekday's midnight-3pm trips, and querying again at
    3pm next occurrence of that weekday won't fix that; only a query made
    EARLIER in the day than the cached fetch covers more of that weekday's
    early hours). So besides the plain WEEKDAY_CACHE_TTL_DAYS calendar
    expiry, a fresh query whose time-of-day is earlier than the cached
    file's time-of-day should overwrite it even though it isn't
    calendar-stale yet -- it's strictly more informative."""
    def check(path):
        mtime = os.path.getmtime(path)
        age_days = (now.timestamp() - mtime) / 86400
        if age_days > WEEKDAY_CACHE_TTL_DAYS:
            return True
        cached_time_of_day = dt.datetime.fromtimestamp(mtime).time()
        return now.time() < cached_time_of_day
    return check


def _cached_trip_status(stop_id, weekday=None, now=None):
    weekday = weekday or _weekday_label()
    now = now or dt.datetime.now()
    path = f"{RAW_TRIP_STATUS_DIR}/{weekday}/{stop_id}.json"
    return _cached(path, lambda: get_current_trip_status(stop_id), is_stale=_weekday_cache_is_stale(now))


def _cached_stop_by_route(route_code):
    return _cached(f"{RAW_STOP_BY_ROUTE_DIR}/{route_code}.json", lambda: get_stop_by_route(route_code))


def _cached_trips_between(start_id, end_id, cache_dir, is_stale=None):
    return _cached(f"{cache_dir}/{start_id}_{end_id}.json",
                    lambda: suggest_trips_between_stops(start_id, end_id), is_stale=is_stale)


def _route_stop_ids(data):
    stops = sorted(data.get("data", []) or [], key=lambda s: s.get("stopSequence", 0))
    return [s["stopId"] for s in stops]


def _trips_from_between_response(data):
    """Yields (trip_id, route_code, next_day, is_source_stop, arrival_time)
    tuples -- sortAvailableTrip is keyed by a group number (only ever seen
    "1" live) whose value is the flat trip list. next_day is the response's
    own explicit nextDay flag (confirmed live: some entries do carry
    nextDay=true) -- unlike getCurrentTripStatusAtStop, no inference needed
    here. is_source_stop: true when this pair's fromStop is the trip's
    actual route origin. NOT taken from the response's own isSourceStop
    field -- confirmed live that field is False on all 922,527 entries in a
    full crawl's cache, including ones where fromStop textually equals
    sourceStop, so it's unreliable/unrelated to what its name suggests.
    Derived instead by comparing fromStop's name to sourceStop's name.
    arrival_time is the raw "HH:MM:SS" string (None if absent) -- when
    is_source_stop is true this is the trip's origin departure time."""
    for group in (data.get("sortAvailableTrip") or {}).values():
        for trip in group:
            tid, route = trip.get("tripNo"), trip.get("routeName")
            if tid and route:
                is_source = trip.get("fromStop") is not None and trip.get("fromStop") == trip.get("sourceStop")
                yield tid, route, bool(trip.get("nextDay")), is_source, trip.get("arrivalTime")


def _seed_one_stop(stop_id, weekday=None):
    try:
        data = _cached_trip_status(stop_id, weekday)
    except requests.exceptions.RequestException as e:
        print(f"  stop {stop_id}: FAILED after retries ({e.__class__.__name__}), skipping")
        return set(), {}
    routes, trips = set(), {}
    for entry in data.get("data", []) or []:
        route, tid = entry.get("routeCode"), entry.get("tripCode")
        if route:
            routes.add(route)
            if tid:
                trips[tid] = route
    return routes, trips


def _fetch_route_stops(route_code):
    try:
        data = _cached_stop_by_route(route_code)
    except requests.exceptions.RequestException as e:
        print(f"  route {route_code}: FAILED after retries ({e.__class__.__name__}), skipping")
        return None
    return _route_stop_ids(data)


def _fetch_pair_trips(pair, cache_dir, is_stale=None):
    a, b = pair
    try:
        data = _cached_trips_between(a, b, cache_dir, is_stale)
    except requests.exceptions.RequestException as e:
        print(f"  pair {a}->{b}: FAILED after retries ({e.__class__.__name__}), skipping")
        return {}
    return {tid: route for tid, route, _, _, _ in _trips_from_between_response(data)}


def _load_seed_stop_ids():
    """Union of msrtc-stops.geojson (the original termini/nearby-sweep set)
    and msrtc-source-stops.json (confirmed trip origins, see
    build_source_stops_json()) -- the latter is additive, not a
    replacement: it doesn't exist yet on a first --discover run (nothing to
    derive it from), and even once built it's only as complete as whatever
    origins have been observed so far, so the original set stays the
    floor."""
    stop_ids = set()
    with open(STOPS_GEOJSON, encoding="utf-8") as f:
        geo = json.load(f)
    stop_ids.update(feat["properties"]["stop_id"] for feat in geo["features"])
    if os.path.exists(OUT_SOURCE_STOPS_JSON):
        with open(OUT_SOURCE_STOPS_JSON, encoding="utf-8") as f:
            source = json.load(f)
        stop_ids.update(s["stop_id"] for s in source["stops"])
    return sorted(stop_ids)


def _load_requery_seed_stop_ids():
    """--requery seeds from msrtc-source-stops.json ALONE, not the full
    43,329-stop geojson set: this is the whole point of --min-cover's
    "smart re-query" pattern applied to the seeding side too -- 85% of the
    full stop set has never shown firstStop=true for anything (see
    build_source_stops_json()), so querying it on every requery is mostly
    wasted budget. Falls back to the full geojson set if
    msrtc-source-stops.json doesn't exist yet (--build hasn't been run
    since a --discover), since an empty/tiny seed set would silently
    undercount routes rather than erroring loudly."""
    if not os.path.exists(OUT_SOURCE_STOPS_JSON):
        print(f"  note: {OUT_SOURCE_STOPS_JSON} not found -- falling back to the full geojson stop set "
              f"for seeding (run --build after --discover to get the smaller targeted set)")
        return _load_seed_stop_ids()
    with open(OUT_SOURCE_STOPS_JSON, encoding="utf-8") as f:
        source = json.load(f)
    return sorted(s["stop_id"] for s in source["stops"])


def run_route_discovery(workers=DEFAULT_WORKERS):
    stop_ids = _load_seed_stop_ids()
    weekday = _weekday_label()
    now = dt.datetime.now()
    pair_cache_dir = f"{RAW_TRIPS_BETWEEN_DIR}/{weekday}"
    is_stale = _weekday_cache_is_stale(now)

    print(f"Seeding route codes from {len(stop_ids)} stops ({workers} parallel workers, weekday={weekday})")
    seen_routes, trip_ids = set(), {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(_seed_one_stop, sid, weekday) for sid in stop_ids]
        for i, fut in enumerate(as_completed(futures), 1):
            routes, trips = fut.result()
            seen_routes |= routes
            trip_ids.update(trips)
            if i % 500 == 0 or i == len(stop_ids):
                print(f"[{i}/{len(stop_ids)} stops done, {len(seen_routes)} route codes seeded so far]")

    frontier = set(seen_routes)
    round_num = 0
    while frontier:
        round_num += 1
        print(f"Round {round_num}: expanding {len(frontier)} route(s)")

        route_stops = {}
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {ex.submit(_fetch_route_stops, r): r for r in frontier}
            for fut in as_completed(futures):
                r = futures[fut]
                sids = fut.result()
                if sids:
                    route_stops[r] = sids

        pairs = {(a, b) for sids in route_stops.values() for a, b in zip(sids, sids[1:])}
        print(f"  {len(pairs)} unique stop pair(s) to query")

        next_frontier = set()
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = [ex.submit(_fetch_pair_trips, p, pair_cache_dir, is_stale) for p in pairs]
            for fut in as_completed(futures):
                for tid, route in fut.result().items():
                    trip_ids[tid] = route
                    if route not in seen_routes:
                        seen_routes.add(route)
                        next_frontier.add(route)

        print(f"  discovered {len(next_frontier)} new route(s), {len(trip_ids)} total trip(s) so far")
        frontier = next_frontier

    print(f"Done: {len(seen_routes)} route(s), {len(trip_ids)} trip(s)")


def build_routes_json():
    routes = {}
    if os.path.isdir(RAW_STOP_BY_ROUTE_DIR):
        for fname in os.listdir(RAW_STOP_BY_ROUTE_DIR):
            if not fname.endswith(".json"):
                continue
            with open(f"{RAW_STOP_BY_ROUTE_DIR}/{fname}", encoding="utf-8") as f:
                data = json.load(f)
            route_code = fname[:-5]
            sids = _route_stop_ids(data)
            if sids:
                routes[route_code] = sids
    with open(OUT_ROUTES_JSON, "w", encoding="utf-8") as f:
        json.dump(routes, f, indent=2, ensure_ascii=False)
    print(f"Wrote {OUT_ROUTES_JSON}: {len(routes)} route(s)")


WEEKDAY_ORDER = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]


def _next_weekday(weekday):
    return WEEKDAY_ORDER[(WEEKDAY_ORDER.index(weekday) + 1) % 7]


def build_trips_json():
    """Neither endpoint returns an explicit "which weekdays does this trip
    run" field (checked live -- getCurrentTripStatusAtStop and
    suggestTripsBetweenStopsV2 entries carry no such field), so weekday
    pattern is built empirically: every raw/ source below is now tagged
    with the weekday it was actually observed on (that's the whole point of
    the weekday/date-partitioned caches), so a trip's pattern is the union
    of weekdays its ORIGIN DEPARTURE has been seen on. This only grows more
    accurate with more --requery runs across different weekdays -- a trip
    seen so far only on "sunday" isn't necessarily Sunday-only, just
    not-yet-observed on other days.

    Deliberately origin-only, not "any segment this trip was seen crossing":
    a single physical trip can legitimately straddle midnight (its own
    interior segments span both today and tomorrow), which is correct for
    that trip's own runtime but says nothing about which weekday(s) it's
    SCHEDULED on -- that's determined once, by when it departs its own
    origin stop. Using every segment's day would tag a single Sunday-night
    departure with ["monday", "sunday"] purely because the trip was still
    en route after midnight, which is a run-time artifact, not a schedule
    fact. getCurrentTripStatusAtStop marks this via firstStop=true;
    suggestTripsBetweenStopsV2 via isSourceStop=true on the pair whose
    fromStop is the trip's actual origin. Non-origin entries are ignored
    entirely for weekday purposes (they're still used for other things, e.g.
    build_trip_times_json's stop-to-stop durations).

    A query made on weekday W can also see into part of W+1 -- both
    endpoints only look ~24h FORWARD from query time, so a query fetched
    Sunday afternoon includes an origin departure that's actually early
    Monday. suggestTripsBetweenStopsV2 says so explicitly via its nextDay
    flag. getCurrentTripStatusAtStop doesn't, so it's inferred: if the
    origin's departureTime-of-day is EARLIER than the query's own
    time-of-day (the file's mtime -- the same signal _weekday_cache_is_stale()
    uses), the departure must be tomorrow, since the window only looks
    forward."""
    trip_weekdays = {}  # trip_id -> {"route_code": str, "weekdays": set(str), "departure_time": str}

    def record(tid, route, weekday, departure_time=None):
        if not (tid and route):
            return
        entry = trip_weekdays.setdefault(tid, {"route_code": route, "weekdays": set(), "departure_time": None})
        entry["route_code"] = route
        entry["weekdays"].add(weekday)
        # departure_time is a schedule fact, not a per-weekday-observation one --
        # any origin sighting's clock time should agree (up to live-tracking
        # noise), so the first one seen is kept rather than overwritten by
        # later, possibly-noisier observations.
        if departure_time and not entry["departure_time"]:
            entry["departure_time"] = departure_time

    if os.path.isdir(RAW_TRIP_STATUS_DIR):
        # raw/trip_status/<weekday>/<stop_id>.json -- folds in every weekday
        # bucket present, not just today's.
        for weekday in os.listdir(RAW_TRIP_STATUS_DIR):
            weekday_dir = f"{RAW_TRIP_STATUS_DIR}/{weekday}"
            if not os.path.isdir(weekday_dir):
                continue
            for fname in os.listdir(weekday_dir):
                if not fname.endswith(".json"):
                    continue
                path = f"{weekday_dir}/{fname}"
                query_time_of_day = dt.datetime.fromtimestamp(os.path.getmtime(path)).time()
                with open(path, encoding="utf-8") as f:
                    data = json.load(f)
                for entry in data.get("data", []) or []:
                    if not entry.get("firstStop"):
                        continue
                    tid, route = entry.get("tripCode"), entry.get("routeCode")
                    trip_weekday = weekday
                    departure = entry.get("departureTime")
                    if departure:
                        try:
                            departure_tod = dt.datetime.strptime(departure, "%H:%M:%S").time()
                        except ValueError:
                            departure_tod = None
                        if departure_tod is not None and departure_tod < query_time_of_day:
                            trip_weekday = _next_weekday(weekday)
                    record(tid, route, trip_weekday, departure)

    if os.path.isdir(RAW_TRIPS_BETWEEN_DIR):
        # raw/trips_between_stops/<weekday>/<a>_<b>.json -- the weekday is
        # exact (the directory name), not inferred from mtime.
        for weekday in os.listdir(RAW_TRIPS_BETWEEN_DIR):
            weekday_dir = f"{RAW_TRIPS_BETWEEN_DIR}/{weekday}"
            if not os.path.isdir(weekday_dir):
                continue
            for fname in os.listdir(weekday_dir):
                if not fname.endswith(".json"):
                    continue
                with open(f"{weekday_dir}/{fname}", encoding="utf-8") as f:
                    data = json.load(f)
                for tid, route, next_day, is_source, arrival in _trips_from_between_response(data):
                    if is_source:
                        record(tid, route, _next_weekday(weekday) if next_day else weekday, arrival)

    if os.path.isdir(RAW_TRIPS_BETWEEN_DAILY_DIR):
        # raw/trips_between_stops_daily/<date>/*.json -- --requery snapshots,
        # explicitly date-labeled, so the base weekday is exact rather than inferred.
        for date_str in os.listdir(RAW_TRIPS_BETWEEN_DAILY_DIR):
            date_dir = f"{RAW_TRIPS_BETWEEN_DAILY_DIR}/{date_str}"
            if not os.path.isdir(date_dir):
                continue
            weekday = _weekday_label(date_str)
            for fname in os.listdir(date_dir):
                if not fname.endswith(".json"):
                    continue
                with open(f"{date_dir}/{fname}", encoding="utf-8") as f:
                    data = json.load(f)
                for tid, route, next_day, is_source, arrival in _trips_from_between_response(data):
                    if is_source:
                        record(tid, route, _next_weekday(weekday) if next_day else weekday, arrival)

    trips_out = {
        tid: {"route_code": v["route_code"],
              "weekdays": sorted(v["weekdays"], key=WEEKDAY_ORDER.index),
              "departure_time": v["departure_time"]}
        for tid, v in trip_weekdays.items()
    }
    with open(OUT_TRIPS_JSON, "w", encoding="utf-8") as f:
        json.dump(trips_out, f, indent=2, ensure_ascii=False)
    no_time = sum(1 for v in trips_out.values() if not v["departure_time"])
    print(f"Wrote {OUT_TRIPS_JSON}: {len(trips_out)} trip(s) ({no_time} without a captured departure time)")


def _load_pair_times_from_file(path):
    """trip_id -> (route_code, arrival datetime.time) from one cached
    suggestTripsBetweenStopsV2 response."""
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    out = {}
    for group in (data.get("sortAvailableTrip") or {}).values():
        for trip in group:
            tid, route, at = trip.get("tripNo"), trip.get("routeName"), trip.get("arrivalTime")
            if not (tid and route and at):
                continue
            try:
                out[tid] = (route, dt.datetime.strptime(at, "%H:%M:%S").time())
            except ValueError:
                continue
    return out


def _load_pair_times(a, b):
    """trip_id -> (route_code, arrival datetime.time) for stop pair (a, b),
    CUMULATIVE across every weekday bucket -- more weekday observations of
    the same pair just means more trip_id samples to compute a duration
    median from in build_trip_times_json(), which only makes the result
    more robust, not weekday-specific (duration is a property of the road,
    not the day)."""
    out = {}
    if not os.path.isdir(RAW_TRIPS_BETWEEN_DIR):
        return out
    for weekday in os.listdir(RAW_TRIPS_BETWEEN_DIR):
        weekday_dir = f"{RAW_TRIPS_BETWEEN_DIR}/{weekday}"
        if not os.path.isdir(weekday_dir):
            continue
        out.update(_load_pair_times_from_file(f"{weekday_dir}/{a}_{b}.json"))
    return out


def build_trip_times_json():
    """Segment (stop[i] -> stop[i+1]) durations, keyed by ROUTE_CODE --
    confirmed empirically (see conversation/analysis, not repeated here as a
    live check): different trips of the SAME route show near-identical
    durations for a given segment (median ~2% relative spread, mostly under
    60s -- real-time GPS noise, not a real schedule difference), so storing
    per-trip would just be redundant. Different ROUTES crossing the same
    physical stop pair CAN differ meaningfully (different vehicle/stopping
    patterns), which is why this isn't a flat stop-pair -> duration map
    either -- it's keyed by route_code, one duration list per route aligned
    to msrtc-routes.json's own stop order.

    Derivation: for pair (X, Y), suggestTripsBetweenStopsV2's reported time
    is the time AT X, the pair's fromStop -- confirmed live via
    isSourceStop entries, where X is the trip's actual route origin and the
    reported time can only sensibly be read as a DEPARTURE from X (there's
    no "arrival" at your own origin). So for a route's own consecutive stop
    sequence s0..s(k-1), pair j = (s_j, s_{j+1})'s time is "time at s_j",
    and duration(s_j -> s_{j+1}) = (pair j+1's time at s_{j+1}) minus (pair
    j's time at s_j), for the SAME trip_id appearing in both. This needs
    pair j+1 to exist, so it only covers segments 0..k-3 -- the LAST
    segment (s_{k-2} -> s_{k-1}) has no "time at s_{k-1}" available from any
    pair in this route's own sequence and is left null, same as any segment
    where no same-route trip_id happened to be observed on both
    neighboring pairs."""
    if not os.path.exists(OUT_ROUTES_JSON):
        raise SystemExit(f"{OUT_ROUTES_JSON} not found -- run --build first")
    with open(OUT_ROUTES_JSON, encoding="utf-8") as f:
        routes = json.load(f)

    out = {}
    total_segments = computed_segments = 0
    for route_code, stops in routes.items():
        if len(stops) < 3:
            out[route_code] = [None] * max(len(stops) - 1, 0)
            continue

        pair_times = [_load_pair_times(a, b) for a, b in zip(stops, stops[1:])]
        durations = []
        for j in range(len(pair_times) - 1):
            total_segments += 1
            t_j, t_j1 = pair_times[j], pair_times[j + 1]
            same_route_durations = []
            for tid, (route, time_j) in t_j.items():
                if route != route_code:
                    continue
                entry_j1 = t_j1.get(tid)
                if not entry_j1:
                    continue
                _, time_j1 = entry_j1
                delta = (dt.datetime.combine(dt.date.min, time_j1)
                         - dt.datetime.combine(dt.date.min, time_j)).total_seconds()
                if 0 < delta < 7200:  # sanity bound -- no single segment realistically takes >2h
                    same_route_durations.append(delta)
            if same_route_durations:
                same_route_durations.sort()
                median = same_route_durations[len(same_route_durations) // 2]
                durations.append({"duration_seconds": round(median), "sample_size": len(same_route_durations)})
                computed_segments += 1
            else:
                durations.append(None)
        durations.append(None)  # last segment -- not computable, see docstring
        out[route_code] = durations

    with open(OUT_TRIP_TIMES_JSON, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    print(f"Wrote {OUT_TRIP_TIMES_JSON}: {len(out)} route(s), "
          f"{computed_segments}/{total_segments} interior segment(s) with a computed duration "
          f"(first/last edge cases and segments with no same-route trip observed on both neighboring "
          f"pairs are left null)")


def build_source_stops_json():
    """Confirmed live: the current stop-seeding set (msrtc-stops.geojson,
    used by _seed_one_stop / getCurrentTripStatusAtStop) was built from
    termini-matching and a geocoded nearby-sweep -- neither criterion has
    anything to do with where trips actually ORIGINATE. Checked against the
    trip-origin signals already computed for build_trips_json() (firstStop
    in trip_status, fromStop==sourceStop in trips_between): of 4,374
    confirmed origin stops, 3,741 (85%) were never in the 4,934-stop seed
    set at all -- most seed queries land on stops that will never show
    firstStop=true for anything, which is wasted seeding effort. This
    builds a dedicated list of confirmed origin stops (with name/lat/lon
    looked up from msrtc-stops.geojson) meant to SUPPLEMENT, not replace,
    the existing seed set -- --requery's stop-seeding phase can be pointed
    at the union of both so it starts spending queries where trips are
    actually known to start."""
    stops_by_id = {}
    if os.path.exists(STOPS_GEOJSON):
        with open(STOPS_GEOJSON, encoding="utf-8") as f:
            geo = json.load(f)
        for feat in geo["features"]:
            p = feat["properties"]
            lon, lat = feat["geometry"]["coordinates"]
            stops_by_id[p["stop_id"]] = {"stop_name": p.get("stop_name"), "stop_lat": lat, "stop_lon": lon}

    origin_ids = set()

    if os.path.isdir(RAW_TRIPS_BETWEEN_DIR):
        for weekday in os.listdir(RAW_TRIPS_BETWEEN_DIR):
            weekday_dir = f"{RAW_TRIPS_BETWEEN_DIR}/{weekday}"
            if not os.path.isdir(weekday_dir):
                continue
            for fname in os.listdir(weekday_dir):
                if not fname.endswith(".json"):
                    continue
                a = int(fname[:-5].split("_", 1)[0])
                with open(f"{weekday_dir}/{fname}", encoding="utf-8") as f:
                    data = json.load(f)
                for _, _, _, is_source, _ in _trips_from_between_response(data):
                    if is_source:
                        origin_ids.add(a)
                        break  # one confirmed origin entry is enough for this pair-file

    if os.path.isdir(RAW_TRIP_STATUS_DIR):
        for weekday in os.listdir(RAW_TRIP_STATUS_DIR):
            weekday_dir = f"{RAW_TRIP_STATUS_DIR}/{weekday}"
            if not os.path.isdir(weekday_dir):
                continue
            for fname in os.listdir(weekday_dir):
                if not fname.endswith(".json"):
                    continue
                stop_id = int(fname[:-5])
                with open(f"{weekday_dir}/{fname}", encoding="utf-8") as f:
                    data = json.load(f)
                if any(e.get("firstStop") for e in data.get("data", []) or []):
                    origin_ids.add(stop_id)

    features = []
    for sid in sorted(origin_ids):
        p = stops_by_id.get(sid)
        features.append({
            "stop_id": sid,
            "stop_name": p.get("stop_name") if p else None,
            "stop_lat": p.get("stop_lat") if p else None,
            "stop_lon": p.get("stop_lon") if p else None,
        })

    with open(OUT_SOURCE_STOPS_JSON, "w", encoding="utf-8") as f:
        json.dump({"stop_count": len(features), "stops": features}, f, indent=2, ensure_ascii=False)
    print(f"Wrote {OUT_SOURCE_STOPS_JSON}: {len(features)} confirmed trip-origin stop(s)")


def _load_pair_routes():
    """pair_key ("start_end") -> set of route codes seen crossing it,
    CUMULATIVE across every weekday bucket under raw/trips_between_stops/
    (i.e. the union of whatever's been observed on Sunday, Monday, etc. so
    far) -- this is what makes --min-cover a cumulative, all-weekdays cover
    rather than a snapshot of whichever single day --discover last ran on:
    a route that only shows up on a pair's Tuesday response and never its
    Sunday one still ends up covered, because the two days' route-sets are
    unioned per pair before the cover is computed."""
    pair_routes = {}
    if not os.path.isdir(RAW_TRIPS_BETWEEN_DIR):
        return pair_routes
    for weekday in os.listdir(RAW_TRIPS_BETWEEN_DIR):
        weekday_dir = f"{RAW_TRIPS_BETWEEN_DIR}/{weekday}"
        if not os.path.isdir(weekday_dir):
            continue
        for fname in os.listdir(weekday_dir):
            if not fname.endswith(".json"):
                continue
            with open(f"{weekday_dir}/{fname}", encoding="utf-8") as f:
                data = json.load(f)
            routes = {route for _, route, _, _, _ in _trips_from_between_response(data)}
            if routes:
                pair_routes.setdefault(fname[:-5], set()).update(routes)
    return pair_routes


def compute_min_pair_cover(pair_routes):
    """Lazy-greedy weighted set cover: repeatedly pick the pair covering the
    most still-uncovered routes. Set cover is NP-hard in general, but greedy
    is the standard ln(n)-approximation, and it does dramatically well here
    in practice -- confirmed live: a handful of busy junction-to-junction
    pairs return 100-225 distinct routes each (a single stop pair returned
    225), so the 105,675 non-empty pairs from a full crawl collapse to
    ~3,700 pairs for the SAME 18,431-route coverage. "Lazy" here means: the
    heap holds possibly-stale gain estimates (an earlier pick may have
    already covered some of a pair's routes), so a popped entry is only
    trusted if its recomputed gain still matches what's in the heap --
    otherwise it's re-pushed with the corrected gain and the loop continues.
    This avoids recomputing every pair's gain on every iteration (the naive
    O(pairs * iterations) version measured ~94s here; lazy greedy is far
    faster and gives an identical result since it's the same greedy rule,
    just without redundant work).

    Returns (chosen_pair_keys, uncovered_routes) -- uncovered is only
    non-empty if some routes never appeared on ANY cached pair (i.e. they're
    seed-only, discoverable solely via getCurrentTripStatusAtStop)."""
    universe = set()
    for routes in pair_routes.values():
        universe |= routes
    uncovered = set(universe)

    heap = [(-len(routes), pair) for pair, routes in pair_routes.items()]
    heapq.heapify(heap)

    chosen = []
    while uncovered and heap:
        neg_gain, pair = heapq.heappop(heap)
        gain = len(pair_routes[pair] & uncovered)
        if gain == 0:
            continue
        if gain != -neg_gain:
            heapq.heappush(heap, (-gain, pair))
            continue
        chosen.append(pair)
        uncovered -= pair_routes[pair]

    return chosen, uncovered


def build_pair_cover_json():
    weekdays_present = sorted(
        (w for w in os.listdir(RAW_TRIPS_BETWEEN_DIR) if os.path.isdir(f"{RAW_TRIPS_BETWEEN_DIR}/{w}"))
        if os.path.isdir(RAW_TRIPS_BETWEEN_DIR) else [],
        key=WEEKDAY_ORDER.index,
    )
    pair_routes = _load_pair_routes()
    if not pair_routes:
        raise SystemExit(f"No cached pairs in {RAW_TRIPS_BETWEEN_DIR}/ -- run --discover first")
    universe = set()
    for routes in pair_routes.values():
        universe |= routes

    chosen, uncovered = compute_min_pair_cover(pair_routes)
    if uncovered:
        print(f"  note: {len(uncovered)} route(s) never appeared on any cached pair "
              f"(seed-only -- only reachable via getCurrentTripStatusAtStop, not coverable by any pair)")

    pairs_out = [{"start": int(a), "end": int(b)} for a, b in (p.split("_", 1) for p in chosen)]
    with open(OUT_PAIR_COVER_JSON, "w", encoding="utf-8") as f:
        json.dump({
            "computed_from_weekdays": weekdays_present,
            "computed_from_pairs": len(pair_routes),
            "covers_routes": len(universe) - len(uncovered),
            "total_routes_seen_on_pairs": len(universe),
            "pairs": pairs_out,
        }, f, indent=2, ensure_ascii=False)
    print(f"Wrote {OUT_PAIR_COVER_JSON}: {len(chosen)} pair(s) covering {len(universe) - len(uncovered)}/{len(universe)} "
          f"pair-discoverable route(s), computed cumulatively across {len(weekdays_present)} weekday(s) "
          f"({', '.join(weekdays_present)}) (reduced from {len(pair_routes)} non-empty cached pairs)")


def run_requery(date=None, workers=DEFAULT_WORKERS):
    """Cheap day-to-day rerun: stop-seeding restricted to
    msrtc-source-stops.json's confirmed trip-origin stops (see
    _load_requery_seed_stop_ids() -- not the full 43k-stop geojson set,
    same "query only what's worth querying" idea as --min-cover, subject to
    the weekday+TTL cache on _cached_trip_status, see WEEKDAY_CACHE_TTL_DAYS)
    + ONLY the minimal-cover pairs from --min-cover, instead of the full
    BFS's ~120k pair queries. Route topology (which stops a route visits)
    doesn't change day-to-day, so there's no need to re-walk getStopByRoute
    either -- msrtc-routes.json from the original --discover run is still
    valid.

    Caveat this trades away deliberately: --min-cover is cumulative across
    every weekday --discover has crawled (see _load_pair_routes()), which
    makes it far more robust than a single day's snapshot, but it's still
    only as complete as whatever weekdays have actually been crawled -- a
    route whose ONLY coverable pair happens to run on a weekday --discover
    has never touched could still be missed by --requery on that weekday.
    Re-running --discover on more weekdays and rebuilding the cover closes
    that gap over time rather than all at once."""
    date = date or dt.date.today().isoformat()
    weekday = _weekday_label(date)
    if not os.path.exists(OUT_PAIR_COVER_JSON):
        raise SystemExit(f"{OUT_PAIR_COVER_JSON} not found -- run --discover then --min-cover first")
    with open(OUT_PAIR_COVER_JSON, encoding="utf-8") as f:
        cover = json.load(f)
    pairs = [(p["start"], p["end"]) for p in cover["pairs"]]

    stop_ids = _load_requery_seed_stop_ids()

    trips_between_dir = f"{RAW_TRIPS_BETWEEN_DAILY_DIR}/{date}"

    print(f"Requery for {date} ({weekday}): {len(stop_ids)} stop(s) seeded "
          f"(weekday cache, TTL {WEEKDAY_CACHE_TTL_DAYS}d) + {len(pairs)} min-cover pair(s) ({workers} parallel workers)")

    seen_routes, trip_ids = set(), {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(_seed_one_stop, sid, weekday) for sid in stop_ids]
        for i, fut in enumerate(as_completed(futures), 1):
            routes, trips = fut.result()
            seen_routes |= routes
            trip_ids.update(trips)
            if i % 500 == 0 or i == len(stop_ids):
                print(f"[{i}/{len(stop_ids)} stops done, {len(seen_routes)} route codes seeded so far]")

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(_fetch_pair_trips, p, trips_between_dir) for p in pairs]
        for i, fut in enumerate(as_completed(futures), 1):
            for tid, route in fut.result().items():
                trip_ids[tid] = route
                seen_routes.add(route)
            if i % 500 == 0 or i == len(pairs):
                print(f"[{i}/{len(pairs)} cover pairs done, {len(seen_routes)} total route(s) so far]")

    print(f"Done ({date}): {len(seen_routes)} route(s), {len(trip_ids)} trip(s) "
          f"-- seed cache at {RAW_TRIP_STATUS_DIR}/{weekday}/, pair snapshot at {trips_between_dir}/")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--discover", action="store_true",
                     help="full seed + BFS route/trip discovery crawl for TODAY's weekday "
                          "(weekday-partitioned cache -- a different weekday queries fresh, doesn't reuse another day's)")
    ap.add_argument("--build", action="store_true",
                     help="aggregate raw/ into msrtc-routes.json + msrtc-trips.json + msrtc-trip-times.json "
                          "+ msrtc-source-stops.json")
    ap.add_argument("--min-cover", action="store_true",
                     help="compute a minimal stop-pair set (msrtc-pair-cover.json) covering every route seen "
                          "in raw/trips_between_stops/, CUMULATIVE across every weekday --discover has crawled "
                          "(requires a prior --discover run)")
    ap.add_argument("--requery", action="store_true",
                     help="cheap rerun using msrtc-pair-cover.json instead of a full BFS (requires --min-cover first); "
                          "writes a date-stamped snapshot instead of touching the main raw/ cache")
    ap.add_argument("--date", help="date label for --requery snapshots, YYYY-MM-DD (default: today)")
    ap.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                     help=f"parallel requests (default {DEFAULT_WORKERS})")
    args = ap.parse_args()

    if args.discover:
        run_route_discovery(workers=args.workers)
    if args.build:
        build_routes_json()
        build_trips_json()
        build_trip_times_json()
        build_source_stops_json()
    if args.min_cover:
        build_pair_cover_json()
    if args.requery:
        run_requery(date=args.date, workers=args.workers)

    if not any([args.discover, args.build, args.min_cover, args.requery]):
        ap.print_help()


if __name__ == "__main__":
    main()
