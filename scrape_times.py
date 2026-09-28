"""
Dedicated, actively-driven stop-to-stop timing pipeline, distinct from
scrape_trip_times.py's PASSIVE analysis of whatever's already cached: this
one deliberately walks routes (busiest first) and queries exactly the
pairs/geofence checks needed to pin down a confirmed arrival time for
every stop a route visits.

Core matching mechanism
-------------------------
suggestTripsBetweenStopsV2(A, B)'s reported time is the time AT A (the
FROM stop) -- confirmed in earlier work. So querying pair (A, B) tells you
WHEN each of its trips is at A, not B. To learn a trip's time at B, you
need a DIFFERENT query where B is the fromStop -- not necessarily "the
next pair in this specific route": if trip N crosses both (A, B) and
(B, C), or a completely different pair (B, D) belonging to some other
route through B, either one anchors N's time-at-B equally well. So this
builds one GLOBAL table, keyed by (trip_id, stop_id) -> confirmed time,
fed by EVERY pair query made (regardless of which route it came from), and
whenever the same (trip_id, stop_id) gets multiple candidate times (e.g.
observed from two different downstream pairs), the SMALLEST is kept --
confirmed live: a live-tracked response's reported time can drift upward
across repeat queries as the same trip's ETA gets updated, so the
earliest-seen reading is the one closest to actual/scheduled truth.
A route's segment duration is then just this table's time-at(s[i+1]) minus
time-at(s[i]) for whichever trip_ids have both.

Once matched, this table is cached PERMANENTLY (raw/matched_times/), never
expired -- unlike scrape_routes.py's weekday+TTL pair cache (which still
governs the underlying raw pair-query responses this reads from), a
successfully cross-referenced arrival time is a validated fact, not a
snapshot that goes stale.

The one stop a route's own consecutive pairs can never anchor is its OWN
LAST stop (there's no "next pair" to supply a from-B reading for it). For
that, suggestTripsWithGeofence(startStopId, geofence) -- a live,
GPS-position-based "what's near here right now" query, confirmed to share
suggestTripsBetweenStopsV2's exact response shape -- is polled instead,
but only once the trip's ESTIMATED arrival (route origin's known
departure_time + cumulative matched/estimated segment durations) is within
GEOFENCE_LEAD_HOURS, and only until a response contains that SAME trip_id
-- not blindly/continuously, since a cold call at the wrong time of day
just wastes ~4s each and returns nothing (confirmed live).

Usage:
  python3 scrape_times.py --test              # 4 most frequent routes only, one pass, reports results
  python3 scrape_times.py --run               # full run: all routes by frequency, background-considerate,
                                                #   loops (pair queries + scheduled geofence polls) until
                                                #   every stop is matched for today's weekday
  python3 scrape_times.py --analyze            # coverage report from raw/matched_times/ alone, no queries
"""

import argparse
import datetime as dt
import json
import os
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from scrape_stops import BASE, HEADERS, _request_with_retry
from scrape_routes import (
    RAW_TRIPS_BETWEEN_DIR, _cached_trips_between, _trips_from_between_response,
    _weekday_cache_is_stale, _weekday_label,
)
from scrape_trip_times import _haversine_km, _load_stop_times_from_status

ROUTES_JSON = "msrtc-routes.json"
TRIPS_JSON = "msrtc-trips.json"
TRIP_TIMES_JSON = "msrtc-trip-times.json"
STOPS_GEOJSON = "msrtc-stops.geojson"

MATCHED_TIMES_DIR = "raw/matched_times"  # permanent, no TTL -- see module docstring

JSON_HEADERS = {**HEADERS, "Content-Type": "application/json"}

DEFAULT_WORKERS = 4  # deliberately modest -- "considerate of API limits", see module docstring
RATE_LIMIT_DELAY_S = 0.3  # baseline delay between requests within a worker, on top of retry backoff

GEOFENCE_RADIUS_M = "50000"  # 50km -- generous enough to catch a bus that hasn't quite reached the stop yet
GEOFENCE_LEAD_HOURS = 4  # only start polling once estimated arrival is within this many hours
GEOFENCE_POLL_INTERVAL_S = 15 * 60  # how often to retry a pending geofence match while waiting -- routes are
                                     # NEVER given up on, only ever retried at roughly this cadence until matched
                                     # (confirmed matches are cached without expiry, so there's no cost to
                                     # persisting until a match actually lands)
SCHEDULING_FALLBACK_SPEED_KMH = 25.0  # for ESTIMATING arrival times only, same role as scrape_trip_times.py's


def suggest_trips_with_geofence(stop_id, geofence_m=GEOFENCE_RADIUS_M):
    return _request_with_retry(
        "POST", f"{BASE}/trip/suggestTripsWithGeofence",
        headers=JSON_HEADERS, json={"startStopId": stop_id, "geofence": geofence_m},
    )


def _matched_path(weekday, stop_id):
    return f"{MATCHED_TIMES_DIR}/{weekday}/{stop_id}.json"


def load_matched_times(weekday, stop_id):
    path = _matched_path(weekday, stop_id)
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


_matched_times_lock = threading.Lock()  # serializes ALL read-modify-write cycles below, across every stop --
                                          # confirmed live: two worker threads updating the SAME stop_id's file
                                          # concurrently raced on a shared (pid-only, not thread-unique) tmp
                                          # filename and crashed with FileNotFoundError on os.replace(). A route
                                          # can legitimately revisit the same stop twice (loop routes), so this
                                          # isn't a hypothetical edge case. A single lock is fine here -- this
                                          # function's own work (small local file I/O) is nowhere near the
                                          # bottleneck (network calls dominate), so serializing it costs nothing
                                          # meaningful.


def update_matched_time(weekday, stop_id, trip_id, time_str):
    """Min-merges (trip_id -> time_str) into the permanent per-stop store.
    Returns True if this call actually improved (or newly created) the
    entry, False if the existing value was already as good or better."""
    with _matched_times_lock:
        path = _matched_path(weekday, stop_id)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        existing = load_matched_times(weekday, stop_id)
        cur = existing.get(trip_id)
        if cur is not None and cur <= time_str:
            return False
        existing[trip_id] = time_str
        return _write_matched_times(path, existing)


def _write_matched_times(path, data):
    tmp = f"{path}.tmp.{os.getpid()}.{threading.get_ident()}"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)
    return True


def route_frequency_order(trips, routes):
    """[(route_code, trip_count), ...] sorted busiest-first, restricted to
    routes actually present in msrtc-routes.json (same reasoning as
    scrape_trip_times.py's _route_departure_index fix -- msrtc-trips.json
    can reference route_codes with no resolved stop sequence)."""
    counts = Counter(v["route_code"] for v in trips.values() if v.get("route_code") in routes)
    return counts.most_common()


def _entries_from_stop_status_cache(a):
    """Synthesizes the same (tid, route, next_day, is_source, arrival)
    shape _trips_from_between_response() yields, but sourced from EXISTING
    raw/trip_status cache for stop `a` ALONE -- no pair query at all.
    getCurrentTripStatusAtStop's arrivalTime already IS the time at the
    queried stop directly (that's the whole basis of
    scrape_trip_times.py's passive phase), so if `a` has ANY cached
    trip_status data already on disk, it substitutes for a live/cached
    pair query outright. Returns None (not an empty list) when there's
    nothing cached yet, so the caller knows to fall back to a pair query
    rather than mistaking "no data" for "confirmed zero trips"."""
    times = _load_stop_times_from_status(a)
    if not times:
        return None
    return [(tid, None, False, False, at) for tid, at in times.items()]


def _fetch_pair_and_record(a, b, weekday, now):
    """Resolves trip-time-at-a with the CHEAPEST available source first:
    (0) raw/matched_times/ itself -- if `a` already has confirmed matches
    (from an EARLIER pair in this same run, e.g. a different route sharing
    stop `a`, or a previous run entirely -- this store never expires), use
    them directly and skip everything else, including the pair-cache
    check. This is what makes route-to-route overlap cheap: two different
    routes' pairs sharing a stop only ever get that stop actually resolved
    once, not once per route that happens to touch it.
    (1) existing raw/trip_status cache for `a` -- zero queries if present;
    (2) scrape_routes.py's weekday+TTL pair cache for (a, b), which itself
    only fires a live request if nothing fresh is already cached; only
    genuinely new data reaches the actual API. Records every trip's
    time-AT-a into the permanent matched-times store either way, and
    returns the parsed entries so the caller can also inspect them (e.g.
    to know which trip_ids are active on this route's final segment)."""
    already_matched = load_matched_times(weekday, a)
    if already_matched:
        return [(tid, None, False, False, at) for tid, at in already_matched.items()]

    cached_entries = _entries_from_stop_status_cache(a)
    if cached_entries is not None:
        for tid, _route, _next_day, _is_source, arrival in cached_entries:
            if tid and arrival:
                update_matched_time(weekday, a, tid, arrival)
        return cached_entries

    try:
        data = _cached_trips_between(a, b, f"{RAW_TRIPS_BETWEEN_DIR}/{weekday}", _weekday_cache_is_stale(now))
    except requests.exceptions.RequestException as e:
        print(f"  pair {a}->{b}: FAILED after retries ({e.__class__.__name__}), skipping")
        return []
    time.sleep(RATE_LIMIT_DELAY_S)
    entries = list(_trips_from_between_response(data))
    for tid, _route, _next_day, _is_source, arrival in entries:
        if tid and arrival:
            update_matched_time(weekday, a, tid, arrival)
    return entries


def _estimate_stop_arrival(stops, matched_by_stop, seg_index, departure_time, stop_coords):
    """Seconds-from-midnight estimate for stops[seg_index], using real
    matched times where available and a haversine/speed guess for any gap
    -- for scheduling geofence polls only, never written as a real value."""
    if departure_time is None:
        return None
    t = _seconds_from_midnight(departure_time)
    for i in range(seg_index):
        a, b = stops[i], stops[i + 1]
        times_a = matched_by_stop.get(a, {})
        times_b = matched_by_stop.get(b, {})
        common = set(times_a) & set(times_b)
        if common:
            deltas = sorted(_seconds_from_midnight(times_b[tid]) - _seconds_from_midnight(times_a[tid])
                             for tid in common)
            t += deltas[len(deltas) // 2]
        elif a in stop_coords and b in stop_coords:
            km = _haversine_km(*stop_coords[a], *stop_coords[b])
            t += (km / SCHEDULING_FALLBACK_SPEED_KMH) * 3600
        else:
            t += 300
    return t


def _seconds_from_midnight(hms):
    h, m, s = (int(x) for x in hms.split(":"))
    return h * 3600 + m * 60 + s


def _load_stop_coords():
    coords = {}
    if not os.path.exists(STOPS_GEOJSON):
        return coords
    with open(STOPS_GEOJSON, encoding="utf-8") as f:
        geo = json.load(f)
    for feat in geo["features"]:
        lon, lat = feat["geometry"]["coordinates"]
        coords[feat["properties"]["stop_id"]] = (lat, lon)
    return coords


def process_route(route_code, stops, weekday, now, workers=DEFAULT_WORKERS, existing_durations=None):
    """Walks every consecutive pair of `stops`, recording time-at-fromStop
    for each into the permanent matched store. Segments that
    msrtc-trip-times.json ALREADY has a duration for (existing_durations,
    the route's own entry from that file) are skipped outright -- not even
    the cheap cache-check path runs for them, since there's nothing this
    pipeline would learn that isn't already known. Returns (matched_by_stop,
    final_stop_trip_ids, skipped, total) -- final_stop_trip_ids is whichever
    trip_ids were seen live-crossing the SECOND-TO-LAST pair, i.e. the
    candidates whose final stop still needs geofence resolution (empty if
    that segment was itself skipped as already-known). skipped/total are
    segment counts, deliberately NOT printed here -- with potentially
    thousands of routes processed in one run, a print per route is mostly
    noise; callers accumulate these across routes and report a single
    aggregated line instead (see run_test/run_full)."""
    pairs = list(zip(stops, stops[1:]))
    matched_by_stop = {sid: load_matched_times(weekday, sid) for sid in stops}
    final_stop_trip_ids = set()

    to_query = [(a, b) for i, (a, b) in enumerate(pairs)
                if not (existing_durations and i < len(existing_durations) and existing_durations[i])]
    skipped = len(pairs) - len(to_query)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(_fetch_pair_and_record, a, b, weekday, now): (a, b) for a, b in to_query}
        for fut in as_completed(futures):
            a, b = futures[fut]
            entries = fut.result()
            matched_by_stop[a] = load_matched_times(weekday, a)
            if b == stops[-1]:
                final_stop_trip_ids.update(tid for tid, *_ in entries if tid)

    return matched_by_stop, final_stop_trip_ids, skipped, len(pairs)


def try_resolve_final_stop(route_code, stops, weekday, now, matched_by_stop, final_stop_trip_ids,
                            departure_time, stop_coords, force=False):
    """One-shot: if the final stop's already-unresolved trip_ids have an
    estimated arrival within GEOFENCE_LEAD_HOURS, poll
    suggestTripsWithGeofence once and record any matches. Returns the
    still-unresolved trip_id set (for the caller's pending-poll queue).

    force=True skips the estimate/lead-time gating and queries regardless
    -- used for the periodic fallback check on routes with no departure-time
    anchor at all (est would otherwise always be None, so without `force`
    this function would return immediately, every single time, forever,
    and such a route could NEVER actually resolve -- see run_full())."""
    final_stop = stops[-1]
    already = load_matched_times(weekday, final_stop)
    pending = {tid for tid in final_stop_trip_ids if tid not in already}
    if not pending:
        return set()

    if not force:
        est = _estimate_stop_arrival(stops, matched_by_stop, len(stops) - 1, departure_time, stop_coords)
        if est is None:
            return pending  # no anchor at all -- caller decides whether/when to retry
        now_seconds = now.hour * 3600 + now.minute * 60 + now.second
        if (est - now_seconds) % 86400 > GEOFENCE_LEAD_HOURS * 3600:
            return pending  # too far out, not worth a live poll yet

    try:
        data = suggest_trips_with_geofence(final_stop)
    except requests.exceptions.RequestException as e:
        print(f"  geofence stop {final_stop}: FAILED after retries ({e.__class__.__name__})")
        return pending
    time.sleep(RATE_LIMIT_DELAY_S)
    for tid, _route, _next_day, _is_source, arrival in _trips_from_between_response(data):
        if tid in pending and arrival:
            if update_matched_time(weekday, final_stop, tid, arrival):
                print(f"  route {route_code}: matched final stop {final_stop} for trip {tid} -> {arrival}")
            pending.discard(tid)
    return pending


def _load_existing_trip_times():
    if os.path.exists(TRIP_TIMES_JSON):
        with open(TRIP_TIMES_JSON, encoding="utf-8") as f:
            return json.load(f)
    return {}


def _route_fully_covered(route_code, stops, existing_trip_times):
    """True if EVERY segment of this route already has a duration in
    msrtc-trip-times.json -- used to skip a route entirely, before ever
    calling process_route(), rather than entering it just to loop over
    segments and print that each one was skipped. A route with <2 stops
    has no segments to cover at all, so it counts as trivially covered."""
    n = max(len(stops) - 1, 0)
    if n == 0:
        return True
    durs = existing_trip_times.get(route_code)
    return bool(durs) and len(durs) >= n and all(durs[i] for i in range(n))


def _filter_uncovered(ranked, routes, existing_trip_times):
    """[(route_code, count), ...] restricted to routes with at least one
    segment still missing a duration -- the rest are reported as skipped
    and never touched again by the caller."""
    kept = [(rc, c) for rc, c in ranked if not _route_fully_covered(rc, routes[rc], existing_trip_times)]
    skipped = len(ranked) - len(kept)
    if skipped:
        print(f"({skipped}/{len(ranked)} route(s) already fully covered in {TRIP_TIMES_JSON} -- skipped entirely)")
    return kept


def run_test(n_routes=4, workers=DEFAULT_WORKERS):
    with open(ROUTES_JSON, encoding="utf-8") as f:
        routes = json.load(f)
    with open(TRIPS_JSON, encoding="utf-8") as f:
        trips = json.load(f)
    existing_trip_times = _load_existing_trip_times()
    stop_coords = _load_stop_coords()
    weekday = _weekday_label()
    now = dt.datetime.now()

    # Filter BEFORE truncating to n_routes -- otherwise --test's whole
    # point (exercising real work on busy routes) could end up vacuous if
    # the top N by frequency happen to already be fully covered.
    uncovered_ranked = _filter_uncovered(route_frequency_order(trips, routes), routes, existing_trip_times)
    ranked = uncovered_ranked[:n_routes]
    print(f"Testing {len(ranked)} most frequent NOT-yet-fully-covered route(s): {ranked}")

    route_departure = {}
    for v in trips.values():
        rc, dep = v.get("route_code"), v.get("departure_time")
        if rc and dep and (rc not in route_departure or dep < route_departure[rc]):
            route_departure[rc] = dep

    for route_code, count in ranked:
        stops = routes[route_code]
        print(f"\nRoute {route_code} ({count} trip(s), {len(stops)} stop(s)):")
        matched_by_stop, final_trip_ids, skipped, total = process_route(
            route_code, stops, weekday, now, workers=workers,
            existing_durations=existing_trip_times.get(route_code))
        if skipped:
            print(f"  ({skipped}/{total} segment(s) skipped -- already have a duration in {TRIP_TIMES_JSON})")
        resolved = sum(1 for sid in stops if load_matched_times(weekday, sid))
        print(f"  {resolved}/{len(stops)} stop(s) have at least one matched time after pair walk")
        pending = try_resolve_final_stop(route_code, stops, weekday, now, matched_by_stop, final_trip_ids,
                                          route_departure.get(route_code), stop_coords)
        if pending:
            print(f"  final stop {stops[-1]}: {len(pending)} trip(s) still unresolved "
                  f"(either not within {GEOFENCE_LEAD_HOURS}h yet, or no match in this one geofence poll)")
        else:
            print(f"  final stop {stops[-1]}: fully resolved")


def run_full(workers=DEFAULT_WORKERS):
    """Background-considerate full run: every route, busiest first, one
    pair-walk pass each, then a scheduled polling loop for whatever final
    stops are still pending -- until every route's every stop is matched
    for today's weekday, or the process is stopped (safe to re-run/resume
    at any point, since raw/matched_times/ never expires and
    scrape_routes.py's own weekday+TTL pair cache means already-fresh
    pairs are skipped for free)."""
    with open(ROUTES_JSON, encoding="utf-8") as f:
        routes = json.load(f)
    with open(TRIPS_JSON, encoding="utf-8") as f:
        trips = json.load(f)
    existing_trip_times = _load_existing_trip_times()
    stop_coords = _load_stop_coords()
    weekday = _weekday_label()

    route_departure = {}
    for v in trips.values():
        rc, dep = v.get("route_code"), v.get("departure_time")
        if rc and dep and (rc not in route_departure or dep < route_departure[rc]):
            route_departure[rc] = dep

    ranked = _filter_uncovered(route_frequency_order(trips, routes), routes, existing_trip_times)
    print(f"Full run: {len(ranked)} route(s) with work remaining, busiest first, weekday={weekday}")

    pending_final = {}  # route_code -> (stops, trip_ids) still needing geofence resolution
    segments_skipped = segments_total = 0
    for i, (route_code, count) in enumerate(ranked, 1):
        stops = routes[route_code]
        matched_by_stop, final_trip_ids, skipped, total = process_route(
            route_code, stops, weekday, dt.datetime.now(), workers=workers,
            existing_durations=existing_trip_times.get(route_code))
        segments_skipped += skipped
        segments_total += total
        already = load_matched_times(weekday, stops[-1])
        still_pending = {tid for tid in final_trip_ids if tid not in already}
        if still_pending:
            pending_final[route_code] = (stops, still_pending, dt.datetime.now())
        if i % 200 == 0 or i == len(ranked):
            print(f"[{i}/{len(ranked)} routes pair-walked, {len(pending_final)} route(s) with a pending final "
                  f"stop, {segments_skipped}/{segments_total} segment(s) skipped so far -- already in "
                  f"{TRIP_TIMES_JSON}]")

    # A route's final stop can get resolved as a SIDE EFFECT of a totally
    # different, LATER-processed route's own pair-walk -- e.g. route A
    # (X -> Y -> Z) and route B (X -> Y) share the X->Y segment, so once
    # route A's (Y, Z) pair query resolves matched_times[Y] for a trip that
    # also runs on route B, route B's own final stop (Y) is done too,
    # without route B itself ever needing a geofence poll. This wasn't
    # causing wasted API calls (try_resolve_final_stop always re-checks
    # raw/matched_times fresh before considering a live query), but a route
    # resolved this way could still sit in pending_final -- inflating the
    # "pending" count and its own scheduled check -- until its next
    # scheduled poll came due. One pass here recognizes it immediately.
    resolved_as_side_effect = [
        rc for rc, (stops, trip_ids, _first_seen) in pending_final.items()
        if trip_ids <= set(load_matched_times(weekday, stops[-1]))
    ]
    for rc in resolved_as_side_effect:
        del pending_final[rc]
    if resolved_as_side_effect:
        print(f"  {len(resolved_as_side_effect)} route(s) turned out to already be resolved (via another "
              f"route sharing their final stop) -- no geofence poll needed for them")

    print(f"Pair-walk pass done. {len(pending_final)} route(s) awaiting final-stop resolution via geofence polling.")

    # pending_final[route_code] = (stops, trip_ids, first_seen, last_polled) --
    # last_polled starts at None (never polled yet).
    pending_final = {rc: (stops, tids, first_seen, None) for rc, (stops, tids, first_seen) in pending_final.items()}

    while pending_final:
        now = dt.datetime.now()
        due = {}  # route_code -> force (True if due only via the periodic fallback, not a real estimate)
        for route_code, (stops, trip_ids, first_seen, last_polled) in list(pending_final.items()):
            est = _estimate_stop_arrival(stops, {sid: load_matched_times(weekday, sid) for sid in stops},
                                          len(stops) - 1, route_departure.get(route_code), stop_coords)
            now_seconds = now.hour * 3600 + now.minute * 60 + now.second
            within_lead = est is not None and (est - now_seconds) % 86400 <= GEOFENCE_LEAD_HOURS * 3600
            # No known departure time to anchor an estimate at all (est is
            # None) means within_lead can NEVER be true for this route --
            # it still needs a periodic fallback check, on the same
            # ~GEOFENCE_POLL_INTERVAL_S cadence as everything else, or it
            # would simply never be retried again. force=True (below) is
            # what makes that fallback check actually fire a query instead
            # of immediately bailing out inside try_resolve_final_stop.
            periodic_fallback_due = last_polled is None or (now - last_polled).total_seconds() >= GEOFENCE_POLL_INTERVAL_S
            if within_lead:
                due[route_code] = False
            elif periodic_fallback_due:
                due[route_code] = True

        if not due:
            print(f"  {len(pending_final)} route(s) pending, none due yet -- "
                  f"sleeping {GEOFENCE_POLL_INTERVAL_S / 60:.0f} min")
            time.sleep(GEOFENCE_POLL_INTERVAL_S)
            continue

        print(f"  polling {len(due)} due route(s)' final stops ({len(pending_final)} pending total)...")
        for route_code, force in due.items():
            stops, trip_ids, first_seen, _last_polled = pending_final[route_code]
            matched_by_stop = {sid: load_matched_times(weekday, sid) for sid in stops}
            remaining = try_resolve_final_stop(route_code, stops, weekday, dt.datetime.now(), matched_by_stop,
                                                trip_ids, route_departure.get(route_code), stop_coords, force=force)
            # Never abandoned -- confirmed matches are cached without expiry
            # (raw/matched_times/), so a route just stays pending, polled
            # roughly every GEOFENCE_POLL_INTERVAL_S, until it's actually
            # resolved. No API budget is wasted by this: try_resolve_final_stop
            # only fires a live geofence query when there's still something
            # unresolved AND (an estimate says it's plausibly due now, since
            # the fallback path above only forces a RECHECK, not a query --
            # try_resolve_final_stop still gates the actual request on its own
            # within-lead-time logic).
            if remaining:
                pending_final[route_code] = (stops, remaining, first_seen, dt.datetime.now())
            else:
                del pending_final[route_code]

    print(f"Done: every route matched for {weekday}.")


def _load_matched_times_all_weekdays(stop_id):
    """trip_id -> time, pooled across every weekday bucket in
    raw/matched_times/ for one stop (that store never expires, so pooling
    across weekdays here is purely additive -- same reasoning
    scrape_routes.py's _load_pair_times() and scrape_trip_times.py's
    _load_stop_times_from_status() already use)."""
    out = {}
    if not os.path.isdir(MATCHED_TIMES_DIR):
        return out
    for weekday in os.listdir(MATCHED_TIMES_DIR):
        path = _matched_path(weekday, stop_id)
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                out.update(json.load(f))
    return out


def compute_durations_from_matched_times(routes):
    """route_code -> [{"duration_seconds","sample_size"} | None, ...],
    same shape/semantics as scrape_trip_times.py's
    compute_durations_from_stop_status(), but sourced from THIS pipeline's
    own permanent raw/matched_times/ cache instead of raw trip_status
    snapshots -- these are confirmed cross-referenced matches, not passive
    snapshot data."""
    stop_cache = {}

    def times_at(stop_id):
        if stop_id not in stop_cache:
            stop_cache[stop_id] = _load_matched_times_all_weekdays(stop_id)
        return stop_cache[stop_id]

    out = {}
    for route_code, stops in routes.items():
        durations = []
        for a, b in zip(stops, stops[1:]):
            t_a, t_b = times_at(a), times_at(b)
            common = set(t_a) & set(t_b)
            deltas = []
            for tid in common:
                try:
                    ta = dt.datetime.strptime(t_a[tid], "%H:%M:%S")
                    tb = dt.datetime.strptime(t_b[tid], "%H:%M:%S")
                except ValueError:
                    continue
                delta = (tb - ta).total_seconds()
                if 0 < delta < 7200:
                    deltas.append(delta)
            if deltas:
                deltas.sort()
                median = deltas[len(deltas) // 2]
                durations.append({"duration_seconds": round(median), "sample_size": len(deltas)})
            else:
                durations.append(None)
        out[route_code] = durations
    return out


def analyze():
    """Reports coverage against msrtc-trip-times.json -- the actual file
    this pipeline caches toward (build_gtfs.py's own input) -- rather than
    a raw/matched_times/ file count that says nothing about how close the
    real target artifact is to complete."""
    with open(ROUTES_JSON, encoding="utf-8") as f:
        routes = json.load(f)
    existing = _load_existing_trip_times()
    total = sum(max(len(stops) - 1, 0) for stops in routes.values())
    already = sum(1 for durs in existing.values() for d in durs if d)

    matched_durations = compute_durations_from_matched_times(routes)
    newly_resolvable = 0
    for route_code, stops in routes.items():
        n = max(len(stops) - 1, 0)
        ex_durs = existing.get(route_code) or [None] * n
        m_durs = matched_durations.get(route_code) or [None] * n
        for i in range(n):
            has_existing = i < len(ex_durs) and ex_durs[i]
            has_matched = i < len(m_durs) and m_durs[i]
            if not has_existing and has_matched:
                newly_resolvable += 1

    print(f"{TRIP_TIMES_JSON}: {already}/{total} segment(s) already have a duration")
    print(f"raw/matched_times/: {newly_resolvable} additional segment(s) now resolvable but not yet merged "
          f"(run --build to merge)")
    pct = 100 * (already + newly_resolvable) / total if total else 0
    print(f"After merge: {already + newly_resolvable}/{total} ({pct:.1f}%)")


def build_merged_trip_times(routes=None):
    """Merges compute_durations_from_matched_times()'s output into
    msrtc-trip-times.json -- never overwrites an existing duration, only
    fills genuinely null segments. Mirrors scrape_trip_times.py's
    build_trip_times_merged(), just sourced from raw/matched_times/
    instead of raw/trip_status/ -- safe to run either/both; whichever
    source has data for a given segment fills it, and neither ever
    regresses what's already there."""
    if routes is None:
        with open(ROUTES_JSON, encoding="utf-8") as f:
            routes = json.load(f)
    existing = _load_existing_trip_times()
    matched_durations = compute_durations_from_matched_times(routes)

    merged = {}
    total = from_existing = from_matched = still_missing = 0
    for route_code, stops in routes.items():
        n = max(len(stops) - 1, 0)
        ex_durs = existing.get(route_code) or [None] * n
        m_durs = matched_durations.get(route_code) or [None] * n
        out = []
        for i in range(n):
            total += 1
            if i < len(ex_durs) and ex_durs[i]:
                out.append(ex_durs[i])
                from_existing += 1
            elif i < len(m_durs) and m_durs[i]:
                out.append(m_durs[i])
                from_matched += 1
            else:
                out.append(None)
                still_missing += 1
        merged[route_code] = out

    with open(TRIP_TIMES_JSON, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)
    print(f"Wrote {TRIP_TIMES_JSON}: {total} segment(s) total -- {from_existing} kept as-is, "
          f"{from_matched} newly filled from raw/matched_times/, {still_missing} still missing")
    return merged


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--test", action="store_true", help="process only the N most frequent routes (default 4)")
    ap.add_argument("--n-routes", type=int, default=4, help="route count for --test (default 4)")
    ap.add_argument("--run", action="store_true",
                     help="full background-considerate run: all routes by frequency, loops until every "
                          "stop is matched for today's weekday")
    ap.add_argument("--analyze", action="store_true",
                     help=f"report coverage against {TRIP_TIMES_JSON} (this pipeline's actual target file), "
                          f"no queries")
    ap.add_argument("--build", action="store_true",
                     help=f"merge raw/matched_times/ into {TRIP_TIMES_JSON} (never overwrites an existing "
                          f"duration, only fills nulls)")
    ap.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help=f"parallel pair-query workers (default {DEFAULT_WORKERS})")
    args = ap.parse_args()

    if args.test:
        run_test(n_routes=args.n_routes, workers=args.workers)
    if args.run:
        run_full(workers=args.workers)
    if args.analyze:
        analyze()
    if args.build:
        build_merged_trip_times()

    if not any([args.test, args.run, args.analyze, args.build]):
        ap.print_help()


if __name__ == "__main__":
    main()
