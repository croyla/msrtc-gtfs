"""
Dedicated, minimal-query pipeline for stop-to-stop segment durations,
refining msrtc-trip-times.json.

Why not just rely on scrape_routes.py's suggestTripsBetweenStopsV2 pair
cache (what build_trip_times_json() there already computes from)? That
data is a BYPRODUCT of route-discovery's BFS -- one query PER (stop_a,
stop_b) EDGE, which scales with the number of distinct route segments
(hundreds of thousands, confirmed live: 120,695+ pair queries in one full
crawl). This pipeline instead queries getCurrentTripStatusAtStop PER STOP
(a NODE, not an edge) -- confirmed live: every entry already carries
arrivalTime for that trip AT THAT SPECIFIC queried stop, regardless of its
firstStop/lastStop/passingThroughStop status -- so cross-referencing
trip_id + arrivalTime between any two stops a route visits gives the same
segment-duration signal, at a fraction of the query count: transit
networks share far more STOPS across routes than they share entire route
EDGES, so per-node querying is the cheaper axis to crawl on.

Three phases:
  1. PASSIVE (always free): compute whatever segment durations are already
     derivable from the EXISTING raw/trip_status/ cache -- zero new
     queries. In practice this alone resolves a lot, since --discover's
     own stop-seeding phase already queried every stop at least once.
  2. TARGETED FILL (--fill): for segments still missing a duration after
     phase 1, query getCurrentTripStatusAtStop at just the STOPS those
     segments touch (deduplicated -- typically far fewer than the segment
     count, since one stop often anchors several missing segments across
     different routes), subject to scrape_routes.py's existing weekday+TTL
     cache, then recompute.
  3. EXHAUSTIVE (--exhaustive): repeats phase 2 in a scheduled loop across
     the day for whatever's STILL missing after one pass (a trip simply
     wasn't active/visible during the single query we made). Rather than
     polling continuously, each retry is scheduled near the missing
     segment's ESTIMATED passage time -- derived from msrtc-trips.json's
     already-known real origin departure times for that route, plus
     cumulative duration from the route's origin to the segment (using
     whatever real durations are already resolved, and a rough
     haversine/speed estimate -- for scheduling purposes only, never
     written as an actual duration -- for whatever isn't). Falls back to a
     fixed retry interval when no departure time is known for a route at
     all. Runs until every segment is covered or a time budget (default
     24h) is spent, whichever comes first. --dry-run previews the computed
     query schedule without sleeping or querying anything, for sanity
     checking.

msrtc-trip-times.json is never regressed by any of this -- --build always
MERGES: an existing (pair-cache-derived) duration for a segment is kept as
is; only genuinely null segments get filled from the stop-status data
computed here.

Usage:
  python3 scrape_trip_times.py --analyze              # coverage report, no queries
  python3 scrape_trip_times.py --fill                 # one targeted pass for missing segments
  python3 scrape_trip_times.py --exhaustive            # --fill, looped + scheduled, up to --budget-hours
  python3 scrape_trip_times.py --exhaustive --dry-run  # preview the query schedule, no queries/sleeping
  python3 scrape_trip_times.py --build                 # merge stop-status data into msrtc-trip-times.json
"""

import argparse
import datetime as dt
import json
import math
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from scrape_routes import (
    DEFAULT_WORKERS, RAW_TRIP_STATUS_DIR, _seed_one_stop, _weekday_label,
)

ROUTES_JSON = "msrtc-routes.json"
TRIPS_JSON = "msrtc-trips.json"
TRIP_TIMES_JSON = "msrtc-trip-times.json"
STOPS_GEOJSON = "msrtc-stops.geojson"

# For SCHEDULING re-queries only (picking WHEN to check a stop again) --
# never written as an actual duration. Doesn't need build_gtfs.py's
# empirically-derived fallback speed's precision, just a reasonable guess.
SCHEDULING_FALLBACK_SPEED_KMH = 25.0
QUERY_LEAD_TIME = dt.timedelta(minutes=10)  # query a bit BEFORE the estimated passage time
FIXED_RETRY_INTERVAL = dt.timedelta(hours=3)  # for routes with no known departure time to anchor an estimate
BATCH_WINDOW = dt.timedelta(minutes=5)  # stops due within this window of each other get queried together
DEFAULT_BUDGET_HOURS = 24


def _load_stop_times_from_status(stop_id):
    """trip_id -> "HH:MM:SS" arrival string for one stop, pooled across
    every weekday bucket that has cached data for it (same
    pool-across-weekdays choice scrape_routes.py's _load_pair_times()
    makes for the pair-based approach -- duration is a property of the
    road, not the day, so more weekday samples only helps)."""
    out = {}
    if not os.path.isdir(RAW_TRIP_STATUS_DIR):
        return out
    for weekday in os.listdir(RAW_TRIP_STATUS_DIR):
        path = f"{RAW_TRIP_STATUS_DIR}/{weekday}/{stop_id}.json"
        if not os.path.exists(path):
            continue
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        for entry in data.get("data", []) or []:
            tid, at = entry.get("tripCode"), entry.get("arrivalTime")
            if tid and at:
                out[tid] = at
    return out


def compute_durations_from_stop_status(routes):
    """route_code -> [{"duration_seconds", "sample_size"} | None, ...],
    aligned to each route's consecutive stop pairs, built PURELY from
    cached getCurrentTripStatusAtStop responses -- zero new queries, safe
    to call any time. Memoizes each stop's trip_id->time lookup across
    routes, since stops repeat heavily (a busy junction can anchor dozens
    of routes) -- without this, a stop appearing in N routes would get its
    cache file re-read and re-parsed N times."""
    stop_cache = {}

    def times_at(stop_id):
        if stop_id not in stop_cache:
            stop_cache[stop_id] = _load_stop_times_from_status(stop_id)
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
                if 0 < delta < 7200:  # sanity bound -- no single segment realistically takes >2h
                    deltas.append(delta)
            if deltas:
                deltas.sort()
                median = deltas[len(deltas) // 2]
                durations.append({"duration_seconds": round(median), "sample_size": len(deltas)})
            else:
                durations.append(None)
        out[route_code] = durations
    return out


def missing_segments(routes, durations):
    """[(route_code, seg_index, stop_a, stop_b), ...] for every segment
    still without a computed duration."""
    missing = []
    for route_code, stops in routes.items():
        segs = durations.get(route_code) or [None] * max(len(stops) - 1, 0)
        for i, (a, b) in enumerate(zip(stops, stops[1:])):
            if i >= len(segs) or not segs[i]:
                missing.append((route_code, i, a, b))
    return missing


def analyze_coverage(routes=None):
    if routes is None:
        with open(ROUTES_JSON, encoding="utf-8") as f:
            routes = json.load(f)
    durations = compute_durations_from_stop_status(routes)
    missing = missing_segments(routes, durations)
    total = sum(max(len(s) - 1, 0) for s in routes.values())
    covered = total - len(missing)
    print(f"Segments: {covered}/{total} coverable from EXISTING trip_status cache alone (no new queries), "
          f"{len(missing)} still missing across {len({m[0] for m in missing})} route(s)")
    return routes, durations, missing


def stops_needed_for(missing):
    needed = set()
    for _, _, a, b in missing:
        needed.add(a)
        needed.add(b)
    return needed


def run_fill(workers=DEFAULT_WORKERS):
    routes, durations, missing = analyze_coverage()
    if not missing:
        print("No missing segments -- nothing to fill.")
        return routes, durations
    needed_stops = sorted(stops_needed_for(missing))
    print(f"Querying {len(needed_stops)} distinct stop(s) (minimal set for {len(missing)} missing segment(s); "
          f"weekday+TTL cache applies, so already-fresh stops are skipped for free)")
    weekday = _weekday_label()
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(_seed_one_stop, sid, weekday) for sid in needed_stops]
        for i, fut in enumerate(as_completed(futures), 1):
            fut.result()
            if i % 500 == 0 or i == len(needed_stops):
                print(f"[{i}/{len(needed_stops)} stops queried]")

    durations = compute_durations_from_stop_status(routes)
    missing_after = missing_segments(routes, durations)
    print(f"After fill: {len(missing) - len(missing_after)} segment(s) newly resolved, "
          f"{len(missing_after)} still missing")
    return routes, durations


def _haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi, dlmb = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


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


def _route_departure_index(trips, routes):
    """route_code -> earliest known real origin departure_time among its
    trips -- the only anchor available for estimating when this route's
    OTHER, not-yet-observed trips pass a given stop (schedules aren't
    published anywhere; this is the closest thing we have). Restricted to
    route_codes present in `routes` (msrtc-routes.json) -- msrtc-trips.json
    can reference route_codes with no resolved stop sequence (filtered out
    of msrtc-routes.json during scrape_routes.py's build), which would
    otherwise inflate this index with entries missing_segments() can never
    actually look up."""
    idx = {}
    for v in trips.values():
        rc, dep = v.get("route_code"), v.get("departure_time")
        if rc and dep and rc in routes and (rc not in idx or dep < idx[rc]):
            idx[rc] = dep
    return idx


def _seconds_from_midnight(hms):
    h, m, s = (int(x) for x in hms.split(":"))
    return h * 3600 + m * 60 + s


def _cumulative_duration_estimate(stops, durations, upto_index, stop_coords):
    """Seconds from route origin (stops[0]) to stops[upto_index], using
    real durations wherever already known and a haversine/
    SCHEDULING_FALLBACK_SPEED_KMH guess for any still-missing segment along
    the way. Only ever used to pick WHEN to re-query -- never written as an
    actual duration value."""
    total = 0.0
    for i in range(upto_index):
        d = durations[i] if i < len(durations) else None
        if d:
            total += d["duration_seconds"]
            continue
        a, b = stops[i], stops[i + 1]
        if a in stop_coords and b in stop_coords:
            km = _haversine_km(*stop_coords[a], *stop_coords[b])
            total += (km / SCHEDULING_FALLBACK_SPEED_KMH) * 3600
        else:
            total += 300  # 5-minute nominal guess -- better than nothing
    return total


def estimate_next_query_time(route_code, seg_index, stops, durations, stop_coords, route_departure_index, now):
    """datetime -- when to next query stops[seg_index] (the missing
    segment's FROM stop) to try to catch this segment's trip live."""
    dep = route_departure_index.get(route_code)
    if not dep:
        return now + FIXED_RETRY_INTERVAL
    target_seconds = _seconds_from_midnight(dep) + _cumulative_duration_estimate(stops, durations, seg_index,
                                                                                  stop_coords)
    # anchor to the next occurrence of this time-of-day from `now` (today if
    # still ahead, else tomorrow) -- schedules repeat daily, we just want
    # the next real chance to observe this trip live.
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    target = midnight + dt.timedelta(seconds=target_seconds % 86400)
    if target <= now:
        target += dt.timedelta(days=1)
    return target - QUERY_LEAD_TIME


def run_exhaustive(workers=DEFAULT_WORKERS, budget_hours=DEFAULT_BUDGET_HOURS, dry_run=False):
    with open(ROUTES_JSON, encoding="utf-8") as f:
        routes = json.load(f)
    with open(TRIPS_JSON, encoding="utf-8") as f:
        trips = json.load(f)
    stop_coords = _load_stop_coords()
    route_departure_index = _route_departure_index(trips, routes)
    print(f"{len(route_departure_index)}/{len(routes)} route(s) have a known departure time to anchor estimates "
          f"from; the rest fall back to a fixed {FIXED_RETRY_INTERVAL} retry interval")

    deadline = dt.datetime.now() + dt.timedelta(hours=budget_hours)
    round_num = 0
    while True:
        durations = compute_durations_from_stop_status(routes)
        missing = missing_segments(routes, durations)
        if not missing:
            print("All segments covered.")
            break
        now = dt.datetime.now()
        if now >= deadline:
            print(f"Budget of {budget_hours}h exhausted -- {len(missing)} segment(s) still missing.")
            break
        round_num += 1

        # One target time per segment, deduped by stop -- several missing
        # segments (different routes, or different points along one route)
        # can share a stop, and querying it once covers all of them.
        target_by_stop = {}
        for route_code, seg_index, a, _b in missing:
            stops = routes[route_code]
            t = estimate_next_query_time(route_code, seg_index, stops, durations.get(route_code) or [],
                                          stop_coords, route_departure_index, now)
            if a not in target_by_stop or t < target_by_stop[a]:
                target_by_stop[a] = t

        due_now = [sid for sid, t in target_by_stop.items() if t <= now + BATCH_WINDOW]
        next_time = min(target_by_stop.values())
        print(f"[round {round_num}] {len(missing)} segment(s) missing, {len(target_by_stop)} distinct stop(s) "
              f"scheduled, {len(due_now)} due within {BATCH_WINDOW} -- next target: "
              f"{next_time.strftime('%Y-%m-%d %H:%M:%S')}")

        if dry_run:
            print("  (--dry-run: not sleeping/querying -- next 10 scheduled stops:)")
            for sid, t in sorted(target_by_stop.items(), key=lambda kv: kv[1])[:10]:
                print(f"    stop {sid} -> {t.strftime('%Y-%m-%d %H:%M:%S')}")
            break

        if not due_now:
            wait_s = max(0.0, (next_time - now).total_seconds())
            remaining_s = max(0.0, (deadline - now).total_seconds())
            sleep_s = min(wait_s, remaining_s)
            print(f"  sleeping {sleep_s / 60:.1f} min until next scheduled stop...")
            time.sleep(sleep_s)
            continue

        weekday = _weekday_label()
        with ThreadPoolExecutor(max_workers=workers) as ex:
            list(ex.map(lambda sid: _seed_one_stop(sid, weekday), due_now))
        print(f"  queried {len(due_now)} due stop(s)")

    if not dry_run:
        build_trip_times_merged(routes)


def build_trip_times_merged(routes=None):
    """Merges compute_durations_from_stop_status()'s output into
    msrtc-trip-times.json -- NEVER overwrites a segment that already has a
    duration from scrape_routes.py's pair-cache-based computation, only
    fills genuinely null ones. Safe to run any time, independent of --fill
    / --exhaustive (e.g. after scrape_routes.py's --build has run again and
    picked up more pair data)."""
    if routes is None:
        with open(ROUTES_JSON, encoding="utf-8") as f:
            routes = json.load(f)
    existing = {}
    if os.path.exists(TRIP_TIMES_JSON):
        with open(TRIP_TIMES_JSON, encoding="utf-8") as f:
            existing = json.load(f)
    stop_based = compute_durations_from_stop_status(routes)

    merged = {}
    total = from_existing = from_stop_status = still_missing = 0
    for route_code, stops in routes.items():
        n = max(len(stops) - 1, 0)
        ex_durs = existing.get(route_code) or [None] * n
        sb_durs = stop_based.get(route_code) or [None] * n
        out = []
        for i in range(n):
            total += 1
            if i < len(ex_durs) and ex_durs[i]:
                out.append(ex_durs[i])
                from_existing += 1
            elif i < len(sb_durs) and sb_durs[i]:
                out.append(sb_durs[i])
                from_stop_status += 1
            else:
                out.append(None)
                still_missing += 1
        merged[route_code] = out

    with open(TRIP_TIMES_JSON, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)
    print(f"Wrote {TRIP_TIMES_JSON}: {total} segment(s) total -- {from_existing} kept from existing "
          f"pair-cache data, {from_stop_status} newly filled from stop-status cross-referencing, "
          f"{still_missing} still missing")
    return merged


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--analyze", action="store_true", help="report current segment-duration coverage, no queries")
    ap.add_argument("--fill", action="store_true",
                     help="one targeted query pass for segments still missing a duration "
                          "(queries only the minimal stop set they touch)")
    ap.add_argument("--exhaustive", action="store_true",
                     help="loop --fill on a schedule (anchored to known departure times where possible) "
                          "until full coverage or --budget-hours is spent")
    ap.add_argument("--dry-run", action="store_true",
                     help="with --exhaustive: preview the computed query schedule, no querying/sleeping")
    ap.add_argument("--budget-hours", type=float, default=DEFAULT_BUDGET_HOURS,
                     help=f"--exhaustive time budget in hours (default {DEFAULT_BUDGET_HOURS})")
    ap.add_argument("--build", action="store_true",
                     help="merge stop-status-derived durations into msrtc-trip-times.json "
                          "(never overwrites existing pair-cache-derived durations)")
    ap.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help=f"parallel requests (default {DEFAULT_WORKERS})")
    args = ap.parse_args()

    if args.analyze:
        analyze_coverage()
    if args.fill:
        run_fill(workers=args.workers)
    if args.exhaustive:
        run_exhaustive(workers=args.workers, budget_hours=args.budget_hours, dry_run=args.dry_run)
    if args.build:
        build_trip_times_merged()

    if not any([args.analyze, args.fill, args.exhaustive, args.build]):
        ap.print_help()


if __name__ == "__main__":
    main()
