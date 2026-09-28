"""
Assembles a standard GTFS feed (agency/stops/routes/trips/stop_times/
calendar/shapes/feed_info .txt files) from the JSON artifacts
scrape_stops.py, scrape_routes.py, and scrape_shapes.py have already built:
  msrtc-stops.geojson, msrtc-routes.json, msrtc-trips.json,
  msrtc-trip-times.json, msrtc-shapes.json.

Weekday coverage
-----------------
Only weekdays with observed trips get service; a weekday we never crawled
is left with no service rather than guessed from a neighbouring day. Each
trip's service days are exactly its observed weekdays.

Per-trip timing
----------------
Each trip's origin departure time comes from msrtc-trips.json (added
alongside its weekday tags -- see build_trips_json() in scrape_routes.py).
Downstream stop times are built by walking the trip's route's stop
sequence and adding msrtc-trip-times.json's per-segment median duration
cumulatively. Segments with no computed duration (see
build_trip_times_json()'s docstring for why some are null) fall back to
(haversine distance between the two stops) / (a fleet-wide average speed
computed empirically from every segment that DOES have both a real
duration and a real distance) -- not an arbitrary guessed constant.

Also zips the result into <out-dir>.zip with the .txt files at the zip
root (not nested in a folder) -- required by the GTFS spec, and what any
consumer (Google, OpenTripPlanner, a validator) expects to unzip directly.
The zip step just archives whatever .txt files actually exist in --out-dir
(minus a small ignore-list like .DS_Store) rather than a hardcoded file
list -- same pattern as croyla/pmpml-gtfs's gtfs_parallel.py -- so adding a
new GTFS file later (fare_attributes.txt, frequencies.txt, ...) doesn't
also require editing the zip step.

Marathi translations
---------------------
msrtc-stops.geojson already carries each stop's Devanagari name
(stop_name_devanagari, ~98% coverage -- confirmed live) alongside the
Latin-script one, so translations.txt gets a "mr" row for every stop that
has one (table_name=stops, field_name=stop_name), and for every route's
long name too (built the same "ORIGIN TO DESTINATION" way as the English
one, just from each endpoint's Devanagari name and joined with "ते"
instead of "TO" -- skipped entirely for a route if either endpoint has no
Devanagari name, rather than emitting a half-translated string).

Parallelism
------------
The per-trip stop_times/trips row-building loop (the dominant cost for
~45k trips x up to tens of stops each) and shapes.txt's polyline decoding
are both embarrassingly parallel -- one trip/shape's work never depends on
another's -- so both run across a ProcessPoolExecutor (--workers, default
os.cpu_count()). Worker processes are seeded once via an initializer with
the read-only shared data (routes, trip_times, stop_info, etc.) rather
than having it re-pickled per chunk, and each worker returns partial
row-lists that main() concatenates -- CSV writing itself stays
single-process (I/O-bound, not the bottleneck, and GTFS row order doesn't
need to be preserved across chunks).

Usage:
  python3 build_gtfs.py [--out-dir gtfs] [--validity-days N] [--workers N]

Builds purely from the local JSON (no API access or key needed). The
calendar window starts at the date the data was crawled, not today.
"""

import argparse
import csv
import datetime as dt
import json
import math
import multiprocessing as mp
import os
import zipfile
from concurrent.futures import ProcessPoolExecutor

import numpy as np

from scrape_routes import WEEKDAY_ORDER
from scrape_shapes import decode_polyline

STOPS_GEOJSON = "msrtc-stops.geojson"
ROUTES_JSON = "msrtc-routes.json"
TRIPS_JSON = "msrtc-trips.json"
TRIP_TIMES_JSON = "msrtc-trip-times.json"
SHAPES_JSON = "msrtc-shapes.json"

AGENCY_ID = "MSRTC"
AGENCY_NAME = "Maharashtra State Road Transport Corporation"
AGENCY_URL = "https://msrtc.maharashtra.gov.in"
AGENCY_TIMEZONE = "Asia/Kolkata"

# feed_info.txt -- identifies this as an unofficial, derived feed (not
# published by MSRTC itself), same spirit as pmpml-gtfs's feed_info.txt.
# feed_publisher_url intentionally left blank -- no public repo/URL exists
# for this project yet (no git remote configured); fill in if/when one does.
FEED_PUBLISHER_NAME = "msrtc-gtfs-gen (unofficial)"
FEED_PUBLISHER_URL = ""

ZIP_IGNORE = {".DS_Store"}

TRANSLATION_LANG = "mr"
TRANSLATION_CONNECTOR = "ते"  # Marathi "to"

DEFAULT_VALIDITY_DAYS = 365
DEFAULT_WORKERS = os.cpu_count() or 4
DEFAULT_SHAPE_TOLERANCE_M = 10.0
FALLBACK_MIN_DURATION_S = 30  # floor so a ~0-distance segment doesn't get a 0s/negative duration


def _haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _route_long_name(stops, stop_info):
    """"ORIGIN TO DESTINATION", built from the route's OWN first/last stop
    names -- deliberately NOT the API's routeName field (e.g. "127259
    (SAKHARWADI TO PHALTAN VIA JINTI)"): that string embeds the route code
    and a "VIA ..." clause GTFS route_long_name isn't meant to carry, and
    it's only ever seen on getCurrentTripStatusAtStop entries, so it's
    missing for most routes anyway (only ones that happened to be "current"
    at one of our seed stops during a crawl). Deriving it from
    msrtc-routes.json's own stop sequence instead covers every route we
    have a stop sequence for, which is all of them."""
    if not stops:
        return ""
    origin = stop_info.get(stops[0], {}).get("name", f"Stop {stops[0]}")
    destination = stop_info.get(stops[-1], {}).get("name", f"Stop {stops[-1]}")
    return f"{origin} TO {destination}"


def _route_long_name_mr(stops, stop_info):
    """Marathi counterpart of _route_long_name() -- None (not a partial
    string) if EITHER endpoint has no Devanagari name, since a half-Latin,
    half-Devanagari long name would be worse than no translation row at
    all for that route."""
    if not stops:
        return None
    origin = stop_info.get(stops[0], {}).get("name_mr")
    destination = stop_info.get(stops[-1], {}).get("name_mr")
    if not (origin and destination):
        return None
    return f"{origin} {TRANSLATION_CONNECTOR} {destination}"


def _seconds_from_midnight(hms):
    h, m, s = (int(x) for x in hms.split(":"))
    return h * 3600 + m * 60 + s


def _format_gtfs_time(total_seconds):
    """GTFS allows/expects hours >= 24 for trips that roll past midnight,
    rather than wrapping back to 00:xx -- that's how a rider (or a
    trip-planning tool) distinguishes "1am the same service-day" from
    "1am, i.e. 25 hours after the service day started"."""
    total_seconds = max(0, round(total_seconds))
    h, rem = divmod(total_seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def simplify_polyline(points, tolerance_m):
    """Douglas-Peucker on an equirectangular projection (metres, centred on
    the shape's mean latitude -- accurate enough for a tolerance of metres
    over a route-sized extent). Keeps endpoints; iterative (explicit stack)
    so a long shape can't hit the recursion limit."""
    n = len(points)
    if tolerance_m <= 0 or n < 3:
        return points
    pts = np.asarray(points, dtype=float)
    lat0 = math.radians(pts[:, 0].mean())
    xy = np.column_stack([np.radians(pts[:, 1]) * math.cos(lat0), np.radians(pts[:, 0])]) * 6371000.0
    keep = np.zeros(n, dtype=bool)
    keep[0] = keep[-1] = True
    stack = [(0, n - 1)]
    while stack:
        lo, hi = stack.pop()
        if hi <= lo + 1:
            continue
        a, b = xy[lo], xy[hi]
        seg = xy[lo + 1:hi]
        ab = b - a
        L2 = ab @ ab
        if L2 == 0:
            d = np.hypot(*(seg - a).T)
        else:
            t = np.clip(((seg - a) @ ab) / L2, 0, 1)
            d = np.hypot(*(seg - (a + t[:, None] * ab)).T)
        i = int(d.argmax())
        if d[i] > tolerance_m:
            m = lo + 1 + i
            keep[m] = True
            stack.append((lo, m))
            stack.append((m, hi))
    return [points[i] for i in np.flatnonzero(keep)]


def _decode_shape_chunk(args):
    """args: (items, tolerance_m); items: list of (route_code,
    encoded_polyline). Runs in a worker process -- decoding + simplifying
    is pure CPU work per-route, independent across routes, so this
    parallelizes trivially. Coordinates are rounded to 5 dp (~1.1 m) after
    simplification so near-identical shapes collapse to identical tuples
    for de-duplication."""
    items, tolerance_m = args
    return [(rc, [(round(la, 5), round(lo, 5)) for la, lo in simplify_polyline(decode_polyline(enc), tolerance_m)])
            for rc, enc in items]


# Populated once per worker process by _init_trip_worker(), not re-sent per
# chunk -- ProcessPoolExecutor's initializer runs once per worker, so this
# is the standard way to share large read-only data with a process pool
# without paying pickling costs on every single task.
_worker = {}


def _init_trip_worker(routes, trip_times, stop_info, trip_service_id, shape_of_route, fallback_speed_kmh):
    _worker.update(routes=routes, trip_times=trip_times, stop_info=stop_info,
                    trip_service_id=trip_service_id, shape_of_route=shape_of_route,
                    fallback_speed_kmh=fallback_speed_kmh)


def _worker_segment_duration(route_code, a, b, precomputed):
    if precomputed:
        return precomputed["duration_seconds"]
    stop_info = _worker["stop_info"]
    if a in stop_info and b in stop_info:
        km = _haversine_km(stop_info[a]["lat"], stop_info[a]["lon"], stop_info[b]["lat"], stop_info[b]["lon"])
        return max(FALLBACK_MIN_DURATION_S, (km / _worker["fallback_speed_kmh"]) * 3600)
    return FALLBACK_MIN_DURATION_S


def _process_trip_chunk(trip_chunk):
    """trip_chunk: list of (trip_id, trip_dict). Runs in a worker process
    against the shared read-only data _init_trip_worker() stashed in
    _worker; returns partial row-lists for the caller to concatenate."""
    routes = _worker["routes"]
    trip_times = _worker["trip_times"]
    trip_service_id = _worker["trip_service_id"]
    shape_of_route = _worker["shape_of_route"]

    trip_rows, stop_time_rows = [], []
    referenced_stops, referenced_routes = set(), set()
    skipped_no_route = skipped_no_departure = 0

    for tid, v in trip_chunk:
        route_code = v["route_code"]
        stops = routes.get(route_code)
        if not stops:
            skipped_no_route += 1
            continue
        if not v.get("departure_time"):
            skipped_no_departure += 1
            continue

        durations = trip_times.get(route_code) or [None] * (len(stops) - 1)
        shape_id = shape_of_route.get(route_code, "")

        trip_rows.append({
            "route_id": route_code, "service_id": trip_service_id[tid], "trip_id": tid,
            "shape_id": shape_id,
        })
        referenced_routes.add(route_code)

        t = _seconds_from_midnight(v["departure_time"])
        for i, sid in enumerate(stops):
            referenced_stops.add(sid)
            stop_time_rows.append({
                "trip_id": tid, "arrival_time": _format_gtfs_time(t), "departure_time": _format_gtfs_time(t),
                "stop_id": sid, "stop_sequence": i,
            })
            if i < len(stops) - 1:
                t += _worker_segment_duration(route_code, stops[i], stops[i + 1],
                                               durations[i] if i < len(durations) else None)

    return trip_rows, stop_time_rows, referenced_stops, referenced_routes, skipped_no_route, skipped_no_departure


def _chunked(items, n_chunks):
    items = list(items)
    if not items:
        return []
    size = max(1, -(-len(items) // n_chunks))  # ceil division
    return [items[i:i + size] for i in range(0, len(items), size)]


def _crawl_date():
    """Date the local data was last queried: newest mtime among the trip
    data files. Used as the start of the service window (not today), since
    the data is a snapshot of when it was queried."""
    mtimes = [os.path.getmtime(p) for p in (TRIPS_JSON, TRIP_TIMES_JSON, ROUTES_JSON) if os.path.exists(p)]
    return dt.date.fromtimestamp(max(mtimes))


def build_gtfs(out_dir="gtfs", validity_days=DEFAULT_VALIDITY_DAYS, zip_output=True, workers=DEFAULT_WORKERS,
               shape_tolerance_m=DEFAULT_SHAPE_TOLERANCE_M):
    with open(STOPS_GEOJSON, encoding="utf-8") as f:
        geo = json.load(f)
    stop_info = {}
    for feat in geo["features"]:
        p = feat["properties"]
        lon, lat = feat["geometry"]["coordinates"]
        stop_info[p["stop_id"]] = {
            "name": p.get("stop_name") or f"Stop {p['stop_id']}",
            "name_mr": p.get("stop_name_devanagari") or None,
            "lat": lat, "lon": lon,
        }

    with open(ROUTES_JSON, encoding="utf-8") as f:
        routes = json.load(f)
    with open(TRIPS_JSON, encoding="utf-8") as f:
        trips = json.load(f)
    with open(TRIP_TIMES_JSON, encoding="utf-8") as f:
        trip_times = json.load(f)
    shapes_encoded = {}
    if os.path.exists(SHAPES_JSON):
        with open(SHAPES_JSON, encoding="utf-8") as f:
            shapes_encoded = json.load(f)
    print(f"Decoding {len(shapes_encoded)} shape(s) across {workers} worker(s)...")
    shapes = {}
    if shapes_encoded:
        with ProcessPoolExecutor(max_workers=workers) as ex:
            chunks = [(c, shape_tolerance_m) for c in _chunked(shapes_encoded.items(), workers * 4)]
            for chunk_result in ex.map(_decode_shape_chunk, chunks):
                shapes.update(chunk_result)

    # Many routes share an identical polyline (e.g. the same road path with
    # different stop patterns), so emit each distinct point sequence once and
    # point every route at it.
    shape_of_route = {}
    shape_pts = {}  # shape_id -> points
    shape_id_of_pts = {}
    for rc in sorted(shapes):
        key = tuple(shapes[rc])
        if key not in shape_id_of_pts:
            sid = f"shp_{len(shape_id_of_pts) + 1}"
            shape_id_of_pts[key] = sid
            shape_pts[sid] = shapes[rc]
        shape_of_route[rc] = shape_id_of_pts[key]
    print(f"Shapes: {len(shapes)} route shape(s) -> {len(shape_pts)} distinct after simplifying "
          f"(tolerance {shape_tolerance_m:g} m), {sum(len(p) for p in shape_pts.values())} points total")

    # --- weekday coverage ----------------------------------------------
    available_weekdays = {w for v in trips.values() for w in v["weekdays"]}
    print(f"Weekdays with data: {sorted(available_weekdays, key=WEEKDAY_ORDER.index)}")

    # --- calendar.txt: one row per DISTINCT resulting 7-day mask -------
    service_of_mask = {}
    calendar_rows = []
    trip_service_id = {}
    for tid, v in trips.items():
        mask = frozenset(v["weekdays"])
        if mask not in service_of_mask:
            sid = "svc_" + "_".join(d[:3] for d in WEEKDAY_ORDER if d in mask) if mask else "svc_none"
            service_of_mask[mask] = sid
            calendar_rows.append((sid, mask))
        trip_service_id[tid] = service_of_mask[mask]

    # --- empirical fallback speed for segments with no computed duration
    dist_dur_pairs = []
    for route_code, durations in trip_times.items():
        stops = routes.get(route_code)
        if not stops:
            continue
        for (a, b), d in zip(zip(stops, stops[1:]), durations):
            if not d or a not in stop_info or b not in stop_info:
                continue
            km = _haversine_km(stop_info[a]["lat"], stop_info[a]["lon"], stop_info[b]["lat"], stop_info[b]["lon"])
            if km > 0 and d["duration_seconds"] > 0:
                dist_dur_pairs.append(km / (d["duration_seconds"] / 3600))
    fallback_speed_kmh = sorted(dist_dur_pairs)[len(dist_dur_pairs) // 2] if dist_dur_pairs else 20.0
    print(f"Empirical fallback speed for segments with no computed duration: {fallback_speed_kmh:.1f} km/h "
          f"(from {len(dist_dur_pairs)} real segment samples)")

    # --- assemble trips.txt + stop_times.txt, in parallel ----------------
    print(f"Building trips/stop_times for {len(trips)} trip(s) across {workers} worker(s)...")
    referenced_stops = set()
    referenced_routes = set()
    trip_rows = []
    stop_time_rows = []
    skipped_no_route = skipped_no_departure = 0

    trip_chunks = _chunked(trips.items(), workers * 4)
    with ProcessPoolExecutor(
        max_workers=workers, initializer=_init_trip_worker,
        initargs=(routes, trip_times, stop_info, trip_service_id, shape_of_route, fallback_speed_kmh),
    ) as ex:
        for tr, sr, rs, rr, snr, snd in ex.map(_process_trip_chunk, trip_chunks):
            trip_rows.extend(tr)
            stop_time_rows.extend(sr)
            referenced_stops |= rs
            referenced_routes |= rr
            skipped_no_route += snr
            skipped_no_departure += snd

    print(f"Trips: {len(trip_rows)} written, {skipped_no_route} skipped (route not in {ROUTES_JSON}), "
          f"{skipped_no_departure} skipped (no captured departure time)")

    # --- write files -----------------------------------------------------
    os.makedirs(out_dir, exist_ok=True)

    def write_csv(fname, fieldnames, rows):
        with open(f"{out_dir}/{fname}", "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(rows)
        print(f"  {fname}: {len(rows)} row(s)")

    write_csv("agency.txt", ["agency_id", "agency_name", "agency_url", "agency_timezone"], [{
        "agency_id": AGENCY_ID, "agency_name": AGENCY_NAME, "agency_url": AGENCY_URL,
        "agency_timezone": AGENCY_TIMEZONE,
    }])

    write_csv("stops.txt", ["stop_id", "stop_name", "stop_lat", "stop_lon"], [
        {"stop_id": sid, "stop_name": stop_info[sid]["name"], "stop_lat": stop_info[sid]["lat"],
         "stop_lon": stop_info[sid]["lon"]}
        for sid in sorted(referenced_stops) if sid in stop_info
    ])

    write_csv("routes.txt",
              ["route_id", "agency_id", "route_short_name", "route_long_name", "route_type"], [
        {"route_id": rc, "agency_id": AGENCY_ID, "route_short_name": rc,
         "route_long_name": _route_long_name(routes.get(rc), stop_info), "route_type": 3}
        for rc in sorted(referenced_routes)
    ])

    write_csv("trips.txt", ["route_id", "service_id", "trip_id", "shape_id"], trip_rows)

    write_csv("stop_times.txt",
              ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"], stop_time_rows)

    start_date = _crawl_date()
    print(f"Service window starts at data crawl date {start_date}")
    end_date = start_date + dt.timedelta(days=validity_days)
    write_csv("calendar.txt",
              ["service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday",
               "start_date", "end_date"], [
        {"service_id": sid, **{d: int(d in mask) for d in WEEKDAY_ORDER},
         "start_date": start_date.strftime("%Y%m%d"), "end_date": end_date.strftime("%Y%m%d")}
        for sid, mask in calendar_rows
    ])

    shape_rows = []
    used_shapes = sorted({shape_of_route[rc] for rc in referenced_routes if rc in shape_of_route},
                         key=lambda x: int(x[4:]))
    for sid in used_shapes:
        for i, (lat, lon) in enumerate(shape_pts[sid]):
            shape_rows.append({"shape_id": sid, "shape_pt_lat": lat, "shape_pt_lon": lon, "shape_pt_sequence": i})
    write_csv("shapes.txt", ["shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"], shape_rows)

    write_csv("feed_info.txt",
              ["feed_publisher_name", "feed_publisher_url", "feed_lang", "default_lang", "feed_start_date",
               "feed_end_date", "feed_contact_url"], [{
        "feed_publisher_name": FEED_PUBLISHER_NAME, "feed_publisher_url": FEED_PUBLISHER_URL, "feed_lang": "en",
        "default_lang": "en",
        "feed_start_date": start_date.strftime("%Y%m%d"), "feed_end_date": end_date.strftime("%Y%m%d"),
        "feed_contact_url": FEED_PUBLISHER_URL,
    }])

    translation_rows = []
    for sid in sorted(referenced_stops):
        name_mr = stop_info.get(sid, {}).get("name_mr")
        if name_mr:
            translation_rows.append({"table_name": "stops", "field_name": "stop_name", "language": TRANSLATION_LANG,
                                      "translation": name_mr, "record_id": sid})
    skipped_route_translations = 0
    for rc in sorted(referenced_routes):
        long_name_mr = _route_long_name_mr(routes.get(rc), stop_info)
        if long_name_mr:
            translation_rows.append({"table_name": "routes", "field_name": "route_long_name",
                                      "language": TRANSLATION_LANG, "translation": long_name_mr, "record_id": rc})
        else:
            skipped_route_translations += 1
    write_csv("translations.txt", ["table_name", "field_name", "language", "translation", "record_id"],
               translation_rows)
    if skipped_route_translations:
        print(f"  ({skipped_route_translations} route(s) skipped for translation -- missing a Devanagari "
              f"name at one or both endpoints)")

    print(f"GTFS feed written to {out_dir}/")

    if zip_output:
        zip_path = f"{out_dir.rstrip('/')}.zip"
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            # Archives whatever .txt files actually exist in out_dir, not a
            # hardcoded list -- so a new GTFS file added later doesn't also
            # need a matching edit here (see croyla/pmpml-gtfs's gtfs_parallel.py).
            for fname in sorted(os.listdir(out_dir)):
                if fname in ZIP_IGNORE:
                    continue
                # arcname (2nd arg) is deliberately just the bare filename, not
                # out_dir/filename -- the GTFS spec requires the .txt files at
                # the zip ROOT, not nested in a folder, or most consumers
                # (Google, OpenTripPlanner, validators) won't find them.
                zf.write(f"{out_dir}/{fname}", arcname=fname)
        print(f"Zipped to {zip_path} ({os.path.getsize(zip_path) / 1e6:.1f} MB)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="gtfs", help="output directory (default: gtfs)")
    ap.add_argument("--validity-days", type=int, default=DEFAULT_VALIDITY_DAYS,
                     help=f"calendar.txt validity window in days from today (default {DEFAULT_VALIDITY_DAYS})")
    ap.add_argument("--no-zip", action="store_true", help="skip zipping (leave just the .txt files in --out-dir)")
    ap.add_argument("--shape-tolerance-m", type=float, default=DEFAULT_SHAPE_TOLERANCE_M,
                     help=f"Douglas-Peucker tolerance for shapes in metres; 0 disables simplification "
                          f"(default {DEFAULT_SHAPE_TOLERANCE_M:g})")
    ap.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                     help=f"parallel worker processes for shape decoding + trip/stop_times building "
                          f"(default {DEFAULT_WORKERS}, i.e. os.cpu_count())")
    args = ap.parse_args()
    build_gtfs(out_dir=args.out_dir, validity_days=args.validity_days, zip_output=not args.no_zip,
               workers=args.workers,
               shape_tolerance_m=args.shape_tolerance_m)


if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)  # explicit, not just relying on the platform default
    main()
