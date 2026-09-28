"""
Builds msrtc-shapes.json (route code -> encoded polyline) from
mobileapi.msrtcvtspis.com via getPathByRoute. Third step of the new-API
GTFS pipeline, after scrape_routes.py.

getPathByRoute (POST, {"routeCode": ...}) returns a flat list of lat/lng
points tagged with linkSequence/pointSequence. A "link" is one road segment
of the route's path, distinct from a route STOP -- confirmed live: a route
with 9 consecutive stops had only 9 links but 1519 total points, i.e. many
points per link, giving the actual road-following geometry, not just
straight stop-to-stop lines. Points come back already sorted by
(linkSequence, pointSequence), but this code sorts again defensively rather
than trusting that.

This corridor geometry is (almost) static -- unlike trip/schedule data it
doesn't vary day-to-day, so raw/path_by_route/<route_code>.json is NOT
weekday-partitioned like scrape_routes.py's caches are (weekday is
irrelevant here). It's not cached forever either, though: roads do
occasionally get realigned/added, so a plain age-based TTL
(PATH_CACHE_TTL_DAYS, no time-of-day component -- there's no "next 24h
window" behavior to work around here) refetches a route's path if the
cached copy is older than that, same 3-month figure as
scrape_routes.py's WEEKDAY_CACHE_TTL_DAYS but without the weekday bucketing
since it doesn't apply.

Usage:
  python3 scrape_shapes.py --fetch   # fetch getPathByRoute for every route in msrtc-routes.json
  python3 scrape_shapes.py --build   # aggregate raw/path_by_route/ into msrtc-shapes.json
"""

import argparse
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from scrape_stops import BASE, HEADERS, DEFAULT_WORKERS, _request_with_retry
from scrape_routes import _cached, STOPS_GEOJSON

RAW_PATH_BY_ROUTE_DIR = "raw/path_by_route"
ROUTES_JSON = "msrtc-routes.json"
OUT_SHAPES_JSON = "msrtc-shapes.json"

PATH_CACHE_TTL_DAYS = 90  # ~3 months

JSON_HEADERS = {**HEADERS, "Content-Type": "application/json"}


def get_path_by_route(route_code):
    return _request_with_retry(
        "POST", f"{BASE}/path/getPathByRoute",
        headers=JSON_HEADERS, json={"routeCode": route_code},
    )


def _path_is_stale(path):
    return (time.time() - os.path.getmtime(path)) / 86400 > PATH_CACHE_TTL_DAYS


def _cached_path_by_route(route_code):
    return _cached(f"{RAW_PATH_BY_ROUTE_DIR}/{route_code}.json", lambda: get_path_by_route(route_code),
                    is_stale=_path_is_stale)


def _fetch_one(route_code):
    try:
        data = _cached_path_by_route(route_code)
    except requests.exceptions.RequestException as e:
        print(f"  route {route_code}: FAILED after retries ({e.__class__.__name__}), skipping")
        return None
    return data.get("count", 0)


def run_fetch(workers=DEFAULT_WORKERS):
    if not os.path.exists(ROUTES_JSON):
        raise SystemExit(f"{ROUTES_JSON} not found -- run scrape_routes.py --discover then --build first")
    with open(ROUTES_JSON, encoding="utf-8") as f:
        routes = json.load(f)
    route_codes = list(routes.keys())
    print(f"Fetching path geometry for {len(route_codes)} route(s) ({workers} parallel workers)")
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(_fetch_one, rc) for rc in route_codes]
        for i, fut in enumerate(as_completed(futures), 1):
            fut.result()
            if i % 500 == 0 or i == len(route_codes):
                print(f"[{i}/{len(route_codes)} routes done]")
    print("Done.")


def _encode_number(num):
    num = num << 1
    if num < 0:
        num = ~num
    chunks = []
    while num >= 0x20:
        chunks.append(chr((0x20 | (num & 0x1f)) + 63))
        num >>= 5
    chunks.append(chr(num + 63))
    return "".join(chunks)


def encode_polyline(coords, precision=5):
    """Standard Google polyline encoding (delta + varint + zigzag) -- the
    de facto standard for GTFS shapes-adjacent tooling, no external
    dependency needed since the algorithm is ~15 lines."""
    factor = 10 ** precision
    out = []
    prev_lat = prev_lng = 0
    for lat, lng in coords:
        lat_i = round(lat * factor)
        lng_i = round(lng * factor)
        out.append(_encode_number(lat_i - prev_lat))
        out.append(_encode_number(lng_i - prev_lng))
        prev_lat, prev_lng = lat_i, lng_i
    return "".join(out)


def decode_polyline(encoded, precision=5):
    """Inverse of encode_polyline() -- needed by build_gtfs.py since GTFS
    shapes.txt wants flat (lat, lng) point rows, not an encoded string."""
    factor = 10 ** precision
    coords = []
    index = lat = lng = 0
    while index < len(encoded):
        for is_lat in (True, False):
            shift = result = 0
            while True:
                b = ord(encoded[index]) - 63
                index += 1
                result |= (b & 0x1f) << shift
                shift += 5
                if b < 0x20:
                    break
            delta = ~(result >> 1) if result & 1 else (result >> 1)
            if is_lat:
                lat += delta
            else:
                lng += delta
        coords.append((lat / factor, lng / factor))
    return coords


def build_shapes_json():
    if not os.path.isdir(RAW_PATH_BY_ROUTE_DIR):
        raise SystemExit(f"{RAW_PATH_BY_ROUTE_DIR}/ not found -- run --fetch first")
    shapes = {}
    empty = 0
    for fname in os.listdir(RAW_PATH_BY_ROUTE_DIR):
        if not fname.endswith(".json"):
            continue
        route_code = fname[:-5]
        with open(f"{RAW_PATH_BY_ROUTE_DIR}/{fname}", encoding="utf-8") as f:
            data = json.load(f)
        points = data.get("data") or []
        if not points:
            empty += 1
            continue
        points = sorted(points, key=lambda p: (p["linkSequence"], p["pointSequence"]))
        coords = [(p["pointLat"], p["pointLng"]) for p in points]
        shapes[route_code] = encode_polyline(coords)
    with open(OUT_SHAPES_JSON, "w", encoding="utf-8") as f:
        json.dump(shapes, f, indent=2, ensure_ascii=False)
    print(f"Wrote {OUT_SHAPES_JSON}: {len(shapes)} route(s) with a shape "
          f"({empty} route(s) had no path data, skipped)")


def _build_segment_index(routes, shapes):
    """(stop_a, stop_b) -> point list, sourced from every ALREADY-SHAPED
    route's own consecutive stop pairs -- lets a route missing its own
    getPathByRoute data borrow real road geometry for whichever of its
    segments some other route happens to also travel.

    Only trusted when a route's link count equals its own pair count
    (confirmed live: this holds for most but not all routes -- e.g. two
    stops close enough together sometimes share one link -- so an
    untrustworthy route is skipped entirely rather than risking a
    mis-aligned link-to-pair zip)."""
    index = {}
    for rc, stops in routes.items():
        encoded = shapes.get(rc)
        if not encoded or len(stops) < 2:
            continue
        path = f"{RAW_PATH_BY_ROUTE_DIR}/{rc}.json"
        if not os.path.exists(path):
            continue
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        points = data.get("data") or []
        if not points:
            continue
        links = {}
        for p in points:
            links.setdefault(p["linkSequence"], []).append(p)
        link_seqs = sorted(links)
        pairs = list(zip(stops, stops[1:]))
        if len(link_seqs) != len(pairs):
            continue
        for (a, b), seq in zip(pairs, link_seqs):
            seg = sorted(links[seq], key=lambda p: p["pointSequence"])
            index.setdefault((a, b), [(p["pointLat"], p["pointLng"]) for p in seg])
    return index


def build_frankenstein_shapes():
    """Fills in shapes for routes getPathByRoute returned nothing for, by
    stitching together real per-segment geometry BORROWED from other
    routes that happen to share the same consecutive stop pair (same
    direction, or reversed) -- see _build_segment_index(). A pair no other
    route covers falls back to a straight line between the two stops'
    own coordinates (msrtc-stops.geojson), so the result is always a
    complete, position-accurate-at-the-stops shape, just not
    road-accurate for whichever segments had no donor."""
    if not os.path.exists(ROUTES_JSON):
        raise SystemExit(f"{ROUTES_JSON} not found -- run scrape_routes.py --build first")
    if not os.path.exists(OUT_SHAPES_JSON):
        raise SystemExit(f"{OUT_SHAPES_JSON} not found -- run --build first")
    with open(ROUTES_JSON, encoding="utf-8") as f:
        routes = json.load(f)
    with open(OUT_SHAPES_JSON, encoding="utf-8") as f:
        shapes = json.load(f)

    missing = [rc for rc in routes if rc not in shapes]
    if not missing:
        print("No routes missing a shape -- nothing to fill.")
        return

    stop_coords = {}
    if os.path.exists(STOPS_GEOJSON):
        with open(STOPS_GEOJSON, encoding="utf-8") as f:
            geo = json.load(f)
        for feat in geo["features"]:
            lon, lat = feat["geometry"]["coordinates"]
            stop_coords[feat["properties"]["stop_id"]] = (lat, lon)

    segment_index = _build_segment_index(routes, shapes)
    print(f"Segment index: {len(segment_index)} distinct stop-pair segment(s) borrowable from shaped routes")

    filled = 0
    total_pairs = borrowed_pairs = straight_line_pairs = 0
    for rc in missing:
        stops = routes[rc]
        if len(stops) < 2:
            continue
        coords = [stop_coords[stops[0]]] if stops[0] in stop_coords else []
        for a, b in zip(stops, stops[1:]):
            total_pairs += 1
            if (a, b) in segment_index:
                coords.extend(segment_index[(a, b)])
                borrowed_pairs += 1
            elif (b, a) in segment_index:
                coords.extend(reversed(segment_index[(b, a)]))
                borrowed_pairs += 1
            elif a in stop_coords and b in stop_coords:
                coords.append(stop_coords[b])
                straight_line_pairs += 1
        if len(coords) >= 2:
            shapes[rc] = encode_polyline(coords)
            filled += 1

    with open(OUT_SHAPES_JSON, "w", encoding="utf-8") as f:
        json.dump(shapes, f, indent=2, ensure_ascii=False)
    print(f"Filled {filled}/{len(missing)} missing route(s) -- {borrowed_pairs}/{total_pairs} segment(s) "
          f"borrowed from another route's real geometry, {straight_line_pairs}/{total_pairs} fell back to a "
          f"straight line between stops")
    print(f"Wrote {OUT_SHAPES_JSON}: {len(shapes)} route(s) total")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fetch", action="store_true", help="fetch getPathByRoute for every route in msrtc-routes.json")
    ap.add_argument("--build", action="store_true", help="aggregate raw/path_by_route/ into msrtc-shapes.json")
    ap.add_argument("--fill-gaps", action="store_true",
                     help="fill routes with no getPathByRoute data by stitching real segment geometry "
                          "borrowed from other routes sharing the same stop pairs, falling back to a "
                          "straight line between stops for any pair no other route covers")
    ap.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                     help=f"parallel requests for --fetch (default {DEFAULT_WORKERS})")
    args = ap.parse_args()

    if args.fetch:
        run_fetch(workers=args.workers)
    if args.build:
        build_shapes_json()
    if args.fill_gaps:
        build_frankenstein_shapes()

    if not any([args.fetch, args.build, args.fill_gaps]):
        ap.print_help()


if __name__ == "__main__":
    main()
