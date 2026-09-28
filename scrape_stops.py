"""
Builds msrtc-stops.geojson from mobileapi.msrtcvtspis.com -- a more
comprehensive, undocumented API discovered separately from the reservation
site this project originally scraped (see old/). This is the first step of
an entirely separate GTFS-generation endeavor against that API; nothing
here touches old/.

Two independent discovery strategies, both writing raw responses to raw/
(so a run is resumable/cacheable -- an interrupted run just re-skips
whatever's already on disk):

1. Text search (getMatchingStops/<query>, min 3 chars) -- SUBSTRING match
   on stop name, not prefix-only (confirmed: querying "pun" matches
   "SWARGATE, PUNE" and "NEW SHIVAJI NAGAR PUNE", neither of which starts
   with "pun"). Capped at 20 results per query with NO working pagination
   (confirmed live: ?limit=100 and ?page=2 are both silently ignored,
   identical response either way) -- so a query returning exactly 20 is
   presumed truncated and gets recursively expanded (try appending each
   possible next character) until every branch returns under 20 or hits
   MAX_QUERY_LEN, rather than trusting a flat enumeration to be complete.

2. Geocoded search (getNearbyStops, POST lat/lon + searchDistance) -- run
   once per terminus location already geocoded by the OLD GIS scrape
   (old/data/gis/bus_terminus.json), to catch real stops that never
   surface via any text substring at all (e.g. a stop whose name doesn't
   share 3+ characters with anything an obvious query would try).

Both endpoints work with ONLY the X-API-Key header -- confirmed live: the
Cookie/X-XSRF-TOKEN/sessionToken headers seen in the app's own traffic are
NOT required for either search endpoint (likely only needed for the actual
booking flow), so this script doesn't send them at all.

Usage:
  python3 scrape_stops.py --matching-test     # small sample of matching-stops queries, sanity check
  python3 scrape_stops.py --matching          # full adaptive matching-stops crawl
  python3 scrape_stops.py --nearby            # nearby-stops sweep over all GIS termini
  python3 scrape_stops.py --build             # aggregate raw/ into msrtc-stops.geojson
"""

import argparse
import itertools
import json
import os
import string
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from dotenv import load_dotenv

load_dotenv()

BASE = "https://mobileapi.msrtcvtspis.com"
# Not required at import time: build_gtfs.py --local imports this module
# (transitively) but never calls the API. Any real request goes through
# require_api_key(), which fails loudly if .env is missing/wrong.
API_KEY = os.environ.get("MSRTC_API_KEY")

HEADERS = {
    "Accept": "*/*",
    "X-API-Key": API_KEY or "",
}


def require_api_key():
    if not API_KEY:
        raise RuntimeError("MSRTC_API_KEY is not set -- add it to .env (see .env.example)")

RAW_MATCHING_DIR = "raw/matching_stops"
RAW_NEARBY_DIR = "raw/nearby_stops"
OUT_GEOJSON = "msrtc-stops.geojson"

MATCH_CAP = 20  # confirmed live: exactly this many results back = presumed truncated
MIN_QUERY_LEN = 3  # confirmed live: shorter queries are rejected/ignored by the API
MAX_QUERY_LEN = 6  # depth ceiling for the recursive expansion, see get_matching_stops_adaptive()
NEARBY_SEARCH_DISTANCE_M = 15000
REQUEST_RETRIES = 4  # confirmed live: the very first request of a run can time out cold, not just
                      # mid-run flakiness -- this needs to be resilient from request #1, not just "eventually"


def _request_with_retry(method, url, **kwargs):
    """Transient network errors (timeouts, connection resets) are real and
    confirmed here (a run's very FIRST request timed out cold, killing the
    whole process with no progress saved -- there was no retry/error
    handling at all before this). Exponential backoff, not a fixed delay,
    since a cold/slow endpoint may just need a bit more time on the next
    attempt. Raises on the last attempt so a truly-broken run still surfaces
    loudly rather than looping forever -- the caller is expected to catch
    that and log a final "FAILED after retries" message with whatever
    domain-specific ID (stop_id/route_code/pair) it has; this function only
    knows the raw HTTP request, so it logs THAT instead (method, url, body)
    so a failing request can be reproduced with a manual curl without
    digging through source to find what was actually sent."""
    require_api_key()
    last_exc = None
    for attempt in range(REQUEST_RETRIES):
        try:
            r = requests.request(method, url, timeout=30, **kwargs)
            r.raise_for_status()
            return r.json()
        except (requests.exceptions.RequestException, ValueError) as e:
            last_exc = e
            body = kwargs.get("json")
            detail = f"{method} {url}" + (f" -- body={json.dumps(body)}" if body is not None else "")
            if attempt < REQUEST_RETRIES - 1:
                wait = 2 ** attempt
                print(f"    (request error: {e.__class__.__name__}, retrying in {wait}s... [{detail}])")
                time.sleep(wait)
            else:
                print(f"    (request error: {e.__class__.__name__}, all {REQUEST_RETRIES} attempts failed [{detail}])")
    raise last_exc


def get_matching_stops(query):
    return _request_with_retry("GET", f"{BASE}/stop/getMatchingStops/{query}", headers=HEADERS)


NEARBY_DISTANCE_FLOOR_M = 1500  # don't shrink the search radius into uselessness chasing a timeout

# Confirmed live, direct measurement at one real (timing-out) terminus:
# response time scales STEEPLY with searchDistance, not a minor effect --
# 15000m timed out outright (>30s), 5000m took 11.5s, 2000m/1000m/500m all
# came back in 2-5s. A small fixed step-down (originally -500m/attempt,
# 15000->14500->14000->13500 across all retries) never leaves the slow
# zone at all -- halving does, in 2-3 attempts instead of never.
NEARBY_DISTANCE_HALVING_FACTOR = 0.5


def get_nearby_stops(lat, lon, distance_m=NEARBY_SEARCH_DISTANCE_M):
    """Same request-retry loop as _request_with_retry, but a ReadTimeout
    specifically also HALVES searchDistance each attempt (down to
    NEARBY_DISTANCE_FLOOR_M) -- other error types don't shrink the radius,
    since those aren't evidence the radius itself is the problem."""
    require_api_key()
    last_exc = None
    for attempt in range(REQUEST_RETRIES):
        try:
            r = requests.post(
                f"{BASE}/stop/getNearbyStops",
                headers={**HEADERS, "Content-Type": "application/json"},
                json={"requestId": "scrape_stops", "location": {"latitude": lat, "longitude": lon},
                      "searchDistance": distance_m},
                timeout=30,
            )
            r.raise_for_status()
            return r.json()
        except requests.exceptions.ReadTimeout as e:
            last_exc = e
            if attempt < REQUEST_RETRIES - 1:
                distance_m = max(NEARBY_DISTANCE_FLOOR_M, int(distance_m * NEARBY_DISTANCE_HALVING_FACTOR))
                wait = 2 ** attempt
                print(f"    (timeout, halving searchDistance to {distance_m}m, retrying in {wait}s...)")
                time.sleep(wait)
        except (requests.exceptions.RequestException, ValueError) as e:
            last_exc = e
            if attempt < REQUEST_RETRIES - 1:
                wait = 2 ** attempt
                print(f"    (request error: {e.__class__.__name__}, retrying in {wait}s...)")
                time.sleep(wait)
    raise last_exc


def _cached_matching_query(query):
    path = f"{RAW_MATCHING_DIR}/{query}.json"
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    data = get_matching_stops(query)
    os.makedirs(RAW_MATCHING_DIR, exist_ok=True)
    tmp = f"{path}.tmp.{os.getpid()}"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)
    time.sleep(0.15)
    return data


def crawl_matching_stops(query, seen_queries, results_out):
    """Recursively expands `query` in BOTH directions when capped -- see
    module docstring's "1. Text search" for why append-only isn't enough:
    confirmed live, appending only ever narrows toward "<query>+something"
    matches (e.g. "pur" -> "puri"), which can NEVER recover a real match
    where the query is a SUFFIX of the stop name (e.g. "SOLAPUR", "NAGPUR"
    contain "pur" but nothing follows it) -- those only come back by
    prepending instead (confirmed: "apur" surfaces SOLAPUR/KOLHAPUR/etc that
    "pur"/"purX" never did). Even bidirectional expansion isn't a
    mathematically guaranteed-complete search for the most common
    substrings (confirmed: "apur" ALSO hits the cap) -- MAX_QUERY_LEN bounds
    the recursion so this doesn't explode combinatorially, and the
    getNearbyStops geocoded sweep (see run_nearby_sweep()) is the real
    completeness backstop for whatever text search still misses, since it
    has no substring-matching blind spot at all.

    seen_queries avoids re-querying/re-expanding the same string twice
    across different recursion branches (bidirectional expansion means the
    same string can be reached two different ways, e.g. "apur" via
    prepending 'a' to "pur" and "apu" -> appending 'r')."""
    if query in seen_queries or len(query) > MAX_QUERY_LEN:
        return
    seen_queries.add(query)
    try:
        data = _cached_matching_query(query)
    except requests.exceptions.RequestException as e:
        # All REQUEST_RETRIES exhausted -- a genuinely dead/unreachable
        # endpoint for this one query shouldn't kill the whole crawl (many
        # hours of other queries' progress). Not cached (no file written),
        # so a re-run retries it for real rather than silently treating it
        # as "0 results".
        print(f"  '{query}' -> FAILED after retries ({e.__class__.__name__}), skipping")
        return
    count = data.get("count", 0)
    results_out[query] = data
    print(f"  '{query}' -> {count} result(s)" + (" (capped, expanding...)" if count >= MATCH_CAP else ""))
    if count >= MATCH_CAP:
        for ch in string.ascii_lowercase:
            crawl_matching_stops(query + ch, seen_queries, results_out)
            crawl_matching_stops(ch + query, seen_queries, results_out)


DEFAULT_WORKERS = 8  # confirmed live: sequential sweeps are far too slow whenever a request times
                      # out (each failing item burns ~90s working through REQUEST_RETRIES' full
                      # backoff before moving on) -- concurrent requests overlap that dead time
                      # instead of paying it serially, same fix this project's old/ reservation-site
                      # scraper already used for the same class of problem.


def run_matching_crawl(limit=None, workers=DEFAULT_WORKERS):
    """Base layer: every 3-letter lowercase combination (26^3 = 17,576) --
    exhaustive enough to find any real stop name containing at least one
    common 3-letter substring, which in practice is effectively all of
    them. Each one recursively expands on its own if it hits the cap (see
    crawl_matching_stops()). Bases are independent of each other, so they
    run across a thread pool -- seen_queries/results are shared, plain
    (not lock-protected) dict/set, same "GIL makes this atomic enough, worst
    case is a harmless duplicate request" reasoning old/scrape_gtfs.py
    already used for its own done_pairs set."""
    seen_queries = set()
    results = {}
    bases = ["".join(c) for c in itertools.product(string.ascii_lowercase, repeat=MIN_QUERY_LEN)]
    if limit:
        bases = bases[:limit]
    print(f"Matching-stops crawl: {len(bases)} base 3-letter quer{'y' if len(bases)==1 else 'ies'} "
          f"({workers} parallel workers, expanding recursively past {MATCH_CAP}-result cap as needed)")
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(crawl_matching_stops, base, seen_queries, results): base for base in bases}
        for i, fut in enumerate(as_completed(futures), 1):
            fut.result()  # surface any unexpected (non-request) exception
            if i % 200 == 0 or i == len(bases):
                print(f"[{i}/{len(bases)} bases done, {len(seen_queries)} total queries so far]")
    print(f"Done: {len(seen_queries)} total quer{'y' if len(seen_queries)==1 else 'ies'} run")


def _fetch_one_nearby(t, i, total):
    slug = f"{t['loc_name']}_{t['lat']}_{t['lon']}".replace("/", "_").replace(" ", "_")
    path = f"{RAW_NEARBY_DIR}/{slug}.json"
    if os.path.exists(path):
        return
    try:
        data = get_nearby_stops(t["lat"], t["lon"])
    except requests.exceptions.RequestException as e:
        # See crawl_matching_stops()'s matching comment -- one dead
        # terminus lookup shouldn't kill the whole sweep. Not cached,
        # so a re-run retries it for real.
        print(f"[{i}/{total}] {t['loc_name']}: FAILED after retries ({e.__class__.__name__}), skipping")
        return
    tmp = f"{path}.tmp.{os.getpid()}"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)
    print(f"[{i}/{total}] {t['loc_name']}: {data.get('count', 0)} nearby stop(s)")


def run_nearby_sweep(workers=DEFAULT_WORKERS):
    with open("old/data/gis/bus_terminus.json", encoding="utf-8") as f:
        termini = json.load(f)
    termini = [t for t in termini if t.get("lat") is not None and t.get("lon") is not None]
    print(f"Nearby-stops sweep: {len(termini)} geocoded termini "
          f"({workers} parallel workers, searchDistance={NEARBY_SEARCH_DISTANCE_M}m)")
    os.makedirs(RAW_NEARBY_DIR, exist_ok=True)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(_fetch_one_nearby, t, i, len(termini)) for i, t in enumerate(termini, 1)]
        for fut in as_completed(futures):
            fut.result()


RAW_STOP_BY_ROUTE_DIR = "raw/stop_by_route"  # populated by scrape_routes.py, not this file --
                                              # getStopByRoute's stop objects are full stops (same
                                              # shape as the two strategies above), and a route can
                                              # pass through a stop neither strategy ever surfaced
                                              # (not near any known terminus, missed by substring
                                              # search), so this is a third completeness source.


def build_geojson():
    """Aggregates every raw/ response (both strategies here, plus whatever
    scrape_routes.py's route-expansion crawl has turned up) into a deduped
    (by stopId) GeoJSON FeatureCollection."""
    stops_by_id = {}
    for d in (RAW_MATCHING_DIR, RAW_NEARBY_DIR, RAW_STOP_BY_ROUTE_DIR):
        if not os.path.isdir(d):
            continue
        for fname in os.listdir(d):
            if not fname.endswith(".json"):
                continue
            with open(f"{d}/{fname}", encoding="utf-8") as f:
                data = json.load(f)
            for s in data.get("data", []):
                stops_by_id[s["stopId"]] = s

    features = []
    for s in stops_by_id.values():
        if s.get("stopLat") is None or s.get("stopLon") is None:
            continue
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [s["stopLon"], s["stopLat"]]},
            "properties": {
                "stop_id": s["stopId"],
                "stop_code": s.get("stopCode"),
                "stop_name": s.get("stopName"),
                "stop_name_devanagari": s.get("stopNameDevanagari"),
                "stop_desc": s.get("stopDesc"),
                "location_type": s.get("locationType"),
                "is_terminal": s.get("isTerminal") == "Y",
                "is_junction": s.get("isJunction") == "Y",
            },
        })
    with open(OUT_GEOJSON, "w", encoding="utf-8") as f:
        json.dump({"type": "FeatureCollection", "features": features}, f, indent=2, ensure_ascii=False)
    print(f"Wrote {OUT_GEOJSON}: {len(features)} unique stop(s)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--matching-test", type=int, nargs="?", const=20,
                     help="run only the first N base queries (default 20), for a quick sanity check")
    ap.add_argument("--matching", action="store_true", help="full adaptive matching-stops crawl")
    ap.add_argument("--nearby", action="store_true", help="nearby-stops sweep over all GIS termini")
    ap.add_argument("--build", action="store_true", help="aggregate raw/ into msrtc-stops.geojson")
    ap.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                     help=f"parallel requests for --matching/--matching-test/--nearby (default {DEFAULT_WORKERS})")
    args = ap.parse_args()

    if args.matching_test is not None:
        run_matching_crawl(limit=args.matching_test, workers=args.workers)
    if args.matching:
        run_matching_crawl(workers=args.workers)
    if args.nearby:
        run_nearby_sweep(workers=args.workers)
    if args.build:
        build_geojson()

    if not any([args.matching_test is not None, args.matching, args.nearby, args.build]):
        ap.print_help()


if __name__ == "__main__":
    main()
