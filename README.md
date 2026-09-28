# msrtc-gtfs-gen

Generates an (unofficial) GTFS feed for MSRTC from `mobileapi.msrtcvtspis.com`,
a mobile-app API that exposes live trip/route data but only ever looks
~24 hours forward from query time. Since there's no historical/bulk export,
this project crawls it repeatedly -- across different times of day and
different weekdays -- caching everything to disk, and reduces from there.

## Notes
The current feed is incomplete and stores incomplete data. This repository hosts methods to generate a gtfs from the endpoint and a feed illustrating data retrieved over a limited period of time to generate an incomplete dataset.

## Setup

```
poetry install
```

Create a `.env` file (git-ignored) with the mobile "Integrity Token", this is used as the API key:

```
MSRTC_API_KEY=<key>
```

## Data files

Intermediate artifacts (git-ignored `raw/` holds the cached API responses they are built from):

| File | Contents |
|---|---|
| `msrtc-stops.geojson` | All known stops: id, code, English + Devanagari name, location |
| `msrtc-routes.json` | Route code -> ordered list of stop ids |
| `msrtc-trips.json` | Trip id -> route, origin departure time, weekdays it was observed running |
| `msrtc-trip-times.json` | Per route: median duration between each consecutive pair of stops (null if unobserved) |
| `msrtc-shapes.json` | Route code -> encoded polyline of the route geometry |
| `msrtc-source-stops.json` | Stops confirmed as trip origins (used by `--requery`) |
| `msrtc-pair-cover.json` | Minimal set of stop pairs that covers every known route (used by `--requery`) |

## Pipeline

Each stage writes JSON artifacts consumed by the next. Run them in order.

### 1. Stops -- `scrape_stops.py`

Discovers stops via two independent strategies (text search + a geocoded
sweep around known termini), then a --build step folds in whatever
scrape_routes.py's route-expansion crawl (step 2) has also turned up. Run
--build again after step 2 to pick that up.

```
python3 scrape_stops.py --matching   # adaptive text-search crawl over all 3-letter substrings
python3 scrape_stops.py --nearby     # geocoded sweep around known GIS termini
python3 scrape_stops.py --build      # -> msrtc-stops.geojson
```

### 2. Routes, trips, timings -- `scrape_routes.py`

```
python3 scrape_routes.py --discover        # full seed + BFS route/trip discovery, for TODAY's weekday
python3 scrape_routes.py --build           # -> msrtc-routes.json, msrtc-trips.json,
                                            #    msrtc-trip-times.json, msrtc-source-stops.json
python3 scrape_routes.py --min-cover       # -> msrtc-pair-cover.json (cumulative across all weekdays crawled so far)
python3 scrape_routes.py --requery         # cheap targeted rerun using msrtc-pair-cover.json + msrtc-source-stops.json
python3 scrape_routes.py --build           # rebuild to fold the requery's fresh data in
```

`--discover` is a full BFS crawl (expensive: tens/hundreds of thousands of
requests) and is cached per WEEKDAY -- running it again on a different
calendar weekday queries fresh rather than reusing another day's answers.
Re-running `--discover` on more weekdays over time is how weekday-specific
schedule differences (e.g. reduced weekend service) show up in the data at
all -- run it as many different weekdays as you can before generating a
feed you care about being accurate for a specific day.

`--requery` is the cheap day-to-day option once a `--discover` +
`--min-cover` baseline exists: it only queries the confirmed trip-origin
stops (`msrtc-source-stops.json`) and the minimal pair cover
(`msrtc-pair-cover.json`), instead of repeating the full BFS. Good for
topping up a weekday's data or checking today's live service without
paying the full crawl's cost. Always re-run `--build` afterward to fold
whatever a `--requery` (or an additional `--discover`) found into the
aggregated JSON files -- none of the CLI flags do that automatically for you.

Useful flags: `--workers N` (default 8) for both `--discover` and
`--requery`; `--date YYYY-MM-DD` for `--requery` to label a snapshot
explicitly (defaults to today).

### 3. Shapes -- `scrape_shapes.py`

```
python3 scrape_shapes.py --fetch      # getPathByRoute for every route in msrtc-routes.json
python3 scrape_shapes.py --build      # -> msrtc-shapes.json (encoded polylines)
python3 scrape_shapes.py --fill-gaps  # fills routes with no path data by borrowing real
                                       # segment geometry from other routes sharing the same
                                       # stop pairs, falling back to a straight line otherwise
```

Route geometry is cached with a 90-day TTL (not weekday-partitioned --
roads don't change day to day, but occasionally do get realigned/added).
Re-run `--fetch`/`--build`/`--fill-gaps` after step 2 discovers new routes,
to cover them too.

### 4. GTFS feed -- `build_gtfs.py`

```
python3 build_gtfs.py
```

Assembles / reorganises relevant files into `gtfs/` .txt files, then zips the output to a `gtfs.zip`.

## Re-running over time

The whole design assumes you'll run `--discover` on more weekdays and
`--requery` more days as time goes on, then re-run `--build` (steps 2-4)
to regenerate an updated feed. Nothing needs to be reset between runs --
every cache is either weekday-partitioned (schedule data) or TTL'd (static
data), so reruns only do the work that's actually needed.
