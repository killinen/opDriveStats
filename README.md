
# Engagement Gauge Development Tools

Python tooling for working with OpenPilot `rlog` files: collect drive data, compute engagement metrics, surface interesting events, and publish summaries to a web API.

## Repository Layout

- `engagement_gauge.py` – Original processing script for downloading/processing drives and writing `engagement_db.json` plus optional debug events.
- `engagement_gauge_dev.py` – Enhanced workflow with better remote-download handling, optional on-device compression, richer statistics, and DBC-assisted signal decoding.
- `event_viewer.py` – Iterate through `debug_events.json` and open each event in PlotJuggler (with optional video playback support).
- `run_plotjuggler.sh` – Launch PlotJuggler in Docker with the correct data mounts.
- `lib/` – Helpers for reading `rlog` files, CAN decoding, discovering local drives, etc.
- `server/` – FastAPI service for serving the aggregated statistics (see `server/README.md` for deployment).
- Data artifacts: `engagement_db.json`, `debug_events.json`, `processed_drives.json`, `engagement_stats.json`.
- Cap’n Proto schemas: `log.capnp`, `car.capnp`, `legacy.capnp`, and bundled `include/` headers for local decoding.

## Prerequisites

- Python 3.10+ with packages: `capnp`, `paramiko`, `cantools`, `bz2` (stdlib), `numpy` (optional but recommended), etc. Use your preferred environment manager and install as needed.
- Docker (for PlotJuggler playback).
- Local directory layout for raw logs: `rlogs/<dongle_id>/<drive_name--segment>/rlog.bz2`.
- Optional: remote SSH access to devices if using download features.

## Processing Workflow

### 1. Gather `rlog` Files

- **Remote collection** – Run `engagement_gauge_dev.py` or `engagement_gauge.py` with SSH host definitions. The scripts can compress and pull logs into `./rlogs/<dongle>/<drive--segment>/` automatically.
- **Manual copy** – Use the provided `rlogs/download_rlogs.py` helper or copy files yourself. Ensure they end up in the expected directory structure before processing.

### 2. Compute Engagement Stats

`engagement_gauge_dev.py` is the recommended entry point; it contains all functionality from `engagement_gauge.py` plus additional decoding and resilience improvements.

```bash
# Process all new drives for a dongle, capturing steering interventions
python3 engagement_gauge_dev.py --dongle <dongle_id> --debug steer

# Reprocess existing drives
python3 engagement_gauge_dev.py --dongle <dongle_id> --debug steer --reprocess

# Filter by drive name or time range
python3 engagement_gauge_dev.py --dongle <dongle_id> --drive "2025-07-30--16-09-38"
python3 engagement_gauge_dev.py --dongle <dongle_id> --start "2025-07-01" --stop "2025-07-31"

# Discover available dongles based on local files + DB
python3 engagement_gauge_dev.py --dongle list
```

Outputs:

- `engagement_db.json` – Aggregated per-drive statistics with metadata (version, fingerprint, interventions, etc.).
- `debug_events.json` – Optional event log when `--debug` flags are provided.
- `processed_drives.json` / `engagement_stats.json` – Tracking files for historical runs.

### 3. Review Events with PlotJuggler

`event_viewer.py` loads `debug_events.json` and opens each event in PlotJuggler via Docker.

```bash
# List drives that contain stored events
./event_viewer.py --list

# Walk through events for a specific drive
./event_viewer.py --drive "2025-07-30--16-09-38"

# Include video playback using the ~/OPstuff/realdata/downloaded_folders/ layout
./event_viewer.py --drive "2025-07-30--16-09-38" --video
```

`run_plotjuggler.sh` mounts `./rlogs` (and optionally video directories) into the container, so make sure those directories exist locally.

## Publishing the Statistics

The `server/` directory contains a FastAPI application that reads `engagement_db.json` and exposes:

- `GET /health` – Basic service status and last data-refresh time.
- `GET /devices` – Summary metrics per device.
- `GET /devices/{dongle_id}` – Detailed drive-level metrics for a device.

Refer to `server/README.md` for:

- Python dependency installation (`fastapi`, `uvicorn`).
- Running the API locally or on a VPS via systemd.
- Securing the API behind Nginx with DuckDNS + Let’s Encrypt TLS.

## Updating the Data Source

- `engagement_gauge_dev.py` automatically keeps `engagement_db.json` updated as new drives are processed.
- The FastAPI layer monitors the file modification time; refreshes are picked up the next time an endpoint is hit (no restart required).
- Use `./sync_stats.sh -s <host>` to push the refreshed `engagement_db.json` to your VPS via `rsync` (or `scp` if `rsync` isn’t installed remotely); the script honors your `~/.ssh/config` user/identity defaults, stores uploads as `~/engagement_gauge_dev/engagement_db.json` by default, and lets you override the local file, remote path, or user with the `-f`, `-p`, and `-u` flags (or matching `SYNC_STATS_*` env vars).

## Tips & Troubleshooting

- If Cap’n Proto parsing fails, ensure `pycapnp` is installed and matches your Python version.
- When working offline, drop fresh logs into `./rlogs/<dongle_id>/` and re-run the processing script with `--reprocess` if needed.
- `rlog_copies/` is ignored by git; feel free to use it for manual backups or temporary experiments.
- For performance, prune large `debug_events.json` files or archive old results regularly.

## Contributing / Next Steps

- Add additional API endpoints or dashboards under `server/` as needed.
- Extend `engagement_gauge_dev.py` with new debug checks or signal decodings (see `config/vehicle_profiles.json`).
- Improve documentation or automation scripts—pull requests welcome once the repository is hosted.
