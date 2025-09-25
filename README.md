
# Engagement Gauge Development Tools

This repository contains a suite of Python scripts designed for analyzing OpenPilot engagement data from `rlog` files. The primary goal is to process driving data, calculate engagement statistics, and identify and visualize specific events like steering interventions.

## Core Components

- **`engagement_gauge.py`**: The main script for processing `rlog` files. It connects to remote devices or uses local files to calculate engagement time, distance, and intervention metrics. It can generate a database of these statistics (`engagement_db.json`) and log specific, interesting events for deeper analysis.

- **`event_viewer.py`**: A utility to visualize the events captured by `engagement_gauge.py`. It reads the `debug_events.json` file and opens the relevant `rlog` segments in PlotJuggler, allowing for a detailed, visual inspection of each event.

- **`run_plotjuggler.sh`**: A helper script that launches PlotJuggler inside a Docker container. It is pre-configured to mount the necessary data volumes and use a specific Docker image (`plotjuggler_video_playback:latest`) that includes video playback capabilities.

- **`engagement_db.json`**: A JSON file that stores the historical engagement statistics for all processed drives. This file is created and updated by `engagement_gauge.py`.

- **`debug_events.json`**: A JSON file that stores detailed information about specific events (e.g., steering interventions) that are flagged for review during the `engagement_gauge.py` processing. This file is the input for `event_viewer.py`.

## Setup

1.  **Docker**: You must have Docker installed and running to use the visualization tools.
2.  **Data Directories**: The scripts expect a specific directory structure for data:
    -   **rlogs for processing**: `rlogs/<dongle_id>/<drive_name--segment>/rlog.bz2`
    -   **rlogs with video**: `~/OPstuff/realdata/downloaded_folders/<drive_name>/<drive_name--segment>/` (This directory should contain both the `rlog.bz2` and the corresponding video files).

## Usage Workflow

A typical workflow involves two main steps: processing the data to find events and then viewing those events.

### Step 1: Process Drives and Find Events

Use `engagement_gauge.py` to analyze your drives and generate the `debug_events.json` file. The `--debug steer` argument is used to specifically log steering intervention events.

```bash
# Process all drives for a specific dongle ID and log steering interventions
python3 engagement_gauge.py --dongle-id <your_dongle_id> --debug steer

# To add new events without clearing the old ones, use --add-events
python3 engagement_gauge.py --dongle-id <your_dongle_id> --debug steer --add-events

# You can also filter by a specific drive or date range
python3 engagement_gauge.py --dongle-id <your_dongle_id> --debug steer --drive "2025-07-30--16-09-38"

# New: list available dongles that have data (from local rlogs and engagement DB)
python3 engagement_gauge.py --dongle list

# New: you can also use --dongle instead of --dongle-id
python3 engagement_gauge.py --dongle <your_dongle_id> --debug steer

# Alias: listing also works via -d
python3 engagement_gauge.py -d list
```

### Step 2: View Events in PlotJuggler

Once `debug_events.json` has been created, use `event_viewer.py` to open the logs in PlotJuggler. The script will iterate through the events, opening them one by one.

- To get a list of all drives with logged events:
```bash
./event_viewer.py --list
```

- To view events from a specific drive **with video playback**, use the `--drive` and `--video` flags:
```bash
./event_viewer.py --drive "2025-07-30--16-09-38" --video
```

The `--video` flag is critical. It tells the script to find the log files in the `~/OPstuff/realdata/downloaded_folders/` directory, which allows PlotJuggler (running via `run_plotjuggler.sh`) to access both the log data and the associated video files.
