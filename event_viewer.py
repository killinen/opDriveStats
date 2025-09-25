#!/usr/bin/env python3

import json
import subprocess
import os
import argparse
from collections import defaultdict

def open_rlogs_from_debug_events(debug_events_path, start_drive=None, end_drive=None, specific_drive=None, use_video_path=False):
    with open(debug_events_path, 'r') as f:
        debug_events = json.load(f)

    # Filter events based on command-line arguments
    if specific_drive:
        debug_events = [event for event in debug_events if event.get("drive") == specific_drive]
    elif start_drive or end_drive:
        debug_events = [event for event in debug_events if
                        (not start_drive or event.get("drive") >= start_drive) and
                        (not end_drive or event.get("drive") <= end_drive)]

    if not debug_events:
        print("No events found for the specified drive(s).")
        return

    script_dir = os.path.dirname(os.path.abspath(__file__))
    plotjuggler_script_path = os.path.join(script_dir, "run_plotjuggler.sh")

    # Group events by rlog_path and collect necessary data
    logs_to_open = defaultdict(lambda: {'times': [], 'drive': None})
    for event in debug_events:
        rlog_path = event.get("rlog_path")
        if rlog_path:
            logs_to_open[rlog_path]['times'].append(event.get("time_in_segment_s"))
            logs_to_open[rlog_path]['drive'] = event.get("drive")

    for rlog_path, data in logs_to_open.items():
        times = data['times']
        drive_name = data['drive']
        path_to_use = None

        if use_video_path:
            segment_name = os.path.basename(os.path.dirname(rlog_path))
            # Corrected video path based on user's ll output
            video_rlog_path_abs = os.path.join(os.path.expanduser('~/OPstuff/realdata/downloaded_folders/'), drive_name, segment_name, 'rlog.bz2')
            
            if os.path.exists(video_rlog_path_abs):
                # Use the absolute path for the script
                path_to_use = video_rlog_path_abs
            else:
                print(f"⚠️  Warning: Video rlog not found at {video_rlog_path_abs}. Skipping.")
                continue
        else:
            # Use the original relative path from the project root
            path_to_use = rlog_path

        full_path_for_display = os.path.abspath(os.path.join(script_dir, path_to_use))

        print("\n" + "="*80)
        print(f"\033[1mLog:\033[0m {full_path_for_display}")
        print(f"\033[1m  Event times (s):\033[0m {', '.join(map(str, sorted(times)))}")
        print("="*80 + "\n")
        print(f"Opening in PlotJuggler. Close PlotJuggler to open the next log...")
        try:
            # Use subprocess.run to wait for PlotJuggler to close
            subprocess.run([plotjuggler_script_path, path_to_use])
        except Exception as e:
            print(f"Error opening {full_path_for_display}: {e}")

    print("All PlotJuggler instances processed. Script finished.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Open rlogs in PlotJuggler from debug events.")
    parser.add_argument("--start", help="The start drive to process (YYYY-MM-DD--HH-MM-SS).")
    parser.add_argument("--end", help="The end drive to process (YYYY-MM-DD--HH-MM-SS).")
    parser.add_argument("--drive", help="A specific drive to process (YYYY-MM-DD--HH-MM-SS).")
    parser.add_argument("--list", action="store_true", help="List all drives found in the debug log.")
    parser.add_argument("--video", action="store_true", help="Use rlogs from the video data directory.")
    args = parser.parse_args()

    debug_events_file = "/home/goran/OPstuff/test_tools/engament_gauge_dev/debug_events.json"

    try:
        if args.list:
            with open(debug_events_file, 'r') as f:
                debug_events = json.load(f)
            drives = sorted(list(set(event.get("drive") for event in debug_events)))
            print("Drives found in the debug log:")
            for drive in drives:
                print(drive)
        else:
            open_rlogs_from_debug_events(debug_events_file, args.start, args.end, args.drive, args.video)
    except KeyboardInterrupt:
        print("\nScript interrupted by user. Exiting gracefully.")