#!/usr/bin/env python3
# This script connects to remote SSH hosts, downloads rlog files, processes them to compute engagement time,
# and maintains a historical database of drive engagement statistics in JSON format.
# NEW: Added offline analysis mode with --dongle-id argument

import os
import json
import paramiko
import bz2
import argparse
import socket
import time
import shutil
from datetime import datetime

# Local modules for file reading and capnp parsing
from lib.helpers import (
    MovingAverageFilter,
    get_device_identifier,
    extract_segment_number,
    parse_drive_timestamp,
    numerical_sort,
    LogReader,
    get_ssh_host_details,
    extract_version_info,
    find_local_rlog_files,
    upsert_drive_to_db,
    filter_drives_by_date_range,
)

# Constants for tracking processed and aggregated data
ENGAGEMENT_DB_FILE = 'engagement_db.json'
DEBUG_DB_FILE = 'debug_events.json'

# Add these constants at the top of the file
STEER_INTERVENTION_THRESHOLD = 1.5
STEER_RESOLUTION_THRESHOLD = 0.3
INTERVENTION_TIMEOUT_NS = 10 * 1_000_000_000  # 10 seconds
ENGAGEMENT_BUFFER_NS = 3 * 1_000_000_000      # 3 second buffer after engagement changes
SFTP_SOCKET_TIMEOUT = 30
SFTP_MAX_RETRIES = 3
SFTP_RETRY_DELAY = 5
VEGO_MOVING_THRESHOLD = 1.0
BACKUP_DIR = 'database_backups'


engagement_db_modified = False
debug_db_modified = False


def configure_sftp_timeout(sftp_client):
    """Ensure SFTP socket operations fail fast when the link drops."""
    try:
        sftp_client.get_channel().settimeout(SFTP_SOCKET_TIMEOUT)
    except Exception as e:
        print(f"⚠️ Unable to set SFTP socket timeout: {e}")


def reopen_sftp_client(ssh_client):
    """Reopen the SFTP channel after a connection issue."""
    new_client = ssh_client.open_sftp()
    configure_sftp_timeout(new_client)
    return new_client


def backup_file(path):
    """Create a .bak copy of the given file if it exists."""
    if os.path.exists(path):
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        os.makedirs(BACKUP_DIR, exist_ok=True)
        filename = os.path.basename(path)
        backup_path = os.path.join(BACKUP_DIR, f"{filename}.{timestamp}.bak")
        try:
            shutil.copy2(path, backup_path)
            print(f"🗃️ Backup created: {backup_path}")
        except Exception as e:
            print(f"⚠️ Could not create backup for {path}: {e}")


def format_pct(value, decimals=2):
    return f"{value:.{decimals}f}%" if value is not None else "N/A"


def process_drive_offline(drive_name, rlog_files, device_id, debug_mode=None, debug_events_db=None):
    """
    Process a single drive's rlog files for offline analysis.
    Returns drive statistics.
    """
    global debug_db_modified
    print(f"\n🔍 Processing drive: {drive_name}, segments: {len(rlog_files)}")
    
    # Sort rlog files by segment number for proper processing
    rlog_files_sorted = sorted(rlog_files, key=extract_segment_number)
    
    # Extract version information from the drive
    version_info = extract_version_info(rlog_files_sorted, "")  # Empty local_host_dir for offline
    
    # Initialize drive-level variables (persistent across segments)
    drive_stats = {
        'total_time': 0,
        'active_time': 0,
        'drive_time': 0,
        'drive_time_active': 0,
        'cruise_press_time_ns': 0,
        'odo_start': None,
        'odo_end': None,
        'git_branch': None,
        'git_commit': None,
        'version': None,
    }
    engaged_distance = 0
    last_odo = None
    currently_engaged = False
    
    # Track engagement interventions
    engagement_state_changes = 0
    interventions = 0
    total_steer_interventions = 0
    last_engagement_state = None
    pending_reengagement = False
    
    for rlog_file in rlog_files_sorted:
        # Enable logic for post-2025-07-06 only
        drive_timestamp_str = parse_drive_timestamp(drive_name)
        if drive_timestamp_str:
            drive_date = datetime.fromisoformat(drive_timestamp_str)
            enable_intervention_check = drive_date >= datetime(2025, 7, 7)
            enable_old_logic = drive_date < datetime(2025, 7, 7)
        else:
            enable_intervention_check = False
            enable_old_logic = True

        # Initialize engagement tracking variables at segment level
        last_engagement_change_time = None
        engagement_stable = True
        steer_intervention_count = 0

        # Signal filters
        filter_mdps = MovingAverageFilter(10)
        filter_eps = MovingAverageFilter(10)
        filtered_mdps = None
        filtered_eps = None

        segment_num = extract_segment_number(rlog_file)

        try:
            lr = LogReader(rlog_file)
            # Initialize segment-level variables
            total_time = 0
            active_time = 0
            drive_time = 0
            drive_time_active = 0
            cruise_press_time = 0
            start_time = -1
            last_active_time = -1
            segment_odo_start = None
            segment_odo_end = None
            in_intervention = False
            last_steer_intervention_time = 0
            prev_moving = False
            prev_engaged = False
            last_carstate_time = None
            last_cruise_state = 0
            last_cruise_time = None

            for msg in lr:
                if msg.which() == 'controlsState':
                    current_time = msg.logMonoTime
                    if start_time == -1:
                        start_time = msg.logMonoTime
                    
                    # Track engagement state changes and count complete intervention cycles
                    current_state = msg.controlsState.active
                    if last_engagement_state is not None and current_state != last_engagement_state:
                        engagement_state_changes += 1
                        
                        # Track engagement change time for steering intervention buffer
                        last_engagement_change_time = current_time
                        engagement_stable = False
                        print(f"    Engagement change at {current_time}, starting buffer period")
                        
                        if current_state == False:  # ON → OFF (disengagement)
                            print(f"    Disengagement #{engagement_state_changes}: ON → OFF")
                            pending_reengagement = True
                        elif current_state == True and pending_reengagement:  # OFF → ON (re-engagement)
                            print(f"    Re-engagement #{engagement_state_changes}: OFF → ON")
                            interventions += 1  # Complete intervention cycle
                            pending_reengagement = False
                            print(f"    ✅ Intervention #{interventions} completed (OFF → ON → OFF cycle)")
                        else:  # First engagement of the drive
                            print(f"    Initial engagement #{engagement_state_changes}: OFF → ON")
                    
                    last_engagement_state = current_state
                    
                    # Check if we're past the buffer period
                    if last_engagement_change_time is not None:
                        if current_time - last_engagement_change_time > ENGAGEMENT_BUFFER_NS:
                            engagement_stable = True
                    
                    if msg.controlsState.active:
                        if last_active_time != -1:
                            active_time += msg.logMonoTime - last_active_time
                        last_active_time = msg.logMonoTime
                        currently_engaged = True
                    else:
                        last_active_time = -1
                        currently_engaged = False
                    total_time = msg.logMonoTime - start_time
                    prev_engaged = currently_engaged

                elif msg.which() == 'can':
                    for m in msg.can:
                        if m.src == 0 and m.address == 1264:
                            # CF_Clu_Odometer is 24 bits starting at bit 40 → bytes 5,6,7, big-endian
                            if len(m.dat) >= 8:
                                odo_val = ((m.dat[7] << 16) | (m.dat[6] << 8) | m.dat[5]) * 0.1
                                
                                # Initialize drive-level odometer tracking
                                if drive_stats['odo_start'] is None:
                                    drive_stats['odo_start'] = odo_val
                                    last_odo = odo_val
                                    print(f"🧾 Drive odometer start: {odo_val} km")
                                
                                # Track segment-level odometer for debugging
                                if segment_odo_start is None:
                                    segment_odo_start = odo_val
                                segment_odo_end = odo_val
                                
                                # Calculate distance increment and track engaged distance
                                if last_odo is not None:
                                    distance_increment = odo_val - last_odo
                                    # Only count positive, reasonable increments (< 1km per reading)
                                    if 0 < distance_increment < 1.0:
                                        if currently_engaged:
                                            engaged_distance += distance_increment
                                    last_odo = odo_val
                                
                                # Update drive end odometer
                                drive_stats['odo_end'] = odo_val

                            # Track cruise switch hold time (lower 3 bits of byte 0)
                            if len(m.dat) >= 1:
                                cruise_state = m.dat[0] & 0x07
                                if last_cruise_time is not None:
                                    delta = msg.logMonoTime - last_cruise_time
                                    if last_cruise_state != 0 and delta > 0:
                                        cruise_press_time += delta
                                last_cruise_state = cruise_state
                                last_cruise_time = msg.logMonoTime

                        if enable_intervention_check:
                            if m.src == 0 and m.address == 357 and len(m.dat) >= 2:
                                raw = ((m.dat[1] & 0x0F) << 8) | m.dat[0]  # Only 12 bits total: lower 8 + lower 4 of byte 1
                                val_mdps = -(raw * 0.01 - 20.48)-0.2        # The -0.2 is corection value
                                filtered_mdps = filter_mdps.update(val_mdps)
                            elif m.src == 1 and m.address == 559 and len(m.dat) >= 3:
                                raw = m.dat[2]
                                if raw >= 128:
                                    raw -= 256
                                val_eps = raw * 0.125
                                filtered_eps = filter_eps.update(val_eps)

                            if (
                                filtered_mdps is not None and
                                filtered_eps is not None and
                                currently_engaged and
                                engagement_stable
                            ):
                                diff = abs(filtered_mdps - filtered_eps)
                                now = msg.logMonoTime

                                # Detect intervention start
                                if (
                                    diff > STEER_INTERVENTION_THRESHOLD and
                                    not in_intervention and
                                    (now - last_steer_intervention_time > INTERVENTION_TIMEOUT_NS)
                                ):
                                    steer_intervention_count += 1
                                    in_intervention = True
                                    last_steer_intervention_time = now
                                    segment_time_s = (now - start_time) / 1e9
                                    if debug_mode == 'steer' and debug_events_db is not None:
                                        debug_db_modified = True
                                        debug_events_db.append({
                                            'drive': drive_name,
                                            'rlog_path': rlog_file,
                                            'event_type': 'steer_intervention',
                                            'time_in_segment_s': segment_time_s,
                                            'reason': f'Steering intervention detected (diff: {diff:.2f})'
                                        })
                                    if last_engagement_change_time is not None:
                                        stable_time = (now - last_engagement_change_time) / 1e9
                                        print(f"⚠️ Steering intervention #{steer_intervention_count}: {filtered_mdps:.2f} vs {filtered_eps:.2f} (diff: {diff:.2f}) - engagement stable for {stable_time:.1f}s")
                                    else:
                                        print(f"⚠️ Steering intervention #{steer_intervention_count}: {filtered_mdps:.2f} vs {filtered_eps:.2f} (diff: {diff:.2f}) - engagement stable")

                                # Detect intervention end
                                elif diff < STEER_RESOLUTION_THRESHOLD and in_intervention:
                                    in_intervention = False
                                    print(f"✅ Steering intervention resolved: {filtered_mdps:.2f} vs {filtered_eps:.2f} (diff: {diff:.2f})")

                elif msg.which() == 'carState':
                    v_ego = getattr(msg.carState, 'vEgo', 0.0)
                    moving = v_ego > VEGO_MOVING_THRESHOLD

                    if last_carstate_time is not None:
                        delta = msg.logMonoTime - last_carstate_time
                        if prev_moving and delta > 0:
                            drive_time += delta
                            if prev_engaged:
                                drive_time_active += delta

                    prev_moving = moving
                    prev_engaged = currently_engaged
                    last_carstate_time = msg.logMonoTime

            # Accumulate segment stats to drive stats
            drive_stats['total_time'] += total_time
            drive_stats['active_time'] += active_time
            drive_stats['drive_time'] += drive_time
            drive_stats['drive_time_active'] += drive_time_active
            drive_stats['cruise_press_time_ns'] += cruise_press_time
            total_steer_interventions += steer_intervention_count

        except Exception as e:
            print(f"❌ Could not process {rlog_file}: {e}")
            continue

    # Calculate final drive statistics
    drive_stats['odo_distance'] = round((drive_stats['odo_end'] - drive_stats['odo_start']), 1) if drive_stats['odo_start'] is not None and drive_stats['odo_end'] is not None else None
    drive_stats['engaged_distance'] = round(engaged_distance, 1) if drive_stats['odo_start'] is not None else None

    if drive_stats['total_time'] > 0:
        drive_stats['engagement_pct'] = round((drive_stats['active_time'] / drive_stats['total_time']) * 100, 2)
    else:
        drive_stats['engagement_pct'] = 0.0

    if drive_stats['drive_time'] > 0:
        drive_stats['drive_time_engagement_pct'] = round((drive_stats['drive_time_active'] / drive_stats['drive_time']) * 100, 2)
    else:
        drive_stats['drive_time_engagement_pct'] = 0.0

    drive_stats['cruise_press_seconds'] = round(drive_stats['cruise_press_time_ns'] / 1e9, 2)
    if drive_stats['drive_time'] > 0:
        drive_stats['cruise_press_seconds_per_hour'] = round((drive_stats['cruise_press_time_ns'] / drive_stats['drive_time']) * 3600, 2)
    else:
        drive_stats['cruise_press_seconds_per_hour'] = 0.0

    # Calculate engagement percentages and intervention rates
    if drive_stats['odo_distance'] and drive_stats['odo_distance'] > 0:
        drive_stats['engagement_pct_odo'] = round((engaged_distance / drive_stats['odo_distance']) * 100, 2)
        drive_stats['steer_interventions_per_100km'] = round((total_steer_interventions / drive_stats['odo_distance']) * 100, 2)
        drive_stats['interventions_per_100km'] = round((interventions / drive_stats['odo_distance']) * 100, 2)
    else:
        drive_stats['engagement_pct_odo'] = None
        drive_stats['steer_interventions_per_100km'] = None
        drive_stats['interventions_per_100km'] = None

    # Store intervention counts
    drive_stats['total_state_changes'] = engagement_state_changes
    drive_stats['intervention_count'] = interventions
    drive_stats['steer_intervention_count'] = total_steer_interventions

    print(f"🔄 Total state changes: {engagement_state_changes}, Complete interventions: {interventions}")

    # Enhanced output with intervention metrics and version info
    intervention_info = f", Interventions: {interventions}" if interventions is not None else ""
    interventions_per_100km_info = f", Interventions/100km: {drive_stats['interventions_per_100km']}" if drive_stats.get('interventions_per_100km') is not None else ""
    version_display = f", Version: {version_info['version']}" if version_info['version'] else ""
    drive_time_engagement_info = (
        f", Drive Time Engagement: {format_pct(drive_stats.get('drive_time_engagement_pct'))}"
        if drive_stats.get('drive_time_engagement_pct') is not None else ""
    )
    cruise_press_info = (
        f", Cruise Press Seconds/Hour: {drive_stats['cruise_press_seconds_per_hour']:.2f}"
        if drive_stats.get('cruise_press_seconds_per_hour') is not None else ""
    )

    git_branch = version_info.get('git_branch')
    git_commit = version_info.get('git_commit')

    branch_display = f", Branch: {git_branch}" if git_branch else ""
    commit_display = f", Commit: {git_commit[:10]}" if git_commit else ""

    drive_stats['git_branch'] = git_branch
    drive_stats['git_commit'] = git_commit
    drive_stats['version'] = version_info.get('version')

    print(
        f"✅ Drive {drive_name} — Total: {drive_stats['odo_distance']}km, Engaged: {engaged_distance:.1f}km, "
        f"Time Engagement: {format_pct(drive_stats['engagement_pct'])}, "
        f"ODO Engagement: {format_pct(drive_stats.get('engagement_pct_odo'))}"
        f"{drive_time_engagement_info}{cruise_press_info}{intervention_info}{interventions_per_100km_info}{version_display}{branch_display}{commit_display}"
    )

    return drive_stats, version_info

# === MAIN EXECUTION ===
def main():
    # Parse command line arguments
    global engagement_db_modified, debug_db_modified
    engagement_db_modified = False
    debug_db_modified = False

    parser = argparse.ArgumentParser(description='Process OpenPilot rlog files for engagement analysis')
    # New: --dongle supports either a specific ID or the special value "list"
    parser.add_argument('--dongle', type=str, help='Dongle ID or "list" to show available dongles with data')
    # Backward-compatible: existing --dongle-id
    parser.add_argument('-d', '--dongle-id', type=str, help='Run offline analysis for a specific dongle ID')
    parser.add_argument('--start', type=str, help='The drive name to start processing from, ignoring older drives')
    parser.add_argument('--stop', type=str, help='The drive name to stop processing at, ignoring newer drives')
    # New, clearer flag replacing --overwrite
    parser.add_argument('--reprocess', action='store_true', help='Reprocess drives even if already processed; updates existing stats (offline mode) and re-evaluates events.')
    # Backward-compat: keep --overwrite as deprecated alias (hidden in help)
    parser.add_argument('--overwrite', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--debug', choices=['steer', 'disengagement'], help='Enable debug mode for specific event logging.')
    parser.add_argument('--compress-on-c2', action='store_true', help='Compress raw rlogs directly on the device before download (experimental).')
    args = parser.parse_args()

    # Map deprecated --overwrite to --reprocess
    if getattr(args, 'overwrite', False):
        print("⚠️ --overwrite is deprecated; use --reprocess instead.")
        setattr(args, 'reprocess', True)

    # Load or initialize historical engagement database
    if os.path.exists(ENGAGEMENT_DB_FILE):
        with open(ENGAGEMENT_DB_FILE, 'r') as f:
            engagement_db = json.load(f)
    else:
        engagement_db = []

    # Handle --dongle list option: list dongles that have data and exit
    if getattr(args, 'dongle', None) and str(args.dongle).lower() in {'list', 'all'}:
        # Collect from local rlogs
        rlogs_root = os.path.join('.', 'rlogs')
        local_dongles = set()
        if os.path.isdir(rlogs_root):
            for name in os.listdir(rlogs_root):
                path = os.path.join(rlogs_root, name)
                if os.path.isdir(path):
                    local_dongles.add(name)

        # Collect from engagement DB
        db_dongles = {entry.get('device_id') for entry in engagement_db if entry.get('device_id')}
        all_dongles = sorted(d for d in (local_dongles | db_dongles) if d)

        if str(args.dongle).lower() == 'all':
            print('Available dongles with data:')
            for d in all_dongles:
                print(f'- {d}')
            target_dongle_id = 'ALL'
        else:
            if not all_dongles:
                print('No dongles with data found.')
            else:
                print('Available dongles with data:')
                for d in all_dongles:
                    print(f'- {d}')
            return

    # Also support: -d list (alias for listing)
    if getattr(args, 'dongle_id', None) and str(args.dongle_id).lower() == 'list':
        rlogs_root = os.path.join('.', 'rlogs')
        local_dongles = set()
        if os.path.isdir(rlogs_root):
            for name in os.listdir(rlogs_root):
                path = os.path.join(rlogs_root, name)
                if os.path.isdir(path):
                    local_dongles.add(name)

        db_dongles = {entry.get('device_id') for entry in engagement_db if entry.get('device_id')}
        all_dongles = sorted(d for d in (local_dongles | db_dongles) if d)

        if not all_dongles:
            print('No dongles with data found.')
        else:
            print('Available dongles with data:')
            for d in all_dongles:
                print(f'- {d}')
        return

    # Load or initialize debug events database
    debug_events_db = None
    if args.debug:
        print("🐛 Debug mode is ON. Existing events will be preserved.")
        if os.path.exists(DEBUG_DB_FILE):
            with open(DEBUG_DB_FILE, 'r') as f:
                try:
                    debug_events_db = json.load(f)
                except json.JSONDecodeError:
                    print(f"⚠️ Could not parse {DEBUG_DB_FILE}, starting a new one.")
                    debug_events_db = []
        else:
            debug_events_db = []

    # Initialize all_stats dictionary that will be used for the summary
    all_stats = {}

    # Discover known dongles from local rlogs and DB
    rlogs_root = os.path.join('.', 'rlogs')
    local_dongles = set()
    if os.path.isdir(rlogs_root):
        for name in os.listdir(rlogs_root):
            path = os.path.join(rlogs_root, name)
            if os.path.isdir(path):
                local_dongles.add(name)
    db_dongles = {entry.get('device_id') for entry in engagement_db if entry.get('device_id')}
    known_dongles = sorted(d for d in (local_dongles | db_dongles) if d)

    # Determine target dongles for offline mode
    selected_dongles = None
    if getattr(args, 'dongle', None):
        dongle_arg = str(args.dongle).lower()
        if dongle_arg == 'list':
            if not known_dongles:
                print('No dongles with data found.')
            else:
                print('Available dongles with data:')
                for d in known_dongles:
                    print(f'- {d}')
            return
        elif dongle_arg == 'all':
            if not known_dongles:
                print('No dongles with data found.')
                return
            selected_dongles = known_dongles
            print('Available dongles with data:')
            for d in known_dongles:
                print(f'- {d}')
        else:
            selected_dongles = [args.dongle]
    elif getattr(args, 'dongle_id', None):
        selected_dongles = [args.dongle_id]

    compress_on_c2 = getattr(args, 'compress_on_c2', False)

    # Check if running in offline mode for selected dongles
    if selected_dongles:
        print(f"🔍 Running offline analysis for dongle ID(s): {', '.join(selected_dongles)}")

        for device_id in selected_dongles:
            if device_id not in all_stats:
                all_stats[device_id] = {}

            for entry in engagement_db:
                if entry.get("device_id") == device_id:
                    drive_name = entry.get("drive")
                    if drive_name:
                        all_stats[device_id][drive_name] = {
                            'total_time': entry.get('total_time'),
                            'active_time': entry.get('active_time'),
                            'drive_time': entry.get('drive_time'),
                        'drive_time_active': entry.get('drive_time_active'),
                        'cruise_press_time_ns': entry.get('cruise_press_time_ns'),
                        'cruise_press_seconds': entry.get('cruise_press_seconds'),
                        'cruise_press_seconds_per_hour': entry.get('cruise_press_seconds_per_hour'),
                        'odo_distance': entry.get('odo_distance'),
                        'engaged_distance': entry.get('engaged_distance'),
                        'engagement_pct': entry.get('engagement_pct'),
                        'engagement_pct_odo': entry.get('engagement_pct_odo'),
                        'drive_time_engagement_pct': entry.get('drive_time_engagement_pct'),
                        'git_branch': entry.get('git_branch'),
                        'git_commit': entry.get('git_commit'),
                        'version': entry.get('version'),
                        'intervention_count': entry.get('intervention_count'),
                        'interventions_per_100km': entry.get('interventions_per_100km'),
                        'steer_intervention_count': entry.get('steer_intervention_count'),
                        'steer_interventions_per_100km': entry.get('steer_interventions_per_100km'),
                    }

            local_drives = find_local_rlog_files(device_id)
            local_drives = filter_drives_by_date_range(local_drives, args.start, args.stop)

            if not local_drives:
                print(f"❌ No new drives found for dongle ID: {device_id}, displaying summary from DB.")
                continue

            existing_keys = {(e.get("device_id", ""), e.get("drive", "")) for e in engagement_db}

            for drive_name, rlog_files in local_drives.items():
                if not args.reprocess and (device_id, drive_name) in existing_keys:
                    print(f"⚠️ Drive {drive_name} already processed, skipping")
                    continue

                drive_stats, version_info = process_drive_offline(drive_name, rlog_files, device_id, args.debug, debug_events_db)
                all_stats[device_id][drive_name] = drive_stats

                timestamp = parse_drive_timestamp(drive_name)

                db_entry = {
                    "device_id": device_id,
                    "host": "offline",
                    "drive": drive_name,
                    "total_time": drive_stats['total_time'],
                    "active_time": drive_stats['active_time'],
                    "drive_time": drive_stats['drive_time'],
                    "drive_time_active": drive_stats['drive_time_active'],
                    "cruise_press_time_ns": drive_stats['cruise_press_time_ns'],
                    "cruise_press_seconds": drive_stats['cruise_press_seconds'],
                    "cruise_press_seconds_per_hour": drive_stats['cruise_press_seconds_per_hour'],
                    "engagement_pct": drive_stats['engagement_pct'],
                    "drive_time_engagement_pct": drive_stats.get('drive_time_engagement_pct'),
                    "engagement_pct_odo": drive_stats.get('engagement_pct_odo'),
                    "odo_distance": drive_stats['odo_distance'],
                    "engaged_distance": drive_stats.get('engaged_distance'),
                    "interventions_per_100km": drive_stats.get('interventions_per_100km'),
                    "total_state_changes": drive_stats.get('total_state_changes'),
                    "intervention_count": drive_stats.get('intervention_count'),
                    "steer_intervention_count": drive_stats.get('steer_intervention_count'),
                    "steer_interventions_per_100km": drive_stats.get('steer_interventions_per_100km'),
                    "recorded_at": timestamp
                }

                if version_info['version']:
                    db_entry['version'] = version_info['version']
                if version_info['git_branch']:
                    db_entry['git_branch'] = version_info['git_branch']
                if version_info['git_commit']:
                    db_entry['git_commit'] = version_info['git_commit']
                if version_info['deviceType']:
                    db_entry['deviceType'] = version_info['deviceType']

                upsert_drive_to_db(engagement_db, db_entry)
                engagement_db_modified = True

    if not selected_dongles:
        # Original SSH-based processing
        # Load SSH host config and manually override targets
        ssh_hosts, host_configurations = get_ssh_host_details()
        if not ssh_hosts:
            print("No SSH hosts found in .ssh/config.")
            return

        processed_devices = set()  # Track which devices we've already processed
        ssh_hosts = ["C2", "C2_hotspot"]

        # Loop through each SSH host
        for host in ssh_hosts:
            print(f"\n🚀 Processing host: {host}")
            host_config = host_configurations[host]
            ssh_host = host_config.get("HostName", host_config["Host"])
            ssh_port = host_config.get("Port", 22)
            ssh_username = host_config.get("User", "root")
            private_key_path = os.path.expanduser("~/OPstuff/Retropilot_server_tools/op.pem")
            remote_directory = '/data/media/0/realdata'
            
            # Initialize device_id with fallback FIRST
            device_id = host
            ssh_client = None
            sftp_client = None

            try:
                private_key = paramiko.RSAKey.from_private_key_file(private_key_path)
                ssh_client = paramiko.SSHClient()
                ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh_client.connect(ssh_host, ssh_port, ssh_username, pkey=private_key, timeout=10)
                
                # Get the actual device identifier
                device_id = get_device_identifier(ssh_client, host)
                print(f"📱 Device identified as: {device_id}")
                
                # Skip if we've already processed this device
                if device_id in processed_devices:
                    print(f"⚠️ Device {device_id} already processed, skipping {host}")
                    ssh_client.close()
                    continue
                processed_devices.add(device_id)
                
                sftp_client = ssh_client.open_sftp()
                configure_sftp_timeout(sftp_client)

                stdin, stdout, stderr = ssh_client.exec_command(f"find {remote_directory} -name 'rlog*' ")
                all_rlog_files = stdout.read().decode().splitlines()

                segments = {}
                for rlog_file in all_rlog_files:
                    segment_path = os.path.dirname(rlog_file)
                    filename = os.path.basename(rlog_file)
                    if segment_path not in segments:
                        segments[segment_path] = {}
                    if filename.endswith('.bz2'):
                        segments[segment_path]['compressed'] = rlog_file
                    else:
                        segments[segment_path]['uncompressed'] = rlog_file

                files_to_process = []
                for segment_path, files in segments.items():
                    compressed_existing = files.get('compressed')
                    uncompressed_path = files.get('uncompressed')

                    if uncompressed_path:
                        segment_label = os.path.basename(segment_path)
                        if compressed_existing:
                            print(f"⚠️ Both raw and compressed logs present for {segment_label}; assuming previous run was interrupted.")
                            try:
                                ssh_client.exec_command(f"rm -f {compressed_existing}")
                            except Exception as e:
                                print(f"⚠️ Unable to remove stale compressed file {compressed_existing}: {e}")

                        compressed_path = uncompressed_path + '.bz2'

                        if compress_on_c2:
                            success = False
                            for attempt in range(1, SFTP_MAX_RETRIES + 1):
                                start_time = time.monotonic()
                                try:
                                    print(f"🗜️ Remote-compressing {uncompressed_path} (attempt {attempt}/{SFTP_MAX_RETRIES})")
                                    stdin_cmd, stdout_cmd, stderr_cmd = ssh_client.exec_command(f"bzip2 -f {uncompressed_path}")
                                    stdout_cmd.channel.settimeout(SFTP_SOCKET_TIMEOUT)
                                    exit_status = stdout_cmd.channel.recv_exit_status()
                                    duration = time.monotonic() - start_time

                                    if exit_status == 0:
                                        print(f"⏱️ Remote compression finished in {duration:.1f}s → {compressed_path}")
                                        files_to_process.append(compressed_path)
                                        success = True
                                        break

                                    error_output = stderr_cmd.read().decode().strip()
                                    print(f"⚠️ Remote compression failed (status {exit_status}) for {uncompressed_path}: {error_output}")
                                except (socket.timeout, paramiko.SSHException, EOFError, OSError) as e:
                                    duration = time.monotonic() - start_time
                                    print(f"⚠️ Remote compression attempt {attempt} failed for {uncompressed_path} after {duration:.1f}s: {e}")

                                if attempt == SFTP_MAX_RETRIES:
                                    print(f"❌ Giving up on remote compression of {uncompressed_path}")
                                    break

                                time.sleep(SFTP_RETRY_DELAY)
                                try:
                                    sftp_client.close()
                                except Exception:
                                    pass
                                sftp_client = reopen_sftp_client(ssh_client)

                            if not success:
                                continue
                        else:
                            success = False
                            for attempt in range(1, SFTP_MAX_RETRIES + 1):
                                start_time = time.monotonic()
                                try:
                                    print(f"🗜️ Compressing {uncompressed_path} → {compressed_path} (attempt {attempt}/{SFTP_MAX_RETRIES})")
                                    download_start = time.monotonic()
                                    with sftp_client.open(uncompressed_path, 'rb') as src_f:
                                        raw_data = src_f.read()
                                    download_duration = time.monotonic() - download_start

                                    compress_start = time.monotonic()
                                    compressed_data = bz2.compress(raw_data)
                                    compress_duration = time.monotonic() - compress_start

                                    upload_start = time.monotonic()
                                    with sftp_client.open(compressed_path, 'wb') as dst_f:
                                        dst_f.write(compressed_data)
                                    upload_duration = time.monotonic() - upload_start

                                    print(f"🧹 Deleting original: {uncompressed_path}")
                                    ssh_client.exec_command(f"rm {uncompressed_path}")
                                    total_duration = time.monotonic() - start_time
                                    print(
                                        f"⏱️ Local compression timings — download: {download_duration:.1f}s, "
                                        f"compress: {compress_duration:.1f}s, upload: {upload_duration:.1f}s, "
                                        f"total: {total_duration:.1f}s → {compressed_path}"
                                    )
                                    files_to_process.append(compressed_path)
                                    success = True
                                    break
                                except (socket.timeout, paramiko.SSHException, EOFError, OSError) as e:
                                    duration = time.monotonic() - start_time
                                    print(f"⚠️ Compression attempt {attempt} failed for {uncompressed_path} after {duration:.1f}s: {e}")
                                    try:
                                        ssh_client.exec_command(f"rm {compressed_path}")
                                    except Exception:
                                        pass

                                if attempt == SFTP_MAX_RETRIES:
                                    print(f"❌ Giving up on {uncompressed_path} after repeated failures")
                                    break

                                time.sleep(SFTP_RETRY_DELAY)
                                try:
                                    sftp_client.close()
                                except Exception:
                                    pass
                                sftp_client = reopen_sftp_client(ssh_client)

                            if not success:
                                continue
                    elif compressed_existing:
                        files_to_process.append(compressed_existing)

                # Check which drives are already in the database
                existing_keys = {(e.get("device_id", ""), e.get("drive", "")) for e in engagement_db}
                new_rlog_files = [f for f in files_to_process if args.reprocess or (device_id, os.path.basename(os.path.dirname(f))[:20]) not in existing_keys]

                if not new_rlog_files:
                    print(f"⚠️ No new drives to process for {host}")
                    sftp_client.close()
                    ssh_client.close()
                    continue

                # Create local directory using device_id instead of host
                local_host_dir = os.path.join(".", "rlogs", device_id)
                os.makedirs(local_host_dir, exist_ok=True)

                downloaded_rlog_files = []
                for rlog_file in new_rlog_files:
                    segment_name = os.path.basename(os.path.dirname(rlog_file))
                    local_segment_dir = os.path.join(local_host_dir, segment_name)
                    os.makedirs(local_segment_dir, exist_ok=True)
                    local_path = os.path.join(local_segment_dir, os.path.basename(rlog_file))
                    if not os.path.exists(local_path):
                        success = False
                        for attempt in range(1, SFTP_MAX_RETRIES + 1):
                            start_time = time.monotonic()
                            try:
                                print(f"⬇️ Copying {rlog_file} to {local_path} (attempt {attempt}/{SFTP_MAX_RETRIES})")
                                sftp_client.get(rlog_file, local_path)
                                duration = time.monotonic() - start_time
                                print(f"⏱️ Download finished in {duration:.1f}s")
                                success = True
                                break
                            except (socket.timeout, paramiko.SSHException, EOFError, OSError) as e:
                                duration = time.monotonic() - start_time
                                print(f"⚠️ Download attempt {attempt} failed for {rlog_file} after {duration:.1f}s: {e}")
                                if os.path.exists(local_path):
                                    try:
                                        os.remove(local_path)
                                    except Exception:
                                        pass

                                if attempt == SFTP_MAX_RETRIES:
                                    print(f"❌ Giving up on download of {rlog_file}")
                                    break

                                time.sleep(SFTP_RETRY_DELAY)
                                try:
                                    sftp_client.close()
                                except Exception:
                                    pass
                                sftp_client = reopen_sftp_client(ssh_client)

                        if not success:
                            continue

                    downloaded_rlog_files.append(rlog_file)

                if not downloaded_rlog_files:
                    print(f"⚠️ No files downloaded successfully for {host}, skipping")
                    sftp_client.close()
                    ssh_client.close()
                    continue

                drives = {}
                for rlog_file in downloaded_rlog_files:
                    drive_name = os.path.basename(os.path.dirname(rlog_file))[:20]
                    if drive_name not in drives:
                        drives[drive_name] = []
                    drives[drive_name].append(rlog_file)

                # Filter drives if --start is provided
                drives = filter_drives_by_date_range(drives, args.start, args.stop)

                # FIX: Use .get() to safely access dictionary keys
                existing_keys = {(e.get("device_id", ""), e.get("drive", "")) for e in engagement_db}

                for drive_name, rlog_files_remote in drives.items(): # Renamed for clarity
                    # Create a list of local paths for the current drive
                    local_rlog_paths_for_drive = []
                    for rlog_file_remote in rlog_files_remote:
                        segment_name = os.path.basename(os.path.dirname(rlog_file_remote))
                        local_path = os.path.join(local_host_dir, segment_name, os.path.basename(rlog_file_remote))
                        local_rlog_paths_for_drive.append(local_path)

                    # CRITICAL: Sort rlog files by segment number for proper processing
                    # This sorting should happen on the local paths now
                    local_rlog_paths_for_drive_sorted = sorted(local_rlog_paths_for_drive, key=extract_segment_number)
                    
                    # NEW: Extract version information from the drive
                    version_info = extract_version_info(local_rlog_paths_for_drive_sorted, local_host_dir)
                    
                    drive_stats, version_info = process_drive_offline(drive_name, local_rlog_paths_for_drive_sorted, device_id, args.debug, debug_events_db)
                    # The rest of the processing for drive_stats and db_entry should remain here,
                    # as process_drive_offline now returns the calculated stats.

                    # Upsert drive statistics in the engagement database
                    timestamp = parse_drive_timestamp(drive_name)

                    db_entry = {
                        "device_id": device_id,
                        "host": host,  # Keep host info for reference
                        "drive": drive_name,
                        "total_time": drive_stats['total_time'],
                        "active_time": drive_stats['active_time'],
                        "drive_time": drive_stats['drive_time'],
                        "drive_time_active": drive_stats['drive_time_active'],
                        "cruise_press_time_ns": drive_stats['cruise_press_time_ns'],
                        "cruise_press_seconds": drive_stats['cruise_press_seconds'],
                        "cruise_press_seconds_per_hour": drive_stats['cruise_press_seconds_per_hour'],
                        "engagement_pct": drive_stats['engagement_pct'],
                        "drive_time_engagement_pct": drive_stats.get('drive_time_engagement_pct'),
                        "engagement_pct_odo": drive_stats.get('engagement_pct_odo'),
                        "odo_distance": drive_stats['odo_distance'],
                        "engaged_distance": drive_stats.get('engaged_distance'),
                        "interventions_per_100km": drive_stats.get('interventions_per_100km'),
                        "total_state_changes": drive_stats.get('total_state_changes'),
                        "intervention_count": drive_stats.get('intervention_count'),
                        "steer_intervention_count": drive_stats.get('steer_intervention_count'),
                        "steer_interventions_per_100km": drive_stats.get('steer_interventions_per_100km'),
                        "recorded_at": timestamp
                    }

                    # Add version information if available
                    if version_info['version']:
                        db_entry['version'] = version_info['version']
                    if version_info['git_branch']:
                        db_entry['git_branch'] = version_info['git_branch']
                    if version_info['git_commit']:
                        db_entry['git_commit'] = version_info['git_commit']
                    if version_info['deviceType']:
                        db_entry['deviceType'] = version_info['deviceType']

                    upsert_drive_to_db(engagement_db, db_entry)
                    engagement_db_modified = True

                    if device_id not in all_stats:
                        all_stats[device_id] = {}
                    all_stats[device_id][drive_name] = drive_stats

                if sftp_client:
                    sftp_client.close()
                if ssh_client:
                    ssh_client.close()

            except Exception as e:
                print(f"❌ Could not connect to {host}: {e}")
                # device_id is already initialized at the top of the loop, so this should work
                print(f"📱 Using fallback device ID: {device_id}")
                # Clean up connections if they were opened
                if sftp_client:
                    try:
                        sftp_client.close()
                    except:
                        pass
                if ssh_client:
                    try:
                        ssh_client.close()
                    except:
                        pass
                continue

    # Print final stats summary to console
    print("\n" + "="*140)
    print("📊 ENGAGEMENT SUMMARY")
    print("="*140)
    
    # Filter the summary if a start or stop drive is provided
    if args.start or args.stop:
        for device_id in all_stats:
            all_stats[device_id] = filter_drives_by_date_range(all_stats[device_id], args.start, args.stop)

    for device_id, drives in all_stats.items():
        print(f"\n🚗 Device: {device_id}")
        print("-" * 140)
        
        total_device_active_time = 0
        total_device_time = 0
        total_device_drive_time = 0
        total_device_drive_active_time = 0
        total_interventions = 0
        total_steer_interventions = 0
        total_distance = 0
        total_engaged_distance = 0
        total_cruise_press_time_ns = 0

        sorted_drives = sorted(
            drives.items(),
            key=lambda item: parse_drive_timestamp(item[0]) or datetime.min
        )

        # Header for drive details
        print(
            f"{'Date/Time':<20} {'Duration':<11} {'DriveDur':<11} {'Distance':<9} {'Engaged':<9} "
            f"{'Time%':<7} {'Drive%':<7} {'ODO%':<7} {'Diseng':<6} {'DIS/100km':<9} {'Steer':<6} {'ST/100km':<8} {'Press_s/h':<10}"
        )
        print("-" * 140)

        for drive, stats in sorted_drives:
            total_device_active_time += stats['active_time']
            total_device_time += stats['total_time']
            if stats.get('drive_time'):
                total_device_drive_time += stats['drive_time']
            if stats.get('drive_time_active'):
                total_device_drive_active_time += stats['drive_time_active']
            
            # Accumulate intervention stats
            if stats.get('intervention_count') is not None:
                total_interventions += stats['intervention_count']
            if stats.get('steer_intervention_count') is not None:
                total_steer_interventions += stats['steer_intervention_count']
            if stats.get('odo_distance') is not None:
                total_distance += stats['odo_distance']
            if stats.get('engaged_distance') is not None:
                total_engaged_distance += stats['engaged_distance']
            if stats.get('cruise_press_time_ns') is not None:
                total_cruise_press_time_ns += stats['cruise_press_time_ns']
            
            if stats['total_time'] > 0:
                percentage = (stats['active_time'] / stats['total_time']) * 100
                duration_minutes = stats['total_time'] / 1e9 / 60  # ns to min
                drive_time_value = stats.get('drive_time', 0) or 0
                drive_duration_minutes = drive_time_value / 1e9 / 60
                
                # Format the drive data in columns
                date_time = drive.replace('--', ' ').replace('-', '/')
                duration_str = f"{duration_minutes:.1f}min"
                drive_duration_str = f"{drive_duration_minutes:.1f}min" if drive_duration_minutes > 0 else "0.0min"
                distance_str = f"{stats.get('odo_distance', 0):.1f}km" if stats.get('odo_distance') else "N/A"
                engaged_str = f"{stats.get('engaged_distance', 0):.1f}km" if stats.get('engaged_distance') else "0.0km"
                time_pct_str = f"{percentage:.1f}%"
                drive_pct_value = stats.get('drive_time_engagement_pct')
                drive_pct_str = f"{drive_pct_value:.1f}%" if drive_pct_value is not None else "N/A"
                odo_pct_value = stats.get('engagement_pct_odo')
                odo_pct_str = f"{odo_pct_value:.1f}%" if odo_pct_value is not None else "N/A"
                diseng_str = f"{stats.get('intervention_count', 0)}"
                diseng_100km_value = stats.get('interventions_per_100km')
                diseng_100km_str = f"{diseng_100km_value:.1f}" if diseng_100km_value is not None else "0.0"
                steer_str = f"{stats.get('steer_intervention_count', 0)}"
                steer_100km_value = stats.get('steer_interventions_per_100km')
                steer_100km_str = f"{steer_100km_value:.1f}" if steer_100km_value is not None else "0.0"
                press_s_per_hour = stats.get('cruise_press_seconds_per_hour')
                press_s_per_hour_str = f"{press_s_per_hour:.2f}" if press_s_per_hour is not None else "0.00"

                print(
                    f"{date_time:<20} {duration_str:<11} {drive_duration_str:<11} {distance_str:<9} {engaged_str:<9} "
                    f"{time_pct_str:<7} {drive_pct_str:<7} {odo_pct_str:<7} {diseng_str:<6} {diseng_100km_str:<9} {steer_str:<6} {steer_100km_str:<8} {press_s_per_hour_str:<10}"
                )

        print("-" * 140)
        
        if total_device_time > 0:
            total_percentage = (total_device_active_time / total_device_time) * 100
            total_drive_percentage = (
                (total_device_drive_active_time / total_device_drive_time) * 100
                if total_device_drive_time > 0 else None
            )
            # Calculate overall intervention rate
            total_interventions_per_100km = (total_interventions / total_distance * 100) if total_distance > 0 else 0
            total_steer_interventions_per_100km = (total_steer_interventions / total_distance * 100) if total_distance > 0 else 0
            
            print(f"📈 TOTALS:")
            print(f"   • Total Distance: {total_distance:.1f} km")
            print(f"   • Overall Time Engagement: {format_pct(total_percentage)}")
            if total_distance > 0 and total_engaged_distance > 0:
                total_odo_engagement_pct = (total_engaged_distance / total_distance) * 100
                print(f"   • Overall ODO Engagement: {format_pct(total_odo_engagement_pct)}")
            if total_device_drive_time > 0:
                total_drive_minutes = total_device_drive_time / 1e9 / 60
                print(
                    f"   • Total Drive Time (> {VEGO_MOVING_THRESHOLD} m/s): {total_drive_minutes:.1f} min"
                )
                print(f"   • Drive-Time Engagement: {format_pct(total_drive_percentage)}")
                total_drive_hours = total_device_drive_time / 1e9 / 3600
                if total_drive_hours > 0 and total_cruise_press_time_ns > 0:
                    total_press_seconds = total_cruise_press_time_ns / 1e9
                    print(f"   • Total Cruise Button Press Time: {total_press_seconds:.1f} s")
                    press_seconds_per_hour = total_press_seconds / total_drive_hours
                    print(f"   • Cruise Press Seconds per Drive Hour: {press_seconds_per_hour:.2f}s")
            print(f"   • Total Disengagements: {total_interventions} ({total_interventions_per_100km:.2f}/100km)")
            print(f"   • Total Steering Interventions: {total_steer_interventions} ({total_steer_interventions_per_100km:.2f}/100km)")
        
        print("="*140)

    # Persist engagement database to disk (with backup) only if it changed
    if engagement_db_modified:
        backup_file(ENGAGEMENT_DB_FILE)
        with open(ENGAGEMENT_DB_FILE, 'w') as f:
            json.dump(engagement_db, f, indent=2)

    # Persist debug events database to disk if debug mode was active and modified
    if args.debug and debug_events_db is not None and debug_db_modified:
        backup_file(DEBUG_DB_FILE)
        with open(DEBUG_DB_FILE, 'w') as f:
            json.dump(debug_events_db, f, indent=2)

if __name__ == "__main__":
    main()
