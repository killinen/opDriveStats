import os
import json
import re
import bz2
import urllib.parse
from datetime import datetime
from collections import deque
from stat import S_ISDIR
import paramiko

try:
    import capnp
except ImportError:
    print("capnp library not found, probably needs OpenPilot environment")

from local_cereal import log as capnp_log

# Constants
# PROCESSED_DRIVES_FILE = 'processed_drives.json'
# ENGAGEMENT_STATS_FILE = 'engagement_stats.json'
numbers = re.compile(r'(\d+)')

class MovingAverageFilter:
    def __init__(self, window_size):
        self.window_size = window_size
        self.values = deque()

    def update(self, val):
        self.values.append(val)
        if len(self.values) > self.window_size:
            self.values.popleft()
        return sum(self.values) / len(self.values)

def get_device_identifier(ssh_client, host):
    """
    Attempt to get a unique device identifier from the remote system.
    Returns the actual device name to use in the database.
    """
    try:
        stdin, stdout, stderr = ssh_client.exec_command("cat /data/params/d/DongleId 2>/dev/null || echo 'no_dongle_id'")
        dongle_id = stdout.read().decode().strip()
        
        if "no_dongle_id" not in dongle_id and dongle_id:
            print(f"⚠️ Found dongle id: {dongle_id}")
            return f"{dongle_id[-8:]}"
            
    except Exception as e:
        print(f"⚠️ Could not get device identifier for {host}: {e}")
    
    return host

def extract_segment_number(rlog_path):
    """Extract segment number from path like '/path/segment--123/rlog.bz2'"""
    segment_name = os.path.basename(os.path.dirname(rlog_path))
    parts = segment_name.split('--')
    if len(parts) >= 2:
        try:
            return int(parts[-1])
        except ValueError:
            return 0
    return 0

def parse_drive_timestamp(drive_name):
    try:
        return datetime.strptime(drive_name, "%Y-%m-%d--%H-%M-%S").isoformat()
    except ValueError:
        return None

def numerical_sort(value):
    parts = numbers.split(value)
    parts[1::2] = map(int, parts[1::2])
    return parts

class LogReader(object):
    def __init__(self, fn):
        self._ents = []
        try:
            print(f"\U0001f9ea Reading file: {fn}")
            with open(fn, "rb") as f:
                dat = f.read()
            _, ext = os.path.splitext(urllib.parse.urlparse(fn).path)
            if ext == ".bz2":
                try:
                    dat = bz2.decompress(dat)
                except Exception as e:
                    print(f"⚠️ Could not decompress bz2: {fn}, skipping. Error: {e}")
                    return
            if len(dat) < 100:
                print(f"⚠️ Skipping too small rlog: {fn}")
                return
            try:
                self._ents = capnp_log.Event.read_multiple_bytes(dat)
            except Exception as e:
                print(f"❌ Failed to parse capnp from {fn}: {e}")
                return
        except Exception as e:
            print(f"⚠️ Could not open/read {fn}: {e}")

    def __iter__(self):
        return iter(self._ents)

def get_ssh_host_details():
    config_path = os.path.expanduser("~/.ssh/config")
    hosts = []
    host_configurations = {}
    if os.path.exists(config_path):
        with open(config_path, "r") as file:
            current_host = None
            for line in file:
                if line.startswith("Host "):
                    current_host = line.split()[1]
                    hosts.append(current_host)
                    host_configurations[current_host] = {'Host': current_host}
                elif current_host:
                    if line.strip().startswith("HostName"):
                        host_configurations[current_host]['HostName'] = line.split()[1]
                    elif line.strip().startswith("Port"):
                        host_configurations[current_host]['Port'] = int(line.split()[1])
                    elif line.strip().startswith("User"):
                        host_configurations[current_host]['User'] = line.split()[1]
    else:
        print("No .ssh/config file found.")
    return hosts, host_configurations

def extract_version_info(rlog_files_sorted, local_host_dir):
    """
    Extract version information from initData messages in the first rlog file of a drive.
    Returns a dictionary with version, gitBranch, and gitCommit if found.
    """
    version_info = {
        'version': None,
        'git_branch': None,
        'git_commit': None,
        'deviceType': None
    }
    
    for rlog_file in rlog_files_sorted[:3]:
        if isinstance(rlog_file, str) and rlog_file.startswith('/data/'):
            local_path = os.path.join(local_host_dir, os.path.basename(os.path.dirname(rlog_file)), os.path.basename(rlog_file))
        else:
            local_path = rlog_file
        
        try:
            lr = LogReader(local_path)
            for msg in lr:
                if msg.which() == 'initData':
                    init_data = msg.initData
                    
                    if hasattr(init_data, 'version') and init_data.version:
                        version_info['version'] = init_data.version
                        print(f"📦 Found version: {init_data.version}")
                    
                    if hasattr(init_data, 'gitBranch') and init_data.gitBranch:
                        version_info['git_branch'] = init_data.gitBranch  
                        print(f"🔧 Found git branch: {init_data.gitBranch}")
                    
                    if hasattr(init_data, 'gitCommit') and init_data.gitCommit:
                        version_info['git_commit'] = init_data.gitCommit
                        print(f"📝 Found git commit: {init_data.gitCommit}")

                    if hasattr(init_data, 'deviceType') and init_data.deviceType is not None:
                        version_info['deviceType'] = str(init_data.deviceType).split('.')[-1]
                        print(f"📦 Found deviceType: {version_info['deviceType']}")
                    
                    if any(version_info.values()):
                        return version_info
                        
        except Exception as e:
            print(f"⚠️ Could not extract version info from {local_path}: {e}")
            continue
    
    if not any(version_info.values()):
        print("⚠️ No version information found in initData messages")
    
    return version_info

def find_local_rlog_files(dongle_id):
    """
    Find all local rlog files for a given dongle_id in the ./rlogs/ directory.
    Returns a list of drives and their rlog files.
    """
    local_dir = os.path.join(".", "rlogs", dongle_id)
    if not os.path.exists(local_dir):
        print(f"❌ Local directory not found: {local_dir}")
        return {}
    
    drives = {}
    
    for item in os.listdir(local_dir):
        item_path = os.path.join(local_dir, item)
        if os.path.isdir(item_path):
            drive_name = item[:20]
            
            rlog_files = []
            for file in os.listdir(item_path):
                if file.startswith('rlog') and (file.endswith('.bz2') or not '.' in file):
                    rlog_files.append(os.path.join(item_path, file))
            
            if rlog_files:
                if drive_name not in drives:
                    drives[drive_name] = []
                drives[drive_name].extend(rlog_files)
    
    for drive_name in drives:
        drives[drive_name] = sorted(list(set(drives[drive_name])), key=extract_segment_number)
    
    print(f"📁 Found {len(drives)} drives in local directory: {local_dir}")
    for drive_name, files in drives.items():
        print(f"  • {drive_name}: {len(files)} segments")
    
    return drives

def upsert_drive_to_db(engagement_db, new_entry):
    """
    Updates an existing drive entry or adds a new one to the engagement database.
    """
    device_id = new_entry.get("device_id")
    drive_name = new_entry.get("drive")
    
    for i, entry in enumerate(engagement_db):
        if entry.get("device_id") == device_id and entry.get("drive") == drive_name:
            engagement_db[i] = new_entry
            print(f"Updated drive {drive_name} in the database.")
            return
    
    engagement_db.append(new_entry)
    print(f"Added new drive {drive_name} to the database.")

def filter_drives_by_date_range(drives, start_drive, stop_drive):
    """
    Filters a dictionary of drives to include only those within the specified date range.
    """
    if start_drive:
        print(f"Filtering drives to start from: {start_drive}")
        drives = {
            drive_name: files
            for drive_name, files in drives.items()
            if drive_name >= start_drive
        }

    if stop_drive:
        print(f"Filtering drives to stop at: {stop_drive}")
        drives = {
            drive_name: files
            for drive_name, files in drives.items()
            if drive_name <= stop_drive
        }

    print(f"Found {len(drives)} drives to process after filtering.")
    return drives
