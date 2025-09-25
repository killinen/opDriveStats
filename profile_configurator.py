#!/usr/bin/env python3
"""Interactive curses configurator for vehicle profile mappings."""

from __future__ import annotations

import curses
import json
import os
from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Tuple

import cantools


CONFIG_PATH = "config/vehicle_profiles.json"
DBC_DIR = "DBC"
SIGNAL_POINTS: List[Tuple[str, str]] = [
    ("odometer", "Odometer"),
    ("cruise_state", "Cruise Buttons"),
    ("steering_driver_torque", "Steering Driver Torque"),
    ("steering_motor_torque", "Steering Motor Torque"),
]
MULTI_SIGNAL_KEYS = {"cruise_state"}
DISPLAY_LABELS = {key: label for key, label in SIGNAL_POINTS}
_DBC_CACHE: Dict[str, List[Dict[str, Any]]] = {}
_DBC_ERRORS: Dict[str, str] = {}


@dataclass
class AppState:
    config: Dict[str, Any]
    profile_order: List[str]
    selected_index: int = 0
    dirty: bool = False
    status: str = "Press ? for help"

    @property
    def profiles(self) -> Dict[str, Any]:
        return self.config.setdefault("profiles", {})

    def ensure_selection(self) -> None:
        if not self.profile_order:
            self.selected_index = 0
            return
        self.selected_index = max(0, min(self.selected_index, len(self.profile_order) - 1))

    def selected_profile_name(self) -> Optional[str]:
        if not self.profile_order:
            return None
        if 0 <= self.selected_index < len(self.profile_order):
            return self.profile_order[self.selected_index]
        return None


def load_config(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {"default_profile": "", "profiles": {}}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Unable to parse {path}: {exc}")


def save_config(path: str, state: AppState) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    ordered_profiles = {name: state.profiles[name] for name in state.profile_order}
    payload = {
        "default_profile": state.config.get("default_profile", ""),
        "profiles": ordered_profiles,
    }
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")


def list_dbc_files(directory: str = DBC_DIR) -> List[str]:
    if not os.path.isdir(directory):
        return []
    files: List[str] = []
    for entry in sorted(os.listdir(directory)):
        full_path = os.path.join(directory, entry)
        if os.path.isfile(full_path) and entry.lower().endswith(".dbc"):
            files.append(entry)
    return files


def resolve_dbc_path(path: str) -> Optional[str]:
    if not path:
        return None
    candidate = path
    if not os.path.isabs(candidate):
        candidate = os.path.join(os.getcwd(), candidate)
    if not os.path.exists(candidate):
        return None
    return candidate


def get_dbc_error(path: str) -> str:
    real_path = resolve_dbc_path(path) or path
    return _DBC_ERRORS.get(real_path) or _DBC_ERRORS.get(path) or ""


def sanitize_dbc_contents(text: str) -> str:
    sanitized_lines: List[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("VAL_") and not stripped.endswith(";"):
            sanitized_lines.append(line.rstrip() + ";")
        else:
            sanitized_lines.append(line)
    return "\n".join(sanitized_lines)


def load_dbc_catalog(path: str) -> Optional[List[Dict[str, Any]]]:
    real_path = resolve_dbc_path(path)
    if real_path is None:
        _DBC_ERRORS[path] = "File not found"
        return None
    if real_path in _DBC_CACHE:
        return _DBC_CACHE[real_path]
    if real_path in _DBC_ERRORS:
        return None

    try:
        database = cantools.database.load_file(real_path)
    except Exception as primary_exc:
        try:
            with open(real_path, "r", encoding="utf-8") as handle:
                sanitized = sanitize_dbc_contents(handle.read())
            database = cantools.database.load_string(sanitized, database_format="dbc")
        except Exception as secondary_exc:
            _DBC_ERRORS[real_path] = str(secondary_exc) or str(primary_exc)
            return None

    entries: List[Dict[str, Any]] = []
    for message in sorted(database.messages, key=lambda m: (m.name or "", m.frame_id)):
        for signal in message.signals:
            entries.append(
                {
                    "message": message.name,
                    "signal": signal.name,
                    "frame_id": message.frame_id,
                    "units": getattr(signal, "unit", "") or "",
                    "comment": getattr(signal, "comment", None) or "",
                }
            )
    _DBC_CACHE[real_path] = entries
    _DBC_ERRORS.pop(real_path, None)
    _DBC_ERRORS.pop(path, None)
    return entries


def explain_entry(entry: Dict[str, Any], include_transform: bool = True) -> str:
    message = entry.get("message", "?")
    signal = entry.get("signal", "?")
    description = f"{message}.{signal}"
    source = entry.get("source")
    if source is not None:
        description += f" [bus={source}]"
    transform = entry.get("transform") if include_transform else None
    if transform:
        bits: List[str] = []
        scale = transform.get("scale")
        offset = transform.get("offset")
        if scale is not None:
            bits.append(f"scale={scale}")
        if offset is not None:
            bits.append(f"offset={offset}")
        if bits:
            description += " (" + ", ".join(bits) + ")"
    return description


def get_signal_entries(signals: Dict[str, Any], key: str) -> List[Dict[str, Any]]:
    value = signals.get(key)
    if value is None:
        return []
    if isinstance(value, list):
        return [entry for entry in value if isinstance(entry, dict)]
    if isinstance(value, dict):
        return [value]
    return []


def set_signal_entries(signals: Dict[str, Any], key: str, entries: List[Dict[str, Any]]) -> None:
    if not entries:
        signals.pop(key, None)
        return
    if len(entries) == 1 and key not in MULTI_SIGNAL_KEYS:
        signals[key] = entries[0]
    else:
        signals[key] = entries


def init_colors() -> None:
    if not curses.has_colors():
        raise SystemExit("Your terminal does not support colors required for the blue interface.")
    curses.start_color()
    curses.use_default_colors()
    curses.init_pair(1, curses.COLOR_WHITE, curses.COLOR_BLUE)   # base
    curses.init_pair(2, curses.COLOR_BLACK, curses.COLOR_CYAN)  # highlight
    curses.init_pair(3, curses.COLOR_YELLOW, curses.COLOR_BLUE) # titles / accents
    curses.init_pair(4, curses.COLOR_RED, curses.COLOR_BLUE)    # warnings


def draw_main(stdscr: "curses._CursesWindow", state: AppState) -> None:
    stdscr.erase()
    height, width = stdscr.getmaxyx()
    stdscr.bkgd(" ", curses.color_pair(1))

    title = " Vehicle Profile Configurator "
    stdscr.attron(curses.color_pair(3) | curses.A_BOLD)
    stdscr.addnstr(0, max(0, (width - len(title)) // 2), title, width - 2)
    stdscr.attroff(curses.color_pair(3) | curses.A_BOLD)

    help_line = "Arrows: navigate  A:add profile  O:edit DBC/details  E:map signals  D:delete  S:set default  W:save config  Q:quit"
    stdscr.attron(curses.color_pair(3))
    stdscr.addnstr(1, 2, help_line, width - 4)
    stdscr.attroff(curses.color_pair(3))

    list_width = max(24, width // 3)
    detail_x = list_width + 4

    stdscr.attron(curses.A_BOLD)
    stdscr.addnstr(3, 2, "Profiles", list_width - 2)
    stdscr.addnstr(3, detail_x, "Details", width - detail_x - 2)
    stdscr.attroff(curses.A_BOLD)

    for idx, name in enumerate(state.profile_order):
        y = 5 + idx
        if y >= height - 4:
            break
        attr = curses.A_BOLD if idx == state.selected_index else curses.A_NORMAL
        color = curses.color_pair(2) if idx == state.selected_index else curses.color_pair(1)
        stdscr.attrset(attr | color)
        marker = "*" if name == state.config.get("default_profile") else " "
        entry = f"{marker} {name}"
        stdscr.addnstr(y, 2, entry.ljust(list_width - 2), list_width - 2)
        stdscr.attrset(curses.color_pair(1))

    selected_name = state.selected_profile_name()
    if selected_name:
        profile = state.profiles[selected_name]
        lines = [
            f"Name: {selected_name}",
            f"Description: {profile.get('description', '')}",
            f"DBC file: {profile.get('dbc_file', '')}",
            "Signals:",
        ]
        row = 5
        for line in lines:
            stdscr.addnstr(row, detail_x, line, width - detail_x - 2)
            row += 1

        signals = profile.get("signals", {})
        if not signals:
            stdscr.addnstr(row, detail_x + 2, "(none)", width - detail_x - 4)
        else:
            for key in sorted(signals.keys()):
                entries = get_signal_entries(signals, key)
                if not entries:
                    continue
                for idx_line, entry in enumerate(entries):
                    if row >= height - 4:
                        break
                    label = DISPLAY_LABELS.get(key, key)
                    prefix = f"- {label}: "
                    if len(entries) > 1:
                        prefix = f"- {label} [{idx_line + 1}/{len(entries)}]: "
                    text = prefix + explain_entry(entry)
                    stdscr.addnstr(row, detail_x + 2, text, width - detail_x - 4)
                    row += 1

    status_color = curses.color_pair(4 if state.dirty else 3)
    stdscr.attron(status_color)
    clean_state = "*" if state.dirty else " "
    status = f"{clean_state} {state.status}"
    stdscr.addnstr(height - 2, 2, status.ljust(width - 4), width - 4)
    stdscr.attroff(status_color)

    stdscr.refresh()


def prompt_string(
    stdscr: "curses._CursesWindow",
    prompt: str,
    default: Optional[str] = None,
) -> Optional[str]:
    height, width = stdscr.getmaxyx()
    curses.echo()
    curses.curs_set(1)
    prompt_text = prompt
    if default:
        prompt_text += f" [{default}]"
    prompt_text += ": "
    stdscr.attrset(curses.color_pair(1) | curses.A_BOLD)
    stdscr.addnstr(height - 4, 2, " " * (width - 4), width - 4)
    stdscr.addnstr(height - 4, 2, prompt_text, width - 4)
    stdscr.refresh()

    input_win = curses.newwin(1, width - len(prompt_text) - 4, height - 4, len(prompt_text) + 2)
    try:
        user_input = input_win.getstr().decode("utf-8").strip()
    except KeyboardInterrupt:
        user_input = ""
    curses.noecho()
    curses.curs_set(0)
    stdscr.addnstr(height - 4, 2, " " * (width - 4), width - 4)
    if not user_input and default is not None:
        return default
    if not user_input:
        return None
    return user_input


def prompt_number(
    stdscr: "curses._CursesWindow",
    prompt: str,
    default: Optional[float] = None,
) -> Optional[float]:
    default_str = None if default is None else f"{default}"
    value = prompt_string(stdscr, prompt, default_str)
    if value in (None, ""):
        return default
    try:
        return float(value)
    except ValueError:
        return None


def prompt_confirm(
    stdscr: "curses._CursesWindow",
    message: str,
    default_yes: bool = False,
) -> bool:
    suffix = "[Y/n]" if default_yes else "[y/N]"
    response = prompt_string(stdscr, f"{message} {suffix}")
    if response is None or response == "":
        return default_yes
    return response.lower().startswith("y")


def add_profile(stdscr: "curses._CursesWindow", state: AppState) -> None:
    name = prompt_string(stdscr, "Profile name")
    if not name:
        state.status = "Name entry cancelled"
        return
    if name in state.profiles:
        state.status = f"Profile '{name}' already exists"
        return

    description = prompt_string(stdscr, "Description") or ""
    dbc_file = choose_dbc_file(stdscr, None)
    if dbc_file is None:
        dbc_file = ""

    state.profiles[name] = {
        "description": description,
        "dbc_file": dbc_file,
        "signals": {},
    }
    state.profile_order.append(name)
    state.selected_index = len(state.profile_order) - 1
    state.dirty = True
    state.status = f"Added profile '{name}'"


def delete_profile(stdscr: "curses._CursesWindow", state: AppState) -> None:
    name = state.selected_profile_name()
    if not name:
        state.status = "No profile selected"
        return
    if not prompt_confirm(stdscr, f"Delete profile '{name}'?", default_yes=False):
        state.status = "Delete cancelled"
        return

    state.profiles.pop(name, None)
    state.profile_order = [n for n in state.profile_order if n != name]
    if state.config.get("default_profile") == name:
        state.config["default_profile"] = ""
    state.dirty = True
    state.status = f"Deleted profile '{name}'"
    state.ensure_selection()


def set_default_profile(state: AppState) -> None:
    name = state.selected_profile_name()
    if not name:
        state.status = "No profile selected"
        return
    state.config["default_profile"] = name
    state.dirty = True
    state.status = f"Default profile set to '{name}'"


def edit_profile_details(stdscr: "curses._CursesWindow", state: AppState) -> None:
    name = state.selected_profile_name()
    if not name:
        state.status = "No profile selected"
        return
    profile = state.profiles[name]
    description = prompt_string(stdscr, "Description", profile.get("description", ""))
    if description is None:
        state.status = "Edit cancelled"
        return
    dbc_file = choose_dbc_file(stdscr, profile.get("dbc_file"))
    if dbc_file is None:
        dbc_file = profile.get("dbc_file", "")
    profile["description"] = description
    profile["dbc_file"] = dbc_file
    state.dirty = True
    state.status = f"Updated profile '{name}'"


def edit_signals(stdscr: "curses._CursesWindow", state: AppState) -> None:
    name = state.selected_profile_name()
    if not name:
        state.status = "No profile selected"
        return

    profile = state.profiles[name]
    dbc_path = profile.get("dbc_file")
    if not dbc_path:
        state.status = "Set a DBC file before mapping signals"
        return

    catalog = load_dbc_catalog(dbc_path)
    if catalog is None:
        reason = get_dbc_error(dbc_path)
        detail = f": {reason}" if reason else ""
        state.status = f"Unable to read DBC '{dbc_path}'{detail}"
        return
    if not catalog:
        state.status = f"DBC '{dbc_path}' has no signals"
        return

    signals = profile.setdefault("signals", {})
    idx = 0
    status = "Select a datapoint to map (Cruise Buttons supports multiples)"

    point_keys = [key for key, _ in SIGNAL_POINTS]
    point_key_set = set(point_keys)

    while True:
        extra_keys = [k for k in signals.keys() if k not in point_key_set]
        stdscr.erase()
        height, width = stdscr.getmaxyx()
        stdscr.bkgd(" ", curses.color_pair(1))

        header = f" Signal Mapping for {name} "
        stdscr.attron(curses.color_pair(3) | curses.A_BOLD)
        stdscr.addnstr(0, max(0, (width - len(header)) // 2), header, width - 2)
        stdscr.attroff(curses.color_pair(3) | curses.A_BOLD)

        instructions = "Arrows: navigate  Enter/E: map signal  C:clear  B:back"
        stdscr.attron(curses.color_pair(3))
        stdscr.addnstr(1, 2, instructions, width - 4)
        stdscr.attroff(curses.color_pair(3))

        display_row = 3
        for point_index, (key, label) in enumerate(SIGNAL_POINTS):
            entries = get_signal_entries(signals, key)
            if entries:
                for entry_idx, entry in enumerate(entries):
                    if display_row >= height - 4:
                        break
                    suffix = f" [{entry_idx + 1}/{len(entries)}]" if len(entries) > 1 else ""
                    description = f"{label}{suffix}: " + explain_entry(entry)
                    highlight = point_index == idx
                    attr = curses.A_BOLD if highlight else curses.A_NORMAL
                    color = curses.color_pair(2) if highlight else curses.color_pair(1)
                    stdscr.attrset(attr | color)
                    stdscr.addnstr(display_row, 2, description.ljust(width - 4), width - 4)
                    display_row += 1
            else:
                description = f"{label}: (not set)"
                highlight = point_index == idx
                attr = curses.A_BOLD if highlight else curses.A_NORMAL
                color = curses.color_pair(2) if highlight else curses.color_pair(1)
                stdscr.attrset(attr | color)
                stdscr.addnstr(display_row, 2, description.ljust(width - 4), width - 4)
                display_row += 1

        info_y = display_row + 1
        if extra_keys and info_y < height - 4:
            stdscr.attrset(curses.color_pair(3) | curses.A_BOLD)
            stdscr.addnstr(info_y, 2, "Additional signals preserved in config:", width - 4)
            stdscr.attrset(curses.color_pair(1))
            for offset, key in enumerate(extra_keys, start=1):
                line_y = info_y + offset
                if line_y >= height - 4:
                    break
                stdscr.addnstr(line_y, 4, key, width - 6)

        stdscr.attrset(curses.color_pair(3))
        stdscr.addnstr(height - 2, 2, status.ljust(width - 4), width - 4)
        stdscr.refresh()

        keypress = stdscr.getch()
        if keypress in (ord("b"), ord("B"), 27):
            break
        if keypress in (curses.KEY_UP, ord("k")):
            idx = (idx - 1) % len(SIGNAL_POINTS)
        elif keypress in (curses.KEY_DOWN, ord("j")):
            idx = (idx + 1) % len(SIGNAL_POINTS)
        elif keypress in (curses.KEY_ENTER, 10, 13, ord("e"), ord("E")):
            point_key, label = SIGNAL_POINTS[idx]
            existing_entries = get_signal_entries(signals, point_key)
            existing_reference = existing_entries[-1] if existing_entries else {}
            selection = choose_message_signal(stdscr, dbc_path, existing_reference)
            if selection is None:
                status = "Selection cancelled"
                continue
            message_name, signal_name = selection
            overrides = prompt_signal_overrides(stdscr, existing_reference)
            if overrides is None:
                status = "Edit cancelled"
                continue

            entry: Dict[str, Any] = {
                "message": message_name,
                "signal": signal_name,
            }
            entry.update(overrides)
            new_entries: List[Dict[str, Any]]
            if existing_entries and point_key in MULTI_SIGNAL_KEYS:
                action_raw = prompt_string(
                    stdscr,
                    "Append (A) or Replace (R) existing mappings?",
                    "A",
                )
                if action_raw is None:
                    status = "Selection cancelled"
                    continue
                action = action_raw.strip().lower()
                if action.startswith("r"):
                    new_entries = [entry]
                    action_word = "replaced"
                elif action == "" or action.startswith("a"):
                    new_entries = existing_entries + [entry]
                    action_word = "added"
                else:
                    status = "Unknown choice, mapping unchanged"
                    continue
            else:
                new_entries = [entry]
                action_word = "mapped"

            set_signal_entries(signals, point_key, new_entries)
            state.dirty = True
            status = f"{action_word.capitalize()} {label} -> {message_name}.{signal_name}"
        elif keypress in (ord("c"), ord("C")):
            point_key, label = SIGNAL_POINTS[idx]
            if get_signal_entries(signals, point_key) and prompt_confirm(stdscr, f"Clear mapping for {label}?", default_yes=False):
                set_signal_entries(signals, point_key, [])
                state.dirty = True
                status = f"Cleared mapping for {label}"
            else:
                status = "Clear cancelled"


def prompt_signal_overrides(
    stdscr: "curses._CursesWindow",
    existing: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    current = existing.copy()

    source_default: Optional[str] = None
    if "source" in current and current["source"] is not None:
        source_default = str(current["source"])
    source_raw = prompt_string(stdscr, "Bus number (blank for any)", source_default)
    if source_raw is None:
        return None
    source_val: Optional[int]
    if source_raw == "":
        source_val = None
    else:
        try:
            source_val = int(source_raw)
        except ValueError:
            source_val = None

    transform = current.get("transform", {})
    scale_default = transform.get("scale")
    offset_default = transform.get("offset")

    scale_val = prompt_number(stdscr, "Scale (blank to skip)", scale_default)
    offset_val = prompt_number(stdscr, "Offset (blank to skip)", offset_default)

    result: Dict[str, Any] = {}
    if source_val is not None:
        result["source"] = source_val
    if scale_val is not None or offset_val is not None:
        transform_cfg: Dict[str, Any] = {}
        if scale_val is not None:
            transform_cfg["scale"] = scale_val
        if offset_val is not None:
            transform_cfg["offset"] = offset_val
        if transform_cfg:
            result["transform"] = transform_cfg
    return result


def choose_dbc_file(
    stdscr: "curses._CursesWindow",
    current_path: Optional[str],
) -> Optional[str]:
    files = list_dbc_files()
    if not files:
        return prompt_string(stdscr, "DBC file path", current_path or "")

    selected = 0
    if current_path:
        current_name = os.path.basename(current_path)
        if current_name in files:
            selected = files.index(current_name)

    status = "Enter: select  T:type custom  B:back"
    start = 0

    while True:
        stdscr.erase()
        height, width = stdscr.getmaxyx()
        stdscr.bkgd(" ", curses.color_pair(1))

        header = " Select DBC File "
        stdscr.attron(curses.color_pair(3) | curses.A_BOLD)
        stdscr.addnstr(0, max(0, (width - len(header)) // 2), header, width - 2)
        stdscr.attroff(curses.color_pair(3) | curses.A_BOLD)

        stdscr.attron(curses.color_pair(3))
        stdscr.addnstr(1, 2, f"Directory: {DBC_DIR}", width - 4)
        stdscr.attroff(curses.color_pair(3))

        max_rows = max(1, height - 5)
        max_start = max(0, len(files) - max_rows)
        bottom_margin = 1
        top_margin = 1
        if selected > start + max_rows - 1 - bottom_margin:
            start = min(max_start, selected - (max_rows - 1 - bottom_margin))
        if selected < start + top_margin:
            start = max(0, selected - top_margin)
        start = max(0, min(start, max_start))

        for row, fname in enumerate(files[start : start + max_rows]):
            y = 3 + row
            attr = curses.A_BOLD if (start + row) == selected else curses.A_NORMAL
            color = curses.color_pair(2) if (start + row) == selected else curses.color_pair(1)
            stdscr.attrset(attr | color)
            stdscr.addnstr(y, 2, fname.ljust(width - 4), width - 4)

        stdscr.attrset(curses.color_pair(3))
        stdscr.addnstr(height - 2, 2, status.ljust(width - 4), width - 4)
        stdscr.refresh()

        key = stdscr.getch()
        if key in (ord("b"), ord("B"), 27):
            return current_path
        if key in (curses.KEY_UP, ord("k")):
            selected = (selected - 1) % len(files)
        elif key in (curses.KEY_DOWN, ord("j")):
            selected = (selected + 1) % len(files)
        elif key in (ord("t"), ord("T")):
            manual = prompt_string(stdscr, "DBC file path", current_path or "")
            return manual
        elif key in (curses.KEY_ENTER, 10, 13):
            return os.path.join(DBC_DIR, files[selected])


def choose_message_signal(
    stdscr: "curses._CursesWindow",
    dbc_path: str,
    existing: Dict[str, Any],
) -> Optional[Tuple[str, str]]:
    catalog = load_dbc_catalog(dbc_path)
    if catalog is None:
        return None
    if not catalog:
        return None

    selected = 0
    existing_message = existing.get("message") if existing else None
    existing_signal = existing.get("signal") if existing else None
    if existing_message and existing_signal:
        for idx, entry in enumerate(catalog):
            if entry["message"] == existing_message and entry["signal"] == existing_signal:
                selected = idx
                break

    start = 0
    status = "Enter: choose  T:type manually  B:back"

    while True:
        stdscr.erase()
        height, width = stdscr.getmaxyx()
        stdscr.bkgd(" ", curses.color_pair(1))

        header = " Select Signal "
        stdscr.attron(curses.color_pair(3) | curses.A_BOLD)
        stdscr.addnstr(0, max(0, (width - len(header)) // 2), header, width - 2)
        stdscr.attroff(curses.color_pair(3) | curses.A_BOLD)

        stdscr.attron(curses.color_pair(3))
        stdscr.addnstr(1, 2, f"DBC: {dbc_path}", width - 4)
        stdscr.attroff(curses.color_pair(3))

        max_rows = max(1, height - 6)
        max_start = max(0, len(catalog) - max_rows)
        bottom_margin = 1
        top_margin = 1
        if selected > start + max_rows - 1 - bottom_margin:
            start = min(max_start, selected - (max_rows - 1 - bottom_margin))
        if selected < start + top_margin:
            start = max(0, selected - top_margin)
        start = max(0, min(start, max_start))

        for row, entry in enumerate(catalog[start : start + max_rows]):
            y = 3 + row
            attr = curses.A_BOLD if (start + row) == selected else curses.A_NORMAL
            color = curses.color_pair(2) if (start + row) == selected else curses.color_pair(1)
            stdscr.attrset(attr | color)
            label = f"{entry['message']}.{entry['signal']}"
            if entry.get("units"):
                label += f" [{entry['units']}]"
            stdscr.addnstr(y, 2, label.ljust(width - 4), width - 4)

        detail_entry = catalog[selected] if catalog else None
        detail_y = 3 + max_rows
        if detail_entry and detail_y < height - 2:
            stdscr.attrset(curses.color_pair(3))
            info = f"Message ID: 0x{detail_entry['frame_id']:03X}"
            stdscr.addnstr(detail_y, 2, info.ljust(width - 4), width - 4)
            comment = detail_entry.get("comment") or ""
            if comment and detail_y + 1 < height - 2:
                stdscr.addnstr(detail_y + 1, 2, comment[: width - 4], width - 4)

        stdscr.attrset(curses.color_pair(3))
        stdscr.addnstr(height - 2, 2, status.ljust(width - 4), width - 4)
        stdscr.refresh()

        key = stdscr.getch()
        if key in (ord("b"), ord("B"), 27):
            return None
        if key in (curses.KEY_UP, ord("k")):
            selected = (selected - 1) % len(catalog)
        elif key in (curses.KEY_DOWN, ord("j")):
            selected = (selected + 1) % len(catalog)
        elif key in (ord("t"), ord("T")):
            manual_msg = prompt_string(stdscr, "Message name", existing_message or "")
            if manual_msg is None:
                continue
            manual_sig = prompt_string(stdscr, "Signal name", existing_signal or "")
            if manual_sig is None:
                continue
            return (manual_msg, manual_sig)
        elif key in (curses.KEY_ENTER, 10, 13):
            entry = catalog[selected]
            return (entry["message"], entry["signal"])


def main(stdscr: "curses._CursesWindow") -> None:
    init_colors()
    curses.curs_set(0)
    stdscr.nodelay(False)
    stdscr.keypad(True)

    config = load_config(CONFIG_PATH)
    profile_order = list(config.get("profiles", {}).keys())
    state = AppState(config=config, profile_order=profile_order)
    state.ensure_selection()

    while True:
        draw_main(stdscr, state)
        key = stdscr.getch()

        if key in (ord("q"), ord("Q")):
            if state.dirty and state.profile_order:
                if prompt_confirm(stdscr, "Save changes before quitting?", default_yes=False):
                    save_config(CONFIG_PATH, state)
                    state.dirty = False
                else:
                    if not prompt_confirm(stdscr, "Discard unsaved changes?", default_yes=False):
                        state.status = "Quit cancelled"
                        continue
            break

        elif key == curses.KEY_RESIZE:
            state.status = ""

        elif key in (curses.KEY_UP, ord("k")):
            if state.profile_order:
                state.selected_index = (state.selected_index - 1) % len(state.profile_order)

        elif key in (curses.KEY_DOWN, ord("j")):
            if state.profile_order:
                state.selected_index = (state.selected_index + 1) % len(state.profile_order)

        elif key in (ord("a"), ord("A")):
            add_profile(stdscr, state)

        elif key in (ord("d"), ord("D")):
            delete_profile(stdscr, state)

        elif key in (ord("o"), ord("O")):
            edit_profile_details(stdscr, state)

        elif key in (ord("s"), ord("S")):
            set_default_profile(state)

        elif key in (ord("w"), ord("W")):
            save_config(CONFIG_PATH, state)
            state.dirty = False
            state.status = f"Configuration written to {CONFIG_PATH}"

        elif key in (ord("e"), ord("E")):
            edit_signals(stdscr, state)

        elif key in (ord("?"), ord("h"), ord("H")):
            state.status = "A:add  O:details  E:signals  W:write  Q:quit"

        else:
            key_name = curses.keyname(key).decode("utf-8", errors="replace") if key != -1 else "?"
            if key_name.startswith("^") and len(key_name) == 2:
                key_desc = f"Ctrl+{key_name[1]}"
            elif key_name.startswith("KEY_"):
                key_desc = key_name
            elif len(key_name) == 1 and 32 <= ord(key_name) < 127:
                key_desc = f"'{key_name}'"
            else:
                key_desc = key_name
            state.status = f"Unhandled key {key_desc} (code {key}, press ? for help)"

        state.ensure_selection()


if __name__ == "__main__":
    curses.wrapper(main)
