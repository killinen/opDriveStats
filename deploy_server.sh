#!/usr/bin/env bash
# Sync the local server/ directory to a remote deployment target without using git.
set -euo pipefail

usage() {
    cat <<'USAGE'
Usage: deploy_server.sh [-s host] [-u user] [-r remote_dir] [-l local_dir] [options]

Options:
  -s  SSH host alias or hostname (required unless DEPLOY_SERVER_HOST is set)
  -u  SSH user (default: use SSH config)
  -r  Remote directory to sync into (default: ~/opDriveStats/server)
  -l  Local source directory (default: server)
  -R  Stop and restart the remote app before/after deployment
  -S  Remote start command (default: ~/start_opdrivestats.sh)
  -T  Remote tmux session name for restart management (default: opdrivestats)
  -C  Remote stop command (default: pkill -f "uvicorn server.app:app" || true)

Environment variables:
  DEPLOY_SERVER_HOST   Default host if -s not provided.
  DEPLOY_SERVER_USER   Default user if -u not provided.
  DEPLOY_SERVER_REMOTE Default remote directory if -r not provided.
  DEPLOY_SERVER_LOCAL  Default local directory if -l not provided.
  DEPLOY_SERVER_RESTART     Set to 1 to enable restart flow without -R
  DEPLOY_SERVER_START_CMD   Override the remote start command.
  DEPLOY_SERVER_TMUX_SESSION Override the tmux session name.
  DEPLOY_SERVER_STOP_CMD    Override the remote stop command.

Notes:
  - Uses rsync when available on the remote host, otherwise falls back to scp.
  - Creates the remote directory if it does not exist.
  - Excludes local __pycache__, *.pyc, Git metadata, and .venv directories.
USAGE
}

REMOTE_HOST=${DEPLOY_SERVER_HOST:-}
REMOTE_USER=${DEPLOY_SERVER_USER:-}
REMOTE_DIR=${DEPLOY_SERVER_REMOTE:-"~/opDriveStats/server"}
LOCAL_DIR=${DEPLOY_SERVER_LOCAL:-server}
RESTART_FLAG=${DEPLOY_SERVER_RESTART:-0}
START_CMD=${DEPLOY_SERVER_START_CMD:-"~/start_opdrivestats.sh"}
TMUX_SESSION=${DEPLOY_SERVER_TMUX_SESSION:-opdrivestats}
STOP_CMD=${DEPLOY_SERVER_STOP_CMD:-"pkill -f uvicorn || true"}

while getopts ':s:u:r:l:RS:T:C:h' opt; do
    case "$opt" in
        s) REMOTE_HOST=$OPTARG ;;
        u) REMOTE_USER=$OPTARG ;;
        r) REMOTE_DIR=$OPTARG ;;
        l) LOCAL_DIR=$OPTARG ;;
        R) RESTART_FLAG=1 ;;
        S) START_CMD=$OPTARG ;;
        T) TMUX_SESSION=$OPTARG ;;
        C) STOP_CMD=$OPTARG ;;
        h)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: -$OPTARG" >&2
            usage
            exit 1
            ;;
    esac
done

shift $((OPTIND - 1))

if [[ -z $REMOTE_HOST ]]; then
    echo 'Error: remote host not specified. Use -s or set DEPLOY_SERVER_HOST.' >&2
    usage
    exit 1
fi

if [[ ! -d $LOCAL_DIR ]]; then
    echo "Error: local directory '$LOCAL_DIR' not found." >&2
    exit 1
fi

if [[ -z $REMOTE_USER ]]; then
    if command -v ssh >/dev/null 2>&1; then
        REMOTE_USER=$(ssh -G "$REMOTE_HOST" 2>/dev/null | awk 'tolower($1)=="user" {print $2; exit}') || true
    fi
fi

if [[ -n $REMOTE_USER ]]; then
    REMOTE_TARGET="${REMOTE_USER}@${REMOTE_HOST}"
else
    REMOTE_TARGET="${REMOTE_HOST}"
fi

REMOTE_DIR_TMP="${REMOTE_DIR%.}/"
if [[ $REMOTE_DIR_TMP == '/' ]]; then
    echo 'Refusing to deploy to root directory.' >&2
    exit 1
fi

quote_remote() {
    local value=$1
    if [[ $value == ~/* ]]; then
        local suffix=${value#~/}
        suffix=${suffix//"/\\"}
        printf '"$HOME/%s"' "$suffix"
    elif [[ $value == ~ ]]; then
        printf '"$HOME"'
    else
        printf '%q' "$value"
    fi
}

REMOTE_DIR_QUOTED=$(quote_remote "$REMOTE_DIR")
REMOTE_DIR_RSYNC="${REMOTE_DIR%/}/"

printf 'Deploying %s -> %s:%s\n' "$LOCAL_DIR" "$REMOTE_TARGET" "$REMOTE_DIR"

ssh "$REMOTE_TARGET" "mkdir -p $REMOTE_DIR_QUOTED"

RSYNC_EXCLUDES=(
    "--exclude=.git"
    "--exclude=.venv"
    "--exclude=__pycache__"
    "--exclude=*.pyc"
)

if [[ $RESTART_FLAG == 1 ]]; then
    echo 'Stopping remote application...'
    ssh "$REMOTE_TARGET" "$(printf 'TMUX_SESSION=%q STOP_CMD=%q bash -s' "$TMUX_SESSION" "$STOP_CMD")" <<'EOF'
set -e
if command -v tmux >/dev/null 2>&1 && tmux has-session -t "$TMUX_SESSION" 2>/dev/null; then
  tmux send-keys -t "$TMUX_SESSION" C-c
  sleep 1
  tmux kill-session -t "$TMUX_SESSION" || true
fi
if [[ -n "$STOP_CMD" ]]; then
  eval "$STOP_CMD"
fi
EOF
fi

if ssh "$REMOTE_TARGET" 'command -v rsync >/dev/null 2>&1'; then
    rsync -av --delete "${RSYNC_EXCLUDES[@]}" "$LOCAL_DIR/" "${REMOTE_TARGET}:${REMOTE_DIR_RSYNC}"
else
    printf 'Remote rsync not found. Falling back to tar stream.\n'
    ssh "$REMOTE_TARGET" "rm -rf $REMOTE_DIR_QUOTED"
    ssh "$REMOTE_TARGET" "mkdir -p $REMOTE_DIR_QUOTED"
    tar --exclude='.git' --exclude='.venv' --exclude='__pycache__' --exclude='*.pyc' -C "$LOCAL_DIR" -cf - . \
        | ssh "$REMOTE_TARGET" "tar -xf - -C $REMOTE_DIR_QUOTED"
fi

if [[ $RESTART_FLAG == 1 ]]; then
    echo 'Starting remote application...'
    ssh "$REMOTE_TARGET" "$(printf 'TMUX_SESSION=%q START_CMD=%q bash -s' "$TMUX_SESSION" "$START_CMD")" <<'EOF'
set -e
if command -v tmux >/dev/null 2>&1; then
  tmux kill-session -t "$TMUX_SESSION" 2>/dev/null || true
  tmux new-session -d -s "$TMUX_SESSION" "bash -lc \"$START_CMD\""
else
  nohup bash -lc "$START_CMD" >/dev/null 2>&1 &
fi
EOF
fi

printf 'Deployment complete. Remote directory: %s:%s\n' "$REMOTE_TARGET" "$REMOTE_DIR"
