#!/usr/bin/env bash
# Upload the generated engagement_db.json to a remote server atomically.
set -euo pipefail

usage() {
    cat <<'USAGE'
Usage: sync_stats.sh [-f local_db] [-p remote_path] [-u user] [-s host]

Options:
  -f  Path to the local engagement DB JSON (default: engagement_db.json)
  -p  Remote absolute path for the DB file (default: /home/oracle/engagement_gauge_dev/engagement_db.json)
  -u  Remote SSH user (default: oracle)
  -s  Remote SSH host (required unless SYNC_STATS_HOST env var set)

Environment variables:
  SYNC_STATS_HOST   Default host if -s is not provided.
  SYNC_STATS_DB     Default local file if -f is not provided.
  SYNC_STATS_PATH   Default remote path if -p is not provided.
  SYNC_STATS_USER   Default remote user if -u is not provided.

Example:
  ./sync_stats.sh -s vps.example.com
  ./sync_stats.sh -s 1.2.3.4 -f ./build/engagement_db.json -p /srv/engagement_db.json -u ubuntu
USAGE
}

DEFAULT_REMOTE_PATH='~/engagement_gauge_dev/engagement_db.json'

LOCAL_DB=${SYNC_STATS_DB:-engagement_db.json}
REMOTE_PATH=${SYNC_STATS_PATH:-$DEFAULT_REMOTE_PATH}
REMOTE_USER=${SYNC_STATS_USER:-}
REMOTE_HOST=${SYNC_STATS_HOST:-}

while getopts ':f:p:u:s:h' opt; do
    case "$opt" in
        f) LOCAL_DB=$OPTARG ;;
        p) REMOTE_PATH=$OPTARG ;;
        u) REMOTE_USER=$OPTARG ;;
        s) REMOTE_HOST=$OPTARG ;;
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
    echo 'Error: remote host not specified. Use -s or set SYNC_STATS_HOST.' >&2
    usage
    exit 1
fi

if [[ ! -f $LOCAL_DB ]]; then
    echo "Error: local stats file '$LOCAL_DB' not found." >&2
    exit 1
fi

if [[ -n $REMOTE_USER ]]; then
    REMOTE_TARGET="${REMOTE_USER}@${REMOTE_HOST}"
else
    REMOTE_TARGET="${REMOTE_HOST}"
fi
REMOTE_TMP="${REMOTE_PATH}.new.$$"

quote_remote_arg() {
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

printf 'Syncing %s to %s:%s\n' "$LOCAL_DB" "$REMOTE_TARGET" "$REMOTE_PATH"

REMOTE_DIR=${REMOTE_PATH%/*}
if [[ -n $REMOTE_DIR && $REMOTE_DIR != "$REMOTE_PATH" ]]; then
    remote_dir_quoted=$(quote_remote_arg "$REMOTE_DIR")
    ssh "$REMOTE_TARGET" "mkdir -p $remote_dir_quoted"
fi

if ssh "$REMOTE_TARGET" 'command -v rsync >/dev/null 2>&1'; then
    rsync -av --info=progress2 "$LOCAL_DB" "${REMOTE_TARGET}:${REMOTE_TMP}"
else
    printf 'Remote rsync not found. Falling back to scp.\n'
    scp -p "$LOCAL_DB" "${REMOTE_TARGET}:${REMOTE_TMP}"
fi

remote_tmp_quoted=$(quote_remote_arg "$REMOTE_TMP")
remote_path_quoted=$(quote_remote_arg "$REMOTE_PATH")

ssh "$REMOTE_TARGET" "mv -f $remote_tmp_quoted $remote_path_quoted"

printf 'Upload complete. Remote file updated at %s:%s\n' "$REMOTE_TARGET" "$REMOTE_PATH"
