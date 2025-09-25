#!/bin/bash

# Get the path to the subdirectory where PlotJuggler lives
PLOTJUGGLER_DIR="/home/goran/OPstuff/PlotJuggler_v3.6.1_video_playback/PlotJuggler"

# Log file path passed as an argument
LOG_FILE="$1"

# Enable X11 forwarding for Docker
xhost +local:docker > /dev/null 2>&1

# Determine the correct log file path inside the container
if [[ "$LOG_FILE" == /* ]]; then
  # Absolute path from --video flag, map it to the Docker volume.
  DOCKER_LOG_FILE="/tmp/dowloaded_realdata/${LOG_FILE##*downloaded_folders/}"
else
  # Relative path from the project, use the rlogs volume
  DOCKER_LOG_FILE="/tmp/rlogs/${LOG_FILE#./rlogs/}"
fi

# Get existing PlotJuggler window IDs before launching a new one
EXISTING_WINDOWS=$(wmctrl -l -x | grep "plotjuggler.PlotJuggler" | awk '{print $1}' | sort)

# Run the Docker container in the background
docker run --rm --privileged \
  --volume "$PLOTJUGGLER_DIR:/tmp/plotjuggler" \
  --volume "$HOME/OPstuff/realdata/:/tmp/realdata/" \
  --volume "$HOME/OPstuff/Retropilot_server_tools/realdata/:/tmp/realdata_another/" \
  --volume "$HOME/OPstuff/realdata/downloaded_folders/:/tmp/dowloaded_realdata/" \
  --volume "$HOME/OPstuff/test_tools/engament_gauge_dev/rlogs:/tmp/rlogs" \
  --volume /tmp/.X11-unix:/tmp/.X11-unix \
  --env DISPLAY=:0 \
  --env DBC_NAME=hyundai_i30_2014 \
  --workdir /tmp/plotjuggler \
  plotjuggler_video_playback:latest ./build/bin/plotjuggler --layout ./3rdparty/debug_intervention.xml -d "$DOCKER_LOG_FILE" & 
DOCKER_PID=$!

# Wait for 5 seconds before attempting to maximize
if command -v wmctrl &> /dev/null; then
    echo "Waiting 8 seconds for PlotJuggler to initialize..."
    sleep 8

    # Find the new window that has appeared
    CURRENT_WINDOWS=$(wmctrl -l -x | grep "plotjuggler.PlotJuggler" | awk '{print $1}' | sort)
    NEW_WINDOW_ID=$(comm -13 <(echo "$EXISTING_WINDOWS") <(echo "$CURRENT_WINDOWS") | tail -n 1)

    if [ -n "$NEW_WINDOW_ID" ]; then
        echo "New PlotJuggler window found ($NEW_WINDOW_ID). Maximizing."
        wmctrl -i -r "$NEW_WINDOW_ID" -b add,maximized_vert,maximized_horz
    else
        echo "Warning: Could not find a new PlotJuggler window to maximize after 5 seconds."
    fi
else
    echo "Warning: wmctrl not found. Cannot make PlotJuggler fullscreen."
    echo "Please install it with: sudo apt-get install wmctrl"
fi

# Wait for the Docker process to finish
wait $DOCKER_PID
