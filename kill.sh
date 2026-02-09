#!/bin/bash

# Ports to clean up
PORTS=("8000" "8001" "8002" "8003" "8004" "8005" "8006" "8007" "8008" "8009" "8010" "8011" "8012" "8013" "8014" "8015")

for PORT in "${PORTS[@]}"; do
    # Find processes using the port
    PIDS=$(lsof -t -i tcp:${PORT})
    if [ -n "$PIDS" ]; then
        echo "Killing processes on port $PORT: $PIDS"
        kill -9 $PIDS
    fi
done

echo "Done."
