#!/bin/bash

set -e

# Variables
SBCLI_CMD="sbcli"
SLEEP_INTEVAL=30


# Helper functions
log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') - $1"
}

SN_IDS=($(${SBCLI_CMD} sn list | grep -Eo '^[|][ ]+[a-f0-9-]+[ ]+[|]' | awk '{print $2}'))
# Print the header
echo "Timestamp           | Total       | HugeTotal  | Free 1      | HugeFree 1 | Free 2      | HugeFree 2 | Free 3      | HugeFree 3"

# Start the infinite loop
while true; do
    timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    row=""  # Initialize row with the timestamp

    total=""
    huge_total=""
    for host_id in "${SN_IDS[@]}"; do
        # Get memory details for the current host
        mem_info=$(${SBCLI_CMD} sn info $host_id | jq '.memory_details')

        # Extract the values from the JSON response
        free=$(echo "$mem_info" | jq '.free')
        huge_free=$(echo "$mem_info" | jq '.huge_free')

        # Extract total and huge_total from the first node only
        if [ -z "$total" ]; then
            total=$(echo "$mem_info" | jq '.total')
            huge_total=$(echo "$mem_info" | jq '.huge_total')
        fi

        # Append free and huge_free values to the row
        row="$row | $free | $huge_free"
    done

    # Insert total and huge_total once after the timestamp
    row="$timestamp | $total | $huge_total$row"

    # Print the formatted row
    echo "$row"

    sleep $SLEEP_INTEVAL
done

log "FIO is started"

