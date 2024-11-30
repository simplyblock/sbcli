#!/bin/bash

# Initialize sbcli command
sbcli_cmd="sbcli-mock"
timeout_duration=900  # 15 minutes in seconds
pool_name="pool1"

# Variables
batch_size=25
total_batches=100

# Log files
lvol_create_log="lvol_create_times.log"
lvol_list_log="lvol_list_times.log"
sn_list_log="sn_list_status.log"

# Clear log files
> $lvol_create_log
> $lvol_list_log
> $sn_list_log

# Helper function to create an lvol and measure response time
create_lvol() {
    start_time=$(date +%s%3N)  # Start time in milliseconds
    timeout ${timeout_duration}s ${sbcli_cmd} lvol add test_lvol_$(($1)) 200M ${pool_name}
    if [[ $? -ne 0 ]]; then
        echo "System has become stuck during lvol creation. Exiting..."
        exit 1
    fi
    end_time=$(date +%s%3N)    # End time in milliseconds
    response_time=$(($end_time - $start_time))
    echo "lvol create time: ${response_time} ms" | tee -a $lvol_create_log
    echo $response_time
}

# Function to check sn list and node status
check_sn_list() {
    echo "Checking sn list for node status..."
    sn_list_start_time=$(date +%s%3N)
    timeout ${timeout_duration}s ${sbcli_cmd} sn list > sn_list_output.log
    if [[ $? -ne 0 ]]; then
        echo "System has become stuck during sn list. Exiting..."
        exit 1
    fi
    sn_list_end_time=$(date +%s%3N)
    sn_list_response_time=$(($sn_list_end_time - $sn_list_start_time))
    echo "sn list time: ${sn_list_response_time} ms" | tee -a $sn_list_log

    # Check for offline nodes
    offline_nodes=$(grep -c "offline" sn_list_output.log)
    if [[ $offline_nodes -ne 0 ]]; then
        echo "Warning: $offline_nodes node(s) are offline." | tee -a $sn_list_log
    else
        echo "All nodes are online." | tee -a $sn_list_log
    fi
}

# Function to check lvol list and validate
check_lvol_list() {
    list_start_time=$(date +%s%3N)
    timeout ${timeout_duration}s ${sbcli_cmd} lvol list > lvol_list_output.log
    if [[ $? -ne 0 ]]; then
        echo "System has become stuck during lvol list. Exiting..."
        exit 1
    fi
    list_end_time=$(date +%s%3N)
    list_response_time=$(($list_end_time - $list_start_time))
    lvol_count=$(grep -c "test_lvol_" lvol_list_output.log)

    # Log lvol list response time and count
    echo "lvol list time: ${list_response_time} ms, lvol count: ${lvol_count}" | tee -a $lvol_list_log

    # Validate lvol count
    if [[ $lvol_count -lt $1 ]]; then
        echo "Validation failed. Expected lvol count: $1, but found $lvol_count. Exiting..."
        exit 1
    fi
    echo "Validation successful."
}

# Main loop to create lvols
for ((batch=1; batch<=total_batches; batch++))
do
    echo "Creating batch $batch of $batch_size lvols..."

    for ((i=1; i<=batch_size; i++))
    do
        lvol_num=$(( (batch - 1) * batch_size + i ))

        # Create lvol and measure response time
        create_lvol $lvol_num

        # Perform lvol list and validate the count
        check_lvol_list $lvol_num

        # Check sn list and node status
        check_sn_list
    done

    # Wait for a random duration between 120 to 200 seconds
    wait_time=$((RANDOM % 81 + 120))
    echo "Waiting for ${wait_time} seconds before creating the next lvol..."
    sleep ${wait_time}

    # Check system responsiveness
    if ! ps -p $$ > /dev/null; then
        echo "System has become unresponsive. Exiting..."
        exit 1
    fi
done

echo "Script completed successfully or reached the maximum number of batches."
