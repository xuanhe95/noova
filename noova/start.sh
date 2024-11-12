#!/bin/bash

# Run Maven clean package
echo "Running 'mvn clean package'..."
mvn clean package

# Run rmPort.sh script
echo "Running './rmPort.sh'..."
./rmPort.sh

# Parameter settings: Receive the number of different types of Workers
KVS_WORKER_COUNT=$1            # Receive the number of KVSWorkers from command-line arguments
FLAME_WORKER_COUNT=$2          # Receive the number of FlameWorkers from command-line arguments

# Check if the provided arguments are valid numbers
if ! [[ "$KVS_WORKER_COUNT" =~ ^[0-9]+$ ]] || ! [[ "$FLAME_WORKER_COUNT" =~ ^[0-9]+$ ]]; then
  echo "Please provide valid numbers for KVSWorker and FlameWorker counts. For example: ./script.sh 5 3"
  exit 1
fi

# Change to the all-jars directory
cd all-jars || { echo "Directory 'all-jars' does not exist"; exit 1; }

echo "Current directory is: $(pwd)"

# Initialize the pids array
pids=()

# Capture Ctrl+C (SIGINT) signal and clean up processes
cleanup() {
  echo "Exit signal detected, terminating all processes..."

  # Attempt to gracefully terminate all processes
  kill "${pids[@]}" 2>/dev/null
  sleep 1  # Wait for processes to release resources

  # Forcefully terminate any processes that did not exit gracefully
  kill -9 "${pids[@]}" 2>/dev/null
  echo "All processes have been terminated"

  sleep 1
  # Check and release all ports
  check_and_release_port 8000  # Base port for KVSCoordinator
  check_and_release_port 9000  # Base port for FlameCoordinator

  for ((i=1; i<=KVS_WORKER_COUNT; i++)); do
    check_and_release_port $((8000 + i))
  done

  for ((i=1; i<=FLAME_WORKER_COUNT; i++)); do
    check_and_release_port $((9000 + i))
  done

  exit 0
}

# Define a function to release ports using lsof and kill
check_and_release_port() {
  local port=$1
  pid=$(lsof -t -i :$port)
  if [[ -n $pid ]]; then
    echo "Port $port is occupied by PID $pid, releasing..."
    kill -9 $pid
  else
    echo "Port $port is already released or not occupied"
  fi
}

# Bind the cleanup function to the SIGINT (Ctrl+C) signal
trap cleanup SIGINT

# Start the KVSCoordinator process
command1="java -cp \"kvs-1.0-SNAPSHOT.jar:generic-1.0-SNAPSHOT.jar:tools-1.0-SNAPSHOT.jar:webserver-1.0-SNAPSHOT.jar\" org.noova.kvs.Coordinator 8000"
echo "Starting KVSCoordinator process (8000)"
eval "$command1" &
pid=$!
pids+=("$pid")  # Save the PID of the KVSCoordinator
echo "KVSCoordinator process PID: $pid"
sleep 1

# Start the FlameCoordinator process
command2="java -cp \"kvs-1.0-SNAPSHOT.jar:generic-1.0-SNAPSHOT.jar:tools-1.0-SNAPSHOT.jar:webserver-1.0-SNAPSHOT.jar:flame-1.0-SNAPSHOT.jar\" org.noova.flame.Coordinator 9000 localhost:8000"
echo "Starting FlameCoordinator process (9000)"
eval "$command2" &
pid=$!
pids+=("$pid")  # Save the PID of the FlameCoordinator
echo "FlameCoordinator process PID: $pid"
sleep 1

# Start the specified number of KVSWorker processes
for ((i=1; i<=KVS_WORKER_COUNT; i++)); do
  port=$((8000 + i))
  command="java -cp \"kvs-1.0-SNAPSHOT.jar:generic-1.0-SNAPSHOT.jar:tools-1.0-SNAPSHOT.jar:webserver-1.0-SNAPSHOT.jar\" org.noova.kvs.Worker $port \"worker$i\" localhost:8000"
  echo "Starting KVSWorker process (port $port)"
  eval "$command" &
  pid=$!
  pids+=("$pid")  # Save the PID of each KVSWorker
  echo "KVSWorker $i process PID: $pid"
  sleep 0.5  # Sleep for 0.5 seconds
done

# Start the specified number of FlameWorker processes
for ((i=1; i<=FLAME_WORKER_COUNT; i++)); do
  port=$((9000 + i))
  command="java -cp \"kvs-1.0-SNAPSHOT.jar:generic-1.0-SNAPSHOT.jar:tools-1.0-SNAPSHOT.jar:webserver-1.0-SNAPSHOT.jar:flame-1.0-SNAPSHOT.jar\" org.noova.flame.Worker $port localhost:9000"
  echo "Starting FlameWorker process (port $port)"
  eval "$command" &
  pid=$!
  pids+=("$pid")  # Save the PID of each FlameWorker
  echo "FlameWorker $i process PID: $pid"
  sleep 0.5  # Sleep for 0.5 seconds
done

command_web="java -cp \"tools-1.0-SNAPSHOT.jar\" -jar webserver-1.0-SNAPSHOT.jar"
echo "Starting WebServer process"
eval "$command_web" &
pid=$!
pids+=("$pid")  # Save the PID of the WebServer
echo "WebServer process PID: $pid"
sleep 1

# Wait for all processes to complete
for pid in "${pids[@]}"; do
  wait $pid
  echo "Process $pid has completed"
done

echo "All processes have completed"
