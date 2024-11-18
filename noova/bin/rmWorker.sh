#!/bin/bash

# __CLEAR__ files in all-jars/worker# for debugging Crawler

# Navigate to the all-jars directory
cd all-jars || { echo "Directory 'all-jars' does not exist"; exit 1; }

# Find directories that start with 'worker' followed by any number
for dir in worker[0-9]*; do
  # Check if it's a directory before trying to clear it
  if [ -d "$dir" ]; then
    echo "Clearing contents of $dir"
    rm -rf "$dir"/*
  else
    echo "No worker directories found matching pattern 'worker[0-9]*'"
    break
  fi
done

echo "Worker directories have been cleared."