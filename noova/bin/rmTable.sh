#!/bin/bash

# __CLEAR__ files in all-jars/worker*/pt-*
# e.g. command to remove pt-crawl:
# ./bin/rmTable crawl

# Check if an argument is provided
if [ $# -ne 1 ]; then
  echo "Usage: ./bin/rmTable <table_name>"
  echo "Example: ./bin/rmTable crawl"
  exit 1
fi

# Get the table name from the argument
TABLE_NAME="$1"

# Define the base directory
BASE_DIR="all-jars"

# Check if the base directory exists
if [ ! -d "$BASE_DIR" ]; then
  echo "Error: Directory '$BASE_DIR' does not exist."
  exit 1
fi

# Iterate over all worker directories
for worker_dir in "$BASE_DIR"/worker*; do
  # Check if the worker directory exists and is a directory
  if [ -d "$worker_dir" ]; then
    table_dir="$worker_dir/pt-$TABLE_NAME"
    # Check if the specific table directory exists and remove it
    if [ -d "$table_dir" ]; then
      echo "Removing $table_dir"
      rm -rf "$table_dir"
    else
      echo "No pt-$TABLE_NAME directory found in $worker_dir"
    fi
  fi
done

echo "Completed removing all pt-$TABLE_NAME directories."
