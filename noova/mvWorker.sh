#!/bin/bash

# __MOVE__ crawled sample from all-jars/worker* to sample/ver*
# note: update the version

# Define source and destination directories
SOURCE_DIR="all-jars"
DEST_DIR="sample/ver3" # __UPDATE__ if needed

# Check if the source and destination directories exist
if [ ! -d "$SOURCE_DIR" ]; then
    echo "Error: Source directory '$SOURCE_DIR' does not exist."
    exit 1
fi

if [ ! -d "$DEST_DIR" ]; then
    echo "Error: Destination directory '$DEST_DIR' does not exist."
    exit 1
fi

# Iterate over worker* directories in the source directory
for worker_dir in "$SOURCE_DIR"/worker*; do
    if [ -d "$worker_dir" ]; then
        echo "Processing contents from '$worker_dir'..."

        # Iterate over subdirectories in the worker directory
        for sub_dir in "$worker_dir"/*; do
            if [ -d "$sub_dir" ]; then
                sub_dir_name=$(basename "$sub_dir")
                dest_sub_dir="$DEST_DIR/$sub_dir_name"

                # Create the subdirectory in the destination if it doesn't exist
                mkdir -p "$dest_sub_dir"

                # Move or copy contents to the destination subdirectory
                echo "Combining contents from '$sub_dir' to '$dest_sub_dir'..."
                mv "$sub_dir"/* "$dest_sub_dir"/ 2>/dev/null

                # Handle empty directories or errors
                if [ $? -eq 0 ]; then
                    echo "Contents from '$sub_dir' combined into '$dest_sub_dir'."
                else
                    echo "No files to move from '$sub_dir' or an error occurred."
                fi
            fi
        done
    else
        echo "No worker directories found in '$SOURCE_DIR'."
    fi
done

echo "All workers processed and combined into '$DEST_DIR'."