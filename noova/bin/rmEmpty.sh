#!/bin/bash

# __CLEAR__ empty file or dir

# Define the target directory
TARGET_DIR="sample/ver3" # __UPDATE__ as needed

# Check if the target directory exists
if [ ! -d "$TARGET_DIR" ]; then
    echo "Error: Directory '$TARGET_DIR' does not exist."
    exit 1
fi

# Remove empty files
echo "Removing empty files from '$TARGET_DIR'..."
find "$TARGET_DIR" -type f -empty -exec rm -v {} \;

# Remove empty directories
echo "Removing empty directories from '$TARGET_DIR'..."
find "$TARGET_DIR" -type d -empty -exec rmdir -v {} \;

echo "Cleanup complete. Empty files and directories have been removed from '$TARGET_DIR'."