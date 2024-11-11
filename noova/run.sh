#!/bin/bash

OUTPUT_DIR="all-jars"
mkdir -p "$OUTPUT_DIR"
echo "Collecting JAR files from submodules..."
for jar in $(find . -name "*.jar" -path "*/target/*" -type f); do
    echo "Copying $jar to $OUTPUT_DIR"
    cp "$jar" "$OUTPUT_DIR"
done

echo "All JAR files have been copied to $OUTPUT_DIR"

# only copy so far, run process TBD