#!/bin/bash

# Directories to compare
dir1="optd_perfbench_workspace/tpch/genned_queries/dbPOSTGRESQL_sf1_sd15721"
dir2="optd_perfbench_workspace/tpch/genned_queries/dbPOSTGRESQL_sf0.01_sd15721"

# Loop through the file numbers
for i in {1..22}; do
  file1="${dir1}/${i}.sql"
  file2="${dir2}/${i}.sql"

  # Check if both files exist
  if [[ -f "$file1" && -f "$file2" ]]; then
    # Use diff to compare files and report differences
    diff_output=$(diff "$file1" "$file2")
    if [ -n "$diff_output" ]; then
      echo "Difference found in file ${i}.sql"
    else
      echo "No differences in file ${i}.sql"
    fi
  else
    echo "File ${i}.sql does not exist in one of the directories."
  fi
done
