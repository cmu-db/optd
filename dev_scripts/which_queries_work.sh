#!/bin/bash
benchmark_name=$1
USAGE="Usage: $0 [job|joblight|tpch]"

if [ $# -ne 1 ]; then
    echo >&2 $USAGE
    exit 1
fi

if [[ "$benchmark_name" == "job" ]]; then
    all_ids="1a,1b,1c,1d,2a,2b,2c,2d,3a,3b,3c,4a,4b,4c,5a,5b,5c,6a,6b,6c,6d,6e,6f,7a,7b,7c,8a,8b,8c,8d,9a,9b,9c,9d,10a,10b,10c,11a,11b,11c,11d,12a,12b,12c,13a,13b,13c,13d,14a,14b,14c,15a,15b,15c,15d,16a,16b,16c,16d,17a,17b,17c,17d,17e,17f,18a,18b,18c,19a,19b,19c,19d,20a,20b,20c,21a,21b,21c,22a,22b,22c,22d,23a,23b,23c,24a,24b,25a,25b,25c,26a,26b,26c,27a,27b,27c,28a,28b,28c,29a,29b,29c,30a,30b,30c,31a,31b,31c,32a,32b,33a,33b,33c"
    vec_var_name="WORKING_JOB_QUERY_IDS"
elif [[ "$benchmark_name" == "joblight" ]]; then
    all_ids="1a,1b,1c,1d,2a,3a,3b,3c,4a,4b,4c,5a,5b,5c,6a,6b,6c,6d,6e,7a,7b,7c,8a,8b,8c,9a,9b,10a,10b,10c,11a,11b,11c,12a,12b,12c,13a,14a,14b,14c,15a,15b,15c,16a,17a,17b,17c,18a,18b,18c,19a,19b,20a,20b,20c,21a,21b,22a,22b,22c,23a,23b,24a,24b,25a,26a,26b,27a,27b,28a"
    vec_var_name="WORKING_JOBLIGHT_QUERY_IDS"
elif [[ "$benchmark_name" == "tpch" ]]; then
    all_ids="1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22"
    vec_var_name="WORKING_QUERY_IDS"
else
    echo >&2 $USAGE
    exit 1
fi

successful_ids=()
IFS=','
for id in $all_ids; do
    cargo run --release --bin optd-perfbench cardbench $benchmark_name --query-ids $id &>/dev/null

    if [ $? -eq 0 ]; then
        echo >&2 $id succeeded
        successful_ids+=("$id")
    else
        echo >&2 $id failed
    fi
done

echo >&2
echo " Useful Outputs"
echo "================"
working_query_ids_vec="pub const ${vec_var_name}: &[&str] = &[\"${successful_ids[0]}\""
IFS=" "
for id in "${successful_ids[@]:1}"; do
    working_query_ids_vec+=", \"$id\""
done
working_query_ids_vec+="]"
echo "${working_query_ids_vec}"
IFS=","
echo "--query-ids ${successful_ids[*]}"
