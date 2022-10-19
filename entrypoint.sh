#!/bin/bash
# if AWS_BATCH_JOB_ARRAY_INDEX is not set, set it to 0
if [ -z "$AWS_BATCH_JOB_ARRAY_INDEX" ]; then
    export AWS_BATCH_JOB_ARRAY_INDEX=0
fi
python3 scripts/contamination_rate.py --train_files $1 --val_files $2 --result_dir $3 --array_size $4 --array_index $AWS_BATCH_JOB_ARRAY_INDEX