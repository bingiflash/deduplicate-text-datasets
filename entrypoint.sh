#!/bin/bash
python3 scripts/contamination_rate.py --train_files $1 --val_files $2 --result_dir $3 --array_size $4 --array_index $AWS_BATCH_JOB_ARRAY_INDEX