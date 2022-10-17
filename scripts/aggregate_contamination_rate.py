import argparse
import json
import os

import s3_accessor
from tqdm import tqdm

local_result_dir = os.path.join('./tmp/results')
os.makedirs(local_result_dir, exist_ok=True)

def main(result_dir):
    result_bucket, result_key = s3_accessor.getBucketNameAndPrefix(result_dir)
    result_files = [f"s3://{result_bucket}/{key}" for key in s3_accessor.getNextKey(bucket=result_bucket, prefixes=[result_key], suffixes=['.txt'])]
    for result_file in tqdm(result_files):
        s3_accessor.download_to_local(result_file, local_result_dir)
    
    # aggregate the results
    total_contaminated_lines = set()
    total_lines = 1716438
    for result_file in tqdm(os.listdir(local_result_dir)):
        with open(os.path.join(local_result_dir, result_file), 'r') as f:
            total_contaminated_lines.update(json.load(f))
    
    print(f"Total contaminated lines: {len(total_contaminated_lines)}")
    print(f"Total lines: {total_lines}")
    print(f"Contamination rate: {len(total_contaminated_lines) / total_lines}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--result_dir', type=str, required=True, help='S3 directory path to result files of batch job. (Provide dir')
    args = parser.parse_args()
    result_dir = args.result_dir
    main(result_dir)