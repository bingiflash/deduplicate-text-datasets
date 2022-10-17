import argparse
import json
import mmap
import os
import shutil
import time
import uuid

# if AWS_BATCH_JOB_ID exists, use it as unique id else use uuid
unique_id = os.environ.get('AWS_BATCH_JOB_ID', str(uuid.uuid4())).split(':')[0]

import s3_accessor
from tqdm import tqdm

data_dir = './data'
temp_folder = './tmp/rate'
content_column = "text"
filter_columns = ["identity_attack", "insult","obscene","severe_toxicity","sexual_explicit","threat","toxicity"]
filter_threshold = 0.5

def get_line_seperator():
    return b"\xff\xff"

def find_no_of_contaminated_lines_from_rust_result(rust_result):
    # aka the last non-empty line as a list
    last_line = ""
    #  find last non-empty line in val
    val_lines = rust_result.split('\n')
    for i in range(len(val_lines) - 1, -1, -1):
        if val_lines[i] != '':
            last_line = val_lines[i]
            break
    # convert the last line to a list
    last_line = json.loads(last_line)
    return last_line


def get_files(path):
    files = []
    # if path is a file, use it directly
    if os.path.isfile(path):
        files = [path]
    # if path is a directory, use all json files in it
    elif os.path.isdir(path):
        for file in os.listdir(path):
            if ".json" in file:
                files.append(os.path.join(path, file))
    return files

# TODO: maybe think of a way to do this parallelly
def extract_lines_from_jsonl_files(files, output_file, include_newline=True):
    with open(output_file, 'wb') as of:
        for file in tqdm(files):
            with open(file, mode="r", encoding="utf-8") as f:
                with mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ) as mmap_in:
                    while True:
                        line = mmap_in.readline()
                        if not line:
                            break
                        json_line = json.loads(line)
                        if not any(column in json_line and float(json_line[column]) >= filter_threshold for column in filter_columns):
                            modified_line = json_line[content_column].replace('\n', ' ')
                            of.write(modified_line.encode('utf-8'))
                            if include_newline:
                                of.write("\n".encode('utf-8'))
                            else:
                                of.write(get_line_seperator())

                            
def remove_lines_from_file(file, lines_to_remove):
    lines = []
    with open(file, 'r') as f:
        lines = f.readlines()
    with open(file, 'w') as f:
        for i, line in enumerate(lines):
            if i not in lines_to_remove:
                f.write(line)
            else:
                f.write('\n')

def main(train_files_path, val_files_path, result_dir):
    val_files = get_files(val_files_path)
    train_files = get_files(train_files_path)
    
    # make temp folder
    os.makedirs(temp_folder, exist_ok=True)

    dataset_contamination_rate = 0
    total_val_contaminated_lines = set()

    modified_val_file = os.path.join(temp_folder, 'val.txt')
    extract_lines_from_jsonl_files(val_files, modified_val_file)
    # find no.of lines in val file using wc
    total_val_lines = int(os.popen(f"wc -l {modified_val_file}").read().split(' ')[0])

    for index, train_file in enumerate(train_files):
        modified_train_file = os.path.join(temp_folder, 'train.txt')
        extract_lines_from_jsonl_files([train_file], modified_train_file, include_newline=False)
        batch_train_lines = int(os.popen(f"wc -l {modified_train_file}").read().split(' ')[0])

        print(f"Validation file: {modified_val_file}({total_val_lines}), Training file: {train_file}({batch_train_lines})")
        # if no.of words in val files is 0, skip this file
        if int(os.popen(f"wc -w {modified_val_file}").read().split(' ')[0]) == 0:
            print("Skipping rest of the training files as no.of words in val file is 0")
            continue

        rust_result = os.popen(f"cargo run contains --data-file {modified_train_file} --query-file {modified_val_file} --gram-size 8 --num-threads {os.cpu_count() or 1}").read()
        print(rust_result)

        val_batch_contaminated_lines = find_no_of_contaminated_lines_from_rust_result(rust_result)
        total_val_contaminated_lines.update(val_batch_contaminated_lines)
        
        print("=========================================")
        remove_lines_from_file(modified_val_file, val_batch_contaminated_lines)

    dataset_contamination_rate = len(total_val_contaminated_lines) / total_val_lines
    
    line_indicies_file_path = os.path.join(temp_folder, f'{unique_id}-{array_index}-contaminated_lines.txt')
    with open(line_indicies_file_path, 'w') as f:
        f.write(json.dumps(list(total_val_contaminated_lines)))
        f.write("\n")
    
    # copy contaminated lines s3 result dir
    s3_accessor.upload(f"{result_dir.strip('/')}/{unique_id}/{array_index}-contaminated_lines.txt", line_indicies_file_path)
    
    print(f"Dataset contamination rate: {dataset_contamination_rate}")


def copy_s3_to_local(train_files, val_files, train_dir, val_dir):
    for train_file in train_files:
        s3_accessor.download_to_local(train_file, train_dir)
    for val_file in val_files:
        s3_accessor.download_to_local(val_file, val_dir)
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--train_files', type=str, required=True, help='S3 path to train files. (Provide dir or file)')
    parser.add_argument('--val_files', type=str, required=True, help='S3 path to val files. (Provide dir or file)')
    parser.add_argument('--result_dir', type=str, default="s3://vbingi-dev/batch-jobs/", help='result directory remote path')
    parser.add_argument('--array_size', type=int, default=1, help='Array size')
    parser.add_argument('--array_index', type=int, default=0, help='Array index')

    args = parser.parse_args()
    train_files_remote_path = args.train_files
    val_files_remote_path = args.val_files
    result_dir = args.result_dir
    array_size = args.array_size
    array_index = args.array_index

    val_files_local_dir = os.path.join(data_dir, 'val')
    train_files_local_dir = os.path.join(data_dir, 'train')

    # check if the directories exist
    if os.path.exists(val_files_local_dir):
        shutil.rmtree(val_files_local_dir)
    if os.path.exists(train_files_local_dir):
        shutil.rmtree(train_files_local_dir)

    # create the directories
    os.makedirs(val_files_local_dir, exist_ok=True)
    os.makedirs(train_files_local_dir, exist_ok=True)

    train_files = []
    val_files = []

    train_bucket, train_key = s3_accessor.getBucketNameAndPrefix(train_files_remote_path)
    val_bucket, val_key = s3_accessor.getBucketNameAndPrefix(val_files_remote_path)
    
    key_index = 0
    for key in s3_accessor.getNextKey(bucket=train_bucket, prefixes=[train_key], suffixes=['.jsonl','.json']):
        if key_index % array_size == array_index:
            train_files.append(f"s3://{train_bucket}/{key}")
        key_index+=1

    val_files =[f"s3://{val_bucket}/{key}" for key in s3_accessor.getNextKey(bucket=val_bucket, prefixes=[val_key], suffixes=['.jsonl','.json'])]

    # copy files from s3 to local
    copy_s3_to_local(train_files, val_files, train_files_local_dir, val_files_local_dir)

    # run the main function
    main(train_files_local_dir, val_files_local_dir, result_dir)