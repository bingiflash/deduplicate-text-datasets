import json
import os
import sys

temp_folder = './tmp/rate'
content_column = "content"

def get_line_seperator():
    return b"\xff\xff"

def find_no_of_contaminated_lines_from_rust_result(rust_result):
    # aka the last non-empty line
    last_line = ""
    #  find last non-empty line in val
    val_lines = rust_result.split('\n')
    for i in range(len(val_lines) - 1, -1, -1):
        if val_lines[i] != '':
            last_line = val_lines[i]
            break
    # cast the last line to integer and return it
    return int(last_line)

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

def extract_lines_from_jsonl_file(file, output_file, include_newline=True):
    with open(output_file, 'wb') as of:
        with open(file, 'r') as f:
            for line in f.readlines():
                modified_line = json.loads(line)[content_column].replace('\n', ' ')
                of.write(modified_line.encode('utf-8'))
                if include_newline:
                    of.write('\n'.encode('utf-8'))
                else:
                    of.write(get_line_seperator())
    

def main(val_files_path, train_files_path):
    val_files = get_files(val_files_path)
    train_files = get_files(train_files_path)
    
    # make temp folder
    os.makedirs(temp_folder, exist_ok=True)

    dataset_contamination_rate = 0
    total_val_lines = 0
    total_val_contaminated_lines = 0

    for val_file in val_files:
        modified_val_file = os.path.join(temp_folder, 'val.txt')
        print("Extracting lines from val file: ", val_file)
        extract_lines_from_jsonl_file(val_file, modified_val_file)
        # find no.of lines in val file using wc
        val_batch_lines = int(os.popen(f"wc -l {modified_val_file}").read().split(' ')[0])
        print(val_batch_lines)
        total_val_lines += val_batch_lines
        for train_file in train_files:
            modified_train_file = os.path.join(temp_folder, 'train.txt')
            print("Extracting lines from train file: ", train_file)
            extract_lines_from_jsonl_file(train_file, modified_train_file, include_newline=False)
            print(f"Validation file: {val_file}, Training file: {train_file}")
            rust_result = os.popen(f"cargo run contains --data-file {modified_train_file} --query-file {modified_val_file} --ngram-size 8 --num-threads 2").read()
            print(rust_result)
            val_batch_contaminated_lines = find_no_of_contaminated_lines_from_rust_result(rust_result)
            total_val_contaminated_lines += val_batch_contaminated_lines
            print("=========================================")

    dataset_contamination_rate = total_val_contaminated_lines / total_val_lines
    print(f"Dataset contamination rate: {dataset_contamination_rate}")

if __name__ == "__main__":
    train_files_path = sys.argv[1]
    val_files_path = sys.argv[2]

    main(val_files_path, train_files_path)




"""
1. possible duplicates of contaminated lines in val file when multiple train files are used
ex:
train1 found a contaminated line in val1 - val1 line no. 11
train2 found a contaminated line in val1 - val1 line no. 11

that would count as 2 contaminated lines in val1, but it's actually 1 contaminated line
"""