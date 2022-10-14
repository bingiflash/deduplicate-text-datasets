import json
import os
import sys

temp_folder = './tmp/rate'
content_column = "content"

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

def extract_lines_from_jsonl_files(files, output_file, include_newline=True):
    with open(output_file, 'wb') as of:
        for file in files:
            with open(file, 'r') as f:
                for line in f.readlines():
                    modified_line = json.loads(line)[content_column].replace('\n', ' ')
                    of.write(modified_line.encode('utf-8'))
                    if include_newline:
                        of.write('\n'.encode('utf-8'))
                    else:
                        of.write(get_line_seperator())
    
def remove_lines_from_a_file(file_path, lines_to_remove):
    file = open(file_path, 'r')
    lines = file.readlines()
    file.close()
    # for safety, remove the file
    os.remove(file_path)
    file = open(file_path, 'w')
    for i in range(len(lines)):
        if i not in lines_to_remove:
            file.write(lines[i])
    file.close()

def main(val_files_path, train_files_path):
    val_files = get_files(val_files_path)
    train_files = get_files(train_files_path)
    
    # make temp folder
    os.makedirs(temp_folder, exist_ok=True)

    dataset_contamination_rate = 0
    total_val_lines = 0
    total_val_contaminated_lines = 0

    modified_val_file = os.path.join(temp_folder, 'val.txt')
    print("Extracting lines from val files...")
    extract_lines_from_jsonl_files(val_files, modified_val_file)
    # find no.of lines in val file using wc
    val_batch_lines = int(os.popen(f"wc -l {modified_val_file}").read().split(' ')[0])
    print(val_batch_lines)
    total_val_lines += val_batch_lines
    for train_file in train_files:
        modified_train_file = os.path.join(temp_folder, 'train.txt')
        print("Extracting lines from train file: ", train_file)
        extract_lines_from_jsonl_files([train_file], modified_train_file, include_newline=False)
        print(f"Validation file: {modified_val_file}, Training file: {train_file}")
        # if size of val file is 0, skip
        if os.path.getsize(modified_val_file) == 0:
            print("Skipping rest of the files as val file is empty")
            break
        rust_result = os.popen(f"cargo run contains --data-file {modified_train_file} --query-file {modified_val_file} --gram-size 8 --num-threads {os.cpu_count() or 1}").read()
        print(rust_result)
        val_batch_contaminated_lines = find_no_of_contaminated_lines_from_rust_result(rust_result)
        total_val_contaminated_lines += len(val_batch_contaminated_lines)
        print("=========================================")
        remove_lines_from_a_file(modified_val_file, val_batch_contaminated_lines)

    dataset_contamination_rate = total_val_contaminated_lines / total_val_lines
    print(f"Dataset contamination rate: {dataset_contamination_rate}")

if __name__ == "__main__":
    train_files_path = sys.argv[1]
    val_files_path = sys.argv[2]

    main(val_files_path, train_files_path)