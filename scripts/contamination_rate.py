import json
import struct
import os
import sys
import time

temp_folder = './tmp/rate'
content_column = "content"

def get_line_seperator():
    return b"\xff\xff"

def main(val_file, train_file):
    
    # extract content from validation file
    combined_val_file = os.path.join(temp_folder, 'val.txt')
    with open(combined_val_file, 'wb') as f:
        with open(val_file, 'r') as vf:
            val_lines = vf.readlines()
            for line in val_lines:
                modified_line = json.loads(line)[content_column].replace('\n', ' ')
                f.write(modified_line.encode('utf-8'))
                f.write('\n'.encode('utf-8'))

    # extract content from training file
    combined_train_file = os.path.join(temp_folder, 'train.txt')
    with open(combined_train_file, 'wb') as f:
        with open(train_file, 'r') as f2:
            for line in f2.readlines():
                modified_line = json.loads(line)[content_column].replace('\n', ' ')
                f.write(modified_line.encode('utf-8')) 
                f.write(get_line_seperator())

    print(os.popen(f"cargo run contains --data-file {combined_train_file} --query-file {combined_val_file}").read())


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

if __name__ == "__main__":
    train_files_path = sys.argv[1]
    val_files_path = sys.argv[2]

    val_files = get_files(val_files_path)
    train_files = get_files(train_files_path)
    
    # make temp folder
    os.makedirs(temp_folder, exist_ok=True)

    for val_file in val_files:
        for train_file in train_files:
            print(f"Validation file: {val_file}, Training file: {train_file}")
            main(val_file, train_file)
            print("=========================================")


"""
Questions: 
1. even if we are doing file pairs, we still need to read the json and extract text(python operation), it will be expensive right?
1. lowercase or not?
2. when generating n-grams, should we remove punctuations?
"""