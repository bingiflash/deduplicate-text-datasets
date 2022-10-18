import json
import os
import re
import sys

import datasets


def main(data_dir_location, output_file_location):
    ds = datasets.load_from_disk(data_dir_location)
    temp_file_name = output_file_location + ".temp"
    ds.to_json(temp_file_name)
    lines = []
    with open(temp_file_name, "r") as f:
        for line in f.readlines():
            json_line = json.loads(line)
            tokens_str = " ".join(json_line["tokens"])
            
            # additional processing
            # replace any space after non-alpha-numeric character with nothing
            tokens_str = re.sub(r"([^\w\s])\s+", r"\1", tokens_str)
            # replace any space before non-alpha-numeric character with nothing
            tokens_str = re.sub(r"\s+([^\w\s])", r"\1", tokens_str)
            # replace all , with , and space same for . and ? and !
            tokens_str = re.sub(r",", r", ", tokens_str)
            tokens_str = re.sub(r"\.", r". ", tokens_str)
            tokens_str = re.sub(r"\?", r"? ", tokens_str)
            tokens_str = re.sub(r"!", r"! ", tokens_str)

            lines.append({"text": tokens_str})
    os.remove(temp_file_name)
    with open(output_file_location, "w") as f:
        for line in lines:
            f.write(json.dumps(line) + "\n")

if __name__ == "__main__":
    data_dir_locations = sys.argv[1]
    output_file_location = sys.argv[2]
    main(data_dir_locations, output_file_location)