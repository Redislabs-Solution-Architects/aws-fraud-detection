#!/usr/bin/python3
import os
import csv
import time
import sys
import datetime
import json
import random
import boto3
import pandas

def get_line_count():
    with open(source_data, encoding='latin-1') as f:
        for i, l in enumerate(f):
            pass
    return i

def send_data(data):
    kinesis_client.put_record(
        StreamName=stream_name,
        Data=data,
        PartitionKey="partitionkey")

def read_data(start_line, num_lines):
    df = pandas.read_csv(source_data)
    df.transpose().to_dict().values()
    out = df.to_json(orient='records',lines=True)
    records = out.splitlines()
    input_row = 0
    lines_processed = 0
    for row in records:
        input_row += 1
        if (input_row > start_line):
            print(row)
            print("\n")
            send_data(row)
            time.sleep(0.1) #add delay
            lines_processed += 1
            if (lines_processed >= num_lines):
                break
    return lines_processed

if __name__ == '__main__':
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    source_data = os.path.join(base_dir, "data/fraud_test_data_long.csv")
    place_holder = os.path.join(base_dir, "data/last_line.txt")
    stream_name = "demo-stream"
    kinesis_client = boto3.client('kinesis')
    num_lines = 100
    start_line = 0

    if (len(sys.argv) > 1):
        num_lines = int(sys.argv[1])

    try:
        with open(place_holder, 'r') as f:
            for line in f:
                 start_line = int(line)
    except IOError:
        start_line = 0

    print("Writing " + str(num_lines) + " lines starting at line " + str(start_line) + "\n")

    total_lines_processed = 0
    lines_in_file = get_line_count()

    while (total_lines_processed < num_lines):
        lines_processed = read_data(start_line, num_lines - total_lines_processed)
        total_lines_processed += lines_processed
        start_line += lines_processed
        if (start_line >= lines_in_file):
            start_line = 0

    print("Wrote " + str(total_lines_processed) + " lines.\n")

    with open(place_holder, 'w') as f:
        f.write(str(start_line))
