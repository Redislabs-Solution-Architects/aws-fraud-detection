#!/usr/bin/python3

import csv
import time
import sys
import datetime
import json
import random
import boto3

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

def prep_data(data):
    return json.dumps(data)

def read_data(start_line, num_lines):
    with open(source_data, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        header = "id,trans_date_trans_time,cc_num,merchant,category,amt,first,last,gender,street,city,state,zip,lat,long,city_pop,job,dob,trans_num,unix_time,merch_lat,merch_long,is_fraud"
        next (reader) #skip header
        input_row = 0
        lines_processed = 0
        for row in reader:
            input_row += 1
            if (input_row > start_line):
                print(row)
                print("\n")
                send_data(prep_data(row))
                time.sleep(1.0) #add delay
                lines_processed += 1
                if (lines_processed >= num_lines):
                    break
        return lines_processed

if __name__ == '__main__':
    stream_name = "demo-stream"
    source_data = "data/fraud_test_data_long.csv"
    place_holder = "data/last_line.txt"
    kinesis_client = boto3.client('kinesis')
    num_lines = 10
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
