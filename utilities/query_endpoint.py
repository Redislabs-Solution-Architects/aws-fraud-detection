#!/usr/bin/python3

import boto3
import time
import json
import decimal
import requests
import random

stream_name = "demo-stream"

# Kinesis setup
kinesis = boto3.client("kinesis")

response = kinesis.describe_stream(StreamName=stream_name)
shard_id = response['StreamDescription']['Shards'][0]['ShardId']

pre_shard_it = kinesis.get_shard_iterator(StreamName=stream_name, ShardId=shard_id, ShardIteratorType="LATEST")
shard_it = pre_shard_it["ShardIterator"]

while 1==1:
    out = kinesis.get_records(ShardIterator=shard_it, Limit=1)
    for record in out['Records']:
        data = json.loads(record['Data'])
        url = f"https://guzhj6cxg8.execute-api.us-east-2.amazonaws.com/test/fraud?q={random.uniform(0.77, 1)}"
        r = requests.post(url, json=data).json()
        print("Fraud ML Score: " + r["fraudScore"])
        print(data)
        print("\n")

    shard_it = out["NextShardIterator"]
    time.sleep(0.1)
