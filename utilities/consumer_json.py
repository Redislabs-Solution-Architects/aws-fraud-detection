#!/usr/bin/python3

import boto3
import time
import json
import decimal
import redis
from redis.commands.json.path import Path

stream_name = "demo-stream"

# Kinesis setup
kinesis = boto3.client("kinesis")

response = kinesis.describe_stream(StreamName=stream_name)
shard_id = response['StreamDescription']['Shards'][0]['ShardId']

pre_shard_it = kinesis.get_shard_iterator(StreamName=stream_name, ShardId=shard_id, ShardIteratorType="LATEST")
shard_it = pre_shard_it["ShardIterator"]

# Redis setup
client = redis.Redis(host='localhost', port=6379, db=0)

while 1==1:
    out = kinesis.get_records(ShardIterator=shard_it, Limit=1)
    for record in out['Records']:
        data = json.loads(record['Data'])
        trans_date_trans_time = data["trans_date_trans_time"]

        # Construct a unique sort key for this line item
        key = "fraud:" + trans_date_trans_time
        client.json().set(key, Path.rootPath(), data)
        result = client.json().get(key)
        print(result)
        print("\n")

    shard_it = out["NextShardIterator"]
    time.sleep(1.0)
