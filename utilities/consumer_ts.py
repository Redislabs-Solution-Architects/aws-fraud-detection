#!/usr/bin/python3

import redis
import time
import datetime
import json
import boto3
import random

stream_name = "demo-stream"

# Kinesis setup
kinesis = boto3.client("kinesis")

response = kinesis.describe_stream(StreamName=stream_name)
shard_id = response['StreamDescription']['Shards'][0]['ShardId']

pre_shard_it = kinesis.get_shard_iterator(StreamName=stream_name, ShardId=shard_id, ShardIteratorType="LATEST")
shard_it = pre_shard_it["ShardIterator"]

# Redis setup
client = redis.Redis(host='localhost', port=6379, db=1)
key = 'fraud-ts'


while 1==1:
    out = kinesis.get_records(ShardIterator=shard_it, Limit=1)
    for record in out['Records']:
        data = json.loads(record['Data'])
        trans_date_trans_time = data["trans_date_trans_time"]
        date = datetime.datetime.strptime(trans_date_trans_time, "%Y-%m-%d %H:%M:%S")
        timestamp = int(datetime.datetime.timestamp(date))
        merchant = data["merchant"]
        category = data["category"]
        fraud_score = float(data["is_fraud"])
        client.ts().add(key,timestamp,fraud_score,retention_msecs=30000,duplicate_policy='last',labels={'merchant': merchant,'category': category})

        print(data)
        print("\n")

    shard_it = out["NextShardIterator"]
    time.sleep(1.0)
