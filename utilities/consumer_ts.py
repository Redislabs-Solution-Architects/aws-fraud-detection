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
client = redis.Redis(host='localhost', port=6379, db=0)
key = 'fraud-ts'


while 1==1:
    out = kinesis.get_records(ShardIterator=shard_it, Limit=1)
    #print (out)
    for record in out['Records']:
        data = json.loads(record['Data'])
        trans_date_trans_time = data["trans_date_trans_time"]
        date = datetime.datetime.strptime(trans_date_trans_time, "%Y-%m-%d %H:%M:%S")
        timestamp = int(datetime.datetime.timestamp(date))
        merchant = data["merchant"]
        category = data["category"]
        #fraud_score = float(data["is_fraud"])
        fraud_score = round(random.uniform(0.1, 1.0), 2)
        print("%s - %s - %2.2f" % (trans_date_trans_time, merchant, fraud_score))
        #client.ts().add(key,timestamp,fraud_score,retention_msecs=30000,duplicate_policy='last',labels={'merchant': merchant,'category': category})
        #client.ts().add(key,timestamp,fraud_score,duplicate_policy='last',labels={'merchant': merchant,'category': category})
        client.ts().add(key,"*",fraud_score,duplicate_policy='last')
        if (fraud_score >= 0.7 ):
            client.ts().add("fraudulent_ts","*",fraud_score,duplicate_policy='last', labels={'type': "fraud_score"})
            print("*** adding to fraudulent series with current timestamp")
        else:
            client.ts().add("non-fraudulent_ts","*",fraud_score,duplicate_policy='last', labels={'type': "fraud_score"})
            print("*** adding to non-fraudulent series with current timestamp")

        #client.ts().add(key,timestamp,fraud_score)

        #print(data)
        print("\n")

    shard_it = out["NextShardIterator"]
    time.sleep(0.1)
