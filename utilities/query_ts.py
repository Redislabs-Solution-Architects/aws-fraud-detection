#!/usr/bin/env python3

from os import environ
from typing import Any, List, Union
from pydantic import BaseModel
import redis

client = redis.Redis(host='localhost', port=6379, db=1)
def get_mrange(
        from_time: Union[int, str],
        to_time: Union[int, str],
        filters: List[str],
        aggregation_type: str,
        bucket_size_msec: int
    ) -> List:

    # res = client.ts().range("fraud-ts","-", "+")

    res = client.ts().mrange(
        from_time=from_time,
        to_time=to_time,
        filters=filters,
        aggregation_type=aggregation_type,
        bucket_size_msec=bucket_size_msec,
        with_labels=True
    )

    result = []
    for series in res:
        for level, data in series.items():
            series = {
                'name': data[0],
                'data': []
            }
            for entry in data[1]:
                series['data'].append({
                    'x': entry[0],
                    'y': entry[1]
                })
            result.append(series)
    print(result)
    return result

if __name__ == '__main__':
    get_mrange(
        from_time = "-",
        to_time = "+",
        aggregation_type = "count",
        bucket_size_msec = 1000,
        filters = ["category=home"]
    )
