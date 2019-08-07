# clickhouse-ganglia
ganglia plugin for clickhouse  
*Python 2.7 tested*

Thanks to https://github.com/yurial/clickhouse-client python version of clickhouse http client.  


### How to use
Put clickhouse_client.py and clickhouse_metric.py to where ganglia python_modules located, usually /usr/lib64/ganglia/python_modules/  
Run `python clickhouse_metric.py` for test

Put clickhouse_metric.pyconf to where ganglia conf.d located, usually /etc/ganglia/conf.d

### Customize metrics to collect
Edit used_metrics in clickhouse_metric.py if your only want to collect key metrics to ganglia like this:
```python
    'used_metrics': {
        'TCPConnection',
        'HTTPConnection',
        'Query',
        'Merge',
        'PartMutation',
        'QueryPreempted',
        'MemoryTracking',
        'DelayedInserts',
        'InsertedRows',
        'InsertedBytes',
        'InsertQuery',
        'SelectQuery',
        'MergedUncompressedBytes',
        'MaxPartCountForPartition',
        'ReplicasMaxQueueSize',
        'ReplicasMaxAbsoluteDelay',
        'ReplicasMaxInsertsInQueue',
        'ReplicasMaxMergesInQueue',
        'total_bytes',
        'total_parts'
    }
```
