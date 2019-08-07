import re
from clickhouse_client import ClickHouseClient
from clickhouse_client import ClickHouseError

NAME_PREFIX = 'clickhouse_'
PARAMS = {
    'used_metrics': {}
    # uncomment these if your only want to collect key metrics to ganglia
    # 'used_metrics': {
    #     'TCPConnection',
    #     'HTTPConnection',
    #     'Query',
    #     'Merge',
    #     'PartMutation',
    #     'QueryPreempted',
    #     'MemoryTracking',
    #     'DelayedInserts',
    #     'InsertedRows',
    #     'InsertedBytes',
    #     'InsertQuery',
    #     'SelectQuery',
    #     'MergedUncompressedBytes',
    #     'MaxPartCountForPartition',
    #     'ReplicasMaxQueueSize',
    #     'ReplicasMaxAbsoluteDelay',
    #     'ReplicasMaxInsertsInQueue',
    #     'ReplicasMaxMergesInQueue',
    #     'total_bytes',
    #     'total_parts'
    # }
}
METRICS = {}
DELTA_METRICS = set([])
DELTA_METRICS_VALUE = {}


def get_value(name):
    q = METRICS[name]
    result = client.select(q)
    v = float(result.data[0][0])
    if name not in DELTA_METRICS:
        return v
    last_value = 0
    # do the delta calculation instead of setting slope to positive in descriptor which is not actually working
    if name in DELTA_METRICS_VALUE:
        last_value = DELTA_METRICS_VALUE[name]
    DELTA_METRICS_VALUE[name] = v
    return v - last_value


def metric_init(lparams):
    """Initialize metric descriptors"""
    global client
    global used_metrics

    # set parameters
    for key in lparams:
        PARAMS[key] = lparams[key]
    used_metrics = PARAMS["used_metrics"]

    descriptors = []

    client = ClickHouseClient(PARAMS['url'], user=PARAMS['user'], password=PARAMS['password'])

    add_to_descriptors('select metric from system.metrics', 'metric', False,
                       'select value from system.metrics where metric=\'{}\'', descriptors)
    add_to_descriptors('select metric from system.asynchronous_metrics', 'asynchronous_metrics', False,
                       'select value from system.asynchronous_metrics where metric=\'{}\'', descriptors)
    add_to_descriptors('select event from system.events', 'event', True,
                       'select value from system.events where event=\'{}\'', descriptors)
    add_to_descriptors('select \'total_bytes\'', 'statistic', False,
                       'select sum(bytes) from system.parts where active = 1', descriptors)
    add_to_descriptors('select \'total_parts\'', 'statistic', False,
                       'select count() from system.parts where active = 1', descriptors)

    return descriptors


def add_to_descriptors(query, group, delta, template, descriptors):
    try:
        result = client.select(query)
        g = NAME_PREFIX + group
        for r in result.data:
            name = r[0].encode('utf-8')
            # ignore this metric if used_metrics is specified
            if len(used_metrics) > 0 and name not in used_metrics:
                continue
            _name = NAME_PREFIX + to_snake(name).replace('zoo_keeper', 'zookeeper')
            if delta:
                _name += '_delta'
                DELTA_METRICS.add(_name)
            METRICS[_name] = template.format(name)
            descriptors.append({
                'name': _name,
                'call_back': get_value,
                'time_max': 60,
                'value_type': 'double',
                'units': '',
                'slope': 'both',
                'format': '%f',
                'description': 'clickhouse ' + name,
                'groups': g
            })
    except ClickHouseError as e:
        print(e)
    except Exception as e:
        print(e)

    return descriptors


def to_snake(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def metric_cleanup():
    DELTA_METRICS_VALUE.clear()
    """clean"""


if __name__ == '__main__':
    lp = {
        'url': 'http://localhost:8023',
        'user': 'default',
        'password': 'default'
    }
    for d in metric_init(lp):
        v = d['call_back'](d['name'])
        print 'value for %s is %u' % (d['name'], v)
