### fork of https://github.com/yurial/clickhouse-client ###


import sys
from sys import version_info

if version_info < (3, 0):
    from urlparse import urlparse, parse_qs
else:
    from urllib.parse import urlparse, parse_qs

from logging import getLogger

logging = getLogger('clickhouse.client')


def raise_exception(data):
    import re
    errre = re.compile('Code: (\d+), e.displayText\(\) = DB::Exception: (.+?), e.what\(\) = (.+)')
    m = errre.search(data)
    if m:
        raise ClickHouseError(*m.groups())
    else:
        raise Exception('unexpected answer: {}'.format(data))


class ClickHouseClient:

    def __init__(self, url=None, **options):
        url = urlparse(url)
        self.scheme = url.scheme
        self.netloc = url.netloc
        self.options = dict([(key, str(val[0])) for key, val in parse_qs(url.query).iteritems()])
        self.options.update(options)

    def __repr__(self):
        return str((self.scheme, self.netloc, self.options))

    def _on_header(self):
        def wrapper(header):
            try:
                key, value = header.split(':', 1)
                value = value.strip()
                logging.debug('header={header} value={value}'.format(header=key, value=value))
            except Exception as e:
                return

        return wrapper

    def _fetch(self, url, query):
        logging.debug('query={query}'.format(query=query))
        import urllib2
        req = urllib2.Request(url, query)
        response = urllib2.urlopen(req)
        return response.read().decode('utf-8')

    def _build_url(self, opts):
        from copy import deepcopy
        options = deepcopy(self.options)  # get copy of self.options
        options.update(opts)  # and override with opts
        options = dict(
            [(key, val) for key, val in options.iteritems() if val is not None])  # remove keys with None values
        urlquery = '&'.join(['{}={}'.format(key, val) for key, val in options.iteritems()])
        url = '{self.scheme}://{self.netloc}/?{urlquery}'.format(self=self, urlquery=urlquery)
        logging.debug('url={url}'.format(url=url))
        return url

    def select(self, query, **opts):
        import re
        from json import loads
        if re.search('[)\s]FORMAT\s', query, re.IGNORECASE):
            raise Exception('Formatting is not available')
        query += ' FORMAT JSONCompact'
        url = self._build_url(opts)
        data = self._fetch(url, query)
        try:
            return Result(**loads(data))
        except BaseException:
            raise_exception(data)

    def execute(self, query, **kwargs):
        url = self._build_url(kwargs)
        data = self._fetch(url, query)
        if data != '':
            raise_exception(data)
        return data


# In Python 3 StandardError --> Exception.
if sys.version_info < (3, 0):
    BaseClass = StandardError
else:
    BaseClass = Exception


class ClickHouseError(BaseClass):
    def __init__(self, code, msg, what):
        BaseClass.__init__(self, msg)
        self.code = code
        self.what = what

    def __str__(self):
        return 'code: {self.code}, message: {self.message}'.format(self=self)

    def __repr__(self):
        return self.__str__()


class Statistic:
    bytes_read = None
    rows_read = None
    elapsed = None

    def __init__(self, bytes_read, rows_read, elapsed):
        self.bytes_read = bytes_read
        self.rows_read = rows_read
        self.elapsed = elapsed


class Result:
    meta = None
    data = None
    totals = None
    statistics = None

    def __init__(self, meta, data, totals=None, statistics=None, **kwargs):
        self.meta = meta
        self.data = data
        self.totals = totals
        if statistics:
            self.statistics = Statistic(*statistics)
