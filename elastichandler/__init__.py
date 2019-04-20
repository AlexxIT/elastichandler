import json
from datetime import datetime
from logging import Handler, LogRecord
from threading import Timer

import yaml
from requests import Session

FIELDS = {
    'levelname': 'level',
    'name': 'name',
    'message': 'message'
}


class ElasticHandler(Handler):
    def __init__(self, url: str, pipeline: str = None):
        super().__init__()

        self.url = url
        self.session = Session()
        self.session.headers = {'Content-Type': 'application/x-ndjson'}
        if pipeline:
            self.session.params = {'pipeline': pipeline}

        self._bulk = []
        self._timer = None

    def flush(self):
        if self._timer is not None and self._timer.is_alive():
            self._timer.cancel()
        self._timer = None

        if self._bulk:
            data = ''.join([
                '{"index":{}}\n' + json.dumps(p) + '\n'
                for p in self._bulk
            ])
            try:
                self.session.post(self.url + '/_doc/_bulk', data=data,
                                  timeout=5)
                self._bulk = []
            except:
                pass

    def close(self):
        self.flush()

    def emit(self, record: LogRecord):
        if record.name == 'urllib3.connectionpool' or len(self._bulk) >= 100:
            return

        data = {
            v: getattr(record, k)
            for k, v in FIELDS.items()
        }

        if record.msg == "%r %r":
            data['message'] = record.args[0]
            extra = record.args[1]
            if isinstance(extra, dict):
                data['extra'] = yaml.dump(extra, default_flow_style=False,
                                          allow_unicode=True)
                data['type'] = 'yaml'
            else:
                data['extra'] = extra
                data['type'] = type(extra).__name__
        elif record.exc_text:
            data['message'] = record.getMessage()
            data['extra'] = record.exc_text
            data['type'] = 'exception'
        else:
            data['message'] = record.getMessage()

        data['@timestamp'] = datetime.utcfromtimestamp(record.created) \
            .isoformat()

        self._bulk.append(data)

        if self._timer is None:
            self._timer = Timer(1, self.flush)
            self._timer.daemon = True
            self._timer.start()
