import types
import operator
import yaml
from celery import Celery

from apscheduler.schedulers.background import BackgroundScheduler

from data_collector.utils.logger import get_logger
from data_collector.api.api import API


###############################################################################
class ExceptionResponse(Exception):
    pass


BROKER_URL = 'redis://redis:6379/0'
CELERY_RESULT_BACKEND = 'redis://redis:6379/0'


###############################################################################
class DataCollector:
    def __init__(self):
        self.logger = get_logger('data-collector')
        self.device_info = None
        self.job_order_queue = None

        self.scheduler = BackgroundScheduler(timezone="Asia/Seoul")
        self.scheduler.start()

        self.job_broker = Celery(
            'routine-jobs', broker=BROKER_URL, backend=CELERY_RESULT_BACKEND)

    # =========================================================================
    def add_job_schedule_by_template_file(self, file_path):
        with open(file_path, 'r') as f:
            templates = yaml.safe_load(f)
        for key in templates:
            name = key
            seconds = templates[key]['interval_sec']

            templates = templates[key]
            self.add_job_schedule(name=name,
                                  interval=seconds,
                                  templates=templates)

    # =========================================================================
    def add_job_schedule(self, **kwargs):
        name, interval_sec = operator.itemgetter('name', 'interval')(kwargs)
        self.scheduler.add_job(
            self.request_data, kwargs=kwargs,
            id=name, trigger='interval', seconds=interval_sec)

    # =========================================================================
    def request_data(self, **kwargs):
        # Collector 에서 데이터 가져오기
        # 키 값과 동일한 데이터를 가져오기
        # 잡브로커에 보내기

        name, templates = operator.itemgetter(
            'name', 'templates', )(kwargs)

        api = API('mdc-restful-modbus-api', 5000)
        data = api.get_data(name)
        if not data:
            self.logger.warning('no data')
            return

        # data = {
        #     "data": {
        #         "data01": {
        #             "hex": "7765 6c63 6f6d 6521",
        #             "note": "String",
        #             "type": "B64_STRING",
        #             "value": "welcome!"
        #         },
        #         "data02": {
        #             "hex": "b669",
        #             "note": "unsigned integer value",
        #             "type": "B16_UINT",
        #             "value": 46697
        #         },
        #         "data03": {
        #             "hex": "fd2e",
        #             "note": "integer value",
        #             "type": "B16_INT",
        #             "value": -722
        #         }
        #     },
        #     "datetime": "2020-11-12 15:18:25",
        #     "hex": "77 65 6c 63 6f 6d 65 21 b6 69 fd 2e"
        # }
        dt = data['datetime']
        data = data['data']

        # """
        # 1-A:
        #   interval_sec: 60
        #   templates:
        #     Z1A01R01K01:
        #       measurement: vtcr
        #       fields: wt
        #       tags:
        #         - rack_id: Z1A01R01K01
        #         - path: 1
        #     Z1A01R01K02:
        #       measurement: vtcr
        #       fields: wt
        #       tags:
        #         - rack_id: Z1A01R01K02
        #         - path: 1
        # """
        templates = templates['templates']
        for key in templates:
            if not data.setdefault(key, None):
                self.logger.warning(f'{key} in not in templates..')
                continue

            value = data[key]['value']
            if value is None:
                self.logger.warning(
                    f"No value for {key}. "
                    f"It might not collect data from the sensor yet.")
                continue
            t = templates[key]
            d = dict()

            fields = dict()
            fields[t['fields']] = value
            d['measurement'] = t['measurement']
            d['tags'] = dict()
            if not t['tags']:
                t['tags'] = []
            for tag in t['tags']:
                d['tags'].update(tag)
            d['fields'] = fields
            d['time'] = dt

            self.job_broker.send_task('influxdb_insert', args=(d, ))

    # =========================================================================
    def remove_job_schedule(self, _id: str):
        self.scheduler.remove_job(_id)
        del self.data[_id]
        return

    # =========================================================================
    def modify_job_schedule(self, _id, seconds):
        self.scheduler.reschedule_job(_id, trigger='interval', seconds=seconds)

    # =========================================================================
    def get_schedule_jobs(self):
        jobs = self.scheduler.get_jobs()
        if not jobs:
            return jobs
        result = list()
        for job in jobs:
            _, _, template = job.args
            code, description, use, comm, seconds = operator.itemgetter(
                'code', 'description', 'use', 'comm',
                'interval_second')(job.kwargs)
            result.append(
                dict(id=job.id, code=code, template=template,
                     description=description, use=use, comm=comm,
                     seconds=seconds))
        return result

