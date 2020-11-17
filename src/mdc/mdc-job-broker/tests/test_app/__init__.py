import time
import bottle
import traceback
import logging

from celery import Celery

logger = logging.getLogger('app')
BROKER_URL = 'redis://redis:6379/0'
CELERY_RESULT_BACKEND = 'redis://redis:6379/0'

celery_app = Celery('routine-jobs', broker=BROKER_URL, backend=CELERY_RESULT_BACKEND)


@bottle.route('/', method='GET')
def trigger_task():
    try:
        logger.info('hello')
        c_app = Celery('jobs', broker=BROKER_URL,
                            backend=CELERY_RESULT_BACKEND)

        r = c_app.send_task('hello', args=(1, 2, 3))
        logger.info(r.__dir__())
        # waiting for finishing the job
        time.sleep(2)
        return bottle.template(
            '<h1>{{ret}}</h1><h2>{{backend}}</h2><h3>{{result}}</h3>',
            ret=r.id, backend=r.__dir__(), result=r.result)

    except Exception as e:
        traceback.print_exc()
        return ''


bottle.run(host='0.0.0.0', port=8080)
