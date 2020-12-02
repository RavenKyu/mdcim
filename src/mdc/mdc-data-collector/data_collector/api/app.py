import operator
import json
from functools import wraps
from collections import deque
from flask import Flask, request, jsonify
from flask_restx import Resource, Api, reqparse

from werkzeug.exceptions import (BadRequest, NotFound)
from data_collector.utils.logger import get_logger
from data_collector.manager import DataCollector

app = Flask(__name__)
api = Api(app)

logger = get_logger('API')

collector = DataCollector()


###############################################################################
def result(f):
    @wraps(f)
    def func(*args, **kwargs):
        try:
            r = f(*args, **kwargs)
            return jsonify(r)
        except NotFound as e:
            api.abort(404, str(e))
        except Exception as e:
            logger.exception(msg=str(e), exc_info=e)
            raise

    return func

#
# ###############################################################################
# def get_column_names(model):
#     columns = [str(x) for x in model.__table__.columns]
#     columns = [x[x.rfind('.') + 1:] for x in columns]
#     return columns
#
#
# @api.route('/devices')
# class Device(Resource):
#     @result
#     def get(self):
#         return list(collector.get_schedule_jobs())
#
#
# @api.route('/schedules')
# class Schedules(Resource):
#     @result
#     def post(self):
#         d = request.get_json(force=True)
#         _id, code, template, seconds, description, comm, use = \
#             operator.itemgetter(
#                 'id',
#                 'code',
#                 'template',
#                 'seconds',
#                 'description',
#                 'comm',
#                 'use')(d)
#
#         if _id in [x['id'] for x in collector.get_schedule_jobs()]:
#             logger.error(f'{_id} is already in the scheduler.')
#             # todo: Raise here
#             return
#
#         comm_type = comm['type']
#         host = comm['setting']['host']
#         port = comm['setting']['port']
#
#         args = [code, _id, seconds, template, host, port, description, use,
#                 comm_type]
#         collector.add_job_schedule(*args)
#         return None
#
#     @result
#     def get(self):
#         return list(collector.get_schedule_jobs())
#
#
# ###############################################################################
# schedule_id_parser = reqparse.RequestParser()
# schedule_id_parser.add_argument(
#     'seconds', type=int, help="interval time", store_missing=False)
#
#
# @api.route('/schedules/<string:_id>')
# class Schedules(Resource):
#     @result
#     def delete(self, _id):
#         if _id not in [x['id'] for x in collector.get_schedule_jobs()]:
#             logger.error(f'{_id} is not in the scheduler.')
#             return
#         return collector.remove_job_schedule(_id)
#
#     def patch(self, _id):
#         args = schedule_id_parser.parse_args()
#         collector.modify_job_schedule(_id, args['seconds'])
#         return
#
#
# ###############################################################################
# device_id_parser = reqparse.RequestParser()
# device_id_parser.add_argument(
#     'last_fetch', action='store', help="interval time")
#
#
# @api.route('/devices/<string:device_name>')
# class Device(Resource):
#     @result
#     def post(self, device_name):
#         d = request.get_json(force=True)
#         d = json.loads(d)
#         if device_name not in collector.data:
#             collector.data[device_name] = deque(maxlen=60)
#         collector.data[device_name].append(d)
#         return None
#
#     @result
#     def get(self, device_name):
#         args = device_id_parser.parse_args()
#         if device_name not in collector.data['__last_fetch'] or \
#                 device_name not in collector.data:
#             raise NotFound(
#                 f'No data for {device_name}. '
#                 f'may not be collecting the data now.')
#         if args['last_fetch'] is not None:
#             data = collector.data['__last_fetch'][device_name]
#             return data.pop() if data else None
#         return list(collector.data[device_name])
#
#     @result
#     def delete(self, device_name):
#         print(device_name)
#
#
# ###############################################################################
# @api.route('/devices/<string:device_name>/<int:index>')
# class Device(Resource):
#     @result
#     def get(self, device_name, index):
#         return list(collector.data[device_name])[index]
