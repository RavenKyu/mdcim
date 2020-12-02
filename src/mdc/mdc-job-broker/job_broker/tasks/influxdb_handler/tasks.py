import logging
from functools import wraps
from influxdb import InfluxDBClient
from job_broker import app

logger = logging.getLogger(__name__)
DATABASE = 'mdcim'

class InfluxDBHandler:
    def __init__(self, *args, **kwargs):
        logger.info('influxdb-handler starts')
        self.client = InfluxDBClient(host='mdc-influxdb',
                                     port=8086,
                                     database=DATABASE)
        if not self.client.ping():
            raise ConnectionError('Failed to connect to the InfluxDB.')

    # ==========================================================================
    def __del__(self):
        if not hasattr(self, 'client'):
            return
        if self.client:
            self.client.close()

    # ==========================================================================
    def is_database_exists(self):
        try:
            next(d for d in self.client.get_list_database() if
                 # d['name'] == self.env['INFLUX_DB_DATABASE'])
                 d['name'] == DATABASE)
            return True
        except StopIteration:
            return False

    # ==========================================================================
    def create_a_retention_policy(self):
        pass

    # ==========================================================================
    def create_a_continuous_query(self, filed, name, database,
                                  target_rp_measurement, source_rp_measurement,
                                  _time, tags):

        """
        select_clause = 'SELECT mean(temperature) ' \
                                'INTO rp52w.temperature_mean_10m ' \
                                'FROM autogen.temperature ' \
                                'GROUP BY time(10m), sensorId'
        :param filed: temperature
        :param name: temperature_mean_10m
        :param database: dcim
        :param target_rp_measurement: rp52w.temperature_mean_10m
        :param source_rp_measurement: autogen.temperature
        :param _time: 10m
        :param tags: ["sensorId", ]
        :return:
        """
        select_clause = f'SELECT mean({filed}) AS {filed} ' \
                        f'INTO {target_rp_measurement} ' \
                        f'FROM {source_rp_measurement} ' \
                        f'GROUP BY time({_time}), {", ".join(tags)}'

        self.client.create_continuous_query(
            name=name,
            select=select_clause,
            database=database)


    # ==========================================================================
    def create_db(self):
        @wraps(self)
        def func(*args, **kwargs):
            this = args[0]
            if not this.is_database_exists():
                this.client.create_database(DATABASE)

                this.client.alter_retention_policy(
                    name='autogen', database=DATABASE,
                    duration='2w', replication=1, default=True,
                    shard_duration='1w')
                # 저장기간 설정

                # 1년
                this.client.create_retention_policy(
                    name='rp52w', database=DATABASE,
                    duration='52w', replication='1' )

                # 10분 온도 데이터, 보존기간 1년
                this.create_a_continuous_query(
                    filed='temperature', name='temperature_mean_10m',
                    database=DATABASE,
                    target_rp_measurement='rp52w.temperature_mean_10m',
                    source_rp_measurement='autogen.temperature',
                    _time='10m',
                    tags=['location',])

                # 10분 습도 데이터, 보존기간 1년
                this.create_a_continuous_query(
                    filed='humidity', name='humidity_mean_10m',
                    database=DATABASE,
                    target_rp_measurement='rp52w.humidity_mean_10m',
                    source_rp_measurement='autogen.humidity',
                    _time='10m',
                    tags=['location',])

                # 1시간 온도 데이터, 보존기간 1년
                this.create_a_continuous_query(
                    filed='temperature', name='temperature_mean_1h',
                    database=DATABASE,
                    target_rp_measurement='rp52w.temperature_mean_1h',
                    source_rp_measurement='rp52w.temperature_mean_10m',
                    _time='1h',
                    tags=['location',])

                # 1시간 습도 데이터, 보존기간 1년
                this.create_a_continuous_query(
                    filed='humidity', name='humidity_mean_1h',
                    database=DATABASE,
                    target_rp_measurement='rp52w.humidity_mean_1h',
                    source_rp_measurement='rp52w.humidity_mean_10m',
                    _time='1h',
                    tags=['location',])

                # 1시간 전력 데이터, 보존기간 1년
                this.create_a_continuous_query(
                    filed='wt', name='wt_mean_1h',
                    database=DATABASE,
                    target_rp_measurement='rp52w.wt_mean_1h',
                    source_rp_measurement='rp52w.wt_mean_10m',
                    _time='1h',
                    tags=['location',])

                # 1일 온도 데이터, 보존기간 1년
                this.create_a_continuous_query(
                    filed='temperature', name='temperature_mean_1d',
                    database=DATABASE,
                    target_rp_measurement='rp52w.temperature_mean_1d',
                    source_rp_measurement='rp52w.temperature_mean_1h',
                    _time='1d',
                    tags=['location',])

                # 1일 습도 데이터, 보존기간 1년
                this.create_a_continuous_query(
                    filed='humidity', name='humidity_mean_1d',
                    database=DATABASE,
                    target_rp_measurement='rp52w.humidity_mean_1d',
                    source_rp_measurement='rp52w.humidity_mean_1h',
                    _time='1d',
                    tags=['location',])
            self(*args, **kwargs)

        return func

    # ==========================================================================

    @create_db
    def insert(self, d, *args, **kwargs):
        logger.debug(f'insert data into influxdb: {d}')
        self.client.write_points([d, ])
        return True


@app.task(name='influxdb_insert', serializer='json', time_limit=20)
def influxdb_insert(d):
    try:
        c = InfluxDBHandler()
        c.insert(d)
    except Exception as e:
        import traceback
        traceback.print_exc()
        logger.error(str(e))
    return
