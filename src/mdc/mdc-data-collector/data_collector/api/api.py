import functools
import json
from data_collector.utils.http_handler import CommunicationHTTP
from data_collector.utils.logger import get_logger


################################################################################
class ConfigureAPIError(Exception):
    pass


################################################################################
class API:
    ENDPOINT = ''

    # ==========================================================================
    def __init__(self, ip, port):
        logger = get_logger('http-api-handler')
        self.api = CommunicationHTTP(ip, port, logger, endpoint=self.ENDPOINT)

    # =========================================================================
    def result(f):
        @functools.wraps(f)
        def func(*args, **kwargs):
            #  커넥션 상태 처리
            ok, data, message = f(*args, **kwargs)
            if not ok:
                raise ConnectionError(str(message))
            data = json.loads(data)
            # API서버에서 주는 상태 값 처리
            return data
        return func

    # =========================================================================
    @result
    def get_data(self, name):
        return self.api.get(api=f'devices/{name}?last_fetch')

