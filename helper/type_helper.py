import json

from helper.error_helper import log_error
from helper.logger_helper import LoggerSimple

logger = LoggerSimple(name=__name__).logger


def cast_int(value, default=None):
    if type(value) == int:
        return value
    if value is not None:
        try:
            return int(value)
        except Exception as e:
            # logger.info(f'ERROR cast int {value}')
            # log_error(e)
            pass

    return default


def cast_float(value, default=None):
    if type(value) == float:
        return value
    if value is not None:
        try:
            return float(value)
        except Exception as e:
            logger.info(f'ERROR cast float {value}')
            log_error(e)

    return default


def cast_bool(value, default=None):
    if type(value) == bool:
        return value
    if value is not None:
        try:
            return bool(value)
        except Exception as e:
            logger.info(f'ERROR cast bool {value}')
            log_error(e)

    return default


def cast_str(value, default=None):
    if type(value) == str:
        return value
    if value is not None:
        try:
            return str(value)
        except Exception as e:
            logger.info(f'ERROR cast str {value}')
            log_error(e)
    return default


def cast_datetime(value, default=None):
    import datetime
    from helper.datetime_helper import get_datetime_from_timestamp
    if type(value) == datetime.datetime:
        return value
    if value is not None:
        if type(value) == int:
            return get_datetime_from_timestamp(value)
        else:
            try:
                return get_datetime_from_timestamp(value)
            except Exception as e:
                logger.info(f'ERROR cast str {value}')
                log_error(e)
    return default


def cast_obj(value, default=None):
    if type(value) == dict or type(value) == list:
        return value
    if value is not None:
        try:
            return json.loads(value)
        except Exception as e:
            logger.info(f'ERROR cast obj {value}')
            log_error(e)
    return default


def cast_json_str(value, default=None):
    if type(value) == str:
        return value
    if value is not None:
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception as e:
            logger.info(f'ERROR cast json str {value}')
            log_error(e)
    return default


def cast_json_byte(value, default=None):
    if type(value) == str:
        return value.encode('utf-8')
    if value is not None:
        try:
            # return json.dumps(value)
            # return orjson.dumps(value)
            return json.dumps(value).encode('utf-8')
        except Exception as e:
            logger.info(f'ERROR cast json str {value}')
            log_error(e)
    return default
