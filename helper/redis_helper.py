import json

import redis

from helper.error_helper import log_error


def is_json(myjson: str):
    try:
        _ = json.loads(myjson)
    except Exception as e:
        return False
    return True


def cache_set(redis_session: redis.client.Redis, key, value, expired_second=None):
    try:
        if type(value) == str:
            redis_session.set(name=key, value=value, ex=expired_second)
        else:
            redis_session.set(name=key, value=json.dumps(value), ex=expired_second)
    except Exception as e:
        log_error(e)


def cache_mset(redis_session: redis.client.Redis, lst_key_value, expired_second=None):
    try:
        lst_key_value_index = {}
        for key_value in lst_key_value:
            key = key_value.get('key')
            value = key_value.get('value')
            if type(value) == str:
                lst_key_value_index[key] = value
                # redis_session.set(name=key, value=value, ex=expired_second)
            else:
                lst_key_value_index[key] = json.dumps(value)
                # redis_session.set(name=key, value=json.dumps(value), ex=expired_second)
        # print(lst_key_value_index)
        redis_session.mset(lst_key_value_index)
    except AttributeError as e:
        pass
    except Exception as e:
        log_error(e)
        # pass


def cache_get(redis_session: redis.client.Redis, key, default_value=None):
    try:
        value = redis_session.get(name=key)

        if is_json(value):
            value_obj = json.loads(value)
            if value_obj is None:
                return default_value
            else:
                return value_obj
        else:
            if value is None:
                return default_value
            else:
                return value

    except Exception as e:
        log_error(e)
    return None


def cache_mget(redis_session: redis.client.Redis, keys):
    try:
        raw_values = redis_session.mget(keys=keys)
        lst_values = []
        for value in raw_values:
            if is_json(value):
                value_obj = json.loads(value)
                if value_obj is not None:
                    lst_values.append(value_obj)
            else:
                if value is not None:
                    lst_values.append(value)
        return lst_values

    except Exception as e:
        log_error(e)
    return None


if __name__ == '__main__':
    from app.db.session_metricseo import redis_session

    print(cache_get(redis_session=redis_session, key='collection_category_1__11416_'))
