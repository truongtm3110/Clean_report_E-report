import requests

from helper.error_helper import log_error


def set(key, value, table='default', url_host='http://159.65.130.110:1323', secret_key='ZY5bYGgBtE'):
    try:
        url = f'{url_host}/set?table={table}&secret_key={secret_key}'
        response = requests.post(url, json={
            'key': key,
            'value': value
        })
        return response.json()
    except Exception as e:
        log_error(e)
    return None


def get(key, value_default=None, table='default', url_host='http://159.65.130.110:1323', secret_key='ZY5bYGgBtE'):
    try:
        url = f'{url_host}/get?key={key}&table={table}&secret_key={secret_key}'
        # print(url)
        response = requests.get(url)
        value = response.json()
        if value is not None:
            return value
        else:
            return value_default
    except Exception as e:
        log_error(e)
    return value_default


def mget(keys, table='default', url_host='http://159.65.130.110:1323', secret_key='ZY5bYGgBtE'):
    try:
        url = f'{url_host}/mget?table={table}&secret_key={secret_key}'
        response = requests.post(url, json=keys)
        return response.json()
    except Exception as e:
        log_error(e)
    return None


def mset(lst_key_value, table='default', url_host='http://159.65.130.110:1323', secret_key='ZY5bYGgBtE'):
    # lst_key_value = [{'key':'', 'value': ''}]
    try:
        url = f'{url_host}/mset?table={table}&secret_key={secret_key}'
        response = requests.post(url, json=lst_key_value)
        return response.json()
    except Exception as e:
        log_error(e)
    return None


def mupsert(lst_values, table='default', url_host='http://159.65.130.110:1323', secret_key='ZY5bYGgBtE',
            map_primary_field='product_base_id', is_debug=False):
    def merge_two_dict(old_dict, new_dict):
        new_dict_not_with_nulls = {k: v for k, v in new_dict.items() if v is not None}
        return {**old_dict, **new_dict_not_with_nulls}

    try:

        current_data_list = mget(keys=[data[map_primary_field] for data in lst_values], url_host=url_host,
                                 secret_key=secret_key, table=table)

        current_data_map = {}
        if current_data_list is not None and len(current_data_list) > 0:
            for current_data in current_data_list:
                if current_data.get(map_primary_field) is not None:
                    current_data_map[current_data[map_primary_field]] = current_data

        # merge data
        merged_lst = []
        for new_data in lst_values:

            current_data = current_data_map.get(new_data[map_primary_field])
            if current_data is not None:
                # dang ton tai data roi --> merge data
                if is_debug:
                    print(f'merge old and new data')
                merged_data = merge_two_dict(old_dict=current_data, new_dict=new_data)

                merged_lst.append(merged_data)

            else:
                merged_lst.append(new_data)
        if len(merged_lst) > 0:
            mset(lst_key_value=[{'key': merged_data[map_primary_field], 'value': merged_data} for merged_data in
                                merged_lst], table=table, url_host=url_host,
                 secret_key=secret_key)
        if is_debug:
            if len(merged_lst) > 0:
                print(f'upserted {len(merged_lst)} items, first is {merged_lst[0]}')
    except Exception as e:
        log_error(e)
        return False
    return True


def delete(keys, table='default', url_host='http://159.65.130.110:1323', secret_key='ZY5bYGgBtE'):
    try:
        url = f'{url_host}/delete?table={table}&secret_key={secret_key}'
        response = requests.post(url, json=keys)
        return response.json()
    except Exception as e:
        log_error(e)
    return None


def add_table(table='default', url_host='http://159.65.130.110:1323', secret_key='ZY5bYGgBtE'):
    try:
        url = f'{url_host}/add_table?table={table}&secret_key={secret_key}'
        response = requests.get(url)
        return response.json()
    except Exception as e:
        log_error(e)
    return None


if __name__ == '__main__':
    # mupsert(lst_values=[
    #     {
    #         "attributes": [
    #             {
    #                 "id": 1072,
    #                 "name": "Thương hiệu",
    #                 "value": "hoco."
    #             }
    #         ],
    #         "category_base_id": "1__8706",
    #         "product_base_id": "phuongpv",
    #         "view_count": 8,
    #         "is_oke": True
    #
    #     }], table='product_base', url_host='http://cache.beecost.com:12232', secret_key='beeKV!!12Rtx_', is_debug=True)
    # print(mget(keys=['phuongpv'], table='product_base', url_host='http://cache.beecost.com:12232',
    #            secret_key='beeKV!!12Rtx_'))
    import time

    table = 'default'
    url_host = 'http://localhost:1323'
    secret_key = 'ZY5bYGgBtE'
    # add_table(table='default', url_host=url_host, secret_key=secret_key)
    lst_key_value = [{
    }]
    for i in range(10_000):
        lst_key_value.append({
            'key': f'{i}',
            'value': {
                'value': f'value_{i}',
                'name': f'name_{i}'
            }
        })
    # start = time.time()
    print(mset(lst_key_value=lst_key_value, table=table, url_host=url_host, secret_key=secret_key))
    # print(f'index in {int((time.time() - start) * 1000)} ms')
    # print(get(key='10000', table=table, url_host=url_host, secret_key=secret_key))
    print(mget(keys=['999', '100'], table=table, url_host=url_host, secret_key=secret_key))
