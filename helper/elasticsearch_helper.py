#   Copyright (c) 2019 BeeCost Team <beecost.com@gmail.com>. All Rights Reserved
#   BeeCost Project can not be copied and/or distributed without the express permission of @tuantmtb
from datetime import datetime
from typing import Union

from elasticsearch import Elasticsearch, NotFoundError
# from elasticsearch import helpers
from elasticsearch.helpers import bulk

# elasticsearch_connection = Elasticsearch(
#     ['http://' + ES_USER + ':' + ES_PASS + '@' + ES_IP + ':' + ES_PORT + '/elasticsearch'],
#     verify_certs=False, timeout=30)
from helper.error_helper import log_error


def insert_doc(es, index, doc_type, body, id=None, verbose=True):
    res = es.index(index=index, doc_type=doc_type, id=id, body=body)
    es.indices.refresh(index=index)

    if verbose:
        print(res)
    return True


def insert_docs_in_bulk_with_raw_body(es, action_data_bulk, verbose=True):
    if action_data_bulk is None or len(action_data_bulk) == 0:
        return None
    res = None
    try:
        res = bulk(es, action_data_bulk, raise_on_error=True)
        if verbose:
            print(res)
    except Exception as e:
        log_error(e)

    return res


def delete_none(_dict):
    import copy
    def recursive(_dict_clone):
        if not isinstance(_dict_clone, dict):
            return _dict_clone
        for key, value in list(_dict_clone.items()):
            if isinstance(value, dict):
                delete_none(value)
            elif value is None:
                del _dict_clone[key]
            elif isinstance(value, list):
                for v_i in value:
                    delete_none(v_i)
        return _dict_clone

    _dict_clone = copy.deepcopy(_dict)
    return recursive(_dict_clone=_dict_clone)


def insert_docs_in_bulk(es, index, data_bulk, id_fields='_id', verbose=True, action_type='index', ignore_null=True):
    if data_bulk is None or len(data_bulk) == 0:
        return None
    lst_data_bulk_normalize = []
    for record in data_bulk:
        if ignore_null:
            record_normalize = delete_none(record)
            if record_normalize:
                lst_data_bulk_normalize.append(record_normalize)
        else:
            lst_data_bulk_normalize.append(record)

    actions = []
    # for doc in data_bulk:
    for doc in lst_data_bulk_normalize:
        _id = doc.get(id_fields)
        if _id is None:
            continue
        # doc.pop(id_fields)  # remove _id in body
        if action_type == 'index':
            action = {
                "_op_type": action_type,
                "_index": index,
                "_id": _id,
                "_source": doc
            }
        elif action_type == 'update':
            del doc[id_fields]
            action = {
                "_op_type": action_type,
                "_index": index,
                "_id": _id,
                "doc": doc,
                "doc_as_upsert": True
            }
        else:
            continue
        actions.append(action)
    res = None
    try:
        res = bulk(es, actions, raise_on_error=True)
        if verbose:
            print(res)
    except Exception as e:
        log_error(e)

    return res


def remove_doc(es, index, doc_type, id, verbose=True):
    try:
        output = es.delete(index=index, doc_type=doc_type, id=id)
    except NotFoundError as e:
        print('id %s not found' % id)
        return False
    if verbose:
        print(output)
    return True


def remove_all_doc_from_index(es, index, verbose=True):
    res = es.indices.delete(index=index)
    if verbose: print(res)
    return True


def remove_by_query(es, index_name, query):
    is_success = True
    try:
        es.delete_by_query(index=index_name, body=query)
    except Exception as e:
        log_error(e)
        is_success = False
    return is_success


def search(es, index_name, query, include_score=False, filter_path=None, timeout: Union[int, float, None] = None):
    response = None
    try:
        if filter_path is None:
            response = es.search(index=index_name, body=query, request_timeout=timeout)
        else:
            response = es.search(index=index_name, body=query, filter_path=filter_path, request_timeout=timeout)
    except Exception as e:
        log_error(e)
    results = []
    meta = {}
    if response is not None:
        # meta['took'] = response.get('took')
        # print(response)
        meta['total_size'] = response.get('hits').get('total').get('value')
        meta['max_score'] = response.get('hits').get('max_score')

        if response.get('aggregations') is not None:
            aggregations = {}
            for aggregation in response.get('aggregations'):
                # print(response.get('aggregations').get())
                aggregations[aggregation] = [{'key': obj['key'], 'count': obj['doc_count']} for obj in
                                             response.get('aggregations').get(aggregation).get('buckets')]
            meta['aggregations'] = aggregations
        if response.get('hits') is not None and response.get('hits').get('hits') is not None:
            for hit in response.get('hits').get('hits'):
                record = {
                    **hit.get('_source')
                }
                if include_score:
                    record.update({
                        'es_percent': hit.get('_score') / response.get('hits').get('max_score'),
                        'es_score': hit.get('_score'),
                        '_id': hit.get('_id'),
                    })
                results.append(record)

    return results, meta


async def search_v2(es, index_name, query, include_score=False, filter_path=None, timeout: Union[int, float, None] = None):
    response = None
    try:
        if filter_path is None:
            response = await es.search(index=index_name, body=query, request_timeout=timeout)
        else:
            response = await es.search(index=index_name, body=query, filter_path=filter_path, request_timeout=timeout)
    except Exception as e:
        log_error(e)
    results = []
    meta = {}
    if response is not None:
        # meta['took'] = response.get('took')
        # print(response)
        meta['total_size'] = response.get('hits').get('total').get('value')
        meta['max_score'] = response.get('hits').get('max_score')

        if response.get('aggregations') is not None:
            meta['aggregations'] = response.get('aggregations')
        if response.get('hits') is not None and response.get('hits').get('hits') is not None:
            for hit in response.get('hits').get('hits'):
                record = {
                    **hit.get('_source')
                }
                if hit.get('fields'):
                    for field in hit.get('fields'):
                        record.update({
                            field: hit.get('fields').get(field)[0]
                        })
                if include_score:
                    record.update({
                        'es_percent': hit.get('_score') / response.get('hits').get('max_score'),
                        'es_score': hit.get('_score'),
                        '_id': hit.get('_id'),
                    })
                results.append(record)

    return results, meta


def search_scroll(es, index_name, query, include_score=False, filter_path=None,
                  timeout: Union[int, float, None] = None, scroll_time='5m'):
    response = None
    try:
        if filter_path is None:
            response = es.search(index=index_name, body=query, request_timeout=timeout, scroll=scroll_time)
        else:
            response = es.search(index=index_name, body=query, filter_path=filter_path, request_timeout=timeout,
                                 scroll=scroll_time)
    except Exception as e:
        log_error(e)

    results = []
    meta = {}
    if response is not None:
        # meta['took'] = response.get('took')
        # print(response)
        meta['total_size'] = response.get('hits').get('total').get('value')
        meta['max_score'] = response.get('hits').get('max_score')

        if response.get('aggregations') is not None:
            meta['aggregations'] = response.get('aggregations')
        if response.get('hits') is not None and response.get('hits').get('hits') is not None:
            for hit in response.get('hits').get('hits'):
                record = {
                    **hit.get('_source')
                }
                if include_score:
                    record.update({
                        'es_percent': hit.get('_score') / response.get('hits').get('max_score'),
                        'es_score': hit.get('_score'),
                        '_id': hit.get('_id'),
                    })
                results.append(record)

    return results, meta


def example():
    es = Elasticsearch(verify_certs=False, timeout=30)
    check_status_es(es)
    index = "test-index"
    doc_type = 'tweet'
    body = {
        'author': 'tuantm',
        'text': 'Elasticsearch: cool. bonsai cool.',
        'timestamp': datetime.now(),
    }
    id = 2

    # insert_doc(es, index, doc_type, id, body, verbose=True)
    # remove_doc(es, index, doc_type, 2)
    data_bulk = [
        {
            '_id': 1,
            'title': 'hihi',
            'url': 'this is a url'
        },
        {
            '_id': 2,
            'title': 'hihi',
            'url': 'this is a url'
        },
        {
            '_id': 3,
            'title': 'hihi',
            'url': 'this is a url'
        }
    ]
    insert_docs_in_bulk(es, index, doc_type, data_bulk, '_id')
    remove_all_doc_from_index(es, index)
    print('done')


# example()

def check_status_es(es):
    if not es.ping():
        raise ValueError("Connection failed")
    else:
        print('ES live', es)
    return True


def add_meta_query(query, size, source, offset):
    query.update({'size': size})
    query.update({'_source': source})
    return query


def build_filter(key, lst_value, condition_should=True):
    filter = {
        "terms": {
            key: lst_value
        }
    }
    return filter


def build_range(key, begin_value=None, end_value=None, boost=None):
    range = None
    range_value = {}
    if begin_value is not None:
        range_value.update({
            "gte": begin_value
        })
    if end_value is not None:
        range_value.update({
            "lte": end_value
        })
    if boost is not None:
        range_value.update({
            "boost": boost
        })
    if begin_value is not None or end_value is not None:
        range = {
            "range": {
                key: range_value
            }
        }

    return range


def build_phrase_boost(key, query, boost=None, minimum_should_match=None, fuzziness: int = None):
    match_value = {
        "query": query
    }
    if boost is not None:
        match_value.update({
            "boost": boost
        })
    if minimum_should_match is not None:
        match_value.update({
            "minimum_should_match": minimum_should_match
        })
    if fuzziness is not None:
        match_value.update({
            "fuzziness": fuzziness
        })
    match_query = {
        "match": {
            key: match_value
        }
    }

    return match_query


def build_agg(name, field, size=10):
    agg = {
        name: {
            'terms': {
                'field': field,
                'size': size
            }
        }
    }
    return agg


def build_sort(field_format, lst_id=None):
    desc = field_format.split('__')[-1]
    field = field_format.split('__')[0]
    # print(field_format.split('__'))
    if desc == 'desc':
        order = 'desc'
    else:
        order = 'asc'

    if field != 'google_queries.pos':
        sort = {
            field: {
                'order': order
            }
        }
    elif lst_id is not None and len(lst_id) > 0:
        sort = {
            field: {
                'order': order,
                'nested': {
                    'path': 'google_queries',
                    'filter': {
                        'terms': {
                            'google_queries.id.keyword': lst_id
                        }
                    }
                }
            }
        }
    else:
        sort = None
    return sort


def query_es_multiple_parts(es, index_name, query, part_num=5):
    lst_aggs = query.get('aggs') or query.get('aggregations')
    query_es_parts = []
    part_num = min(part_num, len(lst_aggs))
    for i in range(part_num):
        _agg = {
            'aggs': {}
        }
        for index, agg in enumerate(lst_aggs):
            start = int(i * len(lst_aggs) / part_num)
            end = int((i + 1) * len(lst_aggs) / part_num)
            if start <= index < end:
                _agg['aggs'][agg] = lst_aggs[agg]
        query_es_parts.append(_agg)

    result_es = {}
    result_lst_product = []
    del query['aggs']
    for i in range(part_num):
        query_es_part = query_es_parts[i]
        query_es_part['size'] = query.get('size') if i == 0 else 0
        # logger.info(
        #     f'query_es_multiple_parts: {i} {len(query_es_parts[i].get("aggs"))} {json.dumps({**query, "aggregations": query_es_parts[i].get("aggs"), "size": query_es_parts[i].get("size")})}')

        lst_product, aggs_ = search_v2(
            es=es,
            index_name=index_name,
            query={
                **query,
                'aggregations': query_es_parts[i].get('aggs'),
                'size': query_es_parts[i].get('size')
            }
        )
        # logger.info({json.dumps(aggs_.get("aggregations"))})
        # logger.info(f'done-query_es_multiple_parts: {i} {list(aggs_.get("aggregations").keys())}')
        result_lst_product += lst_product
        result_es = {
            **result_es,
            **aggs_,
            'aggregations': {
                **result_es.get('aggregations', {}),
                **aggs_.get('aggregations', {})
            }
        }
    return result_lst_product, result_es


if __name__ == '__main__':
    es_config = ['http://elastic:Elast1cS34ch@127.0.0.1:9201', 'http://elastic:Elast1cS34ch@127.0.0.1:9202',
                 'http://elastic:Elast1cS34ch@127.0.0.1:9203']
    es_session = Elasticsearch(es_config, verify_certs=False, timeout=30)
    # insert_docs_in_bulk(es=es_session, index='my-index-000001', id_fields='id',
    #                     verbose=False, data_bulk=[
    #         {
    #             'id': '001',
    #             'name': 'phuongpv'
    #         }, {
    #             'id': '002',
    #             'name': 'tuantmtb'
    #         }, {
    #             'id': '003',
    #             'name': 'tungha'
    #         }, {
    #             'id': '004',
    #             'name': 'linh'
    #         }
    #     ], action_type='index')
    import time

    for index in range(0, 100):
        start_time = time.time()
        result = search(es=es_session, index_name='my-index-000001', query={
            "query": {
                "match": {
                    "id": {
                        "query": "001"
                    }
                }
            }
        })
        end_time = time.time()
        print(index, end_time - start_time, result)
