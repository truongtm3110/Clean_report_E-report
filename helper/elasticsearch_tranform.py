def get_es_value_agg(agg):
    if agg is not None and agg.get('value'):
        return agg.get('value')
    return None
