from elasticsearch import Elasticsearch

from app.core.config_metricseo import settings_seo


def get_es_metric_session(request_timeout=30) -> Elasticsearch:
    # es_config = [get_value_setting(key='ES_URI_METRIC')]
    es_config = [settings_seo.ES_URI_METRIC]
    es_session = Elasticsearch(es_config, verify_certs=False, request_timeout=request_timeout, ssl_show_warn=False)
    return es_session
