from elasticsearch import AsyncElasticsearch

from app.core.config import settings


def get_es_ereport_session(request_timeout=30) -> AsyncElasticsearch:
    es_session = AsyncElasticsearch(
        hosts=[settings.ES_URI_EREPORT],
        verify_certs=False,
        request_timeout=request_timeout,
    )

    # set content type es_session
    # es_session.transport.connection_pool.connection_class.headers.update({'Content-Type': 'application/json'})
    return es_session
