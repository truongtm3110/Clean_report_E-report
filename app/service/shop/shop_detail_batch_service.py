from fastapi import Depends

# from app.db.session_metric import async_session_metric
from helper.alchemy_helper import select_by_query
from helper.logger_helper import LoggerSimple

logger = LoggerSimple(name=__name__).logger


# async def get_shop_detail_batch(lst_shop_base_id, session_metric=Depends(async_session_metric())):
#     lst_shop_base = []
#     if lst_shop_base_id and len(lst_shop_base_id) > 0:
#         query_in = f','.join([f"'{r}'" for r in lst_shop_base_id])
#         query_sql = f"select shop_base_id, username, name as name, url_image, is_official_shop from shop_base where shop_base_id in ({query_in})"
#         lst_shop_base = await select_by_query(query=query_sql, connection=session_metric)
#     return lst_shop_base
#
#
# def get_shop_detail_full_batch(lst_shop_base_id, session_metric=Depends(async_session_metric())):
#     lst_shop_base = []
#     if lst_shop_base_id and len(lst_shop_base_id) > 0:
#         query_in = f','.join([f"'{r}'" for r in lst_shop_base_id])
#         query_sql = f"select shop_base_id, username, name as name, name as name_raw, url_image, is_official_shop, rating_bad, rating_good, rating_normal, rating_avg from shop_base where shop_base_id in ({query_in})"
#         lst_shop_base = select_by_query(query=query_sql, connection=session_metric)
#     return lst_shop_base
#
#
# if __name__ == '__main__':
#     lst_shop_base_id = ['1__338323498', '1__38308826']
#     with async_session_metric() as session_metric:
#         lst_shop_base = get_shop_detail_batch(lst_shop_base_id=lst_shop_base_id, session_metric=session_metric)
#         logger.info(lst_shop_base)
