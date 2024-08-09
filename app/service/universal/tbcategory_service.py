import json
from typing import List, Union

from app.db.session_metric import session_metric_ctx
from app.models.models_metricdb import TbCategory
from helper.array_helper import get_uniq_by_key
from app.schemas.metric.category_base import Category
from data.constant_metric import get_map_category_obj, LST_CATEGORY_ALL
from sqlalchemy import select

from helper.logger_helper import LoggerSimple
from helper.string_helper import slugify

logger = LoggerSimple(name=__name__).logger


def get_all_tb_category(session_metric):
    statement = select([TbCategory.id, TbCategory.name, TbCategory.parent_id, TbCategory.cn_name, TbCategory.cat_level])
    lst_tb_category_tuple = session_metric.execute(statement).fetchall()
    map_category_obj = {}
    for category_tuple in lst_tb_category_tuple:
        id, name, parent_id, cn_name, cat_level = category_tuple
        map_category_obj[id] = {
            'id': id,
            'name': name,
            'parent_id': parent_id,
            'cn_name': cn_name,
            'cat_level': cat_level,
        }
    # logger.info(map_category_obj)
    return map_category_obj


def get_breadcrumb_tb(session_metric, tb_category_id: Union[str, int], ignore_category_redundant=False) -> Union[
    List, None]:
    map_category_obj = get_all_tb_category(session_metric=session_metric)
    # logger.info(map_category_obj)
    lst_category_base = []
    cat_id = tb_category_id
    cat_obj = map_category_obj.get(cat_id)
    if cat_obj:
        cat = {
            'id': cat_obj.get('id'),
            'name': cat_obj.get('name'),
            'slug': slugify(cat_obj.get('name')),
        }
        lst_category_base.append(cat)
        while map_category_obj.get(cat_id).get('parent_id') is not None:
            cat_id = map_category_obj.get(cat_id).get('parent_id')
            cat_obj = map_category_obj.get(cat_id)
            if cat_obj:
                # cat = Category(
                #     id=cat_obj.get('value'),
                #     name=cat_obj.get('label'),
                #     level=cat_obj.get('level'),
                #     # parent_id=cat_obj.get('parent'),
                #     # parent_name=cat_obj.get('parent_name'),
                #     is_leaf=cat_obj.get('is_leaf'),
                # )
                cat = {
                    'id': cat_obj.get('id'),
                    'name': cat_obj.get('name'),
                    'slug': slugify(cat_obj.get('name')),
                }
                lst_category_base.append(cat)
    lst_category_base = lst_category_base[::-1]
    if len(lst_category_base) == 0:
        return None
    if ignore_category_redundant:
        for cat_obj in lst_category_base:
            cat_obj_raw = map_category_obj.get(cat_obj.get('id'))
            if cat_obj_raw.get('cn_name') == '猜你喜欢':
                lst_category_base.remove(cat_obj)
    return lst_category_base


def get_all_child_category(session_metric, tb_category_id: Union[str, int], lst_child: List = None):
    """
    Đệ quy để lấy all child của category
    """
    if tb_category_id is None:
        return None
    map_category_obj = get_all_tb_category(session_metric=session_metric)
    lst_category = []
    for cat_id in map_category_obj:
        lst_category.append(map_category_obj[cat_id])
    if lst_child is None:
        cat = map_category_obj.get(tb_category_id)
        if cat:
            lst_child = [cat]
    # logger.info(lst_category)
    lst_child_l1 = [cat for cat in lst_category if cat.get('parent_id') == tb_category_id]
    if lst_child_l1 and len(lst_child_l1) > 0:
        # lst_child.extend(lst_child_l1)
        for child in lst_child_l1:
            _lst_child = get_all_child_category(session_metric=session_metric, tb_category_id=child.get('id'),
                                                lst_child=None)
            if _lst_child and len(_lst_child) > 0:
                lst_child.extend(_lst_child)
    return get_uniq_by_key(array_dict=lst_child, key='id')


def get_all_child_and_self_category(session_metric, lst_tb_category_id: List[Union[str, int]]):
    if lst_tb_category_id is None:
        return None
    lst_category_child = []
    for tb_category_id in lst_tb_category_id:
        lst_child = get_all_child_category(session_metric=session_metric, tb_category_id=tb_category_id)
        if lst_child and len(lst_child) > 0:
            lst_category_child.extend(lst_child)
    lst_category_child = get_uniq_by_key(array_dict=lst_category_child, key='id')
    return lst_category_child


def get_tb_category_tree(session_metric):
    map_tb_category = get_all_tb_category(session_metric=session_metric)
    lst_tb_category = []
    tb_category_tree = []
    for cat_id in map_tb_category:
        cat = map_tb_category[cat_id]
        lst_tb_category.append(cat)

    for cat in lst_tb_category:
        if cat.get('cat_level') == 0:
            cat0 = cat
            if 'children' not in cat0:
                cat0['children'] = []
            for cat1 in lst_tb_category:
                if cat1.get('parent_id') == cat0.get('id'):
                    if 'children' not in cat1:
                        cat1['children'] = []
                    for cat2 in lst_tb_category:
                        if cat2.get('parent_id') == cat1.get('id'):
                            if 'children' not in cat1:
                                cat1['children'] = []
                            cat1['children'].append(cat2)
                    cat0['children'].append(cat1)
            tb_category_tree.append(cat0)
    # logger.info(json.dumps(tb_category_tree, ensure_ascii=False))
    return tb_category_tree


if __name__ == '__main__':
    with session_metric_ctx() as session_metric:
        get_all_tb_category(session_metric=session_metric)
        # breadcrumb = get_breadcrumb_tb(session_metric=session_metric, tb_category_id=1641)
        # lst_child = get_all_child_category(session_metric=session_metric, tb_category_id=22)
        # lst_child = get_all_child_category(session_metric=session_metric, tb_category_id=219)
        # lst_child = get_all_child_category(session_metric=session_metric, tb_category_id=1599)
        # lst_child = get_all_child_and_self_category(session_metric=session_metric, lst_tb_category_id=[219, 22])
        # logger.info(lst_child)
        # logger.info(len(lst_child))
        tb_category_tree = get_tb_category_tree(session_metric=session_metric)
        logger.info(tb_category_tree)
        # logger.info(breadcrumb)
    # lst_child = get_all_child_and_self_category(lst_tb_category_id=['1__100630', '1__100708'])
    #
    # for child in lst_child:
    #     print(f"{child.get('label')} - {child.get('level')} - {child.get('parent_name')} - {child.get('is_leaf')}")
    #
    # print(len(lst_child))
