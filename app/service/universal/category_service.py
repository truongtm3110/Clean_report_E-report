from typing import List, Union

from app.constant.constant_metric import get_map_category_obj, LST_CATEGORY_ALL
from helper.array_helper import get_uniq_by_key
from app.models.models_ereport import ReportCategory


def get_breadcrumb(category_base_id: str) -> Union[List[ReportCategory], None]:
    map_category_obj = get_map_category_obj()
    lst_category_base: List[ReportCategory] = []
    cat_id = category_base_id
    cat_obj = map_category_obj.get(cat_id)
    if cat_obj:
        cat = ReportCategory(
            id=cat_obj.get('value'),
            name=cat_obj.get('label'),
            level=cat_obj.get('level'),
            # parent_id=cat_obj.get('parent'),
            # parent_name=cat_obj.get('parent_name'),
            is_leaf=cat_obj.get('is_leaf'),
        )
        lst_category_base.append(cat)
        while map_category_obj.get(cat_id).get('parent') is not None:
            cat_id = map_category_obj.get(cat_id).get('parent')
            cat_obj = map_category_obj.get(cat_id)
            if cat_obj:
                cat = ReportCategory(
                    id=cat_obj.get('value'),
                    name=cat_obj.get('label'),
                    level=cat_obj.get('level'),
                    # parent_id=cat_obj.get('parent'),
                    # parent_name=cat_obj.get('parent_name'),
                    is_leaf=cat_obj.get('is_leaf'),
                )
                lst_category_base.append(cat)
    lst_category_base = lst_category_base[::-1]
    if len(lst_category_base) == 0:
        return None
    return lst_category_base


def get_all_child_category(category_base_id: str, lst_child: List = None):
    """
    Đệ quy để lấy all child của category
    """
    if category_base_id is None:
        return None
    if lst_child is None:
        map_category_obj = get_map_category_obj()
        cat = map_category_obj.get(category_base_id)
        if cat:
            lst_child = [cat]
    lst_child_l1 = [cat for cat in LST_CATEGORY_ALL if cat.get('parent') == category_base_id]
    if lst_child_l1 and len(lst_child_l1) > 0:
        # lst_child.extend(lst_child_l1)
        for child in lst_child_l1:
            _lst_child = get_all_child_category(child.get('value'), None)
            if _lst_child and len(_lst_child) > 0:
                lst_child.extend(_lst_child)
    return get_uniq_by_key(array_dict=lst_child, key='value')


def get_all_child_and_self_category(lst_category_base_id: List[str]):
    if lst_category_base_id is None:
        return None
    lst_category_child = []
    for category_base_id in lst_category_base_id:
        lst_child = get_all_child_category(category_base_id=category_base_id)
        if lst_child and len(lst_child) > 0:
            lst_category_child.extend(lst_child)
    lst_category_child = get_uniq_by_key(array_dict=lst_category_child, key='value')
    return lst_category_child


if __name__ == '__main__':
    # print(get_breadcrumb(category_base_id='c285392990'))
    # lst_child = get_all_child_category(category_base_id='1__100636')
    # print(lst_child)
    # lst_category_base_id=['1__100630', '1__100001', '2__4405', '2__10100869', '3__1520', '4__220'])
    lst_category_id = [child.get('value') for child in
                       get_all_child_and_self_category(lst_category_base_id=['1__100636'])]
    print(lst_category_id)
    # for child in lst_child:
    #     print(f"{child.get('label')} - {child.get('level')} - {child.get('parent_name')} - {child.get('is_leaf')}")
    # print(f"{child.get('value')}")
    #
    # print(len(lst_child))
