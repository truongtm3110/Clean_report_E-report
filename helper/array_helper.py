import html
import random

from helper.error_helper import log_error


def merge_array(arr1, arr2, key):
    if arr1 is None and arr2 is not None:
        return arr2
    if arr2 is None and arr1 is not None:
        return arr1
    set_id = set()
    lst_arr = []
    for item in arr1:
        if item.get(key) not in set_id:
            set_id.add(item.get(key))
            lst_arr.append(item)
    for item in arr2:
        if item.get(key) not in set_id:
            set_id.add(item.get(key))
            lst_arr.append(item)
    return lst_arr


def get_sublists(original_list, number_of_sub_list_wanted):
    sublists = list()
    # for sub_list_count in range(number_of_sub_list_wanted):
    #     sublists.append(original_list[sub_list_count::number_of_sub_list_wanted])
    # return sublists

    element_size_per_bulk = int(len(original_list) / number_of_sub_list_wanted)
    for i in range(0, len(original_list), element_size_per_bulk):
        # Create an index range for l of n items:
        sublists.append(original_list[i:i + element_size_per_bulk])
    return sublists


# if __name__ == '__main__':
#     print(get_sublists([100, 200, 300, 101, 102, 103, 111, 222, 333, 888], 999))

def chunks(s, n):
    """Produce `n`-character chunks from `s`."""
    for start in range(0, len(s), n):
        yield s[start:start + n]


def sort_by_value(array_dict, key, desc=True):
    if array_dict is None or len(array_dict) == 0:
        return array_dict
    from pydash import order_by
    return order_by(collection=array_dict, keys=[key], reverse=desc)


def get_uniq_by_key(array_dict, key):
    values_unq = set()
    output = []
    for arr in array_dict:
        value_in_key = arr.get(key)
        if value_in_key not in values_unq:
            output.append(arr)
            values_unq.add(value_in_key)
    return output


def array_to_map(array_dict, key):
    output = {}
    for item in array_dict:
        output[item[key]] = item
    return output

def group_by_key(array_dict, key):
    if key is None:
        return array_dict
    lst_element_by_key = {}
    output = []
    for element in array_dict:
        if element.get(key) is not None:
            value = element.get(key)
            if lst_element_by_key.get(value) is None:
                lst_element_by_key[value] = []
            lst_element_by_key[value].append(element)
    for item_value in lst_element_by_key:
        output.append(lst_element_by_key.get(item_value))
    return output


def group_by_key_count(array_dict, key):
    lst_element_by_key = {}
    output = {}
    for element in array_dict:
        # if element.get(key) is not None:
        value = element.get(key)
        if lst_element_by_key.get(value) is None:
            lst_element_by_key[value] = []
        lst_element_by_key[value].append(element)
    for item_value in lst_element_by_key:
        # output.append(lst_element_by_key.get(item_value))
        output[lst_element_by_key.get(item_value)[0].get(key)] = len(lst_element_by_key.get(item_value))
    return output


def group_by_key_return_list(array_dict, key, id):
    lst_element_by_key = {}
    for element in array_dict:
        if element.get(key) is not None:
            value = element.get(key)
            if lst_element_by_key.get(value) is None:
                lst_element_by_key[value] = []
            lst_element_by_key[value].append(element.get(id))
    return lst_element_by_key


def group_by_key_return_list_obj(array_dict, key):
    lst_element_by_key = {}
    for element in array_dict:
        if element.get(key) is not None:
            value = element.get(key)
            value = value.replace('  ', ' ')
            if lst_element_by_key.get(value) is None:
                lst_element_by_key[value] = []
            lst_element_by_key[value].append(element)
    return lst_element_by_key


def get_item_by_value_in_key(array_dict, key, value):
    if array_dict is None or type(array_dict) is not list or len(array_dict) == 0:
        return None
    try:
        for item in array_dict:
            if html.unescape((str(item.get(key)))) == str(value):
                return item
    except Exception as e:
        log_error(e)
        print(f'error: {key}: {value} in {array_dict}')
    return None


def load_dict_key_by_target(lst_obj, key, target):
    dict_map = {}
    for root in lst_obj:
        key_value = root.get(key)
        if key_value is None:
            continue
        value = root.get(target)
        if value is None:
            continue
        if key_value not in dict_map.keys():
            dict_map[key_value] = set()

        dict_map[key_value].add(value)

    return dict_map


def load_dict_key_by_target_return_obj(lst_obj, key='product_comparable_root_id'):
    dict_map = {}
    for root in lst_obj:
        key_value = root.get(key)
        if key_value is None:
            continue
        if key_value not in dict_map.keys():
            dict_map[key_value] = []

        dict_map[key_value].append(root)
    return dict_map


def load_dict_key_by_target_array(array, key='product_comparable_root_id', target='id'):
    dict_map = {}
    no = 0
    for root in array:
        no += 1
        key_value = root.get(key)
        if key_value is None:
            continue
        value = root.get(target)
        if value is None:
            continue
        if key_value not in dict_map.keys():
            dict_map[key_value] = set()

        dict_map[key_value].add(value)

    return dict_map


def shuffle_array(arr):
    random.shuffle(arr)
    return arr


def is_have_element(arr):
    if arr is not None and type(arr) is list and len(arr) > 0:
        return True
    return False


def append_if_not_null(set_input: set, element):
    if element:
        if type(element) is not list:
            # print(type(element))
            set_input.add(element)
        else:
            set_input.update(element)
    return set_input


if __name__ == '__main__':
    arr = [{'a': 1}, {'b': 2}, {'c': 1}]
    key = 'a'
    # print(get_uniq_by_key(array_dict=arr, key=key))
    # print(group_by_key(array_dict=arr, key=key))

    # random.shuffle(arr)
    print(arr)
    print(shuffle_array(arr))
    print(random.choice(arr))
    arr = [
        {'id': 1, 'count': 10},
        {'id': 2, 'count': 12},
        {'id': 2, 'count': 2}
    ]
    # print(sort_by_value(array_dict=arr, key='count', desc=True))
