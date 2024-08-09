#   Copyright (c) 2019 BeeCost Team <beecost.com@gmail.com>. All Rights Reserved
#   BeeCost Project can not be copied and/or distributed without the express permission of @tuantmtb
import base64
import os
import random
import re
import itertools


def generate_random_code(len=6):
    length_string = 8
    random_string = base64.urlsafe_b64encode(os.urandom(length_string)).decode()
    random_string_normalize = re.sub(r"[^A-Za-z0-9]+", '', random_string)
    code = random_string_normalize[: len].upper()
    return code


def get_random_in_array(lst_arr):
    return random.choice(lst_arr)


def random_many_array(lst_arr, distribute=[0.2, 0.5, 0.3]):
    output = []
    many_part_output = []
    if lst_arr is not None and len(lst_arr) > 0:
        for arr in lst_arr:
            if arr is None:
                continue
            # print('----')
            # print('sub: ', arr)
            l = list(range(0, len(arr)))
            lst_index_split = splitPerc(l=l, distribute=distribute)
            # print('should split idx', lst_index_split)
            for idx, bucket in enumerate(lst_index_split):
                # print('idx', idx, ' - ', bucket)
                if len(many_part_output) - 1 < idx:
                    # print('add new array', many_part_output)
                    many_part_output.append([])
                if len(bucket) >= 2:
                    # print('add', arr[bucket[0]:bucket[-1] + 1])
                    many_part_output[idx] += arr[bucket[0]:bucket[-1] + 1]
                elif len(bucket) == 1:
                    # print('add 1 e: ', [arr[bucket[0]]])
                    many_part_output[idx] += [arr[bucket[0]]]
    # random
    # print('many_part_output:', many_part_output)
    for part in many_part_output:
        random.shuffle(part)
    # print('many_part_output:', many_part_output)
    output = list(itertools.chain(*many_part_output))
    # print(output)
    return output


def splitPerc(l, distribute):
    import numpy as np
    splits = np.cumsum(distribute)

    # if splits[-1] != 1:
    #     raise ValueError("percents don't add up to 100")

    splits = splits[:-1]

    splits *= len(l)

    splits = splits.round().astype(np.int)

    return np.split(l, splits)


if __name__ == '__main__':
    # print(generate_random_code())
    lst_arr = [[10, 11, 12, 13, 14, 15, 16, 17, 18, 19], [20, 21, 22, 23, 24, 25, 26, 27, 28, 29],
               [30, 31, 32, 33, 34, 35, 36, 37, 38, 39], []]

    # lst_arr = []
    # lst_arr = [[], [], []]
    # random_many_array(lst_arr=lst_arr, distribute=[0.7, 0.2, 0.05, 0.33, 0.12, 0.1, 0.2])
    output = random_many_array(lst_arr=lst_arr, distribute=[0.2, 0.3, 0.5])
    print(output)

    # list = np.arange(20)
    # percents = np.array([0.25, 0.10, 0.15, 0.05, 0.05, 0.05, 0.10, 0.25])
    # print(splitPerc(list, percents))
