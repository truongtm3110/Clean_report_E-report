import gzip
import os
import struct
from datetime import datetime
from typing import Union, List

import bson
import fasteners
from bson import InvalidBSON, CodecOptions

BSON_SPLIT = b"\x80\xFF"


def store_bson_objs_to_file(bson_objs, file_output_path, is_append=False):
    if bson_objs is None or len(bson_objs) == 0:
        return
    os.makedirs(os.path.dirname(file_output_path), exist_ok=True)
    with fasteners.InterProcessLock(file_output_path):
        if is_append:
            with gzip.open(file_output_path, 'ab') as f:
                for bson_obj in bson_objs:
                    f.write(bson.encode(bson_obj) + BSON_SPLIT)
        else:
            with gzip.open(file_output_path, 'wb') as f:
                for bson_obj in bson_objs:
                    f.write(bson.encode(bson_obj) + BSON_SPLIT)


def load_obj_bson_from_gz_hp(file_gz_path, buf_size=1024 * 1024 * 8):
    data_obj = bytes()
    with gzip.open(filename=file_gz_path, mode='rb') as file_obj:
        while True:
            buf_data = file_obj.read(buf_size)
            if not buf_data:
                break

            if buf_data.find(BSON_SPLIT):
                split_buf = buf_data.split(BSON_SPLIT)
                split_buf_len = len(split_buf)
                for index, split_data in enumerate(split_buf):
                    data_obj += split_data
                    if index != split_buf_len - 1:
                        if len(data_obj) > 0:
                            try:
                                yield bson.decode(data_obj)
                            except:
                                print(data_obj)
                            data_obj = bytes()
            else:
                data_obj += buf_data
        if len(data_obj) > 0:
            yield bson.decode(data_obj)


def load_obj_bson_from_gz_hp_batch(file_gz_path, batch_size=1000):
    lst_data = []
    with gzip.open(filename=file_gz_path, mode='rb') as file_obj:
        while True:
            # Read size of next object.
            size_data = file_obj.read(4)
            if not size_data:
                break  # Finished with file normaly.
            elif len(size_data) != 4:
                raise InvalidBSON("cut off in middle of objsize")
            obj_size = struct.Struct("<i").unpack_from(size_data, 0)[0] - 4
            data_obj = size_data + file_obj.read(max(0, obj_size))
            lst_data.append(data_obj)
            if len(lst_data) >= batch_size:
                lst_obj_batch = [bson.decode(data_obj)]
                lst_data.clear()
                yield lst_obj_batch
    if len(lst_data) > 0:
        lst_obj_batch = [bson.decode(data_obj) for data_obj in lst_data]
        lst_data.clear()
        yield lst_obj_batch


if __name__ == '__main__':
    start_time = datetime.now()
    for lst_batch in load_obj_bson_from_gz_hp_batch(
            file_gz_path="/storage/data/temp/test-performance/shop-products-bson.gz",
            batch_size=10_000):
        print(f"Loaded batch: {len(lst_batch)}, {lst_batch[0]}")
    print(f"time execute: {datetime.now() - start_time}")
