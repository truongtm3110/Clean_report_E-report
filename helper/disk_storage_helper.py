import struct
import time
from enum import Enum
from helper.logger_helper import LoggerSimple

logger = LoggerSimple(name=__name__).logger


class TypeStorage(str, Enum):
    int = 'int'
    long = 'long'
    str = 'str'
    ints = 'ints'
    longs = 'longs'
    double = 'double'
    byte = 'byte'
    version_element = 'version_element'
    size_element = 'size_element'


def read_bytes_to_type(f, type: TypeStorage = None):
    if type == TypeStorage.size_element:
        return int.from_bytes(f.read(4), 'big', signed=True)

    if type == TypeStorage.version_element:
        return int.from_bytes(f.read(1), 'little')

    if type == TypeStorage.int:
        return int.from_bytes(f.read(4), 'big', signed=True)

    if type == TypeStorage.long:
        return int.from_bytes(f.read(8), 'big', signed=True)

    if type == TypeStorage.byte:
        return int.from_bytes(f.read(1), 'little', signed=True)

    if type == TypeStorage.double:
        return struct.unpack('d', f.read(8))[0]

    if type == TypeStorage.str:
        length_byte = f.read(4)
        length = int.from_bytes(length_byte, 'big', signed=True)
        if length > 0:
            return f.read(length).decode('utf-8')
        if length == -1:
            return None
        return ''

    if type == TypeStorage.ints:
        length = int.from_bytes(f.read(4), 'big', signed=True)
        lst_int = []
        if length > 0:
            for _ in range(length):
                lst_int.append(
                    int.from_bytes(f.read(4), 'big', signed=True)
                )
        elif length == -1:
            return None
        return lst_int

    if type == TypeStorage.longs:
        length = int.from_bytes(f.read(4), 'big', signed=True)
        lst_int = []
        if length > 0:
            for _ in range(length):
                lst_int.append(
                    int.from_bytes(f.read(8), 'big', signed=True)
                )
        elif length == -1:
            return None
        return lst_int


def fetch_binary_data(file_storage_binary_path: str, method_read_element, debug=True):
    start_at = time.time()
    with open(file_storage_binary_path, "rb") as f:
        size_element = read_bytes_to_type(f, TypeStorage.size_element)
        if debug:
            logger.info(f'size_element={size_element}; file_path={file_storage_binary_path}')
        for idx, _ in enumerate(range(size_element)):
            # order_history = read_element_order_history(f, debug=False)
            order_history = method_read_element(f=f)
            if debug and (idx + 1) % 500_000 == 0:
                logger.info(f'read {idx} in {(time.time() - start_at) * 1000} ms')
                start_at = time.time()
            yield order_history


def fetch_binary_data_manual(file_storage_binary_path: str, method_read_element, f, debug=True):
    start_at = time.time()
    # with open(file_storage_binary_path, "rb") as f:
    # size_element = read_bytes_to_type(f, TypeStorage.size_element)
    # if debug:
    #     logger.info(f'size_element={size_element}; file_path={file_storage_binary_path}')
    # for idx, _ in enumerate(range(size_element)):
    # order_history = read_element_order_history(f, debug=False)
    order_history = method_read_element(f=f)
    return order_history
