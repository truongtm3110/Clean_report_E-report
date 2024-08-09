import csv
import gzip
import json
import os
import pathlib
import subprocess
from glob import glob

import fasteners
import orjson

from helper.error_helper import log_error


def get_content_by_gz(file_path):
    with gzip.open(file_path, 'rt') as f:
        file_content = f.read()
    return file_content


def get_obj_by_gz(file_path):
    try:
        if not os.path.isfile(file_path):
            return None
        with gzip.open(file_path, 'rt') as f:
            file_content = f.read()
        return json.loads(file_content)
    except Exception as e:
        log_error(e)
    return None


def get_content(file_path):
    with open(file_path) as f:
        s = f.read()
    return s


def get_files_in_folder(folder_path):
    files_absolute_path = []
    files_name = None
    for root, dirs, files in os.walk(os.path.abspath(folder_path)):
        files_name = files
        for file in files:
            if '.DS_Store' not in file:
                files_absolute_path.append(os.path.join(root, file))
    return files_absolute_path, files_name


def get_file_in_folder_hp(folder_path):
    for root, dirs, files in os.walk(os.path.abspath(folder_path)):
        for file in files:
            if '.DS_Store' not in file:
                yield os.path.join(root, file)


def get_files_absolute_in_folder(folder_path):
    files_absolute_path, files_name = get_files_in_folder(folder_path)
    return sorted(files_absolute_path)


def load_jsonl_from_gz(file_gz_path, min_length_per_line=5):
    output_objs = []
    for text in get_content_by_gz(file_gz_path).split('\n'):
        try:
            if len(text) >= min_length_per_line:
                obj = json.loads(text)
                output_objs.append(obj)
        except Exception as e:
            print(e)
    return output_objs


def load_jsonl_from_gz_hp(file_gz_path, min_length_per_line=5):
    try:
        with gzip.open(filename=file_gz_path, mode='rt') as f:
            for line in f:
                if len(line) > min_length_per_line:
                    yield line
    except Exception as e:
        log_error(e)


def load_line_from_gz_hp(file_gz_path):
    try:
        with gzip.open(filename=file_gz_path, mode='rt') as f:
            for line in f:
                yield line.strip("\n")
    except Exception as e:
        log_error(e)


def load_line_from_text_hp(file_gz_path):
    try:
        with open(file=file_gz_path, mode='rt') as f:
            for line in f:
                yield line.strip("\n")
    except Exception as e:
        log_error(e)


def load_obj_jsonl_from_gz_hp(file_gz_path, min_length_per_line=5):
    try:
        with gzip.open(filename=file_gz_path, mode='rt') as f:
            for line in f:
                if len(line) > min_length_per_line:
                    yield json.loads(line)
    except Exception as e:
        log_error(e)


def load_obj_jsonl_from_gz_hp_by_orjson(file_gz_path, min_length_per_line=5):
    with gzip.open(filename=file_gz_path, mode='rt') as f:
        for line in f:
            if len(line) > min_length_per_line:
                try:
                    yield orjson.loads(line)
                except orjson.JSONDecodeError:
                    continue


def load_jsonl_from_gz_batch_hp(file_gz_path, min_length_per_line=5, batch_size=10000):
    no = 0
    array = []
    with gzip.open(filename=file_gz_path, mode='rt') as f:
        for line in f:
            if len(line) > min_length_per_line:
                array.append(orjson.loads(line))
                no += 1
                if no % batch_size == 0:
                    yield array
                    array = []
    if len(array) > 0:
        yield array


def load_jsonl_from_text_batch_hp(file_gz_path, min_length_per_line=5, batch_size=10000):
    no = 0
    array = []
    with open(file=file_gz_path, mode='rt') as f:
        for line in f:
            if len(line) > min_length_per_line:
                try:
                    # print(json.loads(line))
                    array.append(json.loads(line))
                    no += 1
                    if no % batch_size == 0:
                        yield array
                        array = []
                except Exception as e:
                    pass
    if len(array) > 0:
        yield array


def load_gz_batch_hp(file_gz_path, min_length_per_line=5, batch_size=10000):
    no = 0
    array = []
    with gzip.open(filename=file_gz_path, mode='rt') as f:
        for line in f:
            if len(line) > min_length_per_line:
                array.append(line.split('\n')[0])
                no += 1
                if no % batch_size == 0:
                    yield array
                    array = []
    if len(array) > 0:
        yield array


def load_text_batch_hp(file_path, min_length_per_line=5, batch_size=10000):
    no = 0
    array = []
    with open(file=file_path, mode='rt') as f:
        for line in f:
            if len(line) > min_length_per_line:
                array.append(line.split('\n')[0])
                no += 1
                if no % batch_size == 0:
                    yield array
                    array = []
    if len(array) > 0:
        yield array


def load_timestamp_format_jsonl_from_gz(file_gz_path, data_space_no=1, min_length_per_line=5):
    """

    :param file_gz_path:
    :param data_space_no: thứ tự dấu cách chưa data VD: timestamp datajson
    :param min_length_per_line:
    :return:
    """
    output_objs = []
    for text in get_content_by_gz(file_gz_path).split('\n'):
        try:
            if text is None or text == '':
                return []
            text_data = ' '.join(text.split(' ')[data_space_no:])
            # print(text_data)
            if len(text_data) >= min_length_per_line:
                obj = json.loads(text_data)
                output_objs.append(obj)
        except Exception as e:
            print(e)
    return output_objs


def get_sub_folders_in_folder(folder_path):
    sub_folders = glob(folder_path + "/*")
    if sub_folders is not None and len(sub_folders) > 0:
        return sorted(sub_folders)
    return sub_folders


def create_folder_if_not_exist(folder_path):
    pathlib.Path(folder_path).mkdir(parents=True, exist_ok=True)


def get_sub_folders_and_file_name_in_folder(folder_path):
    sub_folders = glob(folder_path + "/*/")
    files_name = [file_path.split('/')[-2] for file_path in sub_folders]
    return sub_folders, files_name


def load_json(file_path):
    with open(file_path) as f:
        data = json.load(f)
    return data


def store_json(object, file_output_path):
    os.makedirs(os.path.dirname(file_output_path), exist_ok=True)
    with fasteners.InterProcessLock(file_output_path):
        with open(file_output_path, 'w') as fp:
            json.dump(object, fp, ensure_ascii=False, sort_keys=True, indent=1)


def store_file(content, file_output_path, is_append=False):
    os.makedirs(os.path.dirname(file_output_path), exist_ok=True)
    with fasteners.InterProcessLock(file_output_path):
        if is_append:
            with open(file_output_path, 'a') as fh:
                fh.write(str(content))
        else:
            with open(file_output_path, 'w+') as fh:
                fh.write(str(content))


def store_gz(content, file_output_path, is_append=False):
    os.makedirs(os.path.dirname(file_output_path), exist_ok=True)
    with fasteners.InterProcessLock(file_output_path):
        if is_append:
            with gzip.open(file_output_path, 'ab') as f:
                f.write(content.encode('utf-8'))
        else:
            with gzip.open(file_output_path, 'wb') as f:
                f.write(content.encode('utf-8'))


def store_obj_gz(obj, file_output_path, is_append=False):
    os.makedirs(os.path.dirname(file_output_path), exist_ok=True)
    with fasteners.InterProcessLock(file_output_path):
        content = json.dumps(obj, ensure_ascii=False)
        if is_append:
            with gzip.open(file_output_path, 'ab') as f:
                f.write(content.encode('utf-8'))
        else:
            with gzip.open(file_output_path, 'wb') as f:
                f.write(content.encode('utf-8'))


def store_lines_perline_in_file(lines, file_output_path, is_append=False):
    if lines is None or len(lines) == 0:
        return
    os.makedirs(os.path.dirname(file_output_path), exist_ok=True)
    with fasteners.InterProcessLock(file_output_path):
        if is_append:
            with gzip.open(file_output_path, 'ab') as f:
                for idx, line in enumerate(lines):
                    if idx == 0:
                        if len(lines) == 1:
                            f.write(('\n' + line).encode('utf-8'))
                        else:
                            f.write(('\n' + line).encode('utf-8'))
                    else:
                        f.write(('\n' + line).encode('utf-8'))
        else:
            with gzip.open(file_output_path, 'wb') as f:
                for idx, line in enumerate(lines):
                    if idx == 0:
                        f.write(line.encode('utf-8'))
                    else:
                        f.write(('\n' + line).encode('utf-8'))


def store_jsons_perline_in_file(jsons_obj, file_output_path, is_append=False):
    if jsons_obj is None or len(jsons_obj) == 0:
        return
    os.makedirs(os.path.dirname(file_output_path), exist_ok=True)
    is_exits = os.path.exists(file_output_path)
    with fasteners.InterProcessLock(file_output_path):
        if is_append:
            with gzip.open(file_output_path, 'ab') as f:
                for idx, json_obj in enumerate(jsons_obj):
                    try:
                        if idx == 0:
                            if is_exits:
                                f.write(('\n' + json.dumps(json_obj, ensure_ascii=False)).encode('utf-8'))
                            else:
                                f.write((json.dumps(json_obj, ensure_ascii=False)).encode('utf-8'))
                        else:
                            f.write(('\n' + json.dumps(json_obj, ensure_ascii=False)).encode('utf-8'))
                    except UnicodeEncodeError:
                        pass
        else:
            with gzip.open(file_output_path, 'wb') as f:
                for idx, json_obj in enumerate(jsons_obj):
                    try:
                        if idx == 0:
                            f.write((json.dumps(json_obj, ensure_ascii=False)).encode('utf-8'))
                            # f.write((json.dumps(json_obj, ensure_ascii=False)))
                        else:
                            f.write(('\n' + json.dumps(json_obj, ensure_ascii=False)).encode('utf-8'))
                    except UnicodeEncodeError:
                        pass


def store_jsons_perline_in_file_by_orjson(jsons_obj, file_output_path, is_append=False):
    if jsons_obj is None or len(jsons_obj) == 0:
        return
    os.makedirs(os.path.dirname(file_output_path), exist_ok=True)
    is_exits = os.path.exists(file_output_path)
    with fasteners.InterProcessLock(file_output_path):
        if is_append:
            with gzip.open(file_output_path, 'ab') as f:
                for idx, json_obj in enumerate(jsons_obj):
                    try:
                        if idx == 0:
                            if is_exits:
                                f.write(('\n' + orjson.dumps(json_obj).decode('utf-8')).encode('utf-8'))
                            else:
                                f.write((orjson.dumps(json_obj).decode('utf-8')).encode('utf-8'))
                        else:
                            f.write(('\n' + orjson.dumps(json_obj).decode('utf-8')).encode('utf-8'))
                    except Exception as e:
                        log_error(e)
        else:
            with gzip.open(file_output_path, 'wb') as f:
                for idx, json_obj in enumerate(jsons_obj):
                    try:
                        if idx == 0:
                            f.write((orjson.dumps(json_obj).decode('utf-8')).encode('utf-8'))
                            # f.write((json.dumps(json_obj, ensure_ascii=False)))
                        else:
                            f.write(('\n' + orjson.dumps(json_obj).decode('utf-8')).encode('utf-8'))
                    except Exception as e:
                        log_error(e)


def store_jsons_perline_in_file_non_compress(jsons_obj, file_output_path, is_append=False):
    os.makedirs(os.path.dirname(file_output_path), exist_ok=True)
    with fasteners.InterProcessLock(file_output_path):
        if is_append:
            with open(file_output_path, 'ab') as f:
                for json_obj in jsons_obj:
                    f.write((json.dumps(json_obj, ensure_ascii=False) + '\n').encode('utf-8'))
        else:
            with open(file_output_path, 'wb') as f:
                for json_obj in jsons_obj:
                    f.write((json.dumps(json_obj, ensure_ascii=False) + '\n').encode('utf-8'))


def store_jsons_perline_in_file_non_compress_by_orjson(jsons_obj, file_output_path, is_append=False):
    os.makedirs(os.path.dirname(file_output_path), exist_ok=True)
    with fasteners.InterProcessLock(file_output_path):
        if is_append:
            with open(file_output_path, 'ab') as f:
                for json_obj in jsons_obj:
                    f.write((orjson.dumps(json_obj).decode('utf-8') + '\n').encode('utf-8'))
        else:
            with open(file_output_path, 'wb') as f:
                for json_obj in jsons_obj:
                    f.write((orjson.dumps(json_obj).decode('utf-8') + '\n').encode('utf-8'))


def get_content_from_csv_callback(file_input_path, process_callback):
    with open(file_input_path, newline='') as f:
        reader = csv.reader(f)
        for row in reader:
            process_callback(row)


def get_content_from_csv_hp(file_input_path, delimiter=','):
    with open(file_input_path, newline='') as f:
        reader = csv.reader(f, delimiter=delimiter)
        for row in reader:
            yield row


def get_content_from_gz_callback(file_input_path, process_callback):
    with gzip.open(file_input_path, 'rt') as f:
        for line in f:
            process_callback(line)


def get_content_from_csv(file_input_path, delimiter=','):
    output = []
    with open(file_input_path, newline='') as f:
        reader = csv.reader(f, delimiter=delimiter)
        for row in reader:
            output.append(row)
    return output


def get_content_from_csv_batch_hp(file_input_path, batch_size=100):
    rows = []
    with open(file_input_path, newline='') as f:
        reader = csv.reader(f)
        no = 0
        for row in reader:
            no += 1
            rows.append(row)
            if no % batch_size == 0:
                yield rows
                rows = []
    if rows is not None and len(rows) > 0:
        yield rows


def get_content_from_csv_gz_hp(file_input_path):
    with gzip.open(file_input_path, 'rt') as f:
        reader = csv.reader(f)
        for row in reader:
            yield row


def list_uid_to_csv(list_uid, file_path):
    with open(file_path, 'w', newline='') as myfile:
        wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
        for data in list_uid:
            wr.writerow([int(data)])
            # wr.writerow([data])


def wccount(file_path):
    out = subprocess.Popen(['wc', '-l', file_path],
                           stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT
                           ).communicate()[0]
    return int(out.partition(b' ')[0])


def wcgzcount(file_path):
    count = 0
    try:
        bashCommand = "zcat " + file_path + " | wc -l"
        # print(bashCommand)
        out = os.popen(bashCommand)
        data = out.read()
        count = int(data.split('\n')[0])
        out.close()
    except Exception as e:
        print(e)
    return count


def count_line_all_gz(folder_path):
    # print('count ', folder_uid_path)
    # files_absolute_path, files_name = get_files_in_folder(folder_path)
    count = 0
    bashCommand = "zcat " + folder_path + "/*.gz | wc -l"
    # print(bashCommand)
    try:
        print('prepare: ', bashCommand)
        out = os.popen(bashCommand)
        data = out.read()
        count = int(data.split('\n')[0])
        out.close()
    except Exception as e:
        print(e)
    print('counted', count, ': ', bashCommand)
    return count


def read_by_lines(inputfile, is_gzip=True, is_json=True):
    if is_gzip:
        reader = gzip.open(inputfile, 'rt', encoding='utf-8')
    else:
        reader = open(inputfile, 'rt', encoding='utf-8')

    for line in reader:
        line = line.strip('\n')
        if is_json:
            try:
                yield json.loads(line, encoding='utf-8')
            except json.JSONDecodeError:
                continue
        else:
            yield line
    reader.close()


def symlink_force(target, link_name):
    try:
        if os.path.islink(link_name):
            if target == os.readlink(link_name):
                return
            os.remove(link_name)
        os.symlink(target, link_name)
        os.rename(link_name, link_name)
    except OSError as e:
        log_error(e)


def remove_file_if_exist(file_path):
    import os
    if os.path.exists(file_path):
        os.remove(file_path)
