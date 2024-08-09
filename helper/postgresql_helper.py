import datetime
import json
import time

import psycopg2
from psycopg2 import extras

from helper.datetime_helper import get_datetime_from_timestamp
from helper.error_helper import log_error
from helper.logger_helper import LoggerSimple

logger = LoggerSimple(name=__name__).logger


def get_postgresql_database_connection(user, password, host, database, port=5432):
    return psycopg2.connect(user=user,
                            password=password,
                            host=host,
                            port=port,
                            database=database)


def select_by_query(query, connection, params=None):
    results = []
    # query = text(query)
    try:
        cursor = connection.cursor()
        if params is not None:
            cursor.execute(query, **params)
        else:
            cursor.execute(query)
        results = [dict(line) for line in
                   [zip([column[0] for column in cursor.description], row) for row in cursor.fetchall()]]
    except Exception as e:
        log_error(e)
    finally:
        pass
        # cursor.close
        # connection.close()
    return results


def build_query_upsert(connection, array_data: list, schema: dict, primary_columns: list, table: str, batch_size=100,
                       debug=True):
    def transform_tuple_from_obj(array_data):
        record_to_insert = []
        for record in array_data:
            tuple_record = ()
            for key_schema in schema:
                row_schema = schema[key_schema]
                if row_schema.get('type') == 'timestamp':
                    timestamp_value = record.get(row_schema.get('field_in_dict'))
                    if timestamp_value is not None and (type(timestamp_value) is int or type(timestamp_value) is str):
                        tuple_record += (get_datetime_from_timestamp(timestamp_value),)
                    elif timestamp_value is not None and type(timestamp_value) is datetime.datetime:
                        tuple_record += (timestamp_value,)
                    else:
                        tuple_record += (None,)
                elif 'dict' in row_schema.get('type'):
                    dict_value = record.get(row_schema.get('field_in_dict'))
                    if dict_value is not None:
                        tuple_record += (json.dumps(dict_value, ensure_ascii=False),)
                    else:
                        tuple_record += (None,)
                else:
                    tuple_record += (record.get(row_schema.get('field_in_dict')),)
            record_to_insert.append(tuple_record)
        return record_to_insert

    is_success = False
    s = time.perf_counter()
    values = ', '.join(['%s' for _ in list(schema.keys())])
    fields = ','.join(list(schema.keys()))
    override = []
    for key in schema.keys():
        value = schema[key]
        if value.get('override_if_exist') is not False:
            column = key
            override.append(f'\n{column} = COALESCE(EXCLUDED.{column},{table}.{column})')
    if len(primary_columns) == 1:
        primary_column_text = primary_columns[0]
    else:
        primary_column_text = ','.join(primary_columns)
    query = f"""INSERT INTO {table} ({fields}) VALUES ({values})
ON CONFLICT ({primary_column_text}) DO UPDATE SET {','.join(override)}
"""

    record_to_insert = transform_tuple_from_obj(array_data)
    cursor = None
    try:
        cursor = connection.cursor()
        extras.execute_batch(cur=cursor, sql=query, argslist=record_to_insert,
                             page_size=batch_size)
        connection.commit()
        elapsed = time.perf_counter() - s
        if debug:
            logger.info(f'upsert {len(array_data)} records postgres in {elapsed}')
        is_success = True
    except Exception as error:
        log_error(error)
    finally:
        if cursor:
            cursor.close()

    return is_success


def build_query_upsert_v2(connection, array_data: list, insert_schema: dict, update_schema: dict, primary_columns: list,
                          table: str, batch_size=100,
                          debug=True):
    def transform_tuple_from_obj(array_data, insert_schema_key_lst):
        always_null_field_lst = dict.fromkeys(insert_schema_key_lst, True)
        always_not_null_field_lst = dict.fromkeys(insert_schema_key_lst, True)

        record_to_insert = []
        for record in array_data:
            tuple_record = ()
            for key_schema in insert_schema_key_lst:
                row_schema = insert_schema[key_schema]

                # get data
                temp_value = record.get(row_schema.get('field_in_dict'))
                if temp_value is None:
                    if key_schema in always_not_null_field_lst:
                        del always_not_null_field_lst[key_schema]
                if temp_value is not None:
                    if key_schema in always_null_field_lst:
                        del always_null_field_lst[key_schema]

                if row_schema.get('type') == 'timestamp':
                    timestamp_value = record.get(row_schema.get('field_in_dict'))

                    # temp pre refine
                    if timestamp_value is not None and type(
                            timestamp_value) is str and '-' in timestamp_value and ':' in timestamp_value and 'T' in timestamp_value \
                            and '.' in timestamp_value:
                        try:
                            timestamp_value = datetime.datetime.strptime(timestamp_value, "%Y-%m-%dT%H:%M:%S.%f")
                        except Exception as e:
                            log_error(e)

                    if timestamp_value is not None and (type(timestamp_value) is int or type(timestamp_value) is str):
                        tuple_record += (get_datetime_from_timestamp(timestamp_value),)
                    elif timestamp_value is not None and type(timestamp_value) is datetime.datetime:
                        tuple_record += (timestamp_value,)
                    else:
                        tuple_record += (None,)
                elif 'dict' in row_schema.get('type'):
                    dict_value = record.get(row_schema.get('field_in_dict'))
                    if dict_value is not None:
                        tuple_record += (json.dumps(dict_value, ensure_ascii=False),)
                    else:
                        tuple_record += (None,)
                else:
                    tuple_record += (record.get(row_schema.get('field_in_dict')),)
            record_to_insert.append(tuple_record)
        # print(record_to_insert)
        return record_to_insert, always_not_null_field_lst, always_null_field_lst

    insert_schema_keys = list(insert_schema.keys())
    is_success = False
    s = time.perf_counter()
    fields = ','.join(insert_schema_keys)
    override = []
    # new_override = []
    record_to_insert, always_not_null_field_lst, always_null_field_lst = transform_tuple_from_obj(array_data=array_data,
                                                                                                  insert_schema_key_lst=insert_schema_keys)

    logger.info(
        f'always_not_null_field_lst {len(always_not_null_field_lst)}, always_null_field_lst {len(always_null_field_lst)}')
    for key in update_schema.keys():
        value = update_schema[key]
        if value.get('override_if_exist') is not False:
            column = key
            if column in always_null_field_lst:
                continue
            elif column in always_not_null_field_lst:
                override.append(f'\n{column} = EXCLUDED.{column}')
            else:
                override.append(f'\n{column} = COALESCE(EXCLUDED.{column},{table}.{column})')

            # new_override.append(f'\n{column} = COALESCE({insert_temp_table}.{column},{table}.{column})')
    if len(primary_columns) == 1:
        primary_column_text = primary_columns[0]
    else:
        primary_column_text = ','.join(primary_columns)
    #
    # query = f"""INSERT INTO {table} ({fields}) VALUES ({values})
    # ON CONFLICT ({primary_column_text}) DO UPDATE SET {','.join(override)}
    # """

    # query = f"""INSERT INTO {table} ({fields}) VALUES %s
    # ON CONFLICT ({primary_column_text}) DO NOTHING
    # """
    if override != None and len(override) > 0:
        query = f"""INSERT INTO {table} ({fields}) VALUES %s 
        ON CONFLICT ({primary_column_text}) DO UPDATE SET {','.join(override)}
        """
    else:
        query = f"""INSERT INTO {table} ({fields}) VALUES %s 
                ON CONFLICT ({primary_column_text}) DO NOTHING;
                """
    # insert_query = f"""INSERT INTO {insert_temp_table} ({fields}) VALUES %s"""
    cursor = None
    try:
        # print(record_to_insert)
        cursor = connection.cursor()
        cursor.execute(f"SET temp_buffers = {1024 * 40000};")

        extras.execute_values(cur=cursor, sql=query, argslist=record_to_insert, page_size=batch_size, template=None)
        # cursor.execute(f"CREATE TEMP TABLE {insert_temp_table} AS SELECT * FROM product_base LIMIT 0;")
        # extras.execute_values(cur=cursor, sql=insert_query, argslist=record_to_insert,
        #                       page_size=batch_size, template=None)
        # update_query = f"""
        # UPDATE {table} SET {','.join(override)} FROM {insert_temp_table}
        # WHERE  {table}.product_base_id = {insert_temp_table}.product_base_id;
        # """
        # # print(update_query)
        # cursor.execute(update_query)
        connection.commit()
        elapsed = time.perf_counter() - s
        if debug:
            logger.info(f'upsert {len(array_data)} records postgres in {elapsed}')
        is_success = True
    except Exception as error:
        log_error(error)
    finally:
        if cursor:
            cursor.close()

    return is_success


def build_query_update_status_v2(connection, array_data: list, insert_schema: dict, update_schema: dict,
                                 primary_columns: list,
                                 table: str, batch_size=100,
                                 debug=True):
    def transform_tuple_from_obj(array_data, insert_schema_key_lst):
        always_null_field_lst = dict.fromkeys(insert_schema_key_lst, True)
        always_not_null_field_lst = dict.fromkeys(insert_schema_key_lst, True)

        record_to_insert = []
        for record in array_data:
            tuple_record = ()
            for key_schema in insert_schema_key_lst:
                row_schema = insert_schema[key_schema]

                # get data
                temp_value = record.get(row_schema.get('field_in_dict'))
                if temp_value is None:
                    if key_schema in always_not_null_field_lst:
                        del always_not_null_field_lst[key_schema]
                if temp_value is not None:
                    if key_schema in always_null_field_lst:
                        del always_null_field_lst[key_schema]

                if row_schema.get('type') == 'timestamp':
                    timestamp_value = record.get(row_schema.get('field_in_dict'))

                    # temp pre refine
                    if timestamp_value is not None and type(
                            timestamp_value) is str and '-' in timestamp_value and ':' in timestamp_value and 'T' in timestamp_value \
                            and '.' in timestamp_value:
                        try:
                            timestamp_value = datetime.datetime.strptime(timestamp_value, "%Y-%m-%dT%H:%M:%S.%f")
                        except Exception as e:
                            log_error(e)

                    if timestamp_value is not None and (type(timestamp_value) is int or type(timestamp_value) is str):
                        tuple_record += (get_datetime_from_timestamp(timestamp_value),)
                    elif timestamp_value is not None and type(timestamp_value) is datetime.datetime:
                        tuple_record += (timestamp_value,)
                    else:
                        tuple_record += (None,)
                elif 'dict' in row_schema.get('type'):
                    dict_value = record.get(row_schema.get('field_in_dict'))
                    if dict_value is not None:
                        tuple_record += (json.dumps(dict_value, ensure_ascii=False),)
                    else:
                        tuple_record += (None,)
                else:
                    tuple_record += (record.get(row_schema.get('field_in_dict')),)
            record_to_insert.append(tuple_record)
        # print(record_to_insert)
        return record_to_insert, always_not_null_field_lst, always_null_field_lst

    insert_schema_keys = list(insert_schema.keys())
    is_success = False
    s = time.perf_counter()
    fields = ','.join(insert_schema_keys)
    # override = []
    update_override = []
    record_to_insert, always_not_null_field_lst, always_null_field_lst = transform_tuple_from_obj(array_data=array_data,
                                                                                                  insert_schema_key_lst=insert_schema_keys)
    insert_temp_table = f'pbpv_temp_{int(time.time())}'
    logger.info(
        f'always_not_null_field_lst {len(always_not_null_field_lst)}, always_null_field_lst {len(always_null_field_lst)}')
    for key in update_schema.keys():
        value = update_schema[key]
        if value.get('override_if_exist') is not False:
            column = key
            if column in always_null_field_lst:
                continue
            elif column in always_not_null_field_lst:
                # override.append(f'\n{column} = EXCLUDED.{column}')
                update_override.append(f'\n{column} = {insert_temp_table}.{column}')
            else:
                # override.append(f'\n{column} = COALESCE(EXCLUDED.{column},{table}.{column})')

                update_override.append(f'\n{column} = COALESCE({insert_temp_table}.{column},{table}.{column})')

    insert_query = f"""INSERT INTO {insert_temp_table} ({fields}) VALUES %s"""
    cursor = None
    try:
        cursor = connection.cursor()
        cursor.execute(f"SET temp_buffers = {1024 * 5000};")
        cursor.execute(f"""CREATE TEMP TABLE {insert_temp_table} (
    product_base_id       varchar(32) not null
        constraint product_base_pk
            primary key,
    price                 double precision,
   
    historical_sold       integer,
   
    status                smallint,
    updated_at            timestamp
);""")
        # cursor.execute(f"CREATE INDEX {insert_temp_table}_x_id_idx ON {insert_temp_table}(product_base_id);")
        extras.execute_values(cur=cursor, sql=insert_query, argslist=record_to_insert,
                              page_size=batch_size, template=None)
        update_query = f"""
        UPDATE {table} SET {','.join(update_override)} FROM {insert_temp_table}
        WHERE  {table}.product_base_id = {insert_temp_table}.product_base_id;
        """
        print(update_query)
        cursor.execute(update_query)
        connection.commit()
        elapsed = time.perf_counter() - s
        if debug:
            logger.info(f'upsert {len(array_data)} records postgres in {elapsed}')
        is_success = True
    except Exception as error:
        log_error(error)
    finally:
        if cursor:
            cursor.close()

    return is_success
