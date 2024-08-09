import json
import random
import time
from enum import Enum
from typing import List

import orjson
from pgcopy import CopyManager
from pydantic import BaseModel

from helper.array_helper import get_uniq_by_key
from helper.error_helper import log_error
from helper.logger_helper import LoggerSimple
from helper.type_helper import cast_str, cast_int, cast_bool, cast_datetime, cast_obj, cast_float, cast_json_byte

logger = LoggerSimple(name=__name__).logger


class TypeSchema(str, Enum):
    str = 'str'
    int = 'int'
    float = 'float'
    bool = 'bool'
    jsonb = 'jsonb'
    datetime = 'datetime'
    datetimetz = 'datetimetz'


class SchemaColumn(BaseModel):
    column_name: str
    dict_name: str = None
    type: TypeSchema = TypeSchema.str
    override_if_exist: bool = True
    is_pk: bool = False

    def __init__(self, *args, **kwargs):
        dict_name = kwargs.get('dict_name')
        if dict_name is None:
            dict_name = kwargs.get('column_name')
        super().__init__(dict_name=dict_name, **kwargs)


def upsert_via_temp_table(connection, lst_schema_column: List[SchemaColumn], table_name, lst_record, debug=True,
                          filter_unique=True):
    """
    Bước 1: Tạo bảng phụ
    Bước 2: copy via binary vào bảng phụ
    Bước 3: Build query update on conflict
    """
    start_time = time.time()
    try:
        cursor = connection.cursor()
        cursor.execute(f"SET temp_buffers = {1024 * 5000};")
        tb_temp = f'temp_{table_name}_{random.randint(0, 1000)}'
        cursor.execute(f"""CREATE TEMP TABLE {tb_temp} AS table {table_name} WITH NO DATA;""")
        # cursor.execute(
        #     f"""-- CREATE TEMP TABLE {tb_temp} ("product_base_id" varchar NOT NULL,"shop_base_id" varchar,"country" varchar,"platform_id" int2,"product_id_platform" int8,"shop_id_platform" varchar,"name" varchar,"price" float8,"price_before_discount" float8,"created_at_platform" timestamp,"categories" jsonb,"category_base_id" varchar,"rating_detail" jsonb,"rating_count" int4,"rating_avg" float4,"comment_count" int4,"like_count" int4,"stock_count" int4,"view_count" int4,"is_official_shop" bool,"currency" varchar,"brand" varchar,"description" text,"shop_location" varchar,"url_thumbnail" varchar,"url_images" jsonb,"historical_sold" int4,"sold" int4,"attributes" jsonb,"variations" jsonb,"can_use_wholesale" bool,"show_free_shipping" bool,"estimated_days" int2,"verified_label" bool,"is_adult" bool,"status" int2,"created_at" timestamp DEFAULT now(),"updated_at" timestamp,"updated_at_platform" timestamp,"publish_sitemap_at" timestamp, PRIMARY KEY ("product_base_id"));""")

        fields = [schema_column.column_name for schema_column in lst_schema_column]

        always_null_field_lst = set([schema_column.column_name for schema_column in lst_schema_column])
        always_not_null_field_lst = set([schema_column.column_name for schema_column in lst_schema_column])
        record_to_insert = []

        lst_pk = []
        lst_pk_dict = []
        for schema_column in lst_schema_column:
            if schema_column.is_pk:
                lst_pk.append(schema_column.column_name)
                lst_pk_dict.append(schema_column.dict_name)

        if filter_unique:
            lst_record_uniq = get_uniq_by_key(array_dict=lst_record, key=lst_pk_dict[0])  # đang chỉ support pk là 1 cột
        else:
            lst_record_uniq = lst_record
        for record in lst_record_uniq:
            tuple_insert = ()
            for schema_column in lst_schema_column:
                if schema_column.type == TypeSchema.str:
                    value = cast_str(record.get(schema_column.dict_name))
                elif schema_column.type == TypeSchema.int:
                    value = cast_int(record.get(schema_column.dict_name))
                elif schema_column.type == TypeSchema.float:
                    value = cast_float(record.get(schema_column.dict_name))
                elif schema_column.type == TypeSchema.bool:
                    value = cast_bool(record.get(schema_column.dict_name))
                elif schema_column.type == TypeSchema.datetime:
                    value = cast_datetime(record.get(schema_column.dict_name))
                elif schema_column.type == TypeSchema.jsonb:
                    value = cast_json_byte(cast_obj(record.get(schema_column.dict_name)))
                else:
                    value = record.get(schema_column.dict_name)
                tuple_insert = tuple_insert + (value,)
                if value is None:
                    if schema_column.column_name in always_not_null_field_lst:
                        always_not_null_field_lst.remove(schema_column.column_name)
                else:
                    if schema_column.column_name in always_null_field_lst:
                        always_null_field_lst.remove(schema_column.column_name)

            record_to_insert.append(tuple_insert)

        start_time_sql_product = time.time()
        mgr = CopyManager(connection, tb_temp, cols=fields)
        mgr.threading_copy(record_to_insert)
        del record_to_insert

        update_override = []
        for schema_column in lst_schema_column:
            if not schema_column.is_pk:
                if schema_column.override_if_exist:
                    if schema_column.column_name in always_null_field_lst:
                        continue
                    elif schema_column.column_name in always_not_null_field_lst:
                        update_override.append(
                            f'{schema_column.column_name}=EXCLUDED.{schema_column.column_name}')
                    else:
                        update_override.append(
                            f'{schema_column.column_name}=COALESCE(EXCLUDED.{schema_column.column_name},{table_name}.{schema_column.column_name})')
                else:
                    update_override.append(
                        f'{schema_column.column_name}=EXCLUDED.{schema_column.column_name}')
        # logger.info(
        #     f'always_not_null_field_lst {len(always_not_null_field_lst)}, always_null_field_lst {len(always_null_field_lst)}')

        insert_query = f"""INSERT INTO {table_name} ({','.join(fields)})
                               SELECT * FROM {tb_temp} ON CONFLICT ({','.join(lst_pk)}) DO UPDATE SET {", ".join(update_override)};"""
        cursor.execute(insert_query)
        cursor.execute(f"""DROP TABLE {tb_temp};""")
        connection.commit()
        time_elapsed_sql = time.time() - start_time_sql_product
        time_total = time.time() - start_time
        logger.info(
            f"[upsert sql {table_name}] in {time_elapsed_sql} s (total={time_total} s) - {len(lst_record_uniq)} records ~ {int(len(lst_record_uniq) / time_elapsed_sql)} rps")
    except Exception as e:
        log_error(e)
    finally:
        if connection:
            connection.close()


if __name__ == '__main__':
    from app.db.postgresql_connection import PostgreSQLConnection

    lst_schema_column = [
        SchemaColumn(column_name='product_base_id', type=TypeSchema.str, is_pk=True, override_if_exist=False),
        SchemaColumn(column_name='shop_base_id', type=TypeSchema.str),
        SchemaColumn(column_name='country', type=TypeSchema.str),
        SchemaColumn(column_name='platform_id', type=TypeSchema.int),
        SchemaColumn(column_name='product_id_platform', type=TypeSchema.int),
        SchemaColumn(column_name='shop_id_platform', type=TypeSchema.str),
        SchemaColumn(column_name='name', type=TypeSchema.str),
        SchemaColumn(column_name='price', type=TypeSchema.float),
        SchemaColumn(column_name='price_before_discount', type=TypeSchema.float),
        SchemaColumn(column_name='created_at_platform', type=TypeSchema.datetime),
        SchemaColumn(column_name='categories', type=TypeSchema.jsonb),
        SchemaColumn(column_name='category_base_id', type=TypeSchema.str),
        SchemaColumn(column_name='rating_detail', type=TypeSchema.jsonb),
        SchemaColumn(column_name='rating_count', type=TypeSchema.int),
        SchemaColumn(column_name='rating_avg', type=TypeSchema.float),
        SchemaColumn(column_name='comment_count', type=TypeSchema.int),
        SchemaColumn(column_name='like_count', type=TypeSchema.int),
        SchemaColumn(column_name='stock_count', type=TypeSchema.int),
        SchemaColumn(column_name='view_count', type=TypeSchema.int),
        SchemaColumn(column_name='is_official_shop', type=TypeSchema.bool),
        SchemaColumn(column_name='currency', type=TypeSchema.str),
        SchemaColumn(column_name='brand', type=TypeSchema.str),
        SchemaColumn(column_name='description', type=TypeSchema.str),
        SchemaColumn(column_name='shop_location', type=TypeSchema.str),
        SchemaColumn(column_name='url_thumbnail', type=TypeSchema.str),
        SchemaColumn(column_name='url_images', type=TypeSchema.jsonb),
        SchemaColumn(column_name='historical_sold', type=TypeSchema.int),
        SchemaColumn(column_name='sold', type=TypeSchema.int),
        SchemaColumn(column_name='attributes', type=TypeSchema.jsonb),
        SchemaColumn(column_name='variations', type=TypeSchema.jsonb),
        SchemaColumn(column_name='can_use_wholesale', type=TypeSchema.bool),
        SchemaColumn(column_name='show_free_shipping', type=TypeSchema.bool),
        SchemaColumn(column_name='estimated_days', type=TypeSchema.int),
        SchemaColumn(column_name='verified_label', type=TypeSchema.bool),
        SchemaColumn(column_name='is_adult', type=TypeSchema.bool),
        SchemaColumn(column_name='status', type=TypeSchema.int),
        SchemaColumn(column_name='created_at', type=TypeSchema.datetime, override_if_exist=False),
        SchemaColumn(column_name='updated_at', type=TypeSchema.datetime),
        SchemaColumn(column_name='updated_at_platform', type=TypeSchema.datetime),
        SchemaColumn(column_name='publish_sitemap_at', type=TypeSchema.datetime),
    ]
    postgres_connection = PostgreSQLConnection()
    connection = postgres_connection.get_connection(is_create_new_connection=True)
    lst_record = [
        {}
    ]
    upsert_via_temp_table(connection=connection, lst_schema_column=lst_schema_column, table_name='product_base',
                          lst_record=lst_record, debug=True)
