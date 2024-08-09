import datetime

from sqlalchemy import inspect
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from sqlalchemy.orm.collections import InstrumentedList
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.functions import coalesce

from helper.error_helper import log_error
from helper.logger_helper import LoggerSimple

logger = LoggerSimple(name=__name__).logger


def get_or_create(db_session, model, defaults=None, **kwargs):
    is_success = False
    is_created_before = False
    try:
        instance = db_session.query(model).filter_by(**kwargs).first()
        if instance:
            is_created_before = True
            is_success = True
            return instance, is_created_before, is_success
        else:
            params = dict((k, v) for k, v in kwargs.items() if not isinstance(v, ClauseElement))
            params.update(defaults or {})
            instance = model(**params)
            db_session.add(instance)
            is_created_before = False
            is_success = True
            # db_session.commit()
            # db_session.flush()
            return instance, is_created_before, is_success
    except Exception as e:
        log_error(e)

    return None, is_created_before, is_success


def element_model_to_dict(element):
    if element is None or type(element) == dict:
        return element
    output = {}
    for c in inspect(element).mapper.column_attrs:
        key = c.key
        value = getattr(element, c.key)
        if type(value) == datetime.datetime:
            value = int(value.timestamp())
            # print(key, value)
        output[key] = value
    return output


def object_model_to_dict(obj):
    if type(obj) == list or type(obj) == InstrumentedList:
        output = []
        for element in obj:
            output.append(element_model_to_dict(element))
        return output
    else:
        return element_model_to_dict(obj)


from sqlalchemy import text


async def select_by_query(query, connection, params=None, return_result=True, get_pair=False):
    results = []
    if type(query) == str:
        query = text(query)

        if params is not None:
            if isinstance(connection, Session):
                execute = await connection.execute(query, params)
            else:
                execute = await connection.execute(query, **params)
        else:
            execute = await connection.execute(query)
        cursor = execute.cursor
        if return_result:
            if get_pair:
                results = dict({row[0]: row[1] for row in cursor.fetchall()})
            else:
                results = [dict(line) for line in
                           [zip([column[0] for column in cursor.description], row) for row in cursor.fetchall()]]

    return results


def select_by_query_hp(query, connection, params=None, batch=100):
    if type(query) == str:
        query = text(query)

        if params is not None:
            if isinstance(connection, Session):
                execute = connection.execute(query, params)
            else:
                execute = connection.execute(query, **params)
        else:
            execute = connection.execute(query)
        cursor = execute.cursor
        while True:
            chunk = cursor.fetchmany(batch)
            if not chunk:
                break
            for row in chunk:
                for line in [zip([column[0] for column in cursor.description], row)]:
                    yield dict(line)


def format_records(cursor):
    if cursor is None:
        return []
    return [dict(line) for line in
            [zip([column[0] for column in cursor.description], row) for row in cursor.fetchall()]]


def compile_query(query):
    """Via http://nicolascadou.com/blog/2014/01/printing-actual-sqlalchemy-queries"""
    compiler = query.compile if not hasattr(query, 'statement') else query.statement.compile
    return compiler(dialect=postgresql.dialect())


def get_stmt_upsert(model, rows, index_elements=[], no_update_cols=[], return_cols=[], not_update_null=True,
                    debug=False):
    # todo: có bug: no_update_cols có nghĩa là nếu cột ở no_update_cols tồn tại giá trị rồi thì không ghi đè; còn nếu đang null thì vẫn phải ghi
    table = model.__table__
    # if model_columns is None:
    model_columns = [column.key for column in table.columns]  # get columns in model
    rows_insert = []

    for row in rows:
        row_insert = row
        cols_need_insert = [key for key in row.keys()]  # get columns in row
        for col in cols_need_insert:
            if col not in model_columns:  # del column if not in model's columns
                del row_insert[col]
        for col in index_elements:
            # del index column if it is autoincrement and it is none
            if getattr(model, col).autoincrement and row_insert[col] is None:
                del row_insert[col]
        rows_insert.append(row_insert)

    stmt = insert(table).values(rows_insert)
    # print(rows_insert[0])

    update_cols = [c.name for c in table.c
                   if c not in list(table.primary_key.columns)]

    if not_update_null:
        if debug:
            logger.info(f'update_cols: {update_cols}, index_elements: {index_elements}')
        # print(update_cols)
        if update_cols and len(update_cols) > 0:
            on_conflict_stmt = stmt.on_conflict_do_update(
                index_elements=index_elements,
                set_={
                    k: coalesce(getattr(stmt.excluded, k), getattr(model, k)) if k not in no_update_cols else coalesce(
                        getattr(model, k), getattr(stmt.excluded, k))
                    for k in update_cols
                })  # coalesce(A,B) if A == null return B else return A

        else:
            on_conflict_stmt = stmt.on_conflict_do_nothing(
                index_elements=index_elements)
    else:
        on_conflict_stmt = stmt.on_conflict_do_update(
            index_elements=index_elements,
            set_={k: getattr(stmt.excluded, k) if k not in no_update_cols else coalesce(
                getattr(model, k), getattr(stmt.excluded, k)) for k in
                  update_cols})  # coalesce(A,B) if A == null return B else return A
    if debug:
        logger.info(compile_query(on_conflict_stmt))

    return_cols_valid = []
    for col in return_cols:
        if col not in model_columns:  # del column if not in model's columns
            del return_cols[col]
        else:
            return_cols_valid.append(table.columns[col])

    # session.execute(on_conflict_stmt)
    if len(return_cols_valid) > 0:
        return on_conflict_stmt.returning(*return_cols_valid)
    else:
        return on_conflict_stmt


def get_stmt_upsert_v2(model, rows, index_elements=[], no_update_cols=[], return_cols=[], not_update_null=True,
                       debug=False):
    # todo: có bug: no_update_cols có nghĩa là nếu cột ở no_update_cols tồn tại giá trị rồi thì không ghi đè; còn nếu đang null thì vẫn phải ghi
    table = model.__table__
    model_columns = [column.key for column in table.columns]  # get columns in model
    rows_insert = []

    for row in rows:
        row_insert = row
        cols_need_insert = [key for key in row.keys()]  # get columns in row
        for col in cols_need_insert:
            if col not in model_columns:  # del column if not in model's columns
                del row_insert[col]
        # for col in index_elements:
        # del index column if it is autoincrement and it is none
        for col in list(table.primary_key.columns):
            if getattr(model, col.name).autoincrement and row_insert[col.name] is None:
                del row_insert[col.name]
        rows_insert.append(row_insert)

    stmt = insert(table).values(rows_insert)
    # print(rows_insert[0])

    update_cols = [c.name for c in table.c
                   if c not in list(table.primary_key.columns)]

    if not_update_null:
        if debug:
            logger.info(f'update_cols: {update_cols}, index_elements: {index_elements}')
        # print(update_cols)
        if update_cols and len(update_cols) > 0:
            on_conflict_stmt = stmt.on_conflict_do_update(
                index_elements=index_elements,
                set_={
                    k: coalesce(getattr(stmt.excluded, k), getattr(model, k)) if k not in no_update_cols else coalesce(
                        getattr(model, k), getattr(stmt.excluded, k))
                    for k in update_cols
                })  # coalesce(A,B) if A == null return B else return A

        else:
            on_conflict_stmt = stmt.on_conflict_do_nothing(
                index_elements=index_elements)
    else:
        on_conflict_stmt = stmt.on_conflict_do_update(
            index_elements=index_elements,
            set_={k: getattr(stmt.excluded, k) if k not in no_update_cols else coalesce(
                getattr(model, k), getattr(stmt.excluded, k)) for k in
                  update_cols})  # coalesce(A,B) if A == null return B else return A
    if debug:
        logger.info(compile_query(on_conflict_stmt))

    return_cols_valid = []
    for col in return_cols:
        if col not in model_columns:  # del column if not in model's columns
            del return_cols[col]
        else:
            return_cols_valid.append(table.columns[col])

    # session.execute(on_conflict_stmt)
    if len(return_cols_valid) > 0:
        return on_conflict_stmt.returning(*return_cols_valid)
    else:
        return on_conflict_stmt


def join_query_in(lst_id):
    string_query = ', '.join([f"'{id}'" for id in lst_id])
    return string_query


def object_as_dict(obj):
    return {c.key: getattr(obj, c.key)
            for c in inspect(obj).mapper.column_attrs}


if __name__ == '__main__':
    from app import model
    from app.db.session_metricseo import db_session

    blog_category = {
        "id": 1,
        "name": "Review SP 1",
        "parent_id": None,
        "cat_level": 0,
        "url_thumbnail": "Hello 2",
        "description": "Hay KO 3",
        "created_at": None,
        "updated_at": None,
        "slug": "review-top-10"
    }
    on_conflict_stmt = get_stmt_upsert(model=model.BlogCategory,
                                       rows=[blog_category],
                                       index_elements=['id'],
                                       no_update_cols=[],
                                       not_update_null=True,
                                       debug=True)
    db_session.execute(on_conflict_stmt)
    db_session.commit()
