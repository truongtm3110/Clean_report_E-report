from typing import List

from sqlalchemy import null, JSON
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.functions import coalesce, func

from helper.logger_helper import LoggerSimple

logger = LoggerSimple(name=__name__).logger


def get_stmt_upsert(model, rows, index_elements=[], no_update_cols=[], return_cols=[], not_update_null=True,
                    debug=False):
    from sqlalchemy.dialects.postgresql import insert, JSONB
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
            model_col = getattr(model, col)
            if isinstance(model_col.type, JSONB) and row_insert.get(col, None) is None:
                row_insert[col] = null()
        for col in index_elements:
            # del index column if it is autoincrement and it is none
            model_col = getattr(model, col)
            if model_col.autoincrement and row_insert.get(col, None) is None and col in rows_insert:
                del row_insert[col]
        rows_insert.append(row_insert)
    if debug:
        logger.info(f"rows insert: {rows_insert}")
    stmt = insert(table).values(rows_insert)

    update_cols = [c.name for c in table.c
                   if c not in list(table.primary_key.columns)]
    on_conflict_stmt = stmt
    if index_elements and len(index_elements) > 0:
        if not_update_null:
            if debug:
                logger.info(f'update_cols: {update_cols}, index_elements: {index_elements}')

            if update_cols and len(update_cols) > 0:
                on_conflict_stmt = stmt.on_conflict_do_update(
                    index_elements=index_elements,
                    set_={
                        k: coalesce(getattr(stmt.excluded, k),
                                    getattr(model, k)) if k not in no_update_cols else coalesce(
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


def get_stmt_upsert_mysql(model, rows: List[dict], index_elements=[], no_update_cols=[], return_cols=[],
                          not_update_null=True, debug=False):
    from sqlalchemy.dialects.mysql import insert
    table = model.__table__
    model_columns = [column.key for column in table.columns]  # get columns in model
    rows_insert = []

    for row in rows:
        row_insert = row.copy()
        cols_need_insert = [key for key in row.keys()]  # get columns in row
        for col in cols_need_insert:
            if col not in model_columns:  # delete column if not in model's columns
                del row_insert[col]
            model_col = getattr(model, col)
            if isinstance(model_col.type, JSON) and row_insert.get(col, None) is None:
                row_insert[col] = null()
        for col in index_elements:
            # delete index column if it is autoincrement and it is none
            model_col = getattr(model, col)
            if model_col.autoincrement and row_insert.get(col, None) is None and col in rows_insert:
                del row_insert[col]
        rows_insert.append(row_insert)

    if debug:
        logger.info(f"rows insert: {rows_insert}")

    stmt = insert(table).values(rows_insert)

    update_cols = [c.name for c in table.c if c not in list(table.primary_key.columns)]
    on_duplicate_key_stmt = stmt

    if index_elements and len(index_elements) > 0:
        if not_update_null:
            if debug:
                logger.info(f'update_cols: {update_cols}, index_elements: {index_elements}')

            if update_cols and len(update_cols) > 0:
                on_duplicate_key_stmt = stmt.on_duplicate_key_update(
                    **{
                        k: func.coalesce(getattr(stmt.inserted, k),
                                         getattr(model, k)) if k not in no_update_cols else func.coalesce(
                            getattr(model, k), getattr(stmt.inserted, k))
                        for k in update_cols
                    }
                )
            else:
                on_duplicate_key_stmt = stmt.on_duplicate_key_update({})
        else:
            on_duplicate_key_stmt = stmt.on_duplicate_key_update(
                **{k: getattr(stmt.inserted, k) if k not in no_update_cols else func.coalesce(
                    getattr(model, k), getattr(stmt.inserted, k))
                   for k in update_cols}
            )

    if debug:
        logger.info(str(on_duplicate_key_stmt))

    return_cols_valid = []
    for col in return_cols:
        if col not in model_columns:  # delete column if not in model's columns
            del return_cols[col]
        else:
            return_cols_valid.append(table.columns[col])

    if return_cols_valid:
        return on_duplicate_key_stmt.returning(*return_cols_valid)
    else:
        return on_duplicate_key_stmt


def compile_query(query):
    """Via http://nicolascadou.com/blog/2014/01/printing-actual-sqlalchemy-queries"""
    compiler = query.compile if not hasattr(query, 'statement') else query.statement.compile
    return compiler(dialect=postgresql.dialect())
