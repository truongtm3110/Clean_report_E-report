import numpy as np
import pandas as pd

from helper.reader_helper import load_jsonl_from_gz


def load_df_from_csv_file_path(file_csv_path):
    df = pd.read_csv(file_csv_path)
    return df


def load_df_from_array(array_dictionary, is_normalize_float=False, lst_column_str=None):
    if is_normalize_float:
        array_dictionary_normalize = normalize_float_in_array(array_dictionary)
        df = pd.DataFrame(array_dictionary_normalize)
    else:
        df = pd.DataFrame(array_dictionary)
    if lst_column_str is not None:
        df[lst_column_str] = df[lst_column_str].astype(str)
    return df


def df_to_csv(df, index=True):
    return df.to_csv(index=True)


def normalize_float_in_array(array_dictionary):
    for row in array_dictionary:
        for key in row:
            if type(row[key]) == float:
                row[key] = str(round(row[key], 2))
    return array_dictionary


def df_to_file_csv(df, file_output_csv_path, index=True):
    return df.to_csv(file_output_csv_path, index=index, header=index)


def transform_file_jsonl_gz_to_csv(file_gz_input_path, file_csv_output_path):
    arr = load_jsonl_from_gz(file_gz_path=file_gz_input_path)
    df = load_df_from_array(array_dictionary=arr)
    df_to_file_csv(df=df, file_output_csv_path=file_csv_output_path)
    return df


def df_to_array_dict(df):
    # return df.to_dict('records')
    return df.replace({np.nan: None}).to_dict('records')


def df_to_excel(df, file_excel_path, index=False):
    with pd.ExcelWriter(
            file_excel_path, engine_kwargs={'options': {'strings_to_urls': False}},
            engine='xlsxwriter',
    ) as writer:
        df.to_excel(writer, index=index)


def df_to_excel_fit_column(df, file_excel_path, index=False):
    writer = pd.ExcelWriter(file_excel_path)
    df.to_excel(writer, sheet_name='Data', index=index)
    for column in df:
        column_width = max(df[column].astype(str).map(len).max(), len(column))
        col_idx = df.columns.get_loc(column)
        writer.sheets['Data'].set_column(col_idx, col_idx, column_width)
    writer.save()
