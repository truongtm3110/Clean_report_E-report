import pandas as pd


def split_dataframe_into_chunks(df, chunk_size=5):
    return [df[i:i + chunk_size] for i in range(0, df.shape[0], chunk_size)]


def combine_list_dataframe(lst_dataframe):
    return pd.concat(lst_dataframe, ignore_index=True)
