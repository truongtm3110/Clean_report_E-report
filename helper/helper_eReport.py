import pandas as pd
def df_to_excel(df, file_excel_path, index=False):
    with pd.ExcelWriter(
            file_excel_path, engine_kwargs={'options': {'strings_to_urls': False}},
            engine='xlsxwriter',
    ) as writer:
        df.to_excel(writer, index=index)

def check_number_in_word(s):
    for char in s:
        if char.isdigit():
            return True
    return False