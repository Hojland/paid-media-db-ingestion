import pandas as pd


def pd_to_translate_dict(df: pd.DataFrame, col_from: str, col_to: str):
    translate_dct = dict(zip(df[col_from], df[col_to]))
    return translate_dct