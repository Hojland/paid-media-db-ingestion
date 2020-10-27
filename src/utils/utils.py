import pandas as pd
import numpy as np


def pd_to_translate_dict(df: pd.DataFrame, col_from: str, col_to: str):
    translate_dct = dict(zip(df[col_from], df[col_to]))
    return translate_dct

def split_list(lst: list, chunk_size: int):
    return [lst[offs:offs+chunk_size] for offs in range(0, len(lst), chunk_size)]

def standardize_lst_dct(lst: list):
    try:
        keys = set([k for dct in lst for k,v in dct.items()])
        std_lst = [dct if dct else {k: np.nan for k in keys} for dct in lst]
    except AttributeError:
        keys = set([k for dct in lst if dct for k,v in dct[0].items()])
        std_lst = [dct[0] if dct else {k: np.nan for k in keys} for dct in lst]     
    return std_lst

def dict_zip(lst1: list, lst2: list):
    """ 
    zip together two lists of equal size that contains dicts to one list of dicts
    """
    assert len(lst1) == len(lst2), 'The list of dicts should be equally sized'
    [lst1[i].update(lst2[i]) for i in range(0, len(lst1))]
    # returning lst1 because of the update method
    return lst1