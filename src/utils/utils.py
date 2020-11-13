import pandas as pd
import numpy as np
import collections
import re

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

def flatten_dict(d, sep="_"):
    obj = {}

    def recurse(t, parent_key=""):
        if isinstance(t, list):
            for i in range(len(t)):
                recurse(t[i], parent_key + sep + str(i) if parent_key else str(i))
        elif isinstance(t, dict):
            for k,v in t.items():
                recurse(v, parent_key + sep + k if parent_key else k)
        else:
            obj[parent_key] = t

    if isinstance(d, list):
        res_list = []
        for i in range(len(d)):
            recurse(d[i])
            res_list.append(obj.copy())
        return res_list
    else: 
        recurse(d)
    return obj

def dict_key_val_pair_eliminate(dct: dict, pair_id_re: str, key_id_re: str, val_id_re: str):
    out_dct = {}
    pair_dct = {}
    for k,v in dct.items():
        if re.search(key_id_re, k) and re.search(pair_id_re, k):
            pair_id = re.search(pair_id_re, k)[0]
            if pair_id in pair_dct:
                pair_dct[pair_id] = {v: pair_dct[pair_id]}
            else:
                pair_dct[pair_id] = v
        elif re.search(val_id_re, k) and re.search(pair_id_re, k):
            pair_id = re.search(pair_id_re, k)[0]
            if pair_id in pair_dct:
                pair_dct[pair_id] = {pair_dct[pair_id]: v}
            else:
                pair_dct[pair_id] = v
        else:
            out_dct[k] = v
    out_dct_pairs = {f"{k}_{paid_id}": v for paid_id, out_dct_pair in pair_dct.items() for k,v in out_dct_pair.items()}
    out_dct = {**out_dct, **out_dct_pairs}
    return out_dct

def multiple_replace(replace_dct: dict, text: str, **kwargs):
  # Create a regular expression  from the dictionary keys
  regex = re.compile("(%s)" % "|".join(map(re.escape, replace_dct.keys())), **kwargs)

  # For each match, look-up corresponding value in dictionary
  return regex.sub(lambda mo: replace_dct[mo.string[mo.start():mo.end()]], text)
