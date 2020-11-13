import pandas as pd
import os
from tqdm import tqdm
import sys
from sqlalchemy.types import String, Integer, Numeric, DateTime

from utils import sql_utils
from settings import MARIADB_CONFIG


def load_data(engine, sql_query: str):
    df_load = pd.read_sql(sql_query, engine, chunksize=20000)
    df = pd.concat([chunk for chunk in tqdm(df_load, desc='Loading data', file=sys.stdout)], ignore_index=True)
    return df


def get_dtype_trans(df: pd.DataFrame):
    obj_vars = [colname for colname in list(df) if df[colname].dtype == 'object']
    int_vars = [colname for colname in list(df) if df[colname].dtype == 'int64']
    float_vars = [colname for colname in list(df) if df[colname].dtype == 'float64']
    datetime_vars = [colname for colname in list(df) if df[colname].dtype == 'datetime64[ns]']

    dtype_trans = {
        obj_var: String(50) for obj_var in obj_vars
    }
    dtype_trans.update({
        int_var: Integer for int_var in int_vars
    })
    dtype_trans.update({
        float_var: Numeric(14, 5) for float_var in float_vars
    })
    dtype_trans.update({
        datetime_var: DateTime() for datetime_var in datetime_vars
    })
    return dtype_trans