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

COLLECTED_VIEW = '''
    CREATE VIEW IF NOT EXISTS output.v_paid_campaign_report AS
    WITH facebook AS (
        SELECT campaign_name, clicks, spend, impressions, ctr, date, 'facebook' as channel
        FROM output.facebook_campaign_report),
    google_cm AS (
        SELECT campaign as campaign_name, clicks, media_spend as spend, impressions, clicks / impressions as ctr, date, 'google_campaign_manager' as channel
        FROM output.google_cm_campaign_report),
    google_ads AS (
        SELECT name as campaign_name, clicks, cost as spend, impressions, ctr, date, 'google_ads' as channel
        FROM output.google_campaign_report)
    SELECT *
    FROM facebook
    UNION ALL
    SELECT * 
    FROM google_cm
    UNION ALL
    SELECT *
    FROM google_ads
'''