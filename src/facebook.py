#%load_ext autoreload
#%autoreload 2
import os
import requests
import urllib
import jmespath
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import re
import json
from sqlalchemy.types import String, DateTime, Numeric
import sqlalchemy
from typing import Dict, List

import settings
import sql
from utils import utils, sql_utils

APP_ID = os.getenv('FACEBOOK_APP_ID')
APP_SECRET = os.getenv('FACEBOOK_APP_SECRET')
ACCESS_TOKEN = os.getenv('FACEBOOK_ACCESS_TOKEN')


def from_date_to_date(days_since_start, plus_days):
    from_date = datetime.today()-timedelta(days=days_since_start)
    to_date = from_date+timedelta(days=plus_days)
    return from_date.strftime('%Y-%m-%d'), to_date.strftime('%Y-%m-%d')

class FacebookAccount:
    def __init__(self, app_id, app_secret, access_token):
        self.app_id = app_id
        self.app_secret = app_secret
        self.access_token = access_token
        self.api_version = 'v8.0'
        self.base_url = 'https://graph.facebook.com/'

    def request_facebook(
            self, edge: str, params: dict={},
            fields: list=[], level: list=[], time_increment: int=None,
            time_range: dict=None, filtering: str=None,
            breakdown: list=None, date_preset: str=None,
            request_type: str='GET', scroll=True):

        params.update({'access_token': self.access_token})

        if time_range:
            params.update({'time_range': str(time_range)})

        if filtering:
            params.update({'filtering': filtering})

        if time_increment:
            params.update({'time_increment': time_increment})

        if level:
            params.update({'level': ','.join(level)})

        if fields:
            params.update({'fields': ','.join(fields)})

        if breakdown:
            params.update({'breakdown': ','.join(breakdown)})

        if date_preset:
            params.update({'date_preset': date_preset})

        if scroll:
            data = []
            res, paging = self.request_facebook(edge=edge, params=params, scroll=False)
            data.extend(res)
            while paging:
                params.update({'after': paging['after']})   
                res, paging = self.request_facebook(edge=edge, params=params, scroll=False)
                data.extend(res)
            return data
        
        try:
            if request_type == 'GET':
                res = requests.get(f'{self.base_url}/{self.api_version}/{edge}',
                    params=params).json()
                if 'error' in res:
                    print(f"there is an error: {res['error']['message']}")
                    return None, None
                if not res['data'] and 'after' in params:
                    print('No further results')
                    return res['data'], None
                return res['data'], res['paging']['cursors']
            elif request_type == 'POST':
                res = requests.post(f'{self.base_url}/{self.api_version}/{edge}',
                    params=params).json()
                if 'error' in res:
                    print(f"there is an error: {res['error']['message']}")
                    return None, None
                if not res['data'] and 'after' in params:
                    print('No further results')
                    return res['data'], None
                return res['data'], res['paging']['cursors']
            else:
                raise NotImplementedError()
        except:
            print('token expired or api overheat')

    def batch_request_facebook(
            self, edge: str, batch_lst: list, params: dict={},
            fields: list=[], level: list=[], time_increment: int=None,
            time_range: dict=None, filtering: str=None,
            breakdown: list=None, date_preset: str=None,
            request_type: str='GET', scroll=True):

        req_params = {'access_token': self.access_token}

        if time_range:
            params.update({'time_range': time_range})

        if filtering:
            params.update({'filtering': filtering})

        if time_increment:
            params.update({'time_increment': time_increment})

        if level:
            params.update({'level': ','.join(level)})

        if fields:
            params.update({'fields': ','.join(fields)})

        if breakdown:
            params.update({'breakdown': ','.join(breakdown)})

        if date_preset:
            params.update({'date_preset': date_preset})
        
        params = '&'.join(f"{key}={val}" for (key,val) in params.items())
        batch = [
            {'method': request_type,
            'relative_url': f'{self.api_version}/{batch_obj}/{edge}?{params}'} for batch_obj in batch_lst]
        req_params.update({'batch': batch})
        res = requests.post(self.base_url,
            json=req_params).text
        return res

    def get_campaigns(self, account_id: str='act_1377918625828738'):
        res = self.request_facebook(edge=f'{account_id}/campaigns', params={'limit': '200'})
        campaign_ids = jmespath.search('[*].id', res)
        return campaign_ids

    def get_insights(self, campaign: str):
        res = self.request_facebook(edge=f'{campaign}/insights', params={'limit': '200'}, fields=['impressions', 'spend', 'campaign_name', 'clicks', 'conversions', 'conversion_values', 'ctr'])
        return res

    def get_insights_adaccount(self, account_id: str='act_1377918625828738', time_increment: int=1, date_preset: str=None, time_range: dict=None):
        res = self.request_facebook(edge=f'{account_id}/insights', params={'limit': '200'},
                                    level=['campaign'], time_increment=time_increment,
                                    fields=['impressions', 'spend', 'campaign_name', 'clicks', 'conversions', 'ctr', 'conversion_values'],
                                    breakdown=['age'], date_preset=date_preset, time_range=time_range)
        res = utils.flatten_dict(res)
        res = [utils.dict_key_val_pair_eliminate(dct, pair_id_re='\d{1}', key_id_re='action_type', val_id_re='value') for dct in res]
        df = pd.DataFrame(res)
        # pivot with aggregation to delete
        index_cols = ['campaign_name', 'date_start', 'date_stop']
        other_static_fields = ['ctr', 'clicks', 'spend', 'impressions']
        if any(['offsite_conversion.fb_pixel_custom' in col for col in list(df)]):
            df = pd.wide_to_long(df, stubnames=['offsite_conversion.fb_pixel_custom'], i=index_cols, j='conversion_name', sep='.', suffix='(?!\.)[a-zA-Z\d_]*$').reset_index()
            df['conversion_name'] = df['conversion_name'].str.extract('(.*)(?=_\d)')
            df = pd.pivot_table(df, values='offsite_conversion.fb_pixel_custom', columns=['conversion_name'], index=index_cols + other_static_fields, aggfunc=np.sum)
            df = pd.DataFrame(df.to_records()).drop('level_0', axis=1)
        else:
            df.columns = [re.search('(^.*?)(?=_\d|$)', col)[0] for col in df.columns]
        return df
    
    def get_long_lived_access_token(self):
        payload = {
            'client_id': self.app_id, 'client_secret': self.app_secret,
            'grant_type': 'fb_exchange_token', 'fb_exchange_token': self.access_token}
        res = self.request_facebook(edge='oauth/access_token', params=payload)
        return res['access_token'], res['expires_in']

    def get_campaign_insights_batch(self, campaign_lst: list, time_increment: int=1, date_preset: str='last_7d'):
        campaign_lst_lst = utils.split_list(campaign_lst, 50)
        res_lst = []
        for campaign_batch in campaign_lst_lst:
            batch_res = self.batch_request_facebook(edge=f'insights', batch_lst=campaign_batch, params={'limit': '200'},
                                    level=['campaign'], time_increment=time_increment,
                                    fields=['impressions', 'spend', 'campaign_name', 'clicks', 'conversions', 'ctr', 'conversion_values'],
                                    breakdown=['age'], date_preset=date_preset)
            batch_res = batch_res.replace('"{', '{').replace('}"', '}').replace('\\"', '"').replace('""', '"')
            batch_res = json.loads(batch_res)
            res = jmespath.search('[].body[].data[*]. \
                {impressions: impressions, spend: spend, date_start: date_start, date_stop: date_stop}', batch_res)
            campaign_batch_dct = [{'campaign_id': campaign_id} for campaign_id in campaign_batch]
            res = utils.standardize_lst_dct(res)
            res = utils.dict_zip(res, campaign_batch_dct)
            res_lst.extend(res)
        df_insights = pd.DataFrame.from_dict(res_lst)
        return df_insights

def ingest_facebook_campaigns(account: FacebookAccount, mariadb_engine: sqlalchemy.engine, from_date, to_date):
    df = account.get_insights_adaccount(time_increment=1, time_range={'since': from_date, 'until': to_date})
    df['date'] = pd.to_datetime(df['date_start'])
    df = df.drop(['date_start', 'date_stop'], axis=1)

    if sql_utils.table_exists_notempty(mariadb_engine, 'output', 'facebook_campaign_report'):
        min_date = df['date'].min().strftime('%Y-%m-%d')
        sql_utils.delete_date_entries_in_table(mariadb_engine, min_date, 'facebook_campaign_report')
        
        sql_table_cols = sql_utils.table_col_names(mariadb_engine, 'output', 'facebook_campaign_report')
        new_cols = [col for col in list(df) if col not in sql_table_cols]
    else:
        new_cols = []

    dtype_trans = sql_utils.get_dtype_trans_mysql(df, str_len=200)
    
    # maybe make index hash instead using dict? or?
    sql_utils.create_table(mariadb_engine, 'output.facebook_campaign_report', col_datatype_dct=dtype_trans, index_lst=['date', 'campaign_name'])
    
    if new_cols:
        new_col_datatype_dct = {x: dtype_trans[x] for x in new_cols if x in dtype_trans}
        sql_utils.add_columns_to_table(mariadb_engine, 'output.facebook_campaign_report', new_col_datatype_dct)

    sql_utils.df_to_sql(mariadb_engine, df, 'output.facebook_campaign_report')

def main():
    mariadb_engine = sql_utils.create_engine(settings.MARIADB_CONFIG, db_name='output', db_type='mysql')
    account = FacebookAccount(APP_ID, APP_SECRET, ACCESS_TOKEN)
    LAG_TIME = settings.LAG_TIME
    if sql_utils.table_exists_notempty(mariadb_engine, 'output', 'facebook_campaign_report'):
        latest_date = sql_utils.get_latest_date_in_table(mariadb_engine, 'facebook_campaign_report')
        from_date = (latest_date-timedelta(days=LAG_TIME)).strftime('%Y-%m-%d')
        to_date = datetime.today().strftime('%Y-%m-%d')        
        ingest_facebook_campaigns(account, mariadb_engine, from_date, to_date)
    else:
        for months in range(0, 12, 1):
            print(months)
            from_date, to_date = from_date_to_date(365-30*months, 29)
            ingest_facebook_campaigns(account, mariadb_engine, from_date, to_date)


if __name__ == '__main__':
    main()

# CHECK
# https://developers.facebook.com/docs/marketing-api/attribution
# https://developers.facebook.com/docs/marketing-api/offline-conversions

# GET 
# System User