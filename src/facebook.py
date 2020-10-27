import requests
import urllib
import jmespath
import numpy as np
import json

from utils import utils

APP_ID = '2829655310472691'
APP_SECRET = '9927ccb2fd615ac4ece7165b1e6e7e8c'
ACCESS_TOKEN = 'EAAoNjneeFfMBAFtaKzEyXZC1s5ib4ssCfIFne1GhpubIfT3tzvjGzZB5toKaDgU7QNPcC8TLGqaxEBgTU36tx7YrfgPs50u4VAni8AlmnTHzc5DT9C6GQqFes9ZBV5sbgVP2nunIIxnG7nNGkyoqLRUW1WblNwZD'


class FacebookAccount:
    def __init__(self, app_id, app_secret, access_token):
        self.app_id = app_id
        self.app_secret = app_secret
        self.access_token = access_token
        self.api_version = 'v8.0'
        self.base_url = 'https://graph.facebook.com/'

    def request_facebook(self, edge: str, params: dict={}, fields: list=[], request_type='GET'):
        params.update({'access_token': self.access_token})

        if fields:
            params.update({'fields': ','.join(fields)})
        try:
            if request_type == 'GET':
                res = requests.get(f'{self.base_url}/{self.api_version}/{edge}',
                    params=params).json()
                if not res['data'] and 'after' in params:
                    print('No further results')
                    return res['data'], None
                return res['data'], res['paging']['cursors']
            elif request_type == 'POST':
                res = requests.post(f'{self.base_url}/{self.api_version}/{edge}',
                    params=params).json()
                if not res['data'] and 'after' in params:
                    print('No further results')
                    return res['data'], None
                return res['data'], res['paging']['cursors']
            else:
                raise NotImplementedError()
        except:
            print('token expired or api overheat')

    def batch_request_facebook(self, edge: str, batch_lst: list, params: dict={}, fields: list=[], request_type='GET'):
        req_params = {'access_token': self.access_token}

        if fields:
            params.update({'fields': ','.join(fields)})
        
        params = '&'.join(f"{key}={val}" for (key,val) in params.items())
        batch = [
            {'method': request_type,
            'relative_url': f'{self.api_version}/{batch_obj}/{edge}?{params}'} for batch_obj in batch_lst]
        req_params.update({'batch': batch})
        res = requests.post(self.base_url,
            json=req_params).text
        return res
        #if not res['data'] and 'after' in params:
        #    print('No further results')
        #    return res['data'], None
        #return res['data'], res['paging']['cursors']

    def get_campaigns(self, account_id: str='act_1377918625828738'):
        data = []
        res, paging = self.request_facebook(edge=f'{account_id}/campaigns', params={'limit': '200'})
        data.extend(res)
        while paging:
            res, paging = self.request_facebook(edge=f'{account_id}/campaigns', params={'limit': '200', 'after': paging['after']})
            data.extend(res)
        campaign_ids = jmespath.search('[*].id', data)
        return campaign_ids

    def get_insights(self, campaign: str):
        res = self.request_facebook(edge=f'{campaign}/insights', params={'limit': '200'}, fields=['impressions', 'spend', 'campaign_name', 'clicks', 'conversions', 'ctr'])
        return res
    
    def get_long_lived_access_token(self):
        payload = {
            'client_id': self.app_id, 'client_secret': self.app_secret,
            'grant_type': 'fb_exchange_token', 'fb_exchange_token': self.access_token}
        res = self.request_facebook(edge='oauth/access_token', params=payload)
        return res['access_token'], res['expires_in']

    def get_campaign_insights_batch(self, campaign_lst: list):
        campaign_lst_lst = utils.split_list(campaign_lst, 50)
        res_lst = []
        for campaign_batch in campaign_lst_lst:
            batch_res = self.batch_request_facebook(edge=f'insights', batch_lst=campaign_batch, fields=['impressions', 'spend', 'campaign_name']) #, 'ad_id', 'adset_id&level=ad'
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

# CHECK
# https://developers.facebook.com/docs/marketing-api/attribution
# https://developers.facebook.com/docs/marketing-api/offline-conversions

# GET 
# System User

# MAKE
# Date filtering

# TODO
# How can we distribute by variable

def main():
    account = FacebookAccount(APP_ID, APP_SECRET, ACCESS_TOKEN)
    campaigns = account.get_campaigns()
    res = account.get_insights(campaigns[1])
    df_insights = account.get_campaign_insights_batch(campaigns)

if __name__ == '__main__':
    main()



# note to self
# make the whole app, and then get standard-access or a system user to get an access token without expiry

# Batch
curl \
-F 'access_token=<ACCESS_TOKEN>' \
-F 'batch=[ \
  { \
    "method": "GET", \
    "relative_url": "v8.0/<CAMPAIGN_ID_1>/insights?fields=impressions,spend,ad_id,adset_id&level=ad" \
  }, \
  { \
    "method": "GET", \
    "relative_url": "v8.0/<CAMPAIGN_ID_2>/insights?fields=impressions,spend,ad_id,adset_id&level=ad" \
  }, \
  { \
    "method": "GET", \
    "relative_url": "v8.0/<CAMPAIGN_ID_3>/insights?fields=impressions,spend,ad_id,adset_id&level=ad" \
  } \
]' \
'https://graph.facebook.com'