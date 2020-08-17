import requests
import urllib

APP_ID = '2829655310472691'
APP_SECRET = '9927ccb2fd615ac4ece7165b1e6e7e8c'
ACCESS_TOKEN = 'EAAoNjneeFfMBAIwzZAvZAX5zgUh0BO1zs6bBm85xcMFdeDwSfsMhYXiIBhwC65AAZBvZAstfmBgUjlGUk7MhyHgZBroLuZBCm3QUP5ByoPZAHJjl13HJaO9aZB8RFjmTZAAfAaej8eBf4HZB0edZBLSzXf9sg6dC6v3oV7dviTeMO6ZA0gZDZD'


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
            json=req_params).json()
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
        data = [dat['id'] for dat in data]
        return data

    def get_insights(self, campaign_lst: list):
        res = self.batch_request_facebook(edge=f'insights', batch_lst=campaign_lst, fields=['impressions', 'spend', 'ad_id', 'adset_id'])
        return res
    
    def get_long_lived_access_token(self):
        payload = {
            'client_id': self.app_id, 'client_secret': self.app_secret,
            'grant_type': 'fb_exchange_token', 'fb_exchange_token': self.access_token}
        res = self.request_facebook(edge='oauth/access_token', params=payload)
        return res['access_token'], res['expires_in']

    def get_campaign_insights_batch():
        raise NotImplementedError()


def main():
    account = FacebookAccount(APP_ID, APP_SECRET, ACCESS_TOKEN)
    campaigns = account.get_campaigns()
    res = account.get_insights(campaigns[0:1])

if __name__ == '__main__':
    main()

campaigns


# note to self
# make the whole app, and then get standard-access or a system user to get an access token without expiry

# Btach
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