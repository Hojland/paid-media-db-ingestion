from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.objects import AdCampaign

my_app_id = '{app-id}'
my_app_secret = '{appsecret}'
my_access_token = '{access-token}'
FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)
my_account = AdAccount('act_{{adaccount-id}}')
campaigns = my_account.get_campaigns()
print(campaigns)


# campaign insights
# https://developers.facebook.com/docs/marketing-api/insights/
campaign = AdCampaign('<AD_CAMPAIGN_ID>')
params = {
    'date_preset': AdCampaign.Preset.last_7_days,
}
insights = campaign.get_insights(params=params)
print insights