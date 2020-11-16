import os 
import sys
import re
import pandas as pd
import functools
import asyncio
from datetime import datetime, timedelta
from sqlalchemy.types import String, DateTime, Numeric
import sqlalchemy
import jmespath

from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient import discovery
import httplib2
from oauth2client import client
from oauth2client.client import AccessTokenRefreshError

import settings
import sql
from utils import utils, sql_utils, google_campaign_manager_utils

API_NAME = 'dfareporting'
API_USER_ID = os.getenv('GOOGLE_CM_API_USER_ID')
API_VERSION = 'v3.3'
API_SCOPES = ['https://www.googleapis.com/auth/dfareporting',
              'https://www.googleapis.com/auth/dfatrafficking',
              'https://www.googleapis.com/auth/ddmconversions']
LAG_TIME = 365 # Lag time of the upload of conversions in days


def delegated_access_service_account():
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        os.getenv('GOOGLE_ADS_PATH_TO_PRIVATE_KEY_FILE'),
        scopes=API_SCOPES)
    return credentials


def query_dfa_campaign(query: str, client: client, profile_id: str):

    request = client.campaigns().list(profileId=profile_id)
    cols = ['id', 'name'] # temporary until better is found
    campaigns = []
    try:
        while True:
            response = request.execute()
            for campaign_attrs in response['campaigns']:
                campaign = {}
                #for col_table_and_name in col_tables_and_names:
                for col in cols:
                    campaign_attr = {
                        col: campaign_attrs[col]
                    }                   
                    campaign.update(campaign_attr)
                campaigns.append(campaign)

            if response['campaigns'] and response['nextPageToken']:
                request = client.campaigns().list_next(request, response)
            else:
                break
                
    except AccessTokenRefreshError:
        print('The credentials have been revoked or expired, please re-run the '
            'application to re-authorize')
        sys.exit(1)
    df_campaigns = pd.DataFrame.from_dict(campaigns)
    return df_campaigns


def define_conversion_report(from_date, to_date):
    """Defines the report."""
    # Define a date range to report on. This example uses explicit start and end
    # dates to mimic the "LAST_30_DAYS" relative date range.

    report = {
        # Set the required fields "name" and "type".
        'name': 'conversion_report',
        'type': 'STANDARD',
        # Set optional fields.
        'fileName': 'conversion_report',
        'format': 'CSV'
    }

    # Create a report criteria.
    criteria = {
        'dateRange': {
            'startDate': from_date,
            'endDate': to_date
        },
        'dimensions': [
            {'name': 'dfa:date'}, {'name': 'dfa:campaign'}, {'name': 'dfa:activity'}, {'name': 'dfa:advertiser'}, {'name': 'dfa:site'}
            ],
        'metricNames': ['dfa:totalConversions', 'dfa:totalConversionsRevenue',
                        'dfa:activityClickThroughConversions', 'dfa:activityViewThroughConversions']
    }

    # Add the criteria to the report resource.
    report['criteria'] = criteria

    # Add the dimension filters necessary (use  google_campaign_manager_utils.add_dimension_filters(cm_client, profile_id, report) 
    # with the rest of the report setup)
    report['criteria']['dimensionFilters'] = [{
        'kind': 'dfareporting#dimensionValue',
        'etag': '"FAQKY2RK6k6nl_HQY9ED1Uxi0hg"',
        'dimensionName': 'dfa:advertiser',
        'value': 'YOUSEE_DISPLAY',
        'id': '8526768',
    },  {
        'kind': 'dfareporting#dimensionValue',
        'etag': '"5I7yjfTWFJCejGwtjMqsgc9kh6I"',
        'dimensionName': 'dfa:advertiser',
        'value': 'YOUSEE_AFFILIATE',
        'id': '8513510'
    }]
    return report

def define_campaign_report(from_date, to_date):
    """Defines the report."""
    # Define a date range to report on. This example uses explicit start and end
    # dates to mimic the "LAST_30_DAYS" relative date range.

    report = {
        # Set the required fields "name" and "type".
        'name': 'campaign_report',
        'type': 'STANDARD',
        # Set optional fields.
        'fileName': 'campaign_report',
        'format': 'CSV'
    }

    # Create a report criteria.
    criteria = {
        'dateRange': {
            'startDate': from_date,
            'endDate': to_date
        },
        'dimensions': [
            {'name': 'dfa:date'}, {'name': 'dfa:campaign'}, {'name': 'dfa:advertiser'}, {'name': 'dfa:site'}
            ],
        'metricNames': ['dfa:dbmCost', 'dfa:mediaCost', 'dfa:clicks', 'dfa:impressions']
    }

    # Add the criteria to the report resource.
    report['criteria'] = criteria

    # Add the dimension filters necessary (use  google_campaign_manager_utils.add_dimension_filters(cm_client, profile_id, report) 
    # with the rest of the report setup)
    report['criteria']['dimensionFilters'] = [{
        'kind': 'dfareporting#dimensionValue',
        'etag': '"FAQKY2RK6k6nl_HQY9ED1Uxi0hg"',
        'dimensionName': 'dfa:advertiser',
        'value': 'YOUSEE_DISPLAY',
        'id': '8526768',
    },  {
        'kind': 'dfareporting#dimensionValue',
        'etag': '"5I7yjfTWFJCejGwtjMqsgc9kh6I"',
        'dimensionName': 'dfa:advertiser',
        'value': 'YOUSEE_AFFILIATE',
        'id': '8513510'
    }]
    return report

async def create_run_and_stream_report(cm_client: client, profile_id: str, report: dict):
    # insert report
    google_report = google_campaign_manager_utils.insert_report_resource(cm_client, profile_id, report)

    # run report
    report_id = google_report['id']
    file = await google_campaign_manager_utils.run_report(cm_client, profile_id, report_id)

    # stream report
    file_id = file['id']
    report_stream = google_campaign_manager_utils.stream_report(cm_client, report_id, file_id)
    df = pd.read_table(report_stream, sep=',', index_col=False, error_bad_lines=False, encoding='utf-8', skiprows=13)
    return df[:-1]

def from_date_to_date(days_since_start, plus_days):
    from_date = datetime.today()-timedelta(days=days_since_start)
    to_date = from_date+timedelta(days=plus_days)
    return from_date.strftime('%Y-%m-%d'), to_date.strftime('%Y-%m-%d')

async def get_365_days_conversion_report(cm_client, profile_id):
    report_async_gens = []
    for months in range(0, 12, 2):
        from_date, to_date = from_date_to_date(365-30*months, 59)
        report_asyc_gen = await create_run_and_stream_report(cm_client, profile_id, define_conversion_report(from_date, to_date))
        report_async_gens.append(report_asyc_gen)

    from_date, to_date = from_date_to_date(365-60*6, 5)
    last_five_report_asyc_gen = await create_run_and_stream_report(cm_client, profile_id, define_conversion_report(from_date, to_date))
    report_async_gens.append(last_five_report_asyc_gen)
    reports = await asyncio.gather(*(report for report in report_async_gens))
    return reports

def get_brand_product_campaign_name(campaign: pd.Series):
    campaign = fix_backslash(campaign)
    campaign = campaign.str.replace('(\s|~)\{.*\}', '')
    col_split = campaign.str.split('_|~', n=2, expand=True)
    name = campaign
    brand = col_split[0]
    product = col_split[1]
    campaign = col_split[2]
    return name, brand, product, campaign

def fix_backslash(campaign: pd.Series):
    campaign = campaign.str.replace('\\', ' ')
    return campaign

def split_site(site: pd.Series):
    col_split = site.str.split('-', n=1, expand=True)
    site = col_split[0]
    site_media = col_split[1]
    return site, site_media

def get_programmatic_campaigns(db_engine: sqlalchemy.engine, campaign: pd.Series):
    buytype = pd.read_sql('''
    SELECT campaign_name, sum(dbm_cost) as 'dbm_cost', sum(media_cost) as 'media_cost',
    CASE 
        WHEN sum(dbm_cost) = 0 AND sum(media_cost) > 0 THEN 'IO'
        WHEN sum(dbm_cost) > 0 AND sum(media_cost) = 0 THEN 'Programmatic'
        WHEN sum(dbm_cost) = 0 AND sum(media_cost) = 0 THEN 'Not defined'
        ELSE 'Mixed IO and Programmatic'
    END as 'buy_type'
    FROM `output`.google_cm_campaign_report
    GROUP  BY campaign_name
    ''', db_engine)
    
    #translate_dct = utils.pd_to_translate_dict(buytype, 'campaign_name', 'buy_type')
    #buytype_campaign = campaign.replace(translate_dct)
    buytype_campaign = buytype[['campaign_name', 'buy_type']]
    return buytype_campaign

def main():
    credentials = delegated_access_service_account()
    http = credentials.authorize(http=httplib2.Http())
    cm_client = discovery.build(API_NAME, API_VERSION, http=http)

    profile_id = API_USER_ID
    LAG_TIME = settings.LAG_TIME
    try:
        ### get the first year of conversion report
        #df_list =  asyncio.run(get_365_days_conversion_report(cm_client, profile_id))
        #conversion_df = pd.concat(df_list)
        ### get the first year of campaign report
        #report = define_campaign_report(days=365)
        #campaign_df = asyncio.run(create_run_and_stream_report(cm_client, profile_id, report))

        mariadb_engine = sql_utils.create_engine(settings.MARIADB_CONFIG, db_name='output', db_type='mysql')

        # get conversion report
        if sql_utils.table_exists_notempty(mariadb_engine, 'output', 'google_cm_conversion_report'):
            latest_date = sql_utils.get_latest_date_in_table(mariadb_engine, 'google_cm_conversion_report')
        else:
            latest_date = datetime.today()
            LAG_TIME = 365
        from_date = (latest_date-timedelta(days=LAG_TIME)).strftime('%Y-%m-%d')
        to_date = datetime.today().strftime('%Y-%m-%d')
        report = define_conversion_report(from_date, to_date)
        conversion_df = asyncio.run(create_run_and_stream_report(cm_client, profile_id, report))
        conversion_df = conversion_df.rename({'Date': 'date', 'Campaign': 'campaign', 'Activity': 'activity', #'Creative': 'creative',
                                               'Total Conversions': 'conversions', 'Total Revenue': 'conversions_value',
                                               'Click-through Conversions': 'ctc', 'View-through Conversions': 'vtc', 'Advertiser': 'advertiser',
                                               'Site (DCM)': 'site'}, axis=1)
        conversion_df['site'], conversion_df['site_media'] = split_site(conversion_df['site'])
        conversion_df['campaign_name'], conversion_df['brand'], \
            conversion_df['product'], conversion_df['campaign'] = get_brand_product_campaign_name(conversion_df['campaign'])
        conversion_df['conversions_value'] = conversion_df['conversions_value'] / 1000

        dtype_trans = sql.get_dtype_trans(conversion_df)
        dtype_trans.update({'campaign_name': String(150), 'campaign': String(150), 'site': String(100)})
        dtype_trans.update({'activity': String(100)})
        #dtype_trans.update({'creative': String(100)})
        dtype_trans.update({'date': DateTime()})

        # delete entries that may have been updated
        if sql_utils.table_exists_notempty(mariadb_engine, 'output', 'google_cm_conversion_report'):
            sql_utils.delete_date_entries_in_table(mariadb_engine, from_date, 'google_cm_conversion_report')

        # insert into sql
        conversion_df.to_sql('google_cm_conversion_report', con=mariadb_engine, dtype=dtype_trans, if_exists='append', index=False)

        #mariadb_engine.execute('CREATE INDEX google_cm_conversion_report_date_IDX USING BTREE ON `output`.google_cm_conversion_report (date);')
        #mariadb_engine.execute('CREATE INDEX google_cm_conversion_report_dim_IDX USING HASH ON `output`.google_cm_conversion_report (brand, product, campaign_name, activity, advertiser);') #creative

        # get campaign report
        if sql_utils.table_exists_notempty(mariadb_engine, 'output', 'google_cm_campaign_report'):
            latest_date = sql_utils.get_latest_date_in_table(mariadb_engine, 'google_cm_campaign_report')
        else:
            latest_date = datetime.today()
            LAG_TIME = 365
        from_date = (latest_date-timedelta(days=LAG_TIME)).strftime('%Y-%m-%d')
        to_date = datetime.today().strftime('%Y-%m-%d')
        report = define_campaign_report(from_date, to_date)
        campaign_df = asyncio.run(create_run_and_stream_report(cm_client, profile_id, report))
        campaign_df = campaign_df.rename({'Date': 'date', 'Campaign': 'campaign',
                                          'DBM Cost (Account Currency)': 'dbm_cost', 'Clicks': 'clicks', 'Impressions': 'impressions',
                                          'Advertiser': 'advertiser', 'Media Cost': 'media_cost', 'Site (DCM)': 'site'}, axis=1)
        campaign_df['campaign_name'], campaign_df['brand'], \
            campaign_df['product'], campaign_df['campaign'] = get_brand_product_campaign_name(campaign_df['campaign'])
        campaign_df['site'], campaign_df['site_media'] = split_site(campaign_df['site'])
        campaign_df['media_spend'] = campaign_df['media_cost'] + campaign_df['dbm_cost']
        dtype_trans = sql.get_dtype_trans(campaign_df)
        dtype_trans.update({'campaign_name': String(150), 'campaign': String(150), 'site': String(100)})
        dtype_trans.update({'creative': String(100)})
        dtype_trans.update({'date': DateTime()})

        # delete entries that may have been updated
        if sql_utils.table_exists_notempty(mariadb_engine, 'output', 'google_cm_campaign_report'):
            sql_utils.delete_date_entries_in_table(mariadb_engine, from_date, 'google_cm_campaign_report')

        # insert into sql
        campaign_df.to_sql('google_cm_campaign_report', con=mariadb_engine, dtype=dtype_trans, if_exists='append', index=False)

        #mariadb_engine.execute('CREATE INDEX google_cm_campaign_report_date_IDX USING BTREE ON `output`.google_cm_campaign_report (date);')
        #mariadb_engine.execute('CREATE INDEX google_cm_campaign_report_dim_IDX USING HASH ON `output`.google_cm_campaign_report (brand, product, campaign_name, advertiser, site);')

        buytype_campaign = get_programmatic_campaigns(mariadb_engine, campaign_df['campaign_name'])
        buytype_campaign.to_sql('google_cm_programmatic_dim', con=mariadb_engine, if_exists='replace', index=False)

    except AccessTokenRefreshError:
        print('The credentials have been revoked or expired, please re-run the '
                'application to re-authorize')


if __name__ == '__main__':
    main()

# made from looking at the dimension filter possibilites to find different advertisers neceessary for getting the metrics
#dimension_filter_pos = google_campaign_manager_utils.find_dimension_filters(cm_client, profile_id, report)
#campaign_df.groupby(['campaign'])['cost'].sum()