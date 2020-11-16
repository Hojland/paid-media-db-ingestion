import os 
import sys
import re
import pandas as pd
import functools
from datetime import datetime, timedelta
from sqlalchemy.types import String, DateTime, Numeric

from google_auth_oauthlib.flow import InstalledAppFlow
from google.ads.google_ads.client import GoogleAdsClient
from google.ads.google_ads.errors import GoogleAdsException
from google.ads.google_ads.v4.proto import enums

import settings
import sql
from utils import utils, sql_utils
from user_list_service_client import UserListServiceClient


def get_nested_attr(obj, attr, *args):
    """Gets the value of a nested attribute from an object.
    Args:
      obj: an object to retrieve an attribute value from.
      attr: a string of the attribute separated by dots.
    Returns:
      The object attribute value or the given *args if the attr isn't present.
    """
    def _getattr(obj, attr):
        return getattr(obj, attr, *args)

    return functools.reduce(_getattr, [obj] + attr.split('.'))


def get_enum_translation(enum: str):
    # the enum of the supported enums in google ads 
    # (https://github.com/googleads/google-ads-python/tree/master/google/ads/google_ads/v4/proto/enums)

    if enum == 'device':
        enum_list = enums.device_pb2.DeviceEnum.Device.items()
    elif enum == 'conversion_attribution_event_type':
        enum_list = enums.conversion_attribution_event_type_pb2.ConversionAttributionEventTypeEnum.ConversionAttributionEventType.items()
    elif enum == 'conversion_action_category':
        enum_list = enums.conversion_action_category_pb2.ConversionActionCategoryEnum.ConversionActionCategory.items()
    else:
        raise NotImplementedError('This Enum is not yet implemented')
    
    trans_dct = {enum_val: enum_name for enum_name, enum_val in enum_list}
    return trans_dct


def query_ga_campaign(query: str, client: GoogleAdsClient, customer_id: str):
    ga_service = client.get_service('GoogleAdsService', version='v4')

    # Issues a search request using streaming.
    customer_id = customer_id.replace('-', '')
    response = ga_service.search_stream(customer_id, query=query)

    select_section = re.search('(?<=SELECT )(.*)(?= FROM)', query)[0]
    extract_values = select_section.split(', ')

    campaigns = []
    try:
        for batch in response:
            for row in batch.results:
                campaign = {}
                #for col_table_and_name in col_tables_and_names:
                for extract_value in extract_values:
                    attr = get_nested_attr(row, extract_value)
                    col_name = extract_value.split('.')[-1]
                    if type(attr) in [int]:
                        campaign_attr = {
                            col_name: attr
                        }                   
                    else:
                        campaign_attr = {
                            col_name: getattr(attr, 'value')
                        }
                    campaign.update(campaign_attr)
                campaigns.append(campaign)
                
    except GoogleAdsException as ex:
        print(f'Request with ID "{ex.request_id}" failed with status '
            f'"{ex.error.code().name}" and includes the following errors:')
        for error in ex.failure.errors:
            print(f'\tError with message "{error.message}".')
            if error.location:
                for field_path_element in error.location.field_path_elements:
                    print(f'\t\tOn field: {field_path_element.field_name}')
        sys.exit(1)
    df_campaigns = pd.DataFrame.from_dict(campaigns)
    return df_campaigns


def get_campaign_names(client: GoogleAdsClient, customer_id: str):
    query = ('SELECT campaign.id, campaign.name ' 
             'FROM campaign '
             'ORDER BY campaign.id')

    response = query_ga_campaign(query, client, customer_id)
    return response


def get_campaign_report_by_device(client: GoogleAdsClient, customer_id: str, time_period_query: str='DURING LAST_30_DAYS'):
    query = ('SELECT campaign.id, campaign.name, campaign.start_date, campaign.end_date, '
             'metrics.clicks, '
             'metrics.cost_micros, metrics.impressions, metrics.ctr, '
             'segments.device, segments.date '
             'FROM campaign '
             'WHERE campaign.status = "ENABLED" AND '
             f'segments.date {time_period_query} '
             'ORDER BY campaign.id')    

    campaign_report = query_ga_campaign(query, client, customer_id)
    trans_dct = get_enum_translation(enum='device')
    campaign_report['device'] = campaign_report['device'].replace(trans_dct)
    campaign_report['cost'] = campaign_report['cost_micros'] / 1000000
    return campaign_report


def get_conversion_campaign_report(client: GoogleAdsClient, customer_id: str, time_period_query: str='DURING LAST_30_DAYS'):
    query = ('SELECT campaign.id, campaign.name, campaign.start_date, campaign.end_date, '
             'metrics.conversions, metrics.conversions_value, '
             'segments.date, segments.conversion_action_name, segments.conversion_action_category '
             'FROM campaign '
             'WHERE campaign.status = "ENABLED" AND '
             f'segments.date {time_period_query} '
             'ORDER BY campaign.id')

    campaign_report = query_ga_campaign(query, client, customer_id)
    trans_dct = get_enum_translation(enum='conversion_action_category')
    campaign_report['conversion_action_category'] = campaign_report['conversion_action_category'].replace(trans_dct)
    campaign_report['date'] = pd.to_datetime(campaign_report['date'])
    return campaign_report


def get_campaign_report(client: GoogleAdsClient, customer_id: str, time_period_query: str='DURING LAST_30_DAYS'):
    query = ('SELECT campaign.id, campaign.name, campaign.start_date, campaign.end_date, '
             'metrics.clicks, '
             'metrics.cost_micros, metrics.impressions, metrics.ctr, '
             'segments.date '
             'FROM campaign '
             'WHERE campaign.status = "ENABLED" AND '
             f'segments.date {time_period_query} '
             'ORDER BY campaign.id')    

    campaign_report = query_ga_campaign(query, client, customer_id)
    campaign_report['cost'] = campaign_report['cost_micros'] / 1000000
    return campaign_report


def get_platform_type_brandorproduct_campaign_from_naming(name: pd.Series):
    col_split = name.str.split(' - ', n=3, expand=True)
    col_split.loc[col_split[2].str.contains('BrandOnly', na=False), 3] = 'BrandOnly'
    col_split[2] = col_split[2].str.replace(r'\s{1,2}\[BrandOnly\]', '')
    platform = col_split[0]
    campaign_type = col_split[1]
    brandorproduct = col_split[2]
    campaign = col_split[3]
    return platform, campaign_type, brandorproduct, campaign


## THIS IS DEPRECATED FOR USE HERE
def get_access_token_installed_app(client_secrets_path, scopes):
    # A method to get the first access token (after that the client library should refresh it when needed)
    flow = InstalledAppFlow.from_client_secrets_file(
        client_secrets_path, scopes=scopes)

    flow.run_console()

    print('Access token: %s' % flow.credentials.token)
    print('Refresh token: %s' % flow.credentials.refresh_token)

def main():
    client = GoogleAdsClient.load_from_env()
    mariadb_engine = sql_utils.create_engine(settings.MARIADB_CONFIG, db_name='output', db_type='mysql')
    LAG_TIME = settings.LAG_TIME
    # put google_campaign_report to mysql database
    if sql_utils.table_exists_notempty(mariadb_engine, 'output', 'google_campaign_report'):
        latest_date = sql_utils.get_latest_date_in_table(mariadb_engine, 'google_campaign_report')
    else:
        latest_date = datetime.today()
        LAG_TIME = 365
    from_date = (latest_date-timedelta(days=LAG_TIME)).strftime('%Y-%m-%d')
    to_date = (datetime.today()+timedelta(days=1)).strftime('%Y-%m-%d')

    google_campaign_report = get_campaign_report(client, settings.YOUSEE_CUSTOMER_ID, time_period_query=f'BETWEEN "{from_date}" AND "{to_date}"')
    google_campaign_report['platform'], google_campaign_report['campaign_type'], \
        google_campaign_report['brandorproduct'], google_campaign_report['campaign'] = \
            get_platform_type_brandorproduct_campaign_from_naming(google_campaign_report['name'])

    cols = ['name', 'platform', 'campaign_type', 'brandorproduct', 'campaign', 'date', 'start_date', 'clicks', 'impressions', 'cost', 'ctr']
    google_campaign_report = google_campaign_report[cols]
    dtype_trans = sql.get_dtype_trans(google_campaign_report)
    dtype_trans.update({'name': String(150)})
    dtype_trans.update({'campaign': String(80)})
    dtype_trans.update({'date': DateTime()})

    if sql_utils.table_exists_notempty(mariadb_engine, 'output', 'google_campaign_report'):
        sql_utils.delete_date_entries_in_table(mariadb_engine, from_date, 'google_campaign_report')
    google_campaign_report.to_sql('google_campaign_report', con=mariadb_engine, dtype=dtype_trans, if_exists='append', index=False)

    #mariadb_engine.execute('CREATE INDEX google_campaign_report_date_IDX USING BTREE ON `output`.google_campaign_report (date);')
    #mariadb_engine.execute('CREATE INDEX google_campaign_report_name_IDX USING HASH ON `output`.google_campaign_report (name, platform, campaign_type, brandorproduct, campaign);')

    # put google_device_campaign_report to mysql database
    if sql_utils.table_exists_notempty(mariadb_engine, 'output', 'google_device_campaign_report'):
        latest_date = sql_utils.get_latest_date_in_table(mariadb_engine, 'google_device_campaign_report')
    else:
        latest_date = datetime.today()
        LAG_TIME = 365
    from_date = (latest_date-timedelta(days=LAG_TIME)).strftime('%Y-%m-%d')
    to_date = (datetime.today()+timedelta(days=1)).strftime('%Y-%m-%d')

    google_device_campaign_report = get_campaign_report_by_device(client, settings.YOUSEE_CUSTOMER_ID, time_period_query=f'BETWEEN "{from_date}" AND "{to_date}"')
    google_device_campaign_report['platform'], google_device_campaign_report['campaign_type'], \
        google_device_campaign_report['brandorproduct'], google_device_campaign_report['campaign'] = \
            get_platform_type_brandorproduct_campaign_from_naming(google_device_campaign_report['name'])

    cols = ['name', 'platform', 'campaign_type', 'brandorproduct', 'campaign', 'date', 'device', 'start_date', 'clicks', 'impressions', 'cost', 'ctr']
    google_device_campaign_report = google_device_campaign_report[cols]
    dtype_trans = sql.get_dtype_trans(google_device_campaign_report)
    dtype_trans.update({'name': String(150)})
    dtype_trans.update({'campaign': String(80)})
    dtype_trans.update({'date': DateTime()})

    if sql_utils.table_exists_notempty(mariadb_engine, 'output', 'google_device_campaign_report'):
        sql_utils.delete_date_entries_in_table(mariadb_engine, from_date, 'google_device_campaign_report')
    google_device_campaign_report.to_sql('google_device_campaign_report', con=mariadb_engine, dtype=dtype_trans, if_exists='append', index=False)

    #mariadb_engine.execute('CREATE INDEX device_campaign_report_date_IDX USING BTREE ON `output`.google_device_campaign_report (date);')
    #mariadb_engine.execute('CREATE INDEX device_campaign_report_name_IDX USING HASH ON `output`.google_device_campaign_report (name, platform, campaign_type, brandorproduct, campaign);')
    #mariadb_engine.execute('CREATE INDEX device_campaign_report_device_IDX USING HASH ON `output`.google_device_campaign_report (device);')

    # put google_conversion_campaign_report to mysql database
    if sql_utils.table_exists_notempty(mariadb_engine, 'output', 'google_conversion_campaign_report'):
        latest_date = sql_utils.get_latest_date_in_table(mariadb_engine, 'google_conversion_campaign_report')
    else:
        latest_date = datetime.today()
        LAG_TIME = 365
    from_date = (latest_date-timedelta(days=LAG_TIME)).strftime('%Y-%m-%d')
    to_date = (datetime.today()+timedelta(days=1)).strftime('%Y-%m-%d')

    google_conversion_campaign_report = get_conversion_campaign_report(client, settings.YOUSEE_CUSTOMER_ID, time_period_query=f'BETWEEN "{from_date}" AND "{to_date}"')
    google_conversion_campaign_report['platform'], google_conversion_campaign_report['campaign_type'], \
        google_conversion_campaign_report['brandorproduct'], google_conversion_campaign_report['campaign'] = \
            get_platform_type_brandorproduct_campaign_from_naming(google_conversion_campaign_report['name'])


    cols = ['name', 'platform', 'campaign_type', 'brandorproduct', 'campaign', 'date', 'start_date', 'conversion_action_name', 'conversion_action_category', 'conversions', 'conversions_value']
    google_conversion_campaign_report = google_conversion_campaign_report[cols]
    dtype_trans = sql.get_dtype_trans(google_conversion_campaign_report)
    dtype_trans.update({'name': String(150)})
    dtype_trans.update({'campaign': String(80)})
    dtype_trans.update({'date': DateTime()})
    mariadb_engine = sql_utils.create_engine(settings.MARIADB_CONFIG, db_name='output', db_type='mysql')

    if sql_utils.table_exists_notempty(mariadb_engine, 'output', 'google_conversion_campaign_report'):
        sql_utils.delete_date_entries_in_table(mariadb_engine, from_date, 'google_conversion_campaign_report')
    google_conversion_campaign_report.to_sql('google_conversion_campaign_report', con=mariadb_engine, dtype=dtype_trans, if_exists='append', index=False)

    #mariadb_engine.execute('CREATE INDEX google_conversion_campaign_report_date_IDX USING BTREE ON `output`.google_conversion_campaign_report (date);')
    #mariadb_engine.execute('CREATE INDEX google_conversion_campaign_report_name_IDX USING HASH ON `output`.google_conversion_campaign_report (name, platform, campaign_type, brandorproduct, campaign);')
    #mariadb_engine.execute('CREATE INDEX google_conversion_campaign_report_conversionaction_IDX USING HASH ON `output`.google_conversion_campaign_report (conversion_action_name);')



if __name__ == '__main__':
    configured_scopes = [settings.SCOPE]
    #get_access_token('../credentials/google_client_secrets.json', configured_scopes)

    main()