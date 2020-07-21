import os 
import sys
import re
import pandas as pd
import functools
from sqlalchemy.types import String, DateTime, Numeric

from google_auth_oauthlib.flow import InstalledAppFlow
from google.ads.google_ads.client import GoogleAdsClient
from google.ads.google_ads.errors import GoogleAdsException
from google.ads.google_ads.v4.proto import enums

import settings
import sql
from utils import utils, sql_utils


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


def get_enum_translation(enum: str='device'):
    # the enum of the supported enums in google ads 
    # (https://github.com/googleads/google-ads-python/tree/master/google/ads/google_ads/v4/proto/enums)

    if enum == 'device':
        enum_list = enums.device_pb2.DeviceEnum.Device.items()
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


def get_campaign_report_by_device(client: GoogleAdsClient, customer_id: str):
    query = ('SELECT campaign.id, campaign.name, campaign.start_date, campaign.end_date, '
             'metrics.conversions, metrics.conversions_value, metrics.clicks, '
             'metrics.cost_micros, metrics.impressions, metrics.ctr, '
             'segments.device, segments.date '
             'FROM campaign '
             'WHERE campaign.status = "ENABLED" AND '
             'segments.date DURING LAST_30_DAYS '
             'ORDER BY campaign.id')    

    campaign_report = query_ga_campaign(query, client, customer_id)
    trans_dct = get_enum_translation(enum='device')
    campaign_report['device'] = campaign_report['device'].replace(trans_dct)
    campaign_report['cost'] = campaign_report['cost_micros'] / 1000000
    return campaign_report


def get_campaign_report(client: GoogleAdsClient, customer_id: str):
    query = ('SELECT campaign.id, campaign.name, campaign.start_date, campaign.end_date, '
             'metrics.conversions, metrics.conversions_value, metrics.clicks, '
             'metrics.cost_micros, metrics.impressions, metrics.ctr, '
             'segments.date '
             'FROM campaign '
             'WHERE campaign.status = "ENABLED" AND '
             'segments.date DURING LAST_30_DAYS '
             'ORDER BY campaign.id')

    campaign_report = query_ga_campaign(query, client, customer_id)
    campaign_report['cost'] = campaign_report['cost_micros'] / 1000000
    campaign_report['date'] = pd.to_datetime(campaign_report['date'])
    return campaign_report

def get_access_token(client_secrets_path, scopes):
    # A method to get the first access token (after that the client library should refresh it when needed)
    flow = InstalledAppFlow.from_client_secrets_file(
        client_secrets_path, scopes=scopes)

    flow.run_console()

    print('Access token: %s' % flow.credentials.token)
    print('Refresh token: %s' % flow.credentials.refresh_token)

def main():
    client = GoogleAdsClient.load_from_env()
    campaign_report = get_campaign_report_by_device(client, settings.YOUSEE_CUSTOMER_ID)

    # put to mysql database
    cols = ['name', 'date', 'device', 'start_date', 'conversions', 'conversions_value', 'clicks', 'impressions', 'cost', 'ctr']
    campaign_report = campaign_report[cols]
    dtype_trans = sql.get_dtype_trans(campaign_report)
    dtype_trans.update({'name': String(100)})
    dtype_trans.update({'date': DateTime()})
    mariadb_engine = sql_utils.create_engine(settings.MARIADB_CONFIG, db_name='output', db_type='mysql')
    campaign_report.to_sql('google_campaign_report', con=mariadb_engine, dtype=dtype_trans, if_exists='replace', index=False)

    mariadb_engine.execute('CREATE INDEX google_campaign_report_date_IDX USING BTREE ON `output`.google_campaign_report (date);')
    mariadb_engine.execute('CREATE INDEX google_campaign_report_name_IDX USING HASH ON `output`.google_campaign_report (name);')
    mariadb_engine.execute('CREATE INDEX google_campaign_report_device_IDX USING HASH ON `output`.google_campaign_report (device);')

if __name__ == '__main__':
    configured_scopes = [settings.SCOPE]
    #get_access_token('../credentials/google_client_secrets.json', configured_scopes)

    main()