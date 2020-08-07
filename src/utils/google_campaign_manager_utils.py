from datetime import datetime, timedelta
import time
import random
import io
import asyncio

from googleapiclient import http
from oauth2client import client
from oauth2client.client import AccessTokenRefreshError


MIN_RETRY_INTERVAL = 10
# Maximum amount of time between polling requests. Defaults to 10 minutes.
MAX_RETRY_INTERVAL = 10 * 60
# Maximum amount of time to spend polling. Defaults to 1 hour.
MAX_RETRY_ELAPSED_TIME = 60 * 60
# Chunk size to use when downloading report files. Defaults to 32MB.
CHUNK_SIZE = 32 * 1024 * 1024


def find_compatible_fields(cm_client: client, profile_id, report):
    """Finds and adds a compatible dimension/metric to the report."""
    fields = cm_client.reports().compatibleFields().query(
        profileId=profile_id, body=report).execute()

    report_fields = fields['reportCompatibleFields']

    if report_fields['dimensions']:
        # Add a compatible dimension to the report.
        report['criteria']['dimensions'].append({
            'name': report_fields['dimensions'][0]['name']
        })
    elif report_fields['metrics']:
        # Add a compatible metric to the report.
        report['criteria']['metricNames'].append(
            report_fields['metrics'][0]['name'])
    return report


def find_dimension_filters(cm_client: client, profile_id: str, report: dict):
    """Finds and adds a valid dimension filter to the report."""
    # Query advertiser dimension values for report run dates.
    request = {
        'dimensionName': 'dfa:advertiser',
        'endDate': report['criteria']['dateRange']['endDate'],
        'startDate': report['criteria']['dateRange']['startDate']
    }

    values = cm_client.dimensionValues().query(
        profileId=profile_id, body=request).execute()

    return values


def insert_report_resource(cm_client: client, profile_id, report):
    """Inserts the report."""
    inserted_report = cm_client.reports().insert(
        profileId=profile_id, body=report).execute()

    print('\nSuccessfully inserted new report with ID %s.' %
        inserted_report['id'])

    return inserted_report

async def run_report(cm_client: client, profile_id: str, report_id: str):
    def next_sleep_interval(previous_sleep_interval):
        """Calculates the next sleep interval based on the previous."""
        min_interval = previous_sleep_interval or MIN_RETRY_INTERVAL
        max_interval = previous_sleep_interval * 3 or MIN_RETRY_INTERVAL
        return min(MAX_RETRY_INTERVAL, random.randint(min_interval, max_interval))

    try:
        # Run the report.
        report_file = cm_client.reports().run(profileId=profile_id,
                                            reportId=report_id).execute()
        print('File with ID %s has been created.' % report_file['id'])
        # Wait for the report file to finish processing.
        # An exponential backoff strategy is used to conserve request quota.
        sleep = 0
        start_time = time.time()
        while True:
            report_file = cm_client.files().get(reportId=report_id,
                                            fileId=report_file['id']).execute()

            status = report_file['status']
            if status == 'REPORT_AVAILABLE':
                print('File status is %s, ready to download.' % status)
                return report_file
            elif status != 'PROCESSING':
                print('File status is %s, processing failed.' % status)
                return report_file
            elif time.time() - start_time > MAX_RETRY_ELAPSED_TIME:
                print('File processing deadline exceeded.')
                return report_file

            sleep = next_sleep_interval(sleep)
            print('File status is %s, sleeping for %d seconds.' % (status, sleep))
            await asyncio.sleep(sleep)

    except AccessTokenRefreshError:
        print('The credentials have been revoked or expired, please re-run the '
                'application to re-authorize')

def stream_report(cm_client: client, report_id: str, file_id: str):

    try:
        # Retrieve the file metadata.
        report_file = cm_client.files().get(reportId=report_id,
                                            fileId=file_id).execute()

        if report_file['status'] == 'REPORT_AVAILABLE':
            # Prepare a local file to download the report contents to.
            stream_file = io.BytesIO()

            # Create a get request.
            request = cm_client.files().get_media(reportId=report_id, fileId=file_id)

            # Create a media downloader instance.
            # Optional: adjust the chunk size used when downloading the file.
            stream_downloader = http.MediaIoBaseDownload(stream_file, request,
                                                chunksize=CHUNK_SIZE)

            # Execute the get request and download the file.
            download_finished = False
            while download_finished is False:
                status, download_finished = stream_downloader.next_chunk()
                if status:
                    print("Download %d%%." % int(status.progress() * 100))
            stream_file.seek(0)
            
            return stream_file

    except AccessTokenRefreshError:
        print('The credentials have been revoked or expired, please re-run the '
                'application to re-authorize')

#tmp = pd.read_table(stream_file, sep=',', index_col=False, error_bad_lines=False, encoding='utf-8')
