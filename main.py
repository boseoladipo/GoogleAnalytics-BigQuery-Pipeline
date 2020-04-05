# from oauth2client.service_account import ServiceAccountCredentials
import datetime
import logging
from string import Template

import google.auth
import pandas as pd
import psycopg2
from apiclient.discovery import build
from google.cloud import error_reporting
from pytz import timezone
from sqlalchemy import create_engine

import config
from optimize_insert import psql_insert_copy
from sql_functions import delete_from_sql, push_data_to_sql

SCHEMA_NAME = 'ga_staging'
client = error_reporting.Client(service="get_ga_bq_data")

def initialize_analyticsreporting():
    credentials, project = google.auth.default(
    scopes=config.config_vars['SCOPES'])

    # Build the service object.
    analytics = build('analyticsreporting', 'v4', credentials=credentials
                        , cache_discovery=False
                        )

    return analytics

def get_report(analytics, pageToken, today):
    """
    Construct request payload to get report from analytics api.
    Args:
        pageToken (dict): index of result to load from.
    """
    day = today.date().isoformat()
    return analytics.reports().batchGet(
        body={
            'reportRequests': [
            {
            'viewId': config.config_vars['VIEW_ID'],
            'dateRanges': [{'startDate': day, 'endDate': day}],
            'metrics': [{'expression':i} for i in METRICS],
            'dimensions': [{'name':j} for j in DIMENSIONS],
            'pageSize': '10000',
            'pageToken': str(pageToken)
            }]
        }
    ).execute()

def convert_to_dataframe(response):
    """
    Convert repsonse from reporting API to dataframe.
    Args:
        response (dict): response payload.
        """
    for report in response.get('reports', []):
        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = [i.get('name',{}) for i in columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])]
        finalRows = []
        

        for row in report.get('data', {}).get('rows', []):
            dimensions = row.get('dimensions', [])
            metrics = row.get('metrics', [])[0].get('values', {})
            rowObject = {}

            for header, dimension in zip(dimensionHeaders, dimensions):
                rowObject[header] = dimension
            
            
            for metricHeader, metric in zip(metricHeaders, metrics):
                rowObject[metricHeader] = metric

            finalRows.append(rowObject)
        
        
    dataFrameFormat = pd.DataFrame(finalRows)    
    dataFrameFormat.columns = [col.lower().replace('ga:','') for col in dataFrameFormat.columns]   

    return dataFrameFormat      

def load_data_to_gbq(df, table_name):
    df.to_gbq(
                f'{SCHEMA_NAME}.{table_name}', 
                if_exists='replace', 
                chunksize=50000, 
                project_id='cars45-ng-bi'
            )
    print(f'Loaded {len(df)} records to bigquery {table_name}')

def run_process(today):
    analytics = initialize_analyticsreporting()
    startPageToken = 0
    frames = []

    while True:
        response = get_report(analytics, startPageToken,today)
        df = convert_to_dataframe(response)
        print(len(df))
        df = df[df.date!='(other)']
        # df = df[(df['pagepath'].str.contains("ng-\d*", regex=True)) & (df.date!='(other)')]
        df['date']= pd.to_datetime(df['date'])
        frames.append(df)

        try:
            startPageToken = response['reports'][0]['nextPageToken']
        except KeyError:
            print(f"No more records to retrieve for {TABLE_NAME}")
            break
        
    final_df = pd.concat(frames)
    load_data_to_gbq(final_df, TABLE_NAME)
    
def main(data, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
        data (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """
    
    try:
        current_time = datetime.datetime.utcnow()
        log_message = Template('Cloud Function was triggered on $time')
        logging.info(log_message.safe_substitute(time=current_time))
        
        attributes = data['attributes']
        global DIMENSIONS
        DIMENSIONS = attributes['DIMENSIONS'].split(',')
        global METRICS
        METRICS = attributes['METRICS'].split(',')
        
        current_date = datetime.datetime.now(timezone('Africa/Lagos'))
        table_date = current_date if int(current_date.strftime('%H')) > 1 else (current_date - datetime.timedelta(1))

        global TABLE_NAME
        TABLE_NAME = f"""{attributes['TABLE_NAME']}_{table_date.strftime('%Y%m%d')}"""

        run_process(table_date)

    except Exception as error:
        client.report_exception()
        log_message = Template('$error').substitute(error=error)
        logging.error(log_message, exc_info=True)

if __name__ == '__main__':
    main('data', 'context')
