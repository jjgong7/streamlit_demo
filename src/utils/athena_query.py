"""Functions for querying and retreiving results from AWS Athena"""
from collections import namedtuple, defaultdict
import time
import boto3
import pandas as pd
import io

query_listeners = []

def wait_time(elapsed):
    """Returns staggered wait times based on a step function"""
    if elapsed < 5:
        return 1
    if elapsed < 60:
        return 10
    return 60


class AthenaError(Exception):
    """General error from Athena"""

    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason

    def __str__(self):
        return repr(self.reason)


class AthenaQueryCancelledError(AthenaError):
    """Thrown when the Athena query was cancelled (e.g. via the UI)"""


class AthenaQueryFailedError(AthenaError):
    """Thrown when the Athena query failed"""


class AthenaQueryTimeout(AthenaError):
    """Thrown when the Athena query timed out (no result within a specified time)"""


def query_athena(query, session, output_location, workgroup='primary', timeout=300):
    """Run the given athena query and return when the query is complete."""

    client = session.client('athena')

    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
        'Database': 'ghostery-metrics',
        'Catalog': 'AwsDataCatalog'
        },
        ResultConfiguration={
            'OutputLocation': output_location,
        },
        WorkGroup=workgroup,
    )
    elapsed = 0

    # rudimentary event listener for queries
    for fn in query_listeners:
        fn(query)

    while True:
        if elapsed >= timeout:
            raise AthenaQueryTimeout(
                'Athena query not completed after {}s'.format(timeout))
        # poll for results
        poll_wait = min(wait_time(elapsed), timeout - elapsed)
        # print('[wait]', poll_wait)
        time.sleep(poll_wait)
        elapsed += poll_wait

        query_status = client.get_query_execution(
            QueryExecutionId=response['QueryExecutionId'])
        # print('[status]', query_status)
        if 'QueryExecution' not in query_status:
            continue

        status = query_status['QueryExecution']['Status']
        state = status['State']
        if state == 'SUCCEEDED':
            log_query_summary(query_status['QueryExecution'])
            return query_status
        if state == 'FAILED':
            raise AthenaQueryFailedError(status['StateChangeReason'])
        if state == 'CANCELLED':
            raise AthenaQueryCancelledError(status['StateChangeReason'])
        if state in ('RUNNING', 'QUEUED'):
            continue

        raise RuntimeError(
            'Athena query unexpected state: {}'.format(state))


def get_query_results(query, session, extract_header=True):
    """Get the results for a query, using the result object returned from query_athena"""

    client = session.client('athena')
    query_execution_id = query['QueryExecution']['QueryExecutionId']
    paginator = client.get_paginator('get_query_results')
    result_row_factory = None
    for page in paginator.paginate(
            QueryExecutionId=query_execution_id,
            PaginationConfig={'PageSize': 1000}):
        for row in page['ResultSet']['Rows']:
            row_data = [cell['VarCharValue']
                        if 'VarCharValue' in cell else None for cell in row['Data']]
            if not extract_header:
                yield row_data
            elif result_row_factory is None:
                result_row_factory = namedtuple(
                    'QueryResultRow', row_data)
            else:
                yield result_row_factory._make(row_data)


def get_result_df(query_result, session, **pandas_args):
    """Get results from an Athena query as a pandas dataframe"""
    # extract buck and path from OutputLocation
    parts = query_result['QueryExecution']['ResultConfiguration']['OutputLocation'].split(
        '/')
    bucket = parts[2]
    path = '/'.join(parts[3:])

    s3 = session.resource('s3')

    s3resp = s3.Bucket(bucket).Object(key=path).get()
    return pd.read_csv(io.BytesIO(s3resp['Body'].read()), encoding='utf8', **pandas_args)

def log_query_summary(query_execution):
    if 'Statistics' in query_execution:
        query_id = query_execution['QueryExecutionId']
        stats = query_execution['Statistics']
        total_time = round(stats['TotalExecutionTimeInMillis'] / 1000, 1)
        queue_time = round(stats['QueryQueueTimeInMillis'] / 1000, 0)

        def sizeof_fmt(num, suffix='B'):
            for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
                if abs(num) < 1024.0:
                    return "%3.1f%s%s" % (num, unit, suffix)
                num /= 1024.0
            return "%.1f%s%s" % (num, 'Yi', suffix)

        data_scanned = sizeof_fmt(stats['DataScannedInBytes'])

        print(f'[ATHENA]: Query {query_id} completed in {total_time}s ({queue_time}s queued). Scanned {data_scanned}')

# Additional Functions
# Function to run the query and retrieve query ID and return as a dataframe
def get_query_df(query, session, output_location='s3://athena-bi-query-results/',  **pandas_args):
    result = query_athena(query, session, output_location)
    q_result = get_result_df(result, session, **pandas_args)
    return q_result

# Function to convert datetime into a string format
def time_conversion(time):
    time = pd.Timestamp(time.year,time.month,time.day).strftime('%Y-%m-%d')
    return time
