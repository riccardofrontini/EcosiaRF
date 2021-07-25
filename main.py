import os
import io
import json
import pandas as pd
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError


def accessRemoteData(dataUrl):
    """
    open remote URL and download data
    :param dataUrl: URL string
    :return: string formatted JSON data
    """
    req = Request(dataUrl)
    try:
        response = urlopen(req)
    except ValueError as e:
        print('Invalid URL string', e.reason)
        sys.exit()
    except HTTPError as e:
        print('Error code: ', e.code)
        sys.exit()
    except URLError as e:
        print('Reason: ', e.reason)
        sys.exit()
    else:
        print('Request successful, downloading data.')
        return response.read().decode('utf-8')

def createDataframe(httpData):
    """
    create normalized  pandas dataframe from unserialized JSON data
    :param httpData: string formatted JSON data
    :return: pandas DataFrame
    """
    jsonData = json.loads(httpData)
    return pd.json_normalize(jsonData['data'])

def filterDataframe(dataFrame):
    """
    filter DataFrame
    :param dataFrame: unfiltered DataFrame
    :return: filtered and reformatted DataFrame
    """
    return dataFrame[(dataFrame["gender"]=="F") & \
                (dataFrame['location.state']=="CA")] \
                [["device", "age", "client_time", "amount", "gender", "location.state"]]

def addDateColum(dataFrame):
    """
    create 'date' field from 'client_time'
    :param dataFrame: starting DataFrame
    :return: pandas Series containing dates
    """
    return pd.to_datetime(dataFrame["client_time"], unit='s').dt.date

def aggregateData(dataFrame):
    """
    aggregate data
    :param dataFrame: starting DataFrame
    :return: aggregated DataFrame with renamed columns
    """
    aggregations = {
        'client_time': 'count',
        'amount': 'sum'
    }
    return dataFrame.groupby(['age', 'device', 'date'], as_index=False). \
        agg(aggregations). \
        rename(columns={
            "client_time": "count", "amount": "amount_sum"
        })

def writeToS3(dataFrame, s3_key, s3_secret, s3_bucket):
    """
    serialize DataFrame as a CSV inside an AWS S3 bucket
    :param dataFrame: starting DataFrame
    :param s3_key: the AWS Access key ID
    :param s3_secret: the AWS Access key secret
    :param s3_bucket: the AWS S3 destination bucket name
    :return: None
    """
    opt = {
        'key' : s3_key,
        'secret' : s3_secret
        }
    dataFrame.to_csv('s3://{}/csv/total_events.csv'.format(s3_bucket), storage_options=opt, index=False)

def main():
    rawJSONurl = 'https://raw.githubusercontent.com/localytics/data-viz-challenge/master/data.json'
    s3_id = os.environ.get('AWS_ACCESS_KEY_ID')
    s3_secret = os.environ.get('AWS_SECRET_ACCESS_KEY')
    s3_bucket = os.environ.get('AWS_S3_BUCKET_NAME')

    #download data from remote location
    rawData = accessRemoteData(rawJSONurl)

    #transform raw data to a pandas Dataframe
    df = createDataframe(rawData)
    print('Dataframe created')

    #filter and reformat Dataframe
    df = filterDataframe(df)
    print('Dataframe filtered and reformatted')

    #add a date column for further aggregation
    df['date'] = addDateColum(df)
    print('Date column added to Dataframe')

    # aggregate data
    df = aggregateData(df)
    print('Dataframe aggregated')

    # upload data to S3 as a csv file
    writeToS3(df, s3_id, s3_secret, s3_bucket)
    print('CSV uploaded to S3')
    df.to_csv('./out/total_events.csv', index=False)
    print('Local CSV file generated')
    exit(0)

if __name__ == '__main__':
    main()
