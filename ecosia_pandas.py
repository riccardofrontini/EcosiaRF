import os
import io
import json
import pandas as pd
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError


def accessRemoteData(dataUrl):
    req = Request(dataUrl)
    try:
        response = urlopen(req)
    except ValueError as e:
        print('Invalid URL string', e.reason)
    except HTTPError as e:
        print('Error code: ', e.code)
    except URLError as e:
        print('Reason: ', e.reason)
    else:
        print('Request successful, downloading data.')
        return response.read().decode('utf-8')

def createDataframe(httpData):
    jsonData = json.loads(httpData)
    return pd.json_normalize(jsonData['data'])

def filterDataframe(dataFrame):
    return dataFrame[(dataFrame["gender"]=="F") & \
                (dataFrame['location.state']=="CA")] \
                [["device", "age", "client_time", "amount"]]

def addDateColum(dataFrame):
    return pd.to_datetime(dataFrame["client_time"], unit='s').dt.date

def aggregateData(dataFrame):
    aggregations = {
        'client_time': 'count',
        'amount': 'sum'
    }
    return dataFrame.groupby(['age', 'device', 'date'], as_index=False). \
        agg(aggregations). \
        rename(columns={
            "client_time": "countDistinct", "amount": "amount_sum"
        })

def writeToS3(dataFrame, s3_key, s3_secret, s3_bucket):
    opt = {
        'key' : s3_key,
        'secret' : s3_secret
        }
    dataFrame.to_csv('s3://{s3_bucket}/csv/localytics.csv', storage_options=opt, index=False)

def main():
    rawJSONurl = 'https://raw.githubusercontent.com/localytics/data-viz-challenge/master/data.json'
    s3_id = os.environ.get('AWS_ACCESS_KEY_ID')
    s3_secret = os.environ.get('AWS_SECRET_ACCESS_KEY')
    s3_bucket = os.environ.get('AWS_S3_BUCKET_NAME')

    #download data from remote location
    rawData = accessRemoteData(rawJSONurl)

    #transform raw data to a pandas Dataframe
    df = createDataframe(rawData)
    df.head(5)

    #filter and reformat Dataframe
    df = filterDataframe(df)

    #add a date column for further aggregation
    df['date'] = addDateColum(df)
    df.head(5)

    # aggregate data
    df = aggregateData(df)
    df.head(5)

    # upload data to S3 as a csv file
    writeToS3(df, s3_id, s3_secret, s3_bucket)
    exit(0)

if __name__ == '__main__':
    main()
