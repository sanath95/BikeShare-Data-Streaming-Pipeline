import requests
import pandas as pd
# from google.cloud import bigquery
import os
from time import strftime, localtime
import base64
from google.cloud import pubsub_v1
import json

def stream_bikes_data(event, context):
    pubsub_msg = base64.b64decode(event['data']).decode('utf-8')
    print(pubsub_msg)

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "dataengg2-425418-cfcf7799ad5b.json"

    station_info_response = requests.get('https://gbfs.lyft.com/gbfs/2.3/dca-cabi/en/station_information.json')
    if station_info_response.status_code == 200:
        station_info = station_info_response.json()['data']['stations']

    station_status_response = requests.get('https://gbfs.lyft.com/gbfs/2.3/dca-cabi/en/station_status.json')
    if station_status_response.status_code == 200:
        station_status = station_status_response.json()['data']['stations']

    station_info = pd.DataFrame(station_info)
    station_status = pd.DataFrame(station_status)

    station_info = station_info[['station_id', 'name', 'lat', 'lon']]

    for i in range(len(station_status)):
        station_status.loc[i, 'num_bikes_available'] = station_status.loc[i, 'vehicle_types_available'][0]['count']
        station_status.loc[i, 'num_ebikes_available'] = station_status.loc[i, 'vehicle_types_available'][1]['count']

    station_status = station_status[['station_id', 'num_bikes_available', 'num_ebikes_available', 'num_docks_disabled', 'num_bikes_disabled']]

    stations_df = station_info.merge(station_status, left_on='station_id', right_on='station_id', how='inner')

    def convert_to_timestamp(row):
        return strftime('%Y-%m-%d %H:%M:%S', localtime(row['timestamp']))

    stations_df['timestamp'] = station_status_response.json()['last_updated']
    stations_df['timestamp'] = stations_df.apply(lambda row: convert_to_timestamp(row), axis = 1)

    publisher = pubsub_v1.PublisherClient()
    topic_name = 'projects/{project_id}/topics/{topic}'.format(
        project_id='dataengg2-425418',
        topic='data_topic1',
    )

    data = stations_df.to_dict('records')
    for x in data:
        future = publisher.publish(topic_name, json.dumps(x).encode("utf-8"))
        future.result()