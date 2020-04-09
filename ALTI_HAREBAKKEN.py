import pandas as pd
import requests
from sqlalchemy import create_engine

pd.set_option('display.max_columns', 10)
pd.set_option('display.width', 1000)
pd.options.display.float_format = '{:15f}'.format

date_start_end = pd.date_range(start='2020-03-09', end='2020-04-05',freq='D').strftime(date_format='%Y-%m-%d')

data_harebakken = pd.DataFrame()
for i in range(len(date_start_end)):
    api_req = requests.get("https://link.retailflux.com/traffic/api/v2/json/8295D8E73E03EF497989B87FD68A92ED/query/?store_alias=all&start_date=" +
                            date_start_end[i]+"T06:00:00&end_date="+date_start_end[i]+"T23:00:00&counter_type=all").json()
    temp=api_req["command_output"]["store_camera_list"][0]["counter_list"]
    for k in range(len(temp)):
        counter_name = temp[k]["counter_name"]
        store_name = temp[k]["camera_name"]
        output_data = pd.DataFrame(temp[k]["data"]).assign(counter=counter_name, store_name=store_name)
        data_harebakken = pd.concat([output_data, data_harebakken])
    print("Traffic data are extracted from {0}".format(date_start_end[i]))
print('Data extracted successfully!')

data_harebakken['datetime']=pd.to_datetime(data_harebakken['datetime'], format='%Y-%m-%dT%H:%M:%S')
data_harebakken['date']=data_harebakken['datetime'].dt.strftime('%Y-%m-%d')
data_harebakken['month']=data_harebakken['datetime'].dt.strftime('%B')
data_harebakken['weekday']=data_harebakken['datetime'].dt.strftime('%A')
data_harebakken['hour']=data_harebakken['datetime'].dt.strftime('%H')
data_harebakken.sort_values(by=['datetime', 'counter'], inplace=True)

engine = create_engine("mysql://etcinsights_harebakken:Harebakken_2020@etcinsights.nazwa.pl/etcinsights_harebakken")
data_harebakken.to_sql('traffic_data', con = engine, if_exists = 'replace', index = False)
print('Data uploaded to db successfully!')