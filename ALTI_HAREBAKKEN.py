import pandas as pd
import requests
import os
from datetime import date
from sqlalchemy import create_engine
from dfcleaner import cleaner
os.chdir('/home/roman/Dropbox (BigBlue&Company)/ETC Insight/Projects/ALTI_HAREBAKKEN/input data')
pd.set_option('display.max_columns', 15)
pd.set_option('display.width', 1000)
pd.options.display.float_format = '{:15f}'.format

period_month = date.today().strftime("%B") # month, example Oct
period_week = date.today().strftime("%V") # week, for example "41"
period_date = date.today().strftime('%Y-%m-%d')

month = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'] # for month order

date_start_end = pd.date_range(start='2020-03-09', end='2020-04-28',freq='D').strftime(date_format='%Y-%m-%d')

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
data_harebakken['week']=data_harebakken['datetime'].dt.strftime('%V')
data_harebakken['weekday']=data_harebakken['datetime'].dt.strftime('%A')
data_harebakken['hour']=data_harebakken['datetime'].dt.strftime('%H')
data_harebakken.sort_values(by=['datetime', 'counter'], inplace=True)

engine = create_engine("mysql://etcinsights_harebakken:Harebakken_2020@etcinsights.nazwa.pl/etcinsights_harebakken")
data_harebakken.to_sql('traffic_data', con = engine, if_exists = 'replace', index = False)
print('Data uploaded to db successfully!')

# traffic distribution by entrances
split_counter_day=(data_harebakken[data_harebakken['date']=="2020-04-18"].groupby(['counter'])['people_in'].sum().reset_index()
                 .sort_values(by='people_in', ascending=False)).assign(time_period='day')
split_counter_week=(data_harebakken[data_harebakken['week']=="16"].groupby(['counter'])['people_in'].sum().reset_index()
                 .sort_values(by='people_in', ascending=False)).assign(time_period='week')
split_counter_month=(data_harebakken[data_harebakken['month']=="April"].groupby(['counter'])['people_in'].sum().reset_index()
                 .sort_values(by='people_in', ascending=False)).assign(time_period='month')
split_counter=pd.concat([split_counter_day, split_counter_week, split_counter_month])
split_counter=split_counter.assign(type_request='entrances_distr').rename(columns={'counter': 'label'})

# traffic distribution by hour
hour_traffic_day=(data_harebakken[data_harebakken['date']=="2020-04-18"].groupby(['hour'])['people_in'].mean().reset_index()
                 .sort_values(by='hour', ascending=True)).assign(time_period='day')
hour_traffic_week=(data_harebakken[data_harebakken['week']=="16"].groupby(['hour'])['people_in'].mean().reset_index()
                 .sort_values(by='hour', ascending=True)).assign(time_period='week')
hour_traffic_month=(data_harebakken[data_harebakken['month']=="April"].groupby(['hour'])['people_in'].mean().reset_index()
                 .sort_values(by='hour', ascending=True)).assign(time_period='month')
hour_traffic=pd.concat([hour_traffic_day, hour_traffic_week, hour_traffic_month])
hour_traffic=hour_traffic.assign(type_request="hourly_data").rename(columns={'hour': 'label'})


# traffic distribution by day-week-month
traffic_date=(data_harebakken.groupby(['date'])['people_in'].sum().reset_index()
                 .sort_values(by='date', ascending=True).assign(time_period='day')
                 .tail(n=14).rename(columns = {'date':'label'}))
traffic_week=(data_harebakken.groupby(['week'])['people_in'].sum().reset_index()
                 .sort_values(by='week', ascending=True).assign(time_period='week')
                 .tail(n=8).rename(columns = {'week':'label'}))
traffic_month=data_harebakken.groupby(['month'])['people_in'].sum().reset_index().assign(time_period='month')
traffic_month['month'] = pd.Categorical(traffic_month['month'], categories=month, ordered=True)
traffic_month.sort_values(by='month', inplace=True)
traffic_month.rename(columns={'month':'label'}, inplace=True)
traffic=pd.concat([traffic_date, traffic_week, traffic_month])
traffic=traffic.assign(type_request='traffic_mall')

all_mall_data = pd.concat([split_counter, hour_traffic, traffic])
all_mall_data['people_in']=all_mall_data['people_in'].astype(int)
engine = create_engine("mysql://etcinsights_harebakken:Harebakken_2020@etcinsights.nazwa.pl/etcinsights_harebakken")
all_mall_data.to_sql('all_mall_data', con = engine, if_exists = 'replace', index = False)
print('Data uploaded to db successfully!')


date_parser = lambda x: pd.datetime.strptime(x, '%m/%d/%Y')
calendar_weeks = pd.read_excel('calendar weeks.xlsx', parse_dates=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])
calendar_weeks = calendar_weeks.melt(id_vars = ['Week', 'Year'], value_vars = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'], var_name = 'Day', value_name='Date')
traffic_sales_data=pd.read_excel('traffic_sales_data/traffic_sales_2018_2019_2020.xlsx')
traffic_sales_data=traffic_sales_data.melt(id_vars=['Week', 'Year', 'N_P'], value_vars = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'], var_name = 'Day', value_name='Value')
traffic_sales_data=traffic_sales_data.merge(calendar_weeks, how='left', on=['Year', 'Week', 'Day'])
traffic_sales_data.columns = cleaner.sanitize(traffic_sales_data.columns)

traffic_sales_data_date=traffic_sales_data[traffic_sales_data['n_p']=="TRAFFIC"].rename(columns={'value':'people_in'})[['people_in', 'date']]
traffic_sales_data_date['people_in']=traffic_sales_data_date['people_in'].astype(int)

data_harebakken_date=data_harebakken.groupby(by=['date'])['people_in'].sum().reset_index()
traffic_date=pd.concat([traffic_sales_data_date, data_harebakken_date])

traffic_date['date']=pd.to_datetime(traffic_date['date'], format="%Y-%m-%d")
traffic_date.sort_values(by=["date"], inplace=True)
traffic_date['week']=traffic_date['date'].dt.strftime("%V")
traffic_date['month']=traffic_date['date'].dt.strftime("%B")
traffic_date['date']=traffic_date['date'].dt.strftime('%Y-%m-%d')
traffic_date.rename(columns={'people_in':'traffic'}, inplace=True)
traffic_date.reset_index(drop=True, inplace=True)

engine = create_engine("mysql://etcinsights_harebakken:Harebakken_2020@etcinsights.nazwa.pl/etcinsights_harebakken")
traffic_date.to_sql('traffic_date', con = engine, if_exists = 'replace', index = True)
print('Data uploaded to db successfully!')
