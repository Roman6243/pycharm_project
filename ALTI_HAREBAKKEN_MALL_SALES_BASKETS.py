import datetime
import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
pd.set_option('display.max.columns', 25)
pd.set_option('display.width', 1000)

connection = create_engine("mysql://etcinsights_harebakken:Harebakken_2020@etcinsights.nazwa.pl/etcinsights_harebakken").connect()
basket_sales_sector_cols = connection.execute("select * from basket_sales_sector")
basket_sales_sector = connection.execute("select * from basket_sales_sector").fetchall()
print("Data extracted from basket_sales_sector successfully!")
connection.close()
basket_sales_sector = pd.DataFrame(basket_sales_sector)
basket_sales_sector.columns = [col for col in basket_sales_sector_cols.keys()]
basket_sales_sector['Date'] = pd.to_datetime(basket_sales_sector['Date'])
basket_sales_sector['weekdays'] = basket_sales_sector['Date'].dt.strftime("%A")
basket_sales_sector['year'] = basket_sales_sector['Date'].dt.year
basket_sales_sector['month'] = basket_sales_sector['Date'].dt.strftime('%B')
basket_sales_sector['week'] = basket_sales_sector['week'].astype(int)
basket_sales_sector = basket_sales_sector.groupby(['Date', 'week', 'weekdays', 'month', 'year']).sum().reset_index().sort_values(by=['year', 'week', 'Date'])

# download forecast
connection = create_engine("mysql://etcinsights_harebakken:Harebakken_2020@etcinsights.nazwa.pl/etcinsights_harebakken").connect()
sales_forecast_cols = connection.execute("select * from sales_forecast")
sales_forecast = connection.execute("select * from sales_forecast").fetchall()
print("Data extracted from sales_forecast successfully!")
connection.close()
sales_forecast = pd.DataFrame(sales_forecast)
sales_forecast.columns = [col for col in sales_forecast_cols.keys()]
sales_forecast.drop(columns=['row_names'], inplace=True)

sales_forecast['Date'] = pd.to_datetime(sales_forecast['Date'])
sales_forecast['weekdays'] = sales_forecast['Date'].dt.strftime("%A")
sales_forecast['year'] = sales_forecast['Date'].dt.year
sales_forecast['month'] = sales_forecast['Date'].dt.strftime('%B')
sales_forecast['week'] = sales_forecast['Date'].dt.week
sales_forecast = sales_forecast.groupby(['Date', 'week', 'weekdays', 'month', 'year']).sum().reset_index().sort_values(by=['year', 'week', 'Date'])

diff = set(sales_forecast['Date'].unique()).difference(set(basket_sales_sector['Date'].unique()))
diff = [x for x in diff if x < ((np.datetime64(datetime.datetime.now())) - np.timedelta64(1, 'D'))]

add_forecast = sales_forecast[sales_forecast['Date'].isin(diff)]
add_forecast=add_forecast.rename(columns={"baskets":"basket"})
print("Which sales data are missing in real data!")
basket_sales_sector = pd.concat([basket_sales_sector, add_forecast])

basket_sales_sector['month_num']=basket_sales_sector['Date'].dt.month
basket_sales_zones_m=basket_sales_sector.groupby(['month', 'month_num','year'])[['basket', 'sales']].sum().reset_index().assign(type_period = 'month').sort_values(by=['year', 'month_num']).tail(n=4)
basket_sales_zones_w=basket_sales_sector.groupby(['week', 'year'])[['basket', 'sales']].sum().reset_index().assign(type_period = 'week').sort_values(by=['year', 'week']).tail(n=8)
basket_sales_zones_d=basket_sales_sector.groupby('Date')[['basket', 'sales']].sum().reset_index().assign(type_period = 'day').sort_values(by=['Date']).tail(n=14)
basket_sales_zones_m.rename(columns = {'month':'label'}, inplace=True)
basket_sales_zones_m.drop(columns={'month_num'}, inplace=True)
basket_sales_zones_w.rename(columns = {'week':'label'}, inplace=True)
basket_sales_zones_d.rename(columns = {'Date':'label'}, inplace=True)
basket_sales_zones_d['label'] = basket_sales_zones_d['label'].dt.date
data=pd.concat([basket_sales_zones_m, basket_sales_zones_w, basket_sales_zones_d])
data=pd.melt(data, id_vars=['type_period', 'label'], value_vars=['basket', 'sales'], value_name='traffic', var_name='type_request')
data['type_request']=str('mall_') + data['type_request']
data = data[['label', 'traffic', 'type_period', 'type_request']]
data['traffic'] = data['traffic'].astype(int)
#
engine=create_engine("mysql://etcinsights_harebakken:Harebakken_2020@etcinsights.nazwa.pl/etcinsights_harebakken")
data.to_sql("all_mall_data", con = engine, if_exists = 'append', index = False)
