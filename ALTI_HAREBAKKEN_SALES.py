import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
pd.set_option('display.max.columns', 25)
pd.set_option('display.width', 1000)
os.chdir("/home/roman/Dropbox (BigBlue&Company)/ETC Insight/Projects/ALTI_HAREBAKKEN")

dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
calendar_weeks = pd.read_excel('input data/calendar weeks.xlsx',
                               parse_dates=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])
calendar_weeks = calendar_weeks.melt(id_vars = ['Week', 'Year'],
                                     value_vars = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'],
                                     var_name = 'Day', value_name='Date')

'''
please remember to change the folder from wich you will dowload data for new week
'''

files = os.listdir("input data/temp")
data = pd.DataFrame()
for file in files:
    data_temp_file = pd.read_excel("input data/temp/" + file, skiprows=6)
    data_temp_file = (data_temp_file[['År', 'Uke', 'But.nr', 'Mandag', 'Tirsdag', 'Onsdag', 'Torsdag', 'Fredag',
                                     'Lørdag', 'Søndag', 'Bet.Kunder Mandag', 'Bet.Kunder Tirsdag', 'Bet.Kunder Onsdag',
                                     'Bet.Kunder Torsdag', 'Bet.Kunder Fredag', 'Bet.Kunder Lørdag', 'Bet.Kunder Søndag']])
    data_temp_file = data_temp_file.dropna(subset=['But.nr'])
    data_temp_file = (data_temp_file.rename(columns = {'Mandag':'sales Monday', 'Tirsdag': 'sales Tuesday',
                                                      'Onsdag': 'sales Wednesday', 'Torsdag': 'sales Thursday',
                                                      'Fredag': 'sales Friday', 'Lørdag': 'sales Saturday',
                                                      'Søndag':'sales Sunday'}))
    data_temp_file = (data_temp_file.rename(columns = {'Bet.Kunder Mandag':'basket Monday', 'Bet.Kunder Tirsdag': 'basket Tuesday',
                                                      'Bet.Kunder Onsdag': 'basket Wednesday', 'Bet.Kunder Torsdag': 'basket Thursday',
                                                      'Bet.Kunder Fredag': 'basket Friday', 'Bet.Kunder Lørdag': 'basket Saturday',
                                                      'Bet.Kunder Søndag':'basket Sunday'}))
    data_temp_file = pd.melt(data_temp_file, id_vars=['År', 'Uke', 'But.nr'], var_name='Week_day_name', value_name='transactions')
    data_temp_file[['type_sales', 'Week_day_name']] = data_temp_file['Week_day_name'].str.split(' ', expand=True)
    data_temp_file = (pd.merge(data_temp_file, calendar_weeks, left_on=['År', 'Uke', 'Week_day_name'],
                              right_on=['Year', 'Week', 'Day'], how='left'))
    data = pd.concat([data, data_temp_file])
    print(file)
data['transactions'] = data['transactions'].replace(np.nan, 0).astype(int)
data = data.drop(columns=['År', 'Uke', 'Week_day_name', 'Year', 'Day'])
data = pd.pivot_table(data, index=['But.nr', 'Week', 'Date'], values='transactions', columns='type_sales').reset_index()

data = data.groupby(['Week', 'Date'])[['basket', 'sales']].sum().reset_index()
data = data[['Date', 'basket', 'sales', 'Week']]
data = data.rename(columns = {'Week': 'week'}).assign(sector='mall')
data['Date'] = data['Date'].dt.date

engine = create_engine("mysql://etcinsights_harebakken:Harebakken_2020@etcinsights.nazwa.pl/etcinsights_harebakken")
data.to_sql("basket_sales_sector", con=engine, if_exists='append', index=True)