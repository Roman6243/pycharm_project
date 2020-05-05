import numpy as np
import requests
import pandas as pd
from dfcleaner import cleaner
import datetime
pd.set_option('display.max_columns', 30)
pd.set_option('display.width', 1000)
from sqlalchemy import create_engine
import matplotlib.pylab as plt
import matplotlib.dates as mdates
plt.ioff()

connection = create_engine("mysql://etcinsights_ws:White!star@etcinsights.nazwa.pl/etcinsights_ws").connect()
mall_cols = connection.execute("select * from mall")
mall = connection.execute("select * from mall").fetchall()
print("Data extracted from sales_forecast successfully!")
connection.close()
mall = pd.DataFrame(mall)
mall.columns = [col for col in mall_cols.keys()]
mall.columns = cleaner.sanitize(mall.columns)
mall['traffic'] = mall['traffic'].astype(int)
mall['date']=pd.to_datetime(mall['date'], format="%Y-%m-%d")
mall=mall.groupby(by=['year', 'week'])['traffic'].sum().reset_index()
# make graph of total

fig, ax = plt.subplots(figsize=(16,9))
ax.set(xlabel='date', ylabel='people in')
ax.grid(True)
ax.xaxis.set_tick_params(rotation=90)
ax.plot(mall['week']+"-"+mall['year'], mall['traffic'], marker='o')
plt.tight_layout()
fig.show()

mall_rf_data=mall[mall['date'] >= datetime.datetime.strptime('2019-07-29', "%Y-%m-%d")]
mall_rf_data=mall_rf_data.filter(['date', 'traffic'])
fig, ax = plt.subplots(figsize=(16,9))
ax.set(xlabel='date', ylabel='people in')
ax.grid(True)
ax.xaxis.set_major_formatter(myFmt)
ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
ax.xaxis.set_tick_params(rotation=90)
ax.plot(mall_rf_data['date'], mall_rf_data['traffic'])
plt.tight_layout()
fig.show()

#################################################################3
# youtube tutorial 2
#################################################################3

from statsmodels.graphics.tsaplots import plot_acf # plot auto-correlation
mall_rf_data_diff=mall_rf_data.diff(periods=1).dropna()
plot_acf(mall_rf_data_diff['traffic'], lags=20)
plt.show()

from statsmodels.tsa.ar_model import AR
from sklearn.metrics import mean_squared_error

X = mall_rf_data['traffic'].values
train = X
test = X[252:]
predictions=[]

model_ar = AR(train)
model_ar_fit = model_ar.fit()
predictions = model_ar_fit.predict(start=262, end=272)

# plt.plot(test)
plt.plot(predictions, color='red')
plt.show()
# ARIMA model
from statsmodels.tsa.arima_model import  ARIMA
# p, d, q
model_arima = ARIMA(train, order=(2,1,2))
model_arima_fit=model_arima.fit()
predictions=model_arima_fit.forecast(steps=14)[0]
# plt.plot(test)
fig, ax = plt.subplots(figsize=(8, 4))
ax.plot(predictions, color='red', marker='o')
fig.show()

import itertools
p=d=q=range(0,3)
pdq=list(itertools.product(p,d,q))
import warnings
warnings.filterwarnings('ignore')
for param in pdq:
    try:
        model_arima = ARIMA(train, order=param)
        model_arima_fit = model_arima.fit()
        print(param, model_arima_fit.aic)
    except:
        continue

#################################################################3
# youtube tutorial end
#################################################################3

# is time series stacionary or not ?
from statsmodels.tsa.stattools import adfuller

print('Results of Dickey-Fuller Test')
dftest=adfuller(mall_rf_data['traffic'], autolag='AIC')
dfoutput=pd.Series(dftest[0:4], index=['Test Statistic', 'p-value', '#Lags Used', 'Number of Observations Used'])
for key, value in dftest[4].items():
    dfoutput['Critical Value (%s)'%key]=value
print(dfoutput)
# autocorellation