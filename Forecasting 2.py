import numpy as np
import requests
import pandas as pd
from dfcleaner import cleaner
pd.set_option('display.max_columns', 30)
pd.set_option('display.width', 1000)
from sqlalchemy import create_engine
import matplotlib.pylab as plt


connection = create_engine("mysql://etcinsights_ws:White!star@etcinsights.nazwa.pl/etcinsights_ws").connect()
mall_cols = connection.execute("select * from mall")
mall = connection.execute("select * from mall").fetchall()
print("Data extracted from sales_forecast successfully!")
connection.close()
mall = pd.DataFrame(mall)
mall.columns = [col for col in mall_cols.keys()]
mall.columns = cleaner.sanitize(mall.columns)
mall['traffic'] = mall['traffic'].astype(int)

mall.plot.line(x='date', y='traffic')
mall_rf_data=mall[mall.date > pd.to_datetime('2019-07-29').date()]
mall_rf_data=mall_rf_data.filter(['date', 'traffic'])
mall_rf_data=mall_rf_data.set_index(['date'])
plt.xlabel('date')
plt.ylabel('people in')
plt.plot(mall_rf_data)

#################################################################3
# youtube tutorial 1
#################################################################3

rolmean=mall_rf_data.rolling(window=7).mean()
rolstd=mall_rf_data.rolling(window=7).std()
print(rolmean, rolstd)

origin=plt.plot(mall_rf_data, color='blue', label='Original')
mean=plt.plot(rolmean, color='red', label="Rolling Mean")
std=plt.plot(rolstd, color='black', label="Rolling std")
plt.legend(loc="best")
plt.title('Rolling Mean & Standard Deviation')
plt.show(block=False)

from statsmodels.tsa.stattools import adfuller
print('Results of Dickey-Fuller Test')
dftest=adfuller(mall_rf_data['traffic'], autolag='AIC')
dfoutput=pd.Series(dftest[0:4], index=['Test Statistic', 'p-value', '#Lags Used', 'Number of Observations Used'])
for key, value in dftest[4].items():
    dfoutput['Critical Value (%s)'%key]=value
print(dfoutput)

# estimating trend
mall_rf_data_logScale=np.log(mall_rf_data)
plt.plot(mall_rf_data_logScale)

movingAverage=mall_rf_data_logScale.rolling(window=12).mean()
movingSTD=mall_rf_data_logScale.rolling(window=12).std()
plt.plot(mall_rf_data_logScale)
plt.plot(movingAverage, color='red')

from statsmodels.tsa.seasonal import seasonal_decompose
decomposition=seasonal_decompose(mall_rf_data_logScale,period=7)

trend=decomposition.trend
seasonal=decomposition.seasonal
residual=decomposition.resid

plt.subplot(411)
plt.plot(mall_rf_data_logScale, label='Original')
plt.legend(loc='best')
plt.subplot(412)
plt.plot(trend, label='Trend')
plt.legend(loc='best')
plt.subplot(413)
plt.plot(seasonal, label='Seasonality')
plt.legend(loc='best')
plt.subplot(414)
plt.plot(residual, label='Residual')
plt.legend(loc='best')
plt.tight_layout()
#################################################################3
# youtube tutorial 2
#################################################################3
from datetime import datetime
def parser(x):
    return datetime.strptime(x, "%Y-%m")
sales=pd.read_csv('/home/roman/Downloads/sales-cars.csv', index_col=0, parse_dates=['Month'], date_parser=parser)
sales.plot()
sales_diff=sales.diff(periods=1).dropna()

from statsmodels.graphics.tsaplots import plot_acf
plot_acf(sales)
sales.shift(1)
plot_acf(sales_diff)
sales_diff.plot()

from statsmodels.tsa.ar_model import AR
from sklearn.metrics import mean_squared_error

X=sales.values
train = X[0:27]
test = X[26:]
predictions=[]

model_ar = AR(train)
model_ar_fit = model_ar.fit()
predictions = model_ar_fit.predict(start=27, end=36)

plt.plot(test)
plt.plot(predictions, color='red')

# ARIMA model
from statsmodels.tsa.arima_model import  ARIMA
# p, d, q
model_arima = ARIMA(train, order=(4,1,0))
model_arima_fit=model_arima.fit()
predictions=model_arima_fit.forecast(steps=10)[0]
predictions
plt.plot(test)
plt.plot(predictions, color='red')
import itertools
p=d=q=range(0,5)
pdg=list(itertools.product(p,d,q))
import warnings
warnings.filterwarnings('ignore')
for param in pdg:
    try:
        model_arima = ARIMA(train, order=param)
        model_arima_fit = model_arima.fit()
        print(param, model_arima_fit.aic)
    except:
        continue

#################################################################3
# youtube tutorial end
#################################################################3

connection = create_engine("mysql://etcinsights_ws:White!star@etcinsights.nazwa.pl/etcinsights_ws").connect()
hmall_cols = connection.execute("select * from hmall")
hmall = connection.execute("select * from hmall").fetchall()
print("Data extracted from sales_forecast successfully!")
connection.close()
hmall = pd.DataFrame(hmall)
hmall.columns = [col for col in hmall_cols.keys()]
hmall.columns = cleaner.sanitize(hmall.columns)
hmall['traffic'] = hmall['traffic'].astype(int)

hmall['time'] = hmall['time'].dt.components['hours']
hmall=hmall.groupby(by=['type', 'name', 'date', 'year', 'month', 'week', 'weekday', 'time'])['traffic'].sum().reset_index()
hmall['time'].value_counts().reset_index().sort_values(by='index').plot.bar(x = 'index', y = 'time')

calendar_req = requests.get("https://webapi.no/api/v1/Calendar/2020").json()
year=calendar_req['data']['year']
calendar=pd.DataFrame()
for i in range(len(calendar_req['data']['months'])):
    month=calendar_req['data']['months'][i]['month']
    month_name=calendar_req['data']['months'][i]['name']
    temp_data = pd.DataFrame()
    for j in range(len(calendar_req['data']['months'][i]['days'])):
        temp=calendar_req['data']['months'][i]['days'][j]
        weekday = temp['day']
        weeknumber = temp['weekNumber']
        norm_calendar=pd.DataFrame({'year': [year], 'month':[month], 'month_name':[month_name]}).assign(weekday=weekday, weeknumber=weeknumber)
        holidays=pd.DataFrame(temp['holydays'])
        if holidays.empty:
            holidays=pd.DataFrame({'date': ['no holiday'], 'description': ['no holiday']})
        temp_temp=pd.concat([norm_calendar, holidays], axis=1)
        temp_data=pd.concat([temp_data, temp_temp], axis=0)
    calendar=pd.concat([calendar, temp_data])