import pandas as pd
from sqlalchemy import create_engine
from dfcleaner import cleaner
import matplotlib.pyplot as plt
from datetime import date, timedelta

from statsmodels.tsa.seasonal import seasonal_decompose
from pmdarima import auto_arima
import pmdarima as pm
print(f"Using pmdarima {pm.__version__}")

# pd.set_option('display.max.columns', 20)
# pd.set_option('display.width', 1000)

connection = create_engine("mysql://etcinsights_harebakken:Harebakken_2020@etcinsights.nazwa.pl/etcinsights_harebakken").connect()
traffic_cols = connection.execute("select * from traffic_date")
traffic = connection.execute("select * from traffic_date").fetchall()
print("Data extracted from sales_forecast successfully!")
connection.close()
traffic = pd.DataFrame(traffic)
traffic.columns = [col for col in traffic_cols.keys()]
traffic.columns = cleaner.sanitize(traffic.columns)

traffic=traffic.filter(['date', 'traffic'])
traffic.set_index(['date'], inplace=True)
traffic.index = pd.to_datetime(traffic.index)
traffic['traffic']+=0.001

# result = seasonal_decompose(traffic, model='multiplicative', period=7)
# result.plot()
# plt.show()
pm.plot_acf(traffic)
stepwise_model = auto_arima(traffic, start_p=1, start_q=1,
                            max_p=5, max_q=5, m=7,
                            start_P=0, seasonal=True,
                            d=1, D=1, trace=True,
                            error_action='ignore',
                            suppress_warnings=True,
                            stepwise=True)
#
print(stepwise_model.aic())
stepwise_model.summary()
print(stepwise_model.order) # to see the order
# train=traffic.loc['2018-01-01':'2020-04-26']
# test=traffic.loc['2020-04-27':]

stepwise_model.fit(traffic)
future_forecast=stepwise_model.predict(n_periods=7)
# print(future_forecast)

# future_forecast=pd.DataFrame(future_forecast, columns=['Prediction'])

# future_forecast.plot(legend = True)
# test['traffic'].plot(legend = True)

date_today = date.today().strftime('%Y-%m-%d')
date_tomorrow = max(traffic.index) + timedelta(days=1)
date_next_7d=pd.date_range(start=date_tomorrow, periods=7, freq='D').strftime(date_format='%Y-%m-%d').tolist()
future_forecast=pd.DataFrame({'date':date_next_7d, 'traffic':future_forecast}).assign(date_of_forecast = date_today)
future_forecast.loc[future_forecast['traffic'] < 0, 'traffic'] = 0
future_forecast['traffic']=future_forecast['traffic'].astype(int)

engine = create_engine("mysql://etcinsights_harebakken:Harebakken_2020@etcinsights.nazwa.pl/etcinsights_harebakken")
future_forecast.to_sql('traffic_date_forecast_python', con = engine, if_exists = 'append', index = True)
print('Data uploaded to db successfully!')


from pmdarima.arima import ndiffs

kpss_diffs = ndiffs(traffic, alpha=0.05, test='kpss', max_d=6)
adf_diffs = ndiffs(traffic, alpha=0.05, test='adf', max_d=6)
n_diffs = max(adf_diffs, kpss_diffs)

print(f"Estimated differencing term: {n_diffs}")
# Estimated differencing term: 1