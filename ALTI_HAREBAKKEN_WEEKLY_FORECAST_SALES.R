library(RMySQL)
library(forecast)
library(lubridate)
library(dplyr)

db = dbConnect(MySQL(), user='etcinsights_harebakken', password = 'Harebakken_2020', dbname = 'etcinsights_harebakken', host = 'www.etcinsights.nazwa.pl')
sales.data = dbReadTable(db, "basket_sales_sector", row.names = F)
dbDisconnect(db)

sales.data <- sales.data %>% filter(!is.na(sector)) %>%
	mutate(weekday = wday(as.POSIXct(Date), week_start = 1)) %>% filter(Date > '2020-01-01')
sales.data['basket'] = sales.data['basket'] + 1
sales.data['sales'] = sales.data['sales'] + 1
		
bf <- sales.data %>% group_by(sector, Date) %>% summarize(basket = sum(basket))
sf <- sales.data %>% group_by(sector, Date) %>% summarize(sales = sum(sales))

# SALES forecasts for sectors
start_w_sales <- isoweek(min(sf$Date))
start_d_sales <- wday(min(sf$Date), week_start = 1)
end_w_sales <- isoweek(max(sf$Date))
end_d_sales <- wday(max(sf$Date), week_start = 1)

sector <- unique(sf$sector)
days_forecast <- 10
forecast_result <- NULL
period_week = 7

for (i in 1:length(sector))
{
	sts <- ts(sf[sf$sector == sector[i], 3], frequency = period_week)
	d.sts <- auto.arima(sts, lambda = 0)
	forecast.sales <- forecast(d.sts, h = days_forecast)

	bts <- ts(bf[bf$sector == sector[i], 3], frequency = period_week)
	d.bts <- auto.arima(bts, lambda = 0)
	forecast.baskets <- forecast(d.bts, h = days_forecast)
	
	Date <- as.POSIXct(max(sf$Date))
	print(paste("Sector:", sector[i]))
	for (j in 1:days_forecast)
	{
		Date <- Date + days(1)
		#if(format(Date, format='%u') == 7) {Date <- Date + days(1)}
		# mean in that case means "Point Forecast" not as average
		sales <- round(forecast.sales$mean[j], 0) 
		baskets <- round(forecast.baskets$mean[j], 0)
		forecast_result <- rbind(forecast_result, data.frame(Date=Date, sector=sector[i], sales=sales, baskets=baskets))
		print(paste("Date:", Date))
	}
}
# Writting forecast to database
db = dbConnect(MySQL(), user='etcinsights_harebakken', password = 'Harebakken_2020', dbname = 'etcinsights_harebakken', host = 'www.etcinsights.nazwa.pl')
dbSendQuery(db,'DROP TABLE IF EXISTS `sales_forecast`')
dbWriteTable(db, "sales_forecast", forecast_result, append = F, overwrite = F)
dbDisconnect(db)
rm(list=ls())
print('Script works perfectly!')