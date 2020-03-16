import requests

data=requests.get('https://maxbo.link.express/external/api/v2/5d02982d29512bcc1729bb3964efb830/sales/query/?start_date=2020-03-09T00:00:00&end_date=2020-03-14T23:59:59&store_alias=ALL&type=CASH').json()
