import os
import requests
import pandas as pd
from dfcleaner import cleaner
pd.set_option('display.max.column', 30)
pd.set_option('display.width', 4000)
pd.set_option()
date=pd.date_range(start='2020-03-09', end='2020-03-14', freq='D').strftime(date_format='%Y-%m-%d')
data=pd.DataFrame()
for date in date:
    data_req=requests.get('https://maxbo.link.express/external/api/v2/5d02982d29512bcc1729bb3964efb830/sales/query/?start_date='+date+'T00:00:00&end_date='+date+'T23:59:59&store_alias=ALL&type=CASH').json()
    data_date=pd.DataFrame()
    for i in range(len(data_req['store'])):
        store_name=data_req['store'][i]['store_name']
        sales_count=data_req['store'][i]['salesCount']
        print("{0} - {1} from date {2}".format(i+1,store_name,date))
        data_temp = pd.DataFrame()
        for j in range(len(data_req['store'][i]['sales'])):
            data_level_down=data_req['store'][i]['sales'][j]
            temp=pd.DataFrame(data_level_down['lineItems']).assign(store_name=store_name,sales_count=sales_count, date=date, id_tr=str(i+1)+date.astype(str).replace('-','')+str(j+1)) # id identify the transaction store+date+transaction
            data_temp=pd.concat([data_temp, temp], axis=0)
        data_date=pd.concat([data_date, data_temp], axis=0)
    data = pd.concat([data, data_date], axis=0)
data.drop(columns=['edpNr', 'productId', 'gross', 'discountPercent', 'itemCount'], inplace=True)
data.reset_index(drop=True, inplace=True)
data.columns=cleaner.sanitize(data.columns) # cleaning the data headers

data.dropna()[data.dropna()['vendor_name'].str.contains("Jordan")] # extract exactly the same name of vendor_name, which we know only partially

def choose_supplier(vendor_name):
    data_supplier=data[data['vendor_name']==vendor_name]
    # data_supplier=data_supplier.groupby(['store_name','product_name'])['quantity','sales'].sum().reset_index().sort_values(by=['store_name', 'sales', 'quantity'], ascending=[True, False, True]).reset_index(drop=True)
    return data_supplier

data_supplier=choose_supplier('Orkla House Care Norge AS ( Jordan )')


from mlxtend.frequent_patterns import apriori, association_rules
data_supplier['product_name'] = data_supplier['product_name'].str.strip()
# Dropping the rows without any invoice number
data_supplier.dropna(axis=0, subset=['id_tr'], inplace=True)
data_supplier['id_tr'] = data_supplier['id_tr'].astype('str')
data_s=(data_supplier.groupby(['id_tr', 'product_name'])['quantity'].sum().unstack().reset_index().fillna(0).set_index('id_tr'))
def hot_encode(x):
    if(x<= 0):return 0
    if(x>= 1):return 1
    else:return 0
basket_encoded = data_s.applymap(hot_encode)
# Building the model
frq_items = apriori(basket_encoded, min_support = 0.001, use_colnames = True, max_len=3)
rules = association_rules(frq_items, metric ="lift", min_threshold = 0.5)
rules = rules.sort_values(['support', 'confidence'], ascending =[False, False])
print(rules.head(30))

