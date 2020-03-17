import requests
import pandas as pd
from dfcleaner import cleaner
pd.set_option('display.max.column', 30)
pd.set_option('display.width', 4000)
pd.set_option('display.max_colwidth', 1000)
from mlxtend.frequent_patterns import association_rules
from mlxtend.frequent_patterns import apriori
date=pd.date_range(start='2020-01-06', end='2020-03-14', freq='D').strftime(date_format='%Y-%m-%d')
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
            temp=pd.DataFrame(data_level_down['lineItems']).assign(store_name=store_name,sales_count=sales_count, date=date, id_tr=str(i+1)+date.replace('-','')+str(j+1)) # id identify the transaction store+date+transaction
            data_temp=pd.concat([data_temp, temp], axis=0)
        data_date=pd.concat([data_date, data_temp], axis=0)
    data = pd.concat([data, data_date], axis=0)
data.drop(columns=['edpNr', 'productId', 'gross', 'discountPercent', 'itemCount'], inplace=True)
data.reset_index(drop=True, inplace=True)
data.columns=cleaner.sanitize(data.columns) # cleaning the data headers

def original_name(part_name):
    # extract exactly the same name of vendor_name, which we know only partially
    return data.dropna()[data.dropna()['vendor_name'].str.contains(part_name)]['vendor_name'].unique().tolist()
original_name("Jordan")

def choose_supplier(vendor_name):
    return data[data['vendor_name']==vendor_name]

data_supplier=choose_supplier('Orkla House Care Norge AS ( Jordan )')
# choose the top 5 products by penetration
data_supplier_penetration=data_supplier[['product_name', 'id_tr']].drop_duplicates().groupby('product_name').count().reset_index()
data_supplier_penetration['pct']=data_supplier_penetration['id_tr']/data_supplier['id_tr'].nunique()
data_supplier_penetration.sort_values(by='pct', ascending=False, inplace=True)
data_supplier_penetration_top5=data_supplier_penetration.head(5)

data_mba=pd.DataFrame()
for store_name in data_supplier['store_name'].unique():
    data_temp=data_supplier[data_supplier['store_name']==store_name]
    data_temp['product_name'] = data_temp['product_name'].str.strip()
    data_temp=data_temp.groupby(['id_tr', 'product_name'])['quantity'].sum().unstack().reset_index().fillna(0).set_index('id_tr')
    def zero_one_replacement(x):
        if(x<= 0):return 0
        if(x>= 1):return 1
        else:return 0
    basket_encoded = data_temp.applymap(zero_one_replacement)
    # Building the model
    frq_items = apriori(basket_encoded, min_support = 0.001, use_colnames = True, max_len=2)
    # frq_items.sort_values('support', ascending=False)
    rules = association_rules(frq_items, metric ="support", min_threshold = 0.01,support_only=False)
    rules["antecedents"] = rules["antecedents"].apply(lambda x: list(x)[0]).astype("unicode")
    rules["consequents"] = rules["consequents"].apply(lambda x: list(x)[0]).astype("unicode")
    rules.drop(columns=['leverage', 'conviction'], inplace=True)
    rules = rules.sort_values(['support', 'confidence'], ascending =[False, False]).assign(store_name=store_name)
    data_mba=pd.concat([data_mba, rules])
    print('MBA store {0}'.format(store_name))
data_mba.to_excel(r'C:\Users\PC1\Dropbox (BigBlue&Company)\ETC Insight\Projects\Maxbo Basket Analysis\output\data_mba.xlsx')