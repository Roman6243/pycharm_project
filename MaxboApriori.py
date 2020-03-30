import os
import pickle
import requests
import pandas as pd
import matplotlib.pyplot as plt
from dfcleaner import cleaner
from mlxtend.frequent_patterns import association_rules
from mlxtend.frequent_patterns import apriori
pd.set_option('display.max.column', 30)
pd.set_option('display.width', 4000)
pd.set_option('display.max_colwidth', 1000)
os.chdir(r'C:\Users\PC1\Dropbox (BigBlue&Company)\ETC Insight\Projects\Maxbo Basket Analysis')
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

# send data the file named 'sales_data'
file = open('sales_data', 'wb')
pickle.dump(data, file)
file.close()
# load the data
file = open('sales_data', 'rb')
data = pickle.load(file)
file.close()

def original_name(part_name):
    # extract exactly the same name of vendor_name, which we know only partially
    return data.dropna()[data.dropna()['vendor_name'].str.contains(part_name)]['vendor_name'].unique().tolist()
original_name("Jordan")

# return all transactions, in which at least once product from current supplier present
def choose_tran_supplier(vendor_name):
    """
    choose only transactions contains at least one current vendor's products
    """
    list_ids=data[(data['vendor_name']==vendor_name) & (data['category']=='Maling')]['id_tr'].unique()
    data_supplier=data[data['id_tr'].isin(list_ids)]
    return data_supplier
data_supplier=choose_tran_supplier('Orkla House Care Norge AS ( Jordan )')

# return only current supplier products
def choose_prod_supplier(vendor_name):
    # choose only current vendor's products
    return data[(data['vendor_name']==vendor_name) & (data['category']=='Maling')]
data_supplier=choose_prod_supplier('Orkla House Care Norge AS ( Jordan )')

# mba analysis
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
data_mba.to_excel('output/data_mba_all_products_18_03.xlsx')

# transactions distribution by current supplier
temporary=data_supplier.groupby('store_name')['id_tr'].nunique().reset_index().sort_values(by='id_tr', ascending=False)
plot_data = temporary.copy()
fig, ax = plt.subplots()
ax.plot('store_name', 'id_tr', data=plot_data, linestyle='-', marker='o')
ax.grid()
for tick in ax.get_xticklabels():
    tick.set_fontsize(10)
    tick.set_rotation(90)
ax.set_title(('number of transactions contains Orkla House Care Norge AS ( Jordan )').upper(), fontsize=16)
ax.set_ylabel(ylabel = 'transactions', fontsize=16)
ax.set_xlabel(xlabel = 'store', fontsize=16)
fig.set_size_inches(20, 10)
fig.tight_layout()
fig.savefig("plots/test.png", dpi=200, pad_inches=0.5)

##########################################################################################################

data_maling=data[data['category']=='Maling'].reset_index(drop=True)
data_maling_all_tr=data_maling.groupby('store_name')['id_tr'].nunique().reset_index().rename(columns={'id_tr':'all_transactions'})
data_maling.loc[data_maling['vendor_name']=='Orkla House Care Norge AS ( Jordan )', 'supplier'] = 'current_supplier'
data_maling.loc[data_maling['vendor_name']!='Orkla House Care Norge AS ( Jordan )', 'supplier'] = 'other_supplier'

data_temp=data_maling.groupby(['store_name', 'supplier'])['id_tr'].nunique().reset_index()
data_temp=data_temp.pivot_table(index='store_name', columns='supplier', fill_value=0, values='id_tr').reset_index()
data_temp=pd.merge(data_temp, data_maling_all_tr, on='store_name')
data_temp['supplier_products']=data_temp['current_supplier']/data_temp['all_transactions']
data_temp['other_products']=data_temp['other_supplier']/data_temp['all_transactions']
data_temp['ratio']=data_temp['supplier_products']/data_temp['other_products']
data_temp['supplier_products']=round(data_temp['supplier_products']*100, 2).astype('str')+'%'
data_temp['other_products']=round(data_temp['other_products']*100, 2).astype('str')+'%'
data_temp.sort_values(by='supplier_products', ascending=False, inplace=True)
data_temp.to_excel('table_1.xlsx')

value_category=data_maling.groupby(['store_name', 'id_tr'])['sales'].sum().groupby('store_name').mean().reset_index().rename(columns={'sales':'values_per_basket_cat'})
nr_products_category=(data_maling[['store_name', "product_name", 'id_tr']].drop_duplicates().groupby(['store_name', 'id_tr']).count()
                      .groupby('store_name')['product_name'].mean().reset_index()
                      .rename(columns={'product_name':'number_of_products_cat'}))
value_sup=(data_maling[data_maling['vendor_name']=='Orkla House Care Norge AS ( Jordan )'].groupby(['store_name', 'id_tr'])['sales']
                      .sum().groupby('store_name').mean().reset_index().rename(columns={'sales':'values_per_basket_sup'}))
nr_products_sup=(data_maling[data_maling['vendor_name']=='Orkla House Care Norge AS ( Jordan )'][['store_name', "product_name", 'id_tr']]
                      .drop_duplicates().groupby(['store_name', 'id_tr']).count()
                      .groupby('store_name')['product_name'].mean().reset_index()
                      .rename(columns={'product_name':'number_of_products_sup'}))
data_temp2=value_category.merge(nr_products_category, on='store_name').merge(value_sup, on='store_name').merge(nr_products_sup, on='store_name')
data_temp2.to_excel('table_2.xlsx')