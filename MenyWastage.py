import pandas as pd
import numpy as np
import os

pd.set_option('display.max.columns', 20)
pd.set_option('display.width', 1000)

os.chdir("/home/roman/Dropbox (Link Analytix)/ETC Insight/Projects/Meny Wastage")

# products and segments
focus_products_seg = pd.read_excel('input_data/product_segmentation.xlsx', sheet_name="segmentation", usecols=['segment', 'product'])


temp = pd.read_excel('input_data/Master_Ordrebestillinglinje_allebutikker.xlsx', sheet_name=None)
master_file_spreadsheet=temp.keys()
master_file=pd.DataFrame()

for spread_sheet in master_file_spreadsheet:
    temp = pd.read_excel('input_data/Master_Ordrebestillinglinje_allebutikker.xlsx', usecols=['Bestilling til ', 'Uke ', 'Vare', 'Ordredifferanse'], sheet_name=spread_sheet)
    temp=temp.assign(store = spread_sheet)
    master_file=pd.concat([master_file, temp], axis=0)

master_file.columns = ['weekday', 'week', 'product', 'order_diff', 'store']
master_file['product']=master_file['product'].str.split(' ', n=1, expand=True)[1]
master_file['weekday']=master_file['weekday'].str.lower()
master_file['store']=master_file['store'].str.lower()
master_file.loc[master_file['weekday'] == 'mandag', 'weekday'] = 'Monday'
master_file.loc[master_file['weekday'] == 'tirsdag', 'weekday'] = 'Tuesday'
master_file.loc[master_file['weekday'] == 'onsdag', 'weekday'] = 'Wednesday'
master_file.loc[master_file['weekday'] == 'torsdag', 'weekday'] = 'Thursday'
master_file.loc[master_file['weekday'] == 'fredag', 'weekday'] = 'Friday'
master_file.loc[master_file['weekday'] == 'lørdag', 'weekday'] = 'Saturday'
# wastage and sale base
wastage_base = pd.read_excel("input_data/Data_MENY_220420.xlsx", sheet_name="Baseline wastage week 2-9",
                             parse_dates=True,
                             usecols=['Store', 'Day', 'Date', 'Week', 'Product', 'wastage stk'])
wastage_base.columns = ['store', 'day', 'type_period', 'week', 'product', 'wastage']
wastage_base['weekday'] = wastage_base['type_period'].dt.strftime("%A")
wastage_base['store'] = wastage_base['store'].str.lower()
wastage_base = wastage_base[wastage_base['week'] != 1]  # exclude week 1

sales_base_weekly = pd.read_excel('input_data/Sales_base_weekly_MENY.xlsx',
                                  usecols=['Segment', 'ID', "Butikk", "Produkt", "Ukedag",
                                           '2', '3', "4", '5', '6', '7', '8', '9'])
sales_base_weekly = sales_base_weekly[
    sales_base_weekly['Butikk'].isin(['MENY RINGNES PARK', 'MENY MANGLERUD', 'MENY NORDSTRAND'])]
sales_base_weekly = pd.melt(sales_base_weekly, id_vars=['Segment', 'ID', 'Butikk', 'Produkt', 'Ukedag'],
                            value_vars=['2', '3', "4", '5', '6', '7', '8', '9'], var_name='week')
sales_base_weekly.columns = ['segment', 'id', 'store', "product", "day", 'week', 'sales']
sales_base_weekly['day'], sales_base_weekly['store'] = sales_base_weekly['day'].str.lower(), sales_base_weekly[
    'store'].str.lower().str.replace("meny ", "")
sales_base_weekly.loc[sales_base_weekly['product'] == "ALPEBRØD HALVSTEKT 600G JACOBS",
                      'product'] = 'ALPEBRØD 570G JACOBS UTVALGTE'  # only for sale_base
sales_base_weekly['week'] = sales_base_weekly['week'].astype(int)
# sales_base_weekly = sales_base_weekly[sales_base_weekly['sales'] != 0]
# print(sales_base_weekly[(sales_base_weekly['product'] == "SPELTBRØD 100% 660G JACOBS UTVALGTE")
#                         & (sales_base_weekly['store'] == "manglerud") & (sales_base_weekly['id']=='ON CAMPAGIN')])

temp = pd.merge(sales_base_weekly, wastage_base, how="outer", on=["store", 'product', "day", "week"])
temp[['sales', 'wastage']] = temp[['sales', 'wastage']].replace(np.nan, 0)
temp['id'] = temp['id'].replace(np.nan, 'NOT ON CAMPAGIN')
temp.loc[temp['day'] == 'mandag', 'day'] = 'Monday'
temp.loc[temp['day'] == 'tirsdag', 'day'] = 'Tuesday'
temp.loc[temp['day'] == 'onsdag', 'day'] = 'Wednesday'
temp.loc[temp['day'] == 'torsdag', 'day'] = 'Thursday'
temp.loc[temp['day'] == 'fredag', 'day'] = 'Friday'
temp.loc[temp['day'] == 'lørdag', 'day'] = 'Saturday'
temp = temp[(temp['sales'] != 0) | (temp['wastage'] != 0)]  # exclude zero in sales and wastage simultaneously
# temp = temp[~((temp['sales'] == 0) & (temp['wastage'] == 0))] # the same command as line above
temp_not_campagin = temp[temp['id'] == "NOT ON CAMPAGIN"]
temp_campagin = temp[(temp['id'] == "ON CAMPAGIN") & (temp['sales'] > .0)]
temp = pd.concat([temp_not_campagin, temp_campagin], axis=0)
# print(temp[(temp['product'] == "SPELTBRØD 100% 660G JACOBS UTVALGTE") & (temp['store'] == "manglerud") & (temp['id']=='NOT ON CAMPAGIN')])

# sales base
sales_base = (temp.groupby(by=['id', 'store', "product", 'day'])['sales'].mean().reset_index().
              rename(columns={'id': 'diff', 'sales': 'value'}))
sales_base = sales_base.assign(type_period="sales_base")
(sales_base.loc[(sales_base['product'] == "HVERDAGSGROVT 750G BAKEHUSET") & sales_base['store'].
    isin(['ringnes park', 'nordstrand']), 'value']) = sales_base['value'] / 8
sales_base['type_period'] = sales_base['type_period'] + "_" + sales_base['diff']
sales_base.drop(columns='diff', inplace=True)
sales_base.rename(columns={'day': 'weekday'}, inplace=True)
# print(sales_base[(sales_base['product'] == "SPELTBRØD 100% 660G JACOBS UTVALGTE") & (sales_base['store'] == "manglerud")])

# wastage base
wastage_base = temp[['store', 'product', 'day', 'week', 'wastage']].drop_duplicates()
# print(wastage_base[(wastage_base['product'] == "SPELTBRØD 100% 660G JACOBS UTVALGTE") & (wastage_base['store'] == "manglerud")])
wastage_base = wastage_base.groupby(by=['store', 'product', 'day'])['wastage'].mean().reset_index()
wastage_base = wastage_base.assign(type_period="wastage_base").rename(columns={'wastage': 'value', 'day': 'weekday'})
df_base = pd.concat([sales_base, wastage_base], axis=0)


def westage_current_date(path):
    wastage_cur = pd.read_excel(path, parse_dates=True,
                                usecols=['Dato', 'TEKST', 'ANTALL0', 'ANTALL1', 'ANTALL2'])
    wastage_cur.columns = ['type_period', 'product', 'manglerud', 'ringnes park', 'nordstrand']
    wastage_cur['weekday'] = wastage_cur['type_period'].dt.strftime('%A')
    wastage_cur['type_period'] = "wastage-" + wastage_cur['type_period'].dt.strftime("%Y-%m-%d")
    wastage_cur = pd.melt(wastage_cur, id_vars=['product', 'weekday', 'type_period'],
                          value_vars=['manglerud', 'ringnes park', 'nordstrand'], var_name="store")
    return wastage_cur


def sales_current_date(path):
    sales_cur = pd.read_excel(path, parse_dates=True,
                              usecols=['Dato', 'TEKST', 'ANTALL0', 'ANTALL1', 'ANTALL2'])
    sales_cur.columns = ['type_period', 'product', 'manglerud', 'ringnes park', 'nordstrand']
    sales_cur['weekday'] = sales_cur['type_period'].dt.strftime("%A")
    sales_cur = pd.melt(sales_cur, id_vars=['product', 'weekday', 'type_period'],
                        value_vars=['manglerud', 'ringnes park', 'nordstrand'], var_name="store")
    sales_cur['type_period'] = 'sales-' + sales_cur['type_period'].dt.strftime("%Y-%m-%d")
    return sales_cur


def cam_product(week_num):
    # choose campaign product
    print('Choose campaign product in week {0}: \n'
          '17 - NORAS GROVBRØD 100% BUTIKKSTEKT 750G \n'
          '18 - SPELTBRØD 100% 660G JACOBS UTVALGTE\n'
          '19 - KORNBRØD GROVT JACOBS 660G'.format(week_num))
    if week_num==17:
        campaign_product = "NORAS GROVBRØD 100% BUTIKKSTEKT 750G"
    elif week_num==18:
        campaign_product = "SPELTBRØD 100% 660G JACOBS UTVALGTE"
    else:
        campaign_product = "KORNBRØD GROVT JACOBS 660G"
    return campaign_product


# wastage week 17
# wastage and sale for 20-04-2020
wastage_cur = pd.read_excel("input_data/Data_MENY_220420.xlsx", sheet_name="Wastage", parse_dates=True,
                            usecols=['Dato', 'TEKST', 'ANTALL0 - Manglerud', 'ANTALL1 - Ringnes Park',
                                     'ANTALL2 - Nordstrand'])
wastage_cur.columns = ['type_period', 'product', 'manglerud', 'ringnes park', 'nordstrand']
wastage_cur['weekday'] = wastage_cur['type_period'].dt.strftime('%A')
wastage_cur['type_period'] = "wastage-" + wastage_cur['type_period'].dt.strftime("%Y-%m-%d")
wastage_cur = pd.melt(wastage_cur, id_vars=['product', 'weekday', 'type_period'],
                      value_vars=['manglerud', 'ringnes park', 'nordstrand'], var_name="store")

sales_cur = pd.read_excel("input_data/Data_MENY_220420.xlsx", sheet_name="Sales", parse_dates=True,
                          usecols=['Dato', 'TEKST', 'ANTALL0 = manglerud', 'ANTALL1 0 ringnes park',
                                   'ANTALL2 = Nordstrand'])
sales_cur.columns = ['type_period', 'product', 'manglerud', 'ringnes park', 'nordstrand']
sales_cur['weekday'] = sales_cur['type_period'].dt.strftime("%A")
sales_cur = pd.melt(sales_cur, id_vars=['product', 'weekday', 'type_period'],
                    value_vars=['manglerud', 'ringnes park', 'nordstrand'], var_name="store")
sales_cur['type_period'] = 'sales-' + sales_cur['type_period'].dt.strftime("%Y-%m-%d")
df_200420 = pd.concat([sales_cur, wastage_cur], axis=0)

df_210420 = pd.concat([westage_current_date("input_data/Svinn pr dag 21.04.2020.xlsx"),
                       sales_current_date("input_data/Salg pr dag 21.04.2020.xlsx")], axis=0)
df_220420 = pd.concat([westage_current_date("input_data/Svinn pr dag 22.04.2020.xlsx"),
                       sales_current_date("input_data/Salg pr dag 22.04.2020.xlsx")], axis=0)
df_230420 = pd.concat([westage_current_date("input_data/Svinn pr dag 23.04.2020.xlsx"),
                       sales_current_date("input_data/Salg pr dag 23.04.2020.xlsx")], axis=0)
df_240420 = pd.concat([westage_current_date("input_data/Svinn pr dag 24.04.2020.xlsx"),
                       sales_current_date("input_data/Salg pr dag 24.04.2020.xlsx")], axis=0)
df_250420 = pd.concat([westage_current_date("input_data/Svinn pr dag 25.04.2020.xlsx"),
                       sales_current_date("input_data/Salg pr dag 25.04.2020.xlsx")], axis=0)

df = pd.concat([df_base, df_200420, df_210420, df_220420, df_230420, df_240420, df_250420], axis=0)
df = df[df['store'].isin(['manglerud', 'ringnes park', 'nordstrand'])]
df['value'] = round(df['value'], 2)
df = pd.merge(df, focus_products_seg, how="left", on=["product"])

campaign_product=cam_product(17)

df.loc[(df['type_period'] == 'sales_base_NOT ON CAMPAGIN') & (df['product'] == campaign_product), 'value'] = \
    df[(df['type_period'] == 'sales_base_ON CAMPAGIN') & (df['product'] == campaign_product)]['value'].values

df_not_camp_sales = df[df['type_period'] == "sales_base_NOT ON CAMPAGIN"]
df_not_camp_sales.iloc[:]['type_period'] = 'sales_base'

df = df[~df['type_period'].isin(["sales_base_ON CAMPAGIN", "sales_base_NOT ON CAMPAGIN"])]

df.loc[df['type_period'].str.contains('wastage-'), 'type_period'] = "wastage"
df.loc[df['type_period'].str.contains('sales-'), 'type_period'] = 'sales'

df_week17 = pd.concat([df, df_not_camp_sales], axis=0).assign(week=17)

#######################################################################################################################

df_270420 = pd.concat([westage_current_date("input_data/Svinn pr dag 27.04.2020.xlsx"),
                       sales_current_date("input_data/Salg pr dag 27.04.2020.xlsx")], axis=0)
df_280420 = pd.concat([westage_current_date("input_data/Svinn pr dag 28.04.2020.xlsx"),
                       sales_current_date("input_data/Salg pr dag 28.04.2020.xlsx")], axis=0)
df_290420 = pd.concat([westage_current_date("input_data/Svinn pr dag 29.04.2020.xlsx"),
                       sales_current_date("input_data/Salg pr dag 29.04.2020.xlsx")], axis=0)
df_300420 = pd.concat([westage_current_date("input_data/Svinn pr dag 30.04.2020.xlsx"),
                       sales_current_date("input_data/Salg pr dag 30.04.2020.xlsx")], axis=0)
df_020520 = pd.concat([westage_current_date("input_data/Svinn pr dag 02.05.2020.xlsx"),
                       sales_current_date("input_data/Salg pr dag 02.05.2020.xlsx")], axis=0)

df = pd.concat([df_base, df_270420, df_280420, df_290420, df_300420, df_020520], axis=0)
df = df[df['store'].isin(['manglerud', 'ringnes park', 'nordstrand'])]

df['value'] = round(df['value'], 2)
df = pd.merge(df, focus_products_seg, how="left", on=["product"])

campaign_product=cam_product(18)

df.loc[(df['type_period'] == 'sales_base_NOT ON CAMPAGIN') & (df['product'] == campaign_product), 'value'] = \
    df[(df['type_period'] == 'sales_base_ON CAMPAGIN') & (df['product'] == campaign_product)]['value'].values

df_not_camp_sales = df[df['type_period'] == "sales_base_NOT ON CAMPAGIN"]
df_not_camp_sales.iloc[:]['type_period'] = 'sales_base'

df = df[~df['type_period'].isin(["sales_base_ON CAMPAGIN", "sales_base_NOT ON CAMPAGIN"])]

df.loc[df['type_period'].str.contains('wastage-'), 'type_period'] = "wastage"
df.loc[df['type_period'].str.contains('sales-'), 'type_period'] = 'sales'

df_week18 = pd.concat([df, df_not_camp_sales], axis=0).assign(week=18)

#######################################################################################################################

df_040520 = pd.concat([westage_current_date("input_data/Svinn pr dag 04.05.2020.xlsx"),
                       sales_current_date("input_data/Salg pr dag 04.05.2020.xlsx")], axis=0)

df = pd.concat([df_base, df_040520], axis=0)
df = df[df['store'].isin(['manglerud', 'ringnes park', 'nordstrand'])]

df['value'] = round(df['value'], 2)
df = pd.merge(df, focus_products_seg, how="left", on=["product"])

campaign_product=cam_product(19)

df.loc[(df['type_period'] == 'sales_base_NOT ON CAMPAGIN') & (df['product'] == campaign_product), 'value'] = \
    df[(df['type_period'] == 'sales_base_ON CAMPAGIN') & (df['product'] == campaign_product)]['value'].values

df_not_camp_sales = df[df['type_period'] == "sales_base_NOT ON CAMPAGIN"]
df_not_camp_sales.iloc[:]['type_period'] = 'sales_base'

df = df[~df['type_period'].isin(["sales_base_ON CAMPAGIN", "sales_base_NOT ON CAMPAGIN"])]

df.loc[df['type_period'].str.contains('wastage-'), 'type_period'] = "wastage"
df.loc[df['type_period'].str.contains('sales-'), 'type_period'] = 'sales'

df_week19 = pd.concat([df, df_not_camp_sales], axis=0).assign(week=19)

final_data=pd.concat([df_week17, df_week18, df_week19], axis=0)
final_data=final_data.groupby(by=['store', 'product', 'weekday', 'type_period', 'segment', 'week'])['value'].sum().reset_index()
# the same product but with different number code
final_data_sales=final_data[final_data['type_period']=='sales'].merge(master_file,
                                                     how='left',
                                                     on=['store', 'product', 'weekday', 'week'],
                                                     validate='1:1')
final_data_sales.iloc[:]['type_period'] = "order_diff"
final_data_sales.drop(columns=['value'], inplace=True)
final_data_sales.rename(columns={'order_diff': 'value'}, inplace=True)

final_data = pd.concat([final_data, final_data_sales], axis=0)
final_data.to_excel("output_data/pivot_week_17_18_19_2.xlsx")
print("Program run successfully!!!")
