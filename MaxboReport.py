import pandas as pd
from dfcleaner import cleaner
from docx import Document
from docx.oxml.shared import OxmlElement, qn
from docx.shared import Pt
from docx.shared import Inches
import numpy as np
import requests
import os
os.chdir(r'C:\Users\PC1\Dropbox (BigBlue&Company)\ETC Insight\Projects\Maxbo Report')
pd.set_option('display.max_columns', 30)
pd.set_option('display.width', 1000)

activity=requests.get("https://maxbo.link.express/external/api/v2/5d02982d29512bcc1729bb3964efb830/activity/query/?activity_date=2020-03-04T00:00:00&store_alias=ALL").json()
data=pd.DataFrame()
for i in range(len(activity['store'])):
    store_name=activity['store'][i]['store_name']
    region_name=activity['store'][i]['region_name']
    user_count=activity['store'][i]['userCount']
    active_user_count=activity['store'][i]['activeUserCount']
    active_user_count_today=activity['store'][i]['activeTodayUserCount']
    temp=pd.DataFrame(activity['store'][i]['users']).assign(store_name=store_name, region_name=region_name, user_count=user_count,
                                                            active_user_count=active_user_count, active_user_count_today=active_user_count_today)
    data=pd.concat([data, temp])
data.columns=cleaner.sanitize(data.columns) # cleaning the data headers

stores_using_system=data[data['active_today']][['store_name', 'region_name']].drop_duplicates().assign(is_active='yes') # all stores which used system at least once
stores_not_using_system=data[~data['store_name'].isin(stores_using_system['store_name'])][['store_name', 'region_name']].drop_duplicates().assign(is_active='no') # all stores which not used system even once
activities=pd.concat([stores_using_system, stores_not_using_system], axis=0).reset_index(drop=True)

print('Average use of the system (per user/per store/per region/per day): ')
n_usage_per_user_store_day=data[data['session_count']>0].groupby(['region_name', 'store_name'])['session_count'].mean().reset_index().\
    groupby('region_name').mean().reset_index().rename(columns={'session_count':'n_user_store_day'})
n_not_active_stores=stores_not_using_system.groupby('region_name')['store_name'].count().reset_index().sort_values(by='region_name').rename(columns={'store_name':'n_not_active_stores'})
data_table_2=pd.merge(n_usage_per_user_store_day, n_not_active_stores, on='region_name')
data_table_2['score_goal']=(data_table_2['n_user_store_day']-2)/2
data_table_2['Goal'] = 2
data_table_2.loc['mean']=data_table_2.mean()
data_table_2.reset_index(drop=True, inplace=True)
data_table_2.loc[data_table_2['region_name'].isnull(), 'region_name'] = 'Average'
data_table_2=data_table_2.pivot_table(columns='region_name')
data_table_2=data_table_2.reset_index()
data_table_2.loc[data_table_2['index']=='n_not_active_stores', 'index'] = 'Number of stores that have not been using the system'
data_table_2.loc[data_table_2['index']=='n_user_store_day', 'index'] = 'Average use per day (nr of times)'
data_table_2.loc[data_table_2['index']=='score_goal', 'index'] = 'Score against goal'
data_table_2.rename(columns={'index': 'Region'}, inplace=True)

# make a shading for particular cells in table
def shade_cells(cells, shade):
    for cell in cells:
        tcPr = cell._tc.get_or_add_tcPr()
        tcVAlign = OxmlElement("w:shd")
        tcVAlign.set(qn("w:fill"), shade)
        tcPr.append(tcVAlign)

document = Document('input/template.docx')
# define the font family and size for entire file
style = document.styles['Normal']
font = style.font
font.name = 'Avenir Book'
font.size = Pt(10)
# generate stores activity per each region
table = document.add_table(rows=1, cols=7, style='rm')
col_names=data_table_2.columns
header_cells = table.rows[0].cells
for k in range(len(col_names)):
    header_cells[k].paragraphs[0].add_run(col_names[k]).bold=True
shade_cells([table.cell(0, 0), table.cell(0, 1), table.cell(0, 2), table.cell(0, 3), table.cell(0, 4), table.cell(0, 5), table.cell(0, 6)], "#002060")
for i in range(len(data_table_2)):
    row_cells = table.add_row().cells
    for d in range(len(col_names)):
        if data_table_2[col_names[d]].dtypes == 'float64':
            row_cells[d].text = str(round(data_table_2[col_names[d]].values[i], 2))
        else:
            row_cells[d].text = str(data_table_2[col_names[d]].values[i])
document.add_paragraph(text='\n')

# generate stores activity per each region
for region in sorted(activities['region_name'].unique()):
    activities_region=activities[activities['region_name']==region]
    print('Activity of region %s'%region)
    activities_yes=activities_region[activities_region['is_active']=='yes']
    activities_no=activities_region[activities_region['is_active']=='no']

    document.add_paragraph(text=region)
    table = document.add_table(rows=1, cols=3, style='rm')
    header_cells = table.rows[0].cells
    header_cells[0].text = ''
    header_cells[1].paragraphs[0].add_run('Which stores have been using the system').bold=True
    header_cells[2].paragraphs[0].add_run('Which stores have not been using the system').bold=True
    shade_cells([table.cell(0, 1)], "#00b050")
    shade_cells([table.cell(0, 2)], "#c00000")
    for i in range(max([len(activities_no), len(activities_yes)])):
        row_cells = table.add_row().cells
        row_cells[0].text=str(i+1)
        if i< len(activities_yes):
            row_cells[1].text=str(activities_yes['store_name'].values[i])
        if i< len(activities_no):
            row_cells[2].text=str(activities_no['store_name'].values[i])
    document.add_paragraph(text='\n')
document.save('output/Maxbo_Report.docx')
