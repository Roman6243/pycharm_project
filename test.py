import numpy as np
import pandas as pd
import plotly.express as px
import matplotlib as plt
nf_df = pd.read_csv(r'C:\Users\PC1\Downloads\netflix-shows\netflix_titles.csv')
nf_test1_df = nf_df
nf_test1_df['like'] = True
print(len(nf_df.columns))

nf_test2_df = nf_df

nf_test2_df = nf_test2_df.assign(like=True)

print(len(nf_df.columns))

fig = px.histogram(nf_df, x='release_year')
fig.show()

x=pd.Series([1,2,3,4,5,])
df=pd.DataFrame(np.random.rand(4,3))

df.apply(lambda x: sum(x.isnull()), axis=0)
df['var'].fillna(df[0])
from sklearn.model_selection import train_test_split

df[df['column'].notnull()]