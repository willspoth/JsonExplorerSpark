import json
import pandas as pd
import numpy as np
import glob
import matplotlib.pyplot as plt
import os
import seaborn as sns


# dfs = []
# for f in glob.glob('C:\\Users\\William\\Desktop\\VLDB_results\\*\\*.log.res'):
#     df = pd.read_json(f, lines=True)
#     df['Algorithm'] = os.path.basename(f).split('.')[1]
#     if('Recall' in df.columns):
#     	df = df.rename(columns={'Recall':'Validation'})
#     dfs.append(df)

# df = pd.concat(dfs)

# cdfs = []
# for f in glob.glob('C:\\Users\\William\\Desktop\\VLDB_results\\*\\*.log.res.corr'):
# 	with open(f,'r') as file:
# 		lines = file.readlines()
# 		for line in lines:
# 			jl = json.loads(line)
# 			df.loc[(df['inputFile'] == jl['inputFile']) & (df['Algorithm'] == os.path.basename(f).split('.')[1]) & (df['TrainPercent'] == float(jl['TrainPercent'])) & (df['Seed'] == jl['Seed']),'Precision'] = jl['Precision']
#     #ds = pd.read_json(f, lines=True)
#     #ds['Algorithm'] = os.path.basename(f).split('.')[1]
#     #cdfs.append(df)

# #cdf = pd.concat(cdfs)

# file_mappings = {
# 	'Data/yelpFull.json':'Yelp_Merged',
# 	'Data/yelp/business.json':'Yelp_Business',
# 	'Data/yelp/tip.json':'Yelp_Tip',
# 	'Data/yelp/user.json':'Yelp_User',
# 	'Data/yelp/checkin.json':'Yelp_Checkin',
# 	'Data/yelp/photos.json':'Yelp_Photos',
# 	'Data/yelp/review.json':'Yelp_Review',
# 	'Data/githubFull.json':'Github',
# 	'Data/synapse.json':'Synapse',
# 	'Data/nyt2019clean.json':'NYT',
# 	'Data/twitter.json':'Twitter',
# 	'Data/medicineFull.json':'Pharma'
# }

df = pd.read_csv('experiments.csv')

#df['inputFile'] = df['inputFile'].map(lambda x: file_mappings[x])

#print(df.merge(cdf,how='inner',left_on=['inputFile','Algorithm','TrainPercent'],right_on=['inputFile','Algorithm','TrainPercent']).columns)
#df[['inputFile','Precision','Validation','Grouping','TotalTime','BaseSchemaSize','TrainPercent','Seed','Algorithm']].sort_values(by=['inputFile','TrainPercent','Algorithm']).to_csv('experiments.csv')

#inputfiles = df['inputFile'].unique().tolist()
#print(inputfiles)
#fig, axes = plt.subplots(sharey=False)
#for ax in axes:
import math

df['Precision'] = df['Precision'].map(lambda x: math.log2(int(x.split('.')[0])))
#df.groupby('inputFile').boxplot(column=['Precision'],by=['Algorithm'],rot=90,sharey=False,figsize=(20,26))
#df.groupby('inputFile').boxplot(column=['Validation'],by=['TrainPercent','Algorithm'],rot=90,sharey=False,figsize=(20,26))
df.groupby('inputFile').boxplot(column=['TotalTime'],by=['Algorithm'],rot=90,sharey=False,figsize=(20,26))
#plt.legend(['bimax','Baazizi','BaaziziNorm'])

#sns.boxplot(x='inputFile',y="Validation", data=df, ax=ax)
#plt.xticks(rotation=90)
plt.tight_layout()
plt.savefig('runtime.pdf',dpi=2400)