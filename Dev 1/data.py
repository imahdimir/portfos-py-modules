##

import pandas as pd

##
fp0 = '/Users/mahdimir/Documents/DATA/TEPIX/Daily Tepix/Daily Tepix-71-97.xlsx'
df = pd.read_excel(fp0)
##
df = df.drop_duplicates()

##
df = df.drop_duplicates(subset = 'JDate')

##
df.to_excel(fp0)

##
fp1 = '/Users/mahdimir/Documents/DATA/Market/Indices/TSE indexes/TSE indexes.xlsx'
df1 = pd.read_excel(fp1)
##
df1 = df1[['JDate' , 'Tepix']]

##
df2 = df.merge(df1 , how = 'outer')

##
df3 = df2[df2.duplicated(subset = 'JDate' , keep = False)]

##
df3 = df3.sort_values('JDate')

##
df2.to_excel(fp0 , index = False)

##
fp0 = '/Users/mahdimir/Documents/DATA/TEPIX/TEPIX Daily Details/TEPIX Daily Details.xlsx'
df = pd.read_excel(fp0)

##
from persiantools.jdatetime import JalaliDate
from datetime import datetime

##
dt0 = datetime.strptime('20040629' , '%Y%m%d')
dt0

##
JalaliDate.to_jalali(dt0).strftime('%Y%m%d')

##
df['datet'] = df['Date'].apply(lambda
                                 x : datetime.strptime(str(x) , '%Y%m%d'))
##
df['JDate'] = df['datet'].apply(lambda
                                  x : JalaliDate.to_jalali(x).strftime('%Y%m%d'))

##
df.columns

##
df = df[
  ['JDate' , '<Open>' , '<High>' , '<Low>' , '<Close>' , '<Vol>' , '<Openint>']]
##


##
fp0 = '/Users/mahdimir/Documents/DATA/TEPIX/TEPIX Daily Details/TEPIX Daily Details.xlsx'
df = pd.read_excel(fp0)

##
fp1 = '/Users/mahdimir/Documents/DATA/TEPIX/Daily Tepix/Daily Tepix.xlsx'
df1 = pd.read_excel(fp1)

##
df2 = df.merge(df1 , how = 'outer')

##
msk = df2['<Close>'].isna()

##
df2.loc[msk , '<Close>'] = df2['Tepix']

##
msk1 = df2['<Close>'].ne(df2['Tepix'])
df3 = df2[msk1]

##
df2.to_excel(fp0 , index = False)

##
fp0 = '/Users/mahdimir/Documents/DATA/EWI/EWI Daily.xlsx'
df = pd.read_excel(fp0)

##
df['datet'] = df['Date'].apply(lambda
                                 x : datetime.strptime(str(x) , '%Y%m%d'))
##
df['JDate'] = df['datet'].apply(lambda
                                  x : JalaliDate.to_jalali(x).strftime('%Y%m%d'))

##
df = df[
  ['JDate' , '<Open>' , '<High>' , '<Low>' , '<Close>' , '<Vol>' , '<Openint>']]

##
df.to_excel(fp0 , index = False)

##
fp1 = '/Users/mahdimir/Documents/DATA/Market/Indices/TSE indexes/TSE indexes.xlsx'
df1 = pd.read_excel(fp1)

##
df1 = df1[['JDate' , 'EWI']]

##
df1 = df1.dropna()
##
df['JDate'] = df['JDate'].astype(str)

df1['JDate'] = df1['JDate'].astype(str)

##
df2 = df.merge(df1 , how = 'outer')

##
msk = df2['<Close>'].isna()

##
df2.loc[msk , '<Close>'] = df2['EWI']

##
msk1 = df2['<Close>'].ne(df2['EWI'])
df3 = df2[msk1]

##
df2 = df2.drop(columns = 'EWI')

##
df2.to_excel(fp0 , index = False)

##
fp0 = '/Users/mahdimir/Documents/DATA/Market/Industry indexes/Industry indexes 1399-09-28.csv'
df = pd.read_csv(fp0)

##
df.columns
##
df = df[['index_id' , 'date' , 'index']]

##
df['JDate'] = df['date'].apply(lambda
                                 x : JalaliDate(int(x.split('/')[0]) ,
                                                int(x.split('/')[1]) ,
                                                int(x.split('/')[2])).strftime(
    '%Y%m%d'))

##
df.columns

##
df = df[['JDate' , 'index_id' , 'index']]

##
df.to_excel('1.xlsx' , index = False)

##
fp0 = '/Users/mahdimir/Documents/DATA/Industries indices/Industries indices.xlsx'
df = pd.read_excel(fp0)

##
msk = df['index_id'].isin(['overall_index' , 'EWI'])
df1 = df[msk]

##
df = df[~ msk]

##
df.to_excel(fp0 , index = False)

##
fp0 = '/Users/mahdimir/Documents/DATA/Stock details/TseID - Symbol/TseID-Symbol-9906.csv'
df = pd.read_csv(fp0)
##
df.to_excel('1.xlsx' , index = False)

##
fp0 = '/Users/mahdimir/Documents/DATA/Rights Offerings/Rights Offerings 1389 to 1397.xlsx'
df = pd.read_excel(fp0 , sheet_name = None)

##
df1 = df['1397']

##
df = df.replace(to_replace = 0 , value = None)

##
df = df.dropna(how = 'all' , subset = df.columns.difference(['JDate']))

##
df.to_excel(fp0 , index = False)

##
fp0 = '/Users/mahdimir/Documents/DATA/Bonds/Bonds_history.xlsx'
df = pd.read_excel(fp0)

##
df = df1
##
lst = df.columns
for el in lst :
  print(el , end = ', ')

##
df1 = df[df.duplicated()]

df = df.drop_duplicates()

##
df = df.dropna()

##
df['JDate'] = df['JDate'].str.replace('/' , '')

##
df['JDate'] = df['JDate'].str.fullmatch(r'1[34][056789]\d[01')

##
fp1 = '/Users/mahdimir/Documents/DATA/BlockHolders - Annual/BlockHolders - 800308-990528 - Annual.csv'
df = pd.read_csv(fp1 , low_memory = False)

##
df['year'].min()

##
df.to_parquet(
    '/Users/mahdimir/Documents/DATA/Stock details/RDIS-Adjusted-Price/RDIS-Adjusted-Price-870916-970823.prq' ,
    index = False)

##
fp = '/Users/mahdimir/Documents/DATA/RDIS-Adjusted-Price/RDIS-Adjusted-Price-870916-970823.prq'

##
df = pd.read_parquet(fp)

##
