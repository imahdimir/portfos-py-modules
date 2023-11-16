# %%


import pandas as pd
import main as m
import re
import numpy as np


# %%


cur_n = 'M'
pre_n = 'L'
cur_prq = m.within_exe_data_dir + f'/{cur_n}' + m.prqsuf
pre_prq = m.within_exe_data_dir + f'/{pre_n}' + m.prqsuf

# %%


rawportfoliodata_prq = m.within_exe_data_dir + '/raw_portfolio_data' + m.prqsuf
cols1 = ['Start_SharesNo' , 'InPeriod_Buy_SharesNo' , 'InPeriod_Sale_SharesNo' , 'End_SharesNo']


# %%


def main() :
    pass

    # %%


    df = m.read_parquet(rawportfoliodata_prq)
    df

    # %%
    df = df[['FundSymbol' , 'StartJMonth' , 'EndJMonth' , 'Asset'] + cols1]
    df

    # %%
    df['SharesNoSumCheck'] = None

    # %%
    rcnd = df.isna().all(axis = 1)
    rcnd |= df['Asset'].isna()
    rcnd[rcnd]

    # %%
    df = df[~ rcnd]
    df

    # %%
    rcnd |= df[cols1].isna().all(axis = 1)
    rcnd[rcnd]

    # %%
    df = df[~ rcnd]
    df

    # %%
    df['Asset_wos'] = df['Asset'].apply(m.wos)
    df

    # %%
    df['Asset_wos'] = df['Asset_wos'].apply(lambda x : x.replace(':' , ''))
    df['Asset_wos'] = df['Asset_wos'].apply(lambda x : x.replace('-' , ''))
    df['Asset_wos'] = df['Asset_wos'].apply(lambda x : x.replace('_' , ''))

    # %%
    remlist = ['جمع' , 'مجموع' , '' , '-' , ' ' , '0' , '___' , 'جمعکل']
    com_n_hdr = ['نام شرکت' , 'شرکت' , 'نام سهم' , 'سهم' , 'شرکتسرمایهپذیروصندوقسرمایهگذاری' , 'AssetName' ,
                 'نامسهام']

    wos_search = [m.wos(x) for x in com_n_hdr]
    df = df[~ df['Asset_wos'].isin(remlist + wos_search)]
    df

    # %%
    df = df[~ df['Asset_wos'].apply(
            lambda x : pd.notnull(re.match(r'.*نقل.*' , x)) & pd.notnull(re.match(r'.*صفحه.*' , x)))]
    df

    # %%
    df = df[~ df['Asset_wos'].apply(
            lambda x : pd.notnull(re.match(r'.*نقل.*' , x)) & pd.notnull(re.match(r'.*جمع.*' , x)))]
    df

    # %%
    tdf = df[df['Asset_wos'].apply(lambda x : re.match(r'^\d*$' , x)).notna()]

    # %%
    df = df[df['Asset_wos'].apply(lambda x : re.match(r'^\d*$' , x)).isna()]
    df

    # %%
    for col in cols1 :
        df = df[df[col].apply(lambda x : re.match(r'^\d*$' , str(x))).notna() | df[col].isna()]
    df

    # %%
    for col in cols1 :
        # df[col] = df[col].apply(lambda x : m.wos(x) if not pd.isna(x) else x)
        df[col] = df[col].apply(lambda x : np.nan if x in ['' , ' '] else x)
    df

    # %%
    for col in cols1 :
        df.loc[df[col].notna() , col] = df.loc[df[col].notna() , col].astype(int)
    df

    # %%
    df['SharesNoSumCheck'] = df[cols1[0]].fillna(0) + df[cols1[1]].fillna(0) - df[
        cols1[2]].fillna(0) == df[cols1[3]].fillna(0)
    df

    # %%
    for col in cols1 :
        df.loc[df['SharesNoSumCheck'].ne(True) , col] = df[col].apply(lambda x : 0 if 0 < abs(x) < 10 else x)
    df

    # %%
    df.loc[df['SharesNoSumCheck'].ne(True) , 'SharesNoSumCheck'] = df[cols1[0]].fillna(0) + df[cols1[1]].fillna(0) - df[
        cols1[2]].fillna(0) == df[cols1[3]].fillna(0)
    df

    # %%
    df[df['SharesNoSumCheck'].eq(False)]

    # %%
    df[df['SharesNoSumCheck'].eq(True)]

    # %%
    for col in cols1 :
        df.loc[df['SharesNoSumCheck'].eq(True) , col] = df[col].fillna(0)
    df

    # %%
    for col in cols1 :
        df.loc[df[col].notna() , col] = df.loc[df[col].notna() , col].astype(int)
    df

    # %%
    m.save_as_parquet(df , cur_prq)

    # %%


    if __name__ == '__main__' :
        main()

    # noinspection PyUnreachableCode
    if False :
        pass

        # %%
