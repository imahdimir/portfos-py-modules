# %%


import pandas as pd
import main as m
from multiprocessing import cpu_count
from multiprocess import Pool
from persiantools import characters
from persiantools import digits as perdigit
from string import digits
import numpy as np


# %%


cur_n = 'N'
pre_n = 'M'
cur_prq = m.within_exe_data_dir + f'/{cur_n}' + m.prqsuf
pre_prq = m.within_exe_data_dir + f'/{pre_n}' + m.prqsuf
symb_name_xl = m.proj_dir + '/Symbol_Names_Map.xlsx'

# %%


sym_name = pd.read_excel(symb_name_xl)


# %%


def find_symbol_by_name(inp: str) :
    for col1 in sym_name.columns :
        findsym = sym_name[sym_name[col1].eq(inp)]
        if len(findsym) == 1 :
            return findsym['Symbol'].tolist()[0]

    for colno in range(0 , sym_name.shape[1]) :
        check_name = sym_name.iloc[: , colno].isin([inp])
        check_name |= sym_name.iloc[: , colno].apply(lambda x : inp in x)
        check_name = check_name[check_name]
        if len(check_name) == 1 :
            return sym_name.loc[check_name.index , 'Symbol'].values.tolist()[0]

    inp1 = findBetween(inp , '(' , ')')
    for col1 in sym_name.columns :
        findsym = sym_name[sym_name[col1].eq(inp1)]
        if len(findsym) == 1 :
            return findsym['Symbol'].tolist()[0]


def findBetween(s , start , end) :
    try :
        return (s.split(start))[1].split(end)[0]
    except IndexError :
        return None


def main() :
    pass

    # %%


    df = m.read_parquet(pre_prq)
    df

    # %%
    df['Firm'] = None

    # %%
    df = m.update_with_last_data(df , cur_prq)

    # %%
    remcnd = ['نقلبهصفحهبعد' , 'نقلازصفحهقبل']
    rcnd = df['Asset_wos'].isin(remcnd)
    df = df[~ rcnd]
    df

    # %%
    df['Asset_wos'] = df['Asset_wos'].apply(lambda x : x.replace('شرکت' , ''))
    df['Asset_wos'] = df['Asset_wos'].apply(lambda x : x.replace('_' , ''))
    df['Asset_wos'] = df['Asset_wos'].apply(lambda x : x.replace('-' , ''))
    df['Asset_wos'] = df['Asset_wos'].apply(lambda x : x.replace('بانک' , ''))
    df['Asset_wos'] = df['Asset_wos'].apply(lambda x : x.replace('(تقدم)' , ''))

    # %%
    cnd = df['SharesNoCheck'].eq(True)
    cnd &= df['Firm'].isna()
    cnd[cnd]

    # %%
    flt = df[cnd]
    flt

    # %%
    cores_n = cpu_count()
    print(f'Num of cores : {cores_n}')
    clusters = m.return_clusters_indices(flt)
    pool = Pool(cores_n)

    # %%
    for i in range(0 , len(clusters) - 1) :
        strtI = clusters[i]
        endI = clusters[i + 1]
        print(f"Cluster index of {strtI} to {endI}")

        indices = flt.iloc[strtI :endI].index

        assetcol = df.loc[indices , 'Asset_wos']

        out = pool.map(find_symbol_by_name , assetcol)

        df.loc[indices , 'Firm'] = out

        print(df.loc[indices , 'Firm'])
    # break

    # %%
    m.save_as_parquet(df , cur_prq)

    # %%
    df['Asset_wos'].unique().tolist()


# %%


if __name__ == '__main__' :
    main()

# noinspection PyUnreachableCode
if False :
    pass

    # %%

    # %%
