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


cur_n = 'P'
pre_n = 'N'

# %%
cur_prq = m.within_exe_data_dir + f'/{cur_n}' + m.prqsuf
pre_prq = m.within_exe_data_dir + f'/{pre_n}' + m.prqsuf
symb_name_xl = m.proj_dir + '/Symbol_Names_Map.xlsx'

# %%
sym_name = pd.read_excel(symb_name_xl)
sym_name['Symbol'] = sym_name['Symbol'].apply(m.wos)
sym_name = sym_name.drop_duplicates(subset = 'Symbol')
# sym_name.to_excel(symb_name_xl , index = False)
sym_name = sym_name.replace(to_replace = np.nan , value = '')
for col in sym_name.columns.difference(['Symbol']) :
    sym_name[col + '_wos'] = sym_name[col].apply(m.wos)
sym_name


# %%
# sym_name2 = pd.read_parquet(m.proj_dir + '/1' + m.prqsuf)
# sym_name2

# %%
# sym_name = sym_name.merge(sym_name2 , how = 'outer')
# sym_name = sym_name.drop_duplicates(subset = 'Symbol')
# sym_name

# %%
# sym_name.to_excel(symb_name_xl , index = False)


# %%


def find_symbol_by_name(inp: str) :
    for col1 in sym_name.columns :
        findsym = sym_name[sym_name[col1].eq(inp)]
        if len(findsym) == 1 :
            return findsym['Symbol'].tolist()[0]

    for colno in range(1 , sym_name.shape[1]) :
        check_name = sym_name.iloc[: , colno].isin([inp])
        check_name |= sym_name.iloc[: , colno].apply(lambda x : inp in x)
        check_name = check_name[check_name]
        if len(check_name) == 1 :
            return sym_name.loc[check_name.index , 'Symbol'].values.tolist()[0]


def main() :
    pass

    # %%


    df = m.read_parquet(pre_prq)
    df

    # %%
    df = m.update_with_last_data(df , cur_prq)

    # %%
    df['IsPreferred'] = df['Asset'].apply(lambda x : '(تقدم)' in x)

    # %%
    df['IsStocks'] = ~ df['IsPreferred']

    # %%
    df.loc[df['IsPreferred'] , 'Asset'] = df['Asset'].apply(lambda x : x.replace('(تقدم)' , ''))

    # %%
    cnd = df['SharesNoCheck'].eq(True)
    cnd &= df['Firm'].isna()
    cnd[cnd]

    # %%
    flt = df[cnd]
    flt

    # %%
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

        assetcol = df.loc[indices , 'Asset']

        out = pool.map(find_symbol_by_name , assetcol)

        df.loc[indices , 'Firm'] = out

        print(df.loc[indices , 'Firm'])
        # break

    # %%
    m.save_as_parquet(df , cur_prq)


# %%


if __name__ == '__main__' :
    main()

# noinspection PyUnreachableCode
if False :
    pass

    # %%

    # %%
