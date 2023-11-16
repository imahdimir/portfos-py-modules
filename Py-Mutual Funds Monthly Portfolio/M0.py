# %%


import pandas as pd
import main as m
from persiantools import characters
from persiantools import digits as perdigit
import re
import numpy as np


# %%

symb_name_xl = m.proj_dir + '/Symbol_Names_Map.xlsx'


def main() :
    pass

    # %%


    df = pd.read_excel(symb_name_xl)
    df

    # %%
    df = df.applymap(lambda x : characters.ar_to_fa(str(x)))
    df = df.applymap(lambda x : perdigit.ar_to_fa(str(x)))
    df = df.applymap(lambda x : perdigit.fa_to_en(str(x)))
    df

    # %%
    df['Symbol'] = df['Symbol'].apply(m.wos)
    df

    # %%
    rcnd2 = df['Symbol'].apply(lambda x : re.match(r'.*ح$' , x)).notna()
    df = df[~ rcnd2]
    df

    # %%
    df = df.applymap(m.wos)

    # %%
    df = df[df['Symbol'] != df['Name']]
    df

    # %%
    df.to_excel(symb_name_xl , index = False)
    df


# %%


if __name__ == '__main__' :
    main()

# noinspection PyUnreachableCode
if False :
    pass

    # %%

    # %%
