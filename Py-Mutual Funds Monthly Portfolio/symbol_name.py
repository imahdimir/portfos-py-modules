# %%


import pandas as pd
import main as m


# %%


symb_name_xl = m.proj_dir + '/MarketWatch.xlsx'


# %%

def main() :
    pass

    # %%


    sym = pd.read_excel(symb_name_xl)
    sym

    # %%
    sym = sym.dropna()
    sym

    # %%
    sym = sym.applymap(m.ar_to_fa)
    sym

    # %%
    sym = sym.drop_duplicates()
    sym

    # %%
    rcnd5 = sym.duplicated(subset = 'Symbol')
    rcnd5[rcnd5]

    # %%
    sym = sym[~rcnd5]
    sym

    # %%
    sym = sym.sort_values(by = 'Symbol')
    sym

    # %%

    sym.to_excel(symb_name_xl , index = False)


# # %%
#
#
# if __name__ == '__main__' :
#     main()
#
# # noinspection PyUnreachableCode
# if False :
#     pass
#
#     # %%
#
#     # %%

# %%
