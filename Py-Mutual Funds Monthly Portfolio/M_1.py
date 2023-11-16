# %%


import pandas as pd
import main as m
from persiantools import characters
from persiantools import digits as perdigit
import re
from string import digits


# %%

symb_name_xl = m.proj_dir + '/Symbol_Names_Map.xlsx'

remove_digits = str.maketrans('' , '' , digits)


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
    df['Name_WOS'] = df['Name'].apply(m.wos)
    df

    # %%
    df['Symbol'] = df['Symbol'].apply(lambda x : x.translate(remove_digits))

    # %%
    df = df.drop_duplicates(subset = 'Symbol')
    df

    # %%
    rcnlist1 = ['اختیارخ' , 'اختیارف' , 'اسناد خزانه' , 'تسهیلات مسکن' , 'منفعت' , 'اجاره' , 'صکوک' , 'مشارکت' ,
                'مرابحه' , 'سلف' , 'سکه' , 'زعفران' , 'پسته' , 'اسنادخزانه' , 'اوراق']
    rcnd1 = df['Name'].apply(lambda x : m.any_of_list_isin(rcnlist1 , str(x)))
    rcnd1 |= df['Name_WOS'].apply(lambda x : m.any_of_list_isin(rcnlist1 , str(x)))
    rcnd1[rcnd1]

    # %%
    df = df[~rcnd1]
    df

    # %%
    rcnd2 = df['Symbol'].apply(lambda x : re.match(r'.*ح$' , x)).notna()
    df = df[~ rcnd2]
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
