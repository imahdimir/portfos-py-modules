# %%


import pandas as pd
import main as m
import re
import numpy as np
from multiprocessing import cpu_count
from multiprocess import Pool


# %%


cur_n = 'M1'
pre_n = 'M'

# %%


cur_prq = m.within_exe_data_dir + f'/{cur_n}' + m.prqsuf
pre_prq = m.within_exe_data_dir + f'/{pre_n}' + m.prqsuf
symb_name_xl = m.proj_dir + '/Symbol_Names_Map.xlsx'

# %%


share_types = {
        1 : 'Stocks' , 2 : 'PreferredStock' , 3 : 'Call' , 4 : 'Put' , 5 : 'Future' , 6 : 'RealEstateLoanPreference' ,
        7 : 'Bond'
        }
preferred_stock = ['تقدم' , 'حق']
outputs = ['CleanedAssetName' , 'ShareType' , 'Symbol']
outputs_dict = m.make_output_dict(outputs)
outputs_dict

# %%


sym_name = pd.read_excel(symb_name_xl)
sym_name


# %%


class asset :


    def __init__(self , asset_n) :
        self.asset_n = asset_n
        self.asset_wos = m.wos(asset_n)
        self.output = outputs_dict.copy()


    def is_preffered(self) :
        if self.asset_wos[-1] == 'ح' :
            self.output['ShareType'] = share_types[2]
            self.output['Symbol'] = self.asset_wos[:-1]
            return None

        if self.asset_wos[0 : 2] == 'ح.' :
            self.output['ShareType'] = share_types[2]
            self.asset_wos = self.asset_wos[2 :]
            return None

        if m.any_of_list_isin(preferred_stock , self.asset_wos) :
            self.output['ShareType'] = share_types[2]
            for el in preferred_stock :
                self.asset_wos = self.asset_wos.replace(el , '')
            return None


    def is_symbol(self) :
        for col1 in sym_name.columns :
            cond = sym_name[col1].eq(self.asset_wos)
            findsym = sym_name[cond]
            if len(findsym['Symbol'].unique()) == 1 :
                self.output['Symbol'] = findsym['Symbol'].unique().values()[0]
                self.output['ShareType'] = share_types[1]
                return None


    def is_put(self) :
        if 'اختیارف' in self.asset_wos :
            self.output['ShareType'] = share_types[4]


    def is_call(self) :
        if 'اختیارخ' in self.asset_wos :
            self.output['ShareType'] = share_types[3]


    def is_realestate(self) :
        if 'امتیازتسهیلاتمسکن' in self.asset_wos :
            self.output['ShareType'] = share_types[6]


    def is_future(self) :
        if 'سکهتمام' in self.asset_wos :
            self.output['ShareType'] = share_types[5]


    def is_bond(self) :
        if m.any_of_list_isin(['اسنادخزانه' , 'اخزا'] , self.asset_wos) :
            self.output['ShareType'] = share_types[7]
            return None


    def find_symbol1(self) :
        for col1 in sym_name.columns :
            cond = sym_name[col1].isin([self.asset_wos])
            findsym = sym_name[cond]
            if len(findsym['Symbol'].unique()) == 1 :
                self.output['Symbol'] = findsym['Symbol'].unique().values()[0]
                self.output['ShareType'] = share_types[1]


    def find_symbol2(self) :
        for col1 in sym_name.columns :
            cond = sym_name[col1].apply(lambda x : self.asset_wos in x)
            findsym = sym_name[cond]
            if len(findsym['Symbol'].unique()) == 1 :
                self.output['Symbol'] = findsym['Symbol'].unique().values()[0]
                self.output['ShareType'] = share_types[1]


    def clean_name(self) :
        rem = ['-' , '_' , ')' , '(']
        for el in rem :
            self.asset_wos = self.asset_wos.replace(el , '')
        self.output['CleanedAssetName'] = self.asset_wos


    def clean_name_2(self) :
        rem = ['.']
        for el in rem :
            self.asset_wos = self.asset_wos.replace(el , '')
        self.output['CleanedAssetName'] = self.asset_wos


    def clean_name_3(self) :
        rem = ['سهامیعام' , 'هلدینگ' , 'سهام']
        for el in rem :
            self.asset_wos = self.asset_wos.replace(el , '')
        self.output['CleanedAssetName'] = self.asset_wos


    def do_nothing(self) :
        pass


    def process(self) :
        for func in [self.do_nothing , self.clean_name , self.clean_name_2 , self.clean_name_3] :
            func()
            for method in [self.is_preffered , self.is_symbol , self.is_call , self.is_put , self.is_realestate ,
                           self.is_future , self.is_bond , self.find_symbol1 , self.find_symbol2] :
                method()
                if not self.output['ShareType'] :
                    return self.output


def findBetween(s , start , end) :
    try :
        return (s.split(start))[1].split(end)[0]
    except IndexError :
        return None


def target(asset_n) :
    obj = asset(asset_n)
    out = obj.process()
    return list(out.values())


def main() :
    pass

    # %%


    df = m.read_parquet(pre_prq)
    df

    # %%
    df[outputs] = None

    # %%
    df = m.update_with_last_data(df , cur_prq)

    # %%
    cnd = df['SharesNoSumCheck'].eq(True)
    cnd &= df['ShareType'].isna()
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

        assetcol = df.loc[indices , 'Asset']

        out = pool.map(target , assetcol)

        for j , outp in enumerate(outputs) :
            df.loc[indices , outp] = [w[j] for w in out]

        print(df.loc[indices , outputs])
        # break

    # %%
    m.save_as_parquet(df , cur_prq)

    # %%
    df[df['ShareType'] == share_types[2]]


# %%


if __name__ == '__main__' :
    main()

# noinspection PyUnreachableCode
if False :
    pass

    # %%
    str1 = 'جمعنقلازصفحهقبل'
    re.match(r'.*نقل.*' , str1)

    # %%
