# %%
from multiprocessing import cpu_count
import os
import numpy as np
import pandas as pd
from multiprocess import Pool
import main as m


# %%
cur_n = 'G'
pre_n = 'F'

# %%
cur_prq = m.within_exe_data_dir + f'/{cur_n}' + m.prqsuf
pre_prq = m.within_exe_data_dir + f'/{pre_n}' + m.prqsuf

portfo_sheetnames = ['سهام' , '1' , 1]
srch_sh_names = [m.wos(z) for z in portfo_sheetnames]

daramad = ['درآمد' , 'درامد' , 'حاصل' , 'سود' , 'سپرده']
daramad_srch = [m.wos(z) for z in daramad]


# %%
def del_s_dot_one_dash(inp) :
    inp = str(inp)
    inp = inp.replace('.' , '')
    inp = inp.replace('-' , '')
    inp = inp.replace(' ' , '')
    inp = inp.replace('1' , '')
    inp = inp.replace('۱' , '')
    inp = inp.replace('\u200c' , '')
    inp = inp.replace('\u202b' , '')
    return inp


portfo_sheet = ['1-1-سرمایه‌گذاری در سهام و حق تقدم سهام' ,
                '1-1-سرمایه‌گذاری در سهام و حق تقدم سهام وصندوق‌های سرمایه‌گذاری' ,
                '1-1-سرمایه‌گذاری در سهام و حق تقدم سهام  و صندوق ها' ,
                '1-1-سرمایه گذاری در سهام و حق تقدم' ,
                '1-سرمایه گذاری در سهام']

portfo_srch = [del_s_dot_one_dash(x) for x in portfo_sheet]

# %%
outputs = ['PortfoShs' , 'SrchedPortf']
outpus_dict = m.make_output_dict(outputs)


# %%


# noinspection PyTypeChecker
class prq_sheet :


    def __init__(self , tracingno , sheet_names_in_dict_as_str) :
        self.tracingno = str(tracingno)
        self.sheetnames = eval(sheet_names_in_dict_as_str)

        self.dfs_ex = [x for x in self.sheetnames if
                       os.path.exists(f'{m.excels_as_prq_dir}/{self.tracingno}_{x}{m.prqsuf}')]

        self.dfs = {x : m.read_parquet(f'{m.excels_as_prq_dir}/{self.tracingno}_{x}{m.prqsuf}') for x in self.dfs_ex}

        self.outputs = outputs_dict.copy()
        self.outputs['PortfoShs'] = {}


    def find_portfo_sheet_from_shn(self) :
        for key , val in self.sheetnames.items() :
            if m.wos(val) in srch_sh_names :
                self.outputs['PortfoShs'][key] = val


    def find_portfo_sheet_from_srch_keys(self) :
        for key , val in self.dfs.items() :
            wos_sh_df = val.iloc[0 :10]
            wos_sh_df = wos_sh_df.applymap(del_s_dot_one_dash)
            wos_sh_df = wos_sh_df.applymap(m.wos)
            wos_df_check = wos_sh_df.isin(portfo_srch)
            if wos_df_check.any(axis = None) :
                self.outputs['PortfoShs'][key] = self.sheetnames[key]


    def process(self) :
        self.find_portfo_sheet_from_shn()
        self.find_portfo_sheet_from_srch_keys()
        self.outputs['SrchedPortf'] = True
        self.outputs['PortfoShs'] = str(self.outputs['PortfoShs'])
        return self.outputs


def target(tracingno , sheetnames_in_dict_as_str) :
    obj1 = prq_sheet(tracingno , sheetnames_in_dict_as_str)
    out1 = obj1.process()
    return list(out1.values())


def get_all_uniqs(pdser) :
    pdser = pdser[pdser.notna()]
    pdser = pdser.apply(eval)
    all_uniqs = []
    for el1 in pdser :
        for val in el1.values() :
            all_uniqs.append(val)
    all_uniqs = np.unique(all_uniqs)
    return all_uniqs


def remove_daramad(dictasstr) :
    dict1 = eval(dictasstr)
    keys_2_del = []
    for key , val in dict1.items() :
        if m.any_of_list_isin(daramad_srch , m.wos(val)) :
            keys_2_del.append(key)
    for el1 in keys_2_del :
        del dict1[el1]
    return str(dict1)


def main() :
    pass

    # %%
    df = m.read_parquet(pre_prq)
    df

    # %%
    df[outputs] = None
    df

    # %%
    df = m.update_with_last_data(df , cur_prq)

    # %%
    cond = df['XlSavedAsPrq'].eq(True)
    cond &= df['SrchedPortf'].isna()
    cond[cond]

    # %%
    flt = df[cond]
    flt

    # %%
    cores_n = cpu_count()
    print(f'Num of cores : {cores_n}')
    clstrsInd = m.return_clusters_indices(flt)
    pool = Pool(cores_n)

    # %%
    for i in range(0 , len(clstrsInd) - 1) :
        strtI = clstrsInd[i]
        endI = clstrsInd[i + 1]
        print(f"Cluster index of {strtI} to {endI}")

        corrInd = flt.iloc[strtI :endI].index

        tracingnos = df.loc[corrInd , 'TracingNo']
        sheetns = df.loc[corrInd , 'SheetNames']

        out = pool.starmap(target , zip(tracingnos , sheetns))

        for j , outpa in enumerate(outputs) :
            df.loc[corrInd , outpa] = [w[j] for w in out]

        print(out)
        # break

    # %%
    m.save_as_parquet(df , cur_prq)

    # %%
    get_all_uniqs(df['PortfoShs'])

    # %%
    cnd2 = df['SrchedPortf'].eq(True)
    df['PortfoShs'] = df.loc[cnd2 , 'PortfoShs'].apply(remove_daramad)

    # %%
    m.save_as_parquet(df , cur_prq)

    # %%


# %%

#
# if __name__ == '__main__' :
#     main()
#     print(f'{cur_n}.py done!')
#
# # noinspection PyUnreachableCode
# if False :
#     pass
#
#     # %%
#     obj = xl_wb(731449)
#
#     # %%
#     obj.xlpn
#     # %%
#     load_workbook(obj.xlpn , keep_vba = False , data_only = True , read_only = True , keep_links = False)
#
#     # %%
#     df[df['Err_Msg'].apply(lambda x : type(x) == type(TimeOutException))]
#     # %%
#     df['Err_Msg'] = df['Err_Msg'].astype(str)
#
# # %%
