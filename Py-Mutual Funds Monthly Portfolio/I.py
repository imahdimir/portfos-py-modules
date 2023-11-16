

import pandas as pd
import numpy as np
import main as m
from multiprocessing import cpu_count
from multiprocess import Pool


# %%

cur_n = 'I'
pre_n = 'H'

# %%


cur_prq = m.within_exe_data_dir + f'/{cur_n}' + m.prqsuf
pre_prq = m.within_exe_data_dir + f'/{pre_n}' + m.prqsuf

# %%

chng_in_per_col = ['تغییرات' , 'تغییرات طی دوره']

# %%


outputs = ['ChangeLocs' , 'ChangeRowsJDates' , 'JDateSearched']

outputs_dict = {}
for el in outputs :
    outputs_dict[el] = None


# %%


# noinspection PyTypeChecker
class sheet :


    def __init__(self , portfo_shs_bns_list_as_str) :
        self.portfo_shs = eval(portfo_shs_bns_list_as_str)

        self.outputs = outputs_dict.copy()

        self.dfs = {x : pd.read_parquet(m.portfo_sheets_dir + '/' + x + m.prqsuf) for x in self.portfo_shs}

        self.outputs['ChangeLocs'] = {key : find_change_locs(val) for key , val in self.dfs.items()}
        self.outputs['ChangeRowsJDates'] = {key : extract_jmonth_from_change_rows_on_left(self.dfs[key] , val) for
                                            key , val in self.outputs['ChangeLocs'].items()}


    def process(self) :
        for el1 in outputs :
            self.outputs[el1] = str(self.outputs[el1])
        self.outputs['JDateSearched'] = True
        return self.outputs


def find_change_locs(indf) :
    df1 = indf.applymap(lambda x : m.any_of_list_isin(chng_in_per_col , x))
    locs = m.find_all_locs_eq_val(df1 , True)
    return list(locs)


def extract_jmonth_from_change_rows_on_left(indf , change_locs) :
    # print(change_locs)
    df_chage_rows = [indf.iloc[x[0] , int(x[1]) :] for x in change_locs]
    rows_date = [x.apply(m.find_date2) for x in df_chage_rows]
    rows_date = [x[x.ne(-1)].tolist() for x in rows_date]
    return rows_date


def target1(sheets_bns_list_as_str) :
    obj1 = sheet(sheets_bns_list_as_str)
    out = obj1.process()
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
    cnd1 = df['PortfoShsBnsList'].notna()
    cnd1 &= df['JDateSearched'].isna()
    cnd1[cnd1]

    # %%
    flt = df[cnd1]
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

        shs_list = df.loc[corrInd , 'PortfoShsBnsList']

        out = pool.map(target1 , shs_list)

        for j , outp in enumerate(outputs) :
            df.loc[corrInd , outp] = [x[j] for x in out]

        print(df.loc[corrInd , outputs])
        # break

    # %%
    m.save_as_parquet(df , cur_prq)


# %%


if __name__ == '__main__' :
    main()
    print(f'{cur_n}.py done!')

# noinspection PyUnreachableCode
if False :
    pass

    # %%

    # %%
