import pandas as pd
import main as m
from multiprocessing import cpu_count
import os
import numpy as np
from multiprocess import Pool


# %%
cur_n = 'H'
pre_n = 'G'

# %%
cur_prq = m.within_exe_data_dir + f'/{cur_n}' + m.prqsuf
pre_prq = m.within_exe_data_dir + f'/{pre_n}' + m.prqsuf

# %%
outputs = ['PortfoShsBnsList']
outputs_dict = m.make_output_dict(outputs)


# %%


# noinspection PyTypeChecker
class sheet_prq :


    def __init__(self , tracingno , potfosh_dict) :
        self.tracingno = tracingno
        self.potfosh_dict = potfosh_dict
        self.output = outputs_dict.copy()


    def process(self) :
        portfo_as_dict = eval(self.potfosh_dict)
        self.output['PortfoShsBnsList'] = []
        for shno in portfo_as_dict.keys() :
            fpn = m.excels_as_prq_dir + '/' + str(self.tracingno) + f'_{shno}' + m.prqsuf
            try :
                df1 = m.read_parquet(fpn)
                df1 = claen_df(df1)
                fsave = m.portfo_sheets_dir + '/' + str(self.tracingno) + f'_{shno}' + m.prqsuf
                m.save_as_parquet(df1 , fsave)
                self.output['PortfoShsBnsList'].append(str(self.tracingno) + f'_{shno}')
            except FileNotFoundError as e :
                print(e)

        self.output['PortfoShsBnsList'] = str(self.output['PortfoShsBnsList'])
        return self.output


def claen_df(indf: pd.DataFrame) :
    df1 = indf.dropna(how = 'all')
    df1 = df1.dropna(how = 'all' , axis = 1)
    df1 = df1.T.reset_index(drop = True).T.reset_index(drop = True)
    return df1


def target(tracingno , potfosh_dict) :
    obj = sheet_prq(tracingno , potfosh_dict)
    out = obj.process()
    return list(out.values())


def main() :
    pass

    # %%


    df = m.read_parquet(pre_prq)
    df

    # %%
    df['TracingNo'] = df['TracingNo'].astype(int)

    # %%
    df[outputs] = None

    # %%
    df = m.update_with_last_data(df , cur_prq)

    # %%
    cond = (df['PortfoSearched'].eq(True) & df['PortfoShs_InDictAsStr'].ne('{}'))
    cond &= df['PortfoShsBnsList'].isna()
    cond[cond]

    # %%
    flt = df[cond]
    flt

    # %%
    cores_n = cpu_count()
    print(f'Num of cores : {cores_n}')
    clusters = m.return_clusters_indices(flt)
    pool = Pool(cores_n)

    # %%
    for i in range(0 , len(clusters) - 1) :
        start_index_no = clusters[i]
        end_index_no = clusters[i + 1]
        print(f"Cluster index of {start_index_no} to {end_index_no}")

        corresponding_index_labels = flt.iloc[start_index_no :end_index_no].index

        tracingnos = df.loc[corresponding_index_labels , 'TracingNo']
        portfodicts = df.loc[corresponding_index_labels , 'PortfoShs_InDictAsStr']

        out = pool.starmap(target , zip(tracingnos , portfodicts))

        for j , outpa in enumerate(outputs) :
            df.loc[corresponding_index_labels , outpa] = [w[j] for w in out]

        print(df.loc[corresponding_index_labels , outputs])
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
