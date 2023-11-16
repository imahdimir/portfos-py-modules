# %%


import pandas as pd
import numpy as np
import main as m
from multiprocessing import cpu_count
from multiprocess import Pool
import os


# %%


cur_n = 'L'
pre_n = 'K'

# %%


cur_prq = m.within_exe_data_dir + f'/{cur_n}' + m.prqsuf
pre_prq = m.within_exe_data_dir + f'/{pre_n}' + m.prqsuf
rawportfoliodata_prq = m.within_exe_data_dir + '/raw_portfolio_data' + m.prqsuf

# %%
dataset_cols = ['FundSymbol' , 'StartJMonth' , 'EndJMonth' , 'Asset' , 'Start_SharesNo' , 'Start_CoSP' , 'Start_NSV' ,
                'InPeriod_Buy_SharesNo' , 'InPeriod_Buy_CoSP' , 'InPeriod_Sale_SharesNo' , 'InPeriod_Sale_Rev' ,
                'End_SharesNo' , 'End_MktPrice' , 'End_CoSP' , 'End_NSV' , 'End_PctOfAssets']

# %%


output_cols = ['IsDataEx']
output_cols_dict = m.make_output_dict(output_cols)


# %%

class cleaned_sheet :


    def __init__(self , symbol: str , prqbn: str , start_end: tuple) :
        self.symbol = symbol
        self.prqbn = prqbn
        self.prqpn = m.cleaned_sheets_dir + '/' + prqbn + m.prqsuf
        self.df = m.read_parquet(self.prqpn)
        self.start_jmonth = start_end[0]
        self.end_jmonth = start_end[1]
        self.data = None
        self.output = output_cols_dict.copy()


    def process(self) :
        symb_date_jdate = [self.symbol , self.start_jmonth , self.end_jmonth]
        df_sym_date_jdate = pd.DataFrame(np.tile(symb_date_jdate , (self.df.shape[0] , 1)))
        self.data = pd.concat([df_sym_date_jdate , self.df] , axis = 1 , ignore_index = True)
        self.data = self.data.rename(columns = lambda x : dataset_cols[x])
        self.data = self.data.drop([0 , 1 , 2])
        self.output['IsDataEx'] = True
        return self.output , self.data


def target(symbol: str , matched_hdr_dict: str , start_end_dict: str) :
    matched_hdr = eval(matched_hdr_dict)
    start_end_dict = eval(start_end_dict)
    o1 = {}
    o2 = {}

    for key , val in matched_hdr.items() :
        if val :
            obj = cleaned_sheet(symbol , key , start_end_dict[key])
            out = obj.process()
            o1[key] = out[0]['IsDataEx']
            o2[key] = out[1]

    return str(o1) , o2


def main() :
    pass

    # %%


    df = m.read_parquet(pre_prq)
    df

    # %%
    df[output_cols] = None

    # %%
    df = m.update_with_last_data(df , cur_prq)

    # %%
    cnd = df['MathcedhHdr'].notna()
    cnd &= df['MathcedhHdr'].ne('{}')
    cnd &= df[output_cols[0]].isna()
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
    inputs = ['Symbol' , 'MathcedhHdr' , 'StartEndJMonth']

    # %%
    new_data = pd.DataFrame(columns = dataset_cols)
    new_data

    # %%
    for i in range(0 , len(clusters) - 1) :
        strtI = clusters[i]
        endI = clusters[i + 1]
        print(f"Cluster index of {strtI} to {endI}")

        indices = flt.iloc[strtI :endI].index

        inpa = []
        for el in inputs :
            inpa.append(df.loc[indices , el])

        inpzip = zip(*inpa)

        out = pool.starmap(target , inpzip)

        for j , outp in enumerate(output_cols) :
            df.loc[indices , outp] = [w[j] for w in out]

        for w in out :
            for k , v in w[len(output_cols)].items() :
                new_data = new_data.append(v)

        print(df.loc[indices , output_cols])
        # break

    # %%
    if os.path.exists(rawportfoliodata_prq) :
        rawpdata = m.read_parquet(rawportfoliodata_prq)
        new_data = new_data.append(rawpdata)

    # %%
    new_data = new_data.drop_duplicates()
    new_data

    # %%
    m.save_as_parquet(new_data , rawportfoliodata_prq)

    # %%
    m.save_as_parquet(df , cur_prq)
    df


    # %%


if __name__ == '__main__' :
    main()

# noinspection PyUnreachableCode
if False :
    pass

    # %%
