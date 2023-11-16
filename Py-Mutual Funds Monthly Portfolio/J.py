# %%


import pandas as pd
import main as m
import numpy as np
from multiprocessing import cpu_count
from multiprocess import Pool
from persiantools.jdatetime import JalaliDateTime


# %%


cur_n = 'J'
pre_n = 'I'

# %%


cur_prq = m.within_exe_data_dir + f'/{cur_n}' + m.prqsuf
pre_prq = m.within_exe_data_dir + f'/{pre_n}' + m.prqsuf

# %%

jdate = JalaliDateTime.today()
jmonthnow = jdate.year * 100 + jdate.month

# %%


outputs = ['DatecheckStatus' , 'HasError' , 'BnCorrectJMonthDict']

outputs_dict = {}
for el in outputs :
    outputs_dict[el] = None


# %%


# noinspection PyTypeChecker
class portfo_sheet :


    def __init__(self , changerowjdates: list , jmonthfromtitle: int) :
        self.changerowjdates = changerowjdates
        self.jmonthfromtitle = jmonthfromtitle

        self.output = outputs_dict.copy()


    def process(self) :
        if self.jmonthfromtitle > jmonthnow :
            self.output['DatecheckStatus'] = 'jmonthtitle>now'
            self.output['HasError'] = True
            return self.output
        # print(self.changerowjdates)
        if self.changerowjdates == [[]] or not self.changerowjdates :
            self.output['BnCorrectJMonthDict'] = self.jmonthfromtitle
            self.output['DatecheckStatus'] = 'no_founddate'
            return self.output
        dates = np.ravel(self.changerowjdates)
        jdate1 = np.max(dates)
        try :
            jmonthfound = jdate1 // 100
        except TypeError :
            self.output['DatecheckStatus'] = 'TypeError'
            self.output['HasError'] = True
            return self.output
        if jmonthfound > jmonthnow :
            self.output['DatecheckStatus'] = 'jmonthfound>now'
            self.output['HasError'] = True
            return self.output
        if jmonthfound == self.jmonthfromtitle :
            self.output['BnCorrectJMonthDict'] = self.jmonthfromtitle
            self.output['DatecheckStatus'] = 'found==jmonthtitle'
            return self.output
        if jmonthfound > self.jmonthfromtitle :
            self.output['BnCorrectJMonthDict'] = jmonthfound
            self.output['DatecheckStatus'] = 'jmonthfound>jmonthtitle'
            return self.output
        self.output['DatecheckStatus'] = 'jmonth > jfound'
        self.output['HasError'] = True
        return self.output


def target(changerowjdatedict: str , jmonthfromtitle: int) :
    jdatesdict = eval(changerowjdatedict)
    # print(jdatesdict)
    out2 = {}
    out3 = {}
    out4 = {}
    for key , val in jdatesdict.items() :
        obj1 = portfo_sheet(val , jmonthfromtitle)
        out = obj1.process()
        # print(out)
        out2[key] = out['DatecheckStatus']
        out3[key] = out['BnCorrectJMonthDict']
        out4[key] = out['HasError']

    out2str = str(out2)
    out3str = str(out3)
    out4str = str(out4)

    return out2str , out4str , out3str


def all_nan_dict_values(dictstr) :
    dct1 = eval(dictstr)
    vals = list(dct1.values())
    # print(vals)
    checknan = [x is None for x in vals]
    return np.all(checknan)


def main() :
    pass

    # %%


    df = m.read_parquet(pre_prq)
    df

    # %%
    cnd = df['JDateSearched'].eq(True)
    cnd[cnd]

    # %%
    flt = df[cnd]
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

        changedates = df.loc[corrInd , 'ChangeRowsJDates']
        jmonthfromtitle = df.loc[corrInd , 'JMonthFromTitle']

        out = pool.starmap(target , zip(changedates , jmonthfromtitle))

        for j , outp in enumerate(outputs) :
            df.loc[corrInd , outp] = [x[j] for x in out]

        print(df.loc[corrInd , outputs])
        # break

    # %%
    m.save_as_parquet(df , cur_prq)

    # %%
    cnd2 = df['BnCorrectJMonthDict'].notna()
    cnd2 &= ~ df.loc[cnd2 , 'HasError'].apply(lambda x : all_nan_dict_values(x))
    cnd2[cnd2]

    # %%
    flt2 = df[cnd2]
    flt2

    # %%


# %%


if __name__ == '__main__' :
    main()

# noinspection PyUnreachableCode
if False :
    pass

    # %%
    eval(flt.iloc[0]['ChangeRowsJDates'])

    # %%
    target(df.iloc[0]['ChangeRowsJDates'] , df.iloc[0]['JMonthFromTitle'])
