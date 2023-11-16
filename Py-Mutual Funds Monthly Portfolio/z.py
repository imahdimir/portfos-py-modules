# %%


import pandas as pd
import main as m
from lxml import etree

from multiprocessing import cpu_count
from os.path import exists
import numpy as np
from multiprocess import Pool
from io import StringIO


cur_n = 'H'
pre_n = 'G'

cur_prq = m.within_exe_data_dir + cur_n + m.prqsuf
pre_prq = m.within_exe_data_dir + pre_n + m.prqsuf

parser = etree.HTMLParser()

outputs = ['ErrMsgHtml' , 'HtmlReadSucceed']

outputs_dict = {}
for el in outputs :
    outputs_dict[el] = None


# noinspection PyTypeChecker
class month_stocks_portfolio :


    def __init__(self , tracingno) :
        self.tracingno = tracingno
        self.htmlpn = m.htmlportfo_dir + str(tracingno) + '.html'

        self.rawhtml = None
        self.fixed_html = None
        self.error_msg = None
        self.output = outputs_dict.copy()
        self.df = None
        self.fixed_df = None
        self.output['HtmlReadSucceed'] = False


    def process_and_make_outputs(self) :
        with open(self.htmlpn , 'r') as htmlfile :
            self.rawhtml = htmlfile.read()

        self.fixed_html = fix_html(self.rawhtml)
        self.df = pd.read_html(self.fixed_html)[0]
        self.fixed_df = fix_table(self.df)

        m.save_as_parquet(self.fixed_df , m.excels_as_prq_dir + str(self.tracingno) + m.prqsuf)

        self.output['HtmlReadSucceed'] = True

        return self.output


def fix_table(indf) :
    indf = indf.dropna(how = "all")
    indf = indf.dropna(how = "all" , axis = 1)
    indf = indf.T.reset_index().T.reset_index()
    indf = indf.applymap(m.wos)
    indf = indf.applymap(lambda x : np.nan if len(x) >= 60 else x)
    indf = indf.reset_index(drop = True)
    return indf


# Removes hidden rows and cols from html code to pandas can read it correctly
def fix_html(html) :
    tree1 = etree.parse(StringIO(html) , parser)

    for element in tree1.xpath("//*[@hidden]") :
        element.set("rowspan" , "0")
        element.set("colspan" , "0")

    for element in tree1.xpath('//*[contains(@style, "display:none")]') :
        element.set("rowspan" , "0")
        element.set("colspan" , "0")

    fixedhtml = etree.tostring(tree1 , method = "html" , encoding = "unicode")

    return fixedhtml


# noinspection PyTypeChecker
def target(tracingno) :
    obj1 = month_stocks_portfolio(tracingno)
    out = obj1.process_and_make_outputs()

    return list(out.values())


def main() :
    pass

    # %%


    df = pd.read_parquet(pre_prq)
    df

    # %%
    cond = df['IsHtmlSheet4Downloaded'].eq(True)
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

        out = pool.map(target , tracingnos)

        for j , outpa in enumerate(outputs) :
            df.loc[corrInd , outpa] = [w[j] for w in out]

        print(df.loc[corrInd , outputs])

    # %%
    df.to_parquet(cur_prq , index = False)

    # %%
    cond2 = df['IsHtmlSheet4Downloaded'].eq(True)
    cond2 &= df['HtmlReadSucceed'].eq(False)
    df[cond2]


# %%


if __name__ == '__main__' :
    main()
    print(f'{cur_n}.py done!')

# noinspection PyUnreachableCode
if False :
    pass

    # %%

    # %%
