"""[summary]
    Find corruptXl xl files and fix ones that can be fixed by changing
    their suffixes.

    Returns:
        [type]: [description]
    """
##
import multiprocessing as mp
from functools import partial
from pathlib import PurePath

from py.ns import *
from py import cf as cf

pmL , cmL = cf.get_pre_mod_name('j' , )
cmPn = cf.ret_cache_pathname(cmL)
pmPn = cf.ret_cache_pathname(pmL)
neC = []

class HtmlStr :

    def __init__(self , fname) :
        self.fn = fname
        self.stem = None
        self.htmlstr = None
        self.dfs_list = None
        self.hdr = None
        self.vals = None
        self.names = None
        self.df = None

    def process(self) :
        funcs_order = [self._load_data , self._find_tables ,
                       self._build_the_table , self._save_sheet , ]
        for sfun in funcs_order :
            lo = sfun()
            if lo is not None :
                return lo

    def _load_data(self) :
        self.stem = PurePath(self.fn).stem
        pn = dirs.HasAttHtml / self.fn
        if pn.exists() :
            with open(pn , 'r' , encoding="utf-8") as file :
                self.htmlstr = file.read()
        else :
            return cte.noFileErr

    def _find_tables(self) :
        self.dfs_list = pd.read_html(self.htmlstr)

    def _build_the_table(self) :
        if len(self.dfs_list) == 1 :
            self._1df_case()
        elif len(self.dfs_list) == 3 :
            lo = self._3dfs_case()
            if lo is not None :
                return lo
            self._3dfs_case_patch_dfs()

    def _1df_case(self) :
        df = self.dfs_list[0]
        df = df.T.reset_index().T.reset_index(drop=True)
        self.df = df

    def _3dfs_case(self) :
        hdr = None
        dfs = self.dfs_list.copy()
        for i , df in enumerate(dfs) :
            if df.T.reset_index().T.shape == (2 , 14) :
                hdr = df.T.reset_index().T
                hdr = hdr.reset_index(drop=True)
                dfs.pop(i)
        if hdr is None :
            return htc.noHdrE
        vals = None
        for i , df in enumerate(dfs) :
            if df.shape[1] == 14 :
                vals = df.copy()
                vals = vals.reset_index(drop=True)
                dfs.pop(i)
        if vals is None :
            return htc.noValsE
        names = dfs[0]
        if names.shape[1] != 1 :
            return htc.namesColE
        if names.shape[0] != vals.shape[0] :
            return htc.noEqualRowsofNamesCols
        self.hdr , self.vals , self.names = hdr , vals , names

    def _3dfs_case_patch_dfs(self) :
        df = self.hdr.append(self.vals)
        df = df.reset_index(drop=True)
        df.columns = range(1 , df.shape[1] + 1)
        self.names.index = range(2 , len(self.names) + 2)
        self.names = self.names.join(df , how='outer')
        self.df = self.names.copy()

    def _save_sheet(self) :
        opn = dirs.htmlSheets / f'{self.stem}.pkl'
        self.df.to_pickle(opn)
        return opn.name

def _targ(html_fn: str) :
    xo = HtmlStr(html_fn)
    out = xo.process()
    print(out)
    return str(out)

def main() :
    pass
    ##
    df = cf.load_update_df(pre_module_cache_fp=pmPn ,
                           current_module_cache_fp=cmPn ,
                           new_cols_2add_and_update=neC)

    ##
    def _remove_exess_cleaned_sheets() :
        the_dir = dirs.htmlSheets
        sht_pns = list(the_dir.glob('*.pkl'))
        sht_ns = [x.name for x in sht_pns]
        sht_2del = set(sht_ns) - set(df[cdi.foundSh])
        for sht in sht_2del :
            (the_dir / sht).unlink()
            print(sht)
        print('Sheets to remove count:' , len(sht_2del))

    _remove_exess_cleaned_sheets()

    ##
    def _specify_main_cond() :
        c0 = df[cac.dataFmt].eq(fmt.html)
        c0 &= df[cac.HasAttHtmlDlState].eq(cte.suc)
        c0 &= df[cdi.foundSh].isna()
        print(len(c0[c0]))
        return c0

    mcn = _specify_main_cond()
    df2 = df[mcn]

    ##
    def _add_result_to_df(html_sheet_fn , index) :
        if '.pkl' in html_sheet_fn :
            df.at[index , cdi.foundSh] = html_sheet_fn
        else :
            df.at[index , cac.findngShtsProbs] = html_sheet_fn

    ##
    def _main_process(mask) :
        flt = df[mask]
        print("Items to Process count:" , len(flt))
        cpc = mp.cpu_count()
        pool = mp.Pool(cpc)

        for ind , ro in flt.iterrows() :
            ncb = partial(_add_result_to_df , index=ind)
            pool.apply_async(_targ , args=[ro[cac.HasAttHtmlFN]] , callback=ncb)
        pool.close()
        pool.join()
        cf.save_current_module_data(df , cmPn)

    _main_process(mask=mcn)
    df2 = df[mcn]

    ##
    def _find_unsuc_htmls() :
        c0 = df[cac.dataFmt].eq(fmt.html)
        c0 &= df[cac.HasAttHtmlDlState].eq(cte.suc)
        c0 &= df[cdi.foundSh].isna()
        return c0

    df1 = df[_find_unsuc_htmls()]
    print(len(df1))

    ##
    def _find_suc_htmls() :
        c0 = df[cac.dataFmt].eq(fmt.html)
        c0 &= df[cac.HasAttHtmlDlState].eq(cte.suc)
        c0 &= df[cdi.foundSh].str.contains('.pkl')
        print('count:' , len(c0[c0]))
        return c0

    df2 = df[_find_suc_htmls()]

##
if __name__ == "__main__" :
    main()
    print(f'{PurePath(__file__).name} Done.')

def _test() :
    pass
    ##
    tdf = pd.read_parquet(pmPn)

    ##
    o = HtmlStr('787142.html')
    odf = o.dfs_list
    odf1 = o.df
    df = o.hdr.append(o.vals)
    ##
    o.process()
    t = o.names
    print(t.columns)
    t1 = o.df
    print(t1)
    print(t1.columns)
    t4 = t1[0]
    t2 = t1[1]
    t3 = t1[2]
    print(t4)
    t5 = t4.T.reset_index().T
    ##
    tdf = pd.read_pickle(dirs.xlSheets / '777704.pkl')
    t5 = t4.T.reset_index().T.reset_index(drop=True)
    ##
    r1 = df.loc['783789']
    print(r1)
    print(r1.name)
    ##
    print(td1)
    md = td1.isna().all(axis=1)
    first_flase = md.ne(True).idxmax()
    td1 = td1.loc[first_flase :]
    ##
    to1 = NotCle
