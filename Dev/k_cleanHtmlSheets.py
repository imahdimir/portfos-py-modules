"""[summary]
    Find corruptXl xl files and fix ones that can be fixed by changing
    their suffixes.

    Returns:
        [type]: [description]
    """
##
import multiprocessing as mp
import warnings
from functools import partial
from pathlib import PurePath

from py.ns import *
from py import cf as cf

warnings.filterwarnings('ignore')

pmL , cmL = cf.get_pre_mod_name('k' , )
cmPn = cf.ret_cache_pathname(cmL)
pmPn = cf.ret_cache_pathname(pmL)
neC = []
cpnA = True

class NotCleanedHtmlSheet :
    def __init__(self , fname , html_or_xl) :
        self.fn = str(fname)
        self.html_or_xl = html_or_xl
        self.orgdf = None
        self.df = None
        self.names_col = 0
        self.col_finders = HtmlColFinders().cfs_list
        self.found_cols = {}
        self.cols_map = {x : None for x in hcs.colNme_MuLvelTups_duo}
        self.cleaned_odf = None

    def clean(self) :
        funcs_order = [self._load_data , self._elementary_clean ,
                       self._check_min_rows_and_cols , self._find_change_cell ,
                       self._take_only_first_fifteen_cols ,
                       self._find_other_cols ,
                       self._build_standard_cleaned_sheet , self._save_odf , ]
        for sfun in funcs_order :
            lo = sfun()
            if lo is not None :
                return lo

    def _load_data(self) :
        if self.html_or_xl == fmt.xl :
            self.pn = dirs.xlSheets / self.fn
        else :
            self.pn = dirs.htmlSheets / self.fn
        if self.pn.exists() :
            self.orgdf = pd.read_pickle(self.pn)
            self.df = self.orgdf.copy()
        else :
            return cte.noFileErr

    def _elementary_clean(self) :
        self._norm_str_in_df()
        self._convert_nonestr_space_to_nan()
        self._remove_starting_nan_rows()
        self._remove_col_and_rows_with_only_zero_or_none()

    def _norm_str_in_df(self) :
        self.df = cf.norm_str_df(self.df)

    def _convert_nonestr_space_to_nan(self) :
        self.df = cf.convert_blank_cells_to_none(self.df)

    def _remove_starting_nan_rows(self) :
        self.df = cf.remove_starting_nan_rows(self.df)

    def _remove_col_and_rows_with_only_zero_or_none(self) :
        self.df = cf.remove_cols_and_rows_with_only_zero_or_none(self.df)

    def _check_min_rows_and_cols(self) :
        if not self.df.shape[1] >= 15 :
            return xsc.not_enough_col
        if not self.df.shape[0] >= 2 :
            return xsc.notEnoughRows

    def _find_change_cell(self) :
        foo = cf.find_change_cell(self.df)
        if foo is None :
            return xsc.chngCellErr
        else :
            self.df , _ , _ = foo

    def _take_only_first_fifteen_cols(self) :
        self.df = self.df.iloc[: , 0 :15]

    def _find_specific_cols_by_value(self , val , cols_count) :
        return cf.find_specific_cols_by_value(self.df , val , cols_count)

    def _find_other_cols(self) :
        for find_config in self.col_finders :
            for key , val in find_config.items() :
                self.found_cols[key] = self._find_specific_cols_by_value(
                        val[0][0] , val[0][1])  # print(self.found_cols[key])
            self._set_cols_map(find_config)
            lo = self._check_for_missing_cols()
            if lo is None :
                # print(self.cols_map)
                return None

    def _set_cols_map(self , find_config) :
        self.cols_map[xlhdr.asset] = self.names_col
        for key , val in find_config.items() :
            for i , coln in enumerate(val[1]) :
                self.cols_map[coln] = self.found_cols[key][i]

    def _check_for_missing_cols(self) :
        req_cols = self.cols_map.keys()
        for col in req_cols :
            if self.cols_map[col] is None :
                return f'{xsc.missingCol}-{col}'

    def _build_standard_cleaned_sheet(self) :
        self._remove_default_header()
        self.cleaned_odf = pd.DataFrame(columns=hcs.htmlMultiLevelHdr)
        for key , val in self.cols_map.items() :
            multi_lvl_col = hcs.colNme_MuLvelTups_duo[key]
            self.cleaned_odf[multi_lvl_col] = self.df[
                val] if val is not None else None

    def _remove_default_header(self) :
        tec = cf.find_axis_labels_by_any_of_list_is_in(self.df ,
                                                       [hcp.tedadSaham] ,
                                                       axis=0)
        maxhc = max(tec)
        maxhc_i = self.df.index.get_loc(maxhc)
        print(maxhc_i)
        self.df = self.df.iloc[maxhc_i + 1 :]

    def _save_odf(self) :
        if self.html_or_xl == fmt.xl :
            pn = dirs.xlCleanedSheets / self.fn
        else :
            pn = dirs.htmlCleanedSheets / self.fn
        self.cleaned_odf.to_pickle(pn)
        print("Saved as:" , self.fn)
        return xsc.Cleaned

def _targ(sht_pkl_fn: str ,
          sht_cln_state: str ,
          html_or_xl: str ,
          reclean=False) :
    if not (reclean or pd.isna(sht_cln_state)) :
        print('skip')
        return sht_cln_state
    elif str(sht_cln_state) == xsc.Cleaned :
        print('skip')
        return sht_cln_state
    ob = NotCleanedHtmlSheet(sht_pkl_fn , html_or_xl)
    ob_state = ob.clean()
    print(ob_state)
    return ob_state

def main() :
    pass
    ##
    df = cf.load_update_df(pre_module_cache_fp=pmPn ,
                           current_module_cache_fp=cmPn ,
                           new_cols_2add_and_update=neC)

    ##
    def _remove_exess_cleaned_sheets() :
        the_dir = dirs.htmlCleanedSheets
        sht_pns = list(the_dir.glob('*.pkl'))
        sht_ns = [x.name for x in sht_pns]
        c0 = df[cac.isSheetCleaned].eq(True)
        sht_2del = set(sht_ns) - set(df.loc[c0 , cdi.foundSh])
        for sht in sht_2del :
            (the_dir / sht).unlink()
            print(sht)
        print('Sheets to remove count:' , len(sht_2del))

    _remove_exess_cleaned_sheets()

    ##
    def specify_main_cond() :
        c0 = df[cdi.foundSh].notna()
        c0 &= df[cac.isSheetCleaned].ne(True) | df[cac.isSheetCleaned].isna()
        c0 |= df[cac.shClningStatus].eq(fmt.html)
        print('count:' , len(c0[c0]))
        return c0

    mcn = specify_main_cond()

    # df1 = df[mcn]

    ##
    def _add_result_to_df(sht_state , index) :
        df.at[index , cac.shClningStatus] = sht_state
        df.at[index , cac.isSheetCleaned] = (sht_state == xsc.Cleaned)

    ##
    def _clean_sheets(mask) :
        flt = df[mask]
        print("No. of items to clean: " , len(flt))
        cpc = mp.cpu_count()
        pool = mp.Pool(cpc)

        for ind , ro in flt.iterrows() :
            ncb = partial(_add_result_to_df , index=ind)
            pool.apply_async(func=_targ ,
                             args=(ro[cdi.foundSh] , ro[cac.shClningStatus] ,
                                   ro[cac.dataFmt] , cpnA) ,
                             callback=ncb , )
        pool.close()
        pool.join()
        cf.save_current_module_data(df , cmPn)

    _clean_sheets(mask=mcn)

    ##
    def _find_unsuc_html_ones() :
        cn = df[cdi.foundSh].notna()
        cn &= df[cac.dataFmt].eq(fmt.html)
        cn &= df[cac.isSheetCleaned].eq(False)
        print(len(cn[cn]))
        return cn

    df3 = df[_find_unsuc_html_ones()]
    print(len(df3))

##
if __name__ == "__main__" :
    main()
    print(f'{PurePath(__file__).name} Done.')

def _test() :
    pass  ##

    ##

    ##
