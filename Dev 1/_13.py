"""[cleans as level 0 of text based pdf stock portfos]
    """
##
import multiprocessing as mp
import warnings
from pathlib import PurePath

import pandas as pd

from main import (d , simpl_ret_cache_fp)
from Py\._9 import (convert_blank_cells_to_none , convert_str_based_zeros ,
                   remove_cols_and_rows_with_only_zero_or_none ,
                   remove_first_one_unique_value_cols ,
                   remove_first_sparse_rows_with_condition ,
                   remove_sparse_cols_with_condition ,
                   remove_without_header_cols_except_names , )
from Py.Helpers import functions_and_dirs as fu
from Py.Helpers.name_space import cc , indi

warnings.filterwarnings("ignore")

Module_name = '_13.py'

class PdfPgStocksPortfoExcel :
    def __init__(self , sheet_stem) :
        self.stem = str(sheet_stem)
        self.exception = None
        self.df = None
        self.orgdf = None

    def _print(self , *args) :
        print(self.stem , ': ' , *args)

    def _load_data(self) :
        fp0 = (d.PStPortfo0 / self.stem).with_suffix('.xlsx')
        self.df = pd.read_excel(fp0 , engine='openpyxl')

    def _reverse_cols_order(self) :
        self.df = self.df.iloc[: , : :-1]
        self.orgdf = self.df.copy()

    def _elementary_clean(self) :
        self.df = self.df.T.reset_index().T
        self.df = self.df.applymap(fu.normalize_str)
        self.df = empty_unnamed_cells(self.df)
        self.df = convert_str_based_zeros(self.df)
        self.df = convert_blank_cells_to_none(self.df)
        self.df = remove_cols_and_rows_with_only_zero_or_none(self.df)
        self.df = remove_sparse_cols_with_condition(self.df)
        self.df = remove_first_sparse_rows_with_condition(self.df)
        self.df = remove_sparse_cols_with_condition(self.df)
        self.df = remove_cols_and_rows_with_only_zero_or_none(self.df)
        self.df = remove_first_one_unique_value_cols(self.df)
        self.df = remove_without_header_cols_except_names(self.df)

    def _save_odf(self) :
        self.df = self.df.T.reset_index(drop=True).T
        self.df = self.df.reset_index(drop=True)
        fp = d.PStPortfo1 / f'{self.stem}.xlsx'
        fu.save_df_as_a_nice_xl(self.df , fp)
        print("Saved as:" , PurePath(fp).name)

    def process(self) :
        try :
            self._load_data()
            self._reverse_cols_order()
            self._elementary_clean()
            self._save_odf()
        except Exception as e :
            self._print(e)
            self.exception = str(e)

    def get_attrs(self) :
        out = {
                cc.PdfStPg_Exc        : self.exception ,
                cc.PdfStPg_BeforeRows : self.orgdf.shape[0] ,
                cc.PdfStPg_BeforeCols : self.orgdf.shape[1] ,
                cc.PdfStPg_AfterRows  : self.df.shape[0] ,
                cc.PdfStPg_AfterCols  : self.df.shape[1] ,
                cc.PdfStPg_IsClnSuc   : self.exception is None ,
                }
        return out

def empty_unnamed_cells(idf) :
    for col in idf.columns :
        idf[col] = idf[col].astype(str).str.replace(r'^Unnamed \d+$' , '')
    return idf

def _targ_1(xl_stem) :
    print(xl_stem)

    oj = PdfPgStocksPortfoExcel(xl_stem)
    oj.process()
    out = oj.get_attrs()

    print(out)
    return out

def main() :
    pass
    ##
    pre_mod_name = fu.get_pre_mod_name(d.code , Module_name)
    cur_mod_cache_fp = simpl_ret_cache_fp(Module_name)
    pre_mod_cache_fp = simpl_ret_cache_fp(pre_mod_name)
    new_cols: dict = {
            cc.PdfStPg_TblNum     : None ,
            cc.PdfStPg_Exc        : None ,
            cc.PdfStPg_BeforeRows : None ,
            cc.PdfStPg_BeforeCols : None ,
            cc.PdfStPg_AfterRows  : None ,
            cc.PdfStPg_AfterCols  : None ,
            cc.PdfStPg_IsClnSuc   : None ,
            }
    new_index_levels_names: list = [indi.L5_PdfStocksTblNum]
    ##
    df = fu.load_update_df(pre_module_cache_fp=pre_mod_cache_fp ,
                           current_module_cache_fp=cur_mod_cache_fp ,
                           new_cols_2add_and_update=list(new_cols.keys()) ,
                           new_index_cols=new_index_levels_names)
    ##
    col2drop = {
            cc.PdfPg_IsTxtBased      : None ,
            cc.PdfPg_IsStockPortfo_0 : None ,
            cc.PdfPg_Is1stPgAfStock  : None ,
            }
    df = df.drop(columns=col2drop.keys() , errors='ignore')

    ##
    def extend_df_on_table_count(idf) :
        print('len:' , len(idf))
        _mask = idf[cc.PdfPg_TblCou].notna()

        _cou = idf.groupby(level=idf.index.in_a_sheet_names[:-1])[
            cc.PdfStPg_TblNum].transform('count')

        _mask &= _cou.eq(0)

        _df = idf[_mask]
        for _ind , ro in _df.iterrows() :
            for _k in range(int(ro[cc.PdfPg_TblCou])) :
                _ro_1 = ro.to_frame().T

                _ro_1.index.in_a_sheet_names = idf.index.in_a_sheet_names
                _ro_1[cc.PdfStPg_TblNum] = str(_k)
                _ro_1.index = _ro_1.index.set_levels([str(_k)] ,
                                                     level=indi.L5_PdfStocksTblNum)
                idf = idf.append(_ro_1)
        print('len:' , len(idf))
        return idf

    ##
    df = extend_df_on_table_count(df)

    ##
    def remove_excess_rows(idf) :
        print('len:' , len(idf))
        _cou = idf.groupby(level=idf.index.in_a_sheet_names[:-1])[
            cc.PdfStPg_TblNum].transform('count')
        _mask = _cou.ge(1)
        _mask &= idf[cc.PdfStPg_TblNum].isna()

        idf = idf[~_mask]
        print(len(idf))
        return idf

    df = remove_excess_rows(df)

    ##
    def build_pdfstock_pg_table_stem(idf) :
        _msk = idf[cc.PdfStPg_TblNum].notna()

        _df = idf[_msk][[cc.page_shot_stem , cc.PdfStPg_TblNum]]
        _df = _df.applymap(str)

        idf.loc[_msk , cc.PdfStPg_Tbl_FStem] = _df.agg('-'.join , axis=1)
        return idf

    df = build_pdfstock_pg_table_stem(df)

    ##
    def mask_pdfs(idf) :
        _msk = idf[cc.PdfStPg_TblNum].notna()

        print(len(_msk[_msk]))
        return _msk

    msk = mask_pdfs(df)

    ##
    def cln0(idf , mask) :
        flt = idf[mask]
        print('len:' , len(flt))
        cpc = mp.cpu_count()
        pool = mp.Pool(cpc)
        lo = pool.starmap_async(_targ_1 ,
                                zip(flt[cc.PdfStPg_Tbl_FStem])).get()
        for key in lo[0].keys() :
            idf.loc[mask , key] = [x[key] for x in lo]
        return idf

    ##
    df = cln0(df , msk)
    ##
    fu.save_current_module_cache(df , cur_mod_cache_fp)

##
if __name__ == "__main__" :
    main()
    print(f'{Module_name} Done.')

def _test() :
    pass

    ##

    ##
