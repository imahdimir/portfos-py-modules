"""[Image processing skipped due to its hardship
    - Combines cleaned sheets from excels and pdf that are textbased]
    """
##
import multiprocessing as mp
import pickle
import re
import warnings
import shutil
import cv2
import numpy as np
import pandas as pd
import pytesseract
from pytesseract import Output

from main import (d , simpl_ret_cache_fp)
from Py\._13 import (_is_a_meaningful_str , _is_not_stocks_portfo ,
                    _is_stocks_porto , )
from Py.Helpers import functions_and_dirs as fu
from Py.Helpers.name_space import cc , cte
from PIL import Image
from functools import partial

warnings.filterwarnings("ignore")

Module_name = '_15.py'

grand_data = pd.DataFrame()

def _read_xl_file(sheet_stem , the_dir) :
    fp = (the_dir / sheet_stem).with_suffix('.xlsx')
    xl = pd.read_excel(fp , engine='openpyxl')
    print(sheet_stem)
    return xl

def main() :
    global grand_data

    pass
    ##
    pre_mod_name = fu.get_pre_mod_name(d.code , Module_name)
    cur_mod_cache_fp = simpl_ret_cache_fp(Module_name)
    pre_mod_cache_fp = simpl_ret_cache_fp(pre_mod_name)
    new_cols = {}
    new_index_levels_names: list = []
    ##
    df = fu.load_update_df(pre_module_cache_fp=pre_mod_cache_fp ,
                           current_module_cache_fp=cur_mod_cache_fp ,
                           new_cols_2add_and_update=list(new_cols.keys()) ,
                           new_index_cols=new_index_levels_names)

    ##
    col2drop = {

            }
    df = df.drop(columns=col2drop.keys() , errors='ignore')

    ##
    def mask_suc_xls(idf) :
        _cnd = idf[cc.is_clean_1_suc].eq(True)

        print(len(_cnd[_cnd]))
        return _cnd

    msk = mask_suc_xls(df)

    ##
    def append_xlsheets(idf , mask) :
        global grand_data

        flt = idf[mask]
        print('len:' , len(flt))

        for ind , ro in flt.iterrows() :
            _df = _read_xl_file(ro[cc.sheetStem] , d.XlShtCln1)
            grand_data = grand_data.append(_df)

    ##
    append_xlsheets(df , msk)

    ##
    def mask_suc_pdfs(idf) :
        _cnd = idf[cc.PdfStPg_IsClnSuc].eq(True)

        print(len(_cnd[_cnd]))
        return _cnd

    msk = mask_suc_pdfs(df)

    ##
    def append_pdfsheets(idf , mask) :
        global grand_data

        flt = idf[mask]
        print('len:' , len(flt))

        for ind , ro in flt.iterrows() :
            _df = _read_xl_file(ro[cc.PdfStPg_Tbl_FStem] , d.PStPortfo2)
            grand_data = grand_data.append(_df)

    ##
    append_pdfsheets(df , msk)
    ##
    fu.save_df_as_a_nice_xl(grand_data , cte.grand1)

##
if __name__ == "__main__" :
    main()
    print(f'{Module_name} Done.')

def _test() :
    pass

    ##

    ##
