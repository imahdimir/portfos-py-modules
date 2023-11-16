"""[Finds Stocks Portfolio Sheets in Excel files]

    """

##


import warnings

import multiprocess as mp
import pandas as pd

from _1 import smpl_ret_cache_fp
from _2 import any_of_wordlist_isin_str
from _2 import get_pre_mod_name
from _2 import normalize_str
from _2 import save_current_module_cache
from _3 import load_update_df
from Helper.name_space import cc
from Helper.name_space import d

warnings.filterwarnings("ignore")

Module_name = '_7.py'

class FindStockSheetParams :
  def __init__(
      self
      ) :
    self.not_a_stocks_sheet_name = {
        'اوراق مشارکت' : None ,
        }

    self.not_in_a_sheet_name = {
        'جلد'    : None ,
        'سربرگ'  : None ,
        'اغاز'   : None ,
        'تنظیم'  : None ,
        'امضا'   : None ,
        'روکش'   : None ,
        'بانک'   : None ,
        'کارمزد' : None ,
        'مالیات' : None ,
        'جمع'    : None ,
        'لوگو'   : None ,
        'سایر'   : None ,
        'تنزیل'  : None ,
        'کد'     : None ,
        'حاصل'   : None ,
        'درامد'  : None ,
        'تعدیل'  : None ,
        'سود'    : None ,
        'زیان'   : None ,
        }

    self.in_a_sheet_names = {
        'سهام'   : None ,
        'مشارکت' : None ,
        'تبعی'   : None ,
        }

    self.not_in_sheet_vals = {
        'درامد'                      : None ,
        'سپرده'                      : None ,
        'سود'                        : None ,
        'زیان'                       : None ,
        'اطلاعات اماری'              : None ,
        'اوراق بهادار با درآمد ثابت' : None ,
        'اوراق مشارکت با درآمد ثابت' : None ,
        }

    self.in_sheet_vals = {
        'سرمایه گذاری در سهام' : None ,
        'صورت وضعیت پورتفوی'   : None ,
        }

    _func = normalize_str
    self.nassn = [_func(x) for x in self.not_a_stocks_sheet_name.keys()]
    self.nia = [_func(x) for x in self.not_in_a_sheet_name.keys()]
    self.nms = [_func(x) for x in self.in_a_sheet_names.keys()]
    self.nis = [_func(x) for x in self.not_in_sheet_vals.keys()]
    self.kvi = [_func(x) for x in self.in_sheet_vals.keys()]

fss = FindStockSheetParams()

def is_a_stock_sheet_by_name(
    sheet_name
    ) :
  if sheet_name in fss.nassn :
    return False
  elif any_of_wordlist_isin_str(sheet_name , fss.nia) :
    return False
  elif any_of_wordlist_isin_str(sheet_name , fss.nms) :
    return True

def any_of_wordlist_isin_df(
    idf ,
    wordlist: list
    ) :
  _df = idf.applymap(lambda
                       x : any_of_wordlist_isin_str(str(x) , wordlist))
  return _df.any(axis = None)

def is_stock_sheet_by_df_values(
    idf
    ) :
  if any_of_wordlist_isin_df(idf , fss.nis) :
    return False
  elif any_of_wordlist_isin_df(idf , fss.kvi) :
    return True

def _targ(
    sheet_name ,
    sheet_stem
    ) :
  if sheet_name != '' :
    out = is_a_stock_sheet_by_name(sheet_name)
    if out is not None :
      return out

  fp = (d.xl_Sheets / sheet_stem).with_suffix('.xlsx')
  _df = pd.read_excel(fp , engine = 'openpyxl')
  _df = _df.applymap(normalize_str)
  return is_stock_sheet_by_df_values(_df)

def main() :
  pass

  ##

  pre_mod_name = get_pre_mod_name(d.code , Module_name)

  ##
  cur_mod_cache_fp = smpl_ret_cache_fp(Module_name)
  pre_mod_cache_fp = smpl_ret_cache_fp(pre_mod_name)

  ##
  new_cols = {
      cc.modified_sheet_name : None ,
      cc.is_stocks_sheet     : None ,
      }

  ##
  df = load_update_df(pre_module_cache_fp = pre_mod_cache_fp ,
                      current_module_cache_fp = cur_mod_cache_fp ,
                      new_cols_2add_and_update = list(new_cols.keys()))

  ##
  drop_cols = {
      cc.not_hidden_sheets_cou : None ,
      cc.sheet_count           : None ,
      }

  df = df.drop(columns = drop_cols , errors = 'ignore')

  ##
  def normalize_excel_sheet_name(
      idf
      ) :
    _targ = cc.modified_sheet_name

    _cnd = idf[cc.sheet_name].notna()
    _col = idf.loc[_cnd , cc.sheet_name]
    idf.loc[_cnd , _targ] = _col.apply(normalize_str)

    _col_1 = idf.loc[_cnd , _targ]
    idf.loc[_cnd , _targ] = _col_1.str.replace(r'\d' , '' , regex = True)
    _col_1 = idf.loc[_cnd , _targ]
    idf.loc[_cnd , _targ] = _col_1.str.replace(r'-' , '' , regex = True)
    return idf

  df = normalize_excel_sheet_name(df)

  ##
  def mask_xl_sheets(
      idf
      ) :
    _cnd = idf[cc.sheet_name].notna()
    _cnd &= idf[cc.is_empty].ne(True)
    _cnd &= idf[cc.is_stocks_sheet].isna()

    print('len:' , len(_cnd[_cnd]))
    return _cnd

  msk = mask_xl_sheets(df)

  ##
  def process_all_sheets(
      idf ,
      imcn
      ) :
    flt = idf[imcn]
    print("count:" , len(flt))

    cpc = mp.cpu_count()
    pool = mp.Pool(cpc)

    _args = zip(flt[cc.modified_sheet_name] , flt[cc.sheetStem])
    _res = pool.starmap(_targ , _args)

    return _res

  res = process_all_sheets(df , msk)

  ##
  def add2df_all(
      idf ,
      imsk ,
      ires
      ) :
    _wanted_indices = imsk[imsk].index
    idf.loc[_wanted_indices , cc.is_stocks_sheet] = ires
    return idf

  df = add2df_all(df , msk , res)

  ##
  def get_stats(
      idf
      ) :
    _cnd = idf[cc.is_stocks_sheet].notna()
    print(len(_cnd[_cnd]))
    _cnd = idf[cc.is_stocks_sheet].eq(True)
    print(len(_cnd[_cnd]))

  get_stats(df)

  ##
  save_current_module_cache(df , cur_mod_cache_fp)

##


if __name__ == "__main__" :
  main()
  print(f'{Module_name} Done.')

def _test() :
  pass

  ##
