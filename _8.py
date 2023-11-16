"""[Cleans Excel sheets of Portfolio of common_stocks. Level 0]
    """

##

import itertools
import warnings
from pathlib import PurePath

import multiprocess as mp
import numpy as np
import pandas as pd

from _0 import get_all_fps_wt_suffix
from _2 import normalize_str
from _6 import save_df_as_a_nice_xl
from _1 import smpl_ret_cache_fp
from _2 import any_of_wordlist_isin_str
from _2 import find_jdate_as_int_using_re
from _2 import get_pre_mod_name
from _2 import save_current_module_cache
from _3 import load_update_df
from _7 import fss
from Helper.name_space import cc
from Helper.name_space import d

warnings.filterwarnings("ignore")

Module_name = '_8.py'

class SheetCleanErrors0 :
  def __init__(
      self
      ) :
    self.change_cell_er = None
    for key , val in self.__dict__.items() :
      if val is None :
        self.__dict__[key] = key

SCE = SheetCleanErrors0()

class SheetCleaningParams0 :
  def __init__(
      self
      ) :
    self.month = ['ماه منتهی']
    self.change = ['تغییرات' , 'طی دوره']
    self.firms = ['شرکت' , 'نام']
    self.kharid = ['خرید']
    self.forosh = ['فروش']
    self.tedad = ['تعداد']
    self.baha = ['بها' , 'بهای']
    self.khales = ['خالص']
    self.mablagh = ['مبلغ']
    self.gheymat = ['قیمت']
    self.darsad = ['درصد']

scp = SheetCleaningParams0()

class ExcelPossibleStocksPortfoSheet :
  def __init__(
      self ,
      sheet_stem ,
      end_jmonth
      ) :
    self.stem = str(sheet_stem)
    self.end_jmonth = int(end_jmonth)

    self.exception = None

    self.df = None
    self.orgdf = None

    self.is_end_jmonth_ok = False

    self.change_cells_xy = None

    self.min_chng_ro = None
    self.min_chng_col = None
    self.max_chng_col = None

    self.start_jmonth_from_sheet = None
    self.end_jmonth_from_title = None
    self.end_jmonth_from_sheet = None

    self.is_cleaned = False

  def _print(
      self ,
      *args
      ) :
    print(self.stem , ': ' , *args)

  def load(
      self
      ) :
    fp0 = (d.man_clnd_sheets / self.stem).with_suffix('.xlsx')
    if fp0.exists() :
      self.df = pd.read_excel(fp0 , engine = 'openpyxl')
    else :
      fp1 = (d.xl_Sheets / self.stem).with_suffix('.xlsx')
      self.df = pd.read_excel(fp1 , engine = 'openpyxl')
    self.orgdf = self.df.copy()

  def elementary_clean(
      self
      ) :
    self.df = self.df.T.reset_index(drop = True).T
    self.df = self.df.applymap(normalize_str)
    self.df = convert_str_based_zeros(self.df)
    self.df = convert_blank_cells_to_none(self.df)
    self.df = remove_cols_and_rows_with_only_zero_or_none(self.df)
    self.df = remove_sparse_cols_with_condition(self.df)
    self.df = remove_first_sparse_rows_with_condition(self.df)
    self.df = remove_sparse_cols_with_condition(self.df)
    self.df = remove_cols_and_rows_with_only_zero_or_none(self.df)
    self.df = remove_unique_value_cols(self.df)
    self.df = remove_without_header_cols_except_names(self.df)

  def find_change_cell(
      self
      ) :
    _func = lambda \
        x : any_of_wordlist_isin_str(x , scp.change)
    df0 = self.df.applymap(_func)
    df1 = df0.stack(dropna = True)
    cells = df1[df1].index
    if len(cells) == 0 :
      raise Exception(SCE.change_cell_er)
    else :
      self.change_cells_xy = cells

  def find_change_cell_min_and_max_col(
      self
      ) :
    self.min_chng_ro = min(self.change_cells_xy.get_level_values(0))

    self.min_chng_col = min(self.change_cells_xy.get_level_values(1))
    self.max_chng_col = max(self.change_cells_xy.get_level_values(1))

  def find_start_jmonth(
      self
      ) :
    ro_cut = self.df.loc[self.min_chng_ro , :self.min_chng_col].copy()
    sjd = ro_cut.apply(lambda
                         x : find_jmonth_as_int_using_re(x))
    sjl = sjd.to_numpy().flatten()
    sjl = [x for x in sjl if pd.notna(x)]
    self.start_jmonth_from_sheet = max(sjl) if len(sjl) != 0 else None

  def find_end_jmonth(
      self
      ) :
    ro_cut = self.df.loc[self.min_chng_ro , self.max_chng_col :].copy()
    ejd = ro_cut.apply(lambda
                         x : find_jmonth_as_int_using_re(x))
    ejl = ejd.to_numpy().flatten()
    ejl = [x for x in ejl if pd.notna(x)]
    self.end_jmonth_from_sheet = max(ejl) if len(ejl) != 0 else None

  def check_month_from_title(
      self
      ) :
    _func = lambda \
        x : any_of_wordlist_isin_str(x , scp.month)
    mdf = self.df.applymap(_func)
    mdf = mdf.stack(dropna = True)
    mi = mdf[mdf].index
    if len(mi) == 1 :
      row = mi[0][0]
      col = mi[0][1]
      the_cell = self.df.at[row , col]
      self.end_jmonth_from_title = find_jmonth_as_int_using_re(the_cell)

  def check_months(
      self
      ) :
    self.check_month_from_title()
    self.find_start_jmonth()
    self.find_end_jmonth()

    if self.end_jmonth == self.end_jmonth_from_title or self.end_jmonth == self.end_jmonth_from_sheet :
      self.is_end_jmonth_ok = True

  def save(
      self
      ) :
    self.df = self.df.T.reset_index(drop = True).T
    self.df = self.df.reset_index(drop = True)

    fp = d.xl_cln_sheet_0 / f'{self.stem}.xlsx'

    save_df_as_a_nice_xl(self.df , fp , header = False)

    print("Saved as:" , PurePath(fp).name)
    self.is_cleaned = True

  def process(
      self
      ) :
    try :
      self.load()
      self.elementary_clean()
      self.find_change_cell()
      self.find_change_cell_min_and_max_col()
      self.check_months()
      self.save()
    except Exception as e :
      self._print(e)
      self.exception = str(e)

  def get_attrs(
      self
      ) :
    out = {
        cc.sheet_clean_0_exception   : self.exception ,
        cc.start_jm_from_sheet       : self.start_jmonth_from_sheet if self.start_jmonth_from_sheet is not None else None ,
        cc.end_jm_from_sheet         : self.end_jmonth_from_sheet ,
        cc.is_end_jm_ok              : self.is_end_jmonth_ok ,
        cc.before_clean_0_rows_count : self.orgdf.shape[0] ,
        cc.before_clean_0_cols_cou   : self.orgdf.shape[1] ,
        cc.after_clean_0_rows_cou    : self.df.shape[0] ,
        cc.after_clean_0_cols_cou    : self.df.shape[1] ,
        cc.is_clean_sheet_0_suc      : self.is_cleaned ,
        }
    return out

def convert_blank_cells_to_none(
    idf
    ) :
  vs = ['None' , 'nan' , '-']
  idf = idf.replace({x : None for x in vs})
  return idf

def convert_str_based_zeros(
    idf
    ) :
  for coln in idf.columns :
    idf[coln] = idf[coln].str.replace(r'^0\.0*$' , '0')
  return idf

def remove_cols_and_rows_with_only_zero_or_none(
    idf
    ) :
  md = idf.isna() | idf.eq(0) | idf.eq('0') | idf.eq('')
  cm = md.all()
  idf = idf[cm[~cm].index]
  rm = md.all(axis = 1)
  idf = idf[~rm]
  return idf

def remove_sparse_cols_with_condition(
    idf ,
    min_notna_vals=3
    ) :
  cols_notna_cou = idf.count()

  psbl_cols_2del = cols_notna_cou.lt(min_notna_vals)
  psbl_cols_2del = psbl_cols_2del[psbl_cols_2del]

  vals_not2_include = {
      0  : scp.month ,
      1  : scp.change ,
      2  : scp.firms ,
      3  : scp.kharid ,
      4  : scp.forosh ,
      5  : scp.tedad ,
      6  : scp.baha ,
      7  : scp.mablagh ,
      8  : scp.gheymat ,
      9  : scp.darsad ,
      10 : list(fss.in_sheet_vals.keys()) ,
      }
  _the_list = list(itertools.chain.from_iterable(list(vals_not2_include.values())))

  _2del = []
  for coln in psbl_cols_2del.index :
    _func = lambda \
        x : any_of_wordlist_isin_str(str(x) , _the_list)
    if not idf[coln].apply(_func).any() :
      _2del.append(coln)

  idf = idf.drop(columns = _2del)

  return idf

def remove_first_sparse_rows_with_condition(
    idf ,
    min_notna_vals=3
    ) :
  each_row_notna_cou = idf.count(axis = 1)

  psbl_ros_2del = each_row_notna_cou.lt(min_notna_vals)
  psbl_ros_2del = psbl_ros_2del[psbl_ros_2del]

  vals_not2_include = {
      0 : scp.change ,
      1 : scp.kharid ,
      2 : scp.forosh ,
      3 : scp.tedad ,
      4 : scp.baha ,
      5 : scp.mablagh ,
      6 : scp.gheymat ,
      7 : scp.darsad ,
      }
  _the_list = list(itertools.chain.from_iterable(list(vals_not2_include.values())))
  _2del = []
  for inde in psbl_ros_2del.index :
    _func = lambda \
        x : any_of_wordlist_isin_str(str(x) , _the_list)
    if not idf.loc[inde].apply(_func).any() :
      _2del.append(inde)
    else :
      break
  idf = idf.drop(index = _2del)
  return idf

def remove_unique_value_cols(
    idf
    ) :
  uniq_cou = idf.nunique()

  for coln in uniq_cou.index :
    if uniq_cou[coln] == 1 :
      if idf[coln].notna().all() :
        idf = idf.drop(columns = coln)

  return idf

def remove_without_header_cols_except_names(
    idf
    ) :
  hdr = idf.iloc[:3 , :]

  unq_cou = hdr.nunique()
  _cnd = unq_cou == 0
  lbls = unq_cou[_cnd].index

  lbls = set(lbls) - {0}

  idf = idf.drop(columns = lbls)
  return idf

def find_jmonth_as_int_using_re(
    ist
    ) :
  jd = find_jdate_as_int_using_re(ist)
  if jd :
    return jd // 100

def _targ(
    sheet_stem ,
    end_jmonth
    ) :
  print(sheet_stem)

  oj = ExcelPossibleStocksPortfoSheet(sheet_stem , end_jmonth)
  oj.process()
  out = oj.get_attrs()

  print(out)
  return out

def main() :
  pass

  ##

  pre_mod_name = get_pre_mod_name(d.code , Module_name)

  ##
  cur_mod_cache_fp = smpl_ret_cache_fp(Module_name)
  pre_mod_cache_fp = smpl_ret_cache_fp(pre_mod_name)

  ##
  new_cols = {
      cc.isManChecked              : None ,
      cc.sheet_clean_0_exception   : None ,
      cc.start_jm_from_sheet       : None ,
      cc.end_jm_from_sheet         : None ,
      cc.is_end_jm_ok              : None ,
      cc.before_clean_0_rows_count : None ,
      cc.before_clean_0_cols_cou   : None ,
      cc.after_clean_0_rows_cou    : None ,
      cc.after_clean_0_cols_cou    : None ,
      cc.is_clean_sheet_0_suc      : None ,
      cc.revised_jmonth            : None ,
      }

  ##
  df = load_update_df(pre_module_cache_fp = pre_mod_cache_fp ,
                      current_module_cache_fp = cur_mod_cache_fp ,
                      new_cols_2add_and_update = list(new_cols.keys()))

  ##
  drop_cols = {
      cc.is_empty : None ,
      }

  df = df.drop(columns = drop_cols , errors = 'ignore')

  ##
  df[cc.titleJMonth] = df[cc.titleJDate].astype(int) // 100
  df[cc.titleJMonth] = df[cc.titleJMonth].astype(str)

  ##
  def set_is_sheet_man_checked(
      idf
      ) :
    fps = get_all_fps_wt_suffix(d.man_clnd_sheets , '.xlsx')
    sts = [x.stem for x in fps]

    _cnd = idf[cc.sheetStem].isin(sts)
    print('len:' , len(_cnd[_cnd]))

    _cnd_1 = idf[cc.sheetStem].notna()
    idf.loc[_cnd_1 , cc.isManChecked] = False

    idf.loc[_cnd , cc.isManChecked] = True
    idf.loc[_cnd , cc.is_clean_sheet_0_suc] = False
    return idf

  df = set_is_sheet_man_checked(df)

  ##
  def remove_exess_cleaned_sheets() :
    fps = get_all_fps_wt_suffix(d.xl_cln_sheet_0 , '.xlsx')
    sts = [x.stem for x in fps]

    _cnd = df[cc.is_clean_sheet_0_suc].eq(True)
    _cnd &= df[cc.is_stocks_sheet].eq(True)

    sht_2del = set(sts) - set(df.loc[_cnd , cc.sheetStem])
    print('Sheets to remove count:' , len(sht_2del))

    _ = [(d.xl_cln_sheet_0 / x).with_suffix('.xlsx').unlink() for x in sht_2del]

  remove_exess_cleaned_sheets()

  ##
  def mask_sheets(
      idf
      ) :
    _cnd = idf[cc.is_stocks_sheet].eq(True)
    _cnd &= idf[cc.is_clean_sheet_0_suc].ne(True)

    print(len(_cnd[_cnd]))
    return _cnd

  msk = mask_sheets(df)

  ##
  def clean_all_sheets_0(
      idf ,
      imcn
      ) :
    flt = idf[imcn]
    print(len(flt))

    cpc = mp.cpu_count()
    pool = mp.Pool(cpc)

    _args = zip(flt[cc.sheetStem] , flt[cc.titleJMonth])
    _res = pool.starmap(_targ , _args)

    return _res

  res = clean_all_sheets_0(df , msk)

  ##
  def _add2df(
      idf ,
      index ,
      out
      ) :
    for ky , vl in out.items() :
      idf.at[index , ky] = vl
    return idf

  def add2df_all(
      idf ,
      imsk ,
      ires
      ) :
    _wanted_indices = imsk[imsk].index
    for _ind , _hs in zip(_wanted_indices , ires) :
      idf = _add2df(idf , _ind , _hs)
    return idf

  df = add2df_all(df , msk , res)

  ##
  df = df.reset_index(drop = True)

  ##
  def set_revised_jmonth(
      idf
      ) :
    _targc = cc.revised_jmonth

    _cnd = idf[cc.is_end_jm_ok].eq(True)
    idf.loc[_cnd , _targc] = idf[cc.titleJMonth]

    _cnd_1 = idf[cc.is_end_jm_ok].eq(False)
    idf.loc[_cnd_1 , _targc] = idf[cc.end_jm_from_sheet]

    _cnd_2 = idf[cc.is_end_jm_ok].eq(False) & idf[_targc].isna()
    idf.loc[_cnd_2 , _targc] = idf[cc.titleJMonth]

    # fix type
    _cnd_2 = idf[_targc].notna()
    _col = idf.loc[_cnd_2 , _targc]
    idf.loc[_cnd_2 , _targc] = _col.astype(int).astype(str)
    return idf

  df = set_revised_jmonth(df)

  ##
  def do_all_have_revised_jm(
      idf
      ) :
    msk_1 = idf[cc.is_clean_sheet_0_suc].eq(True)
    msk_1 &= idf[cc.revised_jmonth].isna()

    _df = idf[msk_1]
    assert len(_df) == 0

  do_all_have_revised_jm(df)

  ##
  def _count_per_xl_clean0suc_sheets(
      idf
      ) :
    _msk = idf[cc.is_clean_sheet_0_suc].eq(True)

    idf.loc[_msk , 'helper'] = 1
    idf.loc[~ _msk , 'helper'] = 0

    msk = idf[cc.fileSuf].eq('.xlsx')

    _col = cc.countCln0PerXl

    idf.loc[msk , _col] = df.groupby(cc.fileStem)['helper'].transform('sum')

    idf = idf.drop(columns = 'helper')

    return idf

  df = _count_per_xl_clean0suc_sheets(df)

  ##
  def find_xls_with_no_cln0_suc(
      idf
      ) :
    _msk = idf[cc.countCln0PerXl].eq(0)
    return idf[_msk]

  df1 = find_xls_with_no_cln0_suc(df)

  ##
  save_current_module_cache(df , cur_mod_cache_fp)

##


if __name__ == "__main__" :
  main()
  print(f'{Module_name} Done.')

def _test() :
  pass

  ##
  sheet_stem = '196933_4_N_0'
  end_jmonth = 140009
  ##
  oj = ExcelPossibleStocksPortfoSheet(sheet_stem , end_jmonth)
  ##
  oj.load()
  oj.df = oj.df.T.reset_index(drop = True).T
  oj.df = oj.df.applymap(normalize_str)
  df1 = oj.df
  ##
  oj.df = convert_str_based_zeros(oj.df)
  oj.df = convert_blank_cells_to_none(oj.df)

  oj.df = remove_cols_and_rows_with_only_zero_or_none(oj.df)
  oj.df = remove_sparse_cols_with_condition(oj.df)
  df1 = oj.df
  ##
  oj.df = remove_first_sparse_rows_with_condition(oj.df)
  ##
  oj.df = remove_sparse_cols_with_condition(oj.df)
  df1 = oj.df
  ##
  oj.df = remove_cols_and_rows_with_only_zero_or_none(oj.df)
  df1 = oj.df
  ##
  oj.df = remove_unique_value_cols(oj.df)
  df1 = oj.df
  ##
  oj.df = remove_without_header_cols_except_names(oj.df)
  df1 = oj.df

  ##
  oj.process()
  out = oj.get_attrs()

  ##
  _targ('196933_4_N_0' , 140009)

  ##
  vals_not2_include = {
      0  : scp.month ,
      1  : scp.change ,
      2  : scp.firms ,
      3  : scp.kharid ,
      4  : scp.forosh ,
      5  : scp.tedad ,
      6  : scp.baha ,
      7  : scp.mablagh ,
      8  : scp.gheymat ,
      9  : scp.darsad ,
      10 : list(fss.in_sheet_vals.keys()) ,
      }
  _the_list = np.ravel(list(vals_not2_include.values()))
  ##
  vals_not2_include = {
      0 : scp.change ,
      1 : scp.kharid ,
      2 : scp.forosh ,
      3 : scp.tedad ,
      4 : scp.baha ,
      5 : scp.mablagh ,
      6 : scp.gheymat ,
      7 : scp.darsad ,
      }
  _the_list = list(itertools.chain.from_iterable(list(vals_not2_include.values())))
  _the_list

  ##
  df1

  ##
