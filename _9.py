"""[Reads Stocks Portfolio sheet from cleaned sheet 0 level]

    """

##

import warnings

import multiprocess as mp
import pandas as pd

from _0 import get_all_fps_wt_suffix
from _6 import save_df_as_a_nice_xl
from _1 import smpl_ret_cache_fp
from _2 import any_of_wordlist_isin_str
from _2 import get_pre_mod_name
from _2 import save_current_module_cache
from _3 import load_update_df
from _8 import scp
from Helper.name_space import cc
from Helper.name_space import d

warnings.filterwarnings("ignore")

Module_name = '_9.py'

class PortfoSheetCols :
  def __init__(
      self
      ) :
    self.asset_name = None

    self.start_shares = None
    self.start_purchase_cost = None
    self.start_net_sale_val = None

    self.change_buy_shares = None
    self.change_buy_cost = None
    self.change_sell_shares = None
    self.change_sell_revenue = None

    self.end_shares = None
    self.end_asset_price = None
    self.end_purchase_cost = None
    self.end_net_sale_val = None
    self.end_pct_to_all_assets = None

    for key , val in self.__dict__.items() :
      if val is None :
        self.__dict__[key] = key

psc = PortfoSheetCols()

class OutDfCols :
  def __init__(
      self
      ) :
    self.fundKey = None
    self.jmonth = None
    self.sheetStem = None
    for key , val in self.__dict__.items() :
      if val is None :
        self.__dict__[key] = key

odc = OutDfCols()

col_vals_not_vals = {
    psc.start_shares          : (scp.tedad , scp.change) ,
    psc.start_purchase_cost   : (scp.baha , scp.change) ,
    psc.start_net_sale_val    : (scp.khales , scp.change) ,

    psc.change_buy_shares     : (scp.tedad , []) ,
    psc.change_buy_cost       : (scp.baha , []) ,
    psc.change_sell_shares    : (scp.tedad , []) ,
    psc.change_sell_revenue   : (scp.mablagh , []) ,

    psc.end_shares            : (scp.tedad , scp.change) ,
    psc.end_asset_price       : (scp.gheymat , scp.change) ,
    psc.end_purchase_cost     : (scp.baha , scp.change) ,
    psc.end_net_sale_val      : (scp.khales , scp.change) ,
    psc.end_pct_to_all_assets : (scp.darsad , scp.change) ,
    }

class CleanedSheetLevel0 :
  def __init__(
      self ,
      fund_key ,
      sheet_stem ,
      revised_end_jmonth ,
      load_dir ,
      save_dir
      ) :
    self.fundKey = fund_key
    self.jmonth = int(float(revised_end_jmonth))
    self.sheetStem = str(sheet_stem)
    self.load_dir = load_dir
    self.save_dir = save_dir

    self.df = None
    self.hdf = None

    self.change_cells_xy = None
    self.min_chng_col = None
    self.max_chng_col = None

    self.start_section = None
    self.change_section = None
    self.end_section = None
    self.buy_section = None
    self.sell_section = None

    self.colsmap = None
    self.outdf = None

    self.is_cleaning_suc = False
    self.exception = None
    self.cols_cou = None

  def _print(
      self ,
      *args
      ) :
    print(self.sheetStem , ': ' , *args)

  def load_data(
      self
      ) :
    fp = (self.load_dir / self.sheetStem).with_suffix('.xlsx')
    self.df = pd.read_excel(fp , engine = 'openpyxl' , header = None)
    self.hdf = self.df.iloc[:3 , :]

  def find_the_change_cell(
      self
      ) :
    _func = lambda \
      x : any_of_wordlist_isin_str(str(x) , scp.change)
    df0 = self.hdf.applymap(_func)
    df1 = df0.stack(dropna = True)
    cells = df1[df1].index
    self.change_cells_xy = cells

  def find_change_cell_min_and_max_col(
      self
      ) :
    self.min_chng_col = min(self.change_cells_xy.get_level_values(1))
    self.max_chng_col = max(self.change_cells_xy.get_level_values(1))

  def define_each_section_df_header(
      self
      ) :
    self.start_section = self.hdf.iloc[: , :self.min_chng_col]
    _max = max([self.max_chng_col , self.min_chng_col + 4])
    self.change_section = self.hdf.iloc[: , self.min_chng_col : _max]
    self.end_section = self.hdf.iloc[: , _max :]

  def find_buy_section(
      self
      ) :
    _func = lambda \
      x : any_of_wordlist_isin_str(str(x) , scp.kharid)
    df0 = self.change_section.applymap(_func)
    df1 = df0.stack(dropna = True)
    cells = df1[df1].index
    mincol = min(cells.get_level_values(1))
    self.buy_section = self.hdf.iloc[: , mincol :mincol + 2]

  def find_sell_section(
      self
      ) :
    labels = set(self.change_section.columns) - set(self.buy_section.columns)
    self.sell_section = self.hdf[list(labels)]

  def build_cols_map(
      self
      ) :
    self.colsmap = PortfoSheetCols().__dict__.copy()
    self.colsmap[psc.asset_name] = 0
    hdr_df_map = {
        psc.start_shares          : self.start_section ,
        psc.start_purchase_cost   : self.start_section ,
        psc.start_net_sale_val    : self.start_section ,

        psc.change_buy_shares     : self.buy_section ,
        psc.change_buy_cost       : self.buy_section ,

        psc.change_sell_shares    : self.sell_section ,
        psc.change_sell_revenue   : self.sell_section ,

        psc.end_shares            : self.end_section ,
        psc.end_asset_price       : self.end_section ,
        psc.end_purchase_cost     : self.end_section ,
        psc.end_net_sale_val      : self.end_section ,
        psc.end_pct_to_all_assets : self.end_section ,
        }
    keys_2_iter = set(self.colsmap.keys()) - {psc.asset_name}
    for coln in keys_2_iter :
      _fu = find_col_name_by_header_value
      _idfh = hdr_df_map[coln]
      _vals = col_vals_not_vals[coln][0]
      _notval = col_vals_not_vals[coln][1]
      self.colsmap[coln] = _fu(_idfh , _vals , _notval)

  def build_outdf(
      self
      ) :
    self.outdf = pd.DataFrame()

    dcols = []
    for coln in self.colsmap.keys() :
      if self.colsmap[coln] in self.df.columns :
        self.outdf[coln] = self.df[self.colsmap[coln]]
        dcols.append(coln)

    for coln in odc.__dict__.keys() :
      self.outdf[coln] = self.__dict__[coln]

    self.outdf = self.outdf[list(odc.__dict__.keys()) + dcols]
    self.cols_cou = len(dcols)

  def save_outdf(
      self
      ) :
    fp = (self.save_dir / self.sheetStem).with_suffix('.xlsx')
    save_df_as_a_nice_xl(self.outdf , fp)
    print(self.sheetStem , ': outdf saved.')
    self.is_cleaning_suc = True

  def process(
      self
      ) :
    try :
      self.load_data()
      self.find_the_change_cell()
      self.find_change_cell_min_and_max_col()
      self.define_each_section_df_header()
      self.find_buy_section()
      self.find_sell_section()
      self.build_cols_map()
      self.build_outdf()
      self.save_outdf()
    except Exception as e :
      self._print(e)
      self.exception = str(e)

  def get_attrs(
      self
      ) :
    out = {
        cc.sheet_clean_1_exception : self.exception ,
        cc.is_clean_1_suc          : self.is_cleaning_suc ,
        cc.cols_cou                : self.cols_cou ,
        }
    return out

def find_col_name_by_header_value(
    idf_header ,
    vals_list ,
    not_vals_list
    ) :
  _func = lambda \
    x : any_of_wordlist_isin_str(str(x) , vals_list)
  _df1 = idf_header.applymap(_func)
  _df1 = _df1.any()
  _df1 = _df1[_df1]

  _func_1 = lambda \
    x : any_of_wordlist_isin_str(str(x) , not_vals_list)
  _df2 = idf_header.applymap(_func_1)
  _df2 = _df2.any()
  _df2 = _df2[_df2]

  labels = set(_df1.index) - set(_df2.index)

  if len(labels) == 1 :
    return list(labels)[0]

def _targ(
    fund_key ,
    sheet_stem ,
    revised_end_jmonth ,
    load_dir=d.xl_cln_sheet_0 ,
    save_dir=d.xl_cln_sheet_1
    ) :
  oj = CleanedSheetLevel0(fund_key ,
                          sheet_stem ,
                          revised_end_jmonth ,
                          load_dir ,
                          save_dir)
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
      cc.sheet_clean_1_exception : None ,
      cc.is_clean_1_suc          : None ,
      cc.cols_cou                : None ,
      }

  ##
  df = load_update_df(pre_module_cache_fp = pre_mod_cache_fp ,
                      current_module_cache_fp = cur_mod_cache_fp ,
                      new_cols_2add_and_update = list(new_cols.keys()))

  ##
  def remove_exess_cleaned_sheets() :
    fps = get_all_fps_wt_suffix(d.xl_cln_sheet_1 , '.xlsx')
    sts = [x.stem for x in fps]

    _cnd = df[cc.is_clean_1_suc].eq(True)

    sht_2del = set(sts) - set(df.loc[_cnd , cc.sheet_stem])
    print('Sheets to remove count:' , len(sht_2del))

    _ = [(d.xl_cln_sheet_1 / x).with_suffix('.xlsx').unlink() for x in sht_2del]

  remove_exess_cleaned_sheets()

  ##
  def mask_sheets(
      idf
      ) :
    _cnd = idf[cc.is_clean_sheet_0_suc].eq(True)
    _cnd &= idf[cc.is_clean_1_suc].ne(True)
    print(len(_cnd[_cnd]))
    return _cnd

  msk = mask_sheets(df)

  ##
  def clean_all_sheets_1(
      idf ,
      imcn
      ) :
    flt = idf[imcn]
    print(len(flt))

    cpc = mp.cpu_count()
    pool = mp.Pool(cpc)

    _args = zip(flt[cc.fundKey] , flt[cc.sheet_stem] , flt[cc.revised_jmonth])
    _res = pool.starmap(_targ , _args)

    return _res

  res = clean_all_sheets_1(df , msk)

  ##
  def _add2df(
      idf ,
      out_dict ,
      index
      ) :
    for ky , vl in out_dict.items() :
      idf.at[index , ky] = vl
    return idf

  def add2df_all(
      idf ,
      imsk ,
      ires
      ) :
    _wanted_indices = imsk[imsk].index
    print(len(_wanted_indices))
    for _ind , _hs in zip(_wanted_indices , ires) :
      idf = _add2df(idf , _hs , _ind)
    return idf

  df = add2df_all(df , msk , res)

  ##
  print(len(df[df[cc.is_clean_1_suc].eq(True)]))

  ##
  save_current_module_cache(df , cur_mod_cache_fp)

##


if __name__ == "__main__" :
  main()
  print(f'{Module_name} Done.')

def _test() :
  pass

  ##
  oj = CleanedSheetLevel0('ومعین' ,
                          '206846_4_N_0' ,
                          140009 ,
                          d.xl_cln_sheet_0 ,
                          d.xl_cln_sheet_1)

  ##
  oj.load_data()
  df1 = oj.df

  ##
