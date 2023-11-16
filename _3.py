"""[downloads all htmls]

    """

##
import os.path
import pathlib
import warnings
from pathlib import PurePath

import multiprocess as mp
import nest_asyncio
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from _0 import get_all_fps_wt_suffix
from _1 import smpl_ret_cache_fp
from _2 import get_pre_mod_name
from _2 import save_current_module_cache
from Helper.name_space import cc
from Helper.name_space import ctc
from Helper.name_space import cte
from Helper.name_space import d
from Helper.name_space import indi

options = Options()
options.headless = True
options.add_argument('--disable-gpu')  # Last I checked this was necessary.
driver = webdriver.Chrome(chrome_options = options)

warnings.filterwarnings("ignore")
nest_asyncio.apply()

Module_name = '_3.py'

html_sheet_ids = [3 , 4 , 5 , 6 , 7 , 17]

def return_index_level_names_from_to(start , end) :
  indi_levels = indi.__dict__.keys()
  return list(indi_levels)[start :end + 1]

def count_notna_of_thecol_in_groups(idf , groupby_list , the_col) :
  gpby_obj = idf.groupby(groupby_list)
  for gpn , gplabels in gpby_obj.groups.items() :
    _df = idf.loc[gplabels]
    cou = _df[the_col].notna().sum()
    idf.loc[gplabels , 'helper'] = cou
  return idf

def get_rendered_html(url , fstem) :
  fpn = d.html / f'{fstem}.html'
  if not fpn.exists() :
    driver.get(url)
    html = driver.page_source
    write_to_txtfile(html , fpn)

def write_to_txtfile(content , fp) :
  with open(fp , "w" , encoding = "utf-8") as file :
    file.write(content)
  print(f'saved as {PurePath(fp).name}')

def remove_list_from_another_list_keeping_the_order(target_list: list ,
                                                    list_of_elements_to_remove: list) :
  x = list(target_list)
  y = list(list_of_elements_to_remove)

  for el in y :
    if el in x :
      x.remove(el)

  return x

def load_update_df(pre_module_cache_fp: pathlib.Path ,
                   current_module_cache_fp: pathlib.Path ,
                   new_cols_2add_and_update: list = None) :
  """ds"""
  df = pd.read_parquet(pre_module_cache_fp)

  df = df.drop(columns = df.columns.intersection(new_cols_2add_and_update))

  if pathlib.Path(current_module_cache_fp).exists() :
    cdf = pd.read_parquet(current_module_cache_fp)
    c_cols = cdf.columns
    cols_union = set(df.columns) | set(new_cols_2add_and_update)
    cols_2_keep = set(c_cols) & (cols_union)
    cdf = cdf[cols_2_keep]
    df = df.merge(cdf , how = 'outer')

  very_new_cols = set(new_cols_2add_and_update) - set(df.columns)
  df[list(very_new_cols)] = None

  all_cols = df.columns
  _fu = remove_list_from_another_list_keeping_the_order
  not_index_cols = _fu(list(all_cols) , indi.__dict__.keys())
  not_present_index_cols = _fu(indi.__dict__.keys() , list(all_cols))
  present_index_cols = _fu(indi.__dict__.keys() , not_present_index_cols)

  df = df[present_index_cols + not_index_cols]

  df = df.drop_duplicates(subset = present_index_cols)

  df = df.reset_index(drop = True)
  print("Obs:" , len(df))

  return df

def main() :
  pass

  ##
  pre_mod_name = get_pre_mod_name(d.code , Module_name)

  ##
  cur_mod_cache_fp = smpl_ret_cache_fp(Module_name)
  pre_mod_cache_fp = smpl_ret_cache_fp(pre_mod_name)

  ##
  new_cols: dict = {
      indi.L1_HtmlSheetID : None ,
      cc.htmlFS           : None ,
      cc.htmlFUrl         : None ,
      }

  ##
  df = pd.read_parquet(pre_mod_cache_fp)

  ##
  df = load_update_df(pre_module_cache_fp = pre_mod_cache_fp ,
                      current_module_cache_fp = cur_mod_cache_fp ,
                      new_cols_2add_and_update = list(new_cols.keys()))

  ##
  drop_cols = {
      ctc.Symbol          : None ,
      ctc.CompanyName     : None ,
      ctc.Title           : None ,
      ctc.LetterCode      : None ,
      cc.cleanSym         : None ,
      cc.cleanCompanyName : None ,
      }

  df = df.drop(columns = drop_cols , errors = 'ignore')

  ##
  def remove_exceess_rows(idf) :
    print(len(idf))
    _by = return_index_level_names_from_to(0 , 0)
    cou = count_notna_of_thecol_in_groups(idf , _by , indi.L1_HtmlSheetID)
    rc = cou['helper'].ge(1)
    print(len(rc[rc]))
    rc &= idf[indi.L1_HtmlSheetID].isna()
    print(len(rc[rc]))
    rc &= ~ idf[ctc.HasAttachment]
    print(len(rc[rc]))
    idf = idf[~ rc]
    print(len(idf))
    idf = idf.reset_index(drop = True)
    return idf

  df = remove_exceess_rows(df)

  ##
  def extend_on_html_sheet_ids(idf) :
    _cnd = idf[ctc.HasHtml].eq(True)
    _by = return_index_level_names_from_to(0 , 0)
    cou = count_notna_of_thecol_in_groups(idf , _by , indi.L1_HtmlSheetID)
    rc = cou['helper'].eq(0)
    print(len(rc[rc]))
    _cnd &= rc
    print(len(_cnd[_cnd]))
    _df = idf[_cnd]
    for shid in html_sheet_ids :
      ldf = _df.copy()
      ldf[indi.L1_HtmlSheetID] = str(shid)
      _end = ldf[ctc.Url] + '&sheetId=' + str(shid)
      ldf[cc.htmlFUrl] = cte.codal_ir + _end
      idf = pd.concat([idf , ldf])
    print('len:' , len(idf))
    return idf

  df = extend_on_html_sheet_ids(df)

  ##
  df = remove_exceess_rows(df)

  ##
  def build_full_url_4att(idf) :
    _cnd = idf[ctc.HasAttachment]
    idf.loc[_cnd , cc.htmlFUrl] = cte.codal_ir + idf[ctc.AttachmentUrl]
    return idf

  df = build_full_url_4att(df)

  ##
  def build_html_stems(idf) :
    _df = idf[[indi.L0_TracingNo , indi.L1_HtmlSheetID]].copy()
    _df = _df.fillna('N')
    idf[cc.htmlFS] = _df.agg('_'.join , axis = 1)
    return idf

  df = build_html_stems(df)

  ##
  def remove_excess_htmls(idf) :
    _fps = get_all_fps_wt_suffix(d.html , '.html')
    print('len:' , len(_fps))

    _sts = [x.stem for x in _fps]
    _2del = set(_sts) - set(idf[cc.htmlFS])
    print('len deleted:' , len(_2del))

    for fs in _2del :
      (d.html / fs).with_suffix('.html').unlink()

  remove_excess_htmls(df)

  ##
  def remove_small_size_htmls(min_size_kb=5) :
    fps = get_all_fps_wt_suffix(d.html , '.html')

    for _fp in fps :
      if os.path.getsize(_fp) < min_size_kb * 1e3 :
        print(_fp.name)
        _fp.unlink()

  remove_small_size_htmls()

  ##
  def html_dl_mask(idf) :
    _cnd = idf[cc.htmlFUrl].notna()
    _cnd &= ~ idf[cc.htmlFUrl].duplicated()
    fu = lambda x : (d.html / str(x)).with_suffix('.html').exists()
    _cnd &= ~ idf[cc.htmlFS].apply(fu)
    print('len:' , len(_cnd[_cnd]))
    return _cnd

  mcn = html_dl_mask(df)

  ##
  def download_new_htmls(idf , filt) :
    flt = idf[filt]
    print('html to download rendered count' , len(flt))

    cpc = mp.cpu_count()
    print('cpu cores num :' , cpc)
    pool = mp.Pool(cpc)

    hstems = flt[cc.htmlFS]
    urls = flt[cc.htmlFUrl]

    pool.starmap_async(get_rendered_html , zip(urls , hstems))

  download_new_htmls(df , mcn)

  ##
  remove_excess_htmls(df)

  ##
  save_current_module_cache(df , cur_mod_cache_fp)

##


if __name__ == "__main__" :
  main()
  print(f'{Module_name} Done.')

def _test() :
  pass

  ##
  fp = '/Users/mahdimir/Documents/portfos_data/1_htmls/496460_N.html'
  import os

  os.path.getsize(fp)

  ##
