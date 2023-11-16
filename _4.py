"""[Extracts file download links from html texts]

    """

##
from io import StringIO

import multiprocess as mp
import pandas as pd
from lxml import etree

from _1 import smpl_ret_cache_fp
from _2 import get_pre_mod_name
from _2 import save_current_module_cache
from _3 import count_notna_of_thecol_in_groups
from _3 import load_update_df
from _3 import remove_list_from_another_list_keeping_the_order
from _3 import return_index_level_names_from_to
from Helper.name_space import cc
from Helper.name_space import ctc
from Helper.name_space import d
from Helper.name_space import indi

Module_name = '_4.py'

Parser = etree.HTMLParser()

def extract_dl_link(
    html_pn
    ) :
  with open(html_pn , "r" , encoding = "utf-8") as f :
    html = f.read()
  tree = etree.parse(StringIO(html) , Parser)
  onclick_nodes = tree.xpath("//*[@onclick]")
  onclick_val = [x.attrib["onclick"] for x in onclick_nodes]
  return clean_links(onclick_val)

def find_between_2chars_in_str(
    start_char ,
    end_char ,
    thestr
    ) :
  return (thestr.split(start_char))[1].split(end_char)[0]

def clean_links(
    onclick_vals
    ) :
  """removes duplicated links"""
  links = list(set(onclick_vals))
  links = [x for x in links if "DownloadFile" in str(x)]
  links = [find_between_2chars_in_str("('" , "')" , x) for x in links]
  return links

def _targ(
    html_stem
    ) :
  print(html_stem)
  try :
    html_pn = (d.html / html_stem).with_suffix('.html')
    links = extract_dl_link(html_pn)
    print(links)
    if len(links) != 0 :
      return links
  except :
    pass

def main() :
  pass

  ##

  pre_mod_name = get_pre_mod_name(d.code , Module_name)

  ##
  cur_mod_cache_fp = smpl_ret_cache_fp(Module_name)
  pre_mod_cache_fp = smpl_ret_cache_fp(pre_mod_name)

  ##
  new_cols: dict = {
      indi.L2_AttFile  : None ,
      cc.dlLink        : None ,
      cc.attFilesCount : None ,
      }

  ##
  df = pd.read_parquet(pre_mod_cache_fp)

  ##
  df = load_update_df(pre_module_cache_fp = pre_mod_cache_fp ,
                      current_module_cache_fp = cur_mod_cache_fp ,
                      new_cols_2add_and_update = list(new_cols.keys()))

  ##
  drop_cols = {
      ctc.Url             : None ,
      ctc.AttachmentUrl   : None ,
      cc.htmlFUrl         : None ,
      ctc.PublishDateTime : None ,
      cc.titleJMonth      : None ,
      }

  df = df.drop(columns = drop_cols , errors = 'ignore')

  ##
  msk = df[cc.isMonthlyRep]
  df = df[msk]

  ##
  df = df.drop(columns = cc.isMonthlyRep , errors = 'ignore')

  ##
  new_order = return_index_level_names_from_to(0 , 2)
  new_order += [cc.fundKey , cc.titleJDate]
  new_order += remove_list_from_another_list_keeping_the_order(list(df.columns) ,
                                                               new_order)
  assert len(new_order) == len(df.columns)

  ##
  df = df[new_order]

  ##
  def remove_excess_rows(
      idf
      ) :
    print(len(idf))

    by = return_index_level_names_from_to(0 , 1)
    cou = count_notna_of_thecol_in_groups(idf , by , cc.dlLink)
    rc = cou['helper'].ge(1)
    rc &= idf[cc.dlLink].isna()

    idf = idf[~rc]
    print(len(idf))

    idf = idf.drop(columns = 'helper')

    return idf

  df = remove_excess_rows(df)

  ##
  def mask_htmls() :
    _func = lambda \
      x : (d.html / str(x)).with_suffix('.html').exists()
    cn = df[cc.htmlFS].apply(_func)
    # cn &= df[cc.dlLink].isna()
    print('Number of HasAttHtml to search in' , len(cn[cn]))
    return cn

  msk = mask_htmls()

  ##
  def extract_all_file_dl_links(
      idf ,
      imcn
      ) :
    flt = idf[imcn]

    cpc = mp.cpu_count()
    pool = mp.Pool(cpc)

    _res = pool.map(_targ , flt[cc.htmlFS] , chunksize = 1000)

    return _res

  res = extract_all_file_dl_links(df , msk)

  ##
  def _add_result_to_df(
      idf ,
      index ,
      urls: list
      ) :
    if urls is not None :
      if len(urls) == 1 :
        idf.at[index , cc.dlLink] = str(urls[0])
        idf.at[index , cc.attFilesCount] = 1
      elif len(urls) > 1 :
        print('more then 1 !!!')
        _base_row = idf.loc[index].to_frame().T.copy()
        _base_row[cc.attFilesCount] = len(urls)
        print(_base_row)
        idf = idf.drop(index = index)
        for i , el in enumerate(urls) :
          _nrow = _base_row.copy()
          _nrow[indi.L2_AttFile] = str(i)
          _nrow[cc.dlLink] = str(el)
          idf = pd.concat([idf , _nrow])
    return idf

  def add2df(
      idf ,
      imcn ,
      ires
      ) :
    _indices = imcn[imcn].index

    for _ind , _urls in zip(_indices , ires) :
      idf = _add_result_to_df(idf , _ind , _urls)

    return idf

  df = add2df(df , msk , res)

  ##
  df = remove_excess_rows(df)

  ##
  save_current_module_cache(df , cur_mod_cache_fp)

##


if __name__ == "__main__" :
  main()
  print(f'{Module_name} Done.')

def _test() :
  pass

  ##
  _targ('541776_N')
