""" [Extracts codal tables from downloaded json]

    """

##

import json
from functools import partial
from pathlib import PurePath

import multiprocess as mp
import pandas as pd

from _0 import get_all_fps_wt_suffix
from Helper.name_space import cc
from Helper.name_space import ctc
from Helper.name_space import d

Module_name = '_1.py'

def ret_cache_fp(
    cur_mod_name_wt_py ,
    cache_dir ,
    cache_suffix
    ) :
  stem = cur_mod_name_wt_py.split('.py')[0]
  name = stem + cache_suffix
  return cache_dir / name

smpl_ret_cache_fp = partial(ret_cache_fp ,
                            cache_dir = d.cache ,
                            cache_suffix = '')

def extract_data_from_json(
    jspn
    ) :
  print(jspn)
  with open(jspn , 'r') as jsfile :
    json_dict = json.load(jsfile)

  try :
    letters = json_dict['Letters']
    odf = pd.DataFrame(columns = ctc.list + [cc.json_stem])

    for row in letters :
      new_entry = {}

      for key , val in row.items() :
        new_entry[key] = val
      new_df = pd.DataFrame(data = new_entry)

      odf = pd.concat([odf , new_df] , ignore_index = True)

    odf[cc.json_stem] = jspn.stem
    return odf

  except KeyError as e :
    print(e)
    return pd.DataFrame()

def save_current_module_cache(
    df ,
    cpn
    ) :
  df = df.reset_index(drop = True)
  df.to_parquet(cpn)
  print(len(df))
  print("cache saved as" , PurePath(cpn).name)

def main() :
  pass

  ##
  cur_mod_cache_fp = smpl_ret_cache_fp(Module_name)

  if cur_mod_cache_fp.exists() :
    df = pd.read_parquet(cur_mod_cache_fp)
  else :
    df = pd.DataFrame()
    df[cc.json_stem] = None

  ##
  def ex_all_jsons(
      jsons_fp_list
      ) :
    cpc = mp.cpu_count()
    print('cpu cores num :' , cpc)
    pool = mp.Pool(cpc)

    _res = pool.map(extract_data_from_json , jsons_fp_list)

    _df = pd.concat(_res)
    return _df

  ##
  all_prior_json = list(df[cc.json_stem])
  dld_json_pns = get_all_fps_wt_suffix(d.json , '.json')

  for jsoncat in [0 , 1] :
    prior_jsons = [x for x in all_prior_json if str(x)[0 :2] == f'{jsoncat}-']
    if prior_jsons :
      max_n = max([int(x[2 :]) for x in prior_jsons])
      not_again = max(max_n - 5 , 0)
    else :
      not_again = 0
    json_pns_cat = [x for x in dld_json_pns if x.stem[0 :2] == f'{jsoncat}-']
    jsons_2_read = [x for x in json_pns_cat if int(x.stem[2 :]) > not_again]
    print('json count:' , len(jsons_2_read))
    _df = ex_all_jsons(jsons_2_read)
    df = pd.concat([df , _df] , ignore_index = True)

  print(df)

  ##
  df_copy = df.copy()

  ##
  df = df_copy.copy()

  ##
  def _drop_not_useful_cols(
      idf
      ) :
    lcols = {
        ctc.SuperVision : None ,
        ctc.UnderSupervision : None ,
        ctc.SentDateTime : None ,
        ctc.HasXbrl : None ,
        ctc.XbrlUrl : None ,
        ctc.TedanUrl : None ,
        ctc.IsEstimate : None ,
        ctc.HasExcel : None ,
        ctc.HasPdf : None ,
        ctc.ExcelUrl : None ,
        ctc.PdfUrl : None ,
        }
    idf = idf.drop(columns = lcols , errors = 'ignore')
    return idf

  df = _drop_not_useful_cols(df)

  ##
  col = ctc.PublishDateTime
  df[col] = df[col].str.replace(r'\D' , '' , regex = True)
  df[col] = df[col].astype(int).astype(str)

  ##
  df = df.drop_duplicates(subset = df.columns.difference([cc.json_stem]))

  ##
  df = df.sort_values(by = [ctc.PublishDateTime] , ascending = False)

  ##
  save_current_module_cache(df , cur_mod_cache_fp)

##


if __name__ == "__main__" :
  main()
  print(f'{Module_name} Done.')
