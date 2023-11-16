"""[concates all cleaned excel sheets and constructs a grand dataset
    of all excels data.]

    """

##

from _1 import smpl_ret_cache_fp
from _2 import get_pre_mod_name
from _2 import save_current_module_cache
import warnings
from _3 import load_update_df
from Helper.name_space import cc
from Helper.name_space import d , cte
import pandas as pd

warnings.filterwarnings("ignore")

Module_name = '_10.py'

def read_xl_file(
    sheet_stem ,
    the_dir
    ) :
  fp = (the_dir / sheet_stem).with_suffix('.xlsx')
  xl = pd.read_excel(fp , engine = 'openpyxl')
  print(sheet_stem)
  return xl

def main() :
  pass

  ##

  pre_mod_name = get_pre_mod_name(d.code , Module_name)

  ##
  cur_mod_cache_fp = smpl_ret_cache_fp(Module_name)
  pre_mod_cache_fp = smpl_ret_cache_fp(pre_mod_name)

  ##
  new_cols = {

      }

  ##
  df = load_update_df(pre_module_cache_fp = pre_mod_cache_fp ,
                      current_module_cache_fp = cur_mod_cache_fp ,
                      new_cols_2add_and_update = list(new_cols.keys()))

  ##
  def mask_suc_xls(
      idf
      ) :
    _cnd = idf[cc.is_clean_1_suc].eq(True)

    print(len(_cnd[_cnd]))

    return _cnd

  msk = mask_suc_xls(df)

  ##
  def append_xlsheets(
      idf ,
      mask
      ) :
    flt = idf[mask]
    print('len:' , len(flt))

    _gdf = pd.DataFrame()

    for ind , ro in flt.iterrows() :
      _df = read_xl_file(ro[cc.sheet_stem] , d.xl_cln_sheet_1)
      _gdf = pd.concat([_gdf , _df] , ignore_index = True)

    return _gdf

  gdf = append_xlsheets(df , msk)

  ##
  save_current_module_cache(df , cur_mod_cache_fp)

  ##
  gdf = gdf.applymap(str)

  ##
  gdf.to_parquet(cte.allXlsStocksSheets , index = False)

##


if __name__ == "__main__" :
  main()
  print(f'{Module_name} Done.')

def _test() :
  pass

  ##
