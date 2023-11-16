"""[Filters only in common_stocks funds]
    """
##
import pandas as pd

from Py.Helpers import functions_and_dirs as fu
from Py.Helpers.name_space import cte , ft , knc

Module_name = '_5.py'

def main() :
  pass
  ##
  pre_mod_name = fu.get_pre_mod_name(d.code , Module_name)
  cur_mod_cache_fp = simpl_ret_cache_fp(Module_name)
  pre_mod_cache_fp = simpl_ret_cache_fp(pre_mod_name)
  new_cols: dict = {}
  new_index_levels_names: list = []

  ##
  df = fu.load_update_df(pre_module_cache_fp=pre_mod_cache_fp ,
                         current_module_cache_fp=cur_mod_cache_fp ,
                         new_cols_2add_and_update=list(new_cols.keys()) ,
                         new_index_cols=new_index_levels_names)

  funds = pd.read_excel(cte.funds_fp , engine='openpyxl')

  ##
  def find_best_idea_funds_and_save(funds_df) :
    _cnd = funds_df[knc.fundInvMkt].isin([ft.InStocks , ])
    _bif = funds_df[_cnd]
    gps = _bif.groupby(knc.fundManager)
    each_manager_funds_count = gps[knc.key_wos].count()
    _cnd1 = each_manager_funds_count >= 2
    best_idea_managers = each_manager_funds_count[_cnd1]
    _bif = _bif[_bif[knc.fundManager].isin(best_idea_managers.index)]
    _bif.sort_values(by=knc.fundManager , inplace=True)
    print('best idea funds created.')
    fu.save_df_as_a_nice_xl(_bif , cte.best_idea_funds_fp)
    print('funds finished.')
    return _bif

  bif = find_best_idea_funds_and_save(funds)

  ##
  def in_stocks_funds(funds_df) :
    _cnd = funds_df[knc.fundInvMkt].isin([ft.InStocks , ])
    _df = funds_df[_cnd]
    fu.save_df_as_a_nice_xl(_df , cte.in_stocks_funds_fp)
    print('len:' , len(_df))
    return _df

  in_stock = in_stocks_funds(funds)

  ##
  def filter_metadata_with_only_in_stocks(idf) :
    _cnd = idf[knc.fundInvMkt].eq(ft.InStocks)
    _df = idf[_cnd]
    print('len:' , len(_df))
    return _df

  df = filter_metadata_with_only_in_stocks(df)
  ##
  fu.save_current_module_cache(df , cur_mod_cache_fp)

##
if __name__ == "__main__" :
  main()
  print(f'{Module_name} Done.')

def _test() :
  pass

  ##
