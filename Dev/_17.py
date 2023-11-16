"""[Find Stocks Portolio in Pdf Screen Shots]
    """
##
import multiprocessing as mp
import pickle
import warnings

from main import (d , simpl_ret_cache_fp)
from Py._13 import (_is_a_meaningful_str , _is_not_stocks_portfo ,
                    _is_stocks_porto , )
from Py.Helpers import functions_and_dirs as fu
from Py.Helpers.name_space import cc

warnings.filterwarnings("ignore")

Module_name = '_17.py'

def _load_img_data_make_string(img_stem , load_dir , save_dir) :
  fp = (load_dir / img_stem)
  with open(fp , 'rb') as fi :
    dta = pickle.load(fi)
  img_str = ' '.join(dta['text'])
  sfp = (save_dir / img_stem).with_suffix('.txt')
  if not sfp.exists() :
    with open(sfp , 'w') as fi :
      print(sfp.name)
      fi.write(img_str)
  return img_str

def _targ(img_stem) :
  out = {}
  for lod , sav , col1 , col2 in zip([d.shot_0_data , d.PdfSc2Data] ,
                                     [d.shot_0_str , d.PdfSc2Str] ,
                                     [cc.PdfSc1_IsSt , cc.PdfSc2_IsSt] ,
                                     [cc.PdfSc1_NotSt , cc.PdfSc2_NotSt]) :
    img_str = _load_img_data_make_string(img_stem , lod , sav)
    img_str = fu.normalize_str(img_str)
    if _is_a_meaningful_str(img_str) :
      out[col1] = _is_stocks_porto(img_str)
      out[col2] = _is_not_stocks_portfo(img_str)
    else :
      out[col1] = None
      out[col2] = None
  return out

def main() :
  pass
  ##
  pre_mod_name = fu.get_pre_mod_name(d.code , Module_name)
  cur_mod_cache_fp = simpl_ret_cache_fp(Module_name)
  pre_mod_cache_fp = simpl_ret_cache_fp(pre_mod_name)
  new_cols = {
      cc.PdfSc1_IsSt  : None ,
      cc.PdfSc2_IsSt  : None ,
      cc.PdfSc1_NotSt : None ,
      cc.PdfSc2_NotSt : None ,
      cc.IsStFinal    : None ,
      }
  new_index_levels_names: list = []
  ##
  df = fu.load_update_df(pre_module_cache_fp=pre_mod_cache_fp ,
                         current_module_cache_fp=cur_mod_cache_fp ,
                         new_cols_2add_and_update=list(new_cols.keys()) ,
                         new_index_cols=new_index_levels_names)

  ##
  col2drop = {}
  df = df.drop(columns=col2drop.keys() , errors='ignore')

  ##
  def mask_screenshots(idf) :
    _cnd = idf[cc.fileSuf].eq('.pdf')

    print(len(_cnd[_cnd]))
    return _cnd

  msk = mask_screenshots(df)

  ##
  def find_stocks_pages(idf , mask) :
    flt = idf[mask]
    print('len:' , len(flt))
    cpc = mp.cpu_count()
    pool = mp.Pool(cpc)
    lo = pool.starmap_async(_targ , zip(flt[cc.page_shot_stem])).get()
    for key in lo[0].keys() :
      idf.loc[mask , key] = [x[key] for x in lo]
    return idf

  ##
  df = find_stocks_pages(df , msk)

  ##
  def apply_on_gps(gpdf) :
    # finds min stock portfo page in pdf & sets the flag for it true.
    _msk = gpdf[cc.PdfSc1_IsSt].eq(True)
    _msk |= gpdf[cc.PdfSc2_IsSt].eq(True)

    _ser_0 = gpdf.loc[_msk , cc.PdfPgNum].astype(int)
    if not _ser_0.empty :
      is_min_page = str(int(min(_ser_0)))
    else :
      is_min_page = str(0)

    _msk_0 = gpdf[cc.PdfPgNum].eq(is_min_page)
    gpdf.loc[_msk_0 , cc.IsStFinal] = True

    # finds minimun number of first page after stock page
    _msk_1 = gpdf[cc.PdfSc1_NotSt].eq(True)
    _msk_1 |= gpdf[cc.PdfSc2_NotSt].eq(True)

    _ser_1 = gpdf.loc[_msk_1 , cc.PdfPgNum].astype(int)
    if not _ser_1.empty :
      not_min_page = min(_ser_1)
    else :
      not_min_page = max(gpdf[cc.PdfPgNum].astype(int))

    # considers all pages between as common_stocks portfo pages & sets the flag
    pages_in_bet = range(int(is_min_page) + 1 , not_min_page)
    pages_in_bet = [str(x) for x in pages_in_bet]

    _msk_2 = gpdf[cc.PdfPgNum].isin(pages_in_bet)
    _msk_2 &= gpdf[cc.PdfSc1_NotSt].ne(True)
    _msk_2 &= gpdf[cc.PdfSc2_NotSt].ne(True)

    gpdf.loc[_msk_2 , cc.IsStFinal] = True
    return gpdf

  ##
  def set_pgs_to_read_stocks_portfo(idf) :
    _msk = idf[cc.PdfSc1_IsSt].notna()

    idf.loc[_msk] = idf.loc[_msk].groupby(cc.fileStem).apply(apply_on_gps)
    return idf

  ##
  df = set_pgs_to_read_stocks_portfo(df)

  ##
  def mask_1(idf , col) :
    _cnd = idf[col].eq(True)

    print(len(_cnd[_cnd]))
    return _cnd

  msk_1 = mask_1(df , cc.PdfSc1_IsSt)
  df1 = df[msk_1]

  msk_2 = mask_1(df , cc.PdfSc2_IsSt)
  df2 = df[msk_2]

  msk_3 = mask_1(df , cc.IsStFinal)
  df3 = df[msk_3]

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
