"""[Start of pdf files processing
    for each pdf finds its page number and then extends the cache
    for each page]
    """

##

import warnings

import multiprocess as mp
import pandas as pd
import PyPDF2
from PyPDF2.utils import PdfReadError

from _1 import smpl_ret_cache_fp
from _2 import get_pre_mod_name
from _2 import save_current_module_cache
from _3 import count_notna_of_thecol_in_groups
from _3 import load_update_df
from _3 import return_index_level_names_from_to
from Helper.name_space import cc
from Helper.name_space import d
from Helper.name_space import indi

warnings.filterwarnings("ignore")

Module_name = '_11.py'

class PdfReport :
  def __init__(
      self ,
      pdf_stem
      ) :
    self.stem = pdf_stem
    self.fp = d.pdf / f'{self.stem}.pdf'
    self.pgs_cou = None

  def _print(
      self ,
      *args
      ) :
    print(self.stem , ': ' , *args)

  def get_pdf_pages_count(
      self
      ) :
    with open(self.fp , 'rb') as file :
      pdf = PyPDF2.PdfFileReader(file)
      self.pgs_cou = pdf.numPages

    self._print(self.pgs_cou)

  def process(
      self
      ) :
    self.get_pdf_pages_count()
    return int(self.pgs_cou)

def _targ(
    pdf_stem
    ) :
  try :
    pdfrep = PdfReport(pdf_stem)
    out = pdfrep.process()
    return out
  except PdfReadError as e :
    print(e)
    return None

def main() :
  pass

  ##

  pre_mod_name = get_pre_mod_name(d.code , Module_name)

  ##
  cur_mod_cache_fp = smpl_ret_cache_fp(Module_name)
  pre_mod_cache_fp = smpl_ret_cache_fp(pre_mod_name)

  ##
  new_cols = {
      indi.L4_PdfPage  : None ,
      cc.pdf_pages_cou : None ,
      }

  ##
  df0 = pd.read_parquet(pre_mod_cache_fp)

  ##
  if cur_mod_cache_fp.exists() :
    df1 = pd.read_parquet(cur_mod_cache_fp)

  ##
  df = load_update_df(pre_module_cache_fp = pre_mod_cache_fp ,
                      current_module_cache_fp = cur_mod_cache_fp ,
                      new_cols_2add_and_update = list(new_cols.keys()))

  ##
  def remove_exceess_rows(
      idf
      ) :
    """

    """
    print('len:' , len(idf))

    _by = return_index_level_names_from_to(0 , 3)

    _cnd = idf[cc.fileSuf].eq('.pdf')

    _cou = count_notna_of_thecol_in_groups(idf , _by , indi.L4_PdfPage)
    rc = _cou['helper'].ge(1)

    _cnd &= rc
    _cnd &= idf[indi.L4_PdfPage].isna()

    print(len(_cnd[_cnd]))

    idf = idf[~ _cnd]

    print(len(idf))

    return idf

  df = remove_exceess_rows(df)

  ##
  def extend_df_on_pdf_page_num(
      idf
      ) :
    print(len(idf))

    _by = return_index_level_names_from_to(0 , 3)

    _cnd = idf[cc.fileSuf].eq('.pdf')

    _cou = count_notna_of_thecol_in_groups(idf , _by , indi.L4_PdfPage)
    _cnd_1 = _cou['helper'].eq(0)

    _cnd &= _cnd_1
    _cnd &= idf[cc.pdf_pages_cou].notna()

    _df = idf[_cnd]
    print(len(_df))

    for _ , ser in _df.iterrows() :
      _df1 = pd.concat([ser.to_frame().T] * int(ser[cc.pdf_pages_cou]))
      _df1[indi.L4_PdfPage] = range(ser[cc.pdf_pages_cou])
      idf = pd.concat([idf , _df1] , ignore_index = True)

    print(len(idf))

    return idf

  df = extend_df_on_pdf_page_num(df)

  ##
  def mask_pdfs(
      idf
      ) :
    _cnd = idf[cc.fileSuf].eq('.pdf')
    _cnd &= idf[cc.pdf_pages_cou].isna()
    _fu = lambda \
        x : (d.pdf / f'{x}.pdf').exists()
    _cnd &= idf[cc.fileStem].apply(_fu)

    print('len:' , len(_cnd[_cnd]))
    return _cnd

  msk = mask_pdfs(df)

  ##
  def process_all_pdfs(
      idf ,
      imcn
      ) :
    flt = idf[imcn]
    print(len(flt))

    cpc = mp.cpu_count()
    pool = mp.Pool(cpc)

    _res = pool.map(_targ , flt[cc.fileStem])

    return _res

  res = process_all_pdfs(df , msk)

  ##
  def add2df_all(
      idf ,
      imsk ,
      ires
      ) :
    idf.loc[imsk , cc.pdf_pages_cou] = ires
    return idf

  df = add2df_all(df , msk , res)
  save_current_module_cache(df , cur_mod_cache_fp)

  ##
  df = extend_df_on_pdf_page_num(df)

  ##
  df = remove_exceess_rows(df)

  ##
  save_current_module_cache(df , cur_mod_cache_fp)

##


if __name__ == "__main__" :
  main()
  print(f'{Module_name} Done.')

def _test() :
  pass

  ##
  msk = df[cc.pdf_pages_cou].notna()
  df1 = df[msk]
  df1 = df1.sort_values(by = [indi.L0_TracingNo , indi.L4_PdfPage])

##
