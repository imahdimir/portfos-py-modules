"""[takes screen-shot of all pdf pages as .jpg files]

    """

##

import warnings

import multiprocess as mp
import pandas as pd
from pdf2image import convert_from_path
from PIL.Image import DecompressionBombError

from _1 import smpl_ret_cache_fp
from _2 import get_pre_mod_name
from _2 import save_current_module_cache
from _3 import count_notna_of_thecol_in_groups
from _3 import load_update_df
from _3 import return_index_level_names_from_to
from Dev import functions_and_dirs as fu
from name_space import cc
from name_space import d
from name_space import indi
from _0 import get_all_fps_wt_suffix

warnings.filterwarnings("ignore")

Module_name = '_10.py'

class PdfReport :
  def __init__(
      self ,
      pdf_stem
      ) :
    self.stem = pdf_stem
    self.fp = d.pdf / f'{self.stem}.pdf'
    self.imgs = None
    self.pgs_cou = None

  def _print(
      self ,
      *args
      ) :
    print(self.stem , ': ' , *args)

  def convert_pdf_2_imgs(
      self
      ) :
    self.imgs = convert_from_path(self.fp , fmt = 'jpg' , dpi = 300)
    self.pgs_cou = len(self.imgs)

  def save_imgs(
      self
      ) :
    for i , img in enumerate(self.imgs) :
      fp = d.pdf_sc_shot / f'{self.stem}_{i}.jpg'
      img.save(fp)

  def process(
      self
      ) :
    self.convert_pdf_2_imgs()
    self.save_imgs()
    return int(self.pgs_cou)

def _targ(
    pdf_stem
    ) :
  try :
    pdfrep = PdfReport(pdf_stem)
    out = pdfrep.process()
    print(out)
    return out
  except DecompressionBombError as e :
    print(e)
    return None

def main() :
  pass

  ##

  pre_mod_name = get_pre_mod_name(d.code , Module_name)
  cur_mod_cache_fp = smpl_ret_cache_fp(Module_name)
  pre_mod_cache_fp = smpl_ret_cache_fp(pre_mod_name)

  new_cols = {
      indi.L4_PdfPage   : None ,
      cc.pdf_pages_cou  : None ,
      cc.page_shot_stem : None ,
      }

  ##
  df = pd.read_parquet(pre_mod_cache_fp)

  ##
  df = pd.read_parquet(cur_mod_cache_fp)

  ##
  df = load_update_df(pre_module_cache_fp = pre_mod_cache_fp ,
                      current_module_cache_fp = cur_mod_cache_fp ,
                      new_cols_2add_and_update = list(new_cols.keys()))

  ##
  def build_jpgs_stems(
      idf
      ) :
    _cnd = idf[indi.L4_PdfPage].notna()
    cols = [cc.fileStem , indi.L4_PdfPage]
    _df = idf.loc[_cnd , cols]
    _df = _df.applymap(str)
    idf.loc[_cnd , cc.page_shot_stem] = _df.agg('_'.join , axis = 1)
    return idf

  df = build_jpgs_stems(df)

  ##
  def remove_excess_shots(
      idf
      ) :
    _dir = d.pdf_sc_shot
    _suf = '.jpg'

    fps = get_all_fps_wt_suffix(_dir , _suf)
    sts = [x.stem for x in fps]

    _cnd = idf[indi.L4_PdfPage].notna()

    fs_2del = set(sts) - set(idf.loc[_cnd , cc.page_shot_stem])

    for _f in fs_2del :
      print(_f)
      (_dir / _f).with_suffix(_suf).unlink()

    print(len(fs_2del))

  remove_excess_shots(df)

  ##
  def remove_exceess_rows(
      idf
      ) :
    print('len:' , len(idf))

    _by = return_index_level_names_from_to(0 , 3)

    _cnd = idf[cc.fileSuf].eq('.pdf')

    _cou = count_notna_of_thecol_in_groups(idf , _by , indi.L4_PdfPage)
    rc = _cou['helper'].ge(1)

    _cnd &= rc
    _cnd &= idf[indi.L4_PdfPage].isna()

    print(len(_cnd[_cnd]))
    idf = idf[_cnd]
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
    inds = imsk[imsk].index
    idf.loc[inds , cc.pdf_pages_cou] = ires
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
  def build_jpgs_stems(
      idf
      ) :
    _cnd = idf[indi.L4_PdfPage].notna()
    cols = [cc.fileStem , indi.L4_PdfPage]
    _df = idf.loc[_cnd , cols]
    _df = _df.applymap(str)
    idf.loc[_cnd , cc.page_shot_stem] = _df.agg('_'.join , axis = 1)
    return idf

  df = build_jpgs_stems(df)

  ##
  save_current_module_cache(df , cur_mod_cache_fp)

##


if __name__ == "__main__" :
  main()
  print(f'{Module_name} Done.')

def _test() :
  pass

  ##
  fps = fu.get_all_fps_wt_suffix(d.pdf_sc_shot , '.jpg')

  def ren(
      fp
      ) :
    st = fp.stem
    if len(st.split('-')) != 2 :
      print(st)
      return st
    name1 = st.split('-')[0]
    name2 = st.split('-')[1]
    nst = name1 + '_' + name2
    nfp = fp.with_stem(nst)
    return nfp

  for _fp in fps :
    nfp = ren(_fp)
    if nfp is not None :
      _fp.rename(ren(nfp))
      print(nfp)

  ren(fps[0])

##
