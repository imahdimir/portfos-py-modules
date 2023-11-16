"""[for each pdf page
    - checks if it has text if yes consider it text based
    - if it is text based check wheter it is rotated if so
    rotate it back.]
    """

##
import pathlib
import warnings
from pathlib import Path
import cv2
import multiprocess as mp
import PyPDF2
from PyPDF2 import PdfFileWriter
from PyPDF2.utils import PdfReadError

import numpy as np
import pandas as pd
from PIL import Image
from PyPDF2 import PdfFileReader
from PyPDF2 import PdfFileWriter
from pytesseract import Output
from pytesseract import pytesseract
from pytesseract import TesseractError

from _0 import get_all_fps_wt_suffix
from _1 import smpl_ret_cache_fp
from _2 import get_pre_mod_name
from _2 import save_current_module_cache
from _3 import count_notna_of_thecol_in_groups
from _3 import load_update_df
from _3 import return_index_level_names_from_to
from Dev import functions_and_dirs as fu
from Helper.name_space import cc
from Helper.name_space import d , cte
from Helper.name_space import indi

warnings.filterwarnings("ignore")

Module_name = '_12.py'

class PdfReport :
  def __init__(
      self ,
      pdf_stem
      ) :
    self.stem = pdf_stem
    self.fp = d.pdf / f'{self.stem}.pdf'
    self.pdf = None
    self.pages_cou = None
    self.is_txt_based = {}
    self.orient = {}

  def _print(
      self ,
      *args
      ) :
    print(self.stem , ': ' , *args)

  def find_orientation_and_ex_text_from_each_page(
      self ,
      file
      ) :
    self.pdf = PyPDF2.PdfFileReader(file)
    self.pgs_cou = self.pdf.numPages
    for pg in range(self.pgs_cou) :
      self.orient[pg] = self.pdf.getPage(pg).get('/Rotate')
      txt = self.pdf.getPage(pg).extractText()
      self.is_txt_based[pg] = len(txt) > 10

  def rotate_save_txt_based_pdfs(
      self
      ) :
    if not any(self.is_txt_based.values()) :
      self._print('Not text based.')
      return None

    self._print('text based.')

    pdf_w = PdfFileWriter()
    for pgn , ang in self.orient.items() :
      if ang is None :
        ang = 0
      else :
        self._print(pgn , ang)

      page = self.pdf.getPage(pgn)
      page.rotateClockwise(- ang)
      pdf_w.addPage(page)

    sfp = (d.adj_txt_based_pdfs / self.stem).with_suffix('.pdf')
    with open(sfp , 'wb') as _file :
      pdf_w.write(_file)

  def process(
      self
      ) :
    file = open(self.fp , 'rb')

    self.find_orientation_and_ex_text_from_each_page(file)
    self.rotate_save_txt_based_pdfs()

    file.close()

def _targ(
    pdf_stem
    ) :
  try :
    obj = PdfReport(pdf_stem)
    obj.process()
    out = {
        'orient'    : obj.orient ,
        'txt_based' : obj.is_txt_based ,
        }
    return out
  except (PdfReadError , KeyError) as e :
    print(e)
    (d.pdf / pdf_stem).with_suffix('.pdf').unlink()
    print('pdf deleted.')
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
      cc.is_txt_based : None ,
      cc.rotAng       : None ,
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
  def mask_pdfs(
      idf
      ) :
    _cnd = idf[cc.fileSuf].eq('.pdf')
    _cnd &= idf[indi.L4_PdfPage].eq(0)
    _cnd &= idf[cc.is_txt_based].isna()

    _fu = lambda \
        x : (d.pdf / f'{x}.pdf').exists()
    _cnd &= idf[cc.fileStem].apply(_fu)

    print(len(_cnd[_cnd]))

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
    print('cores:' , cpc)
    pool = mp.Pool(cpc)

    _res = pool.map(_targ , flt[cc.fileStem])

    return _res

  res = process_all_pdfs(df , msk)

  ##
  def add2df(
      idf ,
      pdf_stem ,
      res_dct
      ) :
    orie = res_dct['orient']
    txtb = res_dct['txt_based']

    _cnd0 = idf[cc.fileStem].eq(pdf_stem)
    for ky in orie.keys() :
      _cnd1 = _cnd0 & idf[indi.L4_PdfPage].eq(ky)
      idf.loc[_cnd1 , cc.rotAng] = orie[ky]
      idf.loc[_cnd1 , cc.is_txt_based] = txtb[ky]
    return idf

  def add2df_all(
      idf ,
      imsk ,
      ires
      ) :
    flt = idf[imsk]
    for st , _res in zip(flt[cc.fileStem] , ires) :
      if _res is not None :
        idf = add2df(idf , st , _res)
    return idf

  df = add2df_all(df , msk , res)

  ##
  save_current_module_cache(df , cur_mod_cache_fp)

  ##

  ##

if __name__ == "__main__" :
  main()
  print(f'{Module_name} Done.')

def _test() :
  pass

  ##

  ##
