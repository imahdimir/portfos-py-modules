"""[- Downloads all attachment files, xl and pdf files.
    - And Tests their health and deletes corrupt files.]

    """

##

import asyncio
import math
import warnings
from functools import partial
from zipfile import BadZipfile

import aiohttp
import multiprocess as mp
import nest_asyncio
import numpy as np
import openpyxl as xl
from aiohttp.client_exceptions import ClientConnectorError
from openpyxl.utils.exceptions import InvalidFileException
from PyPDF2 import PdfFileReader
from PyPDF2.utils import PdfReadError

from _1 import get_all_fps_wt_suffix
from _1 import smpl_ret_cache_fp
from _2 import get_pre_mod_name
from _2 import save_current_module_cache
from _3 import load_update_df
from _3 import return_index_level_names_from_to
from Helper.name_space import cc
from Helper.name_space import cte
from Helper.name_space import d

warnings.filterwarnings("ignore")
nest_asyncio.apply()

Module_name = '_5.py'

async def _download_file_content(
    url
    ) :
  async with aiohttp.ClientSession(trust_env = True) as session :
    async with session.get(url ,
                           allow_redirects = True ,
                           verify_ssl = False ,
                           timeout = 60) as resp :
      content = await resp.read()
      return content

async def _write_content_to_file(
    content ,
    fp
    ) :
  with open(fp , "wb") as f :
    f.write(content)
  print(fp.name)

async def _web_scrape_task(
    url ,
    fp
    ) :
  try :
    content = await _download_file_content(url)
    await _write_content_to_file(content , fp)
  except (TimeoutError , ClientConnectorError) as e :
    print(str(type(e)))

def _create_async_task(
    url ,
    fp
    ) :
  if fp.exists() :
    return None
  furl = cte.codal_rep + url
  return _web_scrape_task(furl , fp)

async def _targ(
    urls ,
    file_stems ,
    the_dir ,
    suffix
    ) :
  tasks = []
  for url , fs in zip(urls , file_stems) :
    fp = (the_dir / fs).with_suffix(suffix)

    _tsk = _create_async_task(url , fp)
    if url is not None :
      tasks.append(_tsk)
  await asyncio.gather(*tasks , return_exceptions = True)

def is_excel_healthy(
    file_stem
    ) :
  fp = (d.xl / file_stem).with_suffix('.xlsx')

  try :
    xl.load_workbook(fp , data_only = True)
    print(fp.name + ': Healthy')
    return True
  except (BadZipfile , InvalidFileException , OSError) as e :
    print(fp.name + ': ' + str(e))
    return False

def is_pdf_healthy(
    file_stem
    ) :
  fp = (d.pdf / file_stem).with_suffix('.pdf')

  try :
    with open(fp , 'rb') as file :
      _ = PdfFileReader(file , strict = False)
      print(fp.name + ': Healthy')
    return True
  except (PdfReadError , OSError) as e :
    print(fp.name + ': ' + str(e))
    if str(e) == 'startxref not found' :
      print(fp.name + ': Healthy')
      return True
    else :
      return False

def main() :
  pass

  ##

  pre_mod_name = get_pre_mod_name(d.code , Module_name)

  ##
  cur_mod_cache_fp = smpl_ret_cache_fp(Module_name)
  pre_mod_cache_fp = smpl_ret_cache_fp(pre_mod_name)

  ##
  new_cols: dict = {
      cc.fileStem      : None ,
      cc.isFileHealthy : None ,
      cc.fileSuf       : None ,
      }

  ##
  df = load_update_df(pre_module_cache_fp = pre_mod_cache_fp ,
                      current_module_cache_fp = cur_mod_cache_fp ,
                      new_cols_2add_and_update = list(new_cols.keys()))

  ##
  def build_file_stems(
      idf
      ) :
    _cnd = idf[cc.dlLink].notna()
    cols = return_index_level_names_from_to(0 , 2)
    _df = idf.loc[_cnd , cols]
    _df = _df.fillna('N')
    idf.loc[_cnd , cc.fileStem] = _df.agg('_'.join , axis = 1)
    return idf

  df = build_file_stems(df)

  ##
  def _make_not_present_files_health_false(
      idf ,
      idir ,
      suff
      ) :
    _fu = lambda \
        x : (idir / str(x)).with_suffix(suff).exists()
    _cnd = ~ idf[cc.fileStem].apply(_fu)
    _cnd &= idf[cc.fileSuf].eq(suff)

    idf.loc[_cnd , cc.isFileHealthy] = None
    print(len(_cnd[_cnd]))

    return idf

  df = _make_not_present_files_health_false(df , d.xl , '.xlsx')
  df = _make_not_present_files_health_false(df , d.pdf , '.pdf')

  ##
  def mask_xls(
      idf
      ) :
    _fu = lambda \
        x : (d.xl / str(x)).with_suffix('.xlsx').exists()
    _cnd = idf[cc.fileStem].apply(_fu)
    _cnd &= idf[cc.isFileHealthy].ne(True)

    print(len(_cnd[_cnd]))
    return _cnd

  msk = mask_xls(df)

  ##
  def check_all_xls_health(
      idf ,
      imsk
      ) :
    flt = idf[imsk]

    cpc = mp.cpu_count()
    print('cpu cores num:' , cpc)
    pool = mp.Pool(cpc)

    _res = pool.map(is_excel_healthy , flt[cc.fileStem])

    return _res

  res = check_all_xls_health(df , msk)

  ##
  def add2df(
      idf ,
      imsk ,
      ires
      ) :
    _inds = imsk[imsk].index
    idf.loc[_inds , cc.isFileHealthy] = ires
    return idf

  df = add2df(df , msk , res)

  ##
  def mask_pdfs(
      idf
      ) :
    _fu = lambda \
        x : (d.pdf / str(x)).with_suffix('.pdf').exists()
    _cnd = idf[cc.fileStem].apply(_fu)
    _cnd &= idf[cc.isFileHealthy].ne(True)
    print(len(_cnd[_cnd]))
    return _cnd

  msk = mask_pdfs(df)

  ##
  def check_all_pdfs_health(
      idf ,
      imsk
      ) :
    flt = idf[imsk]

    cpc = mp.cpu_count()
    print('cpu cores num:' , cpc)
    pool = mp.Pool(cpc)

    _res = pool.map(is_pdf_healthy , flt[cc.fileStem])

    return _res

  res = check_all_pdfs_health(df , msk)

  ##
  df = add2df(df , msk , res)

  ##
  def _del_a_file(
      file_stem ,
      suff
      ) :
    if suff == '.xlsx' :
      fp = (d.xl / file_stem).with_suffix('.xlsx')
      if fp.exists() :
        fp.unlink()
        print(fp.name)
    else :
      fp = (d.pdf / file_stem).with_suffix('.pdf')
      if fp.exists() :
        fp.unlink()
        print(fp.name)

  _del_a_file_vectorized = np.vectorize(_del_a_file , excluded = ['suff'])

  def del_corrupt_files(
      idf
      ) :
    _cnd = idf[cc.isFileHealthy].eq(False)

    flt = idf[_cnd]
    if flt.empty :
      return None

    _del_a_file_vectorized(flt[cc.fileStem] , suff = '.xlsx')
    _del_a_file_vectorized(flt[cc.fileStem] , suff = '.pdf')

  del_corrupt_files(df)

  ##
  save_current_module_cache(df , cur_mod_cache_fp)

  ##
  def mask_download_xl_files(
      idf
      ) :
    cnd = idf[cc.dlLink].notna()
    cnd &= idf[cc.isFileHealthy].ne(True)
    cnd &= ~ idf[cc.dlLink].duplicated()

    print('to download nof:' , len(cnd[cnd]))
    return cnd

  mcn = mask_download_xl_files(df)

  ##
  def download_xlsx_files(
      idf ,
      imcn
      ) :
    flt = idf[imcn]
    print('xl files to download count:' , len(flt))
    if flt.empty :
      return None
    subdfs = np.array_split(flt , math.ceil(len(flt) / 50))
    for _i , subdf in enumerate(subdfs) :
      print(_i)
      urls = subdf[cc.dlLink]
      stems = subdf[cc.fileStem]
      ltarg = partial(_targ , the_dir = d.xl , suffix = '.xlsx')
      asyncio.run(ltarg(urls , stems))

  download_xlsx_files(df , mcn)

  ##
  msk = mask_xls(df)

  ##
  res = check_all_xls_health(df , msk)

  ##
  df = add2df(df , msk , res)

  ##
  del_corrupt_files(df)

  ##
  def mask_download_pdf_files(
      idf
      ) :
    cnd = idf[cc.dlLink].notna()
    cnd &= idf[cc.isFileHealthy].ne(True)
    cnd &= ~ idf[cc.dlLink].duplicated()

    print('to download: ' , len(cnd[cnd]))
    return cnd

  mcn = mask_download_pdf_files(df)

  ##
  def download_pdf_files(
      idf ,
      imcn
      ) :
    flt = idf[imcn]
    print('files to download count:' , len(flt))
    if flt.empty :
      return None
    subdfs = np.array_split(flt , math.ceil(len(flt) / 20))
    for _i , subdf in enumerate(subdfs) :
      print(_i)
      urls = subdf[cc.dlLink]
      stems = subdf[cc.fileStem]
      ltarg = partial(_targ , the_dir = d.pdf , suffix = '.pdf')
      asyncio.run(ltarg(urls , stems))

  download_pdf_files(df , mcn)

  ##
  msk = mask_pdfs(df)

  ##
  res = check_all_pdfs_health(df , msk)

  ##
  df = add2df(df , msk , res)

  ##
  del_corrupt_files(df)

  ##
  def make_suff_and_health_adjusted(
      idf
      ) :
    for thedir , suff in zip([d.xl , d.pdf] , ['.xlsx' , '.pdf']) :
      _cnd = idf[cc.fileStem].apply(lambda
                                      x : (
          thedir / str(x)).with_suffix(suff).exists())
      idf.loc[_cnd , cc.isFileHealthy] = True
      idf.loc[_cnd , cc.fileSuf] = suff
    return idf

  df = make_suff_and_health_adjusted(df)

  ##
  def _remove_excess_files(
      idf
      ) :
    for thedir , suff in zip([d.xl , d.pdf] , ['.xlsx' , '.pdf']) :
      _cnd = idf[cc.fileSuf].eq(suff)
      valid_files = list(idf.loc[_cnd , cc.fileStem])
      _fps = get_all_fps_wt_suffix(thedir , suff)
      _stms = [x.stem for x in _fps]
      _2del = [x for x in _stms if not x in valid_files]
      print(len(_2del))
      _ = [(thedir / x).with_suffix(suff).unlink() for x in _2del]

  _remove_excess_files(df)

  ##
  save_current_module_cache(df , cur_mod_cache_fp)

##


if __name__ == "__main__" :
  main()
  print(f'{Module_name} Done.')

def _test() :
  pass

  ##
  _cnd = df[cc.fileSuf].eq('.xlsx')
  valid_files = list(df.loc[_cnd , cc.fileStem])
  _fps = get_all_fps_wt_suffix(d.xl , '.xlsx')
  _stms = [x.stem for x in _fps]
  _2del = [x for x in _stms if not x in valid_files]

  ##
  msk = df[cc.dlLink].nunique()
  msk

  ##
  df1 = df[msk]

  ##

  # python 3.10.1  # google chrome v98  # chrome web driver for v98
