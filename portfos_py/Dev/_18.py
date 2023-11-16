"""[crop images]
    """
##
import multiprocessing as mp
import pickle
import re
import warnings
import shutil
import cv2
import numpy as np
import pytesseract
from pytesseract import Output

from main import (d , simpl_ret_cache_fp)
from Py._13 import (_is_a_meaningful_str , _is_not_stocks_portfo ,
                    _is_stocks_porto , )
from Py.Helpers import functions_and_dirs as fu
from Py.Helpers.name_space import cc
from PIL import Image
from functools import partial

warnings.filterwarnings("ignore")

Module_name = '_18.py'

class Under2TedadInst(Exception) :
  print('not enough tedad matches')

def _find_atleast_2_tedad_indices(img_data) :
  def _the_cond(x) :
    x = fu.normalize_str(x)
    ptr = r'[تنث][عغ][دذ]ا[دذ]'
    return re.match(ptr , x)

  img_data['text'] = [fu.normalize_str(x) for x in img_data['text']]

  matches = [i for i , x in enumerate(img_data['text']) if
             _the_cond(x) and img_data['conf'][i] > 50]

  if len(matches) < 2 :
    raise Under2TedadInst

  below_mid_points = []
  for i in [0 , 1] :
    ind = matches[i]
    x = img_data['left'][ind]
    w = img_data['width'][ind]
    x_mid = x + w / 2
    y = img_data['top'][ind]
    h = img_data['height'][ind]
    y_mid = y + h
    below_mid_points.append((x_mid , y_mid))

  crop_y = np.max([img_data['top'][x] + img_data['height'][x] for x in matches])

  return below_mid_points , crop_y

def _rotate_crop_img(below_mid_points ,
                     img ,
                     img_ndata_dir ,
                     img_stem ,
                     cropd_img_save_dir) :
  a = below_mid_points[0]
  b = below_mid_points[1]
  tan_al = (b[1] - a[1]) / (b[0] - a[0])
  al = np.arctan(tan_al) / (2 * np.pi) * 360
  rotated_img = rotate(img , al)

  ndta = pytesseract.image_to_data(rotated_img ,
                                   lang='fas' ,
                                   output_type=Output.DICT)
  dfp = img_ndata_dir / img_stem
  with open(dfp , 'wb') as fi :
    pickle.dump(ndta , fi)

  _ , cropy = _find_atleast_2_tedad_indices(ndta)
  cropped_img = rotated_img[cropy : , :]

  sfp = (cropd_img_save_dir / img_stem).with_suffix('.jpg')
  cv2.imwrite(str(sfp) , cropped_img)
  return al

def rotate(image , angle , center=None , scale=1.0) :
  (h , w) = image.shape[:2]
  if center is None :
    center = (w / 2 , h / 2)
  # Perform the rotation
  m = cv2.getRotationMatrix2D(center , angle , scale)
  rotated = cv2.warpAffine(image , m , (w , h))
  return rotated

def _targ(img_stem ,
          img_suf ,
          img_dir ,
          data_dir ,
          ndata_dir ,
          cropd_img_save_dir) :
  lo = {
      cc.RotAng1    : None ,
      cc.IsCropped1 : None
      }

  fp = (img_dir / img_stem).with_suffix(img_suf)
  img = Image.open(fp)
  img = np.array(img)

  dfp = data_dir / img_stem
  with open(dfp , 'rb') as fi :
    img_dta = pickle.load(fi)

  try :
    bmp , _ = _find_atleast_2_tedad_indices(img_dta)
    ang = _rotate_crop_img(bmp ,
                           img ,
                           ndata_dir ,
                           img_stem ,
                           cropd_img_save_dir)
    lo[cc.RotAng1] = ang
    lo[cc.IsCropped1] = True
  except Under2TedadInst :
    sfp = (cropd_img_save_dir / img_stem).with_suffix('.jpg')
    shutil.copy2(fp , sfp)
    lo[cc.RotAng1] = 0
    lo[cc.IsCropped1] = False
  print(img_stem , lo)
  return lo

def main() :
  pass
  ##
  pre_mod_name = fu.get_pre_mod_name(d.code , Module_name)
  cur_mod_cache_fp = simpl_ret_cache_fp(Module_name)
  pre_mod_cache_fp = simpl_ret_cache_fp(pre_mod_name)
  new_cols = {
      cc.RotAng1    : None ,
      cc.RotAng2    : None ,
      cc.IsCropped1 : None ,
      cc.IsCropped2 : None ,
      }
  new_index_levels_names: list = []
  ##
  df = fu.load_update_df(pre_module_cache_fp=pre_mod_cache_fp ,
                         current_module_cache_fp=cur_mod_cache_fp ,
                         new_cols_2add_and_update=list(new_cols.keys()) ,
                         new_index_cols=new_index_levels_names)

  ##
  col2drop = {
      cc.PdfSc1_IsSt  : None ,
      cc.PdfSc1_NotSt : None ,
      cc.PdfSc2_IsSt  : None ,
      cc.PdfSc2_NotSt : None ,
      }
  df = df.drop(columns=col2drop.keys() , errors='ignore')

  ##
  def mask_screenshots(idf) :
    _cnd = idf[cc.IsStFinal].eq(True)

    print(len(_cnd[_cnd]))
    return _cnd

  msk = mask_screenshots(df)

  ##
  def crop_imgs(idf , mask) :
    flt = idf[mask]
    print('len:' , len(flt))
    cpc = mp.cpu_count()
    pool = mp.Pool(cpc)

    for im_dir , dta_dir , ndta_dir , cropd_img_dir , col1 , col2 in zip([
        d.mod_shot_0 , d.PdfShots2] ,
        [d.shot_0_data , d.PdfSc2Data] ,
        [d.PdfSc1Data2 , d.PdfSc2Data2] ,
        [d.PdfSc1Cropd , d.PdfSc2Cropd] ,
        [cc.RotAng1 , cc.RotAng2] ,
        [cc.IsCropped1 , cc.IsCropped2]) :
      _fu = partial(_targ ,
                    img_suf='.jpg' ,
                    img_dir=im_dir ,
                    data_dir=dta_dir ,
                    ndata_dir=ndta_dir ,
                    cropd_img_save_dir=cropd_img_dir)
      lo = pool.starmap_async(_fu , zip(flt[cc.page_shot_stem])).get()

      idf.loc[mask , col1] = [x[cc.RotAng1] for x in lo]
      idf.loc[mask , col2] = [x[cc.IsCropped1] for x in lo]

    return idf

  ##
  df = crop_imgs(df , msk)

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
  fu.save_current_module_cache(df , cur_mod_cache_fp)

##
if __name__ == "__main__" :
  main()
  print(f'{Module_name} Done.')

def _test() :
  pass

  ##
  _fu = partial(_targ ,
                img_suf='.jpg' ,
                img_dir=d.mod_shot_0 ,
                data_dir=d.shot_0_data ,
                ndata_dir=d.PdfSc1Data2 ,
                cropd_img_save_dir=d.PdfSc1Cropd)
  lo = _fu('414087-N-N-1')

  ##
