"""[Rotates All pdf and images]
    """
##

import shutil
import warnings

import cv2
import multiprocess as mp
import numpy as np
from PIL import Image
from pytesseract import Output , pytesseract , TesseractError
from PyPDF2 import PdfFileReader
from PyPDF2 import PdfFileWriter

from Dev import functions_and_dirs as fu
from Helper.name_space import cc

warnings.filterwarnings("ignore")

Module_name = '_11.py'

def process_img(
    img_stm
    ) :
  fp = (fu.d.pdf_sc_shot / img_stm).with_suffix('.jpg')
  img = Image.open(fp)

  res_img = resize_image(img)
  img_arr = remove_noise_and_smooth(res_img)
  rot_img , ang = fix_rotation(img_arr)

  sfp = (fu.d.mod_shot_0 / img_stm).with_suffix('.jpg')
  rot_img.save(sfp)

  wol_img_arr = remove_lines(rot_img)
  wol_img = Image.fromarray(wol_img_arr)

  sfp1 = (fu.d.PdfShots2 / img_stm).with_suffix('.jpg')
  wol_img.save(sfp1)

  return {
      cc.rotAng : ang
      }

def resize_image(
    img
    ) :
  length_x , width_y = img.size
  factor = max(1 , int(1800 / length_x))  # 1800 for tesserect
  size = factor * length_x , factor * width_y
  im_resized = img.resize(size , Image.ANTIALIAS)  # best for OCR
  return im_resized

def remove_noise_and_smooth(
    img
    ) :
  img_arr = np.array(img)
  img_arr = cv2.cvtColor(img_arr , cv2.COLOR_BGR2GRAY)  # get rid of the color
  kernel = np.ones((1 , 1) , np.uint8)
  opening = cv2.morphologyEx(img_arr , cv2.MORPH_OPEN , kernel)
  closing = cv2.morphologyEx(opening , cv2.MORPH_CLOSE , kernel)
  img_arr = cv2.bitwise_or(img_arr , closing)
  return img_arr

def fix_rotation(
    img_arr
    ) :
  img = Image.fromarray(img_arr)
  try :
    out = pytesseract.image_to_osd(img ,
                                   nice = 1 ,
                                   output_type = Output.DICT ,
                                   lang = 'fas')
    angle = out['rotate']
    rotated_img = img.rotate(-angle , expand = True)
  except TesseractError as e :
    print(e)
    angle = 'Too few chars'
    rotated_img = img
  return rotated_img , angle

def remove_lines(
    img
    ) :
  # Remove lines to improve accuracy of tabular documents
  # https://stackoverflow.com/questions/33949831/whats-the-way-to-remove-all-lines-and-borders-in-imagekeep-texts-programmatic?answertab=votes#tab-top
  img_ar = np.array(img)
  result_ar = img_ar.copy()
  _arg_0 = cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU
  th = cv2.threshold(img_ar , 0 , 255 , _arg_0)[1]

  # Remove horizontal lines
  horizontal_kernel = cv2.getStructuringElement(cv2.MORPH_RECT , (40 , 1))
  remove_horizontal = cv2.morphologyEx(th ,
                                       cv2.MORPH_OPEN ,
                                       horizontal_kernel ,
                                       iterations = 2)
  cnts = cv2.findContours(remove_horizontal ,
                          cv2.RETR_EXTERNAL ,
                          cv2.CHAIN_APPROX_SIMPLE)
  cnts = cnts[0] if len(cnts) == 2 else cnts[1]
  for c in cnts :
    cv2.drawContours(result_ar , [c] , -1 , (255 , 255 , 255) , 5)

  # Remove vertical lines
  vertical_kernel = cv2.getStructuringElement(cv2.MORPH_RECT , (1 , 40))
  remove_vertical = cv2.morphologyEx(th ,
                                     cv2.MORPH_OPEN ,
                                     vertical_kernel ,
                                     iterations = 2)
  cnts = cv2.findContours(remove_vertical ,
                          cv2.RETR_EXTERNAL ,
                          cv2.CHAIN_APPROX_SIMPLE)
  cnts = cnts[0] if len(cnts) == 2 else cnts[1]
  for c in cnts :
    cv2.drawContours(result_ar , [c] , -1 , (255 , 255 , 255) , 5)
  return result_ar

def targ_process_img(
    pdfpage_shot_img_file_stem
    ) :
  oj = process_img(pdfpage_shot_img_file_stem)
  print(pdfpage_shot_img_file_stem , ':' , oj)
  return oj

def rotate_pages_of_pdf(
    pdf_fstem ,
    angles
    ) :
  fp = (fu.d.pdf / pdf_fstem).with_suffix('.pdf')

  pdf_file = open(fp , 'rb')
  pdf_r = PdfFileReader(pdf_file , strict = False)
  pdf_w = PdfFileWriter()

  angs = []
  for el in angles :
    if el != 'Too few chars' :
      angs.append(int(el))
    else :
      angs.append(0)
  try :
    assert pdf_r.getNumPages() == len(angs)
    for i , ang in enumerate(list(angs)) :
      pg = pdf_r.getPage(i)
      pg.rotateClockwise(ang)
      pdf_w.addPage(pg)
    pdf_out_pn = (fu.d.adj_txt_based_pdfs / pdf_fstem).with_suffix('.pdf')
    with open(pdf_out_pn , 'wb') as _file :
      pdf_w.write(_file)
    print(pdf_out_pn.name)
    return fu.cte.suc
  except KeyError as e :
    print('Error' , pdf_fstem , e)
    return str(e)

def main() :
  pass

  ##

  pre_mod_name = fu.get_pre_mod_name(fu.d.code , Module_name)
  cur_mod_cache_fp = fu.simpl_ret_cache_fp(Module_name)
  pre_mod_cache_fp = fu.simpl_ret_cache_fp(pre_mod_name)
  new_cols = {
      cc.rotAng     : None ,
      cc.PdfRotStat : None ,
      }

  ##
  df = fu.load_update_df(pre_module_cache_fp = pre_mod_cache_fp ,
                         current_module_cache_fp = cur_mod_cache_fp ,
                         new_cols_2add_and_update = list(new_cols.keys()))

  ##
  cols2drop = {
      0 : cc.pdf_pages_cou ,
      }
  df = df.drop(columns = cols2drop.values() , errors = 'ignore')

  ##
  def remove_exess_screen_shots() :
    fps = fu.get_all_fps_wt_suffix(fu.d.PdfShots2 , '.jpg')
    sts = [x.stem for x in fps]

    _cnd = df[cc.rotAng].notna()

    sht_2del = set(sts) - set(df.loc[_cnd , cc.page_shot_stem])
    for sht in sht_2del :
      (fu.d.PdfShots2 / sht).with_suffix('.jpg').unlink()
      (fu.d.mod_shot_0 / sht).with_suffix('.jpg').unlink()
      print(sht)
    print('count:' , len(sht_2del))

  remove_exess_screen_shots()

  ##
  def mask_jpgs(
      idf
      ) :
    _cnd = idf[cc.page_shot_stem].notna()
    _cnd &= idf[cc.rotAng].isna()
    print(len(_cnd[_cnd]))
    return _cnd

  msk = mask_jpgs(df)

  ##
  def process_all_pdf_shots(
      idf ,
      imcn
      ) :
    flt = idf[imcn]
    print(len(flt))

    cpc = mp.cpu_count()
    print('cores:' , cpc)
    pool = mp.Pool(cpc)

    _res = pool.map(targ_process_img , flt[cc.page_shot_stem])

    return _res

  res = process_all_pdf_shots(df , msk)

  ##
  def _add2df(
      idf ,
      out_dict ,
      index
      ) :
    for ky , vl in out_dict.items() :
      idf.at[index , ky] = vl
    return idf

  def add2df_all(
      idf ,
      imsk ,
      ires
      ) :
    _wanted_indices = imsk[imsk].index
    print(len(_wanted_indices))
    for _ind , _hs in zip(_wanted_indices , ires) :
      idf = _add2df(idf , _hs , _ind)
    return idf

  df = add2df_all(df , msk , res)

  ##
  fu.save_current_module_cache(df , cur_mod_cache_fp)

  ##
  def mask_pdf_files(
      idf
      ) :
    _msk = idf[cc.fileSuf].eq('.pdf')
    print('len:' , len(_msk[_msk]))
    return _msk

  msk = mask_pdf_files(df)

  ##
  def _add2df_2(
      rot_stat ,
      pdf_stem
      ) :
    _msk = df[cc.fileStem].eq(pdf_stem)
    df.loc[_msk , cc.PdfRotStat] = rot_stat

  ##
  def rotate_all_pdfs(
      idf ,
      mask
      ) :
    flt = idf[mask]

    flt = flt.sort_values(by = [cc.fileStem , cc.PdfPgNum])
    gped = flt.groupby([cc.fileStem])

    print("pdf to process: " , len(gped))
    cpc = mp.cpu_count()
    pool = mp.Pool(cpc)
    for gp_name , gpdf in gped.__iter__() :
      _args = []
      _res = pool.starmap(rotate_pages_of_pdf ,
                          args = [gp_name , gpdf[cc.rotAng]])

    return flt

  ##
  _df1 = rotate_all_pdfs(df , msk)

  ##
  fu.save_current_module_cache(df , cur_mod_cache_fp)

  ##
  msk = df[cc.fileSuf].eq('.pdf')
  msk &= df[cc.PdfRotStat].ne(fu.cte.suc)
  df2 = df[msk]

  msk &= ~ df[cc.fileStem].apply(lambda
                                   x : (d.PdfManRotated / str(x)).with_suffix(
      '.pdf').exists())
  df3 = df[msk]
  fss = df3[cc.fileStem].unique()

  ##
  def copy_man_rotated() :
    _fps = fu.get_all_fps_wt_suffix(fu.d.PdfManRotated , '.pdf')
    for _fp in _fps :
      shutil.copy2(_fp , fu.d.adj_txt_based_pdfs / _fp.name)

  copy_man_rotated()

##


if __name__ == "__main__" :
  main()
  print(f'{Module_name} Done.')

def _test() :
  pass

  ##
  process_img('524170_N_N-1')

  ##
