"""[Extracts Screen shots fins]
    """
##
import multiprocessing as mp
import pickle
import warnings
from functools import partial

import pytesseract
from PIL import Image
from pytesseract import Output

from main import (d , simpl_ret_cache_fp)
from Py.Helpers import functions_and_dirs as fu
from Py.Helpers.name_space import cc

warnings.filterwarnings("ignore")

Module_name = '_16.py'

def ocr_img_save_data(img_stem , img_suf , load_dir , save_dir) :
  sfp = save_dir / img_stem
  if sfp.exists() :
    print('exist')
    return None

  fp = (load_dir / img_stem).with_suffix(img_suf)
  img = Image.open(fp)
  dta = pytesseract.image_to_data(img , lang='fas' , output_type=Output.DICT)
  with open(sfp , 'wb') as fi :
    pickle.dump(dta , fi , protocol=pickle.HIGHEST_PROTOCOL)
  print(dta)
  return dta

def main() :
  pass
  ##
  pre_mod_name = fu.get_pre_mod_name(d.code , Module_name)
  cur_mod_cache_fp = simpl_ret_cache_fp(Module_name)
  pre_mod_cache_fp = simpl_ret_cache_fp(pre_mod_name)
  new_cols = {}
  new_index_levels_names: list = []
  ##
  df = fu.load_update_df(pre_module_cache_fp=pre_mod_cache_fp ,
                         current_module_cache_fp=cur_mod_cache_fp ,
                         new_cols_2add_and_update=list(new_cols.keys()) ,
                         new_index_cols=new_index_levels_names)

  ##
  col2drop = {
      cc.PdfStPg_ColsNum : None ,
      cc.PdfStPg_Cln     : None ,
      }
  df = df.drop(columns=col2drop.keys() , errors='ignore')

  ##
  def mask_screenshots(idf) :
    _cnd = idf[cc.fileSuf].eq('.pdf')

    print(len(_cnd[_cnd]))
    return _cnd

  msk = mask_screenshots(df)

  ##
  def ex_imgs_data(idf , mask) :
    flt = idf[mask]
    print('len:' , len(flt))
    cpc = mp.cpu_count()
    pool = mp.Pool(cpc)

    for lod_dir , s_dir in [(d.mod_shot_0 , d.shot_0_data) ,
                            (d.PdfShots2 , d.PdfSc2Data)] :
      _fu = partial(ocr_img_save_data ,
                    img_suf='.jpg' ,
                    load_dir=lod_dir ,
                    save_dir=s_dir)
      pool.starmap_async(_fu , zip(flt[cc.page_shot_stem])).get()

  ##
  ex_imgs_data(df , msk)

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
