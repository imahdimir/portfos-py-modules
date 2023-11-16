"""[finds common_stocks portfo pages in text based pdf and reads thier fins]
    """
##
import multiprocessing as mp
import warnings
from functools import partial

import fitz
import tabula
from main import d
from main import simpl_ret_cache_fp
from Py
\._8
import any_of_list_isin_str_as_word
from Py.Helpers import functions_and_dirs as fu
from Py.Helpers.name_space import cc

warnings.filterwarnings("ignore")

Module_name = '_12.py'

def extract_txt_from_pdf_using_fitz(
    pdf_stem
    ) :
  fp = (d.adj_txt_based_pdfs / pdf_stem).with_suffix('.pdf')
  doc = fitz.open(fp)
  page_has_text = {}
  for _i , page in enumerate(doc) :
    page_txt = page.getText()
    corctd_txt = correct_rtl_text(page_txt)
    if not _is_a_meaningful_str(corctd_txt) :
      page_has_text[_i] = False
    else :
      page_has_text[_i] = True
      txt_fp = (d.PText0 / f'{pdf_stem}-{_i}').with_suffix('.txt')
      with open(txt_fp , 'w') as tfile :
        tfile.write(corctd_txt)
      print(txt_fp.name)
  doc.close()
  return page_has_text

def correct_rtl_text(
    page_text
    ) :
  line_list = page_text.splitlines()
  cor_lines = []
  for el in line_list :
    ln_txt = el[: :-1]
    ln_txt = fu.normalize_str(ln_txt)
    cor_lines.append(ln_txt)
  cor_text = '\n'.join(cor_lines)
  return cor_text

some_words = ['صورت' , 'وضعیت' , 'تعداد' , 'شرکت']
some_words = [fu.normalize_str(x) for x in some_words]

excl: list = ['سود' , 'درآمد' , 'هزینه' , 'اطلاعات آماری' , 'اوراق بهادار' ,
              '1-2-سرمایه' , 'تاریخ سر رسید' , 'درامد ثابت' , 'زیان' , 'سپرده' ,
              'تنزیل' , 'درآمدها' , 'تعدیل' , 'نرخ موثر' , 'قیمت اعمال' ,
              'تاریخ اعمال']
excl = [fu.normalize_str(x) for x in excl]

inc_list = ['سرمایه گذاری در سهام' , '1-1-سرمایه']
inc_list = [fu.normalize_str(x) for x in inc_list]

def _is_a_meaningful_str(
    istr: str
    ) :
  _wrds: list = some_words + excl + inc_list
  return any_of_list_isin_str_as_word(istr , _wrds)

def _is_stocks_porto(
    istr: str
    ) :
  if any_of_list_isin_str_as_word(istr , excl) :
    return False
  elif any_of_list_isin_str_as_word(istr , inc_list) :
    return True
  return False

def _is_not_stocks_portfo(
    istr: str
    ) :
  if any_of_list_isin_str_as_word(istr , excl) :
    return True

def _targ_0(
    pdf_pg_txt_fstem
    ) :
  fp = (d.PText0 / pdf_pg_txt_fstem).with_suffix('.txt')
  with open(fp , 'r') as file :
    txt = file.read()
  out = {
      cc.PdfPg_IsStockPortfo_0 : _is_stocks_porto(txt) ,
      cc.PdfPg_Is1stPgAfStock  : _is_not_stocks_portfo(txt) ,
      }
  print(pdf_pg_txt_fstem , out)
  return out

def read_tables(
    pdf_stem: str ,
    pages: list
    ) :
  _fp = (d.adj_txt_based_pdfs / pdf_stem).with_suffix('.pdf')
  pages_table_num = {}
  for pgnum in pages :
    tables = tabula.read_pdf(_fp , pages = int(pgnum) + 1 , stream = True)
    pages_table_num[pgnum] = len(tables)
    for i , df in enumerate(tables) :
      df = df.applymap(str)
      stem = f'{pdf_stem}-{pgnum}-{i}'
      _dir = d.PStPortfo0
      _fp_1 = (_dir / stem).with_suffix('.xlsx')
      fu.save_df_as_a_nice_xl(df , _fp_1)
      print(_fp_1.name)
  return pages_table_num

def main() :
  pass
  ##
  pre_mod_name = fu.get_pre_mod_name(d.code , Module_name)
  cur_mod_cache_fp = simpl_ret_cache_fp(Module_name)
  pre_mod_cache_fp = simpl_ret_cache_fp(pre_mod_name)
  new_cols = {
      cc.PdfPg_IsTxtBased      : None ,
      cc.PdfPg_IsStockPortfo_0 : None ,
      cc.PdfPg_Is1stPgAfStock  : None ,
      cc.PdfPg_IsStockPortfo_1 : None ,
      cc.PdfPg_TblCou          : None ,
      }
  new_index_levels_names: list = []

  ##
  df = fu.load_update_df(pre_module_cache_fp = pre_mod_cache_fp ,
                         current_module_cache_fp = cur_mod_cache_fp ,
                         new_cols_2add_and_update = list(new_cols.keys()) ,
                         new_index_cols = new_index_levels_names)
  ##
  col2drop = {
      cc.rotAng     : None ,
      cc.PdfRotStat : None ,
      }
  df = df.drop(columns = col2drop.keys() , errors = 'ignore')

  ##
  def del_everything() :
    for _dir in [d.PText0 , d.PStPortfo0 , d.PStPortfo1] :
      fps = fu.get_all_fps_wt_suffix(_dir , '')
      for el in fps :
        el.unlink()

  # del_everything()

  ##
  def mask_pdfs(
      idf
      ) :
    _msk = idf[cc.fileSuf].eq('.pdf')
    _msk &= idf[cc.PdfPg_IsTxtBased].isna()
    print(len(_msk[_msk]))
    return _msk

  msk = mask_pdfs(df)

  ##
  def _add2df_0(
      out: dict ,
      pdf_stem
      ) :
    _msk = df[cc.fileStem].eq(pdf_stem)
    for ky , vl in out.items() :
      _msk1 = _msk & df[cc.PdfPgNum].eq(str(ky))
      df.loc[_msk1 , cc.PdfPg_IsTxtBased] = vl

  ##
  def extract_text_from_all_pdfs_in_parallel(
      idf ,
      imcn
      ) :
    flt = idf[imcn]

    gped = flt.groupby([cc.fileStem])
    print("pdf to process: " , len(gped))

    cpc = mp.cpu_count()
    pool = mp.Pool(cpc)
    for gp_name , gpdf in gped.__iter__() :
      ncb = partial(_add2df_0 , pdf_stem = gp_name)
      pool.apply_async(extract_txt_from_pdf_using_fitz ,
                       args = [gp_name] ,
                       callback = ncb)
    pool.close()
    pool.join()

  ##
  extract_text_from_all_pdfs_in_parallel(df , msk)

  ##
  def mask_text_based_pdf_files(
      idf
      ) :
    _msk = idf[cc.PdfPg_IsTxtBased].eq(True)
    _msk &= idf[cc.PdfPg_IsStockPortfo_0].isna()

    print('len:' , len(_msk[_msk]))
    return _msk

  msk = mask_text_based_pdf_files(df)

  ##
  def _add2df_1(
      out ,
      _ind
      ) :
    for key , val in out.items() :
      df.loc[_ind , key] = val

  ##
  def find_stocks_portfolio(
      idf ,
      mask
      ) :
    flt = idf[mask]
    print("pages to process: " , len(flt))

    cpc = mp.cpu_count()
    pool = mp.Pool(cpc)

    lo = pool.starmap_async(_targ_0 , zip(flt[cc.page_shot_stem])).get()
    for key in [cc.PdfPg_IsStockPortfo_0 , cc.PdfPg_Is1stPgAfStock] :
      idf.loc[mask , key] = [x[key] for x in lo]
    return idf

  ##
  df = find_stocks_portfolio(df , msk)

  ##
  def apply_on_gps(
      gpdf
      ) :
    # finds min stock portfo page in pdf & sets the flag for it true.
    _msk = gpdf[cc.PdfPg_IsStockPortfo_0].eq(True)
    _ser_0 = gpdf.loc[_msk , cc.PdfPgNum].astype(int)
    if not _ser_0.empty :
      is_min_page = str(int(min(_ser_0)))
    else :
      is_min_page = str(0)

    _msk_0 = gpdf[cc.PdfPgNum].eq(is_min_page)
    gpdf.loc[_msk_0 , cc.PdfPg_IsStockPortfo_1] = True

    # finds minimun number of first page after stock page
    _msk_1 = gpdf[cc.PdfPg_Is1stPgAfStock].eq(True)
    _ser_1 = gpdf.loc[_msk_1 , cc.PdfPgNum].astype(int)
    if not _ser_1.empty :
      not_min_page = min(_ser_1)
    else :
      not_min_page = max(gpdf[cc.PdfPgNum].astype(int))

    # considers all pages between as common_stocks portfo pages & sets the flag
    pages_in_bet = range(int(is_min_page) + 1 , not_min_page)
    pages_in_bet = [str(x) for x in pages_in_bet]

    _msk_2 = gpdf[cc.PdfPgNum].isin(pages_in_bet)
    _msk_2 &= gpdf[cc.PdfPg_Is1stPgAfStock].ne(True)
    _msk_2 &= gpdf[cc.PdfPg_IsTxtBased].eq(True)

    gpdf.loc[_msk_2 , cc.PdfPg_IsStockPortfo_1] = True
    return gpdf

  ##
  def set_pgs_to_read_stocks_portfo(
      idf
      ) :
    _msk = idf[cc.PdfPg_IsStockPortfo_0].notna()

    idf.loc[_msk] = idf.loc[_msk].groupby(cc.fileStem).apply(apply_on_gps)
    return idf

  df = set_pgs_to_read_stocks_portfo(df)

  ##
  def mask_stocks_portfo_pages(
      idf
      ) :
    _msk = idf[cc.PdfPg_IsStockPortfo_1].eq(True)

    print('len:' , len(_msk[_msk]))
    return _msk

  msk = mask_stocks_portfo_pages(df)

  ##
  def _add2df_2(
      out: dict ,
      pdf_stem
      ) :
    _msk = df[cc.fileStem].eq(pdf_stem)
    for ky , vl in out.items() :
      _msk1 = _msk & df[cc.PdfPgNum].eq(str(ky))
      df.loc[_msk1 , cc.PdfPg_TblCou] = vl

  ##
  def extract_stocks_portfo_in_parallel(
      idf ,
      imcn
      ) :
    flt = idf[imcn]

    gped = flt.groupby([cc.fileStem])
    print("pdf to process: " , len(gped))

    cpc = mp.cpu_count()
    pool = mp.Pool(cpc)
    for gp_name , gpdf in gped.__iter__() :
      ncb = partial(_add2df_2 , pdf_stem = gp_name)
      pool.apply_async(read_tables ,
                       args = [gp_name , gpdf[cc.PdfPgNum]] ,
                       callback = ncb)
    pool.close()
    pool.join()

  ##
  extract_stocks_portfo_in_parallel(df , msk)
  ##
  fu.save_current_module_cache(df , cur_mod_cache_fp)

##
if __name__ == "__main__" :
  main()
  print(f'{Module_name} Done.')

def _test() :
  pass

  ##
