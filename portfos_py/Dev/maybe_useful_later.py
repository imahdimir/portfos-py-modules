"""ds"""
import inspect
import multiprocessing as mp
import pathlib
import re
from functools import partial
from typing import Any
import pandas as pd

import numpy as np
import openpyxl as pyxl
from openpyxl.utils.dataframe import dataframe_to_rows
from persiantools import characters , digits

def convert_psble_num_str_2float(istr) :
  try :
    o = float(istr)
    return o
  except ValueError :
    return istr
  except TypeError :
    return istr

def shift_month(cur_month , howmany: int = 0) :
  if howmany >= 0 :
    return find_n_jmonth_ahead(cur_month , howmany)
  elif howmany < 0 :
    return find_n_jmonth_before(cur_month , abs(howmany))

def find_n_jmonth_before(current_month , howmany=1) :
  if howmany == 1 :
    if current_month % 100 == 1 :
      previous_month = (current_month // 100 - 1) * 100 + 12
    else :
      previous_month = current_month - 1
    return previous_month
  if howmany == 0 :
    return current_month
  return find_n_jmonth_before(find_n_jmonth_before(current_month , 1) ,
                              howmany - 1)

def find_n_jmonth_ahead(current_month , howmany=1) :
  if howmany == 1 :
    if current_month % 100 == 12 :
      next_month = (current_month // 100 + 1) * 100 + 1
    else :
      next_month = current_month + 1
    return next_month
  elif howmany == 0 :
    return current_month
  return find_n_jmonth_ahead(find_n_jmonth_ahead(current_month , 1) ,
                             howmany - 1)

def find_axis_labels_by_any_of_list_is_in(df , vals: list , axis: {0 , 1} = 1) :
  od = df.applymap(lambda x : any_of_list_isin(vals , x))
  os = od.stack(dropna=True)
  os = os[os]
  oi = os.index
  ol = list(oi.get_level_values(axis).unique())
  ol.sort()
  return ol

def find_axis_labels_by_value(df , val , axis: {0 , 1} = 1) :
  od = df.applymap(lambda x : x == val)
  os = od.stack(dropna=True)
  os = os[os]
  oi = os.index
  ol = list(oi.get_level_values(axis).unique())
  ol.sort()
  return ol

def find_the_names_col(df , change_cols) :
  dfc = df.loc[: , :change_cols.min()].copy()
  md = dfc.applymap(lambda x : any_of_list_isin(Scp.in_a_sheet_names , x))
  md = md.stack(dropna=True)
  mi = md[md].index
  cs = mi.unique(level=1)
  if cs.size == 1 :
    names_col = cs[0]
    df = df.loc[: , names_col :]
    return df , names_col
  else :
    names_col = 0
    return df , names_col

def find_specific_cols(df , val_list , cols_count) :
  cs = find_axis_labels_by_any_of_list_is_in(df , val_list)
  if len(cs) == cols_count :
    cs.sort()
    return cs
  return [None] * cols_count

def find_specific_cols_by_value(df , val , cols_count) :
  cs = find_axis_labels_by_value(df , val)
  if len(cs) == cols_count :
    cs.sort()
    return cs
  return [None] * cols_count

def reindex_cols_and_rows_as_int(df) :
  df = df.reset_index(drop=True)
  df = df.T.reset_index(drop=True).T
  return df

def _parallelize(data , func , num_of_processes=mp.cpu_count()) :
  data_split = np.array_split(data , num_of_processes)
  pool = mp.Pool(num_of_processes)
  data = pd.concat(pool.map(func , data_split))
  pool.close()
  pool.join()
  return data

def _apply_func_on_pandas_seri(func , seri) :
  return seri.apply(func)

def parallelize_on_rows(seri , func , num_of_processes=mp.cpu_count()) :
  nfunc = partial(_apply_func_on_pandas_seri , func)
  return _parallelize(seri , nfunc , num_of_processes)

import signal

import pandas as pd
import params as pa
from numpy import nan

class TimeOutException(Exception) :
  pass

def alarm_handler(signum , frame) :
  print("ALARM signal received")
  raise TimeOutException()

signal.signal(signal.SIGALRM , alarm_handler)

# %%
def main() :
  ## %%
  df = pd.read_parquet("e" + pa.prqSuf)
  df
  ## %%
  df["portfsheet"] = nan
  ## %%
  df = pa.update_df_with_past_prq(df , "f")
  df
  ## %%
  fltcond = ~df["isxlcorrupt"]
  fltcond &= df["sheetsnames"].notna()
  fltcond &= df["portfsheet"].isna()
  flt = df[fltcond]
  flt
  ## %%
  flt = pa.remove_ones_which_process_hasdone_before(flt)
  flt
  ## %%
  j = 0
  for i , r in flt.iterrows() :
    print(i)
    j += 1

    signal.alarm(5)
    try :
      df.loc[i , "portfsheet"] = find_stocks_sheet(r["BN"] , r["sheetsnames"])
    except TimeOutException as ex :
      print(ex)
    signal.alarm(0)

    if j % 100 == 0 :
      pa.save_as_prq(df , "f")

def duplicate_df_rows(df , targ_col) :
  odf = pd.DataFrame(columns=df.columns)
  for ind , ro in df.iterrows() :
    print(ro[targ_col])
    o1 = duplicate_one_row_of_df(ind , ro , targ_col)
    odf = odf.append(o1)
  return odf

def duplicate_one_row_of_df(ind , row , targ_col) :
  odf = pd.DataFrame(columns=row.index)
  for i , va in enumerate(row[targ_col]) :
    odf.loc[f'{ind}_{i}' , targ_col] = va
    odf.loc[f'{ind}_{i}' , row.index.difference([targ_col])] = row[
      row.index.difference([targ_col])]
  return odf

#     ##
#     dir1 = '/Users/mahdimir/Documents/Projects/TSE-MutualFunds' \
#            '-MonthlyPortfolio/code/UpdateRawData copy'
#     adjust_file_names_based_on_cur_order(dir1, '.py')
#
#     ##
#     print(d.cwd)
#
#     ##
#
#     def _process_2(self):
#         for i, pn in enumerate(self.stocks_pgs_png_pns):
#             image = cv2.imread(pn, cv2.IMREAD_GRAYSCALE)
#             results = pytesseract.image_to_data(image,
#                                                 lang=langs,
#                                                 output_type=Output.DICT)
#             below_mid_points = _find_exactly_4_tedad_indices(results)
#             cropped_img = _rotate_crop_img(below_mid_points, image)
#             img_bin = treshold(cropped_img)
#             cv2.imwrite(d.img_bin / f'{self.sheetStem}-{i}', img_bin)
#
#     ##
#     def _read_tables_from_some_pages_of_pdf_by_tabula(pdf_pn,
#                                                       pages_list_counting_from_1):
#         dfs = tabula.read_pdf(pdf_pn,
#                               pages=pages_list_counting_from_1,
#                               stream=True)
#         print(len(dfs))
#         return dfs
#
#     def _find_exactly_4_tedad_indices(res):
#         def _the_cond(x):
#             x = cf.normalize_str(x)
#             ptr = r'[تنث][عغ][دذ]ا[دذ]'
#             return re.match(ptr, x)
#
#         matches = [i for i, x in enumerate(res['text']) if
#                    _the_cond(x) and res['conf'][i] > 70]
#         assert len(matches) == 4, 'not enough tedad matches'
#         below_mid_points = []
#         for i in [1, 2]:
#             indi = matches[i]
#             x = res['left'][indi]
#             w = res['width'][indi]
#             x_mid = x + w / 2
#             y = res['top'][indi]
#             h = res['height'][indi]
#             y_mid = y + h
#             below_mid_points.append((x_mid, y_mid))
#         return below_mid_points
#
#     def _rotate_crop_img(below_mid_points, img):
#         a = below_mid_points[0]
#         b = below_mid_points[1]
#         tan_al = (b[1] - a[1]) / (b[0] - a[0])
#         al = np.arctan(tan_al) / (2 * np.pi) * 360
#         rotated_img = rotate(img, al)
#         results = pytesseract.image_to_data(rotated_img,
#                                             lang=langs,
#                                             output_type=Output.DICT)
#         below_2 = _find_exactly_4_tedad_indices(results)
#         cropped_img = rotated_img[:, below_2[0][1] + 10]
#         return cropped_img
#
#     def rotate(image, angle, center=None, scale=1.0):
#         (h, w) = image.shape[:2]
#
#         if center is None:
#             center = (w / 2, h / 2)
#
#         # Perform the rotation
#         m = cv2.getRotationMatrix2D(center, angle, scale)
#         rotated = cv2.warpAffine(image, m, (w, h))
#
#         return rotated
#
#     def treshold(img):
#         max_color_val = 255
#         block_size = 15
#         subtract_from_mean = -2
#
#         img_bin = cv2.adaptiveThreshold(img,
#                                         max_color_val,
#                                         cv2.ADAPTIVE_THRESH_MEAN_C,
#                                         cv2.THRESH_BINARY,
#                                         block_size,
#                                         subtract_from_mean, )
#         return img_bin
#
#     def _adjust_save_png(img, i, sheetStem):
#         spn = d.adjustedPngs / f'{sheetStem}-{i}.png'
#         img, ang = process_image(img)
#         cv2.imwrite(str(spn), img)
#         return img, ang
#
#     def _ocr_img_save_its_data(img, i, sheetStem):
#         PyData = pytesseract.image_to_data(img,
#                                          lang=langs,
#                                          output_type=Output.DICT)
#         pkl_pn = d.adjustedPngs / f'{sheetStem}-{i}.pkl'
#         with open(pkl_pn, 'wb') as file:
#             pickle.dump(PyData, file, protocol=pickle.HIGHEST_PROTOCOL)
#         return PyData
#
#     def _check_stocks_pages_continuity(stock_pages_list):
#         min_i = min(stock_pages_list)
#         max_i = max(stock_pages_list)
#         helper = list(range(min_i, max_i + 1))
#         if not helper == stock_pages_list:
#             raise Exception('not successive page nums')
#
#     def _check_and_fix_table_from_camelot(df):
#         for coln in df.columns:
#             df[coln] = df[coln].str.replace(r'^\s*$', '-')
#
#         df = _find_and_filter_first_row_with_at_least_4_number_cells(df)
#         df = _fix_int_columns(df)
#         df = _fix_name_column(df)
#         df = _fix_percent_column(df)
#
#         return df
#
#     def _find_and_filter_first_row_with_at_least_4_number_cells(df):
#         lc0 = df.applymap(_is_this_a_number)
#         lc1 = lc0.sum(axis=1) > 4
#         return df.loc[lc1]
#
#     def _fix_int_columns(df):
#         for col in range(1, 12):
#             df[col] = df[col].str.replace(r',', '')
#             df[col] = df[col].str.replace(r'\D', '')
#         return df
#
#     def _fix_name_column(df):
#         df[12] = df[12].apply(lambda x: str(x)[::-1])
#         return df
#
#     def _fix_percent_column(df):
#         df[0] = df[0].str.replace('%', '')
#         df[0] = df[0].str.replace('/', '.')
#         df[0] = df[0].str.replace(r'[^\d\.]', '')
#         return df
#
#     def _is_this_a_number(istr):
#         istr = istr.replace(',', '')
#         if re.fullmatch(r'\d+', istr):
#             return True
#         else:
#             return False
#
#     def _save_text(text, filepath):
#         with open(filepath, 'w', encoding='utf-8') as file:
#             file.write(text)
#
#     def _ex_img_data(img):
#         dta = pytesseract.image_to_data(img, lang=langs)
#         return dta
#
#     ##
#     import pdftotext
#     from pdfminer.pdfparser import PDFParser
#     from pdfminer.converter import TextConverter
#     from pdfminer.layout import LAParams
#     from pdfminer.pdfdocument import PDFDocument
#     from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
#     from pdfminer.pdfpage import PDFPage
#
#     fp = '/Users/mahdimir/Documents/Projects/TSE-MutualFunds
#     -MonthlyPortfolio/fins/PdfAdj/573583-0.pdf'
#
#     pdfminer_string = StringIO()
#     with open(fp, "rb") as in_file:
#         parser = PDFParser(in_file)
#         doc = PDFDocument(parser)
#         rsrcmgr = PDFResourceManager()
#         device = TextConverter(rsrcmgr, pdfminer_string, laparams=LAParams())
#         interpreter = PDFPageInterpreter(rsrcmgr, device)
#         for page in PDFPage.create_pages(doc):
#             interpreter.process_page(page)
#     pdfminer_lines = pdfminer_string.getvalue().splitlines()
#     pdfminer_lines = [ln for ln in pdfminer_lines if ln]
#
#     with open(fp, 'rb') as file:
#         pdftotext_string = pdftotext.pdf(file)
#     pdftotext_lines = ("\n\n".join(pdftotext_string).splitlines())
#     pdftotext_lines = [ln for ln in pdftotext_lines if ln]
#
#     ##
#
#     def _get_rot_angle_and_text_of_imgs(self):
#         for i, img in enumerate(self.imgs):
#             img, ang = process_image(img)
#             self.rot_angles.append(ang)
#
#             txt_1 = _extract_txt_from_img(img)
#             self.texts_1.append(txt_1)
#
#     def _adjust_pdf_save(self):
#         self.adj_pdf_pn = _rotate_pages_of_pdf(self.fp, self.rot_angles)
#         self._print('pdf rotated and saved to :', self.adj_pdf_pn)
#
#     def _set_stock_pages(self):
#         self.stocks_pages = _find_stocks_pages(self.texts_1)
#         self._set_min_non_stocks_page()
#         self._set_stocks_pages_final()
#
#     def _set_min_non_stocks_page(self):
#         self.max_stocks_pg = max(self.stocks_pages)
#         if self.max_stocks_pg == self.pgs_cou:
#             self.min_non_stock_pg = self.pgs_cou
#             self._print('all pages had detected as common_stocks table')
#         else:
#             self.min_non_stock_pg = _find_first_non_stocks_page(self.texts_1,
#                                                                 self.max_stocks_pg)
#             if self.min_non_stock_pg is None:
#                 self.min_non_stock_pg = self.pgs_cou
#                 self._print(
#                         'non-stock pg is not found and it is set to nof of
#                         pgs')
#             else:
#                 self._print('first non-stock is:', self.min_non_stock_pg)
#
#     def _set_stocks_pages_final(self):
#         self.stocks_pages_final = list(range(min(self.stocks_pages),
#                                              self.min_non_stock_pg))
#         self._print('common_stocks portfo pages are:', self.stocks_pages_final)
#
#     def _read_table(self):
#         self.stocks_dfs, self.tables_pars_reps = _read_tables_by_camelot(
#         self.adj_pdf_pn,
#                                                                          self.stocks_pages_final,
#                                                                          self.sheetStem)
#
#     def _save_tbls(self):
#         for i, df in enumerate(self.stocks_dfs):
#             bn = f'{self.sheetStem}-{i}.xlsx'
#             fpn = d.pdfsTables / bn
#             cf.save_df_to_a_nice_xl(df, fpn)
#             self._print('the common_stocks fins save as ', bn)
#             self.tables_founded_num += 1
#
#     def process(self):
#         try:
#             self._convert_pdf_2_imgs()
#             self._get_rot_angle_and_text_of_imgs()
#             self._adjust_pdf_save()
#             self._set_stock_pages()
#             self._read_table()
#             self._save_tbls()
#         except Exception as err:
#             self._print(err)
#             self.err = str(err)
#
#     def get_attrs(self):
#         outdf = {
#                 cac.pdfPgsNum         : self.pgs_cou,
#                 cac.pdfStocksPgs      : str(self.stocks_pages_final),
#                 cac.pdfTablesFounedNum: self.tables_founded_num,
#                 cac.pdfProcessErr     : self.err,
#                 cac.pdfTablesParsReps : str(self.tables_pars_reps)}
#         self._print(outdf)
#         return outdf
#
#     def remove_noise_and_smooth(img_path):
#         img = cv2.imread(img_path)
#         img = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
#
#         kernel = np.ones((1, 1), np.uint8)
#         opening = cv2.morphologyEx(img, cv2.MORPH_OPEN, kernel)
#         closing = cv2.morphologyEx(opening, cv2.MORPH_CLOSE, kernel)
#         img = cv2.bitwise_or(img, closing)
#         return img
#
#     def fix_rotation(img):
#         rotated_img = img
#         # osd: orientation and script detection
#         tess_data = pytesseract.image_to_osd(img, nice=1)
#         angle = int(re.search(r"(?<=Rotate: )\d+", tess_data).group(0))
#         # print("angle: " + str(angle))
#
#         if angle != 0 and angle != 360:
#             (h, w) = img.shape[:2]
#             center = (w / 2, h / 2)
#
#             # Perform the rotation
#             rotation_mat = cv2.getRotationMatrix2D(center, -angle, 1.0)
#
#             # Fixing the image cut-off by calculating the new center
#             abs_cos = abs(rotation_mat[0, 0])
#             abs_sin = abs(rotation_mat[0, 1])
#
#             bound_w = int(h * abs_sin + w * abs_cos)
#             bound_h = int(h * abs_cos + w * abs_sin)
#
#             rotation_mat[0, 2] += bound_w / 2 - center[0]
#             rotation_mat[1, 2] += bound_h / 2 - center[1]
#
#             rotated_img = cv2.warpAffine(img, rotation_mat, (bound_w,
#             bound_h))
#
#         return rotated_img, angle
#
#     def _extract_txt_from_img(img):
#         txt = str(pytesseract.image_to_string(img, lang=langs, config=''))
#         txt = os.linesep.join([s for s in txt.splitlines() if s.strip()])
#         return txt
#
#     def _find_stocks_pages(imgs_texts):
#         outdf = []
#         first_pg_found = False
#         for i, txt in enumerate(imgs_texts):
#             if _is_stocks_portfolio(txt):
#                 outdf.append(i)
#                 first_pg_found = True
#             elif first_pg_found:
#                 return outdf
#
#     def _is_stocks_portfolio(txt):
#         for fu in [_find_stocks_page_1, ]:
#             lo = fu(txt)
#             if lo:
#                 return lo
#
#     exst0 = [r'[دذ][رز][آا][دذ]م', r'[اآ]او[رز]ق', 'سود']
#     exst0 = [r'\b' + re.escape(x) + r'\b' for x in exst0]
#
#     inc_list = ['سرمایه گذاری در سهام و حق تقد', '1-1-سرمایه', 'تعداد',
#             'بهای تمام شده']
#     inc_list = [r'\b' + re.escape(x) + r'\b' for x in inc_list]
#
#     def _find_stocks_page_1(txt):
#         # excluding items
#         for el in exst0:
#             if re.search(el, txt):
#                 return None
#
#         # including items
#         for el in inc_list:
#             if re.search(el, txt):
#                 return True
#
#     def _find_first_non_stocks_page(imgs_texts, max_stocks_page):
#         other_txts = imgs_texts[max_stocks_page + 1:]
#         for i, txt in enumerate(other_txts):
#             lo = _is_first_non_stocks_page(txt)
#             if lo:
#                 return i + max_stocks_page + 1
#
#     nsp = ['اطلاعات آماری', 'اوراق بهادار با درآمد ثابت', '1-2-سرمایه',
#            'تاریخ سر رسید']
#     nsp = [r'\b' + re.escape(x) + r'\b' for x in nsp]
#
#     def _is_first_non_stocks_page(txt):
#         for el in nsp:
#             if re.search(el, txt):
#                 return True
#
#     def _rotate_pages_of_pdf(pdf_pn, angles):
#         pdf_file = open(pdf_pn, 'rb')
#         pdf_r = PdfFileReader(pdf_file)
#         pdf_w = PdfFileWriter()
#         for i, ang in enumerate(angles):
#             pg = pdf_r.getPage(i)
#             pg.rotateClockwise(ang)
#             pdf_w.addPage(pg)
#         pdf_out_pn = d.PdfAdj / pdf_pn.name
#         with open(pdf_out_pn, 'wb') as file:
#             pdf_w.write(file)
#         return pdf_out_pn
#
#     def _read_tables_by_camelot(pdf_pn, pages_list_counting_from_1, pdf_stem):
#         pages_str = ",".join([str(x + 1) for x in pages_list_counting_from_1])
#         tables = camelot.read_pdf(str(pdf_pn), flavor='stream',
#         pages=pages_str)
#         dfs = []
#         dfs_pars_rep = []
#         for i, tbl in enumerate(tables):
#             ldf = tbl.df
#             ldf = ldf.applymap(str)
#             dfs.append(ldf)
#             print(pdf_stem, tbl.parsing_report)
#             dfs_pars_rep.append(tbl.parsing_report)
#             fp = d.pdfsTables / f'{pdf_stem}-{i}.pkl'
#             _save_pkl(tbl.parsing_report, fp)
#         return dfs, dfs_pars_rep
#
#     def _is_pdf_downloaded(idf):
#         idf[cac.isPdfDownloaded] = idf[cac.filesStem].apply(lambda x: (
#                 d.pdf / f'{x}.pdf').exists())
#         return idf
#
#     def _save_pkl(PyData, pn):
#         with open(pn, 'wb') as file:
#             pickle.dump(PyData, file)
#
#     if os.name in ['Windows', 'nt']:
#         print(os.name)
#         pytesseract.pytesseract.tesseract_cmd = 'C:\\Program
#         Files\\Tesseract-OCR\\tesseract.exe'
#
#     def process_image(img):
#         temp_filename = resize_image(img)
#         img = remove_noise_and_smooth(temp_filename)
#         img, angle = fix_rotation(img)
#         return img, angle
#
#     def resize_image(img):
#         length_x, width_y = img.size
#         factor = max(1, int(1800 / length_x))  # 1800 for tesserect
#         size = factor * length_x, factor * width_y
#         im_resized = img.resize(size, Image.ANTIALIAS)
#
#         import tempfile
#
#         temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".TIFF")
#         temp_filename = temp_file.name
#         im_resized.save(temp_filename, dpi=(300, 300))  # best for OCR
#         return temp_filename
#
# langs = "fas"  # Languages for OCR eng+fas
#
# fp = r"C:\Users\RA_Mir\Downloads\invoice-date-box.jpg"
# print(pytesseract.image_to_osd(fp))
#
# ##
# fp = '/Users/mahdimir/Documents/Projects/TSE-MutualFunds-MonthlyPortfolio
# /fins/pdf/532568-0.pdf'
# tables = camelot.read_pdf(fp, flavor='stream', pages='2,3', encoding='utf-8')
# dfs = tabula.read_pdf(fp, pages=[2, 3], stream=True)
def backup_dir_on_dropbox(the_dir , cwd) :
  the_dir = pathlib.Path(the_dir)
  _dir = '/Users/mahdimir/Dropbox/Code_Backup/'
  dropbox_codes_backup_dir = pathlib.Path(_dir)
  if dropbox_codes_backup_dir.exists() :
    new_dir = dropbox_codes_backup_dir / pathlib.Path(cwd).name
    if new_dir.exists() :
      shutil.rmtree(new_dir)
    shutil.copytree(the_dir , new_dir)
    print(the_dir.name , 'Backed up on Mahdi`s dropbox.')

backup_dir_on_dropbox(d.code , d.cwd)

def add_this_pair_to_key_name_xl(key: str , company_name: str , exchange: str) :
  kyn = pd.read_excel(cte.key_name_fp , engine='openpyxl')
  ldc = {
      knc.key         : key ,
      knc.exchange    : exchange ,
      knc.companyName : company_name ,
      }
  kyn = kyn.append(ldc , ignore_index=True)
  save_df_as_a_nice_xl(kyn , cte.key_name_fp)

def similar(a , b) :
  return SequenceMatcher(None , a , b).ratio()

def find_alikes(name , ky_name) :
  name_wos = str(name).replace(' ' , '')
  lsym = ky_name.copy()
  _col = knc.companyName_wos
  _ncol = _col + '_sim'
  lsym[_ncol] = lsym[_col].apply(lambda x : similar(x , name_wos) > 0.8)
  odf = lsym[lsym[_ncol]]
  odf = odf[[knc.key , ctc.companyName , knc.exchange]]
  odf = odf.drop_duplicates(subset=knc.key)
  odf = odf.reset_index(drop=True)
  return odf

def manually_add_com_key(df , from_which_col) :
  lc = mask_without_key_rows(df)
  print(len(lc[lc]))

  astns = list(df.loc[lc , from_which_col])
  astns = sorted(astns , key=astns.count , reverse=True)
  astns = list(set(astns))  # to get unique keys

  for asset in astns :
    ky_name = pd.read_excel(cte.key_name_fp , engine='openpyxl')
    ky_name = define_wos_cols(ky_name , [knc.companyName])

    print(asset)
    alikes = find_alikes(str(asset) , ky_name)
    print(alikes)

    inp = input('br - sk - number - key')
    if inp == 'br' :
      break
    elif inp == 'sk' :
      continue
    elif inp in alikes.index.astype(str) :
      _key = alikes.at[int(inp) , knc.key]
      _exc = alikes.at[int(inp) , knc.exchange]
      print(_key)
      print(_exc)
      add_this_pair_to_key_name_xl(_key , asset , _exc)
    else :
      msk = ky_name[knc.key].eq(inp)
      msk = msk[msk]
      if len(msk) > 0 :
        _ind = msk.index[0]
        exch = ky_name.at[_ind , knc.exchange]
        print(exch)
      else :
        inp1 = input('br - exchange Name - None')
        if inp1 == 'br' :
          break
        exch = inp1
      add_this_pair_to_key_name_xl(inp , asset , exch)
    print('next')

def unify_attrs_of_the_same_key(idf , key_col: str , which_cols: list) :
  idf = idf.reset_index(drop=True)
  gps = idf.groupby(key_col)
  cou = gps[which_cols].nunique()
  if not cou.le(1).all(axis=None) :
    print('Not unique value')
    msk = cou[which_cols].gt(1)
    return cou[msk].dropna(how='all')
  else :
    func = lambda x : x.fillna(method='ffill').fillna(method='bfill')
    for col in which_cols :
      idf[col] = gps[col].apply(func)
  return idf
