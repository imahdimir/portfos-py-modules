"""[]

  """

# prtfos venv

##

import re
from copy import copy
from pathlib import PurePath , Path

import openpyxl as pyxl
import pandas as pd
from openpyxl.utils.dataframe import dataframe_to_rows
from persiantools import characters
from persiantools import digits

fp = Path('/Users/mahdimir/Dropbox/keyData/tickerMarket/tickerMarket.xlsx')
merge_fp = fp.parent / 'merge.xlsx'

markets_fp = Path(
    '/Users/mahdimir/Dropbox/keyData/MarketKeysNames/MarketKeysNames.xlsx')

class Cols :
  def __init__(self) :
    self.jdate = None
    self.ticker = None
    self.marketKey = None
    self.helper = None
    self.isDateXTickerUnique = None
    self.isMarketKeyValid = None

    for key , val in self.__dict__.items() :
      if val is None :
        self.__dict__[key] = key

cols = Cols()

def apply_func_on_notna_rows_of_cols(idf , cols , func) :
  for col in cols :
    _ms = idf[col].notna()
    idf.loc[_ms , col] = idf.loc[_ms , col].apply(func)
  return idf

def normalize_str(ist: str) :
  os = characters.ar_to_fa(str(ist))
  os = digits.ar_to_fa(os)
  os = digits.fa_to_en(os)
  os = os.strip()
  replce = {
      (r"\u202b" , None)          : ' ' ,
      (r'\u200c' , None)          : ' ' ,
      (r'\u200d' , None)          : '' ,
      (r'ء' , None)               : ' ' ,
      (r':' , None)               : ' ' ,
      (r'\s+' , None)             : ' ' ,
      (r'آ' , None)               : 'ا' ,
      (r'أ' , None)               : 'ا' ,
      (r'ئ' , None)               : 'ی' ,
      (r'\bETF\b' , None)         : '' ,
      (r'\(سهامی\s*عام\)' , None) : '' ,
      (r'\bسهامی\s*عام\b' , None) : '' ,
      (r'[\(\)]' , None)          : '' ,
      (r'\.+$' , None)            : '' ,
      (r'^\.+' , r'^\.+\d+$')     : '' ,
      }

  os = replace_regex_with_care(os , replce)

  return os

def define_wos_cols(df , which_cols: list) :
  for col in which_cols :
    msk = df[col].notna()
    ncol = col + '_wos'
    _pre = df.loc[msk , col]
    df.loc[msk , ncol] = _pre.str.replace(r'\s' , '' , regex=True)
  return df

def replace_regex_with_care(ist: str , replace: dict) :
  """
  (regex, not this) : replace value
  """
  os = str(ist)

  for ky , val in replace.items() :
    if ky[1] is not None :
      if not re.match(ky[1] , os) :
        os = re.sub(ky[0] , val , os)
    else :
      os = re.sub(ky[0] , val , os)

  os = os.strip()

  return os

def make_styles_alike(ws0 , ws1) :
  ws1.conditional_formatting = ws0.conditional_formatting

  for col in ws0.columns :
    wid = ws0.column_dimensions[col[0].column_letter].width
    ws1.column_dimensions[col[0].column_letter].width = wid

  for _col1 , _col0 in zip(ws1.columns , ws0.columns) :
    _col1[0].style = _col0[0].style
    _col1[0].font = copy(_col0[0].font)

  for col in ws1.columns :
    col_1st_cell_add = col[0].column_letter + '2'
    _add = col_1st_cell_add
    _style = ws0[_add].style
    _font = copy(ws0[_add].font)
    _num_format = ws0[_add].number_format

    for cell in col[1 :] :
      cell.style = _style
      cell.font = _font
      cell.number_format = _num_format

  ws1.freeze_panes = 'A2'

  return ws1

def main() :
  pass

  ##
  wb0 = pyxl.load_workbook(fp)

  ##
  ws0 = wb0.active

  ##
  init_cols = [x[0].value for x in ws0.columns]

  ##
  df = pd.DataFrame(ws0.values , columns=init_cols)
  df = df.iloc[1 : , :]

  ##
  wb0.close()

  ##
  main_cols = [cols.jdate , cols.ticker , cols.marketKey]

  ##
  def _merge_with_merge_excel(idf) :
    if not merge_fp.exists() :
      return idf

    print(len(idf))

    _mdf = pd.read_excel(merge_fp , engine='openpyxl')

    _mdf_frst2cols = _mdf.columns[: len(main_cols)]
    assert list(_mdf_frst2cols) == main_cols , 'merge cols are not correct.'

    idf = pd.concat([idf , _mdf] , ignore_index=True)

    print(len(idf))
    return idf

  df = _merge_with_merge_excel(df)

  ##
  def _drop_nan_or_blank_rows(idf) :
    print(len(idf))

    _msk = idf[main_cols].isna().any(axis=1)
    _msk |= idf[main_cols].eq('').any(axis=1)

    _df = idf[_msk]
    idf = idf[~ _msk]

    print(len(idf))
    return idf , _df

  df , df1 = _drop_nan_or_blank_rows(df)

  ##
  def _remove_ex_dups(idf) :
    """
    remove exact duplicates
    """
    idf = apply_func_on_notna_rows_of_cols(idf , main_cols , normalize_str)

    idf = define_wos_cols(idf , main_cols)

    print(len(idf))
    idf = idf.drop_duplicates(subset=[x + '_wos' for x in main_cols])
    print(len(idf))
    return idf

  df = _remove_ex_dups(df)

  ##
  def _assert_uniquness_of_jdate_x_ticker(idf) :
    _jdxti = [cols.jdate , cols.ticker]
    _df = idf[idf[_jdxti].duplicated(keep=False)]
    assert len(_df) == 0 , print(_df)

  _assert_uniquness_of_jdate_x_ticker(df)

  ##
  df[cols.jdate] = df[cols.jdate].astype(int)

  ##
  df = df.sort_values(by=[cols.jdate , cols.marketKey , cols.ticker])

  ##
  fnds_1 = df[init_cols]

  ##
  df = fnds_1

  ##
  wb1 = pyxl.Workbook()
  ws1 = wb1.active

  ##
  df = df.fillna(value='')

  for r in dataframe_to_rows(df , index=False , header=True) :
    ws1.append(r)

  ##
  # =A2&B2
  for _i in range(2 , len(df) + 2) :
    val = f'=A{_i}&B{_i}'
    ws1[f'D{_i}'] = val

  ##
  # =AND(COUNTIF(D:D,D2)=1,A2<>"",B2<>"")
  for _i in range(2 , len(df) + 2) :
    val = f'=AND(COUNTIF(D:D,D{_i})=1,A{_i}<>"", B{_i}<>"")'
    ws1[f'E{_i}'] = val

  ##
  mkts = pd.read_excel(markets_fp , engine='openpyxl')
  mkkeys = mkts[cols.marketKey]

  ##
  # =OR(C2="IEE",C2="TSE",C2="IME",C2="IFB")
  for _i in range(2 , len(df) + 2) :
    val = f'=OR('
    for _el in mkkeys :
      val += f'C{_i}=\"{_el}\",'
    val += ')'

    _add = f'F{_i}'
    ws1[f'F{_i}'] = val

  ##
  ws1 = make_styles_alike(ws0 , ws1)

  ##
  wb1.save(fp)

  ##
  wb1.close()

##


if __name__ == "__main__" :
  main()
  print(f'{PurePath(__file__).name} Done.')

def _t() :
  pass

  ##
  pt = r'.+\b' + r'ثابت' + r'$'
  x = 'اتحاد ثابت'
  re.fullmatch(pt , x)

  ##  ##
