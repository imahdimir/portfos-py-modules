"""[]

    """

##

import warnings
import re
from pathlib import Path

import pandas as pd

from _2 import any_of_wordlist_isin_str
from _2 import normalize_str
from _8 import scp
from _9 import psc
from Helper.name_space import cte , asst

warnings.filterwarnings("ignore")

Module_name = '_12.py'

tickers_fp = Path(
    '/Users/mahdimir/Dropbox/keyData/tickerMarket/tickerMarket.xlsx')

class AllExcelNewCols :
  def __init__(
      self
      ) :
    self.asset_name_c = psc.asset_name + '_cln'

    self.start_shares_c = psc.start_shares + '_cln'
    self.start_purch_cost_c = psc.start_purchase_cost + '_cln'
    self.start_net_sale_val_c = psc.start_net_sale_val + '_cln'

    self.change_buy_shares_c = psc.change_buy_shares + '_cln'
    self.change_buy_cost_c = psc.change_buy_cost + '_cln'
    self.change_sell_shares_c = psc.change_sell_shares + '_cln'
    self.change_sell_revenue_c = psc.change_sell_revenue + '_cln'

    self.end_shares_c = psc.end_shares + '_cln'
    self.end_asset_price_c = psc.end_asset_price + '_cln'
    self.end_purchase_cost_c = psc.end_purchase_cost + '_cln'
    self.end_net_sale_val_c = psc.end_net_sale_val + '_cln'
    self.end_pct_to_all_assets_c = psc.end_pct_to_all_assets + '_cln'

    self.asset_key = None
    self.ast_type = None
    self.exp_date = None
    self.strike_price = None

    for key , val in self.__dict__.items() :
      if val is None :
        self.__dict__[key] = key

aenc = AllExcelNewCols()

def replace_regex_with_care(
    ist: str ,
    replace: dict
    ) :
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

def main() :
  pass

  ##
  df = pd.read_parquet(cte.allXlsStocksSheets)

  ##
  def _replace_str_str_with_none(
      idf
      ) :
    idf = idf.replace({
        'nan' : None
        })

    return idf

  df = _replace_str_str_with_none(df)

  ##
  def _drop_nan_asset_names(
      idf
      ) :
    print(len(idf))

    idf = idf.dropna(subset = psc.asset_name)
    print(len(idf))
    return idf

  df = _drop_nan_asset_names(df)

  ##
  def _def_clean_name_col(
      idf
      ) :
    """

    """
    if aenc.asset_name_c not in idf.columns :
      idf.insert(4 , aenc.asset_name_c , None)

    idf[aenc.asset_name_c] = idf[psc.asset_name].apply(normalize_str)

    return idf

  df = _def_clean_name_col(df)

  ##
  def _drop_header_rows(
      idf
      ) :
    """

    """
    print(len(idf))

    _fu = lambda \
        x : any_of_wordlist_isin_str(x , scp.firms)
    _msk = ~ idf[aenc.asset_name_c].apply(_fu)

    idf = idf[_msk]

    print(len(idf))

    return idf

  df = _drop_header_rows(df)

  ##
  def _drop_not_a_asset_name_rows(
      idf
      ) :
    """
    remove_rows_with_not_eligible_asset_name

    """

    print(len(idf))

    _col = aenc.asset_name_c
    _umsk = idf[_col].apply(lambda
                              x : False)

    _ptrns = {
        (r'^' + r'شرکت' + r'$' , None)                         : None ,
        (r'^' + r'جمع' + r'\b' , None)                         : None ,
        (r'^' + r'نقل' + r'\b' , None)                         : None ,
        (r'^' + r'سایر' + r'$' , None)                         : None ,
        (r'^' + r'دارایی' + r'$' , None)                       : None ,
        (r'^' + r'AssetName' + r'$' , None)                    : None ,
        (r'\b' + r'اوراق بهادار با درامد ثابت' + r'\b' , None) : None ,
        (r'\^' + r'برای ماه منتهی به' + r'\b' , None)          : None ,
        (r'\b' + r'افزایش سرمایه' + r'\b' , None)              : None ,
        }

    _df1 = idf.copy()

    for _ky , _val in _ptrns.items() :
      _msk = idf[_col].str.contains(_ky[0])
      if _ky[1] is not None :
        _msk &= ~ idf[_col].str.contains(_ky[1])

      _umsk |= _msk

      print('len:' , len(_msk[_msk]))

      idf = idf[~ _msk]

    _df = _df1[_umsk]
    print(len(_df))

    print(len(idf))

    return idf , _df

  df , df00 = _drop_not_a_asset_name_rows(df)

  ##
  def _find_preferred_shares(
      idf
      ) :

    if aenc.ast_type not in idf.columns :
      idf.insert(5 , aenc.ast_type , None)

    _ptrns = {
        (r'^' + r'ح' + r'\.?' + r'\b' , None) : '' ,
        (r'\b' + r'حق تقدم' + r'\b' , None)   : '' ,
        (r'تقدم' + r'$' , None)               : '' ,
        (r'^حق\b' , None)                     : '' ,
        (r'\bح ت\b' , None)                   : '' ,
        }

    _col = aenc.asset_name_c

    _umsk = idf[_col].apply(lambda
                              x : False)

    for _ky , _val in _ptrns.items() :
      _msk = idf[_col].str.contains(_ky[0])
      if _ky[1] is not None :
        _msk &= ~ idf[_col].str.contains(_ky[1])

      _msk = _msk.fillna(False)
      print('len:' , len(_msk[_msk]))
      _umsk |= _msk

      idf.loc[_msk , _col] = idf[_col].str.replace(_ky[0] , _val)

      idf.loc[_msk , aenc.ast_type] = asst.preferred_shares

    idf[_col] = idf[_col].apply(normalize_str)

    _df = idf[_umsk]

    return idf , _df

  df , df0 = _find_preferred_shares(df)

  ##
  ticks = pd.read_excel(tickers_fp , engine = 'openpyxl')

  ##
  ticks = ticks.sort_values(by = ['jdate'])

  ##
  all_tickers = ticks['ticker'].drop_duplicates()

  ##
  def _find_pref_shares_1(
      idf
      ) :
    """
    finds asset names that ends with 'ح' & also the asset name without its last
    char (ح) is a valid ticker in the market.
    e.g.: امینح is the prefereed share of امین which is a valid ticker.
    """

    _ptrns = {
        (r'.+ح$' , None) : '' ,
        }

    _col = aenc.asset_name_c

    for _ky , _val in _ptrns.items() :
      _msk = idf[_col].str.fullmatch(_ky[0])
      if _ky[1] is not None :
        _msk &= ~ idf[_col].str.contains(_ky[1])

      _msk &= idf[_col].str[:-1].isin(all_tickers)

      print(len(_msk[_msk]))

      idf.loc[_msk , aenc.ast_type] = asst.preferred_shares

      idf.loc[_msk , _col] = idf[_col].str[:-1]

    return idf

  df = _find_pref_shares_1(df)

  ##
  def _find_call(
      idf
      ) :
    """

    """

    _pt0 = r'^' + r'اختیار' + r'\s?' + r'خ' + r'[\s\.]?' + r'تبعی' + r'\.?' + r'\b'
    _pt1 = r'^' + r'اختیار' + r'\s?' + r'خ' + r'[\s\.]?' + r'ت' + r'\.?' + r'\b'
    _pt2 = r'^' + r'اختیار' + r'\s?' + r'خرید' + r'[\s\.]?' + r'تبعی' + r'\.?' + r'\b'
    _pt3 = r'^' + r'اختیار' + r'\s?' + r'خرید' + r'[\s\.]?' + r'ت' + r'\.?' + r'\b'
    _pt4 = r'^' + r'اختیار' + r'\s?' + r'خ' + r'\.?' + r'\b'
    _pt5 = r'^' + r'اختیار' + r'\s?' + r'خرید' + r'\.?' + r'\b'

    _ptrns = {
        (_pt0 , None) : '' ,
        (_pt1 , None) : '' ,
        (_pt2 , None) : '' ,
        (_pt3 , None) : '' ,
        (_pt4 , None) : '' ,
        (_pt5 , None) : '' ,
        }

    _col = aenc.asset_name_c

    _umsk = idf[_col].apply(lambda
                              x : False)

    for _ky , _val in _ptrns.items() :
      _msk = idf[_col].str.match(_ky[0])
      if _ky[1] is not None :
        _msk &= ~ idf[_col].str.contains(_ky[1])

      print('len:' , len(_msk[_msk]))
      _umsk |= _msk

      idf.loc[_msk , _col] = idf[_col].str.replace(_ky[0] , _val)

      idf.loc[_msk , aenc.ast_type] = asst.call

    idf[_col] = idf[_col].apply(normalize_str)

    _df = idf[_umsk]

    return idf , _df

  df , df1 = _find_call(df)

  ##
  def _find_put(
      idf
      ) :
    """

    """

    _pt0 = r'^' + r'اختیار' + r'[\s\.]?' + r'ف' + r'[\s\.]?' + r'تبعی' + r'\.?' + r'\b'
    _pt1 = r'^' + r'اختیار' + r'[\s\.]?' + r'ف' + r'[\s\.]?' + r'ت' + r'\.?' + r'\b'
    _pt2 = r'^' + r'اختیار' + r'[\s\.]?' + r'فروش' + r'[\s\.]?' + r'تبعی' + r'\.?' + r'\b'
    _pt3 = r'^' + r'اختیار' + r'[\s\.]?' + r'فروش' + r'[\s\.]?' + r'ت' + r'\.?' + r'\b'
    _pt4 = r'^' + r'اختیار' + r'[\s\.]?' + r'ف' + r'\.?' + r'\b'
    _pt5 = r'^' + r'اختیار' + r'[\s\.]?' + r'فروش' + r'\.?' + r'\b'

    _ptrns = {
        (_pt0 , None) : '' ,
        (_pt1 , None) : '' ,
        (_pt2 , None) : '' ,
        (_pt3 , None) : '' ,
        (_pt4 , None) : '' ,
        (_pt5 , None) : '' ,
        }

    _col = aenc.asset_name_c
    _umsk = idf[_col].apply(lambda
                              x : False)

    for _ky , _val in _ptrns.items() :
      _msk = idf[_col].str.contains(_ky[0])
      if _ky[1] is not None :
        _msk &= ~ idf[_col].str.contains(_ky[1])

      _umsk |= _msk

      print('len:' , len(_msk[_msk]))

      idf.loc[_msk , _col] = idf[_col].str.replace(_ky[0] , _val)

      idf.loc[_msk , aenc.ast_type] = asst.call

    idf[_col] = idf[_col].apply(normalize_str)

    _df = idf[_umsk]

    return idf , _df

  df , df2 = _find_put(df)

  ##
  def _split_exp_date_and_strike_price(
      idf
      ) :
    """

    """
    _col = aenc.asset_name_c

    _ncol0 = aenc.strike_price
    if _ncol0 not in idf.columns :
      idf.insert(6 , _ncol0 , None)

    _ncol1 = aenc.exp_date
    if _ncol1 not in idf.columns :
      idf.insert(7 , _ncol1 , None)

    _msk = idf[aenc.ast_type].isin([asst.put , asst.call])

    _ptrn0 = r'\D{2,}' + r'-' + r'[1-9]\d+' + r'-' + r'1[3-4]\d{2}/?[0-1]\d/?[0-3]\d'
    _ptrn1 = r'\D{2,}' + r'-' + r'[1-9]\d+' + r'-' + r'\d{2}/?[0-1]\d/?[0-3]\d'

    mskk1 = idf[_col].str.fullmatch(_ptrn0)
    mskk1 |= idf[_col].str.fullmatch(_ptrn1)

    _msk &= mskk1

    helper = idf.loc[_msk , _col].str.split('-' , expand = True)

    colss = [_col , _ncol0 , _ncol1]
    for _i , coll in enumerate(colss) :
      idf.loc[_msk , coll] = helper[_i]

    _df = idf[_msk]

    return idf , _df

  df , df3 = _split_exp_date_and_strike_price(df)

  ##
  def _fix_exp_date(
      idf
      ) :
    """

    """
    _ptrn0 = r'1[3-4]\d{2}/?[0-1]\d/?[0-3]\d'

    _col = aenc.exp_date

    _msk = idf[_col].str.fullmatch(_ptrn0)
    _msk = _msk.fillna(False)

    idf.loc[_msk , _col] = idf.loc[_msk , _col].str.replace('/' , '')

    _ptrn1 = r'\d{2}/?[0-1]\d/?[0-3]\d'

    _msk1 = idf[_col].str.fullmatch(_ptrn1)
    _msk1 = _msk1.fillna(False)

    idf.loc[_msk1 , _col] = idf.loc[_msk1 , _col].str.replace('/' , '')

    _fu = lambda \
        x : '14' + x if int(x[0 :2]) < 60 else '13' + x
    idf.loc[_msk1 , _col] = idf.loc[_msk1 , _col].apply(_fu)

    _msk2 = _msk | _msk1
    idf.loc[~ _msk2 , _col] = None

    _df = idf[_msk2]

    return idf , _df

  df , df3 = _fix_exp_date(df)

  ##
  def _drop_put_call_without_date_or_price(
      idf
      ) :
    """

    """
    _msk = idf[aenc.ast_type].isin([asst.put , asst.call])

    _msk1 = idf[aenc.strike_price].isna()
    _msk1 |= idf[aenc.exp_date].isna()

    _msk &= _msk1

    _df = idf[_msk]
    print(len(_df))

    idf = idf[~ _msk]

    return idf , _df

  df , df4 = _drop_put_call_without_date_or_price(df)

  ##
  def _find_sekke(
      idf
      ) :
    """

    """
    if aenc.asset_key not in idf.columns :
      idf.insert(8 , aenc.asset_key , None)

    _pt0 = r'^' + 'سکه تمام'
    _pt1 = r'^' + 'تمام سکه'

    _col = aenc.asset_name_c

    _msk = idf[_col].str.contains(_pt0)
    _msk |= idf[_col].str.contains(_pt1)

    idf.loc[_msk , aenc.ast_type] = asst.sekke
    idf.loc[_msk , aenc.asset_key] = asst.sekke

    _df = idf[_msk]
    print(len(_df))

    return idf , _df

  df , df5 = _find_sekke(df)

  ##
  def _drop_only_number_rows(
      idf
      ) :
    """

    """
    _col = aenc.asset_name_c

    _ptr = r'-?\d*\.?\d+'

    _msk = idf[_col].str.fullmatch(_ptr)

    _df = idf[_msk]
    print(len(_df))

    idf = idf[~ _msk]

    return idf , _df

  df , df6 = _drop_only_number_rows(df)

  ##
  def _find_akhza(
      idf
      ) :
    """

    """
    _ptr = r'\b' + r'اسناد' + r'\s?' + r'خزانه' + r'\b'

    _msk = idf[aenc.asset_name_c].str.contains(_ptr)

    idf.loc[_msk , aenc.ast_type] = asst.akhza
    idf.loc[_msk , aenc.asset_key] = asst.akhza

    _df = idf[_msk]
    print(len(_df))

    return idf , _df

  df , df7 = _find_akhza(df)

  ##
  def _find_ejare(
      idf
      ) :
    """

    """
    _ptr = r'^' + r'اجاره' + r'\b'

    _msk = idf[aenc.asset_name_c].str.contains(_ptr)

    idf.loc[_msk , aenc.ast_type] = asst.ejare
    idf.loc[_msk , aenc.asset_key] = asst.ejare

    _df = idf[_msk]
    print(len(_df))

    return idf , _df

  df , df8 = _find_ejare(df)

  ##
  def _find_tese(
      idf
      ) :
    """

    """
    _pt0 = r'^' + r'امتیاز' + r'\s?' + r'تسهیلات' + r'\s?' + r'مسکن' + r'\b'

    _ptrns = {
        (_pt0 , None)                  : None ,
        (r'^' + r'تسه' + r'\b' , None) : None ,
        }

    _col = aenc.asset_name_c

    _umsk = idf[_col].apply(lambda
                              x : False)

    for _ky , _val in _ptrns.items() :
      _msk = idf[_col].str.contains(_ky[0])
      if _ky[1] is not None :
        _msk &= ~ idf[_col].str.contains(_ky[1])

      _umsk |= _msk
      print('len:' , len(_msk[_msk]))

      idf.loc[_msk , aenc.ast_type] = asst.tese
      idf.loc[_msk , aenc.asset_key] = asst.tese

    _df = idf[_umsk]
    print(len(_df))

    return idf , _df

  df , df9 = _find_tese(df)

  ##
  df.to_excel('1.xlsx' , index = False)

  ##
  _vl1 = r'^' + r'صندوق سرمایه گذاری' + r'\b'
  _vl4 = r'^' + r'ص' + r'\.' + r'س' + r'\.\b'

  ptr_rep = {
      (r'^' + 'شرکت' + r'\b' , None) : '' ,
      (_vl1 , None)                  : '' ,
      (_vl4 , None)                  : '' ,
      }

  ##

##


if __name__ == "__main__" :
  main()
  print(f'{Module_name} Done.')

def _test() :
  pass

  ##
  _fp = '/Users/mahdimir/Dropbox/keyData/tickerMarket/tickerMarket.xlsx'
  _df = pd.read_excel(_fp , engine = 'openpyxl')

  _df = _df.drop_duplicates()

  ##
  _df['market'] = _df['market'].apply(lambda
                                        x : x[:3])

  ##
  _df.to_excel(_fp , index = False)

  ##

  ##
