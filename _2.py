"""[cleans codal tables and match keys from funds xl for each company name]

    """

##

import math
import re
from functools import partial

import pandas as pd
from persiantools import characters
from persiantools import digits

from _0 import get_all_fps_wt_suffix
from _1 import save_current_module_cache
from _1 import smpl_ret_cache_fp
from Helper.name_space import cc
from Helper.name_space import ctc
from Helper.name_space import cte
from Helper.name_space import d
from Helper.name_space import indi
from Helper.name_space import knc

Module_name = '_2.py'

out_cols = {
    knc.key : None ,
    }

ptr_rep = {
    'شرکت'                : '' ,
    'صندوق سرمایه گذاری ' : '' ,
    r'\bاختصاصی\b'        : '' ,
    r'\bبازار گردان\b'    : 'بازارگردانی' ,
    r'\bبازار گردانی\b'   : 'بازارگردانی' ,
    }

def make_the_order_by_numbers(
    fps: list
    ) :
  st_fp = {x.stem : x for x in fps}
  ordrd_sts = sorted(list(st_fp.keys()) ,
                     key = lambda
                       x : int(x.split('_')[1]))
  ordrd_fps = [st_fp[x] for x in ordrd_sts]
  return ordrd_fps

def get_pre_mod_name(
    the_dir ,
    cur_mod_name_wt_py
    ) :
  _fps = get_all_fps_wt_suffix(the_dir , '.py')
  ordered_fps = make_the_order_by_numbers(_fps)
  names = [x.name for x in ordered_fps]
  ind = names.index(cur_mod_name_wt_py)
  if ind != 0 :
    ou = names[ind - 1]
  else :
    ou = names[0]
  print(ou , cur_mod_name_wt_py)
  return ou

def is_theword_in_str(
    istr ,
    the_word
    ) :
  if the_word is None :
    return False
  wrd = re.escape(str(the_word))
  return re.search(r'\b' + str(wrd) + r'\b' , istr) is not None

def any_of_wordlist_isin_str(
    istr ,
    wordlist: list
    ) :
  if istr is None :
    return False
  for word in wordlist :
    if is_theword_in_str(istr , word) == True :
      return True
  return False

def find_jdate_as_int_using_re(
    ist
    ) :
  ist = digits.fa_to_en(str(ist))
  match_obj = re.search(r'1[3-4]\d{2}/?[0-1]\d/?[0-3]\d' , ist)
  if match_obj :
    match = match_obj.group()
    num = re.sub(r'\D' , '' , match)
    return int(num)

def find_the_key_by_eq(
    ast_wos: str ,
    ky_name: pd.DataFrame ,
    outp_cols: list
    ) :
  msk = ky_name[knc.companyName + '_wos'].eq(ast_wos)
  msk |= ky_name[knc.key + '_wos'].eq(ast_wos)
  flt = ky_name[msk]
  if flt[knc.key].nunique() == 1 :
    return flt[outp_cols].iloc[0]
  else :
    return [None] * len(outp_cols)

def mask_without_key_rows(
    df ,
    key_col=knc.key
    ) :
  msk = df[key_col].isna()
  print(len(msk[msk]))
  return msk

def map2keys(
    df ,
    fr_which_col: str ,
    ky_name ,
    output_cols: list
    ) :
  wos_col = fr_which_col + '_wos'
  df[wos_col] = df[fr_which_col].str.replace(' ' , '')

  msk = mask_without_key_rows(df)
  flt = df.loc[msk , wos_col].to_frame()
  flt = flt.drop_duplicates()

  _func = partial(find_the_key_by_eq ,
                  ky_name = ky_name ,
                  outp_cols = output_cols)

  _fu = lambda \
    x : _func(x[wos_col])
  flt[output_cols] = flt.apply(_fu , axis = 1 , result_type = 'expand')

  _df = flt.set_index(wos_col)
  for col in output_cols :
    df.loc[msk , col] = df.loc[msk , wos_col].map(_df[col])

  df = df.drop(columns = wos_col)
  return df

def define_wos_cols(
    df ,
    which_cols: list
    ) :
  for col in which_cols :
    msk = df[col].notna()
    ncol = col + '_wos'
    _pre = df.loc[msk , col]
    df.loc[msk , ncol] = _pre.str.replace(r'\s' , '' , regex = True)
  return df

def normalize_str(
    ist: str
    ) :
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
  for key , val in replce.items() :
    if key[1] is not None :
      if not re.match(key[1] , os) :
        os = re.sub(key[0] , val , os)
    else :
      os = re.sub(key[0] , val , os)
  os = os.strip()
  return os

def sub_a_regex_in_a_col_with_care(
    df ,
    ptrn: str ,
    rep: str ,
    which_col: str
    ) :
  df = df.reset_index(drop = True)
  vcou = df[which_col].value_counts()
  nc = which_col + '_cou'
  df[nc] = df[which_col].map(vcou)
  df['help'] = df[which_col].str.replace(ptrn , rep , regex = True)
  hcou = df['help'].value_counts()
  df['helpcou'] = df['help'].map(hcou)
  msk = df[nc].eq(df['helpcou'])
  print(len(msk[msk]))
  _df0 = df[msk]
  df.loc[msk , which_col] = df['help']
  df[which_col] = df[which_col].str.strip()
  col2drop = [nc , 'help' , 'helpcou']
  df = df.drop(columns = col2drop)
  return df , _df0

def clean_names(
    df ,
    which_col: str ,
    ptrn_rep: dict
    ) :
  for ptr , rep in ptrn_rep.items() :
    df , _ = sub_a_regex_in_a_col_with_care(df , ptr , rep , which_col)
  return df

def main() :
  pass

  ##
  pre_mod_name = get_pre_mod_name(d.code , Module_name)

  ##
  cur_mod_cache_fp = smpl_ret_cache_fp(Module_name)
  pre_mod_cache_fp = smpl_ret_cache_fp(pre_mod_name)

  ##
  df = pd.read_parquet(pre_mod_cache_fp)

  ##
  assert len(df[ctc.TracingNo].unique()) == len(df)

  df[ctc.TracingNo] = df[ctc.TracingNo].astype(float)
  df[ctc.TracingNo] = df[ctc.TracingNo].apply(math.floor)
  df[ctc.TracingNo] = df[ctc.TracingNo].astype(int)
  df[ctc.TracingNo] = df[ctc.TracingNo].astype(str)

  ##
  _rename = {
      ctc.TracingNo : indi.L0_TracingNo ,
      }

  df = df.rename(columns = _rename)

  ##
  df = df.drop(columns = cc.json_stem)

  ##
  def _check_is_letter_code_n31_html(
      idf
      ) :
    cn = idf[ctc.HasHtml] & idf[ctc.LetterCode].ne('ن-۳۱')
    _df = idf[cn]
    assert _df.empty , "False Assumption, rather, not all reports in html format have letter code equal to 'ن-۳۱'"

  _check_is_letter_code_n31_html(df)

  ##
  def find_monthly_reps(
      idf
      ) :
    is_monthly_keys = ['۱ ماهه']
    _the_func = any_of_wordlist_isin_str
    _fu = lambda \
      x : _the_func(str(x) , is_monthly_keys)
    idf[cc.isMonthlyRep] = idf[ctc.Title].apply(_fu)
    return idf

  df = find_monthly_reps(df)

  ##
  def find_date_and_month(
      idf
      ) :
    _fu = find_jdate_as_int_using_re
    idf[cc.titleJDate] = idf[ctc.Title].apply(_fu).astype(str)
    idf[cc.titleJMonth] = idf[cc.titleJDate].astype(int) // 100
    idf[cc.titleJMonth] = idf[cc.titleJMonth].astype(str)
    return idf

  df = find_date_and_month(df)

  ##
  def count_monthly_reps(
      idf
      ) :
    cn2 = ~ idf[cc.isMonthlyRep]
    df2 = idf[cn2]
    print("Not monthly reports count" , len(df2))
    print("Monthly reports count" , len(idf[idf[cc.isMonthlyRep]]))
    # idf = idf[~ cn2]
    return idf

  df = count_monthly_reps(df)

  ##
  col_ncol = {
      ctc.Symbol      : cc.cleanSym ,
      ctc.CompanyName : cc.cleanCompanyName ,
      }

  for col , ncol in col_ncol.items() :
    df[ncol] = df[col].apply(normalize_str)

  ##
  for coln in out_cols.keys() :
    df[coln] = None

  ##
  ky_name = pd.read_excel(cte.key_name_fp , engine = 'openpyxl')

  ##
  ky_name = define_wos_cols(ky_name , [knc.key , knc.companyName])

  ##
  for col in [ctc.Symbol , cc.cleanSym , cc.cleanCompanyName] :
    df = map2keys(df ,
                  fr_which_col = col ,
                  ky_name = ky_name ,
                  output_cols = list(out_cols.keys()))
    msk = mask_without_key_rows(df)
    df1 = df[msk]

  ##
  for col in [cc.cleanSym , cc.cleanCompanyName] :
    df = clean_names(df , col , ptr_rep)

  for col in [cc.cleanSym , cc.cleanCompanyName] :
    df = map2keys(df ,
                  fr_which_col = col ,
                  ky_name = ky_name ,
                  output_cols = list(out_cols.keys()))
    msk = mask_without_key_rows(df)
    df1 = df[msk]

  ##
  ren = {
      knc.key : cc.fundKey
      }

  df = df.rename(columns = ren)

  ##
  save_current_module_cache(df , cur_mod_cache_fp)

##


if __name__ == "__main__" :
  main()
  print(f'{Module_name} Done.')

##

def _test() :
  pass

  ##
