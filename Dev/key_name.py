"""[governs key-name.xlsx]

    """

##

import re

import pandas as pd

from Dev import functions_and_dirs as fu
from Helper.name_space import knc

def excel_style(
    row ,
    col
    ) :
  """ Convert given row and column number to an Excel-style cell name. """
  LETTERS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
  result = []
  while col :
    col , rem = divmod(col - 1 , 26)
    result[:0] = LETTERS[rem]
  return ''.join(result) + str(row)

def remove_rows_with_h_at_end(
    df
    ) :
  ptrn = r'.*ح$'
  kys = df[knc.key].unique()
  lcn = df[knc.key].str.fullmatch(ptrn)
  lcn &= df.loc[lcn.index , knc.key].str[:-1].isin(kys)
  lcn = lcn.fillna(False)
  odf = df[~ lcn]
  return odf

def remove_names_with_h_dot_at_the_start(
    idf
    ) :
  lptrn = r'ح\s*[\.\s\b]'
  lcn = idf[knc.companyName].str.match(lptrn)
  lcn = lcn.fillna(False)
  odf = idf[~ lcn]
  return odf

def apply_func_on_notna_rows_of_cols(
    idf ,
    cols ,
    func
    ) :
  for col in cols :
    _ms = idf[col].notna()
    idf.loc[_ms , col] = idf.loc[_ms , col].apply(func)
  return idf

def _test() :
  pass

  ##
  fp0 = '/Users/mahdimir/Documents/Projects/TSE-MutualFunds-MonthlyPortfolio/Data/M-Backup/key-Name copy-000829.xlsx'
  df1 = pd.read_excel(fp0 , engine = 'openpyxl')
  kyn = kyn.append(df1)
  kyn = kyn.drop_duplicates()

  ##
  uniq_keys = kyn[knc.key_wos].value_counts().eq(1)
  uniq_keys = uniq_keys[uniq_keys]
  uniq_keys.index

  ##
  kyn = kyn.reset_index()
  kyn[kyn[knc.exchange]]
  kyn.lt(1).all()
  ##
  df1[knc.companyName] = df1[knc.companyName].apply(normalize_str_4_sym_name)
  df1 = remove_ekhtesasi_fix(df1 , [knc.companyName])
  df1 = remove_extra_words_from_names(df1 , knc.companyName)
  df1 = remove_sandogh_sarmaye_gozari_from_begining(df1 , knc.companyName)
  df1[knc.companyName_wos] = df1[knc.companyName].str.replace('\s' , '')
  df1 = df1.drop_duplicates(subset = knc.companyName_wos)
  ##
  msk = fnds[knc.CodalSymbol].isna()
  print('len' , len(msk[msk]))
  ##
  fnds.loc[fnds[knc.CodalSymbol].isna() , knc.CodalSymbol] = fnds[
    knc.companyName_wos].map(
      df1.set_index(knc.companyName_wos)[knc.CodalSymbol])  ##

  ##
  fp = '/Users/mahdimir/Documents/Projects/TSE-MutualFunds-MonthlyPortfolio/Data/Cleaned_Stock_Prices_1400_06_16.parquet'
  _df = pd.read_parquet(fp)
  ##
  _df = _df[['name' , 'title']].drop_duplicates()
  ##
  _df[knc.key] = _df['name']
  _df[knc.companyName] = _df['title'].str.split('-')
  _df[knc.companyName] = _df[knc.companyName].apply(lambda
                                                      x : x[0])
  _df['ex'] = _df['title'].str.split('-').apply(lambda
                                                  x : x[1])
  ##
  _df[knc.exchange] = None
  x = _df.at[3149 , 'ex']
  msk = _df['ex'].apply(lambda
                          z : x in str(z))
  _df.loc[msk , knc.exchange] = 'TSE(1)-Tablo Asli'
  ##
  x = _df.at[268 , 'ex'].iat[0]
  msk = _df['ex'].apply(lambda
                          z : x in str(z))
  _df.loc[msk , knc.exchange] = 'TSE(2)'
  ##
  x = _df.at[814 , 'ex']
  msk = _df['ex'].apply(lambda
                          z : x in str(z))
  _df.loc[msk , knc.exchange] = 'IFB-Paye-Yellow'
  ##
  x = _df.at[306 , 'ex'].iat[0]
  msk = _df['ex'].apply(lambda
                          z : x in str(z))
  _df.loc[msk , knc.exchange] = 'IFB(2)'
  ##
  x = _df.at[2626 , 'ex']
  msk = _df['ex'].apply(lambda
                          z : x in str(z))
  _df.loc[msk , knc.exchange] = 'IFB(3)'
  ##
  x = _df.at[1808 , 'ex']
  msk = _df['ex'].apply(lambda
                          z : x in str(z))
  _df.loc[msk , knc.exchange] = 'IFB(1)'
  ##
  x = _df.at[622 , 'ex'].iat[0]
  msk = _df['ex'].apply(lambda
                          z : x in str(z))
  _df.loc[msk , knc.exchange] = 'TSE(1)-Tablo Fari'
  ##
  x = _df.at[2135 , 'ex']
  msk = _df['ex'].apply(lambda
                          z : x in str(z))
  _df.loc[msk , knc.exchange] = 'IFB-Paye-Orange'
  ##
  x = _df.at[1774 , 'ex'].iat[0]
  msk = _df['ex'].apply(lambda
                          z : x in str(z))
  _df.loc[msk , knc.exchange] = 'IFB-Paye-Red'
  ##
  x = 'بازار پايه قرمز فرابورس'
  msk = _df['ex'].apply(lambda
                          z : x in str(z))
  _df.loc[msk , knc.exchange] = 'IFB-Paye-Red'
  ##
  msk = _df[knc.exchange].isna()
  _df1 = _df[msk]
  ##
  x = _df.at[62 , 'ex']
  msk = _df['ex'].apply(lambda
                          z : x in str(z))
  _df.loc[msk , knc.exchange] = 'IME'
  ##
  x = 'بازار پايه نارنجي فرابورس'
  msk = _df['ex'].apply(lambda
                          z : x in str(z))
  _df.loc[msk , knc.exchange] = 'IFB-Paye-Orange'
  ##
  x = 'فرابورس'
  msk = _df['ex'].apply(lambda
                          z : x in str(z))
  _df.loc[msk , knc.exchange] = 'IFB'
  ##
  x = 'پذيره'

  msk = _df['ex'].apply(lambda
                          z : x in str(z))
  _df.loc[msk , knc.exchange] = 'Pazire'
  ##
  msk = _df[knc.exchange].isna()
  _df.loc[msk , knc.exchange] = 'TSE'
  ##
  _df['desc'] = None
  ##
  _df = _df[[knc.companyName , knc.key , knc.exchange , 'desc']]
  ##
  _df = _df.applymap(fu.normalize_str)
  ##
  _df = fu.define_wos_cols(_df , [knc.companyName])
  ##
  _df = _df.drop_duplicates(subset = knc.companyName)
  ##
  _df = _df[[knc.companyName , knc.key , knc.exchange , 'desc']]
  ##
  _df['desc'] = _df['desc'].str.replace('None' , '')
  ##
  _df.to_excel('tsesymbs.xlsx' , index = False)
  ##
  kyn = kyn[[knc.companyName , knc.key , knc.exchange , 'desc']]
  ##
  kyn = kyn.append(_df)

  ##
  kyn = ky_name_df
  ##
  x = fnds.loc[139 , knc.companyName]
  x[:19]
  ##
  df1 = df1[['Symbol' , 'companyName']].drop_duplicates()
  ##
  df1['key'] = df1['Symbol']
  df1[['exchange' , 'desc']] = None
  ##
  df1 = df1[[knc.companyName , knc.key , knc.exchange , 'desc']]
  ##
  kyn = kyn.append(df1)
  ##
  x = kyn.loc[0 , knc.companyName]
  re.sub(' ابادا$' , '' , x)
  ##
  """    def sub_as_word(x , y , rep) :
          return re.sub(y , rep , x)

      def remove_key_from_company_name_with_care(idf) :
          _df = idf.copy()
          _df[knc.companyName] = _df.apply(lambda x : sub_as_word(
                  x[knc.companyName] , ' ' + x[knc.key] + r'$' , '') , axis=1)
          return _df

      df1 = remove_key_from_company_name_with_care(kyn)
      kyn = kyn.append(df1)"""

  ##
  fp = '/Users/mahdimir/Documents/Projects/TSE-MutualFunds-MonthlyPortfolio/Data/M-Backup/key-Name-001006 copy.xlsx'
  df1 = pd.read_excel(fp , engine = 'openpyxl')
  df1 = df1.dropna()
  df1[[knc.fundManager , knc.fundInvMkt]] = None
  ##
  kyn = kyn.append(df1)

  ##
  fp = '/Users/mahdimir/Documents/Projects/TSE-MutualFunds-MonthlyPortfolio/Data/M-Backup/Funds-001009-2.xlsx'
  df1 = pd.read_excel(fp , engine = 'openpyxl')
  df1 = df1.drop_duplicates(subset = knc.key)
  fnds[knc.exchange] = fnds[knc.key].map(df1.set_index(knc.key)[knc.exchange])

  ##
