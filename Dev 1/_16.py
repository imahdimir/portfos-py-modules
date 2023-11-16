"""[cleans and checks grand1 fins]

    """

##

import warnings

from Helper.name_space import asst
from Helper.name_space import knc

warnings.filterwarnings("ignore")

Module_name = '_16.py'

out_cols = {
    knc.key         : None ,
    knc.isFund      : None ,
    knc.isListed    : None ,
    knc.isETF       : None ,
    knc.fundInvMkt  : None ,
    knc.fundManager : None ,
    }

class G2NewCols :
  def __init__(
      self
      ) :
    self.asset_name_c = PSC.asset_name + '_cln'
    self.start_shares_c = PSC.start_shares + '_cln'
    self.start_purch_cost_c = PSC.start_purchase_cost + '_cln'
    self.start_net_sale_val_c = PSC.start_net_sale_val + '_cln'
    self.change_buy_shares_c = PSC.change_buy_shares + '_cln'
    self.change_buy_cost_c = PSC.change_buy_cost + '_cln'
    self.change_sell_shares_c = PSC.change_sell_shares + '_cln'
    self.change_sell_revenue_c = PSC.change_sell_revenue + '_cln'
    self.end_shares_c = PSC.end_shares + '_cln'
    self.end_asset_price_c = PSC.end_asset_price + '_cln'
    self.end_purchase_cost_c = PSC.end_purchase_cost + '_cln'
    self.end_net_sale_val_c = PSC.end_net_sale_val + '_cln'
    self.end_pct_to_all_assets_c = PSC.end_pct_to_all_assets + '_cln'

    self.ast_type = None
    self.exp_date = None
    self.strike_price = None
    for key , val in self.__dict__.items() :
      if val is None :
        self.__dict__[key] = key

g2n = G2NewCols()

cols2norm = {
    g2n.asset_name_c            : None ,
    g2n.start_shares_c          : None ,
    g2n.start_purch_cost_c      : None ,
    g2n.start_net_sale_val_c    : None ,
    g2n.change_buy_shares_c     : None ,
    g2n.change_buy_cost_c       : None ,
    g2n.change_sell_shares_c    : None ,
    g2n.change_sell_revenue_c   : None ,
    g2n.end_shares_c            : None ,
    g2n.end_asset_price_c       : None ,
    g2n.end_purchase_cost_c     : None ,
    g2n.end_net_sale_val_c      : None ,
    g2n.end_pct_to_all_assets_c : None ,
    }

def remove_rows_of_a_col_with_ptrn(
    df ,
    col ,
    ptrn
    ) :
  msk = df[col].str.fullmatch(ptrn)
  df = df[~ msk]
  print('len:' , len(msk[msk]))
  return df

def main() :
  pass

  ##

  ##
  ptrn_rep_duo = {
      (r'^س\s?\.\s' , None)            : 'سرمایه گذاری' ,
      (r'\sج\.\s?ا\s?\.\s?ا\b' , None) : 'جمهوری اسلامی ایران' ,
      (r'\_' , None)                   : ' ' ,
      (r',' , None)                    : '' ,
      ('-' , None)                     : ' ' ,
      ('شرکت' , None)                  : '' ,
      ('صندوق سرمایه گذاری ' , None)   : '' ,
      (r'\bاختصاصی\b' , None)          : '' ,
      (r'\bبازار گردان\b' , None)      : 'بازارگردانی' ,
      (r'\bبازار گردانی\b' , None)     : 'بازارگردانی' ,
      (r'\bتبعی\b' , None)             : '' ,
      (r'_' , None)                    : ' ' ,
      (r'^صندوق س\s?\.' , None)        : '' ,
      }

  ##
  def remove_duos_from_col(
      idf ,
      which_col ,
      duos: dict
      ) :
    for pt , rp in duos.items() :
      _msk = mask_without_key_rows(idf)
      _msk &= idf[which_col].str.contains(pt[0])
      if pt[1] :
        _msk &= ~ idf[which_col].str.contains(pt[1])
      idf.loc[_msk , which_col] = idf[which_col].str.replace(pt[0] , rp)
    idf = fu.apply_func_on_notna_rows_of_cols(idf ,
                                              [g2n.asset_name_c] ,
                                              fu.normalize_str)
    return idf

  df = remove_duos_from_col(df , g2n.asset_name_c , ptrn_rep_duo)

  ##
  df = fu.apply_func_on_notna_rows_of_cols(df ,
                                           [g2n.asset_name_c] ,
                                           fu.normalize_str)
  df = remove_duos_from_col(df , g2n.start_shares_c , ptrn_rep_duo)
  ##
  df , df1 = match_wt_keys(df)

  ##
  def find_akhza(
      idf
      ) :
    ptrns = {
        r'\bاسناد\s?خزانه\b' : (r'' , 'akhza') ,
        }
    for ptr , rep in ptrns.items() :
      msk1 = idf[g2n.asset_name_c].str.contains(ptr)
      msk1 = msk1.fillna(False)
      idf.loc[msk1 , g2n.asset_name_c] = idf[g2n.asset_name_c].str.replace(ptr ,
                                                                           rep[
                                                                             0])
      idf.loc[msk1 , g2n.ast_type] = asst.akhza
      print('len:' , len(msk1[msk1]))
      idf.loc[msk1 , knc.key] = rep[1]
    idf = fu.apply_func_on_notna_rows_of_cols(idf ,
                                              [g2n.asset_name_c] ,
                                              fu.normalize_str)
    return idf

  df = find_akhza(df)
  ##
  df , df1 = match_wt_keys(df)

  ##
  def remove_rows_with_word_in_number_cols(
      idf
      ) :
    for _col in [g2n.start_shares_c , g2n.start_purch_cost_c ,
                 g2n.start_net_sale_val_c , g2n.change_buy_shares_c ,
                 g2n.change_buy_cost_c , g2n.change_sell_shares_c ,
                 g2n.end_asset_price_c , g2n.end_shares_c ,
                 g2n.end_purchase_cost_c , g2n.end_net_sale_val_c ,
                 g2n.end_pct_to_all_assets_c] :
      _mask = idf[_col].notna()
      _mask &= idf[_col].str.fullmatch(r'\D+')
      idf = idf[~ _mask]
      print('len:' , len(_mask[_mask]))
    return idf

  df = remove_rows_with_word_in_number_cols(df)
  ##
  df = fu.apply_func_on_notna_rows_of_cols(df ,
                                           [g2n.asset_name_c] ,
                                           fu.normalize_str)

  ##
  def fix_asset_name_start_shares(
      idf
      ) :
    _mask = idf[g2n.asset_name_c].eq(idf[g2n.start_shares_c])
    idf.loc[_mask , g2n.start_shares_c] = idf.loc[
      _mask , g2n.start_shares_c].str.replace(r'\D' , '')
    idf.loc[_mask , g2n.asset_name_c] = idf.loc[
      _mask , g2n.asset_name_c].str.replace(r'\d' , '')
    print('len:' , len(_mask[_mask]))
    return idf

  df = fix_asset_name_start_shares(df)
  ##
  df , df1 = match_wt_keys(df)

  ##
  def remove_digits_at_end(
      idf
      ) :
    _msk = mask_without_key_rows(idf)
    idf.loc[_msk , g2n.asset_name_c] = idf.loc[
      _msk , g2n.asset_name_c].str.replace(r'\d+$' , '')
    print('len:' , len(_msk[_msk]))
    return idf

  df = remove_digits_at_end(df)
