"""[cleans and checks grand1 fins]
    """
##
import warnings
from functools import partial

import pandas as pd

from main import d
from Py\._10 import ODC , PSC
from Py\._17 import g2n
from Py.Helpers import functions_and_dirs as fu
from Py.Helpers.name_space import ast , cte , knc

warnings.filterwarnings("ignore")
Module_name = '_18.py'

class G4NewCols :
    def __init__(self) :
        self.shares_ch = None
        self.pre_M_end_shares = None
        self.start_eqs_lastM_end_shares = None
        self.purchase_cost_ch = None
        self.next_M_start_shares = None
        self.pre_month_end_purchase = None

        for key , val in self.__dict__.items() :
            if val is None :
                self.__dict__[key] = key

nco = G4NewCols()

Cols_dct = {
        g2n.start_shares_c        : None ,
        g2n.start_purch_cost_c    : None ,
        g2n.change_buy_shares_c   : None ,
        g2n.change_buy_cost_c     : None ,
        g2n.change_sell_shares_c  : None ,
        g2n.change_sell_revenue_c : None ,
        g2n.end_shares_c          : None ,
        g2n.end_purchase_cost_c   : None ,
        }
Cols = list(Cols_dct.keys())

def fill_unobserved(df: pd.DataFrame , key_cols_wo_month: list) :
    months = df[ODC.jmonth]
    min_month = months.min()
    max_month = months.max()
    all_months = fu.make_all_months_in_between(min_month , max_month)
    all_months.append(fu.shift_month(min_month , -1))
    all_months.append(fu.shift_month(max_month , +1))
    colnames_2_uniq_vals = {
            ODC.jmonth : all_months
            }
    for col in key_cols_wo_month :
        uval = list(df[col].unique())
        colnames_2_uniq_vals[col] = uval
    _df = fu.create_cartesian_product_dataframe(colnames_2_uniq_vals)
    df = df.append(_df)
    df = df.drop_duplicates(subset=key_cols_wo_month + [ODC.jmonth])
    return df

def mask_not_true(df , which_col) :
    _msk = df[which_col].eq(False)
    print(len(_msk[_msk]))
    return _msk

def main() :
    pass
    ##
    df = pd.read_parquet(cte.grand3)

    ##
    def drop_not_useful_cols_in_portfolio(idf) :
        cols2dr_0 = list(PSC.__dict__.values())
        col2dr_dict = {
                g2n.asset_name_c          : None ,
                g2n.start_net_sale_val_c  : None ,
                g2n.end_net_sale_val_c    : None ,
                g2n.start_purch_cost_c    : None ,
                g2n.change_buy_cost_c     : None ,
                g2n.change_sell_revenue_c : None ,
                g2n.end_purchase_cost_c   : None ,
                g2n.exp_date              : None ,
                g2n.strike_price          : None ,
                ODC.sheetStem             : None ,
                }
        cols2dr = list(col2dr_dict.keys()) + cols2dr_0
        idf = idf.drop(columns=cols2dr , errors='ignore')
        return idf

    pdf0 = drop_not_useful_cols_in_portfolio(df)

    ##
    def keep_only_common_stocks(idf) :
        print(len(idf))
        _msk = idf[g2n.ast_type].eq(ast.common_stocks)
        idf = idf[_msk]
        print(len(_msk[_msk]))
        idf = idf.drop(columns=[g2n.ast_type])
        return idf

    pdf0 = keep_only_common_stocks(pdf0)

    ##
    kys_dct = {
            ODC.fundKey : None ,
            ODC.jmonth  : None ,
            knc.key     : None ,
            }
    kys = list(kys_dct.keys())

    ##
    def drop_duplicted_rows_in_key_cols(idf , key_cols: list) :
        print(len(idf))
        idf = idf.drop_duplicates(subset=key_cols)
        print(len(idf))
        return idf

    pdf0 = drop_duplicted_rows_in_key_cols(pdf0 , kys)

    ##
    def remove_all_nan_obs(idf) :
        _colsdct = {
                g2n.start_shares_c       : None ,
                g2n.change_buy_shares_c  : None ,
                g2n.change_sell_shares_c : None ,
                g2n.end_shares_c         : None ,
                }
        _df = idf[_colsdct.keys()].isna()
        _msk = _df.all(axis=1)
        print(len(_msk[_msk]))
        idf = idf[~ _msk]
        return idf

    pdf0 = remove_all_nan_obs(pdf0)

    ##
    def replace_minute_vals_with_zero(idf) :
        _colsdct = {
                g2n.start_shares_c       : None ,
                g2n.change_buy_shares_c  : None ,
                g2n.change_sell_shares_c : None ,
                g2n.end_shares_c         : None ,
                }
        for _col in _colsdct.keys() :
            _msk = idf[_col].lt(20)
            idf.loc[_msk , _col] = 0
        return idf

    pdf0 = replace_minute_vals_with_zero(pdf0)

    ##
    def shares_num_check_contemporary_with_tol(idf , error_tol=100) :
        _msk0 = idf[g2n.end_shares_c].notna()
        _msk1 = _msk0 & idf[g2n.change_buy_shares_c].isna()
        _msk1 &= idf[g2n.change_sell_shares_c].isna()
        idf.loc[_msk1 , nco.shares_ch] = True

        _msk2 = _msk0 & idf[nco.shares_ch].ne(True)
        _df = idf[_msk2]
        end = _df[g2n.end_shares_c].astype(int)

        _sum = _df[g2n.start_shares_c].fillna(0).astype(int)
        _sum += _df[g2n.change_buy_shares_c].fillna(0).astype(int)
        _sum -= _df[g2n.change_sell_shares_c].fillna(0).astype(int)
        _lb = end - error_tol
        _ub = end + error_tol
        _df[nco.shares_ch] = _sum.gt(_lb)
        _df[nco.shares_ch] &= _sum.lt(_ub)

        idf.loc[_msk2 , nco.shares_ch] = _df[nco.shares_ch]
        return idf

    pdf0 = shares_num_check_contemporary_with_tol(pdf0)

    msk = mask_not_true(pdf0 , nco.shares_ch)
    df1 = pdf0[msk]

    ##
    def fill_unobserved_holding(idf , kys_wo_month: list) :
        func0 = partial(fill_unobserved , key_cols_wo_month=kys_wo_month)
        _gpd = idf.groupby(kys_wo_month , as_index=False , dropna=False)
        idf = _gpd.apply(func0)
        idf = idf.reset_index(drop=True)
        return idf

    kys1 = kys.copy()
    kys1.remove(ODC.jmonth)
    pdf1 = fill_unobserved_holding(pdf0 , kys1)

    ##
    def insert_next_month_start_and_last_end(idf , key_cols_wo_month: list) :
        idf = idf.sort_values(by=ODC.jmonth)
        ncol = nco.next_M_start_shares
        tcol = g2n.start_shares_c
        _by = key_cols_wo_month
        idf[ncol] = idf.groupby(_by , dropna=False)[tcol].shift(-1)
        ncol1 = nco.pre_M_end_shares
        tcol1 = g2n.end_shares_c
        idf[ncol1] = idf.groupby(_by , dropna=False)[tcol1].shift()
        return idf

    pdf2 = insert_next_month_start_and_last_end(pdf1 , key_cols_wo_month=kys1)

    ##
    def fill_end_with_next_start_if_becomes_valid(idf) :
        _df0 = idf.copy()
        _df0 = shares_num_check_contemporary_with_tol(_df0)
        _msk0 = mask_not_true(_df0 , nco.shares_ch)
        print(len(_msk0[_msk0]))
        _df0.loc[_msk0 , g2n.end_shares_c] = _df0[nco.next_M_start_shares]
        _df0 = shares_num_check_contemporary_with_tol(_df0)
        _msk1 = mask_not_true(_df0 , nco.shares_ch)
        print(len(_msk1[_msk1]))
        _msk2 = ~_msk1 & _msk0
        print(len(_msk2[_msk2]))
        _df = idf[_msk2]
        idf.loc[_msk2 , g2n.end_shares_c] = _df0[nco.next_M_start_shares]
        _df1 = idf[_msk2]
        return idf , _df , _df1

    pdf2 , df1 , df2 = fill_end_with_next_start_if_becomes_valid(pdf2)

    ##
    def fill_start_with_pre_month_end_if_becomes_valid(idf) :
        _df0 = idf.copy()
        _df0 = shares_num_check_contemporary_with_tol(_df0)
        _msk0 = mask_not_true(_df0 , nco.shares_ch)
        print(len(_msk0[_msk0]))
        _df0.loc[_msk0 , g2n.start_shares_c] = _df0[nco.pre_M_end_shares]
        _df0 = shares_num_check_contemporary_with_tol(_df0)
        _msk1 = mask_not_true(_df0 , nco.shares_ch)
        print(len(_msk1[_msk1]))
        _msk2 = ~_msk1 & _msk0
        print(len(_msk2[_msk2]))
        idf.loc[_msk2 , g2n.start_shares_c] = _df0[nco.pre_M_end_shares]
        return idf

    pdf2 = fill_start_with_pre_month_end_if_becomes_valid(pdf2)

    ##
    def fill_both_start_end_if_becomes_valid(idf) :
        _df0 = idf.copy()
        _df0 = shares_num_check_contemporary_with_tol(_df0)
        _msk0 = mask_not_true(_df0 , nco.shares_ch)
        print(len(_msk0[_msk0]))
        _df0.loc[_msk0 , g2n.start_shares_c] = _df0[nco.pre_M_end_shares]
        _df0.loc[_msk0 , g2n.end_shares_c] = _df0[nco.next_M_start_shares]
        _df0 = shares_num_check_contemporary_with_tol(_df0)
        _msk1 = mask_not_true(_df0 , nco.shares_ch)
        print(len(_msk1[_msk1]))
        _msk2 = ~_msk1 & _msk0
        print(len(_msk2[_msk2]))
        idf.loc[_msk0 , g2n.start_shares_c] = _df0[nco.pre_M_end_shares]
        idf.loc[_msk0 , g2n.end_shares_c] = _df0[nco.next_M_start_shares]
        return idf

    pdf2 = fill_both_start_end_if_becomes_valid(pdf2)

    ##
    msk = pdf2[nco.pre_M_end_shares].eq(1)
    df1 = pdf2[msk]

    msk = pdf2[g2n.start_shares_c].eq(1)
    df2 = pdf2[msk]

    ##
    def fillna_start_with_last_end(idf) :
        _msk = idf[g2n.start_shares_c].isna()
        _msk &= idf[nco.pre_M_end_shares].notna()
        idf.loc[_msk , g2n.start_shares_c] = idf[nco.pre_M_end_shares]
        print(len(_msk[_msk]))
        return idf

    pdf2 = fillna_start_with_last_end(pdf2)

    ##
    def fill_na_end_shares_wt_next_month_start(idf) :
        _msk = idf[g2n.end_shares_c].isna()
        _msk &= idf[nco.next_M_start_shares].notna()
        idf.loc[_msk , g2n.end_shares_c] = idf[nco.next_M_start_shares]
        print(len(_msk[_msk]))
        return idf

    pdf2 = fill_na_end_shares_wt_next_month_start(pdf2)

    ##
    def drop_invalid_obs(idf: pd.DataFrame) :
        idf = shares_num_check_contemporary_with_tol(idf)
        _msk = idf[nco.shares_ch].eq(False)
        _df = idf[_msk]
        idf = idf[~ _msk]
        print(len(_msk[_msk]))
        return idf , _df

    pdf2 , df1 = drop_invalid_obs(pdf2)

    ##
    def check_start_shares_eq_last_end(idf) :
        _msk = idf[g2n.start_shares_c].notna()
        _msk &= idf[nco.pre_M_end_shares].notna()
        print(len(_msk[_msk]))
        _msk1 = _msk & idf[g2n.start_shares_c].eq(idf[nco.pre_M_end_shares])
        idf.loc[_msk1 , nco.start_eqs_lastM_end_shares] = True
        print(len(_msk1[_msk1]))
        _msk2 = _msk & idf[g2n.start_shares_c].ne(idf[nco.pre_M_end_shares])
        print(len(_msk2[_msk2]))
        idf.loc[_msk2 , nco.start_eqs_lastM_end_shares] = False
        return idf

    pdf3 = check_start_shares_eq_last_end(pdf2)

    ##
    def drop_not_eq_start_share_with_last_end(idf) :
        idf = check_start_shares_eq_last_end(idf)
        _msk = idf[nco.start_eqs_lastM_end_shares].eq(False)
        idf = idf[~ idf.index.isin(_msk[_msk].index)]
        print(len(_msk[_msk]))
        return idf

    pdf4 = drop_not_eq_start_share_with_last_end(pdf3)

    ##
    def fillna_for_valid_sum_ones(idf , error_tol=100) :
        _msk = idf[g2n.end_shares_c].notna()
        _df = idf[_msk]
        end = _df[g2n.end_shares_c].astype(int)
        print(len(_msk[_msk]))

        _sum = _df[g2n.start_shares_c].fillna(0).astype(int)
        _sum += _df[g2n.change_buy_shares_c].fillna(0).astype(int)
        _sum -= _df[g2n.change_sell_shares_c].fillna(0).astype(int)
        _lb = end - error_tol
        _ub = end + error_tol
        _df['helper'] = _sum.gt(_lb)
        _df['helper'] &= _sum.lt(_ub)

        _msk1 = _df['helper'].eq(True)
        _df = _df[_msk1]
        print(len(_df))

        change_cols = [g2n.change_buy_shares_c , g2n.change_sell_shares_c]
        idf.loc[_df.index , change_cols] = idf[change_cols].fillna(0)
        return idf

    pdf5 = fillna_for_valid_sum_ones(pdf4)

    ##
    def fill_na_end_shares_wt_next_month_start(idf) :
        _msk = idf[g2n.end_shares_c].isna()
        _msk &= idf[nco.next_M_start_shares].notna()
        idf.loc[_msk , g2n.end_shares_c] = idf[nco.next_M_start_shares]
        print(len(_msk[_msk]))
        return idf

    pdf5 = fill_na_end_shares_wt_next_month_start(pdf5)

    ##
    pdf6 = remove_all_nan_obs(pdf5)

    ##
    def drop_start_shares_col(idf) :
        idf = idf.drop(columns=g2n.start_shares_c)
        return idf

    pdf6 = drop_start_shares_col(pdf6)

    ##
    def drop_na_end_shares(idf) :
        _msk = idf[g2n.end_shares_c].isna()
        idf = idf[~ _msk]
        print(len(_msk[_msk]))
        return idf

    pdf6 = drop_na_end_shares(pdf6)

    ##
    def drop_helper_cols(idf) :
        idf = idf.drop(columns=[nco.shares_ch , nco.next_M_start_shares ,
                                nco.pre_M_end_shares ,
                                nco.start_eqs_lastM_end_shares])
        return idf

    pdf6 = drop_helper_cols(pdf6)

    ##
    def make_all_zero_end_shares_pct_zero(idf) :
        _msk = idf[g2n.end_shares_c].eq(0)
        idf.loc[_msk , g2n.end_pct_to_all_assets_c] = 0
        print(len(_msk[_msk]))
        return idf

    pdf6 = make_all_zero_end_shares_pct_zero(pdf6)

    ##
    def fix_type(idf) :
        _colsdct = {
                g2n.change_buy_shares_c  : None ,
                g2n.change_sell_shares_c : None ,
                g2n.end_shares_c         : None ,
                }
        for _col in _colsdct.keys() :
            _msk = idf[_col].notna()
            idf.loc[_msk , _col] = idf.loc[_msk , _col].astype(int)
        return idf

    pdf6 = fix_type(pdf6)

    ##
    pdf6 = pdf6.rename(columns={
            ODC.fundKey                 : 'Fund key' ,
            ODC.jmonth                  : 'JMonth' ,
            knc.key                     : 'Ticker' ,
            g2n.change_buy_shares_c     : 'Acquired Shares' ,
            g2n.change_sell_shares_c    : 'Sold Shares' ,
            g2n.end_shares_c            : 'Shares Holding-End of Month' ,
            g2n.end_pct_to_all_assets_c : 'Portion to All Funds Assets'
            })

    ##
    fn = 'Funds Portfolio-Monthly.xlsx'
    fu.save_df_as_a_nice_xl(pdf6 , d.fins / fn)

    ##

##


if __name__ == "__main__" :
    main()
    print(f'{Module_name} Done.')

def _test() :
    pass

    ##
