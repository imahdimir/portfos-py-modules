"""[cleans and checks grand1 fins]
    """
##
import re
import warnings

import pandas as pd

from Py\._10 import ODC , PSC
from Py\._17 import g2n
from Py.Helpers import functions_and_dirs as fu
from Py.Helpers.name_space import cte , knc

warnings.filterwarnings("ignore")

Module_name = '_17.py'

def clean_cols(idf , cols: list , ptr_notptr_rep: dict) :
    for col in cols :
        _msk0 = idf[col].notna()
        for ky , va in ptr_notptr_rep.items() :
            _ms = _msk0.copy()
            if ky[1] :
                _ms &= ~ idf[col].str.contains(ky[1])
            idf.loc[_ms , col] = idf.loc[_ms , col].str.replace(ky[0] , va)
    return idf

def def_helper_cols(idf , cols: list , suf) :
    for col in cols :
        idf[col + suf] = idf[col].copy()
    return idf

def is_float(ist: str) :
    try :
        float(ist)
        return True
    except (ValueError , TypeError) :
        return False

def replace_minute_vals_with_zero(idf , cols: list , value) :
    for col in cols :
        _ms = idf[col].apply(is_float)
        _ms1 = idf.loc[_ms , col].astype(float).lt(value)
        _ms1 = _ms1[_ms1]
        idf.loc[_ms1.index , col] = 0
    return idf

def convert_str_to_int(df , cols: list) :
    for _col in cols :
        _ms = df[_col].apply(is_float)
        _func = lambda x : round(float(x))
        df.loc[_ms , _col] = df.loc[_ms , _col].apply(_func)
    return df

def fix_int_type(df , cols) :
    df[cols] = df[cols].astype('Int64')
    return df

def remove_sapce_from_number(ist: str) :
    ptr = r'-?[\d\s,]*'
    if not re.fullmatch(ptr , str(ist)) :
        return ist

    ist_1 = re.sub(r'[-\s]' , '' , str(ist))

    spl = str(ist_1).split(',')
    if len(spl[-1]) != 3 and len(ist_1) > 3 :
        return ist

    for el in spl[:-1] :
        if len(el) > 3 :
            return ist

    out = re.sub(r'[-\s,]' , '' , str(ist))
    out = int(out)
    return out

def replace_empty_with_na(df) :
    df = df.replace('' , pd.NA)
    df = df.replace('None' , pd.NA)
    df = df.replace('<NA>' , pd.NA)
    return df

def main() :
    pass
    ##
    df = pd.read_parquet(cte.grand2)
    ##
    cols2drop = [knc.isFund , knc.isETF , knc.isListed , knc.fundManager ,
                 knc.fundInvMkt]
    df = df.drop(columns=cols2drop , errors='ignore')
    df1 = df.applymap(type)

    ##
    def fix_month_etc_type(idf) :
        idf[ODC.jmonth] = idf[ODC.jmonth].astype(int)  # no missing val
        for _col in [g2n.exp_date , g2n.strike_price] :
            _msk = idf[_col].notna()
            idf.loc[_msk , _col] = idf.loc[_msk , _col].apply(int)
            idf[_col] = idf[_col].astype('Int32')
        return idf

    df = fix_month_etc_type(df)

    ##
    num_cols_ex_pct = {
            g2n.start_shares_c        : None ,
            g2n.start_purch_cost_c    : None ,
            g2n.start_net_sale_val_c  : None ,
            g2n.change_buy_shares_c   : None ,
            g2n.change_buy_cost_c     : None ,
            g2n.change_sell_shares_c  : None ,
            g2n.change_sell_revenue_c : None ,
            g2n.end_shares_c          : None ,
            g2n.end_purchase_cost_c   : None ,
            g2n.end_net_sale_val_c    : None ,
            g2n.end_asset_price_c     : None
            }
    _2rep = {
            ('^(\s*0\s*)+$' , None) : '0' ,
            ('^-+' , None)          : '' ,
            ('،' , None)            : ',' ,
            }
    ##
    df = clean_cols(df , list(num_cols_ex_pct.keys()) , _2rep)
    df = replace_empty_with_na(df)
    df = df.dropna(subset=[knc.key])

    ##
    def remove_psbl_space_from_nums(idf , which_cols) :
        idf = fu.apply_func_on_notna_rows_of_cols(idf ,
                                                  which_cols ,
                                                  remove_sapce_from_number)
        return idf

    df = remove_psbl_space_from_nums(df , list(num_cols_ex_pct.keys()))
    ##
    df = replace_minute_vals_with_zero(df , list(num_cols_ex_pct.keys()) , 5)
    ##
    df = convert_str_to_int(df , list(num_cols_ex_pct.keys()))
    ##
    cols2drop = [PSC.end_asset_price , g2n.end_asset_price_c]
    df = df.drop(columns=cols2drop , errors='ignore')
    df1 = df.applymap(type)

    ##
    def mask_any_str_in_row(idf , which_cols) :
        _df = idf[which_cols]
        _df_1 = _df.applymap(lambda x : type(x) == str)
        _msk = _df_1.any(axis=1)
        return _msk

    cols_dct = {
            g2n.start_shares_c        : None ,
            g2n.start_purch_cost_c    : None ,
            g2n.start_net_sale_val_c  : None ,
            g2n.change_buy_shares_c   : None ,
            g2n.change_buy_cost_c     : None ,
            g2n.change_sell_shares_c  : None ,
            g2n.change_sell_revenue_c : None ,
            g2n.end_shares_c          : None ,
            g2n.end_purchase_cost_c   : None ,
            g2n.end_net_sale_val_c    : None ,
            }
    cols = list(cols_dct.keys())
    ##
    msk = mask_any_str_in_row(df , cols)
    df1 = df[msk]
    ##
    df = df[~ msk]
    ##
    df = fix_int_type(df , cols)

    ##
    def make_abs(idf , which_cols: list) :
        idf[which_cols] = idf[which_cols].abs()
        return idf

    df = make_abs(df , cols)
    ##
    _2rep_1 = {
            ('/' , None) : '.' ,
            }
    ##
    df = clean_cols(df , [g2n.end_pct_to_all_assets_c] , _2rep_1)
    ##
    df = replace_minute_vals_with_zero(df ,
                                       [g2n.end_pct_to_all_assets_c] ,
                                       1e-10)

    ##
    def convert_pct_to_portion(idf) :
        _col = g2n.end_pct_to_all_assets_c
        _msk = mask_any_str_in_row(idf , [_col])
        _msk &= idf[_col].str.contains(r'%')
        idf[_col] = idf[_col].str.replace(r'%' , '')
        idf.loc[_msk , _col] = idf.loc[_msk , _col].apply(float) / 1e2
        return idf

    df = convert_pct_to_portion(df)

    ##
    def convert_none_to_np_nan(idf) :
        _col = g2n.end_pct_to_all_assets_c
        _msk = idf[_col].isna()
        idf.loc[_msk , _col] = pd.NA
        return idf

    df = convert_none_to_np_nan(df)

    ##
    def convert_to_float(idf) :
        _col = g2n.end_pct_to_all_assets_c
        _msk = idf[_col].notna()
        idf.loc[_msk , _col] = idf.loc[_msk , _col].apply(float)
        return idf

    df = convert_to_float(df)

    ##
    msk = mask_any_str_in_row(df , [g2n.end_pct_to_all_assets_c])
    df1 = df[msk]
    ##
    df = df[~ msk]
    ##
    df.to_parquet(cte.grand3 , index=False)
    ##
    pass

##


if __name__ == "__main__" :
    main()
    print(f'{Module_name} Done.')

def _test() :
    pass

    ##
    x = 'اختیارخ شپنا'
    ptrn = r'اختیارخ'
    print(re.findall(ptrn , x))

    ##
    ts = pd.Series(data=[1 , 2 , 3])
    ts.index[0]

    def remove_sapce_from_number_with_care(ist: str) :
        ptr = r'-?[\d\s,]*'

        fnd = re.findall(ptr , str(ist))
        if len(fnd) != 2 or fnd[1] != '' :
            return [None] * 2

        txt = re.sub(ptr , '' , str(ist))
        txt = txt.strip()
        if txt == '' :
            txt = None

        dgt = fnd[0]
        dgt = re.sub(r'[-\s]' , '' , dgt)

        if not re.fullmatch('[\d,]+' , dgt) :
            return [None] * 2

        lst = dgt.split(',')
        if len(lst[-1]) != 3 :
            return [None] * 2

        for el in lst[:-1] :
            if len(el) > 3 :
                return [None] * 2

        dgt1 = ''.join(lst)
        return txt , dgt1

    def split_asset_name_shares_num(idf , which_col) :
        idf[['txt' ,
             'digit']] = idf.apply(lambda x : remove_sapce_from_number_with_care(
                x[which_col]) , axis=1 , result_type='expand')
        return idf

    ##
    df = split_asset_name_shares_num(df , g2n.asset_name_c)

    ##
    def filter_helping_ones(idf , col1 , col2) :
        msk0 = idf['txt'].eq(idf[col1]) | idf['txt'].eq('')
        idf.loc[msk0 , 'txt'] = None
        msk1 = idf['digit'].eq(idf[col2])
        idf.loc[msk1 , 'digit'] = None
        return idf

    ##
    df = filter_helping_ones(df , g2n.asset_name_c , g2n.start_shares_c)

    ##
    def mask_helping_ones(idf) :
        msk0 = idf['txt'].notna()
        msk0 |= idf['digit'].notna()
        return msk0

    ##
    mask = mask_helping_ones(df)
    df1 = df[mask]

    ##
    def replace_digit(idf , digit_col) :
        msk0 = idf['digit'].notna() & idf['digit'].ne('')
        msk1 = idf[digit_col].isna()
        msk1 |= idf[digit_col].str.replace(r'[\s,-]' , '').eq(
                idf['digit'].astype(str))
        msk0 &= msk1
        idf.loc[msk0 , digit_col] = idf['digit']
        return idf

    ##
    df = replace_digit(df , g2n.start_shares_c)
    ##

    df = split_asset_name_shares_num(df , g2n.start_shares_c)
    ##
    df = filter_helping_ones(df , g2n.asset_name_c , g2n.start_shares_c)
    ##
    mask = mask_helping_ones(df)
    df1 = df[mask]
    ##
    df = replace_digit(df , g2n.start_shares_c)

    ##
    def split_asset_name_shares_num_1(idf , which_col) :
        idf['digit'] = idf.apply(lambda x : remove_sapce_from_number_with_care(
                x[which_col]) , axis=1 , result_type='expand')
        return idf

    def filter_helping_ones_1(idf , col1) :
        msk1 = idf['digit'].eq(idf[col1])
        idf.loc[msk1 , 'digit'] = None
        return idf

    def mask_helping_ones_1(idf) :
        msk0 = idf['digit'].notna()
        return msk0

    def wrapper_1(idf , which_col) :
        idf = split_asset_name_shares_num_1(idf , which_col)
        idf = filter_helping_ones_1(idf , which_col)
        _ms = mask_helping_ones_1(idf)
        _df = idf[_ms]
        print('len df1' , len(_df))
        idf = replace_digit(idf , which_col)
        return idf , _df

    ##
    df , df1 = wrapper_1(df , g2n.start_purch_cost_c)
    ##
    df , df1 = wrapper_1(df , g2n.start_net_sale_val_c)
    ##
    df , df1 = wrapper_1(df , g2n.change_buy_shares_c)
    ##
    df , df1 = wrapper_1(df , g2n.change_buy_cost_c)
    ##
    df , df1 = wrapper_1(df , g2n.change_sell_shares_c)
    ##
    df , df1 = wrapper_1(df , g2n.change_sell_revenue_c)
    ##
    df , df1 = wrapper_1(df , g2n.end_shares_c)
    ##
    df , df1 = wrapper_1(df , g2n.end_asset_price_c)
    ##
    df , df1 = wrapper_1(df , g2n.end_purchase_cost_c)
    ##
    df , df1 = wrapper_1(df , g2n.end_net_sale_val_c)

    ##
    split_duos = {
            0  : (g2n.start_purch_cost_c , g2n.start_shares_c) ,
            1  : (g2n.start_net_sale_val_c , g2n.start_shares_c) ,
            2  : (g2n.change_buy_shares_c , g2n.start_net_sale_val_c) ,
            3  : (g2n.change_buy_cost_c , g2n.change_buy_shares_c) ,
            4  : (g2n.change_sell_shares_c , g2n.change_buy_cost_c) ,
            5  : (g2n.change_sell_revenue_c , g2n.change_sell_shares_c) ,
            6  : (g2n.end_shares_c , g2n.change_sell_revenue_c) ,
            7  : (g2n.end_asset_price_c , g2n.end_shares_c) ,
            8  : (g2n.end_purchase_cost_c , g2n.end_asset_price_c) ,
            9  : (g2n.end_net_sale_val_c , g2n.end_purchase_cost_c) ,
            10 : (g2n.end_pct_to_all_assets_c , g2n.end_net_sale_val_c) ,
            }

    ##
    df = split_data_for_split_duos(df , split_duos)
    ##
    y2 = 66042.269
    x1_2 = 84855.096
    x2_2 = 280
    b1 = 0.72
    b2 = 2.73
    yx1 = 74778.346
    yx2 = 4250.9
    x1x2 = 4796
    sum = y2 + b1 * x1_2 + b2 * x2_2 - 2 * b1 * yx1 - 2 * b2 * yx2 + 2 * b1 * b2 * x1x2
    sum
    1 - sum / y2
