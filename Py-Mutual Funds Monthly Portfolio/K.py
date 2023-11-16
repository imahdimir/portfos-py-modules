# %%

from os.path import exists
import signal

import pandas as pd
import numpy as np
import re
from multiprocessing import cpu_count
from multiprocess import Pool

import main as m


# %%


cur_n = 'K'
pre_n = 'J'

# %%


cur_prq = m.within_exe_data_dir + f'/{cur_n}' + m.prqsuf
pre_prq = m.within_exe_data_dir + f'/{pre_n}' + m.prqsuf

symbs_2drop = ['زر' ,
               'گوهر' ,
               'طلا' ,
               'مفید بازار' ,
               'لوتوس طلا' ,
               'زرافشان طلا' ,
               'پاسارگاد نیکو' ,
               ]


# %%


class TimeoutError(Exception) :
    pass


# noinspection PyUnusedLocal
def handler(signum , frame) :
    raise TimeoutError()


signal.signal(signal.SIGALRM , handler)

# %%


search_itmes = {
        'start_date'               : ['ابتدای دوره' , 'اول دوره'] ,
        'com_name_header'          : ['نام شرکت' , 'شرکت' , 'نام سهم' , 'سهم' ,
                                      'شرکتسرمایهپذیروصندوقسرمایهگذاری' , 'AssetName' , 'نامسهام'] ,
        'end_date'                 : ['انتهای دوره' , 'پایان دوره'] ,
        'pct_of_assets_col'        : ['درصد به کل دارایی ها' , 'درصد به کل دارایی‌های صندوق' ,
                                      'درصد به كل  دارایی‌ها' , 'درصدکلبهدارایی' , 'درصدبهکلداراییصندوق'] ,
        'change_in_period'         : ['تغییرات طی دوره' , 'تغییرات'] ,
        'number_of_shares'         : ['تعداد'] ,
        'cost_of_shares_purchased' : ['بهای تمام شده'] ,
        'net_sale_value'           : ['خالص ارزش فروش'] ,
        'end_share_price'          : ['قیمت بازار هر سهم' , 'قیمت بازار' , 'قیمتبازارهرسهم'] ,
        'period_buy'               : ['خرید طی دوره'] ,
        'period_sell'              : ['فروش طی دوره'] ,
        'sell_rev'                 : ['مبلغ فروش'] ,

        }

wos_srch = {}
for key in search_itmes :
    wos_srch[key] = [m.wos(q) for q in search_itmes[key]]

ilocs_srchk = {
        (0 , 0)  : 'com_name_header' ,
        (1 , 0)  : 'com_name_header' ,
        (2 , 0)  : 'com_name_header' ,
        (0 , 1)  : 'start_date' ,
        (0 , 2)  : 'start_date' ,
        (0 , 3)  : 'start_date' ,
        (0 , 8)  : 'end_date' ,
        (0 , 9)  : 'end_date' ,
        (0 , 10) : 'end_date' ,
        (0 , 11) : 'end_date' ,
        (1 , 1)  : 'number_of_shares' ,
        (2 , 1)  : 'number_of_shares' ,
        (2 , 4)  : 'number_of_shares' ,
        (2 , 6)  : 'number_of_shares' ,
        (1 , 8)  : 'number_of_shares' ,
        (2 , 8)  : 'number_of_shares' ,
        (1 , 2)  : 'cost_of_shares_purchased' ,
        (2 , 2)  : 'cost_of_shares_purchased' ,
        (2 , 5)  : 'cost_of_shares_purchased' ,
        (1 , 10) : 'cost_of_shares_purchased' ,
        (2 , 10) : 'cost_of_shares_purchased' ,
        (1 , 3)  : 'net_sale_value' ,
        (2 , 3)  : 'net_sale_value' ,
        (1 , 11) : 'net_sale_value' ,
        (2 , 11) : 'net_sale_value' ,
        (1 , 4)  : 'period_buy' ,
        (1 , 5)  : 'period_buy' ,
        (1 , 6)  : 'period_sell' ,
        (1 , 7)  : 'period_sell' ,
        (2 , 7)  : 'sell_rev' ,
        (1 , 9)  : 'end_share_price' ,
        (2 , 9)  : 'end_share_price' ,
        (0 , 12) : 'pct_of_assets_col' ,
        (1 , 12) : 'pct_of_assets_col' ,
        (2 , 12) : 'pct_of_assets_col' ,
        }

uniq_iloc_srch = np.unique(list(ilocs_srchk.values()))

cols_to_drop_values = ['افزایش سرمایه' , 'افزایش سرمایه طی دوره' , 'مبلغ افزایش سرمایه' , 'ناشیازافزایشسرمایه' ,
                       'تغییراتبهایتمامشده' , 'تعدادخودم' , 'مغایرت' , 'ارزشیابی' , 'کارمزدسهام' , 'کارمزداوراق' ,
                       'کارمزدصندوق' , 'کارمزدسلف']

cols_to_drop_srch = [m.wos(w) for w in cols_to_drop_values]

cols_2drop_more = ['قیمت']

portfo_sheet = ['1-1-سرمایه‌گذاری در سهام و حق تقدم سهام' ,
                '1-1-سرمایه‌گذاری در سهام و حق تقدم سهام وصندوق‌های سرمایه‌گذاری' ,
                '1-1-سرمایه‌گذاری در سهام و حق تقدم سهام  و صندوق ها' ,
                '1-1-سرمایه گذاری در سهام و حق تقدم' ,
                '1-سرمایه گذاری در سهام']


# %%


def del_s_dot_one_dash(inp) :
    inp = str(inp)
    inp = inp.replace('.' , '')
    inp = inp.replace('-' , '')
    inp = inp.replace(' ' , '')
    inp = inp.replace('1' , '')
    inp = inp.replace('۱' , '')
    inp = inp.replace('\u200c' , '')
    inp = inp.replace('\u202b' , '')
    return inp


portfo_srch = [del_s_dot_one_dash(x) for x in portfo_sheet]

# %%


second_row_hdr_srch = wos_srch['period_buy'] + wos_srch['period_sell']

groups_sh = {
        'start'  : {
                0 : ['com_name_header'] , 1 : ['number_of_shares'] , 2 : ['cost_of_shares_purchased'] ,
                3 : ['net_sale_value']
                } ,
        'change' : {
                4 : ['period_buy' , 'number_of_shares'] , 5 : ['cost_of_shares_purchased'] ,
                6 : ['period_sell' , 'number_of_shares'] , 7 : ['sell_rev']
                } ,
        'end'    : {
                8  : ['number_of_shares'] , 9 : ['end_share_price'] , 10 : ['cost_of_shares_purchased'] ,
                11 : ['net_sale_value'] , 12 : ['pct_of_assets_col']
                }
        }

# %%


outputs = ['Err_Pattern' , 'MathcedhHdr' , 'StartEndJMonth']

outputs_dict = m.make_output_dict(outputs)


# %%


def del_spars_rows(indf: pd.DataFrame , min_notna) :
    dim_cou = indf.count(axis = 1)
    dim_cou = dim_cou[dim_cou >= min_notna]
    return indf.loc[dim_cou.index].reset_index(drop = True)


def del_spars_cols(indf , min_notna) :
    each_col_cou = indf.count()
    each_col_cou_gtm = each_col_cou[each_col_cou >= min_notna].index
    return indf[each_col_cou_gtm].T.reset_index(drop = True).T


def remove_cols_with_nan_header(indf) :
    hdr = indf.iloc[1 : 3]
    hdrcou = hdr.count()
    hdrcou_0 = hdrcou[hdrcou == 0]
    hdrcou_0_indices = hdrcou_0.index.values
    return indf.drop(columns = hdrcou_0_indices)


def remove_axes_with_only_zero_nan(indf: pd.DataFrame) :
    values = [np.nan , '0' , '0.0' , 0]
    cols_ch = indf.isin(values).all()
    cols_2drop = cols_ch[cols_ch].index.values
    outdf = indf.drop(columns = cols_2drop)
    rows_ch = outdf.isin(values).all(axis = 1)
    rows_2drop = rows_ch[rows_ch]
    outdf = outdf.drop(index = rows_2drop.index.values)
    return outdf


def cols_2drop_with_values(indf: pd.DataFrame , cols_2drop_list) :
    val_check = indf.isin(cols_2drop_list).any()
    cols_2drop = val_check[val_check].index.values
    outdf = indf.drop(columns = cols_2drop)
    return outdf


def swap_2_cols(indf: pd.DataFrame , col1 , col2) :
    df1 = indf.copy()
    cols = df1.columns.tolist()
    cols_str = [str(x) for x in cols]
    col1_str = str(col1)
    col2_str = str(col2)
    a = cols_str.index(col1_str)
    b = cols_str.index(col2_str)
    cols[b] , cols[a] = cols[a] , cols[b]
    df1 = df1[cols]
    return df1


# %%


# noinspection PyTypeChecker
class portfo_sheets :


    def __init__(self , prqbn: str , correctdate , jmonth_title: int) :
        self.prqbn = prqbn
        self.prqpn = m.portfo_sheets_dir + '/' + self.prqbn + m.prqsuf
        self.saveprq = m.cleaned_sheets_dir + '/' + self.prqbn + m.prqsuf
        self.correctdate = correctdate
        self.jmonth_title = jmonth_title

        self.df = m.read_parquet(self.prqpn)
        self.df = self.df.applymap(m.wos)
        self.df = cols_2drop_with_values(self.df , cols_to_drop_srch)
        self.df = m.replace_none_strtings_with_nan(self.df)
        self.clndf = self.df.copy()
        self.enddate = None
        self.prev_month = None
        self.wos_search = None

        self.output = outputs_dict.copy()


    def add_dates_to_wos_srch(self) :
        self.wos_search = wos_srch.copy()
        self.wos_search['start_date'].append(self.prev_month)
        self.wos_search['start_date'].append(m.find_n_month_before(self.prev_month , 1))
        self.wos_search['end_date'].append(self.enddate)


    def do_nothing(self) :
        return self.clndf


    def pre_cln0(self) :
        self.clndf = cols_2drop_with_values(self.clndf , cols_2drop_more)
        return self.clndf


    def pre_cln1(self) :
        df1 = self.clndf.copy()
        df2 = df1.applymap(del_s_dot_one_dash)
        check = df2.isin(portfo_srch)
        prtflocs = m.find_all_locs_eq_val(check , True)
        if prtflocs.size == 0 :
            return None
        prtfrows = [x[0] for x in prtflocs]
        maxprtfrow = np.max(prtfrows)
        df1 = df1.iloc[maxprtfrow + 1 :]
        self.clndf = df1.copy()
        return self.clndf


    def pre_cln2(self) :
        df1 = self.clndf.copy()
        check = df1.isin(self.wos_search['change_in_period'])
        prtflocs = m.find_all_locs_eq_val(check , True)
        if prtflocs.size == 0 :
            return None
        prtfrows = [x[0] for x in prtflocs]
        maxprtfrow = np.max(prtfrows)
        df1 = df1.iloc[maxprtfrow :]
        self.clndf = df1


    def pre_cln3(self) :
        df1 = self.clndf.copy()
        check = df1.isin(second_row_hdr_srch)
        prtflocs = m.find_all_locs_eq_val(check , True)
        if prtflocs.size == 0 :
            return None
        prtfrows = [x[0] for x in prtflocs]
        maxprtfrow = np.max(prtfrows)
        df1 = df1.iloc[maxprtfrow - 1 :]
        self.clndf = df1


    def cln1(self , min_nan_no = 2) :
        df1 = self.clndf.copy()
        df1 = remove_axes_with_only_zero_nan(df1)
        df1 = del_spars_cols(df1 , min_notna = min_nan_no)
        df1 = del_spars_rows(df1 , min_notna = min_nan_no)
        df1 = remove_axes_with_only_zero_nan(df1)
        df1 = remove_cols_with_nan_header(df1)
        ## assumed here that first 3 rows are header rows now

        if df1.iloc[3][df1.iloc[3].notna()].unique().tolist() == ['ریال'] :
            df1 = df1.drop(df1.iloc[3].name)

        ser3 = df1.iloc[2]
        cols_to_keep = ser3[~ ser3.isna()].index

        wo_hdr = df1.iloc[3 :]
        wo_hdr = remove_axes_with_only_zero_nan(wo_hdr)

        cols_to_drop = df1.columns.difference(cols_to_keep).difference(wo_hdr.columns)

        df1 = df1.drop(columns = cols_to_drop)

        self.clndf = m.reset_both_index(df1)
        return self.clndf


    def cln2(self) :
        self.cln1(3)
        return self.clndf


    def cln3(self) :
        self.cln1()
        df1 = self.clndf.copy()
        ser3 = df1.iloc[2]
        cols_to_keep = ser3[~ ser3.isna()].index

        wo_hdr = df1.iloc[3 :].copy()
        wo_hdr = remove_axes_with_only_zero_nan(wo_hdr)

        cols_to_drop = df1.columns.difference(cols_to_keep).difference(wo_hdr.columns)

        df1 = df1.drop(columns = cols_to_drop)

        self.clndf = m.reset_both_index(df1)
        return self.clndf


    def post_cln0(self) :
        df1 = self.clndf.copy()
        df1 = df1.iloc[: , : 13]
        self.clndf = df1
        return self.clndf


    def sort_after_cln(self) :
        df1 = self.clndf.copy()
        df1 = m.reset_both_index(df1)

        for gpn , gpval in groups_sh.items() :
            minc , maxc = list(gpval.keys())[0] , list(gpval.keys())[-1]

            for key2 , val2 in gpval.items() :
                gpdf = df1.iloc[: , minc : maxc + 1].copy()
                gpdftrue = gpdf.copy()
                gpdftrue = gpdftrue.applymap(lambda w : True).any()
                # print(gpdftrue)

                for el in val2 :
                    checks = gpdf.isin(wos_srch[el]).any()
                    gpdftrue = gpdftrue & checks

                # print(gpdftrue)
                any_true_cols = gpdftrue[gpdftrue]
                # print(any_true_cols)
                col_labels = any_true_cols.index.tolist()
                # print(col_labels)

                if len(col_labels) == 1 :
                    found_col_lbl = col_labels[0]
                    # print(found_col_lbl)
                    if not str(found_col_lbl) == str(key2) :
                        df1 = swap_2_cols(df1 , found_col_lbl , key2)
                    else :
                        pass
                        # print('correct loc')
                else :
                    pass
                    # print(f'len of col labels is {len(col_labels)}')

        self.clndf = df1.copy()
        return self.clndf


    def check_header(self) :
        indf = self.clndf.copy()
        if not (indf.shape[1] == 13 and indf.shape[0] >= 3) :
            self.output['Err_Pattern'] = 'ShapeErr'
            return None

        hdr = indf.iloc[0 :3].copy()
        df1 = hdr.copy()
        df1 = df1.applymap(lambda l : False)

        for k , val in ilocs_srchk.items() :
            if hdr.iat[k[0] , k[1]] is not None :
                if hdr.iat[k[0] , k[1]] in self.wos_search[val] :
                    df1.iat[k[0] , k[1]] = True

        strt_keys1 = m.get_keys('start_date' , ilocs_srchk)
        end_keys1 = m.get_keys('end_date' , ilocs_srchk)
        dates_locs1 = strt_keys1 + end_keys1

        hdr_date = hdr.applymap(m.find_jmonth)

        for el1 in dates_locs1 :
            val = ilocs_srchk[el1]
            if hdr_date.iat[el1[0] , el1[1]] is not None :
                if hdr_date.iat[el1[0] , el1[1]] in self.wos_search[val] :
                    df1.iat[el1[0] , el1[1]] = True

        for v1 in uniq_iloc_srch :
            kys = m.get_keys(v1 , ilocs_srchk)
            tru_false_vals = []
            for k in kys :
                tru_false_vals.append(df1.iloc[k[0] , k[1]])
            if not np.any(tru_false_vals) :
                self.output['Err_Pattern'] = str(v1) + '_err'
                return None

        self.output['MathcedhHdr'] = True


    def process(self) :
        self.enddate = self.correctdate
        if self.correctdate is None :
            self.enddate = self.jmonth_title

        self.prev_month = m.find_n_month_before(self.enddate , 1)
        self.add_dates_to_wos_srch()

        for pre_method in [self.do_nothing , self.pre_cln0 , self.pre_cln1 , self.pre_cln2 , self.pre_cln3] :
            try :
                pre_method()

                for method in [self.do_nothing , self.cln1 , self.cln2 , self.cln3] :
                    try :
                        method()

                        for post_method in [self.do_nothing , self.post_cln0 , self.sort_after_cln] :
                            try :
                                post_method()

                                for sortmet in [self.do_nothing , self.sort_after_cln] :
                                    sortmet()

                                    self.check_header()

                                    if self.output['MathcedhHdr'] :
                                        m.save_as_parquet(self.clndf , self.saveprq)
                                        self.output['Err_Pattern'] = None
                                        self.output['StartEndJMonth'] = (self.prev_month , self.enddate)
                                        return self.output

                                    self.clndf = self.df.copy()

                            except IndexError :
                                pass

                            self.clndf = self.df.copy()

                    except IndexError :
                        pass

                    self.clndf = self.df.copy()

            except IndexError :
                pass
            self.clndf = self.df.copy()

        return self.output


def target(bn_correctjmonth_dict: str , jmonthtitle: int , matched_hdr_dict: str , startenddict: str) :
    dct1 = eval(bn_correctjmonth_dict)
    hdr_check_dict = eval(matched_hdr_dict)
    startenddate = eval(startenddict)

    o1 , o2 , o3 = {} , {} , {}

    for key1 , val in dct1.items() :
        o1[key1] , o2[key1] , o3[key1] = None , None , None

        try :
            if hdr_check_dict[key1] :
                o1[key1] = None
                o2[key1] = True
                o3[key1] = startenddate[key1]
                continue
        except KeyError :
            pass

        signal.alarm(10)
        try :
            obj1 = portfo_sheets(key1 , val , jmonthtitle)
            out1 = obj1.process()
            o1[key1] = out1['Err_Pattern']
            o2[key1] = out1['MathcedhHdr']
            o3[key1] = out1['StartEndJMonth']
        except (ValueError , TimeoutError , FileNotFoundError) as e :
            o1[key1] = str(e)
            print(e)
        finally :
            signal.alarm(0)

    o1 = str(o1)
    o2 = str(o2)
    o3 = str(o3)

    return o1 , o2 , o3


def all_nan_dict_values(dictstr) :
    dct1 = eval(dictstr)
    vals = list(dct1.values())
    # print(vals)
    checknan = [x is None for x in vals]
    return np.all(checknan)


def main() :
    pass

    # %%


    df = m.read_parquet(pre_prq)
    df

    # %%
    df[outputs] = str({})

    # %%
    df = m.update_with_last_data(df , cur_prq)

    # %%
    cnd = df['BnCorrectJMonthDict'].notna()
    cnd &= ~ df['Symbol'].isin(symbs_2drop)
    cnd[cnd]

    # %%
    flt = df[cnd]
    flt

    # %%
    cores_n = cpu_count()
    print(f'Num of cores : {cores_n}')
    clstrsInd = m.return_clusters_indices(flt)
    pool = Pool(cores_n)

    # %%
    inputs = ['BnCorrectJMonthDict' , 'JMonthFromTitle' , outputs[1] , outputs[2]]

    # %%
    for i in range(0 , len(clstrsInd) - 1) :
        strtI = clstrsInd[i]
        endI = clstrsInd[i + 1]
        print(f"Cluster index of {strtI} to {endI}")

        corrInd = flt.iloc[strtI :endI].index

        inpa = []
        for el in inputs :
            inpa.append(df.loc[corrInd , el])

        inpzip = zip(*inpa)

        out = pool.starmap(target , inpzip)

        for j , outp in enumerate(outputs) :
            df.loc[corrInd , outp] = [w[j] for w in out]

        print(df.loc[corrInd , outputs])
        # break

    # %%
    m.save_as_parquet(df , cur_prq)

    # %%
    cnd2 = df['BnCorrectJMonthDict'].notna()
    cnd2 &= ~ df['Symbol'].isin(symbs_2drop)
    cnd2 &= df.loc[cnd2 , 'MathcedhHdr'].apply(lambda x : all_nan_dict_values(x))
    cnd2 &= df['JMonthFromTitle'].ge(139901)
    cnd2[cnd2]

    # %%
    flt2 = df[cnd2]
    flt2


# %%


if __name__ == '__main__' :
    main()

else :
    pass

    # %%
