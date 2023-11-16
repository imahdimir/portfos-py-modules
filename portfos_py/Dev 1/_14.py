"""[Reads Stocks Portfolio clened level 1]
    """
##
import multiprocessing as mp
import warnings

import pandas as pd

from main import (d , simpl_ret_cache_fp)
from Py\._10 import (col_vals_not_vals , find_col_name_by_header_value , ODC ,
                    PortfoSheetCols , PSC , )
from Py\._8 import any_of_list_isin_str_as_word
from Py.Helpers import functions_and_dirs as fu
from Py.Helpers.name_space import cc , knc

warnings.filterwarnings("ignore")

Module_name = '_14.py'

class CleanedStocksPgPortfoLvl1 :
    def __init__(self ,
                 fund_key ,
                 sheet_stem ,
                 revised_end_jmonth ,
                 load_dir ,
                 save_dir) :
        self.fund_key = fund_key
        self.jmonth = int(float(revised_end_jmonth))
        self.sheet_stem = str(sheet_stem)
        self.load_dir = load_dir
        self.save_dir = save_dir

        self.df = None
        self.hdf = None

        self.change_cells_xy = None
        self.min_chng_col = None
        self.max_chng_col = None

        self.start_section = None
        self.change_section = None
        self.end_section = None
        self.buy_section = None
        self.sell_section = None

        self.colsmap = None
        self.outdf = None

        self.is_cleaning_suc = False
        self.exception = None
        self.cols_cou = None

    def _print(self , *args) :
        print(self.sheet_stem , ': ' , *args)

    def _load_data(self) :
        fp = (self.load_dir / self.sheet_stem).with_suffix('.xlsx')
        self.df = pd.read_excel(fp , engine='openpyxl')
        self.hdf = self.df.iloc[:3 , :]

    def _find_the_change_cell(self) :
        chng = ['دوره' , 'تغییرات' , 'طی']
        df0 = self.hdf.applymap(lambda x : any_of_list_isin_str_as_word(x ,
                                                                        chng))
        df1 = df0.stack(dropna=True)
        cells = df1[df1].index
        self.change_cells_xy = cells

    def _find_change_cell_min_and_max_col(self) :
        self.min_chng_col = min(self.change_cells_xy.get_level_values(1))
        self.max_chng_col = max(self.change_cells_xy.get_level_values(1))

    def _define_each_section_df_header(self) :
        self.start_section = self.hdf.iloc[: , :self.min_chng_col]
        self.change_section = self.hdf.iloc[: ,
                              self.min_chng_col : max([self.max_chng_col ,
                                                       self.min_chng_col + 4])]
        self.end_section = self.hdf.iloc[: ,
                           max([self.max_chng_col , self.min_chng_col + 4]) :]

    def _find_buy_section(self) :
        kharid = ['خرید']
        df0 = self.change_section.applymap(lambda x : any_of_list_isin_str_as_word(
                x ,
                kharid))
        df1 = df0.stack(dropna=True)
        cells = df1[df1].index
        mincol = min(cells.get_level_values(1))
        self.buy_section = self.hdf.iloc[: , mincol :mincol + 2]

    def _find_sell_section(self) :
        labels = set(self.change_section.columns) - set(self.buy_section.columns)
        self.sell_section = self.hdf[list(labels)]

    def _build_cols_map(self) :
        self.colsmap = PortfoSheetCols().__dict__.copy()
        self.colsmap[PSC.asset_name] = 0
        hdr_df_map = {
                PSC.start_shares          : self.start_section ,
                PSC.start_purchase_cost   : self.start_section ,
                PSC.start_net_sale_val    : self.start_section ,

                PSC.change_buy_shares     : self.buy_section ,
                PSC.change_buy_cost       : self.buy_section ,

                PSC.change_sell_shares    : self.sell_section ,
                PSC.change_sell_revenue   : self.sell_section ,

                PSC.end_shares            : self.end_section ,
                PSC.end_asset_price       : self.end_section ,
                PSC.end_purchase_cost     : self.end_section ,
                PSC.end_net_sale_val      : self.end_section ,
                PSC.end_pct_to_all_assets : self.end_section ,
                }
        keys_2_iter = set(self.colsmap.keys()) - {PSC.asset_name}
        for coln in keys_2_iter :
            # print(self.colsmap)
            self.colsmap[coln] = find_col_name_by_header_value(idf_header=
                                                               hdr_df_map[
                                                                   coln] ,
                                                               vals_list=
                                                               col_vals_not_vals[
                                                                   coln][0] ,
                                                               not_vals_list=
                                                               col_vals_not_vals[
                                                                   coln][1])

    def _build_outdf(self) :
        self.outdf = pd.DataFrame()

        dcols = []
        for coln in self.colsmap.keys() :
            if self.colsmap[coln] in self.df.columns :
                self.outdf[coln] = self.df[self.colsmap[coln]]
                dcols.append(coln)

        for coln in ODC.__dict__.keys() :
            self.outdf[coln] = self.__dict__[coln]

        self.outdf = self.outdf[list(ODC.__dict__.keys()) + dcols]
        self.cols_cou = len(dcols)

    def _save_outdf(self) :
        fp = (self.save_dir / self.sheet_stem).with_suffix('.xlsx')
        fu.save_df_as_a_nice_xl(self.outdf , fp)
        print(self.sheet_stem , ': outdf saved.')
        self.is_cleaning_suc = True

    def process(self) :
        try :
            self._load_data()
            self._find_the_change_cell()
            self._find_change_cell_min_and_max_col()
            self._define_each_section_df_header()
            self._find_buy_section()
            self._find_sell_section()
            self._build_cols_map()
            self._build_outdf()
            self._save_outdf()
        except Exception as e :
            self._print(e)
            self.exception = str(e)

    def get_attrs(self) :
        out = {
                cc.PdfStPg_Cln      : self.exception ,
                cc.PdfStPg_IsClnSuc : self.is_cleaning_suc ,
                cc.PdfStPg_ColsNum  : self.cols_cou ,
                }
        return out

def _targ(fund_key ,
          sheet_stem ,
          revised_end_jmonth ,
          load_dir=d.PStPortfo1 ,
          save_dir=d.PStPortfo2) :
    oj = CleanedStocksPgPortfoLvl1(fund_key ,
                                   sheet_stem ,
                                   revised_end_jmonth ,
                                   load_dir ,
                                   save_dir)
    oj.process()
    out = oj.get_attrs()

    print(out)
    return out

def main() :
    pass
    ##
    pre_mod_name = fu.get_pre_mod_name(d.code , Module_name)
    cur_mod_cache_fp = simpl_ret_cache_fp(Module_name)
    pre_mod_cache_fp = simpl_ret_cache_fp(pre_mod_name)
    new_cols = {
            cc.PdfStPg_Cln      : None ,
            cc.PdfStPg_IsClnSuc : None ,
            cc.PdfStPg_ColsNum  : None ,
            }
    new_index_levels_names: list = []
    ##
    df = fu.load_update_df(pre_module_cache_fp=pre_mod_cache_fp ,
                           current_module_cache_fp=cur_mod_cache_fp ,
                           new_cols_2add_and_update=list(new_cols.keys()) ,
                           new_index_cols=new_index_levels_names)

    ##
    col2drop = {
            cc.PdfStPg_Exc        : None ,
            cc.PdfStPg_BeforeRows : None ,
            cc.PdfStPg_BeforeCols : None ,
            cc.PdfStPg_AfterRows  : None ,
            }
    df = df.drop(columns=col2drop.keys() , errors='ignore')

    ##
    def remove_exess_cleaned_sheets() :
        fps = fu.get_all_fps_wt_suffix(d.PStPortfo2 , '.xlsx')
        sts = [x.stem for x in fps]

        _cnd = df[cc.PdfStPg_IsClnSuc].eq(True)

        sht_2del = set(sts) - set(df.loc[_cnd , cc.PdfStPg_Tbl_FStem])
        for sht in sht_2del :
            (d.PStPortfo2 / sht).with_suffix('.xlsx').unlink()
            print(sht)

        print('Sheets to remove count:' , len(sht_2del))

    remove_exess_cleaned_sheets()

    ##
    def mask_sheets(idf) :
        _cnd = idf[cc.PdfStPg_IsClnSuc].eq(True)

        print(len(_cnd[_cnd]))
        return _cnd

    msk = mask_sheets(df)

    ##
    def cln0(idf , mask) :
        flt = idf[mask]
        print('len:' , len(flt))
        cpc = mp.cpu_count()
        pool = mp.Pool(cpc)
        lo = pool.starmap_async(_targ ,
                                zip(flt[knc.key] ,
                                    flt[cc.PdfStPg_Tbl_FStem] ,
                                    flt[cc.titleJMonth])).get()
        for key in lo[0].keys() :
            idf.loc[mask , key] = [x[key] for x in lo]
        return idf

    ##
    df = cln0(df , msk)

    ##
    fu.save_current_module_cache(df , cur_mod_cache_fp)

##
if __name__ == "__main__" :
    main()
    print(f'{Module_name} Done.')

def _test() :
    pass
    ##
    out = _targ('آهنگ سهام کیان' , '420519-N-N-0' , 'سهام' , 139610)

    ##
    oj = CleanedStocksPgPortfoLvl1('اتیمس' ,
                                   '484747-N-N-0-0' ,
                                   1939707 ,
                                   d.PStPortfo1 ,
                                   d.PStPortfo2)
    ##
    oj._load_data()
    oj._find_the_change_cell()
    oj._find_change_cell_min_and_max_col()
    oj._define_each_section_df_header()
    oj._find_buy_section()
    oj._find_sell_section()
    oj._build_cols_map()
    oj._build_outdf()
    oj._save_outdf()

    ##
    x = 'خريد طي دوره'
    any_of_list_isin_str_as_word(x , kharid)

    ##
