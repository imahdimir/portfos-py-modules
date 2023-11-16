from pathlib import Path , PosixPath

class Directories :
  def __init__(
      self
      ) :
    self.cwd = Path.cwd()  # = Main proj dir

    self.code = self.cwd
    self.data = Path('/Users/mahdimir/Documents/portfos_data')
    self.cache = self.code / 'Cache'

    self.helper_data = self.data / 'm_helper_data'

    self.json = self.data / '0_jsons'
    self.html = self.data / '1_htmls'
    self.xl = self.data / '2_excels'
    self.pdf = self.data / '3_pdfs'

    self.man_clnd_sheets = self.data / 'm_clnd_sheets'
    self.xl_Sheets = self.data / '4_excel_sheets'
    self.xl_cln_sheet_0 = self.data / '5_clean_sheets_0'
    self.xl_cln_sheet_1 = self.data / '6_clean_sheets_1'

    self.adj_txt_based_pdfs = self.data / '7_adj_txt_based_pdfs'

  def make_not_existed_dirs(
      self
      ) :
    for val in self.__dict__.values() :
      if type(val) == PosixPath :
        if not val.exists() :
          val.mkdir()

d = Directories()
d.make_not_existed_dirs()

class Constants :
  def __init__(
      self
      ) :
    self.codal_ir = "https://codal.ir"
    self.codal_rep = self.codal_ir + '/Reports/'

    self.key_name_fp = '/Users/mahdimir/Dropbox/keyData/key_name/key_name.xlsx'

    self.allXlsStocksSheets = d.data / 'all_excels'

    self.suc = None

    self.grand1 = d.data / 'grand1.xlsx'
    self.grand2 = d.data / 'g2'
    self.grand3 = d.data / 'g3'

    for key , val in self.__dict__.items() :
      if val is None :
        self.__dict__[key] = key

cte = Constants()

class CodalTableColumns :
  def __init__(
      self
      ) :
    self.TracingNo = None
    self.SuperVision = None
    self.Symbol = None
    self.CompanyName = None
    self.UnderSupervision = None
    self.Title = None
    self.LetterCode = None
    self.SentDateTime = None
    self.PublishDateTime = None
    self.HasHtml = None
    self.Url = None
    self.HasExcel = None
    self.HasPdf = None
    self.HasXbrl = None
    self.HasAttachment = None
    self.AttachmentUrl = None
    self.PdfUrl = None
    self.ExcelUrl = None
    self.XbrlUrl = None
    self.TedanUrl = None
    self.IsEstimate = None

    colslist = []
    for attr_key in self.__dict__ :
      self.__dict__[attr_key] = attr_key
      colslist.append(attr_key)
    self.list = colslist

ctc = CodalTableColumns()

class CacheCols :
  def __init__(
      self
      ) :
    self.json_stem = None
    self.isMonthlyRep = None
    self.titleJDate = None
    self.titleJMonth = None
    self.cleanSym = None
    self.cleanCompanyName = None
    self.fundKey = None

    self.htmlFS = None  # File Stem
    self.htmlFUrl = None  # full url

    self.dlLink = None
    self.attFilesCount = None

    self.fileStem = None
    self.isFileHealthy = None
    self.fileSuf = None

    self.sheet_name = None
    self.is_sheet_hidden = None
    self.is_empty = None
    self.sheet_count = None
    self.not_hidden_sheets_cou = None
    self.sheet_stem = None

    self.modified_sheet_name = None
    self.is_stocks_sheet = None

    self.isManChecked = None
    self.sheet_clean_0_exception = None
    self.start_jm_from_sheet = None
    self.end_jm_from_sheet = None
    self.is_end_jm_ok = None
    self.before_clean_0_rows_count = None
    self.before_clean_0_cols_cou = None
    self.after_clean_0_rows_cou = None
    self.after_clean_0_cols_cou = None
    self.is_clean_sheet_0_suc = None
    self.revised_jmonth = None
    self.countCln0PerXl = None

    self.sheet_clean_1_exception = None
    self.is_clean_1_suc = None
    self.cols_cou = None

    self.pdf_pages_cou = None

    self.is_txt_based = None
    self.rotAng = None

    self.PdfRotStat = None

    self.PdfPg_IsTxtBased = None
    self.PdfPg_IsStockPortfo_0 = None
    self.PdfPg_Is1stPgAfStock = None
    self.PdfPg_IsStockPortfo_1 = None
    self.PdfPg_TblCou = None

    self.PdfStPg_TblNum = None
    self.PdfStPg_Exc = None
    self.PdfStPg_Tbl_FStem = None
    self.PdfStPg_BeforeRows = None
    self.PdfStPg_BeforeCols = None
    self.PdfStPg_AfterRows = None
    self.PdfStPg_AfterCols = None
    self.PdfStPg_IsClnSuc = None

    self.PdfStPg_Cln = None
    self.PdfStPg_IsClnSuc = None
    self.PdfStPg_ColsNum = None

    self.PdfSc1_IsSt = None
    self.PdfSc1_NotSt = None
    self.PdfSc2_IsSt = None
    self.PdfSc2_NotSt = None
    self.IsStFinal = None

    self.RotAng1 = None
    self.RotAng2 = None
    self.IsCropped1 = None
    self.IsCropped2 = None

    for key , val in self.__dict__.items() :
      if val is None :
        self.__dict__[key] = key

cc = CacheCols()

class CacheIndicesNames :
  def __init__(
      self
      ) :
    self.L0_TracingNo = None
    self.L1_HtmlSheetID = None
    self.L2_AttFile = None
    self.L3_SheetNum = None
    self.L4_PdfPage = None
    self.L5_PdfStocksTblNum = None
    for key , val in self.__dict__.items() :
      if val is None :
        self.__dict__[key] = key

indi = CacheIndicesNames()

class KeyNameCols :
  def __init__(
      self
      ) :
    self.key = None
    self.companyName = None

    for key , val in self.__dict__.items() :
      if val is None :
        self.__dict__[key] = key

knc = KeyNameCols()

class FundsTypes :
  def __init__(
      self
      ) :
    self.InStocks = None
    self.Mixed = None
    self.FixedIncome = None
    self.MarketMaker = None
    self.VC = None
    self.Gold = None
    self.RealEstate = None
    self.Project = None
    self.Goods = None
    self.FundInFund = None
    self.Private = None
    self.IndexFund = None
    self.Fixed = None
    for key , val in self.__dict__.items() :
      if val is None :
        self.__dict__[key] = key

ft = FundsTypes()

class AssetTypes :
  def __init__(
      self
      ) :
    self.preferred_shares = None
    self.call = None
    self.put = None
    self.sekke = None
    self.akhza = None
    self.ejare = None
    self.tese = None
    self.mosharekat = None

    self.future = None
    self.common_stocks = None
    for key , val in self.__dict__.items() :
      if val is None :
        self.__dict__[key] = key

asst = AssetTypes()
