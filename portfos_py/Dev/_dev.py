def goup_in_dirs(dirs_attr_n , name_list: list = None) :
  if name_list is None :
    name_list = []

  dirs = DirectoriesRelations()
  dct = dirs.__dict__

  if dct[dirs_attr_n] == d.cwd :
    return list(reversed(name_list))
  else :
    name_list.append((dirs_attr_n , dct[dirs_attr_n][1]))
    return goup_in_dirs(dct[dirs_attr_n][0] , name_list)

def make_dirs(self) :
  hlp = []
  for ky in self.__dict__.keys() :
    if ky != nof(self.cwd) :
      hlp.append(goup_in_dirs(ky))

  _1st_lvl = [x for x in hlp if len(x) == 1]
  for el in _1st_lvl :
    hlp.remove(el)
    if el[0][1] :
      self.__dict__[el[0][0]] = self.cwd / el[0][1]
    else :
      self.__dict__[el[0][0]] = self.cwd / el[0][0]

  if len(hlp) > 0 :
    max_lvl = max([len(x) for x in hlp])
  else :
    max_lvl = 2
  for i in range(2 , max_lvl + 1) :
    _ith_lvl = [x for x in hlp if len(x) == i]
    for el in _ith_lvl :
      if el[-1][1] :
        self.__dict__[el[-1][0]] = (self.__dict__[el[-2][0]] / el[-1][1])
      else :
        self.__dict__[el[-1][0]] = (self.__dict__[el[-2][0]] / el[-1][0])

  def find_axis_labels_by_value(df , val , axis: {0 , 1} = 1) :
    od = df.applymap(lambda x : x == val)
    os = od.stack(dropna=True)
    os = os[os]
    oi = os.index
    ol = list(oi.get_level_values(axis).unique())
    ol.sort()
    return ol

  def find_specific_cols_by_value(df , val , cols_count) :
    cs = find_axis_labels_by_value(df , val)
    if len(cs) == cols_count :
      cs.sort()
      return cs
    return [None] * cols_count

  def reindex_cols_and_rows_as_int(df) :
    df = df.reset_index(drop=True)
    df = df.T.reset_index(drop=True).T
    return df

  def convert_psble_num_str_2float(istr) :
    try :
      o = float(istr)
      return o
    except ValueError :
      return istr
    except TypeError :
      return istr


  class AttrDict(dict) :
    def __init__(self , *args , **kwargs) :
      super(AttrDict , self).__init__(*args , **kwargs)
      self.__dict__ = self

def _add_new_index_levels_to_df(df: pd.DataFrame ,
                                new_index_levels_names ,
                                fill_val) :
  nil = new_index_levels_names
  new_index = df.index.copy()
  for ni in nil :
    new_index = _add_an_index_level_to_df(old_index=new_index ,
                                          fill_value=fill_val ,
                                          name=ni ,
                                          loc=-1)

  if len(df.index.in_a_sheet_names) == 1 :
    df = df.reindex(new_index , level=0)
  else :
    df = df.reindex(new_index)

  return df

def _add_an_index_level_to_df(old_index: pd.Index ,
                              fill_value: Any ,
                              name: str = None ,
                              loc: int = 0) -> pd.MultiIndex :
  """
  Expand a (multi)index by adding a level to it.

  :param old_index: The index to expand
  :param name: The name of the new index level
  :param fill_value: Scalar or list-like, the values of the new index level
  :param loc: Where to insert the level in the index, 0 is at the front,
  negative values count back from the rear end
  :return: A new multi-index with the new level added
  """
  loc = _handle_insert_loc(loc , len(old_index.in_a_sheet_names))
  old_index_df = old_index.to_frame()
  old_index_df.insert(loc , name , fill_value)
  new_index_names = list(old_index.in_a_sheet_names)  # sometimes new index level
  # in_a_sheet_names are invented when converting to
  # a df,
  new_index_names.insert(loc ,
                         name)  # here the original in_a_sheet_names are reconstructed
  new_index = pd.MultiIndex.from_frame(old_index_df , names=new_index_names)
  return new_index

def _handle_insert_loc(loc: int , n: int) -> int :
  """
  Computes the insert index from the right if loc is negative for a given
  size of sheetStem.
  """
  return n + loc + 1 if loc < 0 else loc

def _update_with_current_module_cache(df ,
                                      current_module_cache_fp ,
                                      new_cols_2add_and_update: list) :
  if current_module_cache_fp.exists() :
    cdf = pd.read_parquet(current_module_cache_fp)

    cols_2drop_from_cdf = cdf.columns.difference(df.columns)
    cdf = cdf.drop(columns=cols_2drop_from_cdf)

    for coln in new_cols_2add_and_update :
      if coln in cdf.columns :
        df = df.drop(columns=coln)
        _cdf = cdf[coln]
        df = df.join(_cdf)

    rows_just_on_cdf = cdf.index.difference(df.index)
    _cdf1 = cdf.loc[rows_just_on_cdf]
    df = df.append(_cdf1)
  return df

def save_as_pkl(data , pn) :
  with open(pn , 'wb') as file :
    pickle.dump(data , file)

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

##
options = Options()
# options.headless = True
options.add_argument('--disable-gpu')  # Last I checked this was necessary.
driver = webdriver.Chrome(chrome_options=options)
