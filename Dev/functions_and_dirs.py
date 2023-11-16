"""[Contains functions that are useful across modules]
    """
##
import pathlib
import re
import shutil
from itertools import product

import pandas as pd

def find_n_jmonth_before(cur_month , howmany=1) :
  if howmany == 1 :
    if cur_month % 100 == 1 :
      pre_month = (cur_month // 100 - 1) * 100 + 12
    else :
      pre_month = cur_month - 1
    return pre_month

  else :
    pre = find_n_jmonth_before(cur_month)
    return find_n_jmonth_before(pre , howmany - 1)

def find_n_jmonth_ahead(cur_month , howmany=1) :
  if howmany == 1 :
    if cur_month % 100 == 12 :
      next_month = (cur_month // 100 + 1) * 100 + 1
    else :
      next_month = cur_month + 1
    return next_month

  else :
    next_month = find_n_jmonth_ahead(cur_month)
    return find_n_jmonth_ahead(next_month , howmany - 1)

def shift_month(cur_month , howmany: int = 0) :
  if howmany == 0 :
    return cur_month
  elif howmany > 0 :
    return find_n_jmonth_ahead(cur_month , howmany)
  elif howmany < 0 :
    return find_n_jmonth_before(cur_month , abs(howmany))

def make_all_months_in_between(start_month , end_month) :
  all_months = []
  while start_month <= end_month :
    all_months.append(start_month)
    start_month = shift_month(start_month , 1)
  return all_months

def create_cartesian_product_dataframe(cols_uniq_vals: dict) :
  vals = list(cols_uniq_vals.values())
  cols = list(cols_uniq_vals.keys())
  cart = product(*vals)
  df = pd.DataFrame(cart , columns=cols)
  return df

# HELPERS
def backup_dir_in_parent_dir(the_dir: pathlib.Path) :
  parent = the_dir.parent
  backup_dir_n = the_dir.name + '_preVersion'
  local_backup_dirp = parent / backup_dir_n
  if local_backup_dirp.exists() :
    shutil.rmtree(local_backup_dirp)
  shutil.copytree(the_dir , local_backup_dirp)
  return local_backup_dirp

def del_all_files_in_dir_wt_suf(dirp , suf) :
  dirp = pathlib.Path(dirp)
  fps = get_all_fps_wt_suffix(dirp , suf)
  _ = [fp.unlink() for fp in fps]

def reorder_modules(py_dir) :
  local_backup_dirp = backup_dir_in_parent_dir(py_dir)
  del_all_files_in_dir_wt_suf(py_dir , '.py')

  cur_ord = get_all_fps_wt_suffix(local_backup_dirp , '.py')
  crct_ord = make_the_order_by_numbers(cur_ord)

  new_old = {}
  for _i , _fp in enumerate(crct_ord) :
    new_old[_fp.stem] = '_' + str(_i)

  for module_fp in cur_ord :
    new_name = new_old[module_fp.stem] + '.py'
    fp_new = py_dir / new_name
    print(module_fp.name , 'changed to:' , fp_new.name)

    shutil.copy2(module_fp , fp_new)

    file = open(fp_new , 'r')
    cod = file.read()
    file.close()

    pat = r"Module_name = '.+\.py'"
    cod = re.sub(pat , f"Module_name = '{new_name}'" , cod)

    for ky , val in new_old.items() :
      pat_1 = f'from {py_dir.name}\.{ky} import'
      cod = re.sub(pat_1 , f'from {py_dir.name}.{val} import' , cod)

    f = open(fp_new , 'w')
    f.write(cod)
    f.close()

def _test() :
  pass

  ##
  reorder_modules(d.code)

  ##
