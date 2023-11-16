# %%
import os
import re
import requests
import shutil
import glob
import numpy as np
import pandas as pd
from unidecode import unidecode
import sys
from persiantools import characters
from persiantools import digits


code_backupshare_dir = '/Users/mahdimir/Dropbox/R-Heidari-Mir/Proj-Mutual Funds-Month Portfolio/code'

py_dir = os.getcwd()
within_exe_data_dir = py_dir + '/within_exe_data'
proj_dir = os.path.dirname(py_dir)

htmls_with_xls_links_dir = proj_dir + '/htmls_with_xls_links'
excels_dir = proj_dir + '/excels'
excels_as_prq_dir = proj_dir + '/excels_as_parquet'
cleaned_sheets_dir = proj_dir + '/cleaned_sheets'
portfo_sheets_dir = proj_dir + '/portfo_sheets'

prqsuf = ".parquet"

backup_codes_on_cloud(code_backupshare_dir)


def wos(inp) :
	inp = str(inp)
	inp = inp.replace(' ' , '')
	inp = inp.replace('\u200c' , '')
	inp = inp.replace('\u202b' , '')
	inp = inp.replace('\n' , '')
	inp = inp.replace('\r\n' , '')
	inp = inp.replace('\t' , '')
	inp = characters.ar_to_fa(inp)
	inp = digits.ar_to_fa(inp)
	inp = digits.fa_to_en(inp)
	return inp


def ar_to_fa(inp) :
	inp = characters.ar_to_fa(inp)
	inp = digits.ar_to_fa(inp)
	inp = digits.fa_to_en(inp)
	inp = inp.replace('\u200c' , ' ')
	inp = inp.replace('\u202b' , ' ')
	inp = inp.replace('\n' , ' ')
	inp = inp.replace('\r\n' , ' ')
	inp = inp.replace('\t' , ' ')
	return inp


def find_date(string) :
	string1 = str(string)
	string1 = string1[string1.find("/") - 4 : string1.find("/") + 6]
	string1 = string1.replace("/" , "")
	date1 = str(unidecode(string1))
	if re.match(r'\d{8}' , date1) :
		return date1

	str2 = str(string)
	str2 = str2.replace('/' , '')
	str2 = str2.replace('\u2215' , '')
	str2 = str(unidecode(str2))
	if re.match(r'\d{8}' , str2) :
		return str2

	return None


def find_date2(string) :
	string1 = str(string)
	string1 = string1[string1.find("/") - 4 : string1.find("/") + 6]
	string1 = string1.replace("/" , "")
	date1 = unidecode(string1)

	if re.fullmatch(r'\d{8}' , date1) :
		return int(date1)

	return -1



def find_all_locs_eq_val(dfobj: pd.DataFrame , value) :
	return dfobj[dfobj.eq(value)].stack().index.values



def backout_req_params(urlWtParams) :
	paramstr = urlWtParams.split("&" , 1)[1]
	params = paramstr.split("&")

	requestparams = {}
	for elem in params :
		x = elem.split("=")
		param = x[0]
		paramval = "true"

		if len(x) == 2 :
			paramval = x[1]

		requestparams[param] = paramval
	return requestparams


def get_keys(val , my_dict) :
	keys = []
	for ke , value in my_dict.items() :
		if val == value :
			keys.append(ke)
	return keys


def remove_all_nan_rows_and_cols(indf) :
	df1 = indf
	df1 = replace_none_strtings_with_nan(df1)
	df1 = df1.dropna(how = 'all')
	df1 = df1.dropna(how = 'all' , axis = 1)
	return df1.reset_index(drop = True).T.reset_index(drop = True).T


def reset_both_index(indf: pd.DataFrame) :
	df1 = indf.reset_index(drop = True).T.reset_index(drop = True).T
	df1 = df1.rename(columns = lambda x : str(x))
	return df1


def replace_none_strtings_with_nan(df: pd.DataFrame) :
	return df.replace([None , 'None' , 'nan'] , np.nan)



def find_jmonth(inp: str) :
	inp1 = str(inp)
	inp1 = wos(inp1)
	jdate = find_date(inp1)
	if jdate is not None :
		jdateint = int(jdate)
		jmonth = jdateint // 100
		return jmonth


# %%


def main() :
	pass


# %%

if __name__ == '__main__' :
	main()


# %%
