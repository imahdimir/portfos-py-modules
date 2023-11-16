"""[downloads codal page jsons with wanted filters]

    """

##

import asyncio
import json
import math
import warnings
from pathlib import Path , PosixPath

import nest_asyncio
import numpy as np
import requests
from aiohttp import ClientSession

from Helper.name_space import d

warnings.filterwarnings("ignore")
nest_asyncio.apply()

Module_name = '_0.py'

class ConstantUrls :
  def __init__(
      self
      ) :
    self.url_wt_params_0 = "https://codal.ir/ReportList.aspx?search&LetterType=-1&AuditorRef=-1&PageNumber=1&Audited&NotAudited&IsNotAudited=false&Childs&Mains&Publisher=false&CompanyState=-1&Category=3&CompanyType=3&Consolidatable&NotConsolidatable"
    self.url_wt_params_1 = "https://codal.ir/ReportList.aspx?search&LetterType=151&AuditorRef=-1&PageNumber=1&Audited&NotAudited&IsNotAudited=false&Childs&Mains&Publisher=false&CompanyState=-1&Category=3&CompanyType=-1&Consolidatable&NotConsolidatable"
    self.search_url = "https://search.codal.ir/api/search/v2/q"

consu = ConstantUrls()

def is_machine_connected_to_internet(
    host="https://google.com"
    ) :
  try :
    requests.get(host)
    print("Connected to internet!")
    return True
  except ConnectionError :
    print("No internet!")
    return False

def get_all_fps_wt_suffix(
    the_dir: {PosixPath , str} ,
    suffix: str
    ) :
  fps = sorted(list(Path(the_dir).glob(f'*{suffix}')))
  print('count:' , len(fps))
  return fps

def backout_query_params_as_dict(
    url_with_params: str
    ) :
  paramstr = url_with_params.split("&" , 1)[1]
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

async def _download_page_json(
    params ,
    url=consu.search_url
    ) :
  async with ClientSession(trust_env = True) as session :
    async with session.get(url , verify_ssl = False , params = params) as resp :
      content = await resp.json()
      print(f"Finished downloading")
      return content

async def _write_to_json(
    content ,
    file_pn
    ) :
  with open(file_pn , "w") as f :
    json.dump(content , f)
  print(f'saved as {file_pn}')

async def _web_scrape_task(
    param ,
    file_pn
    ) :
  content = await _download_page_json(param)
  await _write_to_json(content , file_pn)

async def _pages_reading_main(
    all_params ,
    all_file_pns
    ) :
  tasks = []
  for u , f in zip(all_params , all_file_pns) :
    tasks.append(_web_scrape_task(u , f))
  await asyncio.wait(tasks)

def main() :
  pass

  ##
  assert is_machine_connected_to_internet()

  ##
  urls = [consu.url_wt_params_0 , consu.url_wt_params_1]
  for _ind , url_pa in enumerate(urls) :
    pass

    ##
    def _manual_run_only() :
      url_pa = consu.url_wt_params_0
      _ind = 0

      ##
      _manual_run_only()

    ##
    req_params = backout_query_params_as_dict(url_pa)
    print(req_params)

    ##
    fps = get_all_fps_wt_suffix(d.json , '.json')
    stems = [x.stem for x in fps]
    wanted_sts = [x for x in stems if x.split('-')[0] == str(_ind)]
    pgs_scraped_bef = len(wanted_sts)
    print('pages scraped before num: ' , pgs_scraped_bef)

    ##
    task = _download_page_json(req_params)
    _1st_pg_data = asyncio.run(task)

    ##
    total_pages = _1st_pg_data["Page"]
    print(f'Total pages are {total_pages}')

    ##
    if pgs_scraped_bef == 0 :
      pages_2crawl = total_pages
    else :
      pages_2crawl = total_pages - pgs_scraped_bef + 1
    print(f'Scrape first {pages_2crawl} pages.')

    ##
    params_2crawl = []
    fpns = []
    for page_num in range(pages_2crawl + 1) :
      page_reqparam = req_params.copy()
      page_reqparam['PageNumber'] = str(page_num)
      params_2crawl.append(page_reqparam)
      fpns.append(d.json / f"{_ind}-{total_pages - page_num}.json")
    print(params_2crawl[-1])

    ##
    _sub_list_num = math.ceil(len(params_2crawl) / 100)
    sub_lists = np.array_split(params_2crawl , _sub_list_num)
    sub_fps = np.array_split(fpns , math.ceil(len(fpns) / 100))

    ##
    for i , (subl , subfp) in enumerate(zip(sub_lists , sub_fps)) :
      print(i , '\n' , subl , '\n' , subfp)
      params = subl
      file_pns = subfp
      asyncio.run(_pages_reading_main(params , file_pns))

##


if __name__ == "__main__" :
  main()
  print(f'{Module_name} is done!')

def _test() :
  pass

  ##

  ##
