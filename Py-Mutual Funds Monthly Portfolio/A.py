# %%


from asyncio import run , wait
import json
import os
import requests

from aiohttp import ClientSession
import nest_asyncio
import urllib3

import main as m


nest_asyncio.apply()
urllib3.disable_warnings()

# %%


cur_n = 'A'
pgs_pn = m.within_exe_data_dir + '/' + "A_PagesCrawled.txt"
srchUrl = "https://search.codal.ir/api/search/v2/q"
urlWtparams = "https://codal.ir/ReportList.aspx?search&LetterType=-1&AuditorRef=-1&PageNumber=1&Audited&NotAudited" \
              "&IsNotAudited=false&Childs&Mains&Publisher=false&CompanyState=-1&Category=3&CompanyType=3" \
              "&Consolidatable&NotConsolidatable "


# %%


async def download_page_json(param) :
    async with ClientSession() as session :
        async with session.get(srchUrl , verify_ssl = False , params = param) as resp :
            content = await resp.json()
            print(f"Finished downloading")
            return content


async def write_to_json(content , filePN) :
    with open(filePN , "w") as f :
        json.dump(content , f)
    print(f'saved as {filePN}')


async def web_scrape_task(param , filePN) :
    content = await download_page_json(param)
    await write_to_json(content , filePN)


async def pages_reading_main(all_params , allPNs) :
    tasks = []
    for u , f in zip(all_params , allPNs) :
        tasks.append(web_scrape_task(u , f))
    await wait(tasks)


def main() :
    pass
    nest_asyncio.apply()

    # %%


    assert m.is_machine_connected_to_internet()

    # %%
    if os.path.exists(pgs_pn) :
        with open(pgs_pn , "r") as f :
            pgscraped = int(f.read())
    else :
        pgscraped = 0

    print(f'{pgscraped} pages has been crawled before!')

    # %%
    reqparams = m.backout_req_params(urlWtparams)
    reqparams

    # %%
    first_page_response = requests.get(srchUrl , verify = False , params = reqparams)
    print(first_page_response)

    assert first_page_response.status_code == 200

    # %%
    frst_page_data = first_page_response.json()
    frst_page_data

    # %%
    total_pages = frst_page_data["Page"]
    print(f'Total pages are {total_pages}')

    # %%
    pages_2crawl = total_pages - pgscraped + 1
    print(f'I am going to scrape first {pages_2crawl} pages, which are new!')

    # %%
    all_params_2crawl = []
    all_fpns = []

    for page_num in range(1 , pages_2crawl + 1) :
        page_reqparam = reqparams.copy()
        page_reqparam['PageNumber'] = page_num
        all_params_2crawl.append(page_reqparam)
        all_fpns.append(m.within_exe_data_dir + '/' + str(page_num) + '.json')

    all_fpns
    # all_params_2crawl[-1]

    # %%
    clusters = m.return_clusters_indices(all_params_2crawl)

    # %%
    for i in range(len(clusters) - 1) :
        start_index = clusters[i]
        end_index = clusters[i + 1]
        print(f'{start_index} to {end_index}')

        params = all_params_2crawl[start_index : end_index]
        fpns = all_fpns[start_index : end_index]

        run(pages_reading_main(params , fpns))

    # %%
    with open(pgs_pn , 'w') as f :
        f.write(str(total_pages))


# %%
if __name__ == '__main__' :
    main()
    print(f'{cur_n}.py Done!')

# noinspection PyUnreachableCode
if False :
    pass


    # %%


    # %%
