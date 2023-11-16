# %%


from asyncio import run , wait
from os.path import exists
from os.path import getsize
from os.path import basename
import glob
from os import remove
import nest_asyncio
import pandas as pd
from aiohttp import ClientSession
import re
from requests.exceptions import ConnectionError
import requests

import main as m


cur_n = 'F'
pre_n = 'E1'

month_period_in_title = ['دوره ی ۱ ماهه' , 'دوره ۱ ماهه']

nest_asyncio.apply()

splash_url = "http://localhost:8050/render.html"

cur_prq = m.within_exe_data_dir + cur_n + m.prqsuf
pre_prq = m.within_exe_data_dir + pre_n + m.prqsuf

month_srch = [m.wos(x) for x in month_period_in_title]


async def download_html_text(url) :
    async with ClientSession() as session :
        async with session.get(splash_url , verify_ssl = False , params = {"url" : url , "wait" : 30 , "images" : 0}) \
                as resp :
            content = await resp.read()
        print(f"Finished downloading")
        return content


async def write_html_to_file(content , filePN) :
    print(filePN)
    with open(filePN , "wb") as f :
        f.write(content)


async def web_scrape_task(url , filePN) :
    content = await download_html_text(url)
    await write_html_to_file(content , filePN)


async def html_reading_main(allurls , allPNs) :
    tasks = []
    for u , f in zip(allurls , allPNs) :
        tasks.append(web_scrape_task(u , f))
    await wait(tasks)


def assert_splash_server() :
    try :
        requests.get(splash_url , params = {"url" : 'http://google.com' , "wait" : 3 , "images" : 0})
        print('Splash local server is running!')
        return True
    except ConnectionError :
        print('No Spalsh Server!')
        return False


def main() :
    pass
    nest_asyncio.apply()

    # %%


    df = pd.read_parquet(pre_prq)
    df

    # %%
    cond = df['LetterCode'].eq('ن-۳۱')
    c1 = len(cond[cond])
    cond[cond]

    # %%
    df.loc[cond , 'FullUrlForSheetId4'] = 'https://codal.ir' + df['Url'] + '&sheetId=4'

    # %%
    df.loc[cond , 'CheckTitleForMonth'] = df['Title'].apply(lambda x : m.any_of_list_isin(month_srch , m.wos(x)))

    # %%
    cond &= df.loc[cond , 'CheckTitleForMonth']
    cond[cond]
    if c1 != len(cond[cond]) :
        print('some_rows_lost')

    # %%
    df.loc[cond , 'IsHtmlSheet4Downloaded'] = df['TracingNo'].apply(
            lambda x : exists(m.htmlportfo_dir + str(x) + '.html'))

    # %%
    cond2 = cond & df['IsHtmlSheet4Downloaded'].eq(False)
    cond2[cond2]

    # %%
    flt = df[cond2]
    flt

    # %%
    clstrsInd = m.return_clusters_indices(flt , 20)

    # %%
    assert assert_splash_server() , 'Run Splash server : ` docker run -it -p 8050:8050 --rm scrapinghub/splash `'

    # %%
    for i in range(0 , len(clstrsInd) - 1) :
        strtI = clstrsInd[i]
        endI = clstrsInd[i + 1]
        print(f"Cluster index of {strtI} to {endI}")

        corrInd = flt.iloc[strtI : endI].index

        urls = df.loc[corrInd , "FullUrlForSheetId4"]
        filePNs = m.htmlportfo_dir + df.loc[corrInd , "TracingNo"].astype(str) + '.html'

        run(html_reading_main(urls , filePNs))

    # %%
    htmlpns = glob.glob(m.htmlportfo_dir + '*.html')
    htmlpns

    # %%
    timeout_error = '"error": 504, "type": "GlobalTimeoutError"'

    # %%
    timeouts = []
    for htpn in htmlpns :
        with open(htpn , 'r') as htmlf :
            htmlcont = htmlf.read()
        if timeout_error in htmlcont :
            print('TimeOut')
            timeouts.append(htpn)
            remove(htpn)

    # %%
    for htpn in htmlpns :
        if exists(htpn) :
            if getsize(htpn) < 29 * 10 ** 3 :
                htbn = basename(htpn)
                tracno = htbn.split('.')[0]
                tracnoint = int(tracno)
                df.at[tracnoint , 'BlankHtmlSheet4'] = True
                remove(htpn)
                print(htpn)

    # %%
    df.loc[cond , 'IsHtmlSheet4Downloaded'] = df['TracingNo'].apply(
            lambda x : exists(m.htmlportfo_dir + str(x) + '.html'))

    # %%
    df.to_parquet(cur_prq , index = False)
    df


# %%


if __name__ == '__main__' :
    main()
    print(f'{cur_n}.py done!')

# noinspection PyUnreachableCode
if False :
    pass

    # %%

    # %%
