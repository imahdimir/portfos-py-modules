# %%

from asyncio import run , wait
import os
import nest_asyncio
import pandas as pd
from aiohttp import ClientSession
import re
from asyncio import run , wait
from openpyxl import load_workbook
from zipfile import BadZipFile
from multiprocess import Pool
from multiprocessing import cpu_count

import main as m


nest_asyncio.apply()

# %%


cur_n = 'E'
pre_n = 'D'

# %%


cur_prq = m.within_exe_data_dir + f'/{cur_n}' + m.prqsuf
pre_prq = m.within_exe_data_dir + f'/{pre_n}' + m.prqsuf

outputs = ['IsXlDownloaded' , 'IsXlCorrupt']


# %%


async def download_html_text(url) :
    async with ClientSession() as session :
        async with session.get(url , verify_ssl = False) as resp :
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


def is_xl_corrupt(tracingno: int) :
    xlpn = m.excels_dir + '/' + str(tracingno) + '.xlsx'
    print(tracingno)
    try :
        load_workbook(xlpn , read_only = True , data_only = True , keep_links = False , keep_vba = False)
        return False
    except BadZipFile :
        return True


def main() :
    pass
    nest_asyncio.apply()

    # %%


    df = m.read_parquet(pre_prq)
    df

    # %%
    df[outputs] = None

    # %%
    df = m.update_with_last_data(df , cur_prq)

    # %%
    cond = df['XlUrl'].notna()
    cond &= df['IsXlCorrupt'].isna()
    cond[cond]

    # %%
    df.loc[cond , 'XlFullUrl'] = 'https://codal.ir/Reports/' + df['XlUrl']

    # %%
    df.loc[cond , 'IsXlDownloaded'] = df['TracingNo'].apply(
            lambda x : os.path.exists(m.excels_dir + '/' + str(x) + '.xlsx'))
    df

    # %%
    cond2 = cond & df['IsXlDownloaded'].eq(False)
    cond2[cond2]

    # %%
    flt = df[cond2]
    flt

    # %%
    clstrsInd = m.return_clusters_indices(flt , 20)

    # %%
    for i in range(0 , len(clstrsInd) - 1) :
        strtI = clstrsInd[i]
        endI = clstrsInd[i + 1]
        print(f"Cluster index of {strtI} to {endI}")

        corrInd = flt.iloc[strtI : endI].index

        urls = df.loc[corrInd , "XlFullUrl"]
        filePNs = m.excels_dir + '/' + df.loc[corrInd , "TracingNo"].astype(str) + '.xlsx'

        run(html_reading_main(urls , filePNs))

    # %%
    df.loc[cond , 'IsXlDownloaded'] = df['TracingNo'].apply(
            lambda x : os.path.exists(m.excels_dir + '/' + str(x) + '.xlsx'))

    # %%
    cn3 = cond
    cn3 &= df['IsXlDownloaded'].eq(True)

    # %%
    flt3 = df[cn3]
    flt3

    # %%
    cores_n = cpu_count()
    print(f'Num of cores : {cores_n}')
    clusters = m.return_clusters_indices(flt3)
    pool = Pool(cores_n)

    # %%
    for i in range(0 , len(clusters) - 1) :
        start_index_no = clusters[i]
        end_index_no = clusters[i + 1]
        print(f"Cluster index of {start_index_no} to {end_index_no}")

        corresponding_index_labels = flt3.iloc[start_index_no :end_index_no].index

        tracingnos = df.loc[corresponding_index_labels , 'TracingNo']

        out = pool.map(is_xl_corrupt , tracingnos)

        df.loc[corresponding_index_labels , outputs[1]] = out

        print(df.loc[corresponding_index_labels , outputs])
        # break

    # %%
    cnd4 = df['IsXlCorrupt'].eq(True)
    cnd4 &= df['IsXlDownloaded'].eq(True)
    cnd4[cnd4]

    # %%
    df.loc[cnd4 , 'TracingNo'].apply(lambda x : os.remove(m.excels_dir + '/' + str(x) + '.xlsx'))

    # %%
    df.loc[cond , 'IsXlDownloaded'] = df['TracingNo'].apply(
            lambda x : os.path.exists(m.excels_dir + '/' + str(x) + '.xlsx'))

    # %%
    m.save_as_parquet(df , cur_prq)


# %%


if __name__ == '__main__' :
    main()
    print(f'{cur_n}.py done!')

# noinspection PyUnreachableCode
if False :
    pass

    # %%

    # %%
