# %%


from asyncio import run , wait
import os

import nest_asyncio
import pandas as pd
from aiohttp import ClientSession
import re

import main as m


nest_asyncio.apply()

# %%


cur_n = 'C'
pre_n = 'B'

cur_prq = m.within_exe_data_dir + f'/{cur_n}' + m.prqsuf
pre_prq = m.within_exe_data_dir + f'/{pre_n}' + m.prqsuf


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


def main() :
    pass
    nest_asyncio.apply()

    # %%


    df = m.read_parquet(pre_prq)
    df

    # %%
    # assert len(df["TracingNo"].unique()) == len(df)

    # %%
    df["JDateFromTitle"] = df['Title'].apply(m.find_date2)
    df['JMonthFromTitle'] = df['JDateFromTitle'] // 100
    df

    # %%
    cond = df["HasAttachment"]
    cond &= df['LetterCode'].ne('ن-۳۱')
    cond[cond]

    # %%
    cond &= ~(df["Title"].str.contains("اصل ۴۴"))
    cond &= ((df["Title"].str.contains("دوره ۱ ماهه")) | (df["Title"].str.contains("دوره ی ۱ ماهه")))
    cond &= ~(df["LetterCode"].eq("ن-۳۱"))
    cond[cond]

    # %%
    df.loc[cond , f'{cur_n}_ToDownLoadHtml'] = True

    # %%
    df.loc[cond , "AttFullUrl"] = "https://codal.ir" + df["AttachmentUrl"]
    df

    # %%
    df.loc[cond , "IsHtmlDownloaded"] = df["TracingNo"].apply(
            lambda x : os.path.exists(m.htmls_with_xls_links_dir + '/' + str(x) + '.html'))
    df

    # %%
    cond2 = cond & df['IsHtmlDownloaded'].eq(False)
    cond2[cond2]

    # %%
    flt = df[cond2]
    flt

    # %%
    clstrsInd = m.return_clusters_indices(flt)

    # %%
    for i in range(0 , len(clstrsInd) - 1) :
        strtI = clstrsInd[i]
        endI = clstrsInd[i + 1]
        print(f"Cluster index of {strtI} to {endI}")

        corrInd = flt.iloc[strtI : endI].index

        urls = df.loc[corrInd , "AttFullUrl"]
        filePNs = m.htmls_with_xls_links_dir + '/' + df.loc[corrInd , "TracingNo"].astype(str) + '.html'

        run(html_reading_main(urls , filePNs))

    # %%
    df.loc[cond , "IsHtmlDownloaded"] = df["TracingNo"].apply(
            lambda x : os.path.exists(m.htmls_with_xls_links_dir + '/' + str(x) + '.html'))

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
