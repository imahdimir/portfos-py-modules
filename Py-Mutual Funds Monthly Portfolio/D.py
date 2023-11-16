# %%


from io import StringIO
from os.path import exists

import pandas as pd
from lxml import etree

import main as m


parser = etree.HTMLParser()

# %%


cur_n = 'D'
pre_n = 'C'

# %%


cur_prq = m.within_exe_data_dir + f'/{cur_n}' + m.prqsuf
pre_prq = m.within_exe_data_dir + f'/{pre_n}' + m.prqsuf


# %%


def extract_xl_dl(htmlPN) :
    with open(htmlPN , "r") as f :
        html = f.read()
    tree = etree.parse(StringIO(html) , parser)
    ndsWOnclick = tree.xpath("//*[@onclick]")
    onclickAttVal = [x.attrib["onclick"] for x in ndsWOnclick]
    return clean_links(onclickAttVal)


def clean_links(onclickVals) :
    if onclickVals :
        links = list(set(onclickVals))  # removes duplicated links
        links = [x for x in links if "DownloadFile" in str(x)]
        links = [findBetween(x , "('" , "')") for x in links]
        return links
    return []


def check_num_links(htmlPN) :
    links = extract_xl_dl(htmlPN)
    if len(links) == 0 :
        return "not_found"
    elif len(links) == 1 :
        return links[0]
    else :
        return "more_than_one"


def findBetween(s , start , end) :
    return (s.split(start))[1].split(end)[0]


def main() :
    pass

    # %%


    df = m.read_parquet(pre_prq)
    df

    # %%
    cond1 = df[f'{pre_n}_ToDownLoadHtml'].eq(True)
    cond1 &= df['IsHtmlDownloaded'].eq(True)
    cond1[cond1]

    # %%
    flt = df[cond1]
    flt

    # %%
    if len(flt) > 0 :
        df.loc[flt.index , 'XlUrl'] = df.loc[flt.index , 'TracingNo'].apply(
                lambda x : check_num_links(m.htmls_with_xls_links_dir + '/' + str(x) + '.html'))
    df

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
