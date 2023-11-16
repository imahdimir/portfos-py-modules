# %%


import glob
import json
import os
import re

import pandas as pd
from unidecode import unidecode

import main as m


# %%


cur_n = 'B'

# %%


cur_prq = m.within_exe_data_dir + f'/{cur_n}' + m.prqsuf

codal_table_cols = [
        'TracingNo' ,
        'SuperVision' ,
        'Symbol' ,
        'CompanyName' ,
        'UnderSupervision' ,
        'Title' ,
        'LetterCode' ,
        'SentDateTime' ,
        'PublishDateTime' ,
        'HasHtml' ,
        'Url' ,
        'HasExcel' ,
        'HasPdf' ,
        'HasXbrl' ,
        'HasAttachment' ,
        'AttachmentUrl' ,
        'PdfUrl' ,
        'ExcelUrl' ,
        'XbrlUrl' ,
        'TedanUrl'
        ]

not_useful_cols = ['SuperVision' , 'UnderSupervision' , 'HasPdf' , 'HasXbrl' , 'PdfUrl' , 'XbrlUrl' , 'TedanUrl' ,
                   'HasExcel' , 'ExcelUrl']

useful_codal_cols = codal_table_cols
for el in not_useful_cols :
    useful_codal_cols.remove(el)


# %%


def extract_data_from_json(jspn) :
    with open(jspn , 'r') as jsfile :
        json_dict = json.load(jsfile)
    letters = json_dict['Letters']
    outdf = pd.DataFrame(columns = useful_codal_cols)
    for row in letters :
        new_entry = {}
        for key in useful_codal_cols :
            new_entry[key] = row[key]
        outdf = outdf.append(new_entry , ignore_index = True)
    return outdf


def convert_jdatetime_to_int(pubdate) :
    pubdate = pubdate.replace('/' , '')
    pubdate = pubdate.replace(':' , '')
    pubdate = pubdate.replace(' ' , '')
    pubdate = unidecode(pubdate)

    if re.match(r'\d{14}' , pubdate) :
        return int(pubdate)
    else :
        return -1


def main() :
    pass

    # %%


    json_pns = glob.glob(m.within_exe_data_dir + '/*.json')
    json_pns

    # %%
    new_data = pd.DataFrame(columns = useful_codal_cols)
    for jspn in json_pns :
        print(jspn)
        jsondf = extract_data_from_json(jspn)
        new_data = new_data.append(jsondf)
    new_data

    # %%
    # new_data.to_parquet(m.within_exe_data_dir + cur_n + '_NewData' + m.prqsuf , index = False)

    # %%
    new_data['PublishDateTime'] = new_data['PublishDateTime'].apply(convert_jdatetime_to_int)
    new_data['SentDateTime'] = new_data['SentDateTime'].apply(convert_jdatetime_to_int)
    new_data

    # %%
    if os.path.exists(cur_prq) :
        pre_data = m.read_parquet(cur_prq)
        new_data = new_data.append(pre_data)
    new_data

    # %%
    # new_data['SuperVision'] = new_data['SuperVision'].astype(str)

    # %%
    new_data = new_data.drop_duplicates()
    new_data

    # %%
    assert len(new_data['TracingNo'].unique()) == len(new_data) , 'Not_Unique_TracingNo'

    # %%
    new_data = new_data.sort_values(by = ['PublishDateTime'] , ascending = False)
    new_data

    # %%
    m.save_as_parquet(new_data , cur_prq)

    # %%
    for jpn in json_pns :
        os.remove(jpn)
        print(f'Deleted : {jpn}')


# %%


if __name__ == '__main__' :
    main()
    print(f'{cur_n}.py done!')

# noinspection PyUnreachableCode
if False :
    pass

    # %%
    pre_data1 = pd.read_parquet(cur_prq)

    # %%
    tcnd1 = pre_data1['HasExcel'].eq(True)
    tcnd2 = pre_data1['ExcelUrl'].ne('')
    tcnd3 = pre_data1['HasAttachment'].eq(True)
    tcnd2.eq(tcnd1).all()


    # %%
