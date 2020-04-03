import pandas as pd
from io import StringIO
from contextlib import redirect_stdout, redirect_stderr
import re
from bs4 import BeautifulSoup

from external_external_functions import whitespace, table_checker

save_dir = '//luxor/data/branch/Environmental Baseline Data/Version 4 - Final/Saved2/'
# save_dir = 'C:/Users/rodijann/RegDocs/Saved/'

# regex expressions needed for the extractions
empty_line = r'<\/p>\s*<p ?\/?>'
punctuation = r'[^\w\s]'  # punctuation (not letter or number)
figure = r'(?im)(^Figure .*?\n?.*?)\.{2,}(.*)'
table = r'(?im)(^Table (?!of contents?).*?\n?.*?)\.{2,}(.*)'


def fig_checker(doc_text, doc_text_rotated, doc_id, toc_id, toc_page, word1_rex, word2_rex, s2_rex):
    page_list = []
    # check unrotated
    soup = BeautifulSoup(doc_text, 'lxml')
    pages = soup.find_all('div', attrs={'class': 'page'})
    for page_num, page in enumerate(pages):
        text_clean = re.sub(whitespace, ' ', page.text)
        # text_clean = re.sub(punctuation, ' ', text_clean)
        if re.search(word2_rex, text_clean) and re.search(s2_rex, text_clean):
            if (doc_id != toc_id) or (page_num != toc_page):
                page_list.append(page_num)

    # check rotated
    soup = BeautifulSoup(doc_text_rotated, 'lxml')
    pages = soup.find_all('div', attrs={'class': 'page'})
    for page_num, page in enumerate(pages):
        text_clean = re.sub(whitespace, ' ', page.text)
        # text_clean = re.sub(punctuation, ' ', text_clean)
        if re.search(word2_rex, text_clean) and re.search(s2_rex, text_clean):
            if (doc_id != toc_id) or (page_num != toc_page):
                page_list.append(page_num)
    return page_list


# function that takes ID of project, and finds locations of all the tables from that projects' TOC
# saves result to save_dir folder
def get_titles_tables(project):
    print(f"Starting {project}")
    df_tables = pd.read_csv(save_dir + 'all_tables.csv', encoding='utf-8-sig')
    df_tables = df_tables[df_tables['Project'] == project]  # filter out just current project
    df_tables['location_DataID'] = None
    df_tables['location_Page'] = None
    df_tables['count'] = 0
    df_project = pd.read_csv(save_dir + 'project_' + project + '.csv', encoding='utf-8-sig', index_col='DataID')
    df_project['Text_rotated'].fillna('', inplace=True)

    prev_id = 0
    for index, row in df_tables.iterrows():
        title = row['Name']
        # print(title)
        c = title.count(' ')
        if c >= 2:
            word1, word2, s2 = title.split(' ', 2)
        else:
            word1, word2 = title.split(' ', 1)
            s2 = ''
        word1_rex = r'(?i)\b' + word1 + r'\s'
        word2_rex = r'(?i)\b' + word2
        s1_rex = r'(?i)\b' + word1 + r'\s' + word2

        s2 = re.sub(punctuation, ' ', s2)
        s2 = re.sub(whitespace, ' ', s2)  # remove whitespace
        s2_rex = r'(?i)\b'
        for s in s2.split(' '):
            s2_rex = s2_rex + r'[^\w]*' + s
        s2_rex = s2_rex + r'\b'
        toc_id = row['DataID']
        project = row['Project']
        toc_page = row['TOC_Page']

        id_list = []
        page_list = []
        count = 0

        # first try previos id if exists
        if prev_id > 0:
            doc_id = prev_id
            doc_text = df_project.loc[doc_id, 'Text']
            doc_text_rotated = df_project.loc[doc_id, 'Text_rotated']
            success, output, page_list = table_checker(doc_text, doc_text_rotated, doc_id, toc_id, toc_page, s1_rex,
                                                       s2_rex)
            print(output)
            if len(page_list) > 0:
                id_list = [doc_id]
                count = len(page_list)

        # if didn't find try TOC document
        if count == 0:
            doc_id = toc_id
            doc_text = df_project.loc[doc_id, 'Text']
            doc_text_rotated = df_project.loc[doc_id, 'Text_rotated']
            success, output, page_list = table_checker(doc_text, doc_text_rotated, doc_id, toc_id, toc_page, s1_rex,
                                                       s2_rex)
            print(output)
            if len(page_list) > 0:
                id_list = [doc_id]
                count = len(page_list)
                prev_id = doc_id
            else:
                id_list = []
                count = 0

        # if fig still not found, go through all docs in this project and try to find the doc there
        if count == 0:
            for doc_id, doc in df_project.iterrows():
                if (doc_id != toc_id) and (doc_id != prev_id):
                    doc_text = df_project.loc[doc_id, 'Text']
                    doc_text_rotated = df_project.loc[doc_id, 'Text_rotated']
                    success, output, p_list = table_checker(doc_text, doc_text_rotated, doc_id, toc_id, toc_page,
                                                            s1_rex, s2_rex)
                    print(output)
                    if len(p_list) > 0:
                        id_list.append(doc_id)
                        page_list.extend(p_list)
                        count += len(p_list)
            if len(id_list) == 1:
                prev_id = id_list[0]

        df_tables.loc[index, 'location_DataID'] = str(id_list).replace('[', '').replace(']', '').strip()
        df_tables.loc[index, 'location_Page'] = str(page_list).replace('[', '').replace(']', '').strip()
        df_tables.loc[index, 'count'] = count
    df_tables.to_csv(save_dir + project + '-final_tables.csv', index=False, encoding='utf-8-sig')
    print(f"Finished {project}")


def get_titles_figures(project):
    buf = StringIO()
    with redirect_stdout(buf), redirect_stderr(buf):
        try:
            print(f"Starting {project}")
            df_figs = pd.read_csv(save_dir + 'all_figs.csv', encoding='utf-8-sig')
            df_figs = df_figs[df_figs['Project'] == project]  # filter out just current project
            df_figs['location_DataID'] = None
            df_figs['location_Page'] = None
            df_figs['count'] = 0
            df_project = pd.read_csv(save_dir + 'project_' + project + '.csv', encoding='utf-8-sig', index_col='DataID')
            df_project['Text_rotated'].fillna('', inplace=True)

            prev_id = 0
            for index, row in df_figs.iterrows():
                title = row['Name']
                # print(title)
                c = title.count(' ')
                if c >= 2:
                    word1, word2, s2 = title.split(' ', 2)
                else:
                    word1, word2 = title.split(' ', 1)
                    s2 = ''
                word2 = re.sub('[^a-zA-Z0-9]', '[^a-zA-Z0-9]', word2)
                word1_rex = r'(?i)\b' + word1 + r'\s'
                word2_rex = r'(?i)\b' + word2

                # s2 = re.sub(punctuation, ' ', s2)
                s2 = re.sub(whitespace, ' ', s2)  # remove whitespace
                s2 = re.sub(punctuation, '.*', s2)

                s2_rex = r'(?i)\b'
                for s in s2.split(' '):
                    s2_rex = s2_rex + r'[^\w]*' + s
                s2_rex = s2_rex + r'\b'
                toc_id = row['DataID']
                toc_page = row['TOC_Page']

                id_list = []
                page_list = []
                count = 0

                # first try previos id if exists
                if prev_id > 0:
                    doc_id = prev_id
                    doc_text = df_project.loc[doc_id, 'Text']
                    doc_text_rotated = df_project.loc[doc_id, 'Text_rotated']
                    page_list = fig_checker(doc_text, doc_text_rotated, doc_id, toc_id, toc_page, word1_rex,
                                            word2_rex, s2_rex)
                    if len(page_list) > 0:
                        id_list = [doc_id]
                        count = len(page_list)

                # if didn't find try TOC document
                if count == 0:
                    doc_id = toc_id
                    doc_text = df_project.loc[doc_id, 'Text']
                    doc_text_rotated = df_project.loc[doc_id, 'Text_rotated']
                    page_list = fig_checker(doc_text, doc_text_rotated, doc_id, toc_id, toc_page, word1_rex, word2_rex,
                                            s2_rex)
                    if len(page_list) > 0:
                        id_list = [doc_id]
                        count = len(page_list)
                        prev_id = doc_id
                    else:
                        id_list = []
                        count = 0

                # if fig still not found, go through all docs in this project and try to find the doc there
                if count == 0:
                    for doc_id, doc in df_project.iterrows():
                        if (doc_id != toc_id) and (doc_id != prev_id):
                            doc_text = df_project.loc[doc_id, 'Text']
                            doc_text_rotated = df_project.loc[doc_id, 'Text_rotated']
                            p_list = fig_checker(doc_text, doc_text_rotated, doc_id, toc_id, toc_page, word1_rex,
                                                 word2_rex, s2_rex)
                            if len(p_list) > 0:
                                id_list.append(doc_id)
                                page_list.extend(p_list)
                                count += len(p_list)
                    if len(id_list) == 1:
                        prev_id = id_list[0]

                df_figs.loc[index, 'location_DataID'] = str(id_list).replace('[', '').replace(']', '').strip()
                df_figs.loc[index, 'location_Page'] = str(page_list).replace('[', '').replace(']', '').strip()
                df_figs.loc[index, 'count'] = count

            df_figs.to_csv(save_dir + project + '-final_figs.csv', index=False, encoding='utf-8-sig')
            print(f"Finished {project}")
        except Exception as e:
            print(e)
        finally:
            return buf.getvalue()
            # return True
