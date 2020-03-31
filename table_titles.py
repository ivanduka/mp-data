import pandas as pd
import re
from bs4 import BeautifulSoup

# get all project IDs (in a list)
projects_path = 'F:/Environmental Baseline Data/Version 4 - Final/Indices/Index 2 - PDFs for Major Projects with ESAs.xlsx'
all_projects = pd.read_excel(projects_path)
projects = all_projects['Hearing order'].unique()
# print(projects)
# projects = ['GH-1-2003', 'HW-001- # to test

# regex expressions needed for the extractions
empty_line = r'<\/p>\s*<p ?\/?>'
whitespace = r'\s+'  # all white space
punctuation = r'[^\w\s]'  # punctuation (not letter or number)
figure = r'(?im)(^Figure .*?\n?.*?)\.{2,}(.*)'
table = r'(?im)(^Table (?!of contents?).*?\n?.*?)\.{2,}(.*)'
save_dir = 'F:/Environmental Baseline Data/Version 4 - Final/Saved2/'
load_pickles = 1  # if document pickles need to be loaded to projects (only need to do once)

# function that takes ID of project, and finds locations of all the tables from that projects' TOC
# saves result to save_dir folder


def get_titles(project):
    df_tables = pd.read_csv(save_dir + 'all_tables.csv', encoding='utf-8-sig')
    df_tables = df_tables[df_tables['Project'] == project]  # filter out just current project
    df_tables['location_DataID'] = None
    df_tables['location_Page'] = None
    df_tables['count'] = 0
    df_project = pd.read_csv(save_dir + 'project_' + project + '.csv', encoding='utf-8-sig', index_col='DataID')

    for index, row in df_tables.iterrows():
        if project == row['Project']:
            title = row['Name']
            # print(title)
            c = title.count(' ')
            if c >= 2:
                word1, word2, s2 = title.split(' ', 2)
            else:
                word1, word2 = title.split(' ', 1)
                s2 = ''
            word1_rex = r'(?i)\b' + word1 + r'\s'
            word2_rex = r'(?i)\b' + word2 + r'\s'
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

            # first open up same doc (same as TOC) and try to find the fig there
            soup = BeautifulSoup(df_project.loc[row['DataID'], 'Text'], 'lxml')
            pages = soup.find_all('div', attrs={'class': 'page'})
            for page_num, page in enumerate(pages):
                text_clean = re.sub(whitespace, ' ', page.text)
                text_clean = re.sub(punctuation, ' ', text_clean)
                # if re.search(s1_rex, page.text) and re.search(s2_rex, re.sub(whitespace, ' ', page.text)):
                if re.search(word1_rex, page.text) and re.search(word2_rex, page.text) and re.search(s2_rex,
                                                                                                     text_clean):
                    if page_num != toc_page:
                        id_list = [row['DataID']]
                        page_list.append(page_num)
                        count += 1

            # if fig not found, try in rotated document
            if count == 0:
                soup = BeautifulSoup(df_project.loc[row['DataID'], 'Text_rotated'], 'lxml')
                pages = soup.find_all('div', attrs={'class': 'page'})
                for page_num, page in enumerate(pages):
                    text_clean = re.sub(whitespace, ' ', page.text)
                    text_clean = re.sub(punctuation, ' ', text_clean)
                    # if re.search(s1_rex, page.text) and re.search(s2_rex, re.sub(whitespace, ' ', page.text)):
                    if re.search(word1_rex, page.text) and re.search(word2_rex, page.text) and re.search(s2_rex,
                                                                                                         text_clean):
                        if page_num != toc_page:
                            id_list = [row['DataID']]
                            page_list.append(page_num)
                            count += 1

            # if fig still not found, go through all docs in this project and try to find the doc there
            if count == 0:
                for doc_id, doc in df_project.iterrows():
                    soup = BeautifulSoup(doc['Text'], 'lxml')
                    pages = soup.find_all('div', attrs={'class': 'page'})
                    for page_num, page in enumerate(pages):
                        text_clean = re.sub(whitespace, ' ', page.text)
                        text_clean = re.sub(punctuation, ' ', text_clean)
                        # if re.search(s1_rex, page.text) and re.search(s2_rex, re.sub(whitespace, ' ', page.text)):
                        if re.search(
                                word1_rex, page.text) and re.search(
                                word2_rex, page.text) and re.search(
                                s2_rex, text_clean):
                            if (doc_id != toc_id) or (page_num != toc_page):
                                if doc_id not in id_list:
                                    id_list.append(doc_id)
                                page_list.append(page_num)
                                count += 1

            # if still not found, go through all rotated figs and try there
            if count == 0:
                for doc_id, doc in df_project.iterrows():
                    soup = BeautifulSoup(doc['Text_rotated'], 'lxml')
                    pages = soup.find_all('div', attrs={'class': 'page'})
                    for page_num, page in enumerate(pages):
                        text_clean = re.sub(whitespace, ' ', page.text)
                        text_clean = re.sub(punctuation, ' ', text_clean)
                        # if re.search(s1_rex, page.text) and re.search(s2_rex, re.sub(whitespace, ' ', page.text)):
                        if re.search(
                                word1_rex, page.text) and re.search(
                                word2_rex, page.text) and re.search(
                                s2_rex, text_clean):
                            if (doc_id != toc_id) or (page_num != toc_page):
                                if doc_id not in id_list:
                                    id_list.append(doc_id)
                                page_list.append(page_num)
                                count += 1

            df_tables.loc[index, 'location_DataID'] = str(id_list).replace('[', '').replace(']', '').strip()
            df_tables.loc[index, 'location_Page'] = str(page_list).replace('[', '').replace(']', '').strip()
            df_tables.loc[index, 'count'] = count
    df_tables.to_csv(save_dir + project + '-final_tables.csv', index=False, encoding='utf-8-sig')


# assuming saved_dir has all the project csvs already, and has the all_tables file in it already
# go through every table and find it in some document and record ID and real page num
# this is the part that we could multiproccess
for project in projects:
    get_titles(project)

# now need to take all the resulting csv's and put them all together
# do this after all the projects are done above
# this does not need to be multiproccessed
data = []
for project in projects:
    df = pd.read_csv(save_dir + project + '-final_tables.csv', encoding='utf-8-sig')
    data.append(df)
df_all = pd.concat(data, axis=0, ignore_index=True)
df_all.to_csv(save_dir + 'final_tables.csv', index=False, encoding='utf-8-sig')
