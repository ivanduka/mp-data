import pandas as pd
import pickle
from bs4 import BeautifulSoup
import re
import os

from external_functions import get_titles_figures, get_titles_tables, get_category
import constants

load_pickles = 0 # need to load text from pickles and save to each project's csv
get_toc = 0 # need to go through all docs to create lists of tables and figures in csvs
get_figure_titles = 0 # find all figs page #
get_table_titles = 0 # find all table page #

if __name__ == "__main__":
    # get list of all documents and projects (Index2)
    all_projects = pd.read_excel(constants.projects_path)
    projects = all_projects['Hearing order'].unique()
    print(projects)

    # get text for each document in all projects
    if load_pickles:
        print('Creating project csvs with text')
        for project in projects:
            print(project)
            df_project = all_projects[all_projects['Hearing order'] == project].copy()
            df_project.set_index('DataID', inplace=True)
            df_project['Text'] = None
            df_project['Text_rotated'] = None

            for index, row in df_project.iterrows():
                with open(constants.pickles_path + str(index) + '.pkl', 'rb') as f: #unrotated pickle
                    data = pickle.load(f)
                with open(constants.pickles_rotated_path + str(index) + '.pkl', 'rb') as f: # rotated pickle
                    data_rotated = pickle.load(f)
                df_project.loc[index, 'Text'] = data['content']  # save the unrotated text
                df_project.loc[index, 'Text_rotated'] = data_rotated['content']  # save the rotated text
            df_project.to_csv(constants.save_dir + 'project_' + project + '.csv', index=True, encoding='utf-8-sig')

    # now get TOC from each document and create a list of all figs and tables (that were found in TOC's)
    if get_toc:
        print('Searching for TOC tables and figures')
        list_figs = []
        list_tables = []
        for project in projects:
            print(project)
            df_project = pd.read_csv(constants.save_dir + 'project_' + project + '.csv', encoding='utf-8-sig', index_col='DataID')
            for index, row in df_project.iterrows():
                content = row['Text']
                content_rotated = row['Text_rotated']

                # find tables of content and list of figures in the unrotated text
                soup = BeautifulSoup(content, 'lxml')
                pages = soup.find_all('div', attrs={'class': 'page'})
                for page_num, page in enumerate(pages):
                    text = re.sub(constants.empty_line, '', page.text)  # get rid of empty lines

                    # extrat TOC for figures
                    figures = re.findall(constants.figure, text)
                    df_figs = pd.DataFrame(figures, columns=['Name', 'Page'])
                    df_figs['Name'] = df_figs['Name'].str.replace(constants.whitespace, ' ', regex=True).str.strip()
                    df_figs['Page'] = df_figs['Page'].str.strip()
                    df_figs['TOC_Page'] = page_num
                    df_figs['DataID'] = index
                    df_figs['Project'] = project
                    list_figs.append(df_figs)

                    # extract TOC for Tables
                    tables = re.findall(constants.table, text)
                    df_tables = pd.DataFrame(tables, columns=['Name', 'Page'])
                    df_tables['Name'] = df_tables['Name'].str.replace(constants.whitespace, ' ', regex=True).str.strip()
                    df_tables['Page'] = df_tables['Page'].str.strip()
                    df_tables['TOC_Page'] = page_num
                    df_tables['DataID'] = index
                    df_tables['Project'] = project
                    list_tables.append(df_tables)

        df_figs = pd.concat(list_figs, axis=0, ignore_index=True)
        df_tables = pd.concat(list_tables, axis=0, ignore_index=True)
        df_figs.to_csv(constants.save_dir + 'all_figs.csv', index=False, encoding='utf-8-sig')
        df_tables.to_csv(constants.save_dir + 'all_tables.csv', index=False, encoding='utf-8-sig')

    # get page numbers for all the figures found in TOC
    if get_figure_titles:
        # reset projects to what we need
        projects = ['OH-002-2016']

        for project in projects:
            get_titles_figures(project)

        # put everything together
        data = []
        projects = all_projects['Hearing order'].unique()
        for project in projects:
            df = pd.read_csv(constants.save_dir + project + '-final_figs.csv', encoding='utf-8-sig')
            data.append(df)
        df_all = pd.concat(data, axis=0, ignore_index=True)
        df_all.to_csv(constants.save_dir + 'final_figs.csv', index=False, encoding='utf-8-sig')

        # unpivot the page numbers
        data = []
        for index, row in df_all.iterrows():
            if row['count'] <= 1:
                data.append(row)
            else:
                for page in row['location_Page'].split(', '):
                    new_row = row.copy()
                    new_row['location_Page'] = page
                    data.append(new_row)

        df_pivoted = pd.DataFrame(data)
        df_pivoted.to_csv(constants.save_dir + 'final_figs_pivoted.csv', index=False, encoding='utf-8-sig')

    # get page numbers for all the figures found in TOC
    if get_table_titles:
        # reset projects to what we need
        projects = ['OH-002-2016']

        # put them all together
        for project in projects:
            get_titles_tables(project)
        data = []
        projects = all_projects['Hearing order'].unique()
        for project in projects:
            df = pd.read_csv(constants.save_dir + project + '-final_tables.csv', encoding='utf-8-sig')
            data.append(df)
        df_all = pd.concat(data, axis=0, ignore_index=True)
        df_all.to_csv(constants.save_dir + 'final_tables.csv', index=False, encoding='utf-8-sig')

    # put it all together
    paths = os.listdir(constants.pickles_path)
    all_paths = [constants.pickles_path + str(x) for x in paths] # paths to all the pickle files

    # create csv with all tables
    data = []
    for csv_name in os.listdir(constants.csv_path):
        name = csv_name.split('.')[0]
        data_id, page, order = name.split('_')
        data.append([csv_name, data_id, page, order])
    df = pd.DataFrame(data, columns=['CSV_Name', 'DataID', 'Page', 'Order'])
    df.to_csv(constants.save_dir + 'all_tables1.csv', index=False)

    # add table titles using Viboud's method
    df = pd.read_csv(constants.save_dir + 'all_tables1.csv', header=0)
    df['Real Order'] = df.groupby(['DataID', 'Page'])['Order'].rank()
    df['table titles'] = ''
    df['table titles next'] = ''
    df['categories'] = -1
    df['count'] = 0
    for index, row in df.iterrows():
        # get all possible table titles on this page
        page_numbers = []
        table_titles = []
        table_titles_next = []
        categories = []
        path = constants.pickles_path + str(row['DataID']) + '.pkl'
        with open(path, 'rb') as f:
            data = pickle.load(f)
        soup = BeautifulSoup(data['content'], 'lxml')
        pages = soup.find_all('div', attrs={'class': 'page'})
        page_num = row['Page'] - 1
        page = pages[page_num]

        lines = [x.text for x in page.find_all('p')]  # list of lines
        num_lines = len(lines)
        for i, line in enumerate(lines):
            title = re.sub(constants.whitespace, ' ', line).strip()  # replace all whitespace with single space
            # identify if this line is a table line (took out exceptions, should not need)
            if re.match(constants.tables_rex, title):  # and not any(x in line.lower() for x in exceptions_list):
                if i < num_lines - 1:
                    title_next = re.sub(constants.whitespace, ' ',
                                        lines[i + 1]).strip()  # replace all whitespace with single space
                else:
                    title_next = ''
                category = get_category(title)
                if category > 0:
                    table_titles.append(title)
                    table_titles_next.append(title_next)
                    categories.append(category)
        r = int(row['Real Order']) - 1
        count = len(table_titles)
        if r < count:
            df.loc[index, 'table titles'] = table_titles[r]
            df.loc[index, 'table titles next'] = table_titles_next[r]
            df.loc[index, 'categories'] = categories[r]
        else:
            df.loc[index, 'table titles'] = ''
            df.loc[index, 'table titles next'] = ''
        df.loc[index, 'count'] = count
        if index % 1000 == 0:
            df.to_csv('all_tables2-temp.csv', index=False, encoding='utf-8-sig')
    df['final table titles'] = np.where(df['categories'] == 2, df['table titles'] + ' ' + df['table titles next'],
                                        df['table titles'])
    df['final table titles'] = df['final table titles'].str.replace('\d+$', '', regex=True).str.strip()
    df.to_csv(constants.save_dir + 'all_tables2.csv', index=False, encoding='utf-8-sig')

    # add table titles from TOC method
    df = pd.read_csv(constants.save_dir + 'all_tables2.csv', header=0)
    df['TOC Title'] = ''
    df['TOC count'] = 0

    df_all_titles = pd.read_csv('F:/Environmental Baseline Data/Version 4 - Final/Saved/final_tables.csv', header=0)
    df_all_titles = df_all_titles[~df_all_titles['location_DataID'].astype(str).str.contains(',')]
    df_all_titles['location_DataID'] = df_all_titles['location_DataID'].astype(float)  # .astype(int).satype(str)

    for index, row in df.iterrows():
        # find TOC title and assign
        # print(row['DataID'])
        data_id = int(row['DataID'])
        page_rex = r'\b' + str(int(row['Page']) - 1) + r'\b'
        order = row['Real Order']
        df_titles = df_all_titles[(df_all_titles['location_DataID'] == row['DataID'])
                                  & df_all_titles['location_Page'].str.contains(page_rex, regex=True)].reset_index()
        count = df_titles.shape[0]
        if order <= count:
            df.loc[index, 'TOC Title'] = df_titles.loc[order - 1, 'Name']
        df.loc[index, 'TOC count'] = count

        count = df_titles.shape[0]
    df.to_csv(constants.save_dir + 'all_tables3.csv', index=False, encoding='utf-8-sig')

    df = pd.read_csv(constants.save_dir + 'all_tables3.csv', header=0)
    df['Table Title'] = df['final table titles'].fillna(df['TOC Title'])
    df.to_csv(constants.save_dir + 'all_tables-final.csv', index=False, encoding='utf-8-sig')

