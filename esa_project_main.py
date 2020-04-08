import pandas as pd
import numpy as np
import pickle
from bs4 import BeautifulSoup
import re
import os
from multiprocessing import Pool
from sqlalchemy import text, create_engine
from dotenv import load_dotenv

from external_functions import get_titles_figures, get_titles_tables, find_tag_title, find_toc_title, find_final_title
import constants

load_dotenv(override=True)
engine_string = f"mysql+mysqldb://esa_user_rw:{os.getenv('DB_PASS')}@os25.neb-one.gc.ca./esa?charset=utf8"
engine = create_engine(engine_string)

load_pickles = 0  # need to load text from pickles and save to each project's csv
get_toc = 0  # need to go through all docs to create lists of tables and figures in csvs
get_figure_titles = 0  # find all figs page #
get_table_titles = 0  # find all table page #
do_tag_title = 1  # assign table titles to each table using text search method
do_toc_title = 1  # assign table titles to each table using TOC method
do_final_title = 1  # replace continued tables and create final table title

if __name__ == "__main__":
    # get list of all documents and projects (Index2)
    if 1:
        projects = []
    else:
        all_projects = pd.read_excel(constants.projects_path)
        projects = all_projects['Hearing order'].unique()
    # print(projects)

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
                with open(constants.pickles_path + str(index) + '.pkl', 'rb') as f:  # unrotated pickle
                    data = pickle.load(f)
                with open(constants.pickles_rotated_path + str(index) + '.pkl', 'rb') as f:  # rotated pickle
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
            df_project = pd.read_csv(constants.save_dir + 'project_' + project + '.csv', encoding='utf-8-sig',
                                     index_col='DataID')
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
    with engine.connect() as conn:
        stmt = text("SELECT csvFullPath, pdfId, page, tableNumber FROM csvs "
                    "WHERE (hasContent = 1) and (csvColumns > 1) and (whitespace < 78);")
        df = pd.read_sql_query(stmt, conn)
    list_ids = df['pdfId'].unique()
    df.to_csv(constants.save_dir + 'all_tables_list.csv', index=False)

    # update tag method titles
    if do_tag_title:
        print(len(list_ids))
        with Pool() as pool:
            results = pool.map(find_tag_title, list_ids)
        for result in results:
            if result[1] != '':
                print(result[1])

    # update TOC method titles
    if do_toc_title:
        print(len(list_ids))
        with Pool() as pool:
            results = pool.map(find_toc_title, list_ids)
        for result in results:
            if result[1] != '':
                print(result[1])

    # update final titles
    if do_final_title:
        print(len(list_ids))
        with Pool() as pool:
            results = pool.map(find_final_title, list_ids)
        for result in results:
            if result[1] != '':
                print(result[1])

    with engine.connect() as conn:
        stmt = text("SELECT csvFullPath, pdfId, page, tableNumber, topRowJson, titleTag, titleTOC, titleFinal FROM csvs "
                    "WHERE (hasContent = 1) and (csvColumns > 1) and (whitespace < 78);")
        df = pd.read_sql_query(stmt, conn)
    df.to_csv(constants.save_dir + 'all_tables-final.csv', index=False)
