import pandas as pd
import numpy as np
import pickle
from bs4 import BeautifulSoup
import re
import os
from multiprocessing import Pool
from fuzzywuzzy import fuzz
from sqlalchemy import text, create_engine
from dotenv import load_dotenv

from external_functions import get_titles_figures, get_titles_tables, find_tag_title
import constants

load_dotenv(override=True)
engine_string = f"mysql+mysqldb://esa_user_rw:{os.getenv('DB_PASS')}@os25.neb-one.gc.ca./esa?charset=utf8"
engine = create_engine(engine_string)

from_db = 1  # will get data from db, will write titles to db
load_pickles = 0  # need to load text from pickles and save to each project's csv
get_toc = 0  # need to go through all docs to create lists of tables and figures in csvs
get_figure_titles = 0  # find all figs page #
get_table_titles = 0  # find all table page #
create_csv1 = 1  # create csv of all the tables (from camelot csvs)
create_csv2 = 0  # assign table titles to each table using text search method
create_csv3 = 0  # assign table titles to each table using TOC method
create_final = 0  # replace continued tables and create final table title

if __name__ == "__main__":
    # get list of all documents and projects (Index2)
    if from_db:
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
    if create_csv1:
        if from_db:
            with engine.connect() as conn:
                stmt = text("SELECT csvFullPath, pdfId, page, tableNumber, topRowJson FROM csvs "
                            "WHERE (hasContent = 1) and (csvColumns > 1) and (whitespace < 78);")
                df = pd.read_sql(stmt, conn)
        else:
            paths = os.listdir(constants.pickles_path)
            all_paths = [constants.pickles_path + str(x) for x in paths]  # paths to all the pickle files

            # create csv with all tables
            data = []
            for csv_name in os.listdir(constants.csv_path):
                name = csv_name.split('.')[0]
                data_id, page, order = name.split('_')
                df_table = pd.read_csv(constants.csv_path + csv_name, header=0)
                cols = df_table.columns.str.cat(sep=', ')
                cols = re.sub(constants.whitespace, ' ', cols).strip()
                data.append([csv_name, data_id, page, order, cols])
            df = pd.DataFrame(data, columns=['CSV_Name', 'DataID', 'Page', 'Order', 'Columns'])
        df.to_csv(constants.save_dir + 'all_tables1.csv', index=False)

    # add table titles using Viboud's method
    if create_csv2:
        df = pd.read_csv(constants.save_dir + 'all_tables1.csv', header=0)
        df['Real Order'] = df.groupby(['DataID', 'Page'])['Order'].rank()
        df['table titles'] = ''
        df['table titles next'] = ''
        df['categories'] = -1
        df['count'] = 0

        with Pool() as pool:
            results = pool.map(find_tag_title, df.iterrows())
        for result in results:
            success, output, index, table_title, table_title_next, category, count = result
            if success:
                df.loc[index, 'table titles'] = table_title
                df.loc[index, 'table titles next'] = table_title_next
                df.loc[index, 'categories'] = category
                df.loc[index, 'count'] = count
            else:
                print(output)
        df['final table titles'] = np.where(df['categories'] == 2, df['table titles'] + ' ' + df['table titles next'],
                                            df['table titles'])
        # df['final table titles'] = df['final table titles'].str.replace('\d+$', '', regex=True).str.strip() # remove numbers from end
        df.to_csv(constants.save_dir + 'all_tables2.csv', index=False, encoding='utf-8-sig')

    # add table titles from TOC method
    if create_csv3:
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

    if create_final:
        df = pd.read_csv(constants.save_dir + 'all_tables3.csv', header=0)
        df['Table Title'] = df['final table titles'].fillna(df['TOC Title'])
        # sort df by csv_name
        df.sort_values('CSV_Name', ignore_index=True, inplace=True)

        prev_id = 0
        prev_title = ''
        prev_cols = ''
        # fill titles that are continuation of tables
        for index, row in df.iterrows():
            if (row['Table Title'] == '') or (row['categories'] == 1):
                if row['DataID'] == prev_id:
                    # check against previous table's columns
                    cols = row['Columns']
                    ratio_similarity = fuzz.token_sort_ratio(cols, prev_cols)

                    if len(set(prev_cols.split(', ')).difference(set(cols.split(', ')))) == 0 \
                            or len(set(prev_cols.split(', '))) == len(set(cols.split(', '))) \
                            or ratio_similarity > 89:
                        df.loc[index, 'Table Title'] = prev_title
                else:
                    prev_title = row['Table Title']
            else:
                prev_title = ''
        else:
            prev_title = row['Table Title']
        prev_id = row['DataID']
        prev_cols = row['Columns']
        df.to_csv(constants.save_dir + 'all_tables-final.csv', index=False, encoding='utf-8-sig')
