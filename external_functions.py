import pandas as pd
import re
import time
from multiprocessing import Pool

from external_external_functions import table_checker, figure_checker
import constants

# function that takes ID of project, and finds locations of all the tables from that projects' TOC
# saves result to save_dir folder
def get_titles_tables(project):
    print(f"Starting {project}")
    start_time = time.time()
    df_tables = pd.read_csv(constants.save_dir + 'all_tables.csv', encoding='utf-8-sig')
    df_tables = df_tables[df_tables['Project'] == project]  # filter out just current project
    df_tables['location_DataID'] = None
    df_tables['location_Page'] = None
    df_tables['count'] = 0
    df_project = pd.read_csv(constants.save_dir + 'project_' + project + '.csv', encoding='utf-8-sig', index_col='DataID')
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
        word1_rex = re.compile(r'(?i)\b' + word1 + r'\s')
        word2_rex = re.compile(r'(?i)\b' + word2)
        s1_rex = re.compile(r'(?i)\b' + word1 + r'\s' + word2)

        s2 = re.sub(constants.punctuation, ' ', s2)
        s2 = re.sub(constants.whitespace, ' ', s2)  # remove whitespace
        s2_rex = r'(?i)\b'
        for s in s2.split(' '):
            s2_rex = s2_rex + r'[^\w]*' + s
        s2_rex = s2_rex + r'\b'
        s2_rex = re.compile(s2_rex)
        toc_id = row['DataID']
        project = row['Project']
        toc_page = row['TOC_Page']

        id_list = []
        page_list = []
        count = 0

        # first try previous id if exists
        if prev_id > 0:
            doc_id = prev_id
            doc_text = df_project.loc[doc_id, 'Text']
            doc_text_rotated = df_project.loc[doc_id, 'Text_rotated']
            arg = (doc_text, doc_text_rotated, doc_id, toc_id, toc_page, s1_rex, s2_rex)
            success, output, page_list, doc_id = table_checker(arg)
            if not success:
                print(output)
            if len(page_list) > 0:
                id_list = [doc_id]
                count = len(page_list)

        # if didn't find try TOC document
        if count == 0:
            doc_id = toc_id
            doc_text = df_project.loc[doc_id, 'Text']
            doc_text_rotated = df_project.loc[doc_id, 'Text_rotated']
            arg = (doc_text, doc_text_rotated, doc_id, toc_id, toc_page, s1_rex, s2_rex)
            success, output, page_list, doc_id = table_checker(arg)
            if not success:
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
            print("Starting multiprocessing. You will see the errors (if any) only when everything is finished...")
            # Phase 1. Arguments preparation for processing
            args = []
            for doc_id, doc in df_project.iterrows():
                if (doc_id != toc_id) and (doc_id != prev_id):
                    doc_text = df_project.loc[doc_id, 'Text']
                    doc_text_rotated = df_project.loc[doc_id, 'Text_rotated']
                    args.append((doc_text, doc_text_rotated, doc_id, toc_id, toc_page, s1_rex, s2_rex))

            # Phase 2. Processing of arguments
            # Sequential Mode (if using, comment out the multiprocessing mode code)
            # results = []
            # for arg in args:
            #     results.append(table_checker(arg))
            # Multiprocessing Mode (if using, comment out the sequential processing code)
            with Pool() as pool:
                results = pool.map(table_checker, args)

            # Phase 3. Processing of results
            for result in results:
                success, output, p_list, doc_id = result
                if not success:
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
    df_tables.to_csv(constants.save_dir + project + '-final_tables.csv', index=False, encoding='utf-8-sig')

    duration = round(time.time() - start_time)
    print(f"Done {project} in {duration} seconds ({round(duration / 60, 2)} min or {round(duration / 3600, 2)} hours)")
    return

# function that takes ID of project, and finds locations of all the figures from that projects' TOC
# saves result to save_dir folder
def get_titles_figures(project):
    print(f"Starting {project}")
    start_time = time.time()

    df_figs = pd.read_csv(constants.save_dir + 'all_figs.csv', encoding='utf-8-sig')
    df_figs = df_figs[df_figs['Project'] == project]  # filter out just current project
    df_figs['location_DataID'] = None
    df_figs['location_Page'] = None
    df_figs['count'] = 0
    df_project = pd.read_csv(constants.save_dir + 'project_' + project + '.csv', encoding='utf-8-sig', index_col='DataID')
    df_project['Text_rotated'].fillna('', inplace=True)

    prev_id = 0
    for index, row in df_figs.iterrows():
        title = row['Name']
        print(title)
        c = title.count(' ')
        if c >= 2:
            word1, word2, s2 = title.split(' ', 2)
        else:
            word1, word2 = title.split(' ', 1)
            s2 = ''
        word2 = re.sub('[^a-zA-Z0-9]', '[^a-zA-Z0-9]', word2)
        word1_rex = re.compile(r'(?i)\b' + word1 + r'\s')
        word2_rex = re.compile(r'(?i)\b' + word2)

        # s2 = re.sub(punctuation, ' ', s2)
        s2 = re.sub(constants.whitespace, ' ', s2)  # remove whitespace
        s2 = re.sub(constants.punctuation, '.*', s2)

        s2_rex = r'(?i)\b'
        for s in s2.split(' '):
            s2_rex = s2_rex + r'[^\w]*' + s
        s2_rex = s2_rex + r'\b'
        s2_rex = re.compile(s2_rex)
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

            arg = (doc_text, doc_text_rotated, doc_id, toc_id, toc_page, word1_rex, word2_rex, s2_rex)
            success, output, page_list, doc_id = figure_checker(arg)
            if not success:
                print(output)
            if len(page_list) > 0:
                id_list = [doc_id]
                count = len(page_list)

        # if didn't find try TOC document
        if count == 0:
            doc_id = toc_id
            doc_text = df_project.loc[doc_id, 'Text']
            doc_text_rotated = df_project.loc[doc_id, 'Text_rotated']
            arg = (doc_text, doc_text_rotated, doc_id, toc_id, toc_page, word1_rex, word2_rex, s2_rex)
            success, output, page_list, doc_id = figure_checker(arg)
            if not success:
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
            print("Starting multiprocessing. You will see the errors (if any) only when everything is finished...")
            # Phase 1. Arguments preparation for processing
            args = []
            for doc_id, doc in df_project.iterrows():
                if (doc_id != toc_id) and (doc_id != prev_id):
                    doc_text = df_project.loc[doc_id, 'Text']
                    doc_text_rotated = df_project.loc[doc_id, 'Text_rotated']
                    args.append((doc_text, doc_text_rotated, doc_id, toc_id, toc_page, word1_rex, word2_rex, s2_rex))

            # Phase 2. Processing of arguments
            # Sequential Mode (if using, comment out the multiprocessing mode code)
            # results = []
            # for arg in args:
            #     results.append(table_checker(arg))
            # Multiprocessing Mode (if using, comment out the sequential processing code)
            with Pool() as pool:
                results = pool.map(figure_checker, args)

            # Phase 3. Processing of results
            for result in results:
                success, output, p_list, doc_id = result
                if not success:
                    print(output)
                if len(p_list) > 0:
                    id_list.append(doc_id)
                    page_list.extend(p_list)
                    count += len(p_list)

            if len(id_list) == 1:
                prev_id = id_list[0]

        df_figs.loc[index, 'location_DataID'] = str(id_list).replace('[', '').replace(']', '').strip()
        df_figs.loc[index, 'location_Page'] = str(page_list).replace('[', '').replace(']', '').strip()
        df_figs.loc[index, 'count'] = count
    df_figs.to_csv(constants.save_dir + project + '-final_figs.csv', index=False, encoding='utf-8-sig')
    duration = round(time.time() - start_time)
    print(f"Done {project} in {duration} seconds ({round(duration / 60, 2)} min or {round(duration / 3600, 2)} hours)")
    print(f"Finished {project}")
    return

def get_category(title):
    category = False
    # title_clean = re.sub(extra_chars, '', title) # get rid of some extra characters
    title_clean = re.sub(punctuation, '', title)  # remove punctuation
    title_clean = re.sub(small_word, '', title_clean)  # delete any 1 or 2 letter words without digits
    title_clean = re.sub(whitespace, ' ', title_clean).strip()  # replace whitespace with single space
    num_words = title_clean.count(' ') + 1
    _, _, third, _ = (title_clean + '   ').split(' ', 3)

    if num_words <= 3:
        if 'cont' in title_clean.lower():  # if any word starts with cont
            category = 1
        else:
            category = 2
    else:
        if third.lower().startswith('cont') or third[0].isdigit() or third[0].isupper():
            category = 1
        else:
            category = 0
    return category