import pandas as pd
import re
from bs4 import BeautifulSoup
from multiprocessing import Pool
from external_functions import get_titles_figures, save_dir
from pathlib import Path

if __name__ == "__main__":
    # get all project IDs (in a list)
    projects_path = Path(
        r'\\luxor\data\branch\Environmental Baseline Data\Version 4 - Final\Indices\Index 2 - PDFs for Major Projects with ESAs.xlsx')
    all_projects = pd.read_excel(projects_path)
    projects = all_projects['Hearing order'].unique()
    # print(projects)
    projects = ['OH-002-2016']

    # Sequential Mode - if using, comment out the multiprocessing code
    for project in projects:
        get_titles_figures(project)

    data = []
    projects = all_projects['Hearing order'].unique()
    for project in projects:
        df = pd.read_csv(save_dir + project + '-final_figs.csv', encoding='utf-8-sig')
        data.append(df)
    df_all = pd.concat(data, axis=0, ignore_index=True)
    df_all.to_csv(save_dir + 'final_figs.csv', index=False, encoding='utf-8-sig')

    # unpivot the page numbers
    # save_dir = 'F:/Environmental Baseline Data/Version 4 - Final/Saved/'
    # save_dir = 'C:/Users/rodijann/RegDocs/Saved/'
    # df = pd.read_csv(save_dir + 'final_figs.csv', encoding='utf-8-sig')
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
    df_pivoted.to_csv(save_dir + 'final_figs_pivoted.csv', index=False, encoding='utf-8-sig')