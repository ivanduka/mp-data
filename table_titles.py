import pandas as pd
import re
from bs4 import BeautifulSoup
from multiprocessing import Pool
from external_functions import get_titles, save_dir
from pathlib import Path

if __name__ == "__main__":
    # get all project IDs (in a list)
    projects_path = Path(r'\\luxor\data\branch\Environmental Baseline Data\Version 4 - Final\Indices\Index 2 - PDFs for Major Projects with ESAs.xlsx')
    all_projects = pd.read_excel(projects_path)
    projects = all_projects['Hearing order'].unique()
    # print(projects)
    # projects = ['GH-1-2003', 'HW-001- # to test


    # assuming saved_dir has all the project csvs already, and has the all_tables file in it already
    # go through every table and find it in some document and record ID and real page num
    # this is the part that we could multiproccess

    results = []

    # Sequential Mode - if using, comment out the multiprocessing code
    # for project in projects:
    #     results.append(get_titles(project))

    # Multiprocessing Mode - if using, comment out the sequential code
    with Pool() as pool:
        results = pool.map(get_titles, projects)

    for result in results:
        print(result)

    # now need to take all the resulting csv's and put them all together
    # do this after all the projects are done above
    # this does not need to be multiproccessed

    data = []
    for project in projects:
        df = pd.read_csv(save_dir + project + '-final_tables.csv', encoding='utf-8-sig')
        data.append(df)
    df_all = pd.concat(data, axis=0, ignore_index=True)
    df_all.to_csv(save_dir + 'final_tables.csv', index=False, encoding='utf-8-sig')
