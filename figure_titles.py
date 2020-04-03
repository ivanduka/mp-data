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
    print(projects)
    # projects = ['OH-002-2016', 'OH-001-2014', 'GH-002-2019', 'GH-001-2019']
    projects = ['GH-001-2019']

    results = []

    # Sequential Mode - if using, comment out the multiprocessing code
    for project in projects:
        results.append(get_titles_figures(project))

    # Multiprocessing Mode - if using, comment out the sequential code
    # with Pool() as pool:
    #    results = pool.map(get_titles_figures, projects)

    for result in results:
        print(result)

    data = []
    for project in projects:
        df = pd.read_csv(save_dir + project + '-final_figs.csv', encoding='utf-8-sig')
        data.append(df)
    df_all = pd.concat(data, axis=0, ignore_index=True)
    df_all.to_csv(save_dir + 'final_figs.csv', index=False, encoding='utf-8-sig')
