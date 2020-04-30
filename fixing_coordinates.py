import json

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from urllib.parse import quote
import re
import camelot
import PyPDF2
from pathlib import Path
import shutil
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text, create_engine
from wand.image import Image
from multiprocessing import Pool
import os
import pandas as pd
import importlib
import time
import tika
from tika import parser

load_dotenv(override=True)
host = os.getenv("DB_HOST")
database = "pcmr"
user = os.getenv("DB_USER")
password = os.getenv("DB_PASS")
engine_string = f"mysql+mysqldb://{user}:{password}@{host}/{database}?charset=utf8mb4"
engine = create_engine(engine_string)


def get_tables():
    # stmt = text("SELECT tableId,x1,x2,y1,y2 FROM tables WHERE NOT (x1 < x2 and y1 > y2);")
    stmt = text("SELECT tableId,x1,x2,y1,y2 FROM tables;")
    with engine.connect() as conn:
        df = pd.read_sql(stmt, conn)
        return df.to_dict("records")


# def make_backup():
#     data = get_tables()
#     print("DB data length:", len(data))
#     json.dump(data, open("backup.json", "w"), indent=4)
#     backup = json.load(open("backup.json"))
#     print("Backup length:", len(backup))


def restore_backup():
    tables = json.load(open("backup.json"))
    stmt = text("UPDATE tables SET x1=:x1, x2=:x2, y1=:y1, y2=:y2 " +
                "WHERE tableId=:tableId AND (x1!=:x1 OR x2!=:x2 OR y1!=:y1 OR y2!=:y2);")
    with engine.connect() as conn:
        print(f"Starting to restore {len(tables)} tables:")
        counter = 0
        for table in tables:
            result = conn.execute(stmt, table)
            if result.rowcount != 0:
                counter += 1
        print(f"Done. Updated {counter} tables.\n")


def check_table(table_obj):
    x1 = table_obj["x1"]
    x2 = table_obj["x2"]
    y1 = table_obj["y1"]
    y2 = table_obj["y2"]
    if x1 < x2 and y1 > y2:
        return "TL"
    if x1 < x2 and y1 < y2:
        return "BL"
    if x1 > x2 and y1 > y2:
        return "TR"
    if x1 > x2 and y1 < y2:
        return "BR"
    raise Exception("Wrong argument(s)!")


def get_correct(table_obj):
    x1 = table_obj["x1"]
    x2 = table_obj["x2"]
    y1 = table_obj["y1"]
    y2 = table_obj["y2"]
    check = check_table(table_obj)
    if check == "TL":
        return None
    elif check == "BL":
        table_obj["x1"] = x1
        table_obj["x2"] = x2
        table_obj["y1"] = y2
        table_obj["y2"] = y1
    elif check == "TR":
        table_obj["x1"] = x2
        table_obj["x2"] = x1
        table_obj["y1"] = y1
        table_obj["y2"] = y2
    elif check == "BR":
        table_obj["x1"] = x2
        table_obj["x2"] = x1
        table_obj["y1"] = y2
        table_obj["y2"] = y1
    else:
        raise Exception("Wrong argument(s)!")
    return table_obj


def cycle_through():
    tables = get_tables()
    results = {"TL": 0, "TR": 0, "BL": 0, "BR": 0, "total": 0}
    print(f"Checking {len(tables)} tables:")
    for table in tables:
        results[check_table(table)] += 1
        results["total"] += 1
    print(f"Total checked {len(tables)} tables: {results}\n")


def fix_coordinates():
    tables = get_tables()
    stmt = text("UPDATE tables SET x1=:x1, x2=:x2, y1=:y1, y2=:y2 WHERE tableId=:tableId;")
    counter = 0
    skipped = 0
    with engine.connect() as conn:
        for table in tables:
            table_fixed = get_correct(table)
            if table_fixed:
                result = conn.execute(stmt, table_fixed)
                if result.rowcount != 0:
                    counter += 1
            else:
                skipped += 1
        print(f"Done {counter} changes. Skipped {skipped} tables. Processed {len(tables)} in total\n")


if __name__ == "__main__":
    restore_backup()
    cycle_through()
    fix_coordinates()
    cycle_through()
