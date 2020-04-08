import re
from pathlib import Path
from dotenv import load_dotenv
from multiprocessing import Pool
import time
from sqlalchemy import text, create_engine
import os
import pandas as pd
import json

load_dotenv(override=True)
engine_string = f"mysql+mysqldb://esa_user_rw:{os.getenv('DB_PASS')}@os25.neb-one.gc.ca./esa?charset=utf8"
engine = create_engine(engine_string)

pd.set_option("display.max_columns", None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 1200)

regex = re.compile("[0-9a-zA-Z]")


def get_all_csvs():
    with engine.connect() as conn:
        stmt = text("SELECT csvId, csvText FROM esa.csvs WHERE hasContent IS NULL;")
        df = pd.read_sql(stmt, conn)
        return df


def is_empty(lines):
    for line in lines:
        for cell in line:
            if regex.search(cell):
                return False
    return True


def update_has_content(df):
    with engine.connect() as conn:
        for row in df.itertuples(index=False):
            has_content = 1
            if is_empty(row.csvText):
                has_content = 0
            stmt = text("UPDATE esa.csvs SET hasContent = :hasContent WHERE csvId = :csvId;")
            params = {"csvId": row.csvId, "hasContent": has_content}
            result = conn.execute(stmt, params)
            print(f"{row.csvId}: {has_content} (updated {result.rowcount} rows)")
    print(f"Updated {len(df)} rows")


if __name__ == "__main__":
    csvs_without_has_content = get_all_csvs()
    update_has_content(csvs_without_has_content)
