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


def get_all_csvs():
    with engine.connect() as conn:
        stmt = text("SELECT * FROM esa.csvs;")
        df = pd.read_sql(stmt, conn)
        return df


def get_all_pdfs():
    with engine.connect() as conn:
        stmt = text("SELECT * FROM esa.pdfs;")
        df = pd.read_sql(stmt, conn)
        return df


def get_csvs(pdf_id, page):
    with engine.connect() as conn:
        stmt = text("SELECT * FROM esa.csvs WHERE pdfId = :pdfId AND page = :page;")
        params = {"pdfId": pdf_id, "page": page}
        df = pd.read_sql(stmt, conn, params=params)
        return df


def delete_all_test_data():
    with engine.connect() as conn:
        stmt = text("DELETE FROM esa.test_table;")
        result = conn.execute(stmt)
        return result.rowcount


def insert_test_data(data_id, some_text):
    with engine.connect() as conn:
        stmt = text("INSERT INTO esa.test_table (dataId, someText) VALUE (:dataId, :someText);")
        params = {"dataId": data_id, "someText": some_text}
        result = conn.execute(stmt, params)
        return result.rowcount


def update_test_data(data_id, some_text):
    with engine.connect() as conn:
        stmt = text("UPDATE esa.test_table SET someText = :someText WHERE dataId = :dataId;")
        params = {"dataId": data_id, "someText": some_text}
        result = conn.execute(stmt, params)
        return result.rowcount


if __name__ == "__main__":
    print("Get all CSVs")
    print(get_all_csvs().head())

    # print("Get all PDFs")
    # print(get_all_pdfs().head())

    # print("Get CSVs for specific file and page")
    # print(get_csvs(1059875, 32))

    # deleted = delete_all_test_data()
    # print(f"Deleted {deleted} rows")
    # print("-----------------------")
    #
    # items = [(1, "one"), (2, "two"), (3, "three")]
    # for item in items:
    #     inserted = insert_test_data(item[0], item[1])
    #     print(f"Inserted {inserted} rows for {item}")
    # print("-----------------------")
    #
    # items2 = [(1, "uno"), (2, "duo"), (999, "whatever")]
    # for item in items2:
    #     updated = update_test_data(item[0], item[1])
    #     print(f"Update {updated} rows for {item}")
    # print("-----------------------")
