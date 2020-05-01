import time

from dotenv import load_dotenv
from sqlalchemy import text, create_engine
import os
import pandas as pd
from googletrans import Translator
import requests
import urllib.parse
from multiprocessing import Pool
import math

load_dotenv(override=True)
host = os.getenv("DB_HOST")
database = "esa"
user = os.getenv("DB_USER")
password = os.getenv("DB_PASS")
engine_string = f"mysql+mysqldb://{user}:{password}@{host}/{database}?charset=utf8mb4"
engine = create_engine(engine_string)


def get_toc():
    stmt = text("SELECT toc_pdfId, toc_page_num, toc_title_order, titleTOC FROM toc WHERE titleTOC_fr IS NULL;")
    with engine.connect() as conn:
        df = pd.read_sql(stmt, conn)
        return df.to_dict("records")


def update_translations_google():
    rows = get_toc()
    translator = Translator()
    stmt = "UPDATE toc SET titleTOC_fr=%s WHERE toc_pdfId=%s AND toc_page_num=%s AND toc_title_order=%s;"
    with engine.connect() as conn:
        for row in rows[100:]:
            en = row["titleTOC"]
            t = translator.translate(en, dest="fr", src="en")
            print(f"{t.origin} -> {t.text}")
            time.sleep(1)
        print(f"Processed {len(rows)}.")


def unit_work(row):
    en = row["titleTOC"]
    key = os.getenv("TRANSLATION_API_KEY")
    enc = urllib.parse.quote_plus(en)
    s = f"https://translate.yandex.net/api/v1.5/tr.json/translate?key={key}&lang=en-fr&text={enc}"
    r = requests.post(s)
    j = r.json()
    print(j)
    fr = j["text"][0]
    r.close()
    stmt = "UPDATE toc SET titleTOC_fr=%s WHERE toc_pdfId=%s AND toc_page_num=%s AND toc_title_order=%s;"
    params = (fr, row["toc_pdfId"], row["toc_page_num"], row["toc_title_order"])
    result = engine.execute(stmt, params)
    if result.rowcount != 1:
        raise Exception(f"Updated {result.rowcount} rows for {row}")


def update_translations_yandex():
    rows = get_toc()

    # for row in rows:
    #     unit_work(row)

    with Pool(96) as pool:
        pool.map(unit_work, rows, chunksize=1)

    print(f"All {len(rows)} done")


def count_words():
    stmt = text("SELECT titleTOC, titleTOC_fr FROM toc;")
    with engine.connect() as conn:
        df = pd.read_sql(stmt, conn)
    rows = df.to_dict("records")

    translation_per_day = 2000
    revision_per_day = 15000
    administration_days = 2

    def process(field):
        count = 0
        for row in rows:
            f = row[field]
            count += len(f.split())
        days_translation = math.ceil(count / translation_per_day)
        days_revision = math.ceil(count / revision_per_day)
        days_admin = administration_days
        days_total = days_translation + days_revision + days_admin
        print(f"{field}:")
        print(f"Total words: {count}")
        print(f"Days for translation ({translation_per_day}/day): {days_translation}")
        print(f"Days for revision ({revision_per_day}/day): {days_revision}")
        print(f"Days for administration: {days_admin}")
        print(f"Days total: {days_total}")
        print()

    process("titleTOC")
    process("titleTOC_fr")


if __name__ == "__main__":
    # update_translations_yandex()
    # update_translations_google()
    count_words()
