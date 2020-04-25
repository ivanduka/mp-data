from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import text, create_engine
import os
import pandas as pd
import PyPDF2
from tika import parser
import tika
import time
from multiprocessing import Pool
import traceback

pdf_files_folder_normal = Path("//luxor/data/branch/Environmental Baseline Data/Version 4 - Final/PDF")
pdf_files_folder_rotated = Path("//luxor/data/branch/Environmental Baseline Data/Version 4 - Final/PDF_rotated")

if not pdf_files_folder_normal.exists():
    print(pdf_files_folder_normal, "does not exist!")
if not pdf_files_folder_rotated.exists():
    print(pdf_files_folder_normal, "does not exist!")

pd.set_option("display.max_columns", None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 1200)

load_dotenv(override=True)
host = os.getenv("DB_HOST")
database = os.getenv("DB_DATABASE")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASS")
engine_string = f"mysql+mysqldb://{user}:{password}@{host}/{database}?charset=utf8mb4"
engine = create_engine(engine_string)

tika.TikaClientOnly = True
os.environ["TIKA_STARTUP_MAX_RETRY"] = "10"
os.environ["TIKA_CLIENT_ONLY"] = "True"
os.environ["TIKA_SERVER_ENDPOINT"] = "http://127.0.0.1:9998"


def clear_db():
    stmt1 = "DELETE FROM pages_normal_txt;"
    stmt2 = "DELETE FROM pages_normal_xml;"
    stmt3 = "DELETE FROM pages_rotated_txt;"
    stmt4 = "DELETE FROM pages_rotated_xml;"
    with engine.connect() as conn:
        conn.execute(stmt1)
        conn.execute(stmt2)
        conn.execute(stmt3)
        conn.execute(stmt4)
    print("DB is cleared")


def clean_tmp():
    g = list(Path().resolve().parent.joinpath("tmp").glob("*"))
    for f in g:
        try:
            f.unlink()
        except Exception as e:
            print(e)
    print(f"Attempted to clean up {len(g)} files the tmp folder")


def insert_contents():
    t = time.time()

    stmt = text("SELECT pdfId, totalPages FROM pdfs ORDER BY totalPages DESC;")
    with engine.connect() as conn:
        df = pd.read_sql(stmt, conn)
    data = df.to_dict("records")

    # java -d64 -jar -Xms50g -Xmx50g tika-server-1.24.jar
    parser.from_file(__file__)  # testing tika server
    print('Server apparently works...')

    clean_tmp()

    # skipping = []
    # current_id = None
    # for row in data:
    #     try:
    #         current_id = row["pdfId"]
    #         insert_content(row)
    #     except Exception as e:
    #         skipping.append(current_id)
    #         print(f"{current_id}: {e}")
    # print(skipping)

    with Pool() as pool:
        pool.map(insert_content, data, chunksize=1)

    # count = 0
    # while True:
    #     try:
    #         clean_tmp()
    #         with Pool() as pool:
    #             pool.map(insert_content, data, chunksize=1)
    #     except Exception as e:
    #         print("\n===========================================================\n")
    #         print(f"Counter: {count}: {e}")
    #         traceback.print_tb(e.__traceback__)
    #         print("\n===========================================================\n")
    #         count += 1
    #     else:
    #         print(f"Final counter value is {count}")
    #         break

    sec = round(time.time() - t)
    print(f"Done {len(data)} in {sec} seconds ({round(sec / 60, 2)} min or {round(sec / 3600, 2)} hours)")


def insert_content(row):
    # start_time = time.time()

    pdf_id, total_pages = row["pdfId"], int(row["totalPages"])
    process_pdf(pdf_id, total_pages, True, True)
    process_pdf(pdf_id, total_pages, False, True)
    # process_pdf(pdf_id, total_pages, True, False)
    # process_pdf(pdf_id, total_pages, False, False)

    # sec = round(time.time() - start_time)
    # print(f"Done {total_pages}x4 in {sec} seconds ({round(sec / 60, 2)} min or {round(sec / 3600, 2)} hours)")


def process_pdf(pdf_id, pages, xml, normal):
    print(f"Starting {pdf_id}")
    pdf = pdf_files_folder_normal.joinpath(f"{pdf_id}.pdf")
    if not normal:
        pdf = pdf_files_folder_rotated.joinpath(f"{pdf_id}.pdf")
    if not pdf.exists():
        raise Exception(f"{pdf} does not exist! :(")
    # try:
    with pdf.open(mode="rb") as infile, engine.connect() as conn:
        reader = PyPDF2.PdfFileReader(infile)
        if reader.isEncrypted:
            reader.decrypt("")
        return
        for p in range(1, pages + 1):
            if normal and xml:
                check = "SELECT pdfId FROM pages_normal_xml WHERE pdfId = %s AND page_num = %s;"
                stmt = "INSERT INTO pages_normal_xml (pdfId, page_num, content) VALUES (%s,%s,%s);"
            if normal and not xml:
                check = "SELECT pdfId FROM pages_normal_txt WHERE pdfId = %s AND page_num = %s;"
                stmt = "INSERT INTO pages_normal_txt (pdfId, page_num, content) VALUES (%s,%s,%s);"
            if not normal and xml:
                check = "SELECT pdfId FROM pages_rotated_xml WHERE pdfId = %s AND page_num = %s;"
                stmt = "INSERT INTO pages_rotated_xml (pdfId, page_num, content) VALUES (%s,%s,%s);"
            if not normal and not xml:
                check = "SELECT pdfId FROM pages_rotated_txt WHERE pdfId = %s AND page_num = %s;"
                stmt = "INSERT INTO pages_rotated_txt (pdfId, page_num, content) VALUES (%s,%s,%s);"

            result = conn.execute(check, (pdf_id, p))
            if result.rowcount > 0:
                continue

            writer = PyPDF2.PdfFileWriter()
            writer.addPage(reader.getPage(p - 1))  # Reads from 0 page
            random_file = Path().resolve().parent.joinpath("tmp").joinpath(f"{os.urandom(24).hex()}.pdf")
            with random_file.open(mode="wb") as outfile:
                writer.write(outfile)
            content = parser.from_file(outfile.name, xmlContent=xml, requestOptions={'timeout': 300})["content"]
            if content is None:
                content = ""
            random_file.unlink()

            params = (pdf_id, p, content)
            result = conn.execute(stmt, params)
            if result.rowcount != 1:
                raise Exception(f"{pdf_id}-{p}: ERROR! Updated {result.rowcount} rows!")
    # except Exception as e:
    #     print(f"pdfId {pdf_id} (normal: {normal}): {e}")


if __name__ == "__main__":
    # clear_db()  # Careful! Removes all data!
    insert_contents()
