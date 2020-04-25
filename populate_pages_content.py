from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import text, create_engine
import os
import pandas as pd
import PyPDF2
from tika import parser
import time

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


def insert_contents():
    t = time.time()

    stmt = text("SELECT pdfId, totalPages FROM pdfs ORDER BY totalPages DESC;")
    with engine.connect() as conn:
        df = pd.read_sql(stmt, conn)
    data = df.to_dict("records")

    for row in data:
        insert_content(row)

    sec = round(time.time() - t)
    print(f"Done {len(data)} in {sec} seconds ({round(sec / 60, 2)} min or {round(sec / 3600, 2)} hours)")


def insert_content(row):
    start_time = time.time()
    pdf_id, total_pages = row["pdfId"], int(row["totalPages"])
    process_pdf(pdf_id, total_pages, True, True)
    process_pdf(pdf_id, total_pages, False, True)
    process_pdf(pdf_id, total_pages, True, False)
    process_pdf(pdf_id, total_pages, False, False)
    sec = round(time.time() - start_time)
    print(f"Done {total_pages}x4 in {sec} seconds ({round(sec / 60, 2)} min or {round(sec / 3600, 2)} hours)")


def process_pdf(pdf_id, pages, xml, normal):
    pdf = pdf_files_folder_normal.joinpath(f"{pdf_id}.pdf")
    if not normal:
        pdf = pdf_files_folder_rotated.joinpath(f"{pdf_id}.pdf")
    with pdf.open(mode="rb") as infile:
        reader = PyPDF2.PdfFileReader(infile)
        for p in range(1, pages + 1):
            writer = PyPDF2.PdfFileWriter()
            writer.addPage(reader.getPage(p - 1))  # Reads from 0 page
            random_file = Path().joinpath(os.urandom(24).hex())
            with random_file.open(mode="wb") as outfile:
                writer.write(outfile)
            content = parser.from_file(outfile.name, xmlContent=xml)["content"]
            random_file.unlink()
            if normal and xml:
                stmt = "INSERT INTO pages_normal_xml (pdfId, page_num, content) VALUES (%s,%s,%s)"
            if normal and not xml:
                stmt = "INSERT INTO pages_normal_txt (pdfId, page_num, content) VALUES (%s,%s,%s)"
            if not normal and xml:
                stmt = "INSERT INTO pages_rotated_xml (pdfId, page_num, content) VALUES (%s,%s,%s)"
            if not normal and not xml:
                stmt = "INSERT INTO pages_rotated_txt (pdfId, page_num, content) VALUES (%s,%s,%s)"
            params = (pdf_id, p, content)
            with engine.connect() as conn:
                result = conn.execute(stmt, params)
            if result.rowcount != 1:
                raise Exception(f"{pdf_id}-{p}: ERROR! Updated {result.rowcount} rows!")


if __name__ == "__main__":
    clear_db()
    insert_contents()
