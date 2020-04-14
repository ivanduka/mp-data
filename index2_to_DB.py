from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import text, create_engine
import os
import pandas as pd

load_dotenv(override=True)
engine_string = f"mysql+mysqldb://esa_user_rw:{os.getenv('DB_PASS')}@os25.neb-one.gc.ca./esa?charset=utf8mb4"
engine = create_engine(engine_string)

pd.set_option("display.max_columns", None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 1200)

index2 = Path(
    r"\\luxor\data\branch\Environmental Baseline Data\Version 4 - Final\Indices" +
    r"\Index 2 - PDFs for Major Projects with ESAs.xlsx")

df = pd.read_excel(index2)
print(df.head())
df = df[["DataID", "application_name", "Application title short", "short_name", "Commodity"]]
df = df.rename(
    columns={"DataID": "pdfId", "Application title short": "application_title_short", "Commodity": "commodity"})

with engine.connect() as conn:
    for row in df.itertuples():
        # print()
        # print(row)
        # print(row["application_name"])
        stmt = text(
            "UPDATE esa.pdfs SET application_name = :application_name, " +
            "application_title_short = :application_title_short, short_name = :short_name, commodity = :commodity " +
            "WHERE pdfId = :pdfId;")
        params = {"application_name": row.application_name, "application_title_short": row.application_title_short,
                  "short_name": row.short_name, "commodity": row.commodity, "pdfId": row.pdfId}
        result = conn.execute(stmt, params)
        if result.rowcount != 1:
            print(f"{row.pdfId}: ERROR! Updated {result.rowcount} rows!")
print("All done")
