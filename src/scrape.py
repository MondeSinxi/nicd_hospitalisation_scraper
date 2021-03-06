from loguru import logger
import pdfplumber
import pandas as pd
from pathlib import Path
from typing import Union, Tuple, Generator, Optional
import typer
from pandas._libs.tslibs.timestamps import Timestamp
from numpy import NaN

from database.db import generate_engine
from utils.file_handler import get_files
from utils.formatting import to_snake_case, rename_column

app = typer.Typer()

destination_path = Path("/home/monde/nicd_hospitalisation_scraper/extracted_data")
engine = generate_engine()


@app.command()
def scrape_data(
    files_path: str = ".",
    file_glob: str = "*.pdf",
    page_index: int = 1,
    write_output: bool = True,
):
    """ "
    Scrape PDF tables from a specified page.

    Args:
        files_path (str): file path for PDF files
        file_glob (str): glob for PDF file
        page_index (int): base page integer, first page is 0
        write_output (bool):
    """
    # convert to int if str passed to module
    if isinstance(page_index, str):
        page_index = int(page_index)
    pdf_files = get_files(files_path, file_glob)
    for pdf_file in pdf_files:
        df = extract_table(pdf_file, page_index)
        if write_output:
            output_file_path = destination_path / f"{pdf_file.stem}.csv"
            write_csv(df, output_file_path)
            logger.info(f"Sample table:\n{df.head}")
        logger.debug(df)
    return


def write_csv(df, destination_path: Union[Path, str]) -> None:
    """ "Write csv file from Pandas DataFrame."""
    logger.info(f"Writing to {destination_path}")
    if destination_path.parent.exists():
        destination_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(destination_path, index=False)


def extract_date(page) -> pd._libs.tslibs.timestamps.Timestamp:
    text = page.extract_text()
    if not text:
        logger.warning("No data extracted from file")
        return
    date_str = " ".join(text.split()[:7])
    logger.info(date_str)
    cleaned_date_str = (
        date_str.replace(" NICD", "")
        .replace(",", "")
        .replace("Na\x00onal", "")
        .replace("COVID-19", "")
        .replace("Hospital", "")
        .replace("National Daily Report", "")
        .replace("Power BI Desktop ", "")
        .replace("Surveillance", "")
        .replace("v3", "")
        .replace("pg1", "")
    )
    logger.debug(f"Found date: {cleaned_date_str}")
    return pd.to_datetime(cleaned_date_str)


def extract_table(page) -> pd.DataFrame:
    """ "Extract largest table from a page in a pdf file."""
    table = page.extract_table()
    logger.debug(table[0])
    column_names = map(to_snake_case, table[0])
    column_names = map(rename_column, column_names)
    return pd.DataFrame(table[1:], columns=column_names)


def add_hash_to_df(df: pd.DataFrame) -> pd.DataFrame:
    hashes = [hash(str(row.values)) for idx, row in df.iterrows()]
    df["id"] = hashes
    return df


def extract(
    pdf_file: Path, page_index_table: int, page_index_date
) -> Tuple[Timestamp, pd.DataFrame]:
    with pdfplumber.open(pdf_file) as pdf:
        table = extract_table(pdf.pages[page_index_table])
        table = table.convert_dtypes()
        date = extract_date(pdf.pages[page_index_date])
    return (date, table)


def clean_df(df) -> pd.DataFrame:
    provinces = [
        "Eastern Cape",
        "Free State",
        "Gauteng",
        "KwaZulu-Natal",
        "Limpopo",
        "Mpumalanga",
        " North West",
        "North West",
        "Northern Cape",
        "Western Cape",
        "Total",
    ]
    logger.debug(df)
    clean_df = df[df["province"].isin(provinces)]
    # replace empty string with NAN then drop the column
    clean_df = clean_df.replace(r"^\s*$", NaN, regex=True)
    clean_df = clean_df.dropna(axis=1)

    data_columns = list(clean_df.columns)
    data_columns.remove("province")
    logger.debug(clean_df)
    types = {c: int for c in data_columns}
    return clean_df.astype(types).reset_index(drop=True)


def get_national_data(df: pd.DataFrame) -> pd.DataFrame:
    data_columns = [c for c in df.columns if c != "province"]
    return pd.DataFrame(df.sum()[data_columns]).T


def aggregate_data(files: Generator[Path, None, None]) -> pd.DataFrame:
    for file in files:
        logger.debug(f"Extract from {file}")
        date, df = extract(file, 1, 0)
        if date:
            df = clean_df(df)
            df["date"] = date
            df = add_hash_to_df(df)
            yield date, df.sort_values(by=["date"]).reset_index(drop=True)


def print_data(data):
    logger.info(data)


def save_data(data: pd.DataFrame, destination_path: Path, db="csv"):
    if db == "csv":
        write_csv(data, destination_path)
    if db == "sqlite":
        logger.debug(f"writing to database:\n {data}")
        data.to_sql("hospitalisation", engine, if_exists="append", index=False)


@app.command("extract-pdf")
def main(
    source_path: str,
    filename_pattern: str,
    destination_path: str = ".",
    store_data: bool = False,
    db: str = "csv",
):
    """Extract national hospitalisation data."""
    files = Path(source_path).glob(filename_pattern)
    agg = aggregate_data(files)
    destination_path = Path(destination_path)
    if store_data:
        for i, data in enumerate(agg):
            destination_file_path = destination_path / f"{data[0]:%Y-%m-%d}_nicd.csv"
            save_data(data[1], destination_file_path, db=db)


if __name__ == "__main__":
    app()
