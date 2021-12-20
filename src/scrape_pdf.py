from loguru import logger
import pdfplumber
import pandas as pd
from pathlib import Path
from typing import Union
import typer

app = typer.Typer()

destination_path = Path(
    "/home/monde/nicd_hospitalisation_scraper/extracted_data")

@app.command()
def scrape_data(files_path: str = ".", file_glob: str = "*.pdf", page_index: int = 1, write_output: bool = True):
    """"
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
        print(df)
    return

def write_csv(df, output_file_path: Union[Path, str]) -> None:
    """"Write csv file from Pandas DataFrame."""
    logger.info(f"Writing to {output_file_path}")
    if output_file_path.parent.exists():
        output_file_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file_path, index=False)


def extract_table(pdf_file: Path, page_index: int) -> pd.DataFrame:
    """"Extract largest table from a page in a pdf file."""
    with pdfplumber.open(pdf_file) as pdf:
        page = pdf.pages[page_index]
        table = page.extract_table()
    column_names = map(to_snake_case, table[0])
    return pd.DataFrame(table[1:], columns=column_names)


def get_files(files_path: str, file_glob: str) -> list[Path]:
    """Return list of Path objects given a file path and file glob"""
    files = list(Path(files_path).glob(file_glob))
    if not files:
        raise Exception("No files found, make sure path is correct.")
    return files


def to_snake_case(s: str) -> str:
    """"Convert string to snake case"""
    return s.replace('\n', ' ').replace(' ', '_').lower()

if __name__ == "__main__":
    app()
