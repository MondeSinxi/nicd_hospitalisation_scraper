from loguru import logger
import pdfplumber
import pandas as pd
from pathlib import Path
from typing import Union, Tuple, Generator, Optional
import typer
from utils import get_files, to_snake_case
from pandas._libs.tslibs.timestamps import Timestamp

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


def extract_date(page) -> pd._libs.tslibs.timestamps.Timestamp:
    date_str = ' '.join(page.extract_text().split()[:4])
    cleaned_date_str = date_str.replace(',', '').replace(
        ' NICD', '').replace('National Daily Report', '')
    logger.debug(f"Found date: {cleaned_date_str}")
    return pd.to_datetime(cleaned_date_str)


def extract_table(page) -> pd.DataFrame:
    """"Extract largest table from a page in a pdf file."""
    table = page.extract_table()
    column_names = map(to_snake_case, table[0])
    return pd.DataFrame(table[1:], columns=column_names)


def extract(pdf_file: Path, page_index_table: int, page_index_date) -> Tuple[Timestamp, pd.DataFrame]:
    with pdfplumber.open(pdf_file) as pdf:
        table = extract_table(pdf.pages[page_index_table])
        date = extract_date(pdf.pages[page_index_date])
    return (date, table)


def clean_df(df) -> pd.DataFrame:
    provinces = ['Eastern Cape', 'Free State', 'Gauteng', 'KwaZulu-Natal',
                                          'Limpopo', 'Mpumalanga', ' North West', 'Northern Cape', 'Western Cape']
    clean_df = df[df['province'].isin(provinces)]
    return clean_df.astype({'facilities_reporting': 'int', 'admissions_to_date': int, 'died_to_date': int, 'discharged_to_date': int, 'currently_admitted': int,
              'currently_in_icu': int, 'currently_ventilated': int, 'currently_oxygenated': int, 'admissions_in_previous_day': int}).reset_index(drop=True)


def get_national_data(df: pd.DataFrame) -> pd.DataFrame:
    data_columns = [c for c in df.columns if c != 'province']
    return pd.DataFrame(df.sum()[data_columns]).T

def aggregate_data(files: Generator[Path, None, None]) -> pd.DataFrame:
    super_df = pd.DataFrame({})
    for file in files:
        logger.debug(f"Extract from {file}")
        date, df = extract(file, 1, 0)
        df = clean_df(df)
        national_df = get_national_data(df)
        national_df['date'] = date
        super_df = super_df.append(national_df)
    return super_df.sort_values(by=['date']).reset_index(drop=True)

def print_data(data):
    print(data)

def save_data(data: pd.DataFrame, destination_path: Path):
    data.to_csv(destination_path)

@app.command("extract-pdf")
def main(source_path: str, filename_pattern: str, destination_path: Optional[str] = None, write_file: bool = False, show: bool = True):
    """ Extract national hospitalisation data. """
    timestamp = Timestamp.now().strftime('%Y%m%d%H%M%S')
    files = Path(source_path).glob(filename_pattern)
    agg = aggregate_data(files)
    if write_file:
        destination_file_path = Path(destination_path) / f"{timestamp}_nicd_hospitalisation.csv"
        agg.to_csv(destination_file_path)
    if show:
        print(agg)

if __name__ == "__main__":
    app()
