from urllib import request
from urllib.error import URLError, HTTPError
from loguru import logger
from pandas import to_datetime
from datetime import datetime, timedelta
import typer
from typing import List, Optional
from pathlib import Path
from config import NICD_FILENAME_TEMPLATES

app = typer.Typer()


@app.command()
def get_nicd_files(start_date: str, end_date: Optional[str] = None, file_path: Optional[str] = "."):
    dates = datetime_range(start_date, end_date)
    for date in dates:
        _retrieve_nicd_files(date, file_path)


def datetime_range(start_date, end_date) -> List[datetime]:
    start_date = string_to_datetime(start_date)
    if not end_date:
        return [start_date]
    end_date = string_to_datetime(end_date)
    return get_date_range(start_date, end_date)


def string_to_datetime(date: str) -> datetime:
    """ Convert string to timezone object to datetime object"""
    return to_datetime(date).to_pydatetime()


def get_date_range(start_date: datetime, end_date: datetime) -> List[datetime]:
    """ Return a list of datetime objects for the range between start and end dates in days."""
    delta_dates = end_date - start_date
    for s in range(delta_dates.days):
        yield start_date + timedelta(days=s)


def _retrieve_nicd_files(date: datetime, destination_path: Path) -> None:
    """
    File naming is incosistent on the serever side; we have to try as many combinations as possible whilst avoiding 404 errors.
    """
    for file in NICD_FILENAME_TEMPLATES:
        file = file.format(date=date)
        try:
            url_template = 'https://www.nicd.ac.za/wp-content/uploads/{date:%Y}/{date:%m}/{file}'.format(
                date=date, file=file)
            get_file(url_template, destination_path, file)
        except HTTPError:
            continue
        return


@app.command()
def get_file(url: str, destination_path: str, filename: str) -> None:
    local_file_path = _create_local_file_path(destination_path, filename)
    if local_file_path.exists():
        logger.info(f"file exists: {local_file_path}")
        return
    logger.debug(f"Fetching file from {url}")
    Path(destination_path).mkdir(parents=True, exist_ok=True)
    request.urlretrieve(url, local_file_path)
    return


def _create_local_file_path(file_path: str, filename: str) -> Path:
    if file_path:
        root_path = Path(file_path)
        return root_path / filename


if __name__ == "__main__":
    app()
