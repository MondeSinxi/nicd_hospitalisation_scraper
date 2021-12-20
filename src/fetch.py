from urllib import request
from urllib.error import URLError, HTTPError
from loguru import logger
from pandas import to_datetime
from datetime import datetime, timedelta
import typer
from pathlib import Path
from config import NICD_FILENAME_TEMPLATES

app = typer.Typer()


@app.command()
def get_nicd_files(start_date: str, end_date: str = None, file_path: str = None):
    start_date = string_to_datetime(start_date)
    if end_date:
        end_date = string_to_datetime(end_date)
        dates = get_date_range(start_date, end_date)
    else:
        dates = [start_date]
    for date in dates:
        _retrieve_nicd_files(date, file_path)


def string_to_datetime(date):
    return to_datetime(date).to_pydatetime()


def get_date_range(start_date, end_date):
    end_date = to_datetime(end_date).to_pydatetime()
    delta_dates = end_date - start_date
    return [start_date + timedelta(days=s)
            for s in range(delta_dates.days)]


def _retrieve_nicd_files(date: datetime, file_path: Path):
    """
    File naming is incosistent on the serever side; we have to try as many combinations as possible whilst avoiding 404 errors.
    """
    for file in NICD_FILENAME_TEMPLATES:
        file = file.format(date=date)
        try:
            url_template = 'https://www.nicd.ac.za/wp-content/uploads/{date:%Y}/{date:%m}/{file}'.format(
                date=date, file=file)
            get_file(url_template, file_path, file)
        except HTTPError:
            continue
        return


@app.command()
def get_file(url, file_path: Path, filename: str):
    local_file_path = _create_local_file_path(file_path, filename)
    if local_file_path.exists():
        logger.info(f"file exists: {local_file_path}")
        return
    logger.info(f"fetching {filename}")
    logger.debug(f"URL: {url}")
    Path(file_path).mkdir(parents=True, exist_ok=True)
    request.urlretrieve(url, local_file_path)
    return


def _create_local_file_path(file_path, filename):
    root_path = Path(file_path)
    if file_path:
        return root_path / filename


if __name__ == "__main__":
    app()
