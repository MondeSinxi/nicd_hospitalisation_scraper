from sqlalchemy import create_engine
from pathlib import Path
from typing import Generator

def get_files(files_path: str, file_glob: str) -> Generator[Path, None, None]:
    """Return Path objects given a file path and file glob"""
    return Path(files_path).glob(file_glob)


def to_snake_case(s: str) -> str:
    """"Convert string to snake case"""
    return s.replace('\n', ' ').replace(' ', '_').lower()

def to_sqlite(df):
    engine = create_engine('sqlite:///nicd_data.db', echo=True)
    Session = sessionmaker(bind=engine)
    Session.close()
