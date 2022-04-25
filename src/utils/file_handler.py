from pathlib import Path
from typing import Generator


def get_files(files_path: str, file_glob: str) -> Generator[Path, None, None]:
    """Return Path objects given a file path and file glob"""
    return Path(files_path).glob(file_glob)
