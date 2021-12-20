from pathlib import Path

def get_files(files_path: str, file_glob: str) -> list[Path]:
    """Return list of Path objects given a file path and file glob"""
    files = list(Path(files_path).glob(file_glob))
    if not files:
        raise Exception("No files found, make sure path is correct.")
    return files


def to_snake_case(s: str) -> str:
    """"Convert string to snake case"""
    return s.replace('\n', ' ').replace(' ', '_').lower()