REPLACE_COLUMN_NAME = {
    "currently_in_icu": ["current_in_icu_", "current_in_icu"],
    "died_to_date": ["deaths_to_date"],
    "currently_admitted": ["currently_admitted_"],
    "admissions_to_date": ["admissions_to_date_"],
    "admissions_in_previous_day": ["admissions_i_previous_day"],
}


# Add a comment here


def to_snake_case(s: str) -> str:
    """Convert string to snake case"""
    if s:
        s = s.replace("\n", " ").replace(" ", "_").lower()
        return rename_column(s)


def rename_column(s: str) -> str:
    if s:
        for k, v in REPLACE_COLUMN_NAME.items():
            return k if s in v else s
