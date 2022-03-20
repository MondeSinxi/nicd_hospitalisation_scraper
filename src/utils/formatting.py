def to_snake_case(s: str) -> str:
    """"Convert string to snake case"""
    return s.replace('\n', ' ').replace(' ', '_').lower()
