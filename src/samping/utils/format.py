def try_to_int(value: str, default: int = 0) -> int:
    try:
        return int(value)
    except ValueError:
        return default
