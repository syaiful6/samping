def str_to_bytes(s):
    """Convert str to bytes."""
    if isinstance(s, str):
        return s.encode()
    return s


def bytes_to_str(s):
    if isinstance(s, bytes):
        return s.decode(errors="replace")
    return s
