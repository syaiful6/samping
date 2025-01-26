def countdown(num: int, step: int):
    while num > 0:
        if num > step:
            yield step
        else:
            yield num
        num -= step


def dictfilter(d=None, **kw):
    """Remove all keys from dict ``d`` whose value is :const:`None`."""
    d = kw if d is None else (dict(d, **kw) if kw else d)
    return {k: v for k, v in d.items() if v is not None}
