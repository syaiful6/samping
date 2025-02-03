from collections import UserDict


class Conf(dict):
    def __getattr__(self, k):
        """d.key -> d[key]"""
        try:
            return self[k]
        except KeyError:
            raise AttributeError(f"{type(self).__name__} object has no attribute {k!r}")

    def __setattr__(self, key: str, value) -> None:
        self[key] = value
