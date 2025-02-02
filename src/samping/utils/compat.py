from datetime import datetime, date
import uuid
import decimal
import msgpack


def encode_non_standard_msgpack(obj):
    if isinstance(obj, (decimal.Decimal, uuid.UUID)):
        return str(obj)
    if isinstance(obj, (datetime, date)):
        if not isinstance(obj, datetime):
            obj = datetime(obj.year, obj.month, obj.day, 0, 0, 0, 0)
        r = obj.isoformat()
        if r.endswith("+00:00"):
            r = r[:-6] + "Z"
        return r
    return obj


def msgpack_dumps(s, **kwargs):
    return msgpack.packb(s, default=encode_non_standard_msgpack, use_bin_type=True)
