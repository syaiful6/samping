import re
from datetime import datetime
from pytz import FixedOffset, utc

ISO8601_REGEX = re.compile(
    r"(?P<year>[0-9]{4})(-(?P<month>[0-9]{1,2})(-(?P<day>[0-9]{1,2})"
    r"((?P<separator>.)(?P<hour>[0-9]{2}):(?P<minute>[0-9]{2})"
    r"(:(?P<second>[0-9]{2})(\.(?P<fraction>[0-9]+))?)?"
    r"(?P<timezone>Z|(([-+])([0-9]{2}):([0-9]{2})))?)?)?)?"
)
TIMEZONE_REGEX = re.compile(
    r"(?P<prefix>[+-])(?P<hours>[0-9]{2}).(?P<minutes>[0-9]{2})"
)


def parse_iso8601(datestr: str) -> datetime:
    m = ISO8601_REGEX.match(datestr)
    if not m:
        raise ValueError(f"unable to parse date string {datestr}")
    groups = m.groupdict()
    tz = groups["timezone"]
    if tz == "Z":
        tz = FixedOffset(0)
    elif tz:
        m = TIMEZONE_REGEX.match(tz)
        prefix, hours, minutes = m.groups()
        hours, minutes = int(hours), int(minutes)
        if prefix == "-":
            hours = -hours
            minutes = -minutes
        tz = FixedOffset(minutes + hours * 60)
    return datetime(
        int(groups["year"]),
        int(groups["month"]),
        int(groups["day"]),
        int(groups["hour"] or 0),
        int(groups["minute"] or 0),
        int(groups["second"] or 0),
        int(groups["fraction"] or 0),
        tz,
    )


def utcnow():
    t = datetime.utcnow()
    return t.replace(tzinfo=utc)


def to_iso_format(d: datetime) -> str:
    r = d.isoformat()

    if r.endswith("+00:00"):
        r = r[:-6] + "Z"

    return r


def countdown(num: int, step: int):
    while num > 0:
        if num > step:
            yield step
        else:
            yield num
        num -= step


def try_to_int(value: str, default: int = 0) -> int:
    try:
        return int(value)
    except ValueError:
        return default
