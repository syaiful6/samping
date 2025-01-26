from datetime import timezone

from .example_app import app


def test_config_setting():
    tz = app.timezone
    assert isinstance(tz, timezone)


def test_timezone_config():
    app.conf.timezone = "Asia/Jakarta"
    assert app.conf.timezone == "Asia/Jakarta"

    now = app.now()
    assert not now.utcoffset()