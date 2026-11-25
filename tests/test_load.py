from conftest import make_payload

from my_pipeline import load
from my_pipeline.validate import validate_weather


def test_loading_the_same_batch_twice_changes_nothing(tmp_path):
    database = str(tmp_path / "test.sqlite3")
    silver = validate_weather(make_payload(), "accra")

    load.save(database, silver)
    load.save(database, silver)

    assert load.count_rows(database) == {"weather_daily": 3, "weather_features": 3}
    assert str(load.latest_date(database, "accra")) == "2026-01-03"