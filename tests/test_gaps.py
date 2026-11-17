import pytest

def test_anomaly_is_open_gap():
    from src.freshnessgate import anomaly
    with pytest.raises(NotImplementedError):
        anomaly.process({"id": "x"})

def test_schema_check_is_open_gap():
    from src.freshnessgate import schema_check
    with pytest.raises(NotImplementedError):
        schema_check.process({"id": "x"})
