from tests.fixtures.mock_sinks import MockSink, make_mock_sink
import pandas as pd


def test_make_mock_sink_lakehouse():
    sink = make_mock_sink("lakehouse")
    assert isinstance(sink, MockSink)
    assert sink.sink_type == "lakehouse"
    assert sink.write_count == 0
    assert sink.total_rows == 0


def test_make_mock_sink_all_types():
    for sink_type in ["lakehouse", "warehouse", "eventhouse", "sql-database", "sql-server"]:
        sink = make_mock_sink(sink_type)
        assert isinstance(sink, MockSink), f"Failed for {sink_type}"


def test_make_mock_sink_invalid():
    import pytest
    with pytest.raises(ValueError):
        make_mock_sink("invalid-sink")


def test_mock_sink_write_records_rows():
    sink = make_mock_sink("lakehouse")
    # Simulate a GenerationResult.tables dict
    class FakeResult:
        tables = {
            "customer": pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]}),
            "order": pd.DataFrame({"id": [10, 20], "customer_id": [1, 2]}),
        }
    sink.write(FakeResult())
    assert sink.write_count == 1
    assert sink.total_rows == 5
    assert set(sink.tables_written) == {"customer", "order"}


def test_mock_sink_write_stream():
    sink = make_mock_sink("warehouse")
    df = pd.DataFrame({"id": [1, 2]})
    sink.write_stream("my_table", df)
    assert sink.total_rows == 2
    assert "my_table" in sink.tables_written


def test_mock_sink_assert_written():
    sink = make_mock_sink("lakehouse")
    df = pd.DataFrame({"id": [1]})
    sink.write_stream("t", df)
    sink.assert_written(min_rows=1)  # should not raise


def test_mock_sink_assert_written_fails():
    import pytest
    sink = make_mock_sink("lakehouse")
    with pytest.raises(AssertionError):
        sink.assert_written(min_rows=1)
