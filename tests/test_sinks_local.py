from __future__ import annotations
import pytest


def test_sink_protocol_importable():
    from sqllocks_spindle.engine.sinks.base import Sink, FabricConnectionProfile
    assert Sink is not None
    assert FabricConnectionProfile is not None


def test_fabric_connection_profile_fields():
    from sqllocks_spindle.engine.sinks.base import FabricConnectionProfile
    p = FabricConnectionProfile(token="tok", endpoint="https://example.com")
    assert p.token == "tok"
    assert p.endpoint == "https://example.com"
