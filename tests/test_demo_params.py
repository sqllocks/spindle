"""Tests for DemoParams."""
from sqllocks_spindle.demo.params import DemoParams


def test_default_scale_mode_is_auto():
    p = DemoParams()
    assert p.scale_mode == "auto"


def test_scale_mode_can_be_set_to_local():
    p = DemoParams(scale_mode="local")
    assert p.scale_mode == "local"


def test_scale_mode_can_be_set_to_spark():
    p = DemoParams(scale_mode="spark")
    assert p.scale_mode == "spark"
