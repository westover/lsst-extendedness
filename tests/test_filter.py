"""
Tests for filter engine and presets.
"""

from __future__ import annotations

import pytest

from lsst_extendedness.filter.engine import (
    FilterCondition,
    FilterConfig,
    FilterOperator,
)
from lsst_extendedness.filter import presets


class TestFilterCondition:
    """Tests for FilterCondition."""

    def test_eq_condition(self):
        """Test equality condition."""
        cond = FilterCondition.eq("field", "value")
        sql, params = cond.to_sql()

        assert sql == "field = ?"
        assert params == ["value"]

    def test_ne_condition(self):
        """Test not-equal condition."""
        cond = FilterCondition.ne("field", 42)
        sql, params = cond.to_sql()

        assert sql == "field != ?"
        assert params == [42]

    def test_lt_condition(self):
        """Test less-than condition."""
        cond = FilterCondition.lt("value", 100)
        sql, params = cond.to_sql()

        assert sql == "value < ?"
        assert params == [100]

    def test_ge_condition(self):
        """Test greater-than-or-equal condition."""
        cond = FilterCondition.ge("snr", 10.5)
        sql, params = cond.to_sql()

        assert sql == "snr >= ?"
        assert params == [10.5]

    def test_between_condition(self):
        """Test between condition."""
        cond = FilterCondition.between("mjd", 60000.0, 60100.0)
        sql, params = cond.to_sql()

        assert sql == "mjd BETWEEN ? AND ?"
        assert params == [60000.0, 60100.0]

    def test_in_list_condition(self):
        """Test IN condition."""
        cond = FilterCondition.in_list("filter_name", ["g", "r", "i"])
        sql, params = cond.to_sql()

        assert sql == "filter_name IN (?, ?, ?)"
        assert params == ["g", "r", "i"]

    def test_is_null_condition(self):
        """Test IS NULL condition."""
        cond = FilterCondition.is_null("ss_object_id")
        sql, params = cond.to_sql()

        assert sql == "ss_object_id IS NULL"
        assert params == []

    def test_is_not_null_condition(self):
        """Test IS NOT NULL condition."""
        cond = FilterCondition.is_not_null("extendedness_median")
        sql, params = cond.to_sql()

        assert sql == "extendedness_median IS NOT NULL"
        assert params == []


class TestFilterConfig:
    """Tests for FilterConfig."""

    def test_empty_config(self):
        """Test config with no conditions."""
        config = FilterConfig(name="test")
        sql, params = config.to_sql()

        assert sql == "SELECT * FROM alerts_raw"
        assert params == []

    def test_single_condition(self):
        """Test config with single condition."""
        config = FilterConfig(name="test")
        config.add(FilterCondition.eq("has_ss_source", 1))
        sql, params = config.to_sql()

        assert "WHERE has_ss_source = ?" in sql
        assert params == [1]

    def test_multiple_conditions_and(self):
        """Test config with multiple AND conditions."""
        config = FilterConfig(name="test", combine_with="AND")
        config.add(FilterCondition.ge("extendedness_median", 0.3))
        config.add(FilterCondition.le("extendedness_median", 0.7))
        sql, params = config.to_sql()

        assert " AND " in sql
        assert "extendedness_median >= ?" in sql
        assert "extendedness_median <= ?" in sql
        assert params == [0.3, 0.7]

    def test_multiple_conditions_or(self):
        """Test config with multiple OR conditions."""
        config = FilterConfig(name="test", combine_with="OR")
        config.add(FilterCondition.eq("filter_name", "g"))
        config.add(FilterCondition.eq("filter_name", "r"))
        sql, params = config.to_sql()

        assert " OR " in sql
        assert params == ["g", "r"]

    def test_order_by(self):
        """Test ORDER BY clause."""
        config = FilterConfig(name="test", order_by="mjd", order_desc=True)
        sql, params = config.to_sql()

        assert "ORDER BY mjd DESC" in sql

    def test_limit(self):
        """Test LIMIT clause."""
        config = FilterConfig(name="test", limit=100)
        sql, params = config.to_sql()

        assert "LIMIT 100" in sql

    def test_fluent_interface(self):
        """Test fluent add() interface."""
        config = (
            FilterConfig(name="test")
            .add(FilterCondition.ge("snr", 10))
            .add(FilterCondition.eq("has_ss_source", 1))
        )

        assert len(config.conditions) == 2

    def test_serialization(self):
        """Test to_dict and from_dict."""
        config = FilterConfig(
            name="test_filter",
            description="Test description",
            limit=50,
        )
        config.add(FilterCondition.ge("snr", 10))
        config.add(FilterCondition.between("extendedness_median", 0.3, 0.7))

        data = config.to_dict()
        restored = FilterConfig.from_dict(data)

        assert restored.name == config.name
        assert restored.description == config.description
        assert restored.limit == config.limit
        assert len(restored.conditions) == len(config.conditions)

        # Verify conditions restored correctly
        sql1, params1 = config.to_sql()
        sql2, params2 = restored.to_sql()
        assert sql1 == sql2
        assert params1 == params2


class TestPresets:
    """Tests for filter presets."""

    def test_point_sources_preset(self):
        """Test point sources preset."""
        config = presets.point_sources()

        assert config.name == "point_sources"
        assert len(config.conditions) >= 1

        sql, params = config.to_sql()
        assert "extendedness_median" in sql

    def test_extended_sources_preset(self):
        """Test extended sources preset."""
        config = presets.extended_sources()

        assert config.name == "extended_sources"
        sql, params = config.to_sql()
        assert "extendedness_median" in sql

    def test_minimoon_candidates_preset(self):
        """Test minimoon candidates preset."""
        config = presets.minimoon_candidates()

        assert config.name == "minimoon_candidates"
        sql, params = config.to_sql()
        assert "has_ss_source" in sql
        assert "extendedness_median" in sql

    def test_sso_alerts_preset(self):
        """Test SSO alerts preset."""
        config = presets.sso_alerts()

        assert config.name == "sso_alerts"
        sql, params = config.to_sql()
        assert "has_ss_source = ?" in sql

    def test_high_snr_preset(self):
        """Test high SNR preset."""
        config = presets.high_snr(min_snr=100)

        sql, params = config.to_sql()
        assert "snr >= ?" in sql
        assert 100 in params

    def test_preset_with_limit(self):
        """Test preset with limit parameter."""
        config = presets.point_sources(limit=50)

        sql, params = config.to_sql()
        assert "LIMIT 50" in sql

    def test_get_preset(self):
        """Test get_preset function."""
        config = presets.get_preset("point_sources")
        assert config.name == "point_sources"

    def test_get_preset_invalid(self):
        """Test get_preset with invalid name."""
        with pytest.raises(ValueError, match="Unknown preset"):
            presets.get_preset("nonexistent")

    def test_list_presets(self):
        """Test list_presets function."""
        preset_list = presets.list_presets()

        assert len(preset_list) > 0
        assert all("name" in p for p in preset_list)
        assert all("description" in p for p in preset_list)

    def test_sky_region_preset(self):
        """Test sky region preset."""
        config = presets.sky_region(
            ra_min=180.0,
            ra_max=190.0,
            dec_min=40.0,
            dec_max=50.0,
        )

        sql, params = config.to_sql()
        assert "ra >=" in sql
        assert "dec >=" in sql
        assert 180.0 in params
        assert 190.0 in params

    def test_time_window_preset(self):
        """Test time window preset."""
        config = presets.time_window(start_mjd=60000.0, end_mjd=60100.0)

        sql, params = config.to_sql()
        assert "BETWEEN" in sql
        assert 60000.0 in params
        assert 60100.0 in params
