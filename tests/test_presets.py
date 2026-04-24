"""Tests for presets.py."""

from __future__ import annotations

import pytest

from custom_components.bfe_rueckliefertarif.const import BASE_MODE_FIXED, BASE_MODE_RMP
from custom_components.bfe_rueckliefertarif.presets import (
    PRESETS,
    get_preset,
    list_preset_keys,
)


class TestPresetTable:
    def test_all_presets_have_valid_base_mode(self):
        for key, p in PRESETS.items():
            assert p.base_mode in (BASE_MODE_RMP, BASE_MODE_FIXED), (
                f"{key} has invalid base_mode {p.base_mode!r}"
            )

    def test_fixed_mode_has_fixed_rate(self):
        for key, p in PRESETS.items():
            if p.base_mode == BASE_MODE_FIXED:
                assert p.fixed_rate_rp_kwh is not None, (
                    f"{key} is fixed mode but has no fixed_rate_rp_kwh"
                )
                assert p.fixed_rate_rp_kwh > 0

    def test_rmp_mode_has_no_fixed_rate(self):
        for key, p in PRESETS.items():
            if p.base_mode == BASE_MODE_RMP:
                assert p.fixed_rate_rp_kwh is None, (
                    f"{key} is RMP mode but has fixed_rate_rp_kwh set"
                )

    def test_hkn_in_reasonable_range(self):
        for key, p in PRESETS.items():
            assert 0.0 <= p.hkn_bonus_rp_kwh <= 5.0, (
                f"{key} HKN {p.hkn_bonus_rp_kwh} outside sanity range"
            )

    def test_display_name_set(self):
        for key, p in PRESETS.items():
            assert p.display_name
            assert p.key == key


class TestNamedPresets:
    def test_ekz(self):
        p = get_preset("ekz")
        assert p.base_mode == BASE_MODE_RMP
        assert p.hkn_bonus_rp_kwh == 3.0
        assert p.fixed_rate_rp_kwh is None

    def test_iwb(self):
        p = get_preset("iwb")
        assert p.base_mode == BASE_MODE_FIXED
        assert p.fixed_rate_rp_kwh == 14.0

    def test_aew_hkn_inclusive(self):
        p = get_preset("aew")
        assert p.base_mode == BASE_MODE_FIXED
        assert p.fixed_rate_rp_kwh == 8.2
        assert p.hkn_bonus_rp_kwh == 0.0

    def test_unknown_raises(self):
        with pytest.raises(KeyError):
            get_preset("does_not_exist")


class TestListOrder:
    def test_custom_is_last(self):
        keys = list_preset_keys()
        assert keys[-1] == "custom"
        assert "custom" not in keys[:-1]

    def test_all_presets_listed(self):
        assert set(list_preset_keys()) == set(PRESETS.keys())
