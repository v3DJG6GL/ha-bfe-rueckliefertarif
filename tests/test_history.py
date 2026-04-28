"""Tests for v0.8.0+: unified config-history timeline (post-A+ refactor).

Covers:
- ``_resolve_config_at`` picks the correct full-config dict per ``at_date``,
  honoring half-open ``[valid_from, valid_to)`` semantics.
- ``_normalize_history`` sorts by valid_from and chains valid_to.
- ``_parse_valid_from`` accepts ISO-date inputs (YYYY-MM-DD).

Note (v0.9.0): ``_apply_config_change`` and ``_sync_entry_data_from_history``
are removed — the wizard inlines history mutation and entry.data no longer
mirrors versioned fields. The ``apply_change`` wizard's persistence path is
covered by tests/test_setup_and_options_flow.py.
"""

from __future__ import annotations

from datetime import date

from custom_components.bfe_rueckliefertarif.config_flow import (
    _append_history_record,
    _format_config_summary,
    _make_sentinel_record,
    _normalize_history,
    _parse_valid_from,
)
from custom_components.bfe_rueckliefertarif.const import (
    ABRECHNUNGS_RHYTHMUS_MONAT,
    ABRECHNUNGS_RHYTHMUS_QUARTAL,
    CONF_ABRECHNUNGS_RHYTHMUS,
    CONF_EIGENVERBRAUCH_AKTIVIERT,
    CONF_ENERGIEVERSORGER,
    CONF_HKN_AKTIVIERT,
    CONF_INSTALLIERTE_LEISTUNG_KW,
    OPT_CONFIG_HISTORY,
)
from custom_components.bfe_rueckliefertarif.services import _resolve_config_at


def _cfg(
    utility="ekz",
    kw=8.0,
    ev=True,
    hkn=False,
    billing=ABRECHNUNGS_RHYTHMUS_QUARTAL,
):
    return {
        CONF_ENERGIEVERSORGER: utility,
        CONF_INSTALLIERTE_LEISTUNG_KW: kw,
        CONF_EIGENVERBRAUCH_AKTIVIERT: ev,
        CONF_HKN_AKTIVIERT: hkn,
        CONF_ABRECHNUNGS_RHYTHMUS: billing,
    }


class TestResolveConfigAt:
    def test_no_history_falls_back_to_entry_data(self):
        out = _resolve_config_at({}, date(2024, 6, 1), _cfg(utility="bkw", kw=15.0))
        assert out[CONF_ENERGIEVERSORGER] == "bkw"
        assert out[CONF_INSTALLIERTE_LEISTUNG_KW] == 15.0

    def test_picks_active_record_at_date(self):
        opts = {
            OPT_CONFIG_HISTORY: [
                {"valid_from": "1970-01-01", "valid_to": "2024-07-01",
                 "config": _cfg(utility="ekz", hkn=False)},
                {"valid_from": "2024-07-01", "valid_to": "2025-04-01",
                 "config": _cfg(utility="ekz", hkn=True)},
                {"valid_from": "2025-04-01", "valid_to": None,
                 "config": _cfg(utility="ewz", hkn=True, kw=35.0,
                                billing=ABRECHNUNGS_RHYTHMUS_MONAT)},
            ]
        }
        # Pre-HKN window
        r = _resolve_config_at(opts, date(2024, 1, 1), _cfg())
        assert r[CONF_ENERGIEVERSORGER] == "ekz"
        assert r[CONF_HKN_AKTIVIERT] is False
        # Post-HKN, pre-utility-switch window
        r = _resolve_config_at(opts, date(2024, 8, 1), _cfg())
        assert r[CONF_HKN_AKTIVIERT] is True
        assert r[CONF_ENERGIEVERSORGER] == "ekz"
        # Post utility switch
        r = _resolve_config_at(opts, date(2025, 6, 1), _cfg())
        assert r[CONF_ENERGIEVERSORGER] == "ewz"
        assert r[CONF_INSTALLIERTE_LEISTUNG_KW] == 35.0
        assert r[CONF_ABRECHNUNGS_RHYTHMUS] == ABRECHNUNGS_RHYTHMUS_MONAT

    def test_predating_first_record_falls_back_to_entry_data(self):
        # When at_date predates every record's valid_from, the resolver
        # MUST NOT extrapolate the newer tariff backward in time. It falls
        # back to entry.data (today's open-ended config) and logs a warning
        # so the missing-sentinel state is observable.
        opts = {
            OPT_CONFIG_HISTORY: [
                {"valid_from": "2025-04-01", "valid_to": None,
                 "config": _cfg(utility="ewz", hkn=True)},
            ]
        }
        r = _resolve_config_at(opts, date(2024, 1, 1), _cfg(utility="other"))
        assert r[CONF_ENERGIEVERSORGER] == "other"

    def test_boundary_valid_from_belongs_to_new_record(self):
        opts = {
            OPT_CONFIG_HISTORY: [
                {"valid_from": "1970-01-01", "valid_to": "2025-04-01",
                 "config": _cfg(utility="ekz")},
                {"valid_from": "2025-04-01", "valid_to": None,
                 "config": _cfg(utility="ewz")},
            ]
        }
        # Half-open: 2025-04-01 is the first day of the new record.
        assert _resolve_config_at(opts, date(2025, 4, 1), _cfg())[
            CONF_ENERGIEVERSORGER] == "ewz"
        assert _resolve_config_at(opts, date(2025, 3, 31), _cfg())[
            CONF_ENERGIEVERSORGER] == "ekz"


class TestMakeSentinelRecord:
    def test_uses_entry_data(self):
        rec = _make_sentinel_record(_cfg(utility="bkw", kw=12.5, hkn=True))
        assert rec["valid_from"] == "1970-01-01"
        assert rec["valid_to"] is None
        assert rec["config"][CONF_ENERGIEVERSORGER] == "bkw"
        assert rec["config"][CONF_INSTALLIERTE_LEISTUNG_KW] == 12.5
        assert rec["config"][CONF_HKN_AKTIVIERT] is True


class TestAppendHistoryRecord:
    def test_seeds_sentinel_when_history_is_empty(self):
        new_rec = {
            "valid_from": "2026-04-01",
            "valid_to": None,
            "config": _cfg(utility="age_sa", kw=105.0),
        }
        out = _append_history_record(
            [], new_rec, _cfg(utility="ekz", kw=10.0)
        )
        # Sentinel auto-prepended so past quarters resolve to entry.data,
        # not to the just-added 2026 record.
        assert len(out) == 2
        assert out[0]["valid_from"] == "1970-01-01"
        assert out[0]["config"][CONF_ENERGIEVERSORGER] == "ekz"
        assert out[1]["valid_from"] == "2026-04-01"
        assert out[1]["config"][CONF_ENERGIEVERSORGER] == "age_sa"

    def test_appends_without_sentinel_when_history_has_records(self):
        existing = [
            {"valid_from": "1970-01-01", "valid_to": None,
             "config": _cfg(utility="ekz")},
        ]
        new_rec = {
            "valid_from": "2026-04-01",
            "valid_to": None,
            "config": _cfg(utility="age_sa"),
        }
        out = _append_history_record(existing, new_rec, _cfg(utility="other"))
        # Existing sentinel preserved as-is, no second one prepended.
        assert len(out) == 2
        assert out[0]["config"][CONF_ENERGIEVERSORGER] == "ekz"
        assert out[1]["config"][CONF_ENERGIEVERSORGER] == "age_sa"

    def test_input_history_not_mutated(self):
        existing = [
            {"valid_from": "1970-01-01", "valid_to": None, "config": _cfg()},
        ]
        _append_history_record(
            existing,
            {"valid_from": "2026-04-01", "valid_to": None, "config": _cfg()},
            _cfg(),
        )
        assert len(existing) == 1


class TestNormalizeHistory:
    def test_sorts_by_valid_from(self):
        records = [
            {"valid_from": "2025-04-01", "config": _cfg(utility="ewz")},
            {"valid_from": "1970-01-01", "config": _cfg(utility="ekz")},
            {"valid_from": "2024-07-01", "config": _cfg(utility="bkw")},
        ]
        out = _normalize_history(records)
        assert [r["valid_from"] for r in out] == [
            "1970-01-01", "2024-07-01", "2025-04-01"
        ]

    def test_chains_valid_to(self):
        records = [
            {"valid_from": "2024-01-01", "config": _cfg()},
            {"valid_from": "2024-07-01", "config": _cfg()},
            {"valid_from": "2025-01-01", "config": _cfg()},
        ]
        out = _normalize_history(records)
        assert out[0]["valid_to"] == "2024-07-01"
        assert out[1]["valid_to"] == "2025-01-01"
        assert out[2]["valid_to"] is None

    def test_dedupes_same_valid_from_last_wins(self):
        records = [
            {"valid_from": "2024-01-01", "config": _cfg(utility="ekz")},
            {"valid_from": "2024-01-01", "config": _cfg(utility="bkw")},
        ]
        out = _normalize_history(records)
        assert len(out) == 1
        assert out[0]["config"][CONF_ENERGIEVERSORGER] == "bkw"


class TestParseValidFrom:
    def test_iso_date(self):
        assert _parse_valid_from("2024-08-15") == "2024-08-15"

    def test_rejects_garbage(self):
        import pytest
        with pytest.raises(ValueError):
            _parse_valid_from("foobar")
        with pytest.raises(ValueError):
            _parse_valid_from("")
        # v0.9.8: quarter shorthand (YYYYQN) no longer accepted; DateSelector
        # always emits ISO. The parser should reject quarter strings now.
        with pytest.raises(ValueError):
            _parse_valid_from("2024Q3")


class TestDeriveBilling:
    """v0.9.8 #9 — billing rhythm comes from utility's settlement_period."""

    def test_quartal_utility_yields_quartal(self):
        from custom_components.bfe_rueckliefertarif.config_flow import _derive_billing
        from custom_components.bfe_rueckliefertarif.const import (
            ABRECHNUNGS_RHYTHMUS_QUARTAL,
        )

        # All bundled utilities use settlement_period="quartal" today.
        assert _derive_billing("ekz", "2026-04-01") == ABRECHNUNGS_RHYTHMUS_QUARTAL
        # v0.11.0 — AEW unified into one entry with user_inputs.tariff_model.
        assert _derive_billing("aew", "2026-04-01") == ABRECHNUNGS_RHYTHMUS_QUARTAL

    def test_unknown_utility_raises(self):
        import pytest

        from custom_components.bfe_rueckliefertarif.config_flow import _derive_billing

        with pytest.raises(KeyError):
            _derive_billing("does_not_exist", "2026-04-01")

    def test_no_active_rate_raises(self):
        import pytest

        from custom_components.bfe_rueckliefertarif.config_flow import _derive_billing

        # Bundled rates start 2026-01-01.
        with pytest.raises(LookupError):
            _derive_billing("ekz", "2025-06-01")


class TestHknGate:
    """v0.9.8 #1 — gate ``hkn_aktiviert`` toggle on the utility's
    ``hkn_structure``."""

    def test_active_hkn_structure_resolves_for_known_utility(self):
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _active_hkn_structure,
        )

        # ekz has hkn_structure="additive_optin" in bundled data.
        assert _active_hkn_structure("ekz", "2026-04-01") == "additive_optin"
        # v0.11.0 — AEW's first power_tier (applies_when={tariff_model:fixpreis})
        # has hkn_structure="bundled". The function picks the first tier as a
        # heuristic; both AEW variants gate the HKN toggle either way (rmp
        # tier has hkn_structure="none").
        assert _active_hkn_structure("aew", "2026-04-01") == "bundled"

    def test_active_hkn_structure_returns_none_on_lookup_failure(self):
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _active_hkn_structure,
        )

        # Unknown utility → None (graceful degradation; UI shows toggle).
        assert _active_hkn_structure("does_not_exist", "2026-04-01") is None
        # Date before any rate window → None.
        assert _active_hkn_structure("ekz", "2020-01-01") is None
        # Garbage date → None (caught by ValueError).
        assert _active_hkn_structure("ekz", "garbage") is None

    def test_force_hkn_for_save_additive_optin_preserves_choice(self):
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _force_hkn_for_save,
        )

        assert _force_hkn_for_save("additive_optin", True) is True
        assert _force_hkn_for_save("additive_optin", False) is False

    def test_force_hkn_for_save_bundled_forces_false(self):
        # Math-correct: HKN already in base rate, don't double-add.
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _force_hkn_for_save,
        )

        assert _force_hkn_for_save("bundled", True) is False
        assert _force_hkn_for_save("bundled", False) is False

    def test_force_hkn_for_save_none_forces_false(self):
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _force_hkn_for_save,
        )

        assert _force_hkn_for_save("none", True) is False
        assert _force_hkn_for_save("none", False) is False

    def test_force_hkn_for_save_null_preserves_choice(self):
        # Legacy / missing field → preserve user choice (don't force).
        from custom_components.bfe_rueckliefertarif.config_flow import (
            _force_hkn_for_save,
        )

        assert _force_hkn_for_save(None, True) is True
        assert _force_hkn_for_save(None, False) is False


class TestFormatConfigSummary:
    def test_includes_all_fields(self):
        s = _format_config_summary(_cfg(
            utility="ekz", kw=8.5, ev=True, hkn=True,
            billing=ABRECHNUNGS_RHYTHMUS_QUARTAL,
        ))
        assert "ekz" in s
        assert "8.5 kW" in s
        assert "EV" in s and "no-EV" not in s
        assert "HKN" in s and "no-HKN" not in s
        assert "quartal" in s

    def test_negated_flags_render_no_prefix(self):
        s = _format_config_summary(_cfg(ev=False, hkn=False))
        assert "no-EV" in s
        assert "no-HKN" in s
