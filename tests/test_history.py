"""Tests for Phase 4: per-customer history + import snapshots.

Covers:
- ``_record_history_changes`` correctly closes prior records and appends new ones
  when the user edits kW / Eigenverbrauch / HKN-opt-in.
- ``_resolve_plant`` / ``_resolve_hkn_optin`` honor half-open boundaries:
  the change date itself belongs to the new record.
"""

from __future__ import annotations

from datetime import date

from custom_components.bfe_rueckliefertarif.config_flow import _record_history_changes
from custom_components.bfe_rueckliefertarif.const import (
    CONF_EIGENVERBRAUCH_AKTIVIERT,
    CONF_HKN_AKTIVIERT,
    CONF_INSTALLIERTE_LEISTUNG_KW,
    OPT_HKN_OPTIN_HISTORY,
    OPT_PLANT_HISTORY,
)
from custom_components.bfe_rueckliefertarif.services import (
    _resolve_hkn_optin,
    _resolve_plant,
)


class TestRecordHistoryChanges:
    def test_no_change_yields_empty_history(self):
        old_data = {
            CONF_INSTALLIERTE_LEISTUNG_KW: 25.0,
            CONF_EIGENVERBRAUCH_AKTIVIERT: True,
            CONF_HKN_AKTIVIERT: True,
        }
        new_data = dict(old_data)
        opts = _record_history_changes(
            old_data=old_data, new_data=new_data, old_options={}
        )
        # No changes → history dicts not seeded (we don't backfill on no-op saves).
        assert OPT_PLANT_HISTORY not in opts
        assert OPT_HKN_OPTIN_HISTORY not in opts

    def test_kw_change_appends_new_record(self):
        old_data = {
            CONF_INSTALLIERTE_LEISTUNG_KW: 25.0,
            CONF_EIGENVERBRAUCH_AKTIVIERT: True,
            CONF_HKN_AKTIVIERT: False,
        }
        new_data = {**old_data, CONF_INSTALLIERTE_LEISTUNG_KW: 35.0}
        opts = _record_history_changes(
            old_data=old_data, new_data=new_data, old_options={}
        )
        history = opts[OPT_PLANT_HISTORY]
        assert len(history) == 1
        assert history[0]["installierte_leistung_kw"] == 35.0
        assert history[0]["eigenverbrauch_aktiviert"] is True
        assert history[0]["valid_to"] is None
        assert history[0]["valid_from"] == date.today().isoformat()

    def test_kw_change_closes_prior_record(self):
        today = date.today().isoformat()
        old_data = {
            CONF_INSTALLIERTE_LEISTUNG_KW: 25.0,
            CONF_EIGENVERBRAUCH_AKTIVIERT: True,
            CONF_HKN_AKTIVIERT: False,
        }
        new_data = {**old_data, CONF_INSTALLIERTE_LEISTUNG_KW: 35.0}
        prior = {
            OPT_PLANT_HISTORY: [
                {
                    "valid_from": "2024-03-15",
                    "valid_to": None,
                    "installierte_leistung_kw": 25.0,
                    "eigenverbrauch_aktiviert": True,
                }
            ]
        }
        opts = _record_history_changes(
            old_data=old_data, new_data=new_data, old_options=prior
        )
        history = opts[OPT_PLANT_HISTORY]
        assert len(history) == 2
        assert history[0]["valid_to"] == today  # closed
        assert history[1]["valid_from"] == today
        assert history[1]["valid_to"] is None
        assert history[1]["installierte_leistung_kw"] == 35.0

    def test_eigenverbrauch_change_records_plant_history(self):
        # EV is part of plant_history, not a separate list.
        old_data = {
            CONF_INSTALLIERTE_LEISTUNG_KW: 25.0,
            CONF_EIGENVERBRAUCH_AKTIVIERT: True,
            CONF_HKN_AKTIVIERT: False,
        }
        new_data = {**old_data, CONF_EIGENVERBRAUCH_AKTIVIERT: False}
        opts = _record_history_changes(
            old_data=old_data, new_data=new_data, old_options={}
        )
        history = opts[OPT_PLANT_HISTORY]
        assert len(history) == 1
        assert history[0]["eigenverbrauch_aktiviert"] is False

    def test_hkn_optin_change_appends_new_record(self):
        old_data = {
            CONF_INSTALLIERTE_LEISTUNG_KW: 25.0,
            CONF_EIGENVERBRAUCH_AKTIVIERT: True,
            CONF_HKN_AKTIVIERT: False,
        }
        new_data = {**old_data, CONF_HKN_AKTIVIERT: True}
        opts = _record_history_changes(
            old_data=old_data, new_data=new_data, old_options={}
        )
        history = opts[OPT_HKN_OPTIN_HISTORY]
        assert len(history) == 1
        assert history[0]["opted_in"] is True
        assert history[0]["valid_to"] is None

    def test_hkn_change_closes_prior_record(self):
        today = date.today().isoformat()
        old_data = {
            CONF_INSTALLIERTE_LEISTUNG_KW: 25.0,
            CONF_EIGENVERBRAUCH_AKTIVIERT: True,
            CONF_HKN_AKTIVIERT: False,
        }
        new_data = {**old_data, CONF_HKN_AKTIVIERT: True}
        prior = {
            OPT_HKN_OPTIN_HISTORY: [
                {
                    "valid_from": "2025-01-01",
                    "valid_to": None,
                    "opted_in": False,
                }
            ]
        }
        opts = _record_history_changes(
            old_data=old_data, new_data=new_data, old_options=prior
        )
        history = opts[OPT_HKN_OPTIN_HISTORY]
        assert len(history) == 2
        assert history[0]["valid_to"] == today
        assert history[1]["opted_in"] is True

    def test_simultaneous_kw_and_hkn_change(self):
        old_data = {
            CONF_INSTALLIERTE_LEISTUNG_KW: 25.0,
            CONF_EIGENVERBRAUCH_AKTIVIERT: True,
            CONF_HKN_AKTIVIERT: False,
        }
        new_data = {
            CONF_INSTALLIERTE_LEISTUNG_KW: 50.0,
            CONF_EIGENVERBRAUCH_AKTIVIERT: True,
            CONF_HKN_AKTIVIERT: True,
        }
        opts = _record_history_changes(
            old_data=old_data, new_data=new_data, old_options={}
        )
        assert len(opts[OPT_PLANT_HISTORY]) == 1
        assert len(opts[OPT_HKN_OPTIN_HISTORY]) == 1


class TestResolvePlant:
    def _cfg(self, kw=25.0, ev=True):
        return {
            CONF_INSTALLIERTE_LEISTUNG_KW: kw,
            CONF_EIGENVERBRAUCH_AKTIVIERT: ev,
        }

    def test_no_history_returns_current(self):
        kw, ev = _resolve_plant(self._cfg(kw=25.0, ev=True), {}, date(2026, 4, 1))
        assert (kw, ev) == (25.0, True)

    def test_history_active_record_used(self):
        opts = {
            OPT_PLANT_HISTORY: [
                {
                    "valid_from": "2024-01-01",
                    "valid_to": "2026-06-01",
                    "installierte_leistung_kw": 25.0,
                    "eigenverbrauch_aktiviert": True,
                },
                {
                    "valid_from": "2026-06-01",
                    "valid_to": None,
                    "installierte_leistung_kw": 35.0,
                    "eigenverbrauch_aktiviert": True,
                },
            ]
        }
        kw, ev = _resolve_plant(self._cfg(kw=99.0), opts, date(2026, 3, 1))
        assert (kw, ev) == (25.0, True)
        kw, ev = _resolve_plant(self._cfg(kw=99.0), opts, date(2026, 7, 1))
        assert (kw, ev) == (35.0, True)

    def test_history_boundary_belongs_to_new_record(self):
        opts = {
            OPT_PLANT_HISTORY: [
                {
                    "valid_from": "2024-01-01",
                    "valid_to": "2026-06-01",
                    "installierte_leistung_kw": 25.0,
                    "eigenverbrauch_aktiviert": True,
                },
                {
                    "valid_from": "2026-06-01",
                    "valid_to": None,
                    "installierte_leistung_kw": 35.0,
                    "eigenverbrauch_aktiviert": False,
                },
            ]
        }
        kw, ev = _resolve_plant(self._cfg(), opts, date(2026, 6, 1))
        assert (kw, ev) == (35.0, False)


class TestResolveHknOptin:
    def _cfg(self, hkn=True):
        return {CONF_HKN_AKTIVIERT: hkn}

    def test_no_history_returns_current(self):
        assert _resolve_hkn_optin(self._cfg(hkn=True), {}, date(2026, 4, 1)) is True
        assert _resolve_hkn_optin(self._cfg(hkn=False), {}, date(2026, 4, 1)) is False

    def test_history_active_record_used(self):
        opts = {
            OPT_HKN_OPTIN_HISTORY: [
                {"valid_from": "2025-01-01", "valid_to": "2026-01-01", "opted_in": False},
                {"valid_from": "2026-01-01", "valid_to": None, "opted_in": True},
            ]
        }
        # Pre-2026 → opted_in=False
        assert _resolve_hkn_optin(self._cfg(), opts, date(2025, 6, 1)) is False
        # 2026+ → opted_in=True
        assert _resolve_hkn_optin(self._cfg(), opts, date(2026, 4, 1)) is True
