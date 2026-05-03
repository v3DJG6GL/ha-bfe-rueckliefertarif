"""v0.13.0 (Phase 3) — repair flow for tariff drift.

When ``data_coordinator._scan_history_for_drift`` detects that a stored
``OPT_CONFIG_HISTORY`` entry's ``user_inputs`` no longer fit the current
rate window's declarations, it surfaces the drift via
``async_create_issue`` with translation_key ``tariff_drift``. This module
exposes the fix flow that runs when the user clicks "Fix" in
Settings → Repairs.

The flow is single-step: it renders one selector per drifted key
(missing + stale), and on submit appends a new history entry at the
period boundary carrying the patched user_inputs while preserving
everything else from the original record (kW, EV, HKN, etc.). HA's
issue registry auto-resolves the issue when the flow completes.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

import voluptuous as vol
from homeassistant import data_entry_flow
from homeassistant.components.repairs import RepairsFlow
from homeassistant.core import HomeAssistant

from .config_flow import (
    _add_user_input_fields,
    _append_history_record,
    _hass_lang,
    _normalize_history,
)
from .const import CONF_USER_INPUTS, OPT_CONFIG_HISTORY
from .tariffs_db import find_active_rate_window

_LOGGER = logging.getLogger(__name__)


async def async_create_fix_flow(
    hass: HomeAssistant,
    issue_id: str,
    data: dict[str, Any] | None,
) -> RepairsFlow:
    """HA-required factory: returns a fix-flow handler per drift issue.

    The ``data`` dict is the one passed to ``async_create_issue`` in the
    drift scanner (carries ``entry_id``, ``entry_idx``, ``utility``,
    ``period_from``, ``period_to``, ``missing_keys``, ``stale_values``).
    """
    return TariffDriftRepairFlow(data or {})


class TariffDriftRepairFlow(RepairsFlow):
    """Single-step flow that resolves a single ``tariff_drift`` issue."""

    def __init__(self, descriptor: dict[str, Any]) -> None:
        super().__init__()
        self._desc = descriptor
        self._utility = descriptor.get("utility")
        self._period_from = descriptor.get("period_from")
        self._period_to = descriptor.get("period_to") or "open"
        self._entry_id = descriptor.get("entry_id")
        self._entry_idx = descriptor.get("entry_idx", 0)

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> data_entry_flow.FlowResult:
        if not self._utility or not self._period_from or not self._entry_id:
            return self.async_abort(reason="invalid_descriptor")

        try:
            rate = find_active_rate_window(
                self._utility, date.fromisoformat(self._period_from)
            )
        except (KeyError, ValueError, LookupError):
            rate = None
        if rate is None:
            # The rate window for this period vanished from the schema
            # since the issue was created. Nothing to fix; abort cleanly.
            return self.async_abort(reason="no_rate_window")

        all_decls = tuple(rate.get("user_inputs") or ())
        affected_keys = (
            set(self._desc.get("missing_keys") or [])
            | set(self._desc.get("stale_values") or [])
        )
        decls_to_ask = tuple(
            d for d in all_decls if d.get("key") in affected_keys
        )
        if not decls_to_ask:
            return self.async_abort(reason="nothing_to_fix")

        if user_input is not None:
            return self._save(user_input, decls_to_ask)

        lang = _hass_lang(self.hass)
        schema_dict: dict[Any, Any] = {}
        _add_user_input_fields(schema_dict, decls_to_ask, {}, lang)
        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema(schema_dict),
            description_placeholders={
                "utility": self._utility,
                "period_from": self._period_from,
                "period_to": self._period_to,
            },
        )

    def _save(
        self, user_input: dict[str, Any], decls_to_ask: tuple[dict, ...]
    ) -> data_entry_flow.FlowResult:
        entry = self.hass.config_entries.async_get_entry(self._entry_id)
        if entry is None:
            return self.async_abort(reason="entry_gone")

        history = list((entry.options or {}).get(OPT_CONFIG_HISTORY) or [])
        sorted_h = sorted(history, key=lambda r: r.get("valid_from") or "")
        if not (0 <= self._entry_idx < len(sorted_h)):
            return self.async_abort(reason="entry_idx_out_of_range")

        base = sorted_h[self._entry_idx]
        base_cfg = dict(base.get("config") or {})
        base_inputs = dict(base_cfg.get(CONF_USER_INPUTS) or {})

        # Patch the affected keys with the user's new picks; preserve
        # any other stored keys (their values may still be referenced
        # by future / other rate windows).
        new_inputs = dict(base_inputs)
        for decl in decls_to_ask:
            key = decl["key"]
            new_inputs[key] = user_input.get(key, decl.get("default"))

        new_cfg = dict(base_cfg)
        new_cfg[CONF_USER_INPUTS] = new_inputs
        new_record = {
            "valid_from": self._period_from,
            "valid_to": None,
            "config": new_cfg,
        }

        # Drop any prior record sharing the new valid_from to avoid
        # duplicates after normalize.
        history = [
            r for r in history if r.get("valid_from") != self._period_from
        ]
        history = _append_history_record(history, new_record, dict(entry.data))
        normalized = _normalize_history(history)
        new_options = {
            **dict(entry.options or {}),
            OPT_CONFIG_HISTORY: normalized,
        }
        self.hass.config_entries.async_update_entry(entry, options=new_options)
        return self.async_create_entry(title="", data={})
