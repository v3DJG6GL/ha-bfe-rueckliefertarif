"""Config flow for BFE Rückliefertarif (v0.9.0+).

Initial 3-step flow (unchanged):
1. ``user`` — menu of utilities (data/tariffs.json driven).
2. ``tariff`` — kW, Eigenverbrauch, HKN opt-in, billing rhythm.
3. ``entities`` — 3 entity-wiring fields.

OptionsFlow menu (v0.9.0 lean redesign):
- Apply config change (wizard — utility/HKN/kW/billing change effective from a date)
- Manage configuration history (full CRUD on records)
- Recompute full history (rewrites all LTS for published quarters + current-quarter estimate)
- Refresh prices from BFE
- Re-wire HA entities

After v0.9.0, ``OPT_CONFIG_HISTORY`` is the sole source of truth for versioned
tariff fields. ``entry.data`` only carries entity-wiring (no sync needed).
"""

from __future__ import annotations

import logging
from datetime import date
from typing import TYPE_CHECKING, Any

import voluptuous as vol
from homeassistant import config_entries
from homeassistant.helpers import selector

_LOGGER = logging.getLogger(__name__)

from .const import (
    ABRECHNUNGS_RHYTHMUS_MONAT,
    ABRECHNUNGS_RHYTHMUS_QUARTAL,
    CONF_ABRECHNUNGS_RHYTHMUS,
    CONF_EIGENVERBRAUCH_AKTIVIERT,
    CONF_ENERGIEVERSORGER,
    CONF_HKN_AKTIVIERT,
    CONF_INSTALLIERTE_LEISTUNG_KW,
    CONF_NAMENSPRAEFIX,
    CONF_PLANT_NAME,
    CONF_RUECKLIEFERVERGUETUNG_CHF,
    CONF_STROMNETZEINSPEISUNG_KWH,
    CONF_VALID_FROM,
    DOMAIN,
    CONFIG_HISTORY_FIELDS,
    OPT_CONFIG_HISTORY,
)
from .tariffs_db import find_active, list_utility_keys, load_tariffs


async def _async_warm_cache(hass) -> None:
    """Pre-load tariffs.json via executor so the in-event-loop callers below
    hit the lru_cache instead of triggering HA's blocking-I/O detector.

    v0.9.1: also lazy-init the ``TariffsDataCoordinator`` if it's missing
    from ``hass.data[DOMAIN]``. Without this, the first-time config flow
    (and any post-uninstall re-install) renders only the bundled utility
    list — the remote companion-repo fetch otherwise only fires inside
    ``async_setup_entry``, which runs *after* an entry exists. Reusing
    the same ``_tariffs_data`` slot ``async_setup_entry`` would have
    populated keeps init idempotent.

    Cheap to call repeatedly — after the first hit the cache is populated
    and the executor job is a no-op dict return.
    """
    hass.data.setdefault(DOMAIN, {})
    if "_tariffs_data" not in hass.data[DOMAIN]:
        from .data_coordinator import TariffsDataCoordinator

        tdc = TariffsDataCoordinator(hass)
        await tdc.async_load()
        hass.data[DOMAIN]["_tariffs_data"] = tdc
    await hass.async_add_executor_job(load_tariffs)

if TYPE_CHECKING:
    from homeassistant.data_entry_flow import FlowResult


# Locale-aware data-source URLs surfaced via description_placeholders.
# Hassfest forbids URLs in translation strings — must be runtime-injected.
_AGENCY_URLS: dict[str, str] = {
    "de": "https://www.bfe.admin.ch/bfe/de/home/foerderung/erneuerbare-energien/einspeiseverguetung.html",
    "en": "https://www.bfe.admin.ch/bfe/en/home/promotion/renewable-energy/feed-in-remuneration-at-cost.html",
}
_OPENDATA_URLS: dict[str, str] = {
    "de": "https://opendata.swiss/de/dataset/referenz-marktpreise-gemass-art-15-enfv",
    "en": "https://opendata.swiss/en/dataset/referenz-marktpreise-gemass-art-15-enfv",
    "fr": "https://opendata.swiss/fr/dataset/referenz-marktpreise-gemass-art-15-enfv",
}
# Fedlex (Swiss federal law portal) — deep-linked to the relevant article:
# - EnV SR 730.01 (ELI 2017/763), Art. 12 Abs. 1bis: Mindestvergütung floors.
# - StromVV SR 734.71 (ELI 2008/226), Art. 4 Abs. 3 Bst. e: cap mechanism.
# Both in force 1.1.2026 via AS 2025 138 / AS 2025 139.
_FEDLEX_ENV_URLS: dict[str, str] = {
    "de": "https://www.fedlex.admin.ch/eli/cc/2017/763/de#art_12",
    "en": "https://www.fedlex.admin.ch/eli/cc/2017/763/de#art_12",
    "fr": "https://www.fedlex.admin.ch/eli/cc/2017/763/fr#art_12",
}
_FEDLEX_STROMVV_URLS: dict[str, str] = {
    "de": "https://www.fedlex.admin.ch/eli/cc/2008/226/de#art_4",
    "en": "https://www.fedlex.admin.ch/eli/cc/2008/226/de#art_4",
    "fr": "https://www.fedlex.admin.ch/eli/cc/2008/226/fr#art_4",
}


def _source_links(hass) -> dict[str, str]:
    """Return locale-correct data-source URLs for description_placeholders."""
    lang = (getattr(hass.config, "language", None) or "en").split("-")[0].lower()
    return {
        "agency_url": _AGENCY_URLS.get(lang, _AGENCY_URLS["en"]),
        "opendata_url": _OPENDATA_URLS.get(lang, _OPENDATA_URLS["en"]),
        "env_url": _FEDLEX_ENV_URLS.get(lang, _FEDLEX_ENV_URLS["en"]),
        "stromvv_url": _FEDLEX_STROMVV_URLS.get(lang, _FEDLEX_STROMVV_URLS["en"]),
    }


def _utility_display_name(key: str, db: dict | None = None) -> str:
    """Human-readable label from tariffs.json (`name_de`, falling back to key)."""
    db = db if db is not None else load_tariffs()
    u = db["utilities"].get(key)
    if u is None:
        return key
    return u.get("name_de") or u.get("name_fr") or key


def _tariff_schema(
    defaults: dict[str, Any] | None = None,
    *,
    hkn_structure: str | None = None,
) -> vol.Schema:
    """Build the tariff-step schema with optional pre-filled defaults.

    Personal inputs: installed kW, Eigenverbrauch yes/no, HKN opt-in
    yes/no. Billing rhythm is no longer collected (#9 — derived from
    utility's ``settlement_period``).

    v0.9.8 #1 — ``hkn_structure`` (when known from the active utility)
    gates the HKN opt-in toggle:
    - ``additive_optin`` / ``None`` (legacy) → toggle rendered.
    - ``bundled`` / ``none`` → toggle omitted; the form's
      description_placeholders explain why and the save site forces a
      math-correct value.
    """
    from datetime import date

    d = defaults or {}
    schema_dict: dict[Any, Any] = {
        vol.Required(
            CONF_VALID_FROM,
            default=d.get(CONF_VALID_FROM, date.today().isoformat()),
        ): selector.DateSelector(),
        vol.Required(
            CONF_INSTALLIERTE_LEISTUNG_KW,
            default=d.get(CONF_INSTALLIERTE_LEISTUNG_KW, 0.0),
        ): selector.NumberSelector(
            selector.NumberSelectorConfig(
                min=0,
                max=10000,
                step=0.1,
                mode=selector.NumberSelectorMode.BOX,
                unit_of_measurement="kW",
            )
        ),
        vol.Required(
            CONF_EIGENVERBRAUCH_AKTIVIERT,
            default=d.get(CONF_EIGENVERBRAUCH_AKTIVIERT, True),
        ): selector.BooleanSelector(),
    }
    if hkn_structure not in ("bundled", "none"):
        schema_dict[
            vol.Required(
                CONF_HKN_AKTIVIERT,
                default=d.get(CONF_HKN_AKTIVIERT, False),
            )
        ] = selector.BooleanSelector()
    return vol.Schema(schema_dict)


_HKN_GATE_NOTES: dict[str, dict[str, str]] = {
    "bundled": {
        "de": (
            "**HKN:** Im Tarif des Versorgers bereits enthalten — kein "
            "separates Opt-in nötig. Die Auswahl ist ausgeblendet, weil "
            "Aktivieren den HKN doppelt zählen würde."
        ),
        "en": (
            "**HKN:** Included in the utility's base rate — no separate "
            "opt-in needed. The toggle is hidden because activating it "
            "would double-count."
        ),
        "fr": (
            "**GO :** Déjà incluse dans le tarif du fournisseur — aucun "
            "opt-in séparé. Le bouton est masqué car l'activer ferait "
            "compter la GO en double."
        ),
    },
    "none": {
        "de": (
            "**HKN:** Dieser Versorger zahlt keine separate HKN-Vergütung. "
            "Anlagenbetreiber, die ihre HKN vermarkten möchten, tun das "
            "üblicherweise über Pronovo oder einen Dritten. Die Auswahl "
            "ist ausgeblendet, weil die versorgereigene Option entfällt."
        ),
        "en": (
            "**HKN:** This utility does not pay separately for HKN. "
            "Operators wishing to monetise HKN typically market it via "
            "Pronovo or a third party. The toggle is hidden because "
            "the utility-side option does not apply."
        ),
        "fr": (
            "**GO :** Ce fournisseur ne rémunère pas la GO séparément. "
            "Les exploitants qui souhaitent monétiser leurs GO passent "
            "généralement par Pronovo ou un tiers. Le bouton est masqué "
            "car l'option côté fournisseur ne s'applique pas."
        ),
    },
}


def _hkn_gate_note(hkn_structure: str | None, hass=None) -> str:
    """Localized note rendered when the HKN toggle is hidden because the
    active utility's ``hkn_structure`` is ``"bundled"`` or ``"none"``.
    Returns an empty string for ``additive_optin`` / ``None`` (legacy) so
    the description placeholder stays unobtrusive when the toggle is shown.
    """
    if hkn_structure not in ("bundled", "none"):
        return ""
    lang = "en"
    if hass is not None:
        lang = (getattr(hass.config, "language", None) or "en").split("-")[0].lower()
    return _HKN_GATE_NOTES[hkn_structure].get(lang) or _HKN_GATE_NOTES[hkn_structure]["en"]


def _validate_tariff(user_input: dict[str, Any]) -> dict[str, str]:
    """Per-field error keys for the tariff step. Empty dict = valid.

    v0.5: only need to validate that kW is positive (the federal degressive
    formula and most utility cap-rules need a non-zero kW). The 8-segment
    plant-category dropdown is gone, so there's no "kw_required_for_degressive"
    case anymore — kW is required for everyone.
    """
    errors: dict[str, str] = {}
    if float(user_input.get(CONF_INSTALLIERTE_LEISTUNG_KW, 0)) <= 0:
        errors[CONF_INSTALLIERTE_LEISTUNG_KW] = "kw_required"
    return errors


def _quarter_start_today() -> str:
    """ISO date of the first day of the current quarter (Zurich-local year/month)."""
    from datetime import date

    today = date.today()
    q_start_month = ((today.month - 1) // 3) * 3 + 1
    return date(today.year, q_start_month, 1).isoformat()


def _parse_valid_from(s: str) -> str:
    """Validate ``s`` as an ISO YYYY-MM-DD date string and return the canonical
    form. Raises ValueError on invalid input or empty string.

    All forms now use HA's DateSelector, which always emits ISO dates — the
    quarter shorthand (YYYYQN) was dropped in v0.9.8 along with the raw
    text inputs.
    """
    from datetime import date

    s = (s or "").strip()
    if not s:
        raise ValueError("empty valid_from")
    return date.fromisoformat(s).isoformat()


def _active_hkn_structure(utility_key: str, valid_from_iso: str) -> str | None:
    """Return the active rate window's first power-tier ``hkn_structure``,
    or ``None`` on lookup failure / legacy data without the field.

    v0.9.8 #1 — used to gate the ``hkn_aktiviert`` form toggle. The gate
    is per *rate window* (settlement_period level) but ``hkn_structure``
    lives on each ``power_tier``. We use the first tier as a safe heuristic:
    the dominant case is a single tier, and bundled/none utilities today
    have a uniform ``hkn_structure`` across tiers. If a future utility
    splits hkn_structure across tiers, the resolver still produces the
    correct math at compute-time; the UI gate would just be slightly
    inaccurate for the user's specific kW band (they'd see the toggle
    even though their tier is bundled, or vice versa).
    """
    try:
        db = load_tariffs()
        utility = db["utilities"].get(utility_key)
        if utility is None:
            return None
        rate = find_active(utility["rates"], date.fromisoformat(valid_from_iso))
        if rate is None or not rate.get("power_tiers"):
            return None
        return rate["power_tiers"][0].get("hkn_structure")
    except (KeyError, ValueError, LookupError):
        return None


def _force_hkn_for_save(hkn_structure: str | None, user_hkn: bool) -> bool:
    """Pick the persisted ``hkn_aktiviert`` value given the gate.

    - ``additive_optin`` / ``None`` (legacy) → preserve the user's choice.
    - ``bundled`` → force ``False``: HKN is already inside the utility's
      fixed/base rate; persisting ``True`` would let the resolver double-add.
    - ``none`` → force ``False``: utility doesn't pay HKN at all.

    The form hides the toggle for bundled/none and renders an inline note,
    so the user isn't surprised by the override.
    """
    if hkn_structure in ("bundled", "none"):
        return False
    return bool(user_hkn)


def _derive_billing(utility_key: str, valid_from_iso: str) -> str:
    """Return the user-side billing constant matching the utility's
    ``settlement_period`` at ``valid_from_iso``.

    v0.9.8 — the user no longer chooses billing rhythm; it's derived from
    the utility's published settlement_period. Raises ``NotImplementedError``
    if the active rate window declares ``"stunde"`` (Vernehmlassung 2025/59
    hourly Day-Ahead, not yet implemented). Raises ``LookupError`` if no
    active rate window exists for the date, ``KeyError`` for an unknown
    utility.
    """
    db = load_tariffs()
    utility = db["utilities"].get(utility_key)
    if utility is None:
        raise KeyError(f"unknown utility {utility_key!r}")
    rate = find_active(utility["rates"], date.fromisoformat(valid_from_iso))
    if rate is None:
        raise LookupError(
            f"no active rate for {utility_key!r} on {valid_from_iso}"
        )
    sp = rate["settlement_period"]
    if sp == "quartal":
        return ABRECHNUNGS_RHYTHMUS_QUARTAL
    if sp == "monat":
        return ABRECHNUNGS_RHYTHMUS_MONAT
    if sp == "stunde":
        raise NotImplementedError(
            f"utility {utility_key!r} uses hourly Day-Ahead settlement "
            f"(Vernehmlassung 2025/59); not yet supported."
        )
    raise ValueError(f"unknown settlement_period {sp!r}")


def _make_sentinel_record(entry_data: dict) -> dict:
    """Synthesize the open-ended 1970 sentinel record from entry.data."""
    return {
        "valid_from": "1970-01-01",
        "valid_to": None,
        "config": {k: entry_data.get(k) for k in CONFIG_HISTORY_FIELDS},
    }


def _append_history_record(
    history: list[dict], new_record: dict, entry_data: dict
) -> list[dict]:
    """Append a new record to history. If history is empty, prepend the 1970
    sentinel first so per-quarter resolution has a fallback for any quarter
    predating ``new_record["valid_from"]``."""
    out = list(history)
    if not out:
        out.append(_make_sentinel_record(entry_data))
    out.append(new_record)
    return out


def _normalize_history(records: list[dict]) -> list[dict]:
    """Sort records by valid_from, derive each valid_to from the next record's
    valid_from, and set the last record's valid_to = None. Also de-duplicates
    records sharing a valid_from (last write wins)."""
    by_from: dict[str, dict] = {}
    for r in records:
        by_from[r["valid_from"]] = {
            "valid_from": r["valid_from"],
            "valid_to": None,
            "config": dict(r["config"]),
        }
    sorted_recs = sorted(by_from.values(), key=lambda r: r["valid_from"])
    for i, rec in enumerate(sorted_recs):
        rec["valid_to"] = sorted_recs[i + 1]["valid_from"] if i + 1 < len(sorted_recs) else None
    return sorted_recs


def _format_config_summary(config: dict) -> str:
    """Compact one-line summary used for menu labels."""
    utility = config.get(CONF_ENERGIEVERSORGER) or "—"
    kw = config.get(CONF_INSTALLIERTE_LEISTUNG_KW)
    kw_s = f"{float(kw):.1f} kW" if kw is not None else "—"
    ev = "EV" if config.get(CONF_EIGENVERBRAUCH_AKTIVIERT) else "no-EV"
    hkn = "HKN" if config.get(CONF_HKN_AKTIVIERT) else "no-HKN"
    billing = config.get(CONF_ABRECHNUNGS_RHYTHMUS) or "—"
    return f"{utility} · {kw_s} · {ev} · {hkn} · {billing}"


class BfeRuecklieferTarifFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Menu-first 3-step config flow."""

    VERSION = 4

    def __init__(self) -> None:
        self._data: dict[str, Any] = {}

    @staticmethod
    def async_get_options_flow(
        config_entry: config_entries.ConfigEntry,
    ) -> "BfeRuecklieferTarifOptionsFlow":
        return BfeRuecklieferTarifOptionsFlow()

    # ----- Step 1: utility menu --------------------------------------------------

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> "FlowResult":
        await _async_warm_cache(self.hass)
        keys = list_utility_keys()
        return self.async_show_menu(
            step_id="user",
            menu_options={
                f"preset_{k}": _utility_display_name(k) for k in keys
            },
            description_placeholders=_source_links(self.hass),
        )

    async def _apply_preset(self, key: str) -> "FlowResult":
        self._data[CONF_ENERGIEVERSORGER] = key
        return await self.async_step_tariff()

    def __getattr__(self, name: str):
        # Dynamic dispatch for menu options: HA looks up
        # ``async_step_preset_<key>`` from menu_options. Accept any key
        # present in tariffs.json; everything else raises AttributeError so
        # genuine typos still surface.
        if name.startswith("async_step_preset_"):
            key = name.removeprefix("async_step_preset_")
            try:
                valid = set(list_utility_keys())
            except Exception:  # noqa: BLE001
                valid = set()
            if key in valid:
                async def _step(user_input=None, _key=key):
                    return await self._apply_preset(_key)
                return _step
        raise AttributeError(name)

    # ----- Step 2: tariff configuration -----------------------------------------

    async def async_step_tariff(
        self, user_input: dict[str, Any] | None = None
    ) -> "FlowResult":
        errors: dict[str, str] = {}
        utility_key = self._data[CONF_ENERGIEVERSORGER]

        if user_input is not None:
            errors = _validate_tariff(user_input)
            if not errors:
                hkn_structure = _active_hkn_structure(
                    utility_key, user_input[CONF_VALID_FROM]
                )
                user_input[CONF_HKN_AKTIVIERT] = _force_hkn_for_save(
                    hkn_structure, user_input.get(CONF_HKN_AKTIVIERT, False)
                )
                try:
                    user_input[CONF_ABRECHNUNGS_RHYTHMUS] = _derive_billing(
                        utility_key, user_input[CONF_VALID_FROM]
                    )
                except NotImplementedError:
                    errors["base"] = "settlement_period_unsupported"
                except (KeyError, LookupError):
                    errors["base"] = "no_active_rate"
                else:
                    self._data.update(user_input)
                    return await self.async_step_entities()

        defaults = user_input if user_input is not None else self._data
        # Gate the HKN toggle on the chosen utility's hkn_structure.
        gate_valid_from = (
            (user_input or defaults or {}).get(CONF_VALID_FROM)
            or date.today().isoformat()
        )
        hkn_structure = _active_hkn_structure(utility_key, gate_valid_from)
        return self.async_show_form(
            step_id="tariff",
            data_schema=_tariff_schema(defaults, hkn_structure=hkn_structure),
            errors=errors,
            description_placeholders={
                "utility_name": _utility_display_name(utility_key),
                "hkn_gate_note": _hkn_gate_note(hkn_structure, self.hass),
                **_source_links(self.hass),
            },
        )

    # ----- Step 3: HA entities --------------------------------------------------

    async def async_step_entities(
        self, user_input: dict[str, Any] | None = None
    ) -> "FlowResult":
        if user_input is not None:
            from homeassistant.util import slugify

            plant_name = (user_input.get(CONF_PLANT_NAME) or "").strip()
            # If the user leaves namenspraefix empty, derive it from the
            # plant name (stable identifier, decoupled from the utility).
            prefix = (user_input.get(CONF_NAMENSPRAEFIX) or "").strip()
            if not prefix:
                prefix = f"{slugify(plant_name)}_rueckliefertarif"
            user_input[CONF_NAMENSPRAEFIX] = prefix
            user_input[CONF_PLANT_NAME] = plant_name
            self._data.update(user_input)
            return self.async_create_entry(
                title=plant_name,
                data=self._data,
            )

        schema = vol.Schema(
            {
                vol.Required(CONF_STROMNETZEINSPEISUNG_KWH): selector.EntitySelector(
                    selector.EntitySelectorConfig(
                        domain="sensor", device_class="energy"
                    )
                ),
                vol.Required(CONF_RUECKLIEFERVERGUETUNG_CHF): selector.EntitySelector(
                    selector.EntitySelectorConfig(domain="sensor")
                ),
                vol.Required(CONF_PLANT_NAME): str,
                vol.Optional(CONF_NAMENSPRAEFIX, default=""): str,
            }
        )
        return self.async_show_form(step_id="entities", data_schema=schema)


class BfeRuecklieferTarifOptionsFlow(config_entries.OptionsFlowWithReload):
    """Options flow: menu with tariff edit, specific-quarter re-import, and entity wiring.

    HA 2024.12+ exposes ``config_entry`` as a read-only property on
    OptionsFlow, sourced from ``self.handler`` (the entry_id). Don't override
    ``__init__`` or assign ``self.config_entry`` — that raises AttributeError
    on current HA.

    Inherits from ``OptionsFlowWithReload`` (HA 2024.11+) so HA reloads the
    entry automatically *after* options are committed — eliminates the
    earlier wipe race where a manual ``async_create_task(async_reload(...))``
    plus ``async_create_entry(data={})`` overwrote ``entry.options`` with
    ``{}`` before the reload picked up our writes.
    """

    # ----- Menu --------------------------------------------------------------

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> "FlowResult":
        return self.async_show_menu(
            step_id="init",
            menu_options=[
                "apply_change",
                "manage_history",
                "recompute_history",
                "refresh_data",
                "entities",
            ],
        )

    # ----- Sub-step: apply config change (wizard) ----------------------------

    async def async_step_apply_change(
        self, user_input: dict[str, Any] | None = None
    ) -> "FlowResult":
        """Wizard for applying any config change effective from a given date.

        Single-step form covering utility / kW / EV / HKN all at once.
        Billing rhythm is no longer user-input as of v0.9.8 — it's derived
        from the chosen utility's published ``settlement_period`` at the
        ``valid_from`` date. Defaults inherited from the current open record.
        Effective date defaults to today's quarter-start. Submit appends a
        new record to ``OPT_CONFIG_HISTORY``; OptionsFlowWithReload reloads
        the entry.
        """
        await _async_warm_cache(self.hass)
        history = list(self.config_entry.options.get(OPT_CONFIG_HISTORY) or [])
        open_rec = next((r for r in history if r.get("valid_to") is None), None)
        open_cfg = (open_rec or {}).get("config") or {}

        errors: dict[str, str] = {}
        if user_input is not None:
            try:
                effective_from = _parse_valid_from(user_input.get("valid_from", ""))
            except ValueError:
                errors["valid_from"] = "invalid_valid_from"

            tariff_errs = _validate_tariff(user_input)
            errors.update(tariff_errs)

            derived_billing: str | None = None
            if not errors:
                try:
                    derived_billing = _derive_billing(
                        user_input[CONF_ENERGIEVERSORGER], effective_from
                    )
                except NotImplementedError:
                    errors["base"] = "settlement_period_unsupported"
                except (KeyError, LookupError):
                    errors["base"] = "no_active_rate"

            if not errors:
                hkn_structure = _active_hkn_structure(
                    user_input[CONF_ENERGIEVERSORGER], effective_from
                )
                new_config = {
                    CONF_ENERGIEVERSORGER: user_input[CONF_ENERGIEVERSORGER],
                    CONF_INSTALLIERTE_LEISTUNG_KW: float(
                        user_input[CONF_INSTALLIERTE_LEISTUNG_KW]
                    ),
                    CONF_EIGENVERBRAUCH_AKTIVIERT: bool(
                        user_input[CONF_EIGENVERBRAUCH_AKTIVIERT]
                    ),
                    CONF_HKN_AKTIVIERT: _force_hkn_for_save(
                        hkn_structure, user_input.get(CONF_HKN_AKTIVIERT, False)
                    ),
                    CONF_ABRECHNUNGS_RHYTHMUS: derived_billing,
                }
                # No-op detection — if effective_from matches the open record's
                # valid_from AND the config is identical, skip the write.
                if (
                    open_rec
                    and open_rec["valid_from"] == effective_from
                    and open_rec["config"] == new_config
                ):
                    return self.async_create_entry(
                        title="", data=dict(self.config_entry.options or {})
                    )

                new_record = {
                    "valid_from": effective_from,
                    "valid_to": None,
                    "config": new_config,
                }
                # Strip any existing record at the same date so this one wins.
                history = [r for r in history if r.get("valid_from") != effective_from]
                history = _append_history_record(
                    history, new_record, dict(self.config_entry.data)
                )
                normalized = _normalize_history(history)
                new_options = {
                    **dict(self.config_entry.options or {}),
                    OPT_CONFIG_HISTORY: normalized,
                }
                return self.async_create_entry(title="", data=new_options)

        defaults_cfg = (
            user_input
            if user_input is not None
            else (open_cfg or {k: self.config_entry.data.get(k) for k in CONFIG_HISTORY_FIELDS})
        )
        default_valid_from = (
            user_input.get("valid_from", _quarter_start_today())
            if user_input is not None
            else _quarter_start_today()
        )

        utility_keys = list_utility_keys()
        gate_utility = defaults_cfg.get(CONF_ENERGIEVERSORGER) or utility_keys[0]
        gate_valid_from = default_valid_from or _quarter_start_today()
        hkn_structure = _active_hkn_structure(gate_utility, gate_valid_from)

        schema_dict: dict[Any, Any] = {
            vol.Required(
                "valid_from", default=default_valid_from
            ): selector.DateSelector(),
            vol.Required(
                CONF_ENERGIEVERSORGER,
                default=defaults_cfg.get(CONF_ENERGIEVERSORGER) or utility_keys[0],
            ): selector.SelectSelector(
                selector.SelectSelectorConfig(
                    options=[
                        selector.SelectOptionDict(
                            value=k, label=_utility_display_name(k)
                        )
                        for k in utility_keys
                    ],
                    mode=selector.SelectSelectorMode.DROPDOWN,
                )
            ),
            vol.Required(
                CONF_INSTALLIERTE_LEISTUNG_KW,
                default=defaults_cfg.get(CONF_INSTALLIERTE_LEISTUNG_KW, 0.0),
            ): selector.NumberSelector(
                selector.NumberSelectorConfig(
                    min=0, max=10000, step=0.1,
                    mode=selector.NumberSelectorMode.BOX,
                    unit_of_measurement="kW",
                )
            ),
            vol.Required(
                CONF_EIGENVERBRAUCH_AKTIVIERT,
                default=bool(defaults_cfg.get(CONF_EIGENVERBRAUCH_AKTIVIERT, True)),
            ): selector.BooleanSelector(),
        }
        if hkn_structure not in ("bundled", "none"):
            schema_dict[
                vol.Required(
                    CONF_HKN_AKTIVIERT,
                    default=bool(defaults_cfg.get(CONF_HKN_AKTIVIERT, False)),
                )
            ] = selector.BooleanSelector()
        schema = vol.Schema(schema_dict)
        return self.async_show_form(
            step_id="apply_change",
            data_schema=schema,
            errors=errors,
            description_placeholders={
                "current_summary": _format_config_summary(open_cfg) if open_cfg else "—",
                "hkn_gate_note": _hkn_gate_note(hkn_structure, self.hass),
                **_source_links(self.hass),
            },
        )

    # ----- Sub-step: manage configuration history ----------------------------

    async def async_step_manage_history(
        self, user_input: dict[str, Any] | None = None
    ) -> "FlowResult":
        history = list(self.config_entry.options.get(OPT_CONFIG_HISTORY) or [])
        _LOGGER.debug("manage_history: %d record(s) read from options", len(history))
        today_iso = date.today().isoformat()
        menu: dict[str, str] = {}
        for i, rec in enumerate(history):
            valid_to = rec.get("valid_to")
            if valid_to:
                end_label = valid_to
            elif rec["valid_from"] <= today_iso:
                end_label = "now"
            else:
                end_label = "..."
            label = (
                f"{rec['valid_from']} → {end_label}: "
                f"{_format_config_summary(rec['config'])}"
            )
            menu[f"edit_row_{i}"] = label
        menu["add_new_row"] = "+ Add new transition"
        menu["done_history"] = "Done"
        return self.async_show_menu(step_id="manage_history", menu_options=menu)

    async def async_step_done_history(
        self, user_input: dict[str, Any] | None = None
    ) -> "FlowResult":
        # Pass current options through so HA's commit doesn't wipe them.
        # OptionsFlowWithReload's auto-reload silently skips when _edit_row
        # has pre-written options inline (its diff check sees no change),
        # so trigger the reload explicitly. Required so coordinator and
        # hass.data caches see the fresh history (also reflected by the
        # services-level live read of _first_entry_data, but the
        # coordinator's BFE breakdown sensor needs a rebuild to refresh).
        # Safe (no wipe race): the flow result carries the current options
        # dict, not data={}.
        self.hass.async_create_task(
            self.hass.config_entries.async_reload(self.config_entry.entry_id)
        )
        return self.async_create_entry(
            title="", data=dict(self.config_entry.options or {})
        )

    async def async_step_add_new_row(
        self, user_input: dict[str, Any] | None = None
    ) -> "FlowResult":
        return await self._edit_row(idx=None, user_input=user_input)

    async def async_step_edit_row(
        self, user_input: dict[str, Any] | None = None
    ) -> "FlowResult":
        # Form-resubmit lands here. ``self._editing_idx`` was set by
        # ``__getattr__`` when the menu item was first clicked.
        return await self._edit_row(getattr(self, "_editing_idx", None), user_input)

    def __getattr__(self, name: str):
        # Dynamic dispatch for "edit_row_<N>" menu options. We stash the row
        # index on self and delegate to async_step_edit_row, which uses a
        # static step_id so translations resolve.
        if name.startswith("async_step_edit_row_"):
            try:
                idx = int(name.removeprefix("async_step_edit_row_"))
            except ValueError:
                raise AttributeError(name) from None
            async def _step(user_input=None, _idx=idx):
                self._editing_idx = _idx
                return await self.async_step_edit_row(user_input)
            return _step
        raise AttributeError(name)

    async def _edit_row(
        self, idx: int | None, user_input: dict[str, Any] | None = None
    ) -> "FlowResult":
        """Show the edit form for one history record. ``idx=None`` → add new."""
        history = list(self.config_entry.options.get(OPT_CONFIG_HISTORY) or [])
        _LOGGER.debug(
            "_edit_row entry: idx=%s, %d record(s) currently in OPT_CONFIG_HISTORY",
            idx, len(history),
        )
        is_edit = idx is not None and 0 <= idx < len(history)
        existing = history[idx] if is_edit else None

        errors: dict[str, str] = {}
        if user_input is not None:
            # Delete branch (only when editing). Refuse to delete the last
            # record so the sentinel is always present — without it the
            # per-quarter resolver loses its fallback for past dates.
            if is_edit and bool(user_input.get("delete")):
                if len(history) <= 1:
                    errors["base"] = "cannot_delete_last_record"
                else:
                    history.pop(idx)
                    normalized = _normalize_history(history)
                    new_options = {
                        **dict(self.config_entry.options or {}),
                        OPT_CONFIG_HISTORY: normalized,
                    }
                    self.hass.config_entries.async_update_entry(
                        self.config_entry, options=new_options
                    )
                    return await self.async_step_manage_history()

            # Save / overwrite branch.
            try:
                valid_from = _parse_valid_from(user_input.get("valid_from", ""))
            except ValueError:
                errors["valid_from"] = "invalid_valid_from"

            derived_billing: str | None = None
            if not errors:
                try:
                    derived_billing = _derive_billing(
                        user_input[CONF_ENERGIEVERSORGER], valid_from
                    )
                except NotImplementedError:
                    errors["base"] = "settlement_period_unsupported"
                except (KeyError, LookupError):
                    errors["base"] = "no_active_rate"

            if not errors:
                hkn_structure = _active_hkn_structure(
                    user_input[CONF_ENERGIEVERSORGER], valid_from
                )
                new_config = {
                    CONF_ENERGIEVERSORGER: user_input[CONF_ENERGIEVERSORGER],
                    CONF_INSTALLIERTE_LEISTUNG_KW: float(
                        user_input[CONF_INSTALLIERTE_LEISTUNG_KW]
                    ),
                    CONF_EIGENVERBRAUCH_AKTIVIERT: bool(
                        user_input[CONF_EIGENVERBRAUCH_AKTIVIERT]
                    ),
                    CONF_HKN_AKTIVIERT: _force_hkn_for_save(
                        hkn_structure, user_input.get(CONF_HKN_AKTIVIERT, False)
                    ),
                    CONF_ABRECHNUNGS_RHYTHMUS: derived_billing,
                }
                # Replace at idx (edit) or append (add). The normalize step
                # de-duplicates by valid_from, so editing an existing row's
                # valid_from to clash with another row will collapse them.
                if is_edit:
                    history[idx] = {
                        "valid_from": valid_from,
                        "valid_to": None,
                        "config": new_config,
                    }
                else:
                    history = _append_history_record(
                        history,
                        {
                            "valid_from": valid_from,
                            "valid_to": None,
                            "config": new_config,
                        },
                        dict(self.config_entry.data),
                    )
                normalized = _normalize_history(history)
                _LOGGER.debug(
                    "_edit_row save: writing %d record(s) to OPT_CONFIG_HISTORY",
                    len(normalized),
                )
                new_options = {
                    **dict(self.config_entry.options or {}),
                    OPT_CONFIG_HISTORY: normalized,
                }
                self.hass.config_entries.async_update_entry(
                    self.config_entry, options=new_options
                )
                return await self.async_step_manage_history()

        # Build form defaults: prior submission > existing record > open record.
        if user_input is not None:
            defaults_cfg = {k: user_input.get(k) for k in CONFIG_HISTORY_FIELDS}
            default_valid_from = user_input.get("valid_from", "")
        elif existing is not None:
            defaults_cfg = dict(existing["config"])
            default_valid_from = existing["valid_from"]
        else:
            # Add-new mode: prefill from the open record (most recent state).
            open_rec = next(
                (r for r in history if r.get("valid_to") is None), None
            )
            defaults_cfg = (
                dict(open_rec["config"]) if open_rec
                else {k: self.config_entry.data.get(k) for k in CONFIG_HISTORY_FIELDS}
            )
            default_valid_from = _quarter_start_today()

        utility_keys = list_utility_keys()
        gate_utility = defaults_cfg.get(CONF_ENERGIEVERSORGER) or utility_keys[0]
        gate_valid_from = default_valid_from or _quarter_start_today()
        hkn_structure = _active_hkn_structure(gate_utility, gate_valid_from)

        schema_dict: dict[Any, Any] = {
            vol.Required(
                "valid_from", default=default_valid_from
            ): selector.DateSelector(),
            vol.Required(
                CONF_ENERGIEVERSORGER,
                default=defaults_cfg.get(CONF_ENERGIEVERSORGER) or utility_keys[0],
            ): selector.SelectSelector(
                selector.SelectSelectorConfig(
                    options=[
                        selector.SelectOptionDict(
                            value=k, label=_utility_display_name(k)
                        )
                        for k in utility_keys
                    ],
                    mode=selector.SelectSelectorMode.DROPDOWN,
                )
            ),
            vol.Required(
                CONF_INSTALLIERTE_LEISTUNG_KW,
                default=defaults_cfg.get(CONF_INSTALLIERTE_LEISTUNG_KW, 0.0),
            ): selector.NumberSelector(
                selector.NumberSelectorConfig(
                    min=0, max=10000, step=0.1,
                    mode=selector.NumberSelectorMode.BOX,
                    unit_of_measurement="kW",
                )
            ),
            vol.Required(
                CONF_EIGENVERBRAUCH_AKTIVIERT,
                default=bool(defaults_cfg.get(CONF_EIGENVERBRAUCH_AKTIVIERT, True)),
            ): selector.BooleanSelector(),
        }
        if hkn_structure not in ("bundled", "none"):
            schema_dict[
                vol.Required(
                    CONF_HKN_AKTIVIERT,
                    default=bool(defaults_cfg.get(CONF_HKN_AKTIVIERT, False)),
                )
            ] = selector.BooleanSelector()
        if is_edit:
            schema_dict[vol.Optional("delete", default=False)] = selector.BooleanSelector()

        # Use static step_ids so translations resolve. For edit, we still need
        # a static id "edit_row" — the row index is held in self._editing_idx.
        step_id = "edit_row" if is_edit else "add_new_row"
        return self.async_show_form(
            step_id=step_id,
            data_schema=vol.Schema(schema_dict),
            errors=errors,
            description_placeholders={
                "hkn_gate_note": _hkn_gate_note(hkn_structure, self.hass),
            },
        )

    # ----- Sub-step: recompute full history ----------------------------------

    async def async_step_recompute_history(
        self, user_input: dict[str, Any] | None = None
    ) -> "FlowResult":
        """Confirm + run full-history recompute (published quarters + current Q estimate)."""
        from homeassistant.components.persistent_notification import async_create

        from .services import (
            _build_recompute_report,
            _notify_recompute,
            _reimport_all_history,
        )

        errors: dict[str, str] = {}
        if user_input is not None and user_input.get("confirm"):
            try:
                result = await _reimport_all_history(self.hass)
            except Exception as exc:  # noqa: BLE001
                _LOGGER.exception("Recompute full history failed")
                async_create(
                    self.hass,
                    f"Recompute failed: {exc}",
                    title="BFE Rückliefertarif",
                    notification_id=f"{DOMAIN}_{self.config_entry.entry_id}_recompute_history",
                )
                errors["base"] = "reimport_failed"
            else:
                quarters_for_report = list(result["imported"]) + list(result["estimated"])
                before_active = result.get("before_active") or []
                history = (self.config_entry.options or {}).get(
                    OPT_CONFIG_HISTORY
                ) or []
                earliest = history[0]["valid_from"] if history else None
                if quarters_for_report:
                    report = _build_recompute_report(
                        self.hass,
                        quarters_for_report,
                        before_active_count=len(before_active),
                        before_active_earliest=earliest,
                    )
                    _notify_recompute(self.hass, self.config_entry.entry_id, report)
                else:
                    skipped = result.get("skipped") or []
                    failed = result.get("failed") or []
                    lines = ["0 quarters recomputed."]
                    if skipped:
                        lines.append(
                            f"{len(skipped)} skipped (not yet published by BFE)."
                        )
                    if before_active:
                        lines.append(
                            f"{len(before_active)} skipped (predate plant install "
                            f"{earliest})."
                        )
                    if failed:
                        lines.append(
                            f"{len(failed)} errors — see logs."
                        )
                    async_create(
                        self.hass,
                        "\n".join(lines),
                        title="BFE Rückliefertarif",
                        notification_id=f"{DOMAIN}_{self.config_entry.entry_id}_recompute_history",
                    )
                return self.async_create_entry(
                    title="", data=dict(self.config_entry.options or {})
                )

        schema = vol.Schema(
            {vol.Required("confirm", default=False): selector.BooleanSelector()}
        )
        return self.async_show_form(
            step_id="recompute_history",
            data_schema=schema,
            errors=errors,
        )

    # ----- Sub-step: refresh prices from BFE ---------------------------------

    async def async_step_refresh_data(
        self, user_input: dict[str, Any] | None = None
    ) -> "FlowResult":
        """v0.9.6: combined refresh — BFE prices + companion-repo tariffs.json.

        Replaces the v0.9.0 ``refresh_prices`` step which only polled BFE.
        Surfaces both fetch results in a single notification.
        """
        from homeassistant.components.persistent_notification import async_create

        from .services import _refresh_upstream_data

        errors: dict[str, str] = {}
        if user_input is not None and user_input.get("confirm"):
            try:
                result = await _refresh_upstream_data(self.hass)
            except Exception as exc:  # noqa: BLE001
                _LOGGER.exception("Refresh data failed")
                async_create(
                    self.hass,
                    f"Refresh failed: {exc}",
                    title="BFE Rückliefertarif",
                    notification_id=f"{DOMAIN}_{self.config_entry.entry_id}_refresh",
                )
                errors["base"] = "reimport_failed"
            else:
                # Line 1: BFE poll status.
                avail = result["available"]
                new = result["newly_imported"]
                bfe_line = f"BFE poll OK — {len(avail)} quarter(s) available"
                if avail:
                    bfe_line += f" (latest: {max(avail)})"
                if new:
                    bfe_line += (
                        f"; newly imported: {', '.join(str(q) for q in new)}"
                    )
                else:
                    bfe_line += "; no new quarters since last import."
                # Line 2: tariff data refresh status.
                if result.get("tariffs_refreshed"):
                    version = result.get("tariffs_version") or "unknown"
                    tariffs_line = (
                        f"Tariff data refreshed (v{version} from companion repo)."
                    )
                elif result.get("tariffs_error"):
                    tariffs_line = (
                        f"Tariff data refresh failed: {result['tariffs_error']} "
                        "— using cached tariffs."
                    )
                else:
                    tariffs_line = (
                        "Tariff data refresh skipped (coordinator not ready)."
                    )
                async_create(
                    self.hass,
                    f"{bfe_line}\n{tariffs_line}",
                    title="BFE Rückliefertarif",
                    notification_id=f"{DOMAIN}_{self.config_entry.entry_id}_refresh",
                )
                return self.async_create_entry(
                    title="", data=dict(self.config_entry.options or {})
                )

        schema = vol.Schema(
            {vol.Required("confirm", default=False): selector.BooleanSelector()}
        )
        return self.async_show_form(
            step_id="refresh_data",
            data_schema=schema,
            errors=errors,
        )

    # ----- Sub-step: re-wire HA entities -------------------------------------

    async def async_step_entities(
        self, user_input: dict[str, Any] | None = None
    ) -> "FlowResult":
        if user_input is not None:
            # Plant name doubles as the entry title — extract before merging.
            plant_name = (user_input.get(CONF_PLANT_NAME) or "").strip()
            new_data = {**self.config_entry.data, **user_input}
            new_data[CONF_PLANT_NAME] = plant_name
            update_kwargs: dict[str, Any] = {"data": new_data}
            # Only push a new title when plant_name was provided AND differs;
            # blank plant_name means "leave title alone".
            if plant_name and plant_name != self.config_entry.title:
                update_kwargs["title"] = plant_name
            self.hass.config_entries.async_update_entry(
                self.config_entry, **update_kwargs
            )
            # Reload happens automatically via OptionsFlowWithReload.
            return self.async_create_entry(
                title="", data=dict(self.config_entry.options or {})
            )

        current = dict(self.config_entry.data)
        # Plant name default: existing entry.data value if present (set by
        # initial flow on v0.9.1+ entries), else the current entry title
        # (legacy entries created pre-v0.9.1 still show their utility-derived
        # title, which the user can now overwrite here).
        plant_name_default = current.get(CONF_PLANT_NAME) or self.config_entry.title
        schema = vol.Schema(
            {
                vol.Required(
                    CONF_STROMNETZEINSPEISUNG_KWH,
                    default=current.get(CONF_STROMNETZEINSPEISUNG_KWH),
                ): selector.EntitySelector(
                    selector.EntitySelectorConfig(
                        domain="sensor", device_class="energy"
                    )
                ),
                vol.Required(
                    CONF_RUECKLIEFERVERGUETUNG_CHF,
                    default=current.get(CONF_RUECKLIEFERVERGUETUNG_CHF),
                ): selector.EntitySelector(
                    selector.EntitySelectorConfig(domain="sensor")
                ),
                vol.Required(
                    CONF_PLANT_NAME, default=plant_name_default
                ): str,
                vol.Optional(
                    CONF_NAMENSPRAEFIX,
                    default=current.get(CONF_NAMENSPRAEFIX, "bfe_rueckliefertarif"),
                ): str,
            }
        )
        return self.async_show_form(step_id="entities", data_schema=schema)
