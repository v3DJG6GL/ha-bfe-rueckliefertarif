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

import itertools
import logging
from datetime import date
from typing import TYPE_CHECKING, Any

import voluptuous as vol
from homeassistant import config_entries
from homeassistant.helpers import selector

_LOGGER = logging.getLogger(__name__)

from .const import (  # noqa: E402  — intentionally below _LOGGER guard
    ABRECHNUNGS_RHYTHMUS_MONAT,
    ABRECHNUNGS_RHYTHMUS_QUARTAL,
    CONF_ABRECHNUNGS_RHYTHMUS,
    CONF_EIGENVERBRAUCH_AKTIVIERT,
    CONF_ENERGIEVERSORGER,
    CONF_HKN_AKTIVIERT,
    CONF_INSTALLIERTE_LEISTUNG_KWP,
    CONF_NAMENSPRAEFIX,
    CONF_PLANT_NAME,
    CONF_RUECKLIEFERVERGUETUNG_CHF,
    CONF_STROMNETZEINSPEISUNG_KWH,
    CONF_USER_INPUTS,
    CONF_VALID_FROM,
    DOMAIN,
    OPT_CONFIG_HISTORY,
    build_history_config,
)
from .tariffs_db import (  # noqa: E402
    compute_user_inputs_periods,
    find_active,
    find_active_rate_window,
    find_tier_for,
    list_utility_keys,
    load_tariffs,
    match_applies_when,
    pick_localised_label,
    pick_value_label,
    resolve_user_inputs_decl,
    self_consumption_relevant,
)


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


# Fired when the user changes the active-since date in the ConfigFlow
# tariff step — HA forms aren't reactive, so the first submit on a date
# change re-renders with this banner instead of progressing, giving the
# user a chance to review the (now updated) notes / tarif_urls /
# user_inputs before confirming.
_CHANGE_ADVISORY: dict[str, str] = {
    "de": (
        "ℹ️ **Aktiv-ab-Datum geändert.** Das Formular wurde anhand des "
        "neuen Datums aktualisiert (Hinweise / Tarifinformationen / "
        "versorgerspezifische Felder unten). **Weiter** erneut klicken, "
        "um zu übernehmen."
    ),
    "en": (
        "ℹ️ **Active-since date changed.** The form below has been "
        "updated for the new date (notes / tariff documentation / "
        "utility-specific fields). Click **Submit** again to apply."
    ),
    "fr": (
        "ℹ️ **Date d'activation modifiée.** Le formulaire ci-dessous a "
        "été mis à jour selon la nouvelle date (informations / "
        "documentation tarifaire / champs spécifiques au fournisseur). "
        "Cliquez sur **Suivant** à nouveau pour appliquer."
    ),
}


def _format_change_advisory(should_show: bool, lang: str) -> str:
    """Locale-picked advisory banner. Empty string when not shown so
    the description placeholder collapses cleanly."""
    if not should_show:
        return ""
    return _CHANGE_ADVISORY.get(lang) or _CHANGE_ADVISORY["en"]


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
            CONF_INSTALLIERTE_LEISTUNG_KWP,
            default=d.get(CONF_INSTALLIERTE_LEISTUNG_KWP, 0.0),
        ): selector.NumberSelector(
            selector.NumberSelectorConfig(
                min=0,
                max=10000,
                step=0.1,
                mode=selector.NumberSelectorMode.BOX,
                unit_of_measurement="kWp",
            )
        ),
        vol.Required(
            CONF_EIGENVERBRAUCH_AKTIVIERT,
            default=d.get(CONF_EIGENVERBRAUCH_AKTIVIERT, True),
        ): selector.BooleanSelector(),
    }
    if hkn_structure == "additive_optin":
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
    if float(user_input.get(CONF_INSTALLIERTE_LEISTUNG_KWP, 0)) <= 0:
        errors[CONF_INSTALLIERTE_LEISTUNG_KWP] = "kw_required"
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


def _find_tier_dry_run(
    utility_key: str, valid_from_iso: str, kw: float, user_inputs: dict | None
) -> bool:
    """O4 — return True iff ``find_tier_for`` resolves a tier for these
    args. False → save would fail at runtime (e.g. AEW kW=10 + RMP);
    reject at form submit instead.

    Permissive on lookup failure (no rate window, unknown utility): the
    real failure surfaces via the existing ``no_active_rate`` path, not
    here.
    """
    try:
        rate = find_active_rate_window(utility_key, date.fromisoformat(valid_from_iso))
    except (ValueError, KeyError):
        return True
    if rate is None:
        return True
    return find_tier_for(rate.get("power_tiers") or [], kw, user_inputs) is not None


_NOTE_SEVERITY_PREFIX: dict[str, str] = {
    # Variation-selector forms force emoji presentation (coloured icon)
    # rather than the stark monochrome glyph (the bare "ℹ" easily reads
    # as a stray letter "i" in HA's default font).
    "info": "ℹ️",
    "warning": "⚠️",
    "error": "🛑",
}

_NOTE_SEVERITY_LABEL: dict[str, dict[str, str]] = {
    "info": {"de": "Hinweis", "en": "Note", "fr": "Information"},
    "warning": {"de": "Warnung", "en": "Warning", "fr": "Avertissement"},
    "error": {"de": "Fehler", "en": "Error", "fr": "Erreur"},
}


def _pick_note_text(text_dict: dict[str, str] | None, lang: str) -> str | None:
    """Pick the best language match from a note's ``text`` dict.

    Order: user locale → ``de`` (Swiss default) → first available key.
    Returns ``None`` when ``text_dict`` is empty / missing.
    """
    if not text_dict:
        return None
    if lang in text_dict:
        return text_dict[lang]
    if "de" in text_dict:
        return text_dict["de"]
    return next(iter(text_dict.values()), None)


def _render_rate_notes(rate: dict, at_date: date, lang: str) -> str:
    """Render the notes-block markdown for a specific rate window at a
    specific date. Extracted from ``_notes_block`` so the per-period
    editor (Phase 2) can call it once per period without re-loading
    the db.
    """
    raw_notes = rate.get("notes") or []
    blocks: list[str] = []
    for n in raw_notes:
        nf = n.get("valid_from")
        nt = n.get("valid_to")
        f = date.fromisoformat(nf) if nf else date.min
        t = date.fromisoformat(nt) if nt else date.max
        if not (f <= at_date < t):
            continue
        text = _pick_note_text(n.get("text"), lang)
        if not text:
            continue
        severity = n.get("severity", "info")
        prefix = _NOTE_SEVERITY_PREFIX.get(severity, _NOTE_SEVERITY_PREFIX["info"])
        sev_dict = _NOTE_SEVERITY_LABEL.get(severity, _NOTE_SEVERITY_LABEL["info"])
        sev_label = sev_dict.get(lang) or sev_dict["en"]
        text_lines = text.splitlines() or [""]
        first = f"> {prefix} **{sev_label}:** {text_lines[0]}"
        rest = [f"> {ln}" for ln in text_lines[1:]]
        blocks.append("\n".join([first, *rest]))
    return "\n\n".join(blocks)


def _notes_block(utility_key: str, valid_from_iso: str, hass=None) -> str:
    """Markdown block rendered for utility notes active at ``valid_from_iso``.

    Each note is emitted as a markdown blockquote so HA's renderer paints
    a left-side bar / indent — visually distinct from prose body text:

        > ℹ️ **Hinweis:** {text}

    Severity emoji and label are locale-picked. Returns ``""`` when no
    notes apply (so the description placeholder collapses cleanly).
    """
    try:
        db = load_tariffs()
        utility = db["utilities"].get(utility_key)
        if utility is None:
            return ""
        at_date = date.fromisoformat(valid_from_iso)
        rate = find_active(utility["rates"], at_date)
        if rate is None:
            return ""
    except (KeyError, ValueError, LookupError):
        return ""

    lang = "en"
    if hass is not None:
        lang = (getattr(hass.config, "language", None) or "en").split("-")[0].lower()
    return _render_rate_notes(rate, at_date, lang)


# Locale-aware heading for the tarif_urls block. v0.12.0 (schema v1.2.0).
# Kept in Python rather than strings.json so the whole block can collapse
# to "" when no URLs apply (avoids an empty section in the rendered form).
_TARIF_URLS_HEADING: dict[str, str] = {
    "de": "**Tarifinformationen des Versorgers:**",
    "en": "**Utility tariff documentation:**",
    "fr": "**Informations tarifaires du fournisseur :**",
}


# Fallback metadata for entries lacking a curator-supplied label. The
# rendered label becomes "<emoji> <kind_label> · <domain>" (e.g.
# "📄 PDF · aew.ch") which is more readable than the raw URL.
_LINK_KIND_LABELS: dict[str, dict[str, str]] = {
    "pdf": {"de": "PDF", "en": "PDF", "fr": "PDF"},
    "html": {"de": "Webseite", "en": "Webpage", "fr": "Site web"},
}
_LINK_KIND_EMOJI: dict[str, str] = {"pdf": "📄", "html": "🌐"}


def _infer_link_kind(url: str, declared: str | None) -> str:
    """Pick ``pdf`` or ``html``. Honour curator-declared kind first;
    else infer from the URL extension (everything not ending in .pdf
    is treated as html)."""
    if declared in ("pdf", "html"):
        return declared
    return "pdf" if url.lower().rsplit("?", 1)[0].endswith(".pdf") else "html"


def _derive_link_fallback_label(url: str, kind: str, lang: str) -> str:
    """Build ``<emoji> <kind_label> · <domain>`` when no curator label."""
    from urllib.parse import urlparse

    domain = (urlparse(url).netloc or "").removeprefix("www.")
    emoji = _LINK_KIND_EMOJI.get(kind, "🌐")
    kind_dict = _LINK_KIND_LABELS.get(kind, _LINK_KIND_LABELS["html"])
    kind_label = kind_dict.get(lang) or kind_dict["en"]
    return f"{emoji} {kind_label} · {domain}" if domain else f"{emoji} {kind_label}"


def _resolve_tarif_urls(
    utility_key: str | None,
    valid_from_iso: str | None,
    user_inputs: dict | None,
) -> list[dict]:
    """Return the active rate-window's ``tarif_urls`` filtered by
    ``applies_when`` against ``user_inputs``. Empty list if no utility,
    no active window, no urls, or all entries gated out.
    """
    if not utility_key or not valid_from_iso:
        return []
    try:
        d = date.fromisoformat(valid_from_iso)
    except ValueError:
        return []
    rate = find_active_rate_window(utility_key, d)
    if rate is None:
        return []
    return [
        entry
        for entry in (rate.get("tarif_urls") or [])
        if match_applies_when(entry.get("applies_when"), user_inputs)
    ]


def _format_tarif_urls_block(urls: list[dict], lang: str) -> str:
    """Render ``urls`` as a markdown bullet list with a locale-picked
    heading. Returns ``""`` when there are no usable entries so the
    description placeholder collapses cleanly.
    """
    if not urls:
        return ""
    heading = _TARIF_URLS_HEADING.get(lang, _TARIF_URLS_HEADING["en"])
    lines = [heading]
    for entry in urls:
        url = entry.get("url")
        if not url:
            continue
        # "" sentinel triggers URL-derived fallback ("📄 PDF · domain")
        # below; with a curator label, _pick_localised_label wins.
        raw_label = pick_localised_label(entry, "label", lang, "")
        if raw_label:
            label = raw_label
        else:
            kind = _infer_link_kind(url, entry.get("kind"))
            label = _derive_link_fallback_label(url, kind, lang)
        lines.append(f"- [{label}]({url})")
    if len(lines) == 1:
        return ""
    return "\n".join(lines)


# v0.17.0 — heading per language for the user-inputs help block. Falls
# back to "en" for unknown languages.
_USER_INPUTS_HELP_HEADING = {
    "de": "**Versorger-spezifische Optionen:**",
    "en": "**Utility-specific options:**",
    "fr": "**Options spécifiques au fournisseur :**",
}


def _user_inputs_help_block(decls: list | tuple | None, lang: str) -> str:
    """Render a Markdown bullet list from each user_input declaration's
    ``description_<lang>`` field, prefixed with the field's localised label.

    Returns ``""`` when no decl has a usable description (so the
    ``{user_inputs_help}`` placeholder collapses cleanly when the rate
    window has no per-field help text).

    Example output for Regio's ``regio_top40_opted_in`` (lang=de):

        **Versorger-spezifische Optionen:**

        - **Wahltarif TOP-40 abonniert:** Voraussetzungen: dauerhafte
          Begrenzung der Einspeiseleistung auf 60 % der DC-Maximalleistung;
          DC-Leistung > 3.7 kWp; Pronovo-Zertifizierung.
    """
    if not decls:
        return ""
    bullets: list[str] = []
    for decl in decls:
        if not isinstance(decl, dict):
            continue
        description = pick_localised_label(decl, "description", lang, "")
        if not description:
            continue
        label = pick_localised_label(
            decl, "label", lang, decl.get("key", "—")
        )
        bullets.append(f"- **{label}:** {description}")
    if not bullets:
        return ""
    heading = _USER_INPUTS_HELP_HEADING.get(
        lang, _USER_INPUTS_HELP_HEADING["en"]
    )
    return heading + "\n\n" + "\n".join(bullets)


# v0.17.0 — title for the refresh-prices notification. Static, bilingual.
_REFRESH_NOTIFICATION_TITLE = "Referenzmarktpreise / Tarifdaten aktualisiert"


def _format_rate_window_dates(dates: list[str]) -> list[str]:
    """Group rate-window valid_from dates by year.

    - Single window per year → ``"2026"``
    - Multiple windows per year → all full dates (``"2026-01-01"``, ``"2026-07-01"``)

    Returns a list of display strings, one per year, in chronological order
    of the year (newest first when input dates are descending; otherwise
    by year value).
    """
    if not dates:
        return []
    by_year: dict[str, list[str]] = {}
    for d in dates:
        if not isinstance(d, str) or len(d) < 4:
            continue
        year = d[:4]
        by_year.setdefault(year, []).append(d)
    out: list[str] = []
    for year in sorted(by_year.keys(), reverse=True):
        windows = by_year[year]
        if len(windows) == 1:
            out.append(year)
        else:
            for w in sorted(windows):
                out.append(w)
    return out


def _render_refresh_notification(result: dict) -> str:
    """Build the Markdown body for the refresh-prices notification (v0.17.0).

    Two top-level sections:

    1. **Reference market prices (SFOE)** — BFE OGD CSV poll status. Always
       present. Shows the count of available BFE-published quarters and any
       newly-imported quarters this tick.

    2. **Tariff data (bfe-tariffs-data v…)** — companion-repo refresh status.
       Renders per-utility added/modified rate windows as a nested bullet
       list. When nothing changed, prints "No changes since last refresh".

    On fetch failure, the tariff-data section reports the error message.
    """
    lines: list[str] = []

    # --- Section 1: BFE reference market prices --------------------------
    avail = result.get("available") or []
    new = result.get("newly_imported") or []
    lines.append("## Reference market prices (SFOE)")
    lines.append("")
    if avail:
        lines.append(
            f"- {len(avail)} quarter(s) available (latest: {max(avail)})"
        )
    else:
        lines.append("- No BFE quarters available yet")
    if new:
        lines.append(
            "- Newly imported: " + ", ".join(str(q) for q in new)
        )
    else:
        lines.append("- No new quarters since last import")
    lines.append("")

    # --- Section 2: tariff data ------------------------------------------
    data_v = result.get("tariffs_data_version")
    version_suffix = f" (bfe-tariffs-data v{data_v})" if data_v else ""
    lines.append(f"## Tariff data{version_suffix}")
    lines.append("")

    err = result.get("tariffs_error")
    if err and not result.get("tariffs_refreshed"):
        lines.append(f"- Refresh failed: {err}")
        lines.append("- Using cached tariffs.")
        return "\n".join(lines)

    diff = result.get("tariffs_diff")
    if not diff or diff.get("no_changes"):
        lines.append("- No changes since last refresh")
        return "\n".join(lines)

    added_utilities = diff.get("added_utilities") or []
    added_rate_windows = diff.get("added_rate_windows") or []
    modified_rate_windows = diff.get("modified_rate_windows") or []

    if added_utilities:
        lines.append("### Newly added utilities")
        for entry in added_utilities:
            lines.append(f"- {entry['name']}")
            for d in _format_rate_window_dates(
                entry.get("rate_window_dates") or []
            ):
                lines.append(f"    - {d}")
        lines.append("")

    if added_rate_windows:
        lines.append("### Newly added rate windows")
        for entry in added_rate_windows:
            lines.append(f"- {entry['name']}")
            for d in _format_rate_window_dates(
                entry.get("rate_window_dates") or []
            ):
                lines.append(f"    - {d}")
        lines.append("")

    if modified_rate_windows:
        lines.append("### Modified rate windows")
        for entry in modified_rate_windows:
            lines.append(f"- {entry['name']}")
            for d in _format_rate_window_dates(
                entry.get("rate_window_dates") or []
            ):
                lines.append(f"    - {d}")
        lines.append("")

    # Trailing blank from the last section — strip it.
    while lines and lines[-1] == "":
        lines.pop()
    return "\n".join(lines)


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
        "config": build_history_config(entry_data),
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
    kwp = config.get(CONF_INSTALLIERTE_LEISTUNG_KWP)
    kwp_s = f"{float(kwp):.1f} kWp" if kwp is not None else "—"
    ev = "EV" if config.get(CONF_EIGENVERBRAUCH_AKTIVIERT) else "no-EV"
    hkn = "HKN" if config.get(CONF_HKN_AKTIVIERT) else "no-HKN"
    billing = config.get(CONF_ABRECHNUNGS_RHYTHMUS) or "—"
    return f"{utility} · {kwp_s} · {ev} · {hkn} · {billing}"


# v0.11.0 (Batch D) — declared user_inputs helpers ----------------------------


def _candidates_for_decl(decl: dict) -> list | None:
    """Enumerate testable candidate values for one user_input decl.
    Returns ``None`` for non-gate-affecting types (text/number)."""
    dtype = decl.get("type")
    if dtype == "enum":
        return list(decl.get("values") or [])
    if dtype == "boolean":
        return [True, False]
    return None


def _other_field_combinations(
    other_decls: list[dict],
    defaults_user_inputs: dict,
) -> list[dict]:
    """Cartesian product of candidate values for the OTHER user_inputs.
    Each output dict maps key → one candidate value. Non-gate-affecting
    fields (text/number) carry the stored/default value through every
    combination so probes don't accidentally drop them.
    """
    legs: list[list[tuple]] = []
    for d in other_decls:
        cands = _candidates_for_decl(d)
        if cands is None:
            cands = [defaults_user_inputs.get(d["key"], d.get("default"))]
        legs.append([(d["key"], v) for v in cands])
    return [dict(combo) for combo in itertools.product(*legs)] or [{}]


def _filter_candidates_by_tier(
    decl: dict,
    all_decls: tuple[dict, ...],
    defaults_user_inputs: dict,
    gate_utility: str,
    gate_valid_from: str,
    gate_kw: float,
) -> list | None:
    """Constraint-aware filter: keep candidate values for ``decl`` that
    resolve a tier under SOME combination of the other gate-affecting
    fields' candidates. Returns ``None`` for non-gate-affecting types
    (caller renders them un-filtered).

    A value is hidden ONLY if no combination of siblings makes it valid;
    a value is kept whenever it could resolve under some sibling combo.
    The submit-time ``_find_tier_dry_run`` defensive still catches the
    user's chosen specific combination if it doesn't resolve.
    """
    candidates = _candidates_for_decl(decl)
    if candidates is None:
        return None
    key = decl["key"]
    other_decls = [d for d in all_decls if d["key"] != key]
    other_combos = _other_field_combinations(other_decls, defaults_user_inputs)
    keep: list = []
    for c in candidates:
        for combo in other_combos:
            probe = {**defaults_user_inputs, **combo, key: c}
            if _find_tier_dry_run(gate_utility, gate_valid_from, gate_kw, probe):
                keep.append(c)
                break
    return keep


def _add_user_input_fields_namespaced(
    schema_dict: dict,
    decl_list: tuple[dict, ...],
    defaults_user_inputs: dict,
    lang: str,
    prefix: str = "",
    *,
    gate_utility: str | None = None,
    gate_valid_from: str | None = None,
    gate_kw: float | None = None,
) -> None:
    """Append one ``vol.Required`` selector per declared user_input.

    v0.13.0 — Phase 2: ``prefix`` namespaces the form key (e.g.
    ``"period_0_"``) so multi-period editors can ask the same logical
    user_input multiple times without form-key collision. ``defaults_
    user_inputs`` is keyed by the ORIGINAL declaration key (not the
    prefixed form key), so call sites pass the per-period default dict
    de-namespaced.

    v0.14.0 — kW-aware filtering: when all of ``gate_utility``,
    ``gate_valid_from``, ``gate_kw`` are non-None, candidate values for
    enum/boolean decls are filtered via constraint-aware probing —
    impossible combos at the given kW never reach the form. A field is
    omitted entirely if zero candidates remain (page-level
    ``no_matching_tier`` defensive surfaces it).

    ``type="enum"`` → dropdown; values list comes from the declaration.
    ``type="boolean"`` → toggle. Defaults: existing record's value if
    present, else ``decl["default"]``.
    """
    gates_active = (
        gate_utility is not None
        and gate_valid_from is not None
        and gate_kw is not None
    )
    for decl in decl_list:
        key = decl["key"]
        decl_default = decl.get("default")
        chosen = defaults_user_inputs.get(key, decl_default)
        prefixed_key = f"{prefix}{key}"

        if decl["type"] == "enum":
            values = list(decl.get("values", []) or [])
            if gates_active:
                filtered = _filter_candidates_by_tier(
                    decl, decl_list, defaults_user_inputs,
                    gate_utility, gate_valid_from, gate_kw,
                )
                if filtered is not None:
                    values = filtered
            if not values:
                continue
            if chosen not in values:
                chosen = values[0]
            options = [
                selector.SelectOptionDict(
                    value=str(v),
                    label=pick_value_label(decl, str(v), lang),
                )
                for v in values
            ]
            schema_dict[
                vol.Required(prefixed_key, default=chosen)
            ] = selector.SelectSelector(
                selector.SelectSelectorConfig(
                    options=options,
                    mode=selector.SelectSelectorMode.DROPDOWN,
                )
            )
        elif decl["type"] == "boolean":
            allowed = [True, False]
            if gates_active:
                filtered = _filter_candidates_by_tier(
                    decl, decl_list, defaults_user_inputs,
                    gate_utility, gate_valid_from, gate_kw,
                )
                if filtered is not None:
                    allowed = filtered
            if not allowed:
                continue
            chosen_bool = bool(chosen) if chosen is not None else False
            if chosen_bool not in allowed:
                chosen_bool = allowed[0]
            # If only one bool is valid, default to it. The user CAN still
            # toggle to the invalid value — submit-time _find_tier_dry_run
            # catches it via no_matching_tier. (BooleanSelector can't be
            # rendered as a constrained single-option widget without
            # breaking _validate_user_inputs's isinstance(bool) check.)
            schema_dict[
                vol.Required(prefixed_key, default=chosen_bool)
            ] = selector.BooleanSelector()


def _add_user_input_fields(
    schema_dict: dict,
    decl_list: tuple[dict, ...],
    defaults_user_inputs: dict,
    lang: str,
    *,
    gate_utility: str | None = None,
    gate_valid_from: str | None = None,
    gate_kw: float | None = None,
) -> None:
    """Single-period variant — see ``_add_user_input_fields_namespaced``.
    Kept as a thin wrapper so existing call sites stay unchanged.
    """
    _add_user_input_fields_namespaced(
        schema_dict, decl_list, defaults_user_inputs, lang, prefix="",
        gate_utility=gate_utility,
        gate_valid_from=gate_valid_from,
        gate_kw=gate_kw,
    )


_PERIOD_LABEL: dict[str, str] = {
    "de": "Zeitraum", "en": "Period", "fr": "Période",
}
_PERIOD_OPEN: dict[str, str] = {
    "de": "offen", "en": "open", "fr": "ouvert",
}


def _period_prefix(idx: int) -> str:
    """Key prefix for the i-th period's user_input form fields."""
    return f"period_{idx}_"


def _format_periods_block(
    periods: list[tuple],
    period_user_inputs: list[dict] | None,
    lang: str,
) -> str:
    """v0.13.0 — Phase 2: render the multi-period editor's per-period
    markdown summary (header + notes + tarif_urls). Returns ``""`` for
    single-period (caller falls back to the single-period
    ``notes_block`` + ``tarif_urls_block`` placeholders).

    ``period_user_inputs[i]`` is the i-th period's currently-selected
    user_inputs (used for tarif_urls ``applies_when`` filtering). Passing
    ``None`` shows only unconditional links.
    """
    if len(periods) <= 1:
        return ""
    period_label = _PERIOD_LABEL.get(lang) or _PERIOD_LABEL["en"]
    open_label = _PERIOD_OPEN.get(lang) or _PERIOD_OPEN["en"]
    blocks: list[str] = []
    for idx, (period_from, period_to, rep_rate) in enumerate(periods):
        end_str = period_to.isoformat() if period_to is not None else open_label
        blocks.append(
            f"### {period_label} {idx + 1}: {period_from.isoformat()} → {end_str}"
        )
        notes_md = _render_rate_notes(rep_rate, period_from, lang)
        if notes_md:
            blocks.append(notes_md)
        ui = period_user_inputs[idx] if period_user_inputs and idx < len(period_user_inputs) else None
        urls = []
        for entry in rep_rate.get("tarif_urls") or []:
            if match_applies_when(entry.get("applies_when"), ui):
                urls.append(entry)
        urls_md = _format_tarif_urls_block(urls, lang)
        if urls_md:
            blocks.append(urls_md)
    return "\n\n".join(blocks)


def _split_user_inputs_per_period(
    periods: list[tuple],
    user_input: dict,
) -> list[dict]:
    """Extract per-period user_inputs dicts from a Step 2 form payload
    that namespaces keys with ``period_<idx>_<key>``. Each returned dict
    contains the period's declared user_input keys with values pulled
    from ``user_input`` (or the schema-declared default when missing).
    """
    out: list[dict] = []
    for idx, (_pf, _pt, rep_rate) in enumerate(periods):
        prefix = _period_prefix(idx)
        decls = rep_rate.get("user_inputs") or []
        period_dict: dict = {}
        for decl in decls:
            key = decl["key"]
            period_dict[key] = user_input.get(
                f"{prefix}{key}", decl.get("default")
            )
        out.append(period_dict)
    return out


def _validate_user_inputs_namespaced(
    periods: list[tuple], user_input: dict
) -> dict[str, str]:
    """Validate enum and boolean per-period values; returns errors dict
    keyed by the namespaced form-key (so HA highlights the correct
    field on re-render)."""
    errors: dict[str, str] = {}
    for idx, (_pf, _pt, rep_rate) in enumerate(periods):
        prefix = _period_prefix(idx)
        decls = rep_rate.get("user_inputs") or []
        for decl in decls:
            key = decl["key"]
            prefixed = f"{prefix}{key}"
            if prefixed not in user_input:
                continue
            value = user_input[prefixed]
            if decl["type"] == "enum":
                allowed = set(decl.get("values") or [])
                if value not in allowed:
                    errors[prefixed] = "invalid_user_input_value"
            elif decl["type"] == "boolean":
                if not isinstance(value, bool):
                    errors[prefixed] = "invalid_user_input_value"
    return errors


def _validate_user_inputs(
    decl_list: tuple[dict, ...], user_input: dict
) -> dict[str, str]:
    """Validate user-supplied values against declarations. Returns a dict
    of ``{field_key: error_code}`` (empty when all valid). Schema-side
    JSON validation already ran on the data file; this only guards against
    user form submissions that don't match the active declarations.
    """
    errors: dict[str, str] = {}
    for decl in decl_list:
        key = decl["key"]
        if key not in user_input:
            continue  # voluptuous's vol.Required raises before we see this
        val = user_input[key]
        if decl["type"] == "enum":
            values = decl.get("values", []) or []
            if val not in values:
                errors[key] = "invalid_choice"
        elif decl["type"] == "boolean":
            if not isinstance(val, bool):
                errors[key] = "invalid_type"
    return errors


def _user_inputs_payload(
    decl_list: tuple[dict, ...], user_input: dict
) -> dict:
    """Project ``user_input`` to just the declared keys. Missing keys
    fall back to the declaration's ``default`` so the stored record
    always carries a complete dict."""
    out: dict = {}
    for decl in decl_list:
        key = decl["key"]
        out[key] = user_input.get(key, decl.get("default"))
    return out


class BfeRuecklieferTarifFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Menu-first 3-step config flow."""

    VERSION = 4

    def __init__(self) -> None:
        self._data: dict[str, Any] = {}

    @staticmethod
    def async_get_options_flow(
        config_entry: config_entries.ConfigEntry,
    ) -> BfeRuecklieferTarifOptionsFlow:
        return BfeRuecklieferTarifOptionsFlow()

    # ----- Step 1: combined utility + date + kW picker ---------------------------

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """v0.18.0 — first-time setup Step 1 (Issue 6.4.1): combined
        utility + active-since + kW form. Replaces the v0.14.0 split of
        a 200+ utility menu (`user`) followed by a separate `tariff_pick`
        step. Mirrors ``async_step_apply_change`` so every flow's first
        page renders identically.
        """
        await _async_warm_cache(self.hass)

        errors: dict[str, str] = {}
        if user_input is not None:
            try:
                picked_from = _parse_valid_from(
                    user_input.get(CONF_VALID_FROM, "")
                )
            except ValueError:
                errors[CONF_VALID_FROM] = "invalid_valid_from"
            if not errors:
                kw = float(user_input.get(CONF_INSTALLIERTE_LEISTUNG_KWP) or 0.0)
                if kw <= 0:
                    errors[CONF_INSTALLIERTE_LEISTUNG_KWP] = "kw_required"
            if not errors:
                if find_active_rate_window(
                    user_input[CONF_ENERGIEVERSORGER],
                    date.fromisoformat(picked_from),
                ) is None:
                    errors[CONF_VALID_FROM] = "no_active_rate"
            if not errors:
                self._data[CONF_ENERGIEVERSORGER] = user_input[
                    CONF_ENERGIEVERSORGER
                ]
                self._setup_pick = {
                    CONF_ENERGIEVERSORGER: user_input[CONF_ENERGIEVERSORGER],
                    CONF_VALID_FROM: picked_from,
                    CONF_INSTALLIERTE_LEISTUNG_KWP: kw,
                }
                return await self.async_step_tariff_details()

        utility_keys = list_utility_keys()
        default_valid_from = (
            user_input.get(CONF_VALID_FROM) if user_input is not None
            else _quarter_start_today()
        )
        default_utility = (
            user_input.get(CONF_ENERGIEVERSORGER) if user_input is not None
            else utility_keys[0]
        )
        default_kw = (
            user_input.get(CONF_INSTALLIERTE_LEISTUNG_KWP)
            if user_input is not None
            else 0.0
        )
        schema = vol.Schema({
            vol.Required(CONF_VALID_FROM, default=default_valid_from):
                selector.DateSelector(),
            vol.Required(CONF_ENERGIEVERSORGER, default=default_utility):
                selector.SelectSelector(
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
                CONF_INSTALLIERTE_LEISTUNG_KWP,
                default=default_kw,
            ): selector.NumberSelector(
                selector.NumberSelectorConfig(
                    min=0, max=10000, step=0.1,
                    mode=selector.NumberSelectorMode.BOX,
                    unit_of_measurement="kWp",
                )
            ),
        })
        return self.async_show_form(
            step_id="user",
            data_schema=schema,
            errors=errors,
            last_step=False,
            description_placeholders=_source_links(self.hass),
        )

    # ----- Step 2: tariff configuration -----------------------------------------

    async def async_step_tariff_details(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """v0.14.0 — first-time setup Step 2b: EV + HKN + user_inputs[].

        Mirrors ``async_step_apply_change_details`` exactly. Pre-builds
        the OPT_CONFIG_HISTORY records (single or split-on-multi-period)
        and stashes them in ``self._setup_history`` for
        ``async_step_entities`` to pass via ``async_create_entry``'s
        ``options`` kwarg — bypasses ``__init__.py``'s history synthesis
        path so user_inputs and multi-period splits make it into storage.
        """
        pick = getattr(self, "_setup_pick", None)
        if pick is None:
            return await self.async_step_user()
        gate_utility: str = pick[CONF_ENERGIEVERSORGER]
        gate_valid_from: str = pick[CONF_VALID_FROM]
        gate_kw: float = float(pick[CONF_INSTALLIERTE_LEISTUNG_KWP])

        span_from = date.fromisoformat(gate_valid_from)
        periods = compute_user_inputs_periods(gate_utility, span_from, None)
        is_multi = len(periods) > 1

        errors: dict[str, str] = {}
        decl_list = resolve_user_inputs_decl(gate_utility, gate_valid_from)

        if user_input is not None:
            if is_multi:
                errors.update(_validate_user_inputs_namespaced(periods, user_input))
            else:
                errors.update(_validate_user_inputs(decl_list, user_input))

            derived_billing: str | None = None
            if not errors:
                try:
                    derived_billing = _derive_billing(gate_utility, gate_valid_from)
                except NotImplementedError:
                    errors["base"] = "settlement_period_unsupported"
                except (KeyError, LookupError):
                    errors["base"] = "no_active_rate"

            if is_multi:
                period_user_inputs = _split_user_inputs_per_period(periods, user_input)
            else:
                period_user_inputs = [_user_inputs_payload(decl_list, user_input)]

            if not errors:
                for idx, (pf, _pt, _rep) in enumerate(periods):
                    if not _find_tier_dry_run(
                        gate_utility, pf.isoformat(), gate_kw, period_user_inputs[idx]
                    ):
                        errors["base"] = "no_matching_tier"
                        break

            if not errors:
                hkn_structure = _active_hkn_structure(gate_utility, gate_valid_from)
                show_eigenverbrauch = self_consumption_relevant(
                    gate_utility, gate_valid_from, gate_kw
                )
                ev_value = bool(
                    user_input.get(CONF_EIGENVERBRAUCH_AKTIVIERT, True)
                ) if show_eigenverbrauch else True
                hkn_value = _force_hkn_for_save(
                    hkn_structure, user_input.get(CONF_HKN_AKTIVIERT, False)
                )

                new_records: list[dict] = []
                for idx, (pf, _pt, _rep) in enumerate(periods):
                    record_valid_from = (
                        gate_valid_from if idx == 0 else pf.isoformat()
                    )
                    new_records.append({
                        "valid_from": record_valid_from,
                        "valid_to": None,
                        "config": {
                            CONF_ENERGIEVERSORGER: gate_utility,
                            CONF_INSTALLIERTE_LEISTUNG_KWP: gate_kw,
                            CONF_EIGENVERBRAUCH_AKTIVIERT: ev_value,
                            CONF_HKN_AKTIVIERT: hkn_value,
                            CONF_ABRECHNUNGS_RHYTHMUS: derived_billing,
                            CONF_USER_INPUTS: period_user_inputs[idx],
                        },
                    })

                # Stash for async_create_entry's options kwarg.
                self._setup_history = _normalize_history(new_records)
                # Also seed entry.data with period-0 values so any code
                # path that reads from entry.data (e.g. legacy fallbacks)
                # still works coherently.
                self._data.update({
                    CONF_VALID_FROM: gate_valid_from,
                    CONF_INSTALLIERTE_LEISTUNG_KWP: gate_kw,
                    CONF_EIGENVERBRAUCH_AKTIVIERT: ev_value,
                    CONF_HKN_AKTIVIERT: hkn_value,
                    CONF_ABRECHNUNGS_RHYTHMUS: derived_billing,
                    CONF_USER_INPUTS: period_user_inputs[0],
                })
                return await self.async_step_entities()

        defaults_cfg = (
            user_input if user_input is not None else {}
        )
        hkn_structure = _active_hkn_structure(gate_utility, gate_valid_from)
        show_eigenverbrauch = self_consumption_relevant(
            gate_utility, gate_valid_from, gate_kw
        )

        schema_dict: dict[Any, Any] = {}
        if show_eigenverbrauch:
            schema_dict[
                vol.Required(
                    CONF_EIGENVERBRAUCH_AKTIVIERT,
                    default=bool(
                        defaults_cfg.get(CONF_EIGENVERBRAUCH_AKTIVIERT, True)
                    ),
                )
            ] = selector.BooleanSelector()
        if hkn_structure == "additive_optin":
            schema_dict[
                vol.Required(
                    CONF_HKN_AKTIVIERT,
                    default=bool(defaults_cfg.get(CONF_HKN_AKTIVIERT, False)),
                )
            ] = selector.BooleanSelector()

        decl_defaults: dict = {}
        lang = (
            getattr(self.hass.config, "language", None) or "de"
        ).split("-")[0].lower()

        if is_multi:
            for idx, (pf, _pt, rep_rate) in enumerate(periods):
                period_decls = tuple(rep_rate.get("user_inputs") or ())
                if user_input is not None:
                    period_defaults = {
                        d["key"]: user_input.get(
                            f"{_period_prefix(idx)}{d['key']}", d.get("default")
                        )
                        for d in period_decls
                    }
                else:
                    period_defaults = {}
                _add_user_input_fields_namespaced(
                    schema_dict, period_decls, period_defaults, lang,
                    prefix=_period_prefix(idx),
                    gate_utility=gate_utility,
                    gate_valid_from=pf.isoformat(),
                    gate_kw=gate_kw,
                )
            periods_block = _format_periods_block(
                periods,
                _split_user_inputs_per_period(periods, user_input or {}),
                lang,
            )
        else:
            _add_user_input_fields(
                schema_dict, decl_list, decl_defaults, lang,
                gate_utility=gate_utility,
                gate_valid_from=gate_valid_from,
                gate_kw=gate_kw,
            )
            periods_block = ""

        return self.async_show_form(
            step_id="tariff_details",
            data_schema=vol.Schema(schema_dict),
            errors=errors,
            last_step=False,
            description_placeholders={
                "utility_name": _utility_display_name(gate_utility),
                "valid_from": gate_valid_from,
                "hkn_gate_note": _hkn_gate_note(hkn_structure, self.hass),
                "notes_block": (
                    "" if is_multi
                    else _notes_block(gate_utility, gate_valid_from, self.hass)
                ),
                "tarif_urls_block": (
                    "" if is_multi
                    else _format_tarif_urls_block(
                        _resolve_tarif_urls(
                            gate_utility, gate_valid_from, decl_defaults
                        ),
                        lang,
                    )
                ),
                "user_inputs_help": (
                    "" if is_multi
                    else _user_inputs_help_block(decl_list, lang)
                ),
                "periods_block": periods_block,
                **_source_links(self.hass),
            },
        )

    # ----- Step 3: HA entities --------------------------------------------------

    async def async_step_entities(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
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
            # v0.14.0 — when tariff_details has pre-built history records
            # (single or multi-period split), pass them via options so
            # __init__.py's synthesis path doesn't drop user_inputs or
            # collapse a multi-period setup into one record.
            create_kwargs: dict[str, Any] = {
                "title": plant_name,
                "data": self._data,
            }
            history = getattr(self, "_setup_history", None)
            if history:
                create_kwargs["options"] = {OPT_CONFIG_HISTORY: history}
            return self.async_create_entry(**create_kwargs)

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
    ) -> FlowResult:
        return self.async_show_menu(
            step_id="init",
            menu_options=[
                "manage_history",
                "recompute_history",
                "refresh_data",
                "entities",
            ],
        )

    # ----- Sub-step: manage configuration history ----------------------------

    async def async_step_manage_history(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
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
            menu[f"edit_pick_row_{i}"] = label
        menu["add_pick_row"] = "+ Add new transition"
        menu["done_history"] = "Done"
        return self.async_show_menu(step_id="manage_history", menu_options=menu)

    async def async_step_done_history(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
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

    # ----- Manage-history wizard: Step 1 (picker) ----------------------------

    async def async_step_add_pick_row(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Step 1 (add): pick utility + valid_from for a new history row."""
        return await self._pick_row(idx=None, user_input=user_input)

    async def async_step_edit_pick_row(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Step 1 (edit): pick utility + valid_from for an existing row."""
        return await self._pick_row(
            getattr(self, "_editing_idx", None), user_input
        )

    def __getattr__(self, name: str):
        # Dynamic dispatch for "edit_pick_row_<N>" menu options. We stash
        # the row index on self and delegate to async_step_edit_pick_row,
        # which uses a static step_id so translations resolve.
        if name.startswith("async_step_edit_pick_row_"):
            try:
                idx = int(name.removeprefix("async_step_edit_pick_row_"))
            except ValueError:
                raise AttributeError(name) from None
            async def _step(user_input=None, _idx=idx):
                self._editing_idx = _idx
                return await self.async_step_edit_pick_row(user_input)
            return _step
        raise AttributeError(name)

    async def _pick_row(
        self, idx: int | None, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Render the Step 1 picker form (utility + valid_from) for either
        add (idx=None) or edit (idx=N) of a history row. On submit, stash
        the picks in ``self._row_pick`` and route to Step 2.
        """
        history = list(self.config_entry.options.get(OPT_CONFIG_HISTORY) or [])
        is_edit = idx is not None and 0 <= idx < len(history)
        existing = history[idx] if is_edit else None

        errors: dict[str, str] = {}
        if user_input is not None:
            try:
                picked_from = _parse_valid_from(user_input.get("valid_from", ""))
            except ValueError:
                errors["valid_from"] = "invalid_valid_from"
            if not errors:
                kw = float(user_input.get(CONF_INSTALLIERTE_LEISTUNG_KWP) or 0.0)
                if kw <= 0:
                    errors[CONF_INSTALLIERTE_LEISTUNG_KWP] = "kw_required"
            if not errors:
                # v0.13.0 — Step 1 validation: reject if (utility, valid_from)
                # doesn't resolve to a rate window.
                if find_active_rate_window(
                    user_input[CONF_ENERGIEVERSORGER],
                    date.fromisoformat(picked_from),
                ) is None:
                    errors["valid_from"] = "no_active_rate"
            if not errors:
                self._row_pick = {
                    CONF_ENERGIEVERSORGER: user_input[CONF_ENERGIEVERSORGER],
                    "valid_from": picked_from,
                    CONF_INSTALLIERTE_LEISTUNG_KWP: kw,
                }
                if is_edit:
                    return await self.async_step_edit_row()
                return await self.async_step_add_new_row()

        # Defaults: existing record (edit) or open record (add).
        if existing is not None:
            default_utility = existing["config"].get(CONF_ENERGIEVERSORGER)
            default_valid_from = existing["valid_from"]
            default_kw = existing["config"].get(CONF_INSTALLIERTE_LEISTUNG_KWP, 0.0)
        else:
            open_rec = next(
                (r for r in history if r.get("valid_to") is None), None
            )
            cfg = (open_rec or {}).get("config") or build_history_config(
                self.config_entry.data
            )
            default_utility = cfg.get(CONF_ENERGIEVERSORGER)
            default_valid_from = _quarter_start_today()
            default_kw = cfg.get(CONF_INSTALLIERTE_LEISTUNG_KWP, 0.0)

        utility_keys = list_utility_keys()
        schema = vol.Schema({
            vol.Required("valid_from", default=default_valid_from):
                selector.DateSelector(),
            vol.Required(
                CONF_ENERGIEVERSORGER,
                default=default_utility or utility_keys[0],
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
                CONF_INSTALLIERTE_LEISTUNG_KWP,
                default=default_kw,
            ): selector.NumberSelector(
                selector.NumberSelectorConfig(
                    min=0, max=10000, step=0.1,
                    mode=selector.NumberSelectorMode.BOX,
                    unit_of_measurement="kWp",
                )
            ),
        })
        # v0.18.1: surface the open record's config as "Currently active:"
        # context. For add (idx=None) we use the open record; for edit, the
        # row being edited. None → em-dash.
        if existing is not None:
            ctx_cfg = existing.get("config")
        else:
            open_rec = next((r for r in history if r.get("valid_to") is None), None)
            ctx_cfg = (open_rec or {}).get("config") if open_rec else None
        return self.async_show_form(
            step_id="edit_pick_row" if is_edit else "add_pick_row",
            data_schema=schema,
            errors=errors,
            last_step=False,
            description_placeholders={
                "current_summary": (
                    _format_config_summary(ctx_cfg) if ctx_cfg else "—"
                ),
                **_source_links(self.hass),
            },
        )

    # ----- Manage-history wizard: Step 2 (details) ---------------------------

    async def async_step_add_new_row(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        return await self._edit_row(idx=None, user_input=user_input)

    async def async_step_edit_row(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        return await self._edit_row(
            getattr(self, "_editing_idx", None), user_input
        )

    async def _edit_row(
        self, idx: int | None, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Step 2: render the details form (kW / EV / HKN / user_inputs)
        for the (utility, valid_from) the user picked in Step 1
        (``self._row_pick``). Save on submit. ``idx=None`` = add-new
        mode; otherwise edit-existing mode.
        """
        pick = getattr(self, "_row_pick", None)
        if pick is None:
            # Defensive — fall back to Step 1 if state was lost.
            return await self._pick_row(idx, None)
        gate_utility: str = pick[CONF_ENERGIEVERSORGER]
        gate_valid_from: str = pick["valid_from"]
        gate_kw: float = float(pick[CONF_INSTALLIERTE_LEISTUNG_KWP])

        history = list(self.config_entry.options.get(OPT_CONFIG_HISTORY) or [])
        _LOGGER.debug(
            "_edit_row entry: idx=%s, %d record(s) currently in OPT_CONFIG_HISTORY",
            idx, len(history),
        )
        is_edit = idx is not None and 0 <= idx < len(history)
        existing = history[idx] if is_edit else None

        # v0.13.0 (Phase 2) — compute the entry's effective span. span_to
        # is the next history entry's valid_from in chronological order
        # (excluding the entry being edited so we don't pin span_to to
        # the entry's own valid_from). None = open-ended.
        span_from = date.fromisoformat(gate_valid_from)
        span_to: date | None = None
        for r in sorted(
            (h for h in history if h is not existing),
            key=lambda h: h.get("valid_from") or "",
        ):
            rf_iso = r.get("valid_from")
            if rf_iso and rf_iso > gate_valid_from:
                span_to = date.fromisoformat(rf_iso)
                break
        periods = compute_user_inputs_periods(gate_utility, span_from, span_to)
        is_multi = len(periods) > 1

        errors: dict[str, str] = {}
        decl_list = resolve_user_inputs_decl(gate_utility, gate_valid_from)

        if user_input is not None:
            # Delete branch (only when editing). Refuse to delete the last
            # record so the sentinel is always present.
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
                    # v0.16.1 — trigger reload so the coordinator re-runs
                    # auto-import and the recompute notification fires.
                    # async_update_entry alone doesn't reload (apply_change
                    # uses async_create_entry → OptionsFlowWithReload — but
                    # manage_history goes via this direct-update path).
                    self.hass.async_create_task(
                        self.hass.config_entries.async_reload(
                            self.config_entry.entry_id
                        )
                    )
                    return await self.async_step_manage_history()

            if is_multi:
                errors.update(_validate_user_inputs_namespaced(periods, user_input))
            else:
                errors.update(_validate_user_inputs(decl_list, user_input))

            derived_billing: str | None = None
            if not errors:
                try:
                    derived_billing = _derive_billing(
                        gate_utility, gate_valid_from
                    )
                except NotImplementedError:
                    errors["base"] = "settlement_period_unsupported"
                except (KeyError, LookupError):
                    errors["base"] = "no_active_rate"

            if is_multi:
                period_user_inputs = _split_user_inputs_per_period(periods, user_input)
            else:
                period_user_inputs = [_user_inputs_payload(decl_list, user_input)]

            # O4 defensive check — for multi-period, dry-run each period.
            if not errors:
                for p_idx, (pf, _pt, _rep) in enumerate(periods):
                    if not _find_tier_dry_run(
                        gate_utility, pf.isoformat(), gate_kw, period_user_inputs[p_idx]
                    ):
                        errors["base"] = "no_matching_tier"
                        break

            if not errors:
                hkn_structure = _active_hkn_structure(
                    gate_utility, gate_valid_from
                )
                show_eigenverbrauch = self_consumption_relevant(
                    gate_utility, gate_valid_from, gate_kw
                )
                ev_default = bool(
                    (existing["config"] if existing else {}).get(
                        CONF_EIGENVERBRAUCH_AKTIVIERT, True
                    )
                )
                ev_value = bool(
                    user_input.get(CONF_EIGENVERBRAUCH_AKTIVIERT, ev_default)
                ) if show_eigenverbrauch else ev_default
                hkn_value = _force_hkn_for_save(
                    hkn_structure, user_input.get(CONF_HKN_AKTIVIERT, False)
                )

                # Build N records, one per period.
                new_records: list[dict] = []
                for p_idx, (pf, _pt, _rep) in enumerate(periods):
                    record_valid_from = (
                        gate_valid_from if p_idx == 0 else pf.isoformat()
                    )
                    new_records.append({
                        "valid_from": record_valid_from,
                        "valid_to": None,
                        "config": {
                            CONF_ENERGIEVERSORGER: gate_utility,
                            CONF_INSTALLIERTE_LEISTUNG_KWP: gate_kw,
                            CONF_EIGENVERBRAUCH_AKTIVIERT: ev_value,
                            CONF_HKN_AKTIVIERT: hkn_value,
                            CONF_ABRECHNUNGS_RHYTHMUS: derived_billing,
                            CONF_USER_INPUTS: period_user_inputs[p_idx],
                        },
                    })

                if is_edit:
                    # Replace at idx with the first record; subsequent
                    # records are appended. Normalize de-duplicates by
                    # valid_from at the end.
                    history[idx] = new_records[0]
                    for rec in new_records[1:]:
                        history = _append_history_record(
                            history, rec, dict(self.config_entry.data)
                        )
                else:
                    for rec in new_records:
                        history = _append_history_record(
                            history, rec, dict(self.config_entry.data)
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
                # v0.16.1 — same reload trigger as the delete branch above.
                self.hass.async_create_task(
                    self.hass.config_entries.async_reload(
                        self.config_entry.entry_id
                    )
                )
                return await self.async_step_manage_history()

        # Defaults: prior submission > existing record > open record.
        if user_input is not None:
            defaults_cfg = dict(user_input)
        elif existing is not None:
            defaults_cfg = dict(existing["config"])
        else:
            open_rec = next(
                (r for r in history if r.get("valid_to") is None), None
            )
            defaults_cfg = (
                dict(open_rec["config"]) if open_rec
                else build_history_config(self.config_entry.data)
            )

        hkn_structure = _active_hkn_structure(gate_utility, gate_valid_from)
        show_eigenverbrauch = self_consumption_relevant(
            gate_utility, gate_valid_from, gate_kw
        )
        schema_dict: dict[Any, Any] = {}
        if show_eigenverbrauch:
            schema_dict[
                vol.Required(
                    CONF_EIGENVERBRAUCH_AKTIVIERT,
                    default=bool(
                        defaults_cfg.get(CONF_EIGENVERBRAUCH_AKTIVIERT, True)
                    ),
                )
            ] = selector.BooleanSelector()
        if hkn_structure == "additive_optin":
            schema_dict[
                vol.Required(
                    CONF_HKN_AKTIVIERT,
                    default=bool(defaults_cfg.get(CONF_HKN_AKTIVIERT, False)),
                )
            ] = selector.BooleanSelector()

        decl_defaults = (defaults_cfg.get(CONF_USER_INPUTS) or {})
        lang = (
            getattr(self.hass.config, "language", None) or "de"
        ).split("-")[0].lower()

        if is_multi:
            for p_idx, (pf, _pt, rep_rate) in enumerate(periods):
                period_decls = tuple(rep_rate.get("user_inputs") or ())
                if user_input is not None:
                    period_defaults = {
                        d["key"]: user_input.get(
                            f"{_period_prefix(p_idx)}{d['key']}", d.get("default")
                        )
                        for d in period_decls
                    }
                else:
                    period_defaults = decl_defaults if p_idx == 0 else {}
                _add_user_input_fields_namespaced(
                    schema_dict, period_decls, period_defaults, lang,
                    prefix=_period_prefix(p_idx),
                    gate_utility=gate_utility,
                    gate_valid_from=pf.isoformat(),
                    gate_kw=gate_kw,
                )
            periods_block = _format_periods_block(
                periods,
                _split_user_inputs_per_period(periods, user_input or {}),
                lang,
            )
        else:
            _add_user_input_fields(
                schema_dict, decl_list, decl_defaults, lang,
                gate_utility=gate_utility,
                gate_valid_from=gate_valid_from,
                gate_kw=gate_kw,
            )
            periods_block = ""

        if is_edit:
            schema_dict[vol.Optional("delete", default=False)] = (
                selector.BooleanSelector()
            )

        return self.async_show_form(
            step_id="edit_row" if is_edit else "add_new_row",
            data_schema=vol.Schema(schema_dict),
            errors=errors,
            description_placeholders={
                "utility_name": _utility_display_name(gate_utility),
                "valid_from": gate_valid_from,
                "hkn_gate_note": _hkn_gate_note(hkn_structure, self.hass),
                "notes_block": (
                    "" if is_multi
                    else _notes_block(
                        gate_utility, gate_valid_from, self.hass
                    )
                ),
                "tarif_urls_block": (
                    "" if is_multi
                    else _format_tarif_urls_block(
                        _resolve_tarif_urls(
                            gate_utility, gate_valid_from, decl_defaults
                        ),
                        lang,
                    )
                ),
                "user_inputs_help": (
                    "" if is_multi
                    else _user_inputs_help_block(decl_list, lang)
                ),
                "periods_block": periods_block,
            },
        )

    # ----- Sub-step: recompute full history ----------------------------------

    async def async_step_recompute_history(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
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
            except Exception as exc:
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
    ) -> FlowResult:
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
            except Exception as exc:
                _LOGGER.exception("Refresh data failed")
                async_create(
                    self.hass,
                    f"Refresh failed: {exc}",
                    title=_REFRESH_NOTIFICATION_TITLE,
                    notification_id=f"{DOMAIN}_{self.config_entry.entry_id}_refresh",
                )
                errors["base"] = "reimport_failed"
            else:
                async_create(
                    self.hass,
                    _render_refresh_notification(result),
                    title=_REFRESH_NOTIFICATION_TITLE,
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
    ) -> FlowResult:
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
