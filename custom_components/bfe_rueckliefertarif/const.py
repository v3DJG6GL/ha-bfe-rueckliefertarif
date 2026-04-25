"""Constants for the BFE Rückliefertarif integration."""

from __future__ import annotations

DOMAIN = "bfe_rueckliefertarif"

BFE_QUARTALSPREISE_URL = "https://www.bfe-ogd.ch/ogd60_rmp_quartalspreise.csv"
BFE_MONATSPREISE_URL = "https://www.bfe-ogd.ch/ogd60_rmp_monatspreise.csv"

TIMEZONE = "Europe/Zurich"

# Config-entry keys (v3 — German, matching DE labels exactly)
CONF_ENERGIEVERSORGER = "energieversorger"
CONF_ANLAGENKATEGORIE = "anlagenkategorie"
CONF_INSTALLIERTE_LEISTUNG_KW = "installierte_leistung_kw"
CONF_BASISVERGUETUNG = "basisverguetung"
CONF_HKN_VERGUETUNG_RP_KWH = "hkn_verguetung_rp_kwh"
CONF_FIXPREIS_RP_KWH = "fixpreis_rp_kwh"
CONF_ABRECHNUNGS_RHYTHMUS = "abrechnungs_rhythmus"
CONF_VERGUETUNGS_OBERGRENZE = "verguetungs_obergrenze"
CONF_STROMNETZEINSPEISUNG_KWH = "stromnetzeinspeisung_kwh"
CONF_RUECKLIEFERVERGUETUNG_CHF = "rueckliefervergutung_chf"
CONF_NAMENSPRAEFIX = "namenspraefix"

# Basisvergütung values
BASISVERGUETUNG_REFERENZMARKTPREIS = "referenz_marktpreis"
BASISVERGUETUNG_FIXPREIS = "fixpreis"

# Legacy "base_mode" values used inside `presets.py` (kept stable so the preset
# file doesn't need to know about the v2 vocabulary). The config flow translates
# these to BASISVERGUETUNG_* via `_PRESET_LEGACY_TO_NEW` when seeding defaults.
BASE_MODE_RMP = "rmp_passthrough"
BASE_MODE_FIXED = "fixed_rate"

# Abrechnungs-Rhythmus values
ABRECHNUNGS_RHYTHMUS_QUARTAL = "quartal"
ABRECHNUNGS_RHYTHMUS_MONAT = "monat"

# v1 → v2 migration map (key renames)
_V1_TO_V2_KEY_MAP: dict[str, str] = {
    "preset": CONF_ENERGIEVERSORGER,
    "segment": CONF_ANLAGENKATEGORIE,
    "kw": CONF_INSTALLIERTE_LEISTUNG_KW,
    "base_mode": CONF_BASISVERGUETUNG,
    "hkn_bonus_rp_kwh": CONF_HKN_VERGUETUNG_RP_KWH,
    "fixed_rate_rp_kwh": CONF_FIXPREIS_RP_KWH,
    "billing_mode": CONF_ABRECHNUNGS_RHYTHMUS,
    "export_entity": CONF_STROMNETZEINSPEISUNG_KWH,
    "compensation_entity": CONF_RUECKLIEFERVERGUETUNG_CHF,
    "entity_prefix": CONF_NAMENSPRAEFIX,
}

# v1 → v2 migration map (value renames per field)
_V1_TO_V2_VALUE_MAP: dict[str, dict[str, str]] = {
    CONF_BASISVERGUETUNG: {
        "rmp_passthrough": BASISVERGUETUNG_REFERENZMARKTPREIS,
        "fixed_rate": BASISVERGUETUNG_FIXPREIS,
    },
    CONF_ABRECHNUNGS_RHYTHMUS: {
        "quarterly": ABRECHNUNGS_RHYTHMUS_QUARTAL,
        "monthly": ABRECHNUNGS_RHYTHMUS_MONAT,
    },
}
