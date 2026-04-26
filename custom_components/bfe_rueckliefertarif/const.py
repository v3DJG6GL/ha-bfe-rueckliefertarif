"""Constants for the BFE Rückliefertarif integration."""

from __future__ import annotations

DOMAIN = "bfe_rueckliefertarif"

BFE_QUARTALSPREISE_URL = "https://www.bfe-ogd.ch/ogd60_rmp_quartalspreise.csv"
BFE_MONATSPREISE_URL = "https://www.bfe-ogd.ch/ogd60_rmp_monatspreise.csv"

TIMEZONE = "Europe/Zurich"

# Config-entry keys (v0.5 — German labels match what's shown in the UI).
# The legacy v0.4 keys (anlagenkategorie / basisverguetung / fixpreis_rp_kwh /
# verguetungs_obergrenze) are retired in v0.5: utility-published values come
# straight from data/tariffs.json now, with the user only providing personal
# inputs (kW, Eigenverbrauch yes/no, HKN opt-in yes/no).
CONF_ENERGIEVERSORGER = "energieversorger"
CONF_INSTALLIERTE_LEISTUNG_KW = "installierte_leistung_kw"
CONF_EIGENVERBRAUCH_AKTIVIERT = "eigenverbrauch_aktiviert"
CONF_HKN_AKTIVIERT = "hkn_aktiviert"
CONF_ABRECHNUNGS_RHYTHMUS = "abrechnungs_rhythmus"
CONF_STROMNETZEINSPEISUNG_KWH = "stromnetzeinspeisung_kwh"
CONF_RUECKLIEFERVERGUETUNG_CHF = "rueckliefervergutung_chf"
CONF_NAMENSPRAEFIX = "namenspraefix"

# Abrechnungs-Rhythmus values
ABRECHNUNGS_RHYTHMUS_QUARTAL = "quartal"
ABRECHNUNGS_RHYTHMUS_MONAT = "monat"

# entry.options key for the unified per-entry config timeline (v0.8.0).
# A list of half-open `[valid_from, valid_to)` records, each carrying a full
# snapshot of the rate-affecting inputs. Looked up via `tariffs_db.find_active`.
# Replaces the v0.7-era separate OPT_PLANT_HISTORY / OPT_HKN_OPTIN_HISTORY
# lists (clean break — no migration of legacy data).
OPT_CONFIG_HISTORY = "config_history"

# Single source of truth for which config keys are time-versioned. Anything
# in this tuple is stored in each history record's "config" sub-dict.
CONFIG_HISTORY_FIELDS = (
    CONF_ENERGIEVERSORGER,
    CONF_INSTALLIERTE_LEISTUNG_KW,
    CONF_EIGENVERBRAUCH_AKTIVIERT,
    CONF_HKN_AKTIVIERT,
    CONF_ABRECHNUNGS_RHYTHMUS,
)
