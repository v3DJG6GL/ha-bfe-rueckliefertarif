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
# v0.9.1: stable plant identity, used as the entry title and as the basis for
# the namenspraefix default. Decoupled from CONF_ENERGIEVERSORGER so a utility
# switch via the apply_change wizard doesn't change the integration's display
# name or sensor IDs.
CONF_PLANT_NAME = "plant_name"
# v0.9.2: plant install date. Anchors the *first* OPT_CONFIG_HISTORY record's
# valid_from (replaces the artificial 1970-01-01 sentinel). NOT included in
# CONFIG_HISTORY_FIELDS — it's a per-entry constant, not a versioned field.
# Quarters before this date are skipped during _reimport_all_history.
# Same key name as OPT_CONFIG_HISTORY records' valid_from for consistency.
CONF_VALID_FROM = "valid_from"

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
