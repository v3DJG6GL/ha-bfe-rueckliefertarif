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

# entry.options keys for per-customer history (Phase 4). Both are lists of
# half-open `[valid_from, valid_to)` records — same shape as tariffs.json's
# date-versioned records, looked up via `tariffs_db.find_active`.
OPT_PLANT_HISTORY = "plant_history"
OPT_HKN_OPTIN_HISTORY = "hkn_optin_history"
