"""Constants for the BFE Rückliefertarif integration."""

from __future__ import annotations

DOMAIN = "bfe_rueckliefertarif"

BFE_QUARTALSPREISE_URL = "https://www.bfe-ogd.ch/ogd60_rmp_quartalspreise.csv"
BFE_MONATSPREISE_URL = "https://www.bfe-ogd.ch/ogd60_rmp_monatspreise.csv"

TIMEZONE = "Europe/Zurich"

CONF_PRESET = "preset"
CONF_SEGMENT = "segment"
CONF_KW = "kw"
CONF_BASE_MODE = "base_mode"
CONF_HKN_BONUS = "hkn_bonus_rp_kwh"
CONF_FIXED_RATE = "fixed_rate_rp_kwh"
CONF_BILLING_MODE = "billing_mode"
CONF_EXPORT_ENTITY = "export_entity"
CONF_COMPENSATION_ENTITY = "compensation_entity"
CONF_ENTITY_PREFIX = "entity_prefix"

BASE_MODE_RMP = "rmp_passthrough"
BASE_MODE_FIXED = "fixed_rate"

BILLING_MODE_QUARTERLY = "quarterly"
BILLING_MODE_MONTHLY = "monthly"
