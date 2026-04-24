"""Utility-specific presets for Swiss Energieversorger.

These are commercial choices that vary per utility (HKN bonus is market-driven;
fixed rates are political/commercial decisions). Values reflect 2026-01-01 state;
user may override in config flow since HKN in particular shifts quarter-by-quarter
at some utilities.

Sources: each utility's own 2026 Rückliefertarif publication (see README).
"""

from __future__ import annotations

from dataclasses import dataclass

from .const import BASE_MODE_FIXED, BASE_MODE_RMP


@dataclass(frozen=True)
class Preset:
    """A named utility preset loaded into config flow defaults."""

    key: str
    display_name: str
    base_mode: str
    hkn_bonus_rp_kwh: float
    fixed_rate_rp_kwh: float | None = None
    note: str = ""


PRESETS: dict[str, Preset] = {
    "ekz": Preset(
        key="ekz",
        display_name="EKZ (Elektrizitätswerke des Kantons Zürich)",
        base_mode=BASE_MODE_RMP,
        hkn_bonus_rp_kwh=3.0,
        note="HKN opt-in. Base = BFE quarterly reference market price.",
    ),
    "bkw": Preset(
        key="bkw",
        display_name="BKW (Berner Kraftwerke)",
        base_mode=BASE_MODE_RMP,
        hkn_bonus_rp_kwh=2.0,
        note="HKN 2.0 Rp/kWh from Q2 2026 for naturemade star-certified only.",
    ),
    "ckw": Preset(
        key="ckw",
        display_name="CKW (Centralschweizerische Kraftwerke)",
        base_mode=BASE_MODE_RMP,
        hkn_bonus_rp_kwh=3.0,
    ),
    "groupe_e": Preset(
        key="groupe_e",
        display_name="Groupe E",
        base_mode=BASE_MODE_RMP,
        hkn_bonus_rp_kwh=4.0,
    ),
    "romande_energie": Preset(
        key="romande_energie",
        display_name="Romande Energie",
        base_mode=BASE_MODE_RMP,
        hkn_bonus_rp_kwh=0.0,
        note="HKN 1.5 Rp/kWh for Q1 2026 only; 0 thereafter.",
    ),
    "sak": Preset(
        key="sak",
        display_name="SAK (St. Galler Stadtwerke)",
        base_mode=BASE_MODE_RMP,
        hkn_bonus_rp_kwh=3.0,
    ),
    "sgsw": Preset(
        key="sgsw",
        display_name="SGSW (St. Gallen Stadt)",
        base_mode=BASE_MODE_RMP,
        hkn_bonus_rp_kwh=3.0,
    ),
    "ewz": Preset(
        key="ewz",
        display_name="ewz (Elektrizitätswerk der Stadt Zürich)",
        base_mode=BASE_MODE_FIXED,
        hkn_bonus_rp_kwh=3.0,
        fixed_rate_rp_kwh=12.91,
        note="Fixed rate (above-market). 2026 HT-average ~12.91 Rp/kWh.",
    ),
    "iwb": Preset(
        key="iwb",
        display_name="IWB (Basel)",
        base_mode=BASE_MODE_FIXED,
        hkn_bonus_rp_kwh=3.0,
        fixed_rate_rp_kwh=14.0,
        note="Basel political fixed rate 14.0 Rp/kWh 2026.",
    ),
    "sig": Preset(
        key="sig",
        display_name="SIG (Services Industriels Genève)",
        base_mode=BASE_MODE_FIXED,
        hkn_bonus_rp_kwh=0.0,
        fixed_rate_rp_kwh=10.96,
        note="Fixed at federal cap 10.96; HKN included.",
    ),
    "aew": Preset(
        key="aew",
        display_name="AEW Energie (Aargau)",
        base_mode=BASE_MODE_FIXED,
        hkn_bonus_rp_kwh=0.0,
        fixed_rate_rp_kwh=8.2,
        note="Einheitstarif 8.2 Rp/kWh HKN-inclusive.",
    ),
    "custom": Preset(
        key="custom",
        display_name="Custom / Other utility",
        base_mode=BASE_MODE_RMP,
        hkn_bonus_rp_kwh=0.0,
        note="Manually configure base_mode, HKN, optional fixed rate.",
    ),
}


def get_preset(key: str) -> Preset:
    """Look up a preset by its key. Raises KeyError if unknown."""
    return PRESETS[key]


def list_preset_keys() -> list[str]:
    """Preset keys in the canonical config-flow order (custom last)."""
    return [k for k in PRESETS if k != "custom"] + ["custom"]
