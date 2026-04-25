"""Utility-specific presets for Swiss Energieversorger.

These are commercial choices that vary per utility (HKN-Vergütung is market-driven;
fixed prices are political/commercial decisions). Values reflect 2026-01-01 state;
user may override in config flow since HKN in particular shifts quarter-by-quarter
at some utilities.

The ``verguetungs_obergrenze`` flag captures whether the utility applies the
Anrechenbarkeitsgrenze (StromVV Art. 4) as a hard payment cap:

- True  → EKZ, Groupe E, Primeo. Total Vergütung never exceeds the cap; HKN is
          reduced or forfeited if base + HKN would exceed it.
- False → all other Swiss utilities. Producer receives base + HKN additively.

Sources: each utility's own 2026 Rückliefertarif publication (see README); the
True/False classification is based on the 2026-04 VESE pvtarif sweep + direct
verification at each utility's website.
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
    verguetungs_obergrenze: bool
    fixed_rate_rp_kwh: float | None = None
    note: str = ""


PRESETS: dict[str, Preset] = {
    "ekz": Preset(
        key="ekz",
        display_name="EKZ (Elektrizitätswerke des Kantons Zürich)",
        base_mode=BASE_MODE_RMP,
        hkn_bonus_rp_kwh=3.0,
        verguetungs_obergrenze=True,
        note="HKN opt-in. Base = BFE quarterly reference market price. "
             "Anrechenbarkeitsgrenze enforced — HKN reduced if base + HKN > cap.",
    ),
    "bkw": Preset(
        key="bkw",
        display_name="BKW (Berner Kraftwerke)",
        base_mode=BASE_MODE_RMP,
        hkn_bonus_rp_kwh=2.0,
        verguetungs_obergrenze=False,
        note="HKN 2.0 Rp/kWh from Q2 2026 for naturemade star-certified only. "
             "Federal Mindestvergütung floor for ≤30/<150 kW; no payment cap.",
    ),
    "ckw": Preset(
        key="ckw",
        display_name="CKW (Centralschweizerische Kraftwerke)",
        base_mode=BASE_MODE_RMP,
        hkn_bonus_rp_kwh=3.0,
        verguetungs_obergrenze=False,
    ),
    "groupe_e": Preset(
        key="groupe_e",
        display_name="Groupe E",
        base_mode=BASE_MODE_RMP,
        hkn_bonus_rp_kwh=4.0,
        verguetungs_obergrenze=True,
        note="Plafonnement du prix global per art. 4 al. 3 OApEl: max 10.96 ct/kWh "
             "≤100 kW (7.2 ct/kWh ≥100 kW) per quarter.",
    ),
    "primeo": Preset(
        key="primeo",
        display_name="Primeo Energie",
        base_mode=BASE_MODE_RMP,
        hkn_bonus_rp_kwh=2.0,
        verguetungs_obergrenze=True,
        note="HKN 2.0 Rp/kWh as Maximalwert; reduced when total Vergütung approaches cap.",
    ),
    "romande_energie": Preset(
        key="romande_energie",
        display_name="Romande Energie",
        base_mode=BASE_MODE_RMP,
        hkn_bonus_rp_kwh=1.5,
        verguetungs_obergrenze=False,
        note="GO 1.5 Rp/kWh paid additively (no cap). VESE explText for 2026 was "
             "stale — utility's own page confirms GO purchase.",
    ),
    "sak": Preset(
        key="sak",
        display_name="SAK (St. Galler Stadtwerke)",
        base_mode=BASE_MODE_RMP,
        hkn_bonus_rp_kwh=1.5,
        verguetungs_obergrenze=False,
    ),
    "sgsw": Preset(
        key="sgsw",
        display_name="SGSW (St. Gallen Stadt)",
        base_mode=BASE_MODE_RMP,
        hkn_bonus_rp_kwh=4.6,
        verguetungs_obergrenze=False,
        note="HKN 4.6 Rp/kWh paid additively (Zusätzlich) plus 2 Rp/kWh flexibility bonus.",
    ),
    "ewz": Preset(
        key="ewz",
        display_name="ewz (Elektrizitätswerk der Stadt Zürich)",
        base_mode=BASE_MODE_FIXED,
        hkn_bonus_rp_kwh=5.0,
        verguetungs_obergrenze=False,
        fixed_rate_rp_kwh=7.91,
        note="Fixed base 7.91 + HKN 5.0 paid additively. Förderbeitrag "
             "(2 Rp/kWh) sits inside the HKN-Vergütung.",
    ),
    "iwb": Preset(
        key="iwb",
        display_name="IWB (Basel)",
        base_mode=BASE_MODE_FIXED,
        hkn_bonus_rp_kwh=0.0,
        verguetungs_obergrenze=False,
        fixed_rate_rp_kwh=12.95,
        note="Cantonal Anhang 12 — flat 12.95 Rp/kWh including HKN abgabe. "
             "12-year guarantee. No Anrechenbarkeitsgrenze enforcement.",
    ),
    "sig": Preset(
        key="sig",
        display_name="SIG (Services Industriels Genève)",
        base_mode=BASE_MODE_FIXED,
        hkn_bonus_rp_kwh=2.8,
        verguetungs_obergrenze=False,
        fixed_rate_rp_kwh=8.16,
        note="Base 8.16 + HKN 2.8 additive. Higher rate for plants without "
             "Eigenverbrauch >100 kW (separate config).",
    ),
    "aew": Preset(
        key="aew",
        display_name="AEW Energie (Aargau)",
        base_mode=BASE_MODE_FIXED,
        hkn_bonus_rp_kwh=0.0,
        verguetungs_obergrenze=False,
        fixed_rate_rp_kwh=8.2,
        note="Einheitstarif 8.2 Rp/kWh inklusive HKN (≤30 kW). ≥30 kW: "
             "BFE-Marktpreis ohne HKN.",
    ),
    "custom": Preset(
        key="custom",
        display_name="Custom / Other utility",
        base_mode=BASE_MODE_RMP,
        hkn_bonus_rp_kwh=0.0,
        verguetungs_obergrenze=False,
        note="Manually configure base mode, HKN, optional fixed price, "
             "Vergütungs-Obergrenze. Default: no cap (matches the majority of CH utilities).",
    ),
}


def get_preset(key: str) -> Preset:
    """Look up a preset by its key. Raises KeyError if unknown."""
    return PRESETS[key]


def list_preset_keys() -> list[str]:
    """Preset keys in the canonical config-flow order (custom last)."""
    return [k for k in PRESETS if k != "custom"] + ["custom"]
