"""Swiss national tariff law — pure functions, no HA dependencies.

Tables are nationally mandated and identical for every Swiss Netzbetreiber.
User-configurable inputs are limited to HKN bonus (utility commercial choice)
and the optional fixed rate (for utilities that pay above the BFE reference).

Legal references (all in force since 1.1.2026 via the Mantelerlass / Stromgesetz):
- EnG Art. 15 Abs. 1 + EnFV Art. 15:
    Basisvergütung = BFE Referenz-Marktpreis (quartalsweise publiziert).
- EnG Art. 15 Abs. 1bis + EnV Art. 12 Abs. 1bis Bst. a/b/c/d (SR 730.01, AS 2025 138):
    Mindestvergütung floor — 6.00 / 6.20 / 180/kW / 12.00 Rp/kWh.
- StromVV Art. 4 Abs. 3 Bst. e i.V.m. EnG Art. 15 Abs. 1 (SR 734.71, AS 2025 139):
    Anrechenbarkeitsgrenze cap — formula: max remuneration = Gestehungskosten of
    the reference plant minus subsidies. The four cap values used here (10.96 /
    8.20 / 7.20 / 5.40) are EKZ's published derivation; other utilities applying
    the cap typically arrive at similar numbers because the reference Gestehungskosten
    are nationally uniform.
"""

from __future__ import annotations

from enum import Enum


class Segment(str, Enum):
    """8 PV segments defined by national law (size × Eigenverbrauch)."""

    SMALL_MIT_EV = "small_mit_ev"      # ≤30 kW mit Eigenverbrauch
    SMALL_OHNE_EV = "small_ohne_ev"    # ≤30 kW ohne Eigenverbrauch (Volleinspeisung)
    MID_MIT_EV = "mid_mit_ev"          # 30–<100 kW mit Eigenverbrauch
    MID_OHNE_EV = "mid_ohne_ev"        # 30–<100 kW ohne Eigenverbrauch
    LARGE_MIT_EV = "large_mit_ev"      # 100–<150 kW mit Eigenverbrauch
    LARGE_OHNE_EV = "large_ohne_ev"    # 100–<150 kW ohne Eigenverbrauch
    XL_MIT_EV = "xl_mit_ev"            # ≥150 kW mit Eigenverbrauch
    XL_OHNE_EV = "xl_ohne_ev"          # ≥150 kW ohne Eigenverbrauch


_MIT_EV = {Segment.SMALL_MIT_EV, Segment.MID_MIT_EV, Segment.LARGE_MIT_EV, Segment.XL_MIT_EV}
_DEGRESSIVE = {Segment.MID_MIT_EV, Segment.LARGE_MIT_EV}


def has_eigenverbrauch(seg: Segment) -> bool:
    return seg in _MIT_EV


def mindestverguetung_rp_kwh(seg: Segment, kw: float) -> float | None:
    """Federal Mindestvergütung floor in Rp/kWh.

    Source: EnV Art. 12 Abs. 1bis Bst. a/b/c/d (SR 730.01, AS 2025 138,
    in force since 1.1.2026). Pre-2026 there was no federal Rp/kWh floor —
    Art. 12 Abs. 1 just required "avoided procurement cost".

    Returns None for ≥150 kW segments (no federal floor).
    For mit-Eigenverbrauch 30–<150 kW: degressive formula 180/kW (range 1.20–6.00).
    For ohne-Eigenverbrauch 30–<150 kW: flat 6.20.
    For ≤30 kW: flat 6.00.
    """
    if seg in (Segment.XL_MIT_EV, Segment.XL_OHNE_EV):
        return None
    if seg in (Segment.SMALL_MIT_EV, Segment.SMALL_OHNE_EV):
        return 6.00
    if seg in _DEGRESSIVE:
        if kw <= 0:
            raise ValueError("kW must be positive for degressive formula")
        return round(180.0 / kw, 4)
    # ohne Eigenverbrauch, 30–<150 kW
    return 6.20


def anrechenbarkeitsgrenze_rp_kwh(seg: Segment) -> float:
    """Anrechenbarkeitsgrenze cap in Rp/kWh — 4 tiers by 100 kW × Eigenverbrauch.

    Legal source: StromVV Art. 4 Abs. 3 Bst. e (SR 734.71, AS 2025 139,
    in force since 1.1.2026). The article mandates a *formula* — the cap on
    what utilities may charge captive customers for procured PV equals the
    Gestehungskosten of a reference plant minus subsidies. The four numbers
    here (10.96 / 8.20 / 7.20 / 5.40) are EKZ's published derivation per
    plant-size×Eigenverbrauch quadrant; other Swiss utilities applying this
    cap converge on similar values because the reference Gestehungskosten
    are nationally uniform.
    """
    if has_eigenverbrauch(seg):
        if seg == Segment.SMALL_MIT_EV or seg == Segment.MID_MIT_EV:
            return 10.96
        return 7.20  # LARGE_MIT_EV, XL_MIT_EV
    # ohne Eigenverbrauch
    if seg == Segment.SMALL_OHNE_EV or seg == Segment.MID_OHNE_EV:
        return 8.20
    return 5.40  # LARGE_OHNE_EV, XL_OHNE_EV


def effective_rp_kwh(
    base_input_rp_kwh: float,
    seg: Segment,
    kw: float,
    hkn_verguetung_rp_kwh: float = 0.0,
    *,
    verguetungs_obergrenze: bool = False,
) -> float:
    """Effective Rückliefervergütung in Rp/kWh.

    Federal floor (Mindestvergütung, EnV Art. 12 Abs. 1bis) is always applied
    to the base. Whether the Anrechenbarkeitsgrenze (StromVV Art. 4 Abs. 3
    Bst. e) acts as a payment cap is a per-utility commercial choice — most
    Swiss utilities pay base + HKN additively without enforcing the cap; only
    EKZ, Groupe E, and Primeo apply it strictly per their published 2026 terms.

    When ``verguetungs_obergrenze`` is True, the EKZ-style two-clause cap rule
    applies:
    - If base alone already meets/exceeds the cap → HKN forfeited entirely.
    - Otherwise → HKN reduced just enough to keep base + HKN ≤ cap.
    """
    floor = mindestverguetung_rp_kwh(seg, kw) or 0.0
    base = max(base_input_rp_kwh, floor)

    if not verguetungs_obergrenze:
        return base + hkn_verguetung_rp_kwh

    cap = anrechenbarkeitsgrenze_rp_kwh(seg)
    if base >= cap:
        return base
    return min(base + hkn_verguetung_rp_kwh, cap)


def chf_per_mwh_to_rp_per_kwh(chf_per_mwh: float) -> float:
    """BFE publishes in CHF/MWh; HA dashboard thinks in Rp/kWh."""
    return chf_per_mwh / 10.0


def rp_per_kwh_to_chf_per_kwh(rp_per_kwh: float) -> float:
    return rp_per_kwh / 100.0
