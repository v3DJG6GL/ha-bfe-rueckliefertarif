"""External tariff database — JSON loader + lookup helpers.

The bundled `data/tariffs.json` is the v0.5 source-of-truth; Phase 6 layers a
companion-repo runtime fetch on top with the same shape. All math in
tariff.py / importer.py / coordinator.py / services.py flows through the
helpers here so adding a utility or adjusting a federal floor is a JSON-only
change.

Key shapes:
- `find_active(records, at_date)` — half-open `[valid_from, valid_to)` lookup.
  Used for federal_minimum, utility rates, plant_history, hkn_optin_history.
  Boundary day belongs to the *new* record (if at_date == valid_from of
  record N+1, return record N+1).
- `find_rule(rules, kw, eigenverbrauch)` — first record whose kw band and
  EV bool match. Used for federal_minimum.rules and utility cap_rules.
- `find_tier(tiers, kw)` — kW-band lookup for power_tiers (no EV dimension).
- `resolve_tariff_at(...)` — composes the above into a `ResolvedTariff`
  with everything an importer needs to compute one quarter's effective rate.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Any

_BUNDLED_DATA_PATH = Path(__file__).parent / "data" / "tariffs.json"

# Phase 6: data_coordinator may set an override path pointing at the
# .storage/-cached remote tariffs.json. None = bundled.
_OVERRIDE_PATH: Path | None = None


@dataclass(frozen=True)
class ResolvedTariff:
    """What every consumer (importer, coordinator, services) needs to compute
    a single quarter's effective rate. Returned by `resolve_tariff_at`."""

    utility_key: str
    valid_from: str                 # ISO date of the active rate window
    settlement_period: str          # "quartal" | "monat" | "stunde"
    base_model: str                 # "fixed_flat" | "fixed_ht_nt" | "rmp_quartal" | "rmp_monat"

    fixed_rp_kwh: float | None      # populated when base_model = fixed_flat
    fixed_ht_rp_kwh: float | None
    fixed_nt_rp_kwh: float | None

    hkn_rp_kwh: float
    hkn_structure: str              # "additive_optin" | "bundled" | "none"

    cap_mode: bool
    cap_rp_kwh: float | None        # picked from cap_rules by (kw, ev); None if no cap

    federal_floor_rp_kwh: float | None
    federal_floor_label: str        # runtime-rendered: "<30 kW", "≥150 kW", etc.

    requires_naturemade_star: bool
    price_floor_rp_kwh: float | None

    tariffs_json_version: str
    tariffs_json_source: str        # "remote" | "bundled" — Phase 6 fills "remote"

    # HT/NT day-of-week + hour windows for fixed_ht_nt utilities. Shape:
    # {"mofr": [start_h, end_h] | None, "sa": ..., "su": ...}. None for any
    # day type means all-NT for that day. None on the field means "no HT/NT
    # structure" (any base_model other than fixed_ht_nt).
    ht_window: dict | None = None


# ----- Loader ---------------------------------------------------------------


def load_tariffs(path: Path | str | None = None) -> dict[str, Any]:
    """Read the active tariffs.json and return as dict.

    Resolution order:
    1. Explicit ``path`` argument (used by tests).
    2. ``_OVERRIDE_PATH`` set by ``data_coordinator`` when a fresh remote
       fetch has succeeded.
    3. Bundled ``data/tariffs.json``.

    Cached on (path, mtime) so a remote refresh — which rewrites the
    cache file — invalidates automatically.
    """
    if path is not None:
        p = Path(path)
    elif _OVERRIDE_PATH is not None and _OVERRIDE_PATH.is_file():
        p = _OVERRIDE_PATH
    else:
        p = _BUNDLED_DATA_PATH
    return _load_cached(str(p), p.stat().st_mtime_ns)


def set_override_path(path: Path | str | None) -> None:
    """Set or clear the override path for ``load_tariffs``.

    Called by ``data_coordinator`` after a successful remote fetch. Pass
    ``None`` to revert to the bundled fallback.
    """
    global _OVERRIDE_PATH
    _OVERRIDE_PATH = Path(path) if path is not None else None
    _load_cached.cache_clear()


def get_source() -> str:
    """Return ``"remote"`` if ``_OVERRIDE_PATH`` is set, else ``"bundled"``."""
    return "remote" if _OVERRIDE_PATH is not None else "bundled"


@lru_cache(maxsize=4)
def _load_cached(path_str: str, _mtime_ns: int) -> dict[str, Any]:
    with open(path_str, encoding="utf-8") as f:
        return json.load(f)


# ----- Half-open / rule / tier lookups (the three fundamentals) -------------


def find_active(records: list[dict], at_date: date) -> dict | None:
    """Half-open interval lookup `[valid_from, valid_to)`.

    Boundary day belongs to the *new* record: if at_date == valid_from of
    record N+1, return record N+1 (not record N). Used for every list of
    date-versioned records: federal_minimum, utility rates, plant_history,
    hkn_optin_history.
    """
    for r in records:
        f = date.fromisoformat(r["valid_from"])
        t = date.fromisoformat(r["valid_to"]) if r.get("valid_to") else date.max
        if f <= at_date < t:
            return r
    return None


def find_rule(rules: list[dict], kw: float, self_consumption: bool) -> dict | None:
    """Match (kW, EV bool) against a list of rule records.

    Used for `federal_minimum.rules` and utility `cap_rules`. The first
    matching record wins. `self_consumption: null` in a record means
    "applies regardless of EV state".
    """
    for r in rules:
        kw_max = r["kw_max"] if r["kw_max"] is not None else float("inf")
        kw_ok = r["kw_min"] <= kw < kw_max
        rule_ev = r["self_consumption"]
        ev_ok = rule_ev is None or rule_ev == self_consumption
        if kw_ok and ev_ok:
            return r
    return None


def find_tier(tiers: list[dict], kw: float) -> dict | None:
    """kW-range lookup for power_tiers (no EV dimension)."""
    for t in tiers:
        kw_max = t["kw_max"] if t["kw_max"] is not None else float("inf")
        if t["kw_min"] <= kw < kw_max:
            return t
    return None


# ----- Federal-floor evaluation --------------------------------------------


def evaluate_federal_floor(rule: dict, kw: float) -> float | None:
    """Apply a federal-minimum rule to a specific kW value.

    Flat rules return `min_rp_kwh` directly. Degressive rules (formula:
    "180/kw") evaluate the formula at the given kW. Returns None for
    "no federal floor" rules (e.g. ≥150 kW).
    """
    if rule.get("formula") == "180/kw":
        if kw <= 0:
            raise ValueError("kW must be positive for the 180/kw degressive formula")
        return round(180.0 / kw, 4)
    val = rule.get("min_rp_kwh")
    return float(val) if val is not None else None


def floor_label(rule: dict) -> str:
    """Human-readable bucket label rendered from a federal-minimum rule.

    Output in DE (the integration's default; FR/EN translation is a v0.6
    polish):
      "<30 kW"
      "30–<150 kW mit Eigenverbrauch"
      "30–<150 kW ohne Eigenverbrauch"
      "≥150 kW"
    """
    kw_min, kw_max = rule["kw_min"], rule["kw_max"]
    if kw_min == 0 and kw_max is not None:
        kw_text = f"<{_fmt_kw(kw_max)} kW"
    elif kw_max is None:
        kw_text = f"≥{_fmt_kw(kw_min)} kW"
    else:
        kw_text = f"{_fmt_kw(kw_min)}–<{_fmt_kw(kw_max)} kW"

    ev = rule.get("self_consumption")
    if ev is True:
        ev_text = " mit Eigenverbrauch"
    elif ev is False:
        ev_text = " ohne Eigenverbrauch"
    else:
        ev_text = ""

    return kw_text + ev_text


def _fmt_kw(v: float) -> str:
    """Strip pointless ``.0`` so "30 kW" not "30.0 kW"."""
    if isinstance(v, int) or v == int(v):
        return str(int(v))
    return f"{v:g}"


# ----- One-call resolver used by importer/coordinator/services -------------


def resolve_tariff_at(
    utility_key: str,
    at_date: date,
    kw: float,
    eigenverbrauch: bool,
    *,
    data: dict[str, Any] | None = None,
    source: str | None = None,
) -> ResolvedTariff:
    """One-call resolver: utility rate window → power tier → caps + floor.

    Composes `find_active` (utility rate window) → `find_tier` (kW band) →
    `find_rule` (cap_rules + federal_minimum.rules) into a single
    ``ResolvedTariff`` carrying everything an importer needs.
    """
    db = data if data is not None else load_tariffs()

    utility = db["utilities"].get(utility_key)
    if utility is None:
        raise KeyError(f"unknown utility {utility_key!r}")

    rate = find_active(utility["rates"], at_date)
    if rate is None:
        raise LookupError(
            f"no active rate for {utility_key!r} on {at_date.isoformat()}"
        )

    tier = find_tier(rate["power_tiers"], kw)
    if tier is None:
        raise LookupError(
            f"{utility_key!r} has no power_tier covering {kw} kW "
            f"(rate window starting {rate['valid_from']})"
        )

    cap_rp_kwh: float | None = None
    if rate.get("cap_mode") and rate.get("cap_rules"):
        cap_rule = find_rule(rate["cap_rules"], kw, eigenverbrauch)
        if cap_rule is not None:
            cap_rp_kwh = float(cap_rule["cap_rp_kwh"])

    fed_record = find_active(db["federal_minimum"], at_date)
    federal_floor: float | None = None
    floor_lbl = ""
    if fed_record is not None:
        fed_rule = find_rule(fed_record["rules"], kw, eigenverbrauch)
        if fed_rule is not None:
            federal_floor = evaluate_federal_floor(fed_rule, kw)
            floor_lbl = floor_label(fed_rule)

    return ResolvedTariff(
        utility_key=utility_key,
        valid_from=rate["valid_from"],
        settlement_period=rate["settlement_period"],
        base_model=tier["base_model"],
        fixed_rp_kwh=tier.get("fixed_rp_kwh"),
        fixed_ht_rp_kwh=tier.get("fixed_ht_rp_kwh"),
        fixed_nt_rp_kwh=tier.get("fixed_nt_rp_kwh"),
        hkn_rp_kwh=float(tier["hkn_rp_kwh"]),
        hkn_structure=tier["hkn_structure"],
        cap_mode=bool(rate.get("cap_mode", False)),
        cap_rp_kwh=cap_rp_kwh,
        federal_floor_rp_kwh=federal_floor,
        federal_floor_label=floor_lbl,
        requires_naturemade_star=bool(rate.get("requires_naturemade_star", False)),
        price_floor_rp_kwh=rate.get("price_floor_rp_kwh"),
        tariffs_json_version=str(db["schema_version"]),
        tariffs_json_source=source if source is not None else get_source(),
        ht_window=tier.get("ht_window"),
    )


def list_utility_keys(data: dict[str, Any] | None = None) -> list[str]:
    """All known utility keys in the order they appear in tariffs.json."""
    db = data if data is not None else load_tariffs()
    return list(db["utilities"].keys())
