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

    price_floor_rp_kwh: float | None

    tariffs_json_version: str
    tariffs_json_source: str        # "remote" | "bundled" — Phase 6 fills "remote"

    # HT/NT day-of-week + hour windows for fixed_ht_nt utilities. Shape:
    # {"mofr": [start_h, end_h] | None, "sa": ..., "su": ...}. None for any
    # day type means all-NT for that day. None on the field means "no HT/NT
    # structure" (any base_model other than fixed_ht_nt).
    ht_window: dict | None = None

    # Seasonal (summer/winter) override for the rate window. Shape:
    # {"summer_months": [...], "winter_months": [...],
    #  "summer_rp_kwh": x, "winter_rp_kwh": y,           # for fixed_flat
    #  "summer_ht_rp_kwh"/"summer_nt_rp_kwh"/...}        # for fixed_ht_nt
    # None means no seasonal overlay; tier rates apply year-round.
    seasonal: dict | None = None

    # v0.9.9 — rate-window-level notes filtered to ``at_date``. Each entry:
    # ``{"valid_from": iso|None, "valid_to": iso|None, "severity":
    # "info"|"warning"|"error", "text": {lang: str}}``. ``None`` when the
    # rate window has no notes; empty list when notes exist but are all
    # outside the ``at_date`` window.
    notes: tuple[dict, ...] | None = None

    # v0.10.0 — rate-window-level bonuses. Each entry carries
    # ``{"kind": "additive_rp_kwh"|"multiplier_pct", "name": str,
    # "applies_when": "always"|"opt_in"}`` plus ``rate_rp_kwh`` (additive)
    # or ``multiplier_pct`` (multiplier), and may include ``"note"`` and
    # ``"when"`` (a strict when_clause). v0.11.0 (Batch D) evaluates the
    # ``when`` clause + ``kind`` per hour. ``None`` for "nothing to show" —
    # both missing key and empty array collapse to ``None``.
    bonuses: tuple[dict, ...] | None = None

    # v0.11.0 (Batch D) — declarations of user-supplied toggles for the
    # active rate window. Each entry follows the schema's ``user_input``
    # shape: ``{"key": str, "type": "enum"|"boolean", "default": str|bool,
    # "label_de": str, ...}``. Used by config_flow for dynamic form
    # rendering and by the per-hour resolver to default any user choice
    # that wasn't supplied. ``None`` when the rate window declares nothing.
    user_inputs_decl: tuple[dict, ...] | None = None

    # v0.11.0 (Batch D) — power-tier-level conditional HKN overrides.
    # Each entry carries ``{"when": when_clause, "rp_kwh": float,
    # "note"?: str}``. First match wins per hour; falls through to the
    # static ``hkn_rp_kwh`` field. ``None`` when the matched tier
    # declares no cases.
    hkn_cases: tuple[dict, ...] | None = None

    # v0.11.0 (Batch D) — the matched tier's ``applies_when`` clause,
    # retained raw for downstream introspection (e.g. notification
    # config-block "active because: tariff_model=ht"). ``None`` when the
    # matched tier has no clause (the unconditional default tier).
    tier_applies_when: dict | None = None


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


def find_tier_for(
    tiers: list[dict], kw: float, user_inputs: dict | None
) -> dict | None:
    """kW-range lookup that also filters by ``applies_when``.

    Among tiers covering ``kw`` (kw_min ≤ kw < kw_max), prefers one whose
    ``applies_when`` clause matches every key in ``user_inputs``. Tiers
    without an ``applies_when`` clause act as the unconditional fallback —
    they match any user_inputs but lose to a more-specific clause-match.

    Match semantics for ``applies_when``: every key in the clause must be
    present in ``user_inputs`` with the same scalar value. Missing keys =
    no match. ``user_inputs=None`` is treated as an empty dict.

    Returns the most-specific match (clause-match beats no-clause); falls
    back to the no-clause tier; returns None only if no tier covers ``kw``.
    """
    ui = user_inputs or {}
    fallback: dict | None = None
    for t in tiers:
        kw_max = t["kw_max"] if t["kw_max"] is not None else float("inf")
        if not (t["kw_min"] <= kw < kw_max):
            continue
        clause = t.get("applies_when")
        if clause:
            if all(ui.get(k) == v for k, v in clause.items()):
                return t
        else:
            # First unconditional tier in this kw band wins as fallback.
            if fallback is None:
                fallback = t
    return fallback


def evaluate_when(
    clause: dict, *, season: str | None, user_inputs: dict | None
) -> bool:
    """Evaluate a strict ``when_clause`` against an hour's context.

    Schema vocabulary (v1.1.0): ``season`` ∈ {"summer", "winter"} and/or
    ``user_inputs`` (a sub-dict of key→scalar). All keys present in the
    clause must match (logical AND); missing keys in the runtime context
    count as no-match.

    Returns True iff every key in ``clause`` matches the runtime context.
    Raises ValueError on unknown clause keys (the schema's
    ``additionalProperties: false`` already rejects them at validation
    time, but loud failure here helps if a future schema bump slips
    through without integration support).
    """
    ui = user_inputs or {}
    for key, expected in clause.items():
        if key == "season":
            if season != expected:
                return False
        elif key == "user_inputs":
            if not isinstance(expected, dict):
                return False
            for sub_k, sub_v in expected.items():
                if ui.get(sub_k) != sub_v:
                    return False
        else:
            raise ValueError(
                f"unknown when_clause key {key!r} — schema is at "
                f"v1.1.0 (season + user_inputs only)"
            )
    return True


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


def _note_active_at(note: dict, at_date: date) -> bool:
    """Half-open ``[valid_from, valid_to)`` for a single note. Missing
    bounds default to ±∞, so a note without dates is always active for the
    rate window it lives in.
    """
    f_str = note.get("valid_from")
    t_str = note.get("valid_to")
    f = date.fromisoformat(f_str) if f_str else date.min
    t = date.fromisoformat(t_str) if t_str else date.max
    return f <= at_date < t


# ----- One-call resolver used by importer/coordinator/services -------------


def resolve_tariff_at(
    utility_key: str,
    at_date: date,
    kw: float,
    eigenverbrauch: bool,
    *,
    user_inputs: dict | None = None,
    data: dict[str, Any] | None = None,
    source: str | None = None,
) -> ResolvedTariff:
    """One-call resolver: utility rate window → power tier → caps + floor.

    Composes ``find_active`` (utility rate window) → ``find_tier_for``
    (kW band + tier ``applies_when`` filter on user_inputs) → ``find_rule``
    (cap_rules + federal_minimum.rules) into a single ``ResolvedTariff``
    carrying everything an importer needs.

    ``user_inputs`` is the user-supplied dict from the active
    OPT_CONFIG_HISTORY record (``record["config"]["user_inputs"]``).
    Missing or None defaults each declared ``user_input.key`` to its
    schema-declared ``default`` before tier filtering — so a sensor
    attribute call without a record (sensor.py) still resolves a tier.
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

    if rate["settlement_period"] == "stunde":
        raise NotImplementedError(
            f"settlement_period='stunde' not yet supported "
            f"(utility={utility_key!r}, rate window {rate['valid_from']} → "
            f"{rate.get('valid_to') or 'open'}); hourly Day-Ahead settlement "
            f"per Vernehmlassung 2025/59 is deferred to a future release."
        )

    raw_user_inputs_decl = rate.get("user_inputs")
    user_inputs_decl: tuple[dict, ...] | None = (
        tuple(raw_user_inputs_decl) if raw_user_inputs_decl else None
    )

    # Default any missing user choice from declared `default` before tier
    # filtering. Empty/missing user_inputs is fine — declarations without
    # a chosen value fall back to schema defaults; unmatched tiers fall
    # back to the unconditional one.
    effective_inputs: dict = dict(user_inputs or {})
    if user_inputs_decl:
        for decl in user_inputs_decl:
            effective_inputs.setdefault(decl["key"], decl["default"])

    tier = find_tier_for(rate["power_tiers"], kw, effective_inputs)
    if tier is None:
        raise LookupError(
            f"{utility_key!r} has no power_tier covering {kw} kW "
            f"(rate window starting {rate['valid_from']})"
        )

    seasonal = rate.get("seasonal")
    # Batch D: seasonal blocks now serve two purposes — (1) per-season rate
    # variation for fixed_flat / fixed_ht_nt (legacy), and (2) season
    # classification for ``hkn_cases[].when.season`` / ``bonuses[].when.season``
    # in any base_model. A "classification-only" seasonal block carries
    # ``summer_months`` and ``winter_months`` but no rate keys; that's
    # always allowed. A seasonal block with rate keys (summer_rp_kwh /
    # winter_rp_kwh / summer_ht_rp_kwh / etc.) is still incompatible with
    # rmp_* base_models since rmp uses BFE quarterly prices, not utility
    # fixed rates.
    if seasonal is not None and tier["base_model"].startswith("rmp_"):
        rate_keys = (
            "summer_rp_kwh",
            "winter_rp_kwh",
            "summer_ht_rp_kwh",
            "summer_nt_rp_kwh",
            "winter_ht_rp_kwh",
            "winter_nt_rp_kwh",
        )
        if any(k in seasonal for k in rate_keys):
            raise ValueError(
                f"{utility_key!r}@{rate['valid_from']}: seasonal rate "
                f"overrides are not supported for base_model "
                f"{tier['base_model']!r}"
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

    raw_notes = rate.get("notes")
    if raw_notes is None:
        notes_filtered: tuple[dict, ...] | None = None
    else:
        notes_filtered = tuple(n for n in raw_notes if _note_active_at(n, at_date))

    raw_bonuses = rate.get("bonuses")
    bonuses_loaded: tuple[dict, ...] | None = (
        tuple(raw_bonuses) if raw_bonuses else None
    )

    raw_hkn_cases = tier.get("hkn_cases")
    hkn_cases_loaded: tuple[dict, ...] | None = (
        tuple(raw_hkn_cases) if raw_hkn_cases else None
    )

    tier_applies_when = tier.get("applies_when") or None

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
        price_floor_rp_kwh=rate.get("price_floor_rp_kwh"),
        tariffs_json_version=str(db["schema_version"]),
        tariffs_json_source=source if source is not None else get_source(),
        ht_window=tier.get("ht_window"),
        seasonal=seasonal,
        notes=notes_filtered,
        bonuses=bonuses_loaded,
        user_inputs_decl=user_inputs_decl,
        hkn_cases=hkn_cases_loaded,
        tier_applies_when=tier_applies_when,
    )


def list_utility_keys(data: dict[str, Any] | None = None) -> list[str]:
    """All known utility keys in the order they appear in tariffs.json."""
    db = data if data is not None else load_tariffs()
    return list(db["utilities"].keys())
