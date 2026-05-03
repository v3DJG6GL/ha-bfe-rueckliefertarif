"""External tariff database — JSON loader + lookup helpers.

The bundled `data/tariffs.json` is the source-of-truth; a companion-repo
runtime fetch layers on top with the same shape. All math in
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

# data_coordinator may set an override path pointing at the
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

    # Cap activation is signaled solely by a non-empty ``cap_rules`` array
    # at the rate-window level; the resolver evaluates
    # ``find_rule(cap_rules, kw, ev)`` and exposes the resulting cap.
    # ``None`` here = no cap (either no ``cap_rules``, an empty array, or
    # no rule covered (kw, ev)).
    cap_rp_kwh: float | None

    federal_floor_rp_kwh: float | None
    federal_floor_label: str        # runtime-rendered: "<30 kW", "≥150 kW", etc.

    price_floor_rp_kwh: float | None

    tariffs_json_version: str
    tariffs_json_source: str        # "remote" | "bundled"

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

    # Rate-window-level notes filtered to ``at_date``. Each entry:
    # ``{"valid_from": iso|None, "valid_to": iso|None, "severity":
    # "info"|"warning"|"error", "text": {lang: str}}``. ``None`` when the
    # rate window has no notes; empty list when notes exist but are all
    # outside the ``at_date`` window.
    notes: tuple[dict, ...] | None = None

    # Rate-window-level bonuses. Each entry carries
    # ``{"kind": "additive_rp_kwh"|"multiplier_pct", "name": str,
    # "applies_when": "always"|"opt_in"}`` plus ``rate_rp_kwh`` (additive)
    # or ``multiplier_pct`` (multiplier), and may include ``"note"`` and
    # ``"when"`` (a strict when_clause). The resolver evaluates the
    # ``when`` clause + ``kind`` per hour. ``None`` for "nothing to show" —
    # both missing key and empty array collapse to ``None``.
    bonuses: tuple[dict, ...] | None = None

    # Declarations of user-supplied toggles for the active rate window.
    # Each entry follows the schema's ``user_input`` shape:
    # ``{"key": str, "type": "enum"|"boolean", "default": str|bool,
    # "label_de": str, ...}``. Used by config_flow for dynamic form
    # rendering and by the per-hour resolver to default any user choice
    # that wasn't supplied. ``None`` when the rate window declares nothing.
    user_inputs_decl: tuple[dict, ...] | None = None

    # Power-tier-level conditional HKN overrides. Each entry carries
    # ``{"when": when_clause, "rp_kwh": float, "note"?: str}``. First
    # match wins per hour; falls through to the static ``hkn_rp_kwh``
    # field. ``None`` when the matched tier declares no cases.
    hkn_cases: tuple[dict, ...] | None = None

    # The matched tier's ``applies_when`` clause, retained raw for
    # downstream introspection (e.g. notification config-block "active
    # because: tariff_model=ht"). ``None`` when the matched tier has no
    # clause (the unconditional default tier).
    tier_applies_when: dict | None = None

    # Schema 1.5.0 rate-level HKN defaults. When a tier omits
    # ``hkn_structure`` / ``hkn_rp_kwh``, the resolver inherits these
    # values into ``hkn_structure`` / ``hkn_rp_kwh`` above. The defaults
    # are also retained verbatim so downstream consumers (curator UIs,
    # diagnostic exports) can distinguish "tier explicitly set" from
    # "tier inherited from rate-level default".
    hkn_structure_default: str = "none"
    hkn_rp_kwh_default: float | None = None

    # Schema 1.6.0 tier-level bonuses (additive). Concatenated AFTER
    # rate-level bonuses; ``multiplier_pct`` stacking is multiplicative
    # (rate first, then tier compounds on the rate-modified base).
    # ``base_model == "fixed_seasonal"`` signals tier-level seasonal as
    # the authoritative base price, and the resolver writes that block
    # into ``seasonal`` directly.
    tier_bonuses: tuple[dict, ...] | None = None


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


def match_applies_when(
    clause: dict | None, user_inputs: dict | None
) -> bool:
    """True iff every key in ``clause`` matches ``user_inputs``.

    Empty/None clause = unconditional match. Missing or mismatched key =
    no match. Mirrors the scalar-equality semantics used by
    ``find_tier_for`` for ``power_tiers[].applies_when``; reused here
    for ``rate.tarif_urls[].applies_when`` (schema v1.2.0).
    """
    if not clause:
        return True
    ui = user_inputs or {}
    return all(ui.get(k) == v for k, v in clause.items())


def find_active_rate_window(
    utility_key: str, on_date: date
) -> dict | None:
    """Return the rate-window record active for ``utility_key`` on
    ``on_date``, or ``None``. Convenience wrapper for callers that only
    have the utility key + a date (e.g. config-flow link rendering).
    """
    db = load_tariffs()
    utility = db.get("utilities", {}).get(utility_key)
    if utility is None:
        return None
    return find_active(utility.get("rates", []), on_date)


def self_consumption_relevant(
    utility_key: str, valid_from_iso: str, kw: float
) -> bool:
    """Return True iff the self-consumption bool actually changes resolver
    output for this (utility, valid_from, kW). False → hide the form
    field (or annotate the recompute notification's EV line as inert)
    because the resolver picks the same rule regardless.

    Identity comparison: ``find_rule`` returns the rule dict by reference
    from the input list. Same dict for both EV values means a
    ``self_consumption=null`` rule matched both, so the user's choice is
    inert. Different dicts (or one None) means the choice matters.

    Permissive on lookup failure — return True (treat as relevant) so the
    caller defaults to the conservative path.
    """
    try:
        db = load_tariffs()
        valid_date = date.fromisoformat(valid_from_iso)
    except (OSError, KeyError, ValueError):
        return True

    fed_record = find_active(db.get("federal_minimum") or [], valid_date)
    if fed_record is not None:
        rules = fed_record.get("rules") or []
        if find_rule(rules, kw, True) is not find_rule(rules, kw, False):
            return True

    utility = db.get("utilities", {}).get(utility_key)
    if utility is not None:
        rate = find_active(utility.get("rates") or [], valid_date)
        # Cap activation = non-empty `cap_rules`.
        if rate is not None and rate.get("cap_rules"):
            cap_rules = rate["cap_rules"]
            if find_rule(cap_rules, kw, True) is not find_rule(cap_rules, kw, False):
                return True

    return False


def pick_localised_label(
    d: dict, prefix: str, lang: str, fallback: str
) -> str:
    """Pick ``d[f"{prefix}_{lang}"]`` → ``d[f"{prefix}_de"]`` →
    ``d[f"{prefix}_en"]`` → ``fallback``. Mirrors the locale chain used
    elsewhere (``user_input_label``, note text picking).
    """
    return (
        d.get(f"{prefix}_{lang}")
        or d.get(f"{prefix}_de")
        or d.get(f"{prefix}_en")
        or fallback
    )


def resolve_user_inputs_decl(
    utility_key: str, valid_from: str
) -> tuple[dict, ...]:
    """Resolve the rate window's ``user_inputs[]`` declarations active at
    ``valid_from`` for the given utility. Returns ``()`` when the utility
    doesn't exist, no rate window covers the date, or the rate window
    declares nothing.

    Bypasses ``resolve_tariff_at`` so the lookup doesn't depend on a kW
    band — declarations live at rate-window scope, not power_tier scope.
    """
    if not utility_key or not valid_from:
        return ()
    try:
        d = date.fromisoformat(valid_from)
    except ValueError:
        return ()
    db = load_tariffs()
    utility = db.get("utilities", {}).get(utility_key)
    if utility is None:
        return ()
    rate = find_active(utility.get("rates", []), d)
    if rate is None:
        return ()
    return tuple(rate.get("user_inputs") or ())


def user_input_label(decl: dict, lang: str) -> str:
    """Pick the localized label for a user_input declaration. Falls back
    to ``label_de`` (schema-required), then ``label_en``, then the key.
    """
    return pick_localised_label(decl, "label", lang, decl.get("key", "—"))


def pick_value_label(decl: dict, value: str, lang: str) -> str:
    """Look up the per-value display label from ``value_labels_<lang>`` on
    a user_input declaration (schema v1.2.0 additive). Falls back to the
    raw value when no label dict matches.
    """
    labels = (
        decl.get(f"value_labels_{lang}")
        or decl.get("value_labels_de")
        or decl.get("value_labels_en")
        or {}
    )
    return str(labels.get(value, value))


# Display labels for tariff model + settlement period enums.
# The schema defines these as raw enum strings (no localisation fields),
# so we hard-code the display tables here. French falls back to English.
_TARIFF_MODEL_LABELS: dict[str, dict] = {
    "de": {
        ("fixed_flat", False): "Fixpreis",
        ("fixed_flat", True): "Fixpreis (saisonal)",
        ("fixed_ht_nt", False): "Fixpreis (HT/NT)",
        ("fixed_ht_nt", True): "Fixpreis (HT/NT, saisonal)",
        "fixed_seasonal": "Saisonal (Sommer/Winter)",
        "rmp_quartal": "Referenzmarktpreis (Quartal)",
        "rmp_monat": "Referenzmarktpreis (Monat)",
    },
    "en": {
        ("fixed_flat", False): "Fixed flat rate",
        ("fixed_flat", True): "Fixed flat rate (seasonal)",
        ("fixed_ht_nt", False): "Fixed HT/NT rate",
        ("fixed_ht_nt", True): "Fixed HT/NT rate (seasonal)",
        "fixed_seasonal": "Seasonal (summer/winter)",
        "rmp_quartal": "Reference market price (quarterly)",
        "rmp_monat": "Reference market price (monthly)",
    },
}

_SETTLEMENT_PERIOD_LABELS: dict[str, dict] = {
    "de": {"quartal": "Quartal", "monat": "Monat", "stunde": "Stunde"},
    "en": {"quartal": "Quarterly", "monat": "Monthly", "stunde": "Hourly"},
}


def tariff_model_label(
    base_model: str | None, seasonal: dict | None, lang: str
) -> str:
    """Localised display label for a tariff model.

    fixed_flat        → "Fixpreis"  / "Fixed flat rate"
    fixed_flat + sea  → "Fixpreis (saisonal)" / "Fixed flat rate (seasonal)"
    fixed_ht_nt       → "Fixpreis (HT/NT)" / "Fixed HT/NT rate"
    fixed_ht_nt + sea → "Fixpreis (HT/NT, saisonal)" / "Fixed HT/NT rate (seasonal)"
    rmp_quartal       → "Referenzmarktpreis (Quartal)" / "Reference market price (quarterly)"
    rmp_monat         → "Referenzmarktpreis (Monat)" / "Reference market price (monthly)"

    Unknown / missing models fall back to the raw enum value (or ``"—"``
    when ``base_model is None``). Unknown languages fall back to English.
    """
    if not base_model:
        return "—"
    table = _TARIFF_MODEL_LABELS.get(lang) or _TARIFF_MODEL_LABELS["en"]
    has_seasonal = bool(seasonal)
    if base_model in ("fixed_flat", "fixed_ht_nt"):
        return table.get((base_model, has_seasonal), base_model)
    return table.get(base_model, base_model)


def settlement_period_label(period: str | None, lang: str) -> str:
    """Localised display label for a settlement_period enum value.

    Unknown / missing periods fall back to the raw value. Unknown
    languages fall back to English.
    """
    if not period:
        return "—"
    table = _SETTLEMENT_PERIOD_LABELS.get(lang) or _SETTLEMENT_PERIOD_LABELS["en"]
    return table.get(period, period)


def user_inputs_decl_signature(rate: dict | None) -> tuple:
    """Stable hashable signature of a rate window's ``user_inputs[]``
    declarations.

    Two windows produce equal signatures iff their declarations are
    materially equivalent for form-rendering purposes — same keys, same
    types, same defaults, same enum value sets. Differences in i18n
    label fields (label_de / value_labels_*) intentionally do NOT
    affect the signature: they don't change which questions the form
    asks or which stored values are valid, only how those choices look.

    Returns ``()`` for windows with no ``user_inputs[]`` (or with an
    empty list). Used by ``compute_user_inputs_periods`` to group
    consecutive rate windows that the per-period editor can render
    as a single section.
    """
    decls = rate.get("user_inputs") if rate else None
    if not decls:
        return ()
    sig = []
    for d in decls:
        key = d.get("key")
        type_ = d.get("type")
        default = d.get("default")
        # values is a list for enum, absent for boolean. Hash via a
        # frozenset so order doesn't matter (defensive against curator
        # reorderings that don't actually change the choice space).
        values = frozenset(d.get("values") or ())
        sig.append((key, type_, default, values))
    # Sort by key so two windows with the same decls in different
    # array order produce equal signatures.
    return tuple(sorted(sig, key=lambda t: t[0] or ""))


def compute_user_inputs_periods(
    utility_key: str,
    span_from: date,
    span_to: date | None,
) -> list[tuple[date, date | None, dict]]:
    """Walk a utility's rate windows that overlap ``[span_from, span_to)``
    and group consecutive windows with equal ``user_inputs_decl_signature``
    into periods.

    Each returned tuple is ``(period_from, period_to, representative_rate)``:
    - ``period_from`` is clamped to ``span_from`` for the first period.
    - ``period_to`` is clamped to ``span_to`` for the last period; ``None``
      means open (the entry is the latest in user history and the last
      rate window has no ``valid_to``).
    - ``representative_rate`` is the FIRST rate window in the group; since
      grouped windows have equal signatures, any one works for rendering
      the form's user_input fields. Notes/tarif_urls in subsequent
      grouped windows are NOT merged — the editor shows the
      representative window's content.

    Returns an empty list when the utility doesn't exist or has no rate
    windows overlapping the span. Single-element list when the entire
    span is covered by one signature (the common case today).
    """
    db = load_tariffs()
    utility = db.get("utilities", {}).get(utility_key)
    if utility is None:
        return []
    rates = utility.get("rates") or []

    eff_span_to = span_to if span_to is not None else date.max
    overlapping: list[tuple[date, date, dict]] = []
    for r in rates:
        rf = date.fromisoformat(r["valid_from"])
        rt = date.fromisoformat(r["valid_to"]) if r.get("valid_to") else date.max
        # Half-open intersection: max(rf, span_from) < min(rt, eff_span_to)
        if max(rf, span_from) < min(rt, eff_span_to):
            overlapping.append((rf, rt, r))

    if not overlapping:
        return []
    overlapping.sort(key=lambda triple: triple[0])

    periods: list[tuple[date, date | None, dict]] = []
    i = 0
    while i < len(overlapping):
        sig_i = user_inputs_decl_signature(overlapping[i][2])
        j = i
        while j + 1 < len(overlapping):
            if user_inputs_decl_signature(overlapping[j + 1][2]) != sig_i:
                break
            j += 1
        first_rf, _first_rt, first_rate = overlapping[i]
        _last_rf, last_rt, _last_rate = overlapping[j]
        period_from = max(first_rf, span_from)
        # Map sentinel date.max back to None (= open) for callers.
        last_to_clamped = min(last_rt, eff_span_to)
        period_to = None if last_to_clamped == date.max else last_to_clamped
        periods.append((period_from, period_to, first_rate))
        i = j + 1

    return periods


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

    Output in DE (the integration's default; FR/EN translation pending):
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
    # ``base_model == "fixed_seasonal"`` makes the tier-level seasonal
    # block the authoritative source of both prices and the summer/winter
    # calendar (per Q1 decision). Rate-level seasonal is ignored for these
    # tiers. The schema's allOf rule guarantees the tier has a seasonal
    # block when this base_model is selected.
    if tier["base_model"] == "fixed_seasonal":
        seasonal = tier["seasonal"]
    # Seasonal blocks serve two purposes — (1) per-season rate variation
    # for fixed_flat / fixed_ht_nt, and (2) season classification for
    # ``hkn_cases[].when.season`` / ``bonuses[].when.season`` in any
    # base_model. A "classification-only" seasonal block carries
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

    # Cap activation by non-empty `cap_rules` only. `[]` (empty array)
    # and missing key both → no cap.
    cap_rp_kwh: float | None = None
    if rate.get("cap_rules"):
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

    # Rate-level HKN defaults. When a tier omits `hkn_structure` /
    # `hkn_rp_kwh`, the resolver inherits the rate-level default.
    # Curators dedupe homogeneous tiers in the importer pipeline by
    # lifting common values to the rate-level default; heterogeneous
    # tiers each declare their own. Schema (oneOf: [number, null]) permits
    # null/missing for `hkn_rp_kwh` in {"none", "bundled"} — those tiers
    # don't carry an additive HKN bonus, so the value is "not applicable".
    # Coerce to 0.0 at the boundary; the importer's _resolve_hkn_for_hour
    # short-circuits before reading this on non-additive_optin tiers.
    rate_hkn_struct_default = rate.get("hkn_structure_default") or "none"
    rate_hkn_rp_kwh_default = rate.get("hkn_rp_kwh_default")
    tier_hkn_struct = tier.get("hkn_structure") or rate_hkn_struct_default
    raw_hkn = tier.get("hkn_rp_kwh")
    if raw_hkn is None:
        raw_hkn = rate_hkn_rp_kwh_default
    hkn_rp_kwh_value = float(raw_hkn) if raw_hkn is not None else 0.0

    # Tier-level bonuses (additive overlay). Concatenated after
    # rate-level ``bonuses`` at evaluation time; ``multiplier_pct`` stacks
    # multiplicatively in iteration order.
    raw_tier_bonuses = tier.get("bonuses")
    tier_bonuses_loaded: tuple[dict, ...] | None = (
        tuple(raw_tier_bonuses) if raw_tier_bonuses else None
    )

    return ResolvedTariff(
        utility_key=utility_key,
        valid_from=rate["valid_from"],
        settlement_period=rate["settlement_period"],
        base_model=tier["base_model"],
        fixed_rp_kwh=tier.get("fixed_rp_kwh"),
        fixed_ht_rp_kwh=tier.get("fixed_ht_rp_kwh"),
        fixed_nt_rp_kwh=tier.get("fixed_nt_rp_kwh"),
        hkn_rp_kwh=hkn_rp_kwh_value,
        hkn_structure=tier_hkn_struct,
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
        hkn_structure_default=rate_hkn_struct_default,
        hkn_rp_kwh_default=rate_hkn_rp_kwh_default,
        tier_bonuses=tier_bonuses_loaded,
    )


def list_utility_keys(data: dict[str, Any] | None = None) -> list[str]:
    """All known utility keys in the order they appear in tariffs.json."""
    db = data if data is not None else load_tariffs()
    return list(db["utilities"].keys())


def _utility_display_name_from(util: dict | None) -> str:
    """Best-effort display name for a parsed utility dict."""
    if not isinstance(util, dict):
        return "—"
    return (
        util.get("name_de")
        or util.get("name_en")
        or util.get("name_fr")
        or "—"
    )


def diff_tariffs_data(old: dict | None, new: dict | None) -> dict:
    """Compare two parsed tariffs.json dicts and return a structured diff
    surface for the refresh-prices notification.

    Returns:
        ``{
            "added_utilities":     [{"key", "name", "rate_window_dates": [...]}],
            "removed_utilities":   [{"key", "name"}],
            "added_rate_windows":  [{"key", "name", "rate_window_dates": [...]}],
            "modified_rate_windows": [{"key", "name", "rate_window_dates": [...]}],
            "data_version_changed": (old_v, new_v) | None,
            "no_changes": bool,
        }``

    Comparison key for rate windows is ``valid_from``. A window is "added"
    when its ``valid_from`` is new; "modified" when ``valid_from`` exists in
    both but the rate-window dict's deep-equal canonical JSON form differs.
    Removed rate windows are not surfaced (rare, usually a re-keying).

    Robust to ``None`` inputs (treated as empty dicts) so callers can pass
    a missing pre-snapshot from a fresh install without branching.
    """
    old = old or {}
    new = new or {}
    old_utils = (old.get("utilities") or {}) if isinstance(old, dict) else {}
    new_utils = (new.get("utilities") or {}) if isinstance(new, dict) else {}

    added_utilities: list[dict] = []
    removed_utilities: list[dict] = []
    added_rate_windows: list[dict] = []
    modified_rate_windows: list[dict] = []

    for key in new_utils.keys() - old_utils.keys():
        util = new_utils.get(key) or {}
        added_utilities.append({
            "key": key,
            "name": _utility_display_name_from(util),
            "rate_window_dates": [
                r.get("valid_from") for r in (util.get("rates") or [])
                if isinstance(r, dict) and r.get("valid_from")
            ],
        })

    for key in old_utils.keys() - new_utils.keys():
        util = old_utils.get(key) or {}
        removed_utilities.append({
            "key": key,
            "name": _utility_display_name_from(util),
        })

    # For utilities present in both: detect added vs modified rate windows
    # by valid_from. Deep-equality via canonical JSON dump.
    for key in new_utils.keys() & old_utils.keys():
        old_util = old_utils.get(key) or {}
        new_util = new_utils.get(key) or {}
        old_rates = {
            r.get("valid_from"): r for r in (old_util.get("rates") or [])
            if isinstance(r, dict) and r.get("valid_from")
        }
        new_rates = {
            r.get("valid_from"): r for r in (new_util.get("rates") or [])
            if isinstance(r, dict) and r.get("valid_from")
        }
        added_dates = sorted(new_rates.keys() - old_rates.keys())
        if added_dates:
            added_rate_windows.append({
                "key": key,
                "name": _utility_display_name_from(new_util),
                "rate_window_dates": added_dates,
            })
        modified_dates = []
        for vf in sorted(new_rates.keys() & old_rates.keys()):
            old_canon = json.dumps(old_rates[vf], sort_keys=True)
            new_canon = json.dumps(new_rates[vf], sort_keys=True)
            if old_canon != new_canon:
                modified_dates.append(vf)
        if modified_dates:
            modified_rate_windows.append({
                "key": key,
                "name": _utility_display_name_from(new_util),
                "rate_window_dates": modified_dates,
            })

    old_v = old.get("data_version") if isinstance(old, dict) else None
    new_v = new.get("data_version") if isinstance(new, dict) else None
    data_version_changed = (old_v, new_v) if old_v != new_v else None

    no_changes = (
        not added_utilities
        and not removed_utilities
        and not added_rate_windows
        and not modified_rate_windows
        and data_version_changed is None
    )

    return {
        "added_utilities": added_utilities,
        "removed_utilities": removed_utilities,
        "added_rate_windows": added_rate_windows,
        "modified_rate_windows": modified_rate_windows,
        "data_version_changed": data_version_changed,
        "no_changes": no_changes,
    }
