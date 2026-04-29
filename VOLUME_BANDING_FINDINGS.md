# Volume-banding tariff conditions — findings & roadmap

## Context

Discovered during Batch D planning (importer session, 2026-04-29) while researching whether the importer's structured `when_clause` editor needs to support arbitrary numeric keys (e.g. `kwh_le: 100000`). The investigation surfaced a class of Swiss feed-in tariff conditions that the **current schema + integration cannot cleanly express**: piecewise rates conditional on **cumulative annual feed-in volume**.

Out of scope for Batch D — captured here so the gap is durable across future planning sessions.

## The pattern

A utility offers a tariff (typically HKN, but the pattern doesn't preclude energy or bonus components) where the rate per kWh depends on which "band" the producer's cumulative annual feed-in falls into. The rate **changes during the year** as the producer's running total crosses thresholds.

### Canonical example — EWA Aadorf 2024–2026

Literal quote from `Preisblatt_EWAadorf_Spezialtarife-2026_web.pdf` (and identical in the 2024 / 2025 PDFs — verified directly):

```
Eingespiesene Energiemenge          Vergütung exkl. MWST
0 bis 15'000 kWh                    4.00 Rp./kWh
15'001 bis 30'000 kWh               3.00 Rp./kWh
30'001 bis 100'000 kWh              2.00 Rp./kWh
> 100'001 kWh                       1.00 Rp./kWh
```

PDF text confirms the time window: *"Der Vergütungsansatz ist abhängig von der Energiemenge, die in das Verteilnetz eingespiesen wird"* + *"Die Vergütung erfolgt im 1. Quartal des Produktionsfolgejahres"* (paid in Q1 of the next year) → **calendar-year cumulative**.

In EWA Aadorf's case the bands gate **HKN compensation only** (a separate opt-in `Kaufangebot` contract); the energy-side compensation uses HT/NT (2024–25) or BFE Referenzmarktpreis + size-based floor (2026). Neither uses volume bands.

VESE API exposure (live, license-keyed):
- 2024 — `explText = "ab 100'000 kWh Rücklieferung 1 Rp./kWh"` (compresses the 4 bands into a single hint).
- 2025 — `explText = "Detaillierte Abstufung HKN Vergütung siehe Tarifblatt"`.
- 2026 — VESE response sparse for this EVU; PDF is the source of truth.

The German `explText` is auto-mapped to `notes[]` (Batch B). Curators see it in the review UI and can mentally account for it, but the data layer can't encode the actual band structure.

### Breadth — not just EWA Aadorf

Pattern likely applies to other Swiss utilities. Recommended follow-up scan when entering Batch F: grep VESE `explText` across the full catalog for tokens such as
- thousands-separated digit groups: `15'000`, `30'000`, `100'000`
- band markers: "0 bis", "ab", "über", "bis zu"
- annual markers: "pro Jahr", "Jahreseinspeisemenge", "Produktionsfolgejahr"

Also worth checking: bands gating bonuses (% uplift only above X kWh), bands gating energy price (less common but schema-permissible), bands with sub-annual time windows (theoretical — not yet observed in practice; if any exist they'd add complexity).

## Distinguishing characteristic — runtime-stateful vs. config-time conditional

Existing schema constructs (`bonuses[].kind: multiplier_pct`, `power_tier.applies_when`, `hkn_cases[].when`) handle **config-time conditional** pricing:
- Conditions depend on static plant attributes (DC max power, certification, opt-in subscription) and one-time user choices.
- Settled when the user fills out OptionsFlow.
- Once set, the rate is constant for the rate window.
- Resolver is stateless.

**Volume bands are runtime-stateful conditional**:
- Conditions depend on a counter that changes throughout the year.
- Same plant, same setup, same opt-in: rate goes through 2–4 distinct values across the year as YTD volume crosses thresholds.
- Resolver needs an accumulator.

Concrete contrast — Regio Solothurn TOP-40 vs. EWA Aadorf:

| Axis                 | Regio Solothurn TOP-40 (current schema fits)             | EWA Aadorf bands (gap)                                  |
|----------------------|----------------------------------------------------------|---------------------------------------------------------|
| Condition value type | Static plant attributes (DC > 3.7 kWp, 60 % power cap)   | Cumulative kWh fed in this year                         |
| Evaluated when       | Once at config time (opt-in checkbox)                    | Per-hour during operation                               |
| Resolver state       | None — boolean flag                                      | YTD-feed-in counter (HA sensor / integration state)     |
| Rates per year       | 1 (constant after opt-in)                                | 2–4 (one per band crossed)                              |
| Schema fit today     | `bonuses[].kind: multiplier_pct` + `applies_when: opt_in`| No clean construct — `when_clause` is for config-time   |

Note: TOP-40's "60 % power curtailment" is **instantaneous power** (kW set in inverter firmware, never changes); EWA Aadorf's bands are **cumulative energy** (kWh integrated over time). Don't conflate the two — they sit on opposite sides of this axis.

## Cross-repo impact

| Repo                       | Impact                                                                                                                                                                                                                                              |
|----------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **bfe-tariffs-data**       | New schema construct: `power_tier.hkn_volume_bands[]` (per-tier, more flexible — mirrors `power_tier.kw_min/kw_max` precedent). Items: `[{kwh_min, kwh_max, rp_kwh, note?}]`, with `kwh_max: null` for the last open-ended band. Encode EWA Aadorf 2024/25/26 sample data as the canonical first case. |
| **bfe_rueckliefertarif**   | Resolver tracks calendar-year cumulative feed-in. Two implementation routes: (a) Producer wires an existing HA `sensor.grid_export_today` (utility-meter integration) into OptionsFlow; resolver reads its YTD value (resets Jan 1). Cheaper but couples to user setup. (b) Synthesise an accumulator inside the integration. More invasive, self-contained. Default to (a) for the first cut. Per-hour rate calculation evaluates the active band; notification renders it; sensor surface exposes "current band" + "kWh remaining in band". |
| **bfe-tariffs-importer**   | Bespoke `_render_volume_bands_editor` in `widgets.py` (similar pattern to `_render_cap_rules_editor`). Field-types + field-help registrations for `power_tiers.*.hkn_volume_bands.*.{kwh_min, kwh_max, rp_kwh, note}`. STOP_FLATTEN_PATHS entry. Tests.                |

## Proposal

Make this its own dedicated batch — call it **Batch F**, parallel to (but independent from) Batch E (hourly Day-Ahead). Single concern, cross-repo. Triggered by a real-world utility being ready to consume it (EWA Aadorf is ready today; other candidates likely emerge from the breadth scan above).

**Suggested ordering** (adds-first-from-schema, per `SCHEMA_COVERAGE_ANALYSIS.md`'s existing rule):

1. **Session 1 — `bfe-tariffs-data`**: schema $def for `hkn_volume_bands[]`, JSON-Schema validator, EWA Aadorf 2024/25/26 sample data (HKN-only — leave the existing energy-side HT/NT or RMP encoding untouched).
2. **Session 2 — `bfe_rueckliefertarif`**: integration resolver tracks YTD via user-wired entity (option (a)); per-hour rate calculation uses tier; notification renders the active band + a sub-row when the period straddles a band crossing (re-uses the Batch B helper for season-straddle / config-history straddle); new sensor: "current HKN band" + "kWh until next band".
3. **Session 3 — `bfe-tariffs-importer`**: `_render_volume_bands_editor`, register fields, add tests.

## Caveats

- **YTD reset semantics** — must be **calendar-year**, NOT rolling-12-months. Rate-window `valid_from`/`valid_to` may not align with calendar years (utility might switch tariff structure mid-year, or onboard a producer mid-year). The accumulator must be calendar-aware; sub-window proration is a design question for Session 2.
- **Mid-band crossing within a notification period** — what UX is right when the producer's YTD crosses a band boundary mid-quarter? Render two sub-rows in the recompute notification (one per band slice) — same shared helper as Batch B's seasonal-straddle / mid-period-config-history. Spec in Session 2.
- **First-year prorated bands** — when a plant is commissioned mid-year, do the bands apply against the partial-year volume or get scaled? Real-world utilities likely treat them as "first 15k kWh starting from commissioning, regardless of calendar". Verify against PDFs in Session 1.
- **Backward-compat** — none needed; pre-v1.0, no migration concern. Existing tariffs without `hkn_volume_bands[]` continue working unchanged.
- **Schema location** — per-tier (`power_tier.hkn_volume_bands[]`) is more flexible than rate-window-level. Even if no real utility today varies bands across power tiers, the precedent matches `power_tier.hkn_cases[]` and avoids future migration.
- **Naming** — avoid magic key suffixes like `kwh_le_15000`. Use a typed numeric pair `kwh_min` / `kwh_max` (with `kwh_max: null` for the open-ended last band, mirroring `kw_max: null` precedent on `power_tier`).
- **Curator workaround until Batch F ships** — pick the conservative HKN tier as a flat `hkn_rp_kwh`. For EWA Aadorf's typical residential 5 kWp PV producing ~5,000 kWh/year, that's tier 1 = 4.0 Rp/kWh. For a large commercial producer expected to cross 100k, that's 1.0 Rp/kWh. The German `explText` is already mapped to `notes[]` (Batch B), so the curator sees the warning when reviewing.
- **Why not encode bands as 4 separate `bonuses[]` or `hkn_cases[]` with magic numeric `when` keys** — schema-syntactically possible (`when_clause.user_inputs.additionalProperties` accepts numbers), but the resolver wouldn't know what `kwh_le_per_year` means without bespoke handling. Cleaner to design the construct properly than to overload an existing one with magic semantics.

## Status as of 2026-04-29

**Out of scope for Batch D.** Documented in `bfe-tariffs-importer` Batch D plan as a known gap. EWA Aadorf and similar utilities continue to live with curator-picked conservative flat HKN until Batch F ships.

**Trigger condition for Batch F:** demand from a producer trying to import a real utility that uses this pattern, OR proactive scan of VESE catalog identifying ≥3 utilities affected (do this scan first to size the work).
