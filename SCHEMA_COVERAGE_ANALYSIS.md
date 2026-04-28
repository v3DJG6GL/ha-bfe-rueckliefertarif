# Tariff schema vs. integration coverage

Analysis of `tariffs-v1.schema.json` (companion repo `bfe-tariffs-data`) against
the BFE Rückliefertarif Home Assistant integration. Generated 2026-04-27 against
v0.9.7. Reflects research + decisions across the Q&A round.

## Legend

| symbol | meaning                                          |
|--------|--------------------------------------------------|
| ✓      | wired                                            |
| ~      | partial / implicit only                          |
| ✗      | absent / not implemented                         |
| n/a    | not applicable                                   |
| 🔴     | scheduled for removal from codebase / schema     |
| ➕     | scheduled to be added                            |

## Coverage axes

- **Loaded** — parsed into a code structure (e.g. `ResolvedTariff`)
- **Calculation** — used in tariff calculation
- **Notification** — surfaced in a recompute notification body
- **Sensor** — surfaced as a HA sensor state or attribute
- **UI** — user-configurable in any OptionsFlow surface

---

## cap_mode + cap_rules — fully wired

`cap_mode` (bool) + `cap_rules[]` are completely implemented in
`tariff.py:effective_rp_kwh_breakdown` (StromVV Art. 4 Abs. 3 Bst. e
two-clause rule), surfaced in notifications + sensors, and tested in
`test_tariff.py:167–223`. No action items. Used by EKZ, Groupe E, Primeo.

---

## Field-by-field coverage matrix

### Top-level metadata

| Field                | Loaded | Calculation | Notification | Sensor | UI  | Notes |
|----------------------|:------:|:-----------:|:------------:|:------:|:---:|-------|
| `schema_version`     | ✓      | n/a         | ✗            | ✗      | n/a | Validated by `TariffsDataCoordinator._validate`. |
| `last_updated`       | ✓      | n/a         | ✗            | ✓      | n/a | `TariffsDataLastUpdateSensor` native value. |
| `notes`              | ✗      | n/a         | ✗            | ✗      | n/a | Pure documentation. |
| `federal_minimum[]`  | ✓      | ✓           | ~            | ~      | n/a | Surfaces as `Federal floor (Mindestvergütung): {label} ({rp_kwh})`. |

### Utility-level

| Field                    | Loaded | Calculation | Notification | Sensor | UI  | Notes |
|--------------------------|:------:|:-----------:|:------------:|:------:|:---:|-------|
| `nr_elcom`, `uid`        | ✗      | n/a         | ✗            | ✗      | n/a | Registry metadata; ignored. |
| `name_de`                | ✓      | n/a         | ✓            | ✓      | ✓   | Shown in config block, sensor attr, dropdown. |
| `name_fr`, `name_en`     | ~      | n/a         | ~            | ✗      | ~   | Display fallback name_de → fr → en. |
| `homepage`, `tarif_url`  | ✗      | n/a         | ✗            | ✗      | n/a | Not surfaced. |
| `rates[]`                | ✓      | ✓           | ✓            | ✓      | n/a | `find_active(rates, at_date)` selects active window. |

### Rate-window level (current state + planned changes)

| Field                       | Loaded | Calculation | Notification | Sensor | UI  | Decision |
|-----------------------------|:------:|:-----------:|:------------:|:------:|:---:|----------|
| `valid_from`, `valid_to`    | ✓      | ✓           | ✗            | ✗      | n/a | Keep. |
| `settlement_period`         | ✓      | ~           | ~            | ✓      | n/a | Keep, but extend with `stunde` Phase 2 (deferred). Today's user toggle `abrechnungs_rhythmus` to be **🔴 dropped** (derived from utility's `settlement_period`). |
| `power_tiers[]`             | ✓      | ✓           | ✓            | ✓      | n/a | Keep. |
| `requires_naturemade_star`  | ✓      | ✗           | ✗            | ✗      | ✗   | **🔴 DROP** entirely. Replaced by `notes[]`. |
| `price_floor_rp_kwh`        | ✓      | ✗           | ✗            | ✗      | ✗   | Keep. **➕ Wire** in calculation: `effective_floor = max(federal_floor, utility_floor or 0)` per hour. |
| `hkn_switch_cadence`        | ✗      | ✗           | ✗            | ✗      | ✗   | **🔴 DROP** entirely. No real legal meaning at app level. |
| `bonuses[]`                 | ✗      | ✗           | ✗            | ✗      | ✗   | Keep. **➕ Schema extension Phase 2**: add `kind`, `when {}`. Most "bonuses" are conditional HKN — see `hkn_cases[]`. |
| `seasonal`                  | ✓      | ✓           | ~            | ✗      | n/a | Keep. **➕ Notification**: surface "seasonal applied" + per-period sub-rows when straddling. |
| `cap_mode`                  | ✓      | ✓           | ✓            | ✓      | n/a | Keep — fully wired. |
| `cap_rules[]`               | ✓      | ✓           | ~            | ✓      | n/a | Keep. |
| `notes[]`                   | n/a    | n/a         | n/a          | n/a    | n/a | **➕ NEW**: per-rate-window `[{valid_from, valid_to, lang, text, severity}]`. Surfaced via HA `description_placeholders` in OptionsFlow forms + recompute config blocks. |
| `user_inputs[]`             | n/a    | n/a         | n/a          | n/a    | n/a | **➕ NEW**: declarative user-side toggles per rate window. See Part 6. |

### Power-tier level

| Field                                    | Loaded | Calculation | Notification | Sensor | UI  | Decision |
|------------------------------------------|:------:|:-----------:|:------------:|:------:|:---:|----------|
| `kw_min`, `kw_max`                       | ✓      | ✓           | ✗            | ✗      | n/a | Keep. |
| `base_model`                             | ✓      | ✓           | ✓            | ✓      | n/a | Keep. Per-tier variation supported (endigo case). |
| `fixed_rp_kwh`                           | ✓      | ✓           | ✓            | ✓      | n/a | Keep. |
| `fixed_ht_rp_kwh`, `fixed_nt_rp_kwh`     | ✓      | ✓           | ~            | ~      | n/a | Keep. |
| `fixed_annualized_rp_kwh`                | ✗      | ✗           | ✗            | ✗      | n/a | **🔴 DROP** from schema + importer. Display-only metric not useful for per-hour math. |
| `ht_window.{mofr,sa,su}`                 | ✓      | ✓           | ✗            | ✗      | n/a | Keep. |
| `hkn_rp_kwh`                             | ✓      | ✓           | ✓            | ✓      | n/a | Keep — fallback / unconditional HKN value. |
| `hkn_structure`                          | ~      | ~           | ✗            | ✗      | ✗   | Keep. **➕ Wire**: gate `hkn_aktiviert` UI toggle on the value. |
| `hkn_cases[]`                            | n/a    | n/a         | n/a          | n/a    | n/a | **➕ NEW**: conditional HKN overrides. `[{when: {season, supply_product, kwh_le, ...}, rp_kwh}]`. First match wins; falls through to `hkn_rp_kwh`. |

### VESE importer fields without schema mapping

| Field                                | Decision |
|--------------------------------------|----------|
| `autot` / `energyAuto*` / `ecoAuto*` | **🔴 DROP** from importer parsing. Never made it into v1 schema; only 1 historical hit (EW Uznach 2022). Clean break. |

---

## User-configurable surfaces (planned)

After cleanup: 5 versioned core fields + 4 entity-wiring fields + dynamic
user_inputs declared per rate window.

| Field                          | Initial setup | Apply change | Manage history (add/edit) |
|--------------------------------|:-------------:|:------------:|:-------------------------:|
| `valid_from`                   | ✓             | ✓ (renamed from `effective_date`) | ✓ |
| `energieversorger`             | ✓             | ✓            | ✓                         |
| `installierte_leistung_kw`     | ✓             | ✓            | ✓                         |
| `eigenverbrauch_aktiviert`     | ✓             | ✓            | ✓                         |
| `hkn_aktiviert`                | ✓ (gated on `hkn_structure`) | ✓ (same gate) | ✓ (same gate) |
| `abrechnungs_rhythmus`         | 🔴 dropped — derived from utility's `settlement_period` | | |
| Entity wiring (4 fields)       | ✓             | n/a          | n/a                       |
| **Dynamic user_inputs[]**       | ✓ (rendered when active utility declares them) | ✓ | ✓ |

All form fields use `DateSelector` for dates (no more raw-string parsing
inconsistency between initial flow and OptionsFlow).

---

## Notification surfaces

| Notification           | Trigger                                                                                       | notification_id                              |
|------------------------|-----------------------------------------------------------------------------------------------|----------------------------------------------|
| Recompute summary      | `recompute_history` step                                                                      | `{DOMAIN}_{entry_id}_recompute_summary`      |
| Skipped quarters       | Coordinator auto-import predates utility's earliest rate window                               | `{DOMAIN}_{entry_id}_skipped_quarters`       |
| Refresh data (success) | OptionsFlow `refresh_data` step                                                               | `{DOMAIN}_{entry_id}_refresh`                |
| Recompute error        | `recompute_history` failure                                                                   | `{DOMAIN}_{entry_id}_recompute_history`      |
| Refresh error          | `refresh_data` failure                                                                        | `{DOMAIN}_{entry_id}_refresh` (overwrite)    |

Recompute summary body = active-today config block + per-group config blocks
+ per-period table (Base / HKN / Total / kWh / CHF) + forfeit footnote.

**Planned additions**: per-period sub-rows for (a) seasonal straddling, (b)
mid-period config-history transition, (c) eventually mixed quartal/hourly DA
quarters. One shared rendering helper.

---

## Corrections to earlier (faulty) findings

> **Relevant for Batch D (bonuses + `hkn_cases[]` + `user_inputs[]`).**
> Implementers working on Batches A, B, or C can safely skip this section.

The first agent-driven analysis identified "9 bonus patterns" in the wild. After
verification against primary sources, several were wrong. Recording them here so
the corrected interpretation is the durable reference.

| Utility | Earlier (wrong) characterisation | Actual reality |
|---------|----------------------------------|----------------|
| **EW Bürglen** | Tariff window restricted to "Ostteil der Gemeinde" | "Ost" is part of the *utility's name* — `EW Bürglen Ost` is one utility (Leimbach + Opfershofen); SAK covers the rest of Bürglen TG. Coverage area = utility identity, not a tariff condition. |
| **Primeo** | "Mid-year temporal sub-window bonus" pattern | Quarterly tariff change → modelled as additional `rate_window` with new `valid_from/valid_to`. Not a bonus. |
| **Dorfkorporation Ebnat-Kappel** | "HKN bonus 3.0 Rp/kWh only with supply product" | HKN tops at 2.00 (not 3.0). >30 kVA *without* supply product still gets HKN (1.00 winter / 0.30 summer), "nach Absprache". Real shape: 2×2×2 truth table over (size, supply_product, season). Maps to `hkn_cases[]`. |
| **endigo AG** | "+2 Rp/kWh HKN bonus only for >30 kWp" | HKN is **flat 2.0 Rp/kWh** for both size classes in 2025 *and* 2026. The size threshold drives the **base model** (fixed_flat ≤30 kWp / Marktpreis >30 kWp in 2025; fixed_flat seasonal ≤150 kWp / RMP >150 kWp in 2026). Already supported via per-tier `base_model`. |

After these corrections, the bonus landscape collapses substantially:

- **Conditional HKN** is the dominant pattern → `hkn_cases[]` covers it (season,
  supply_product, kwh_le).
- **Genuine non-HKN bonuses** are rare (Regio Solothurn TOP-40 curtailment is
  the one solid example) → `bonuses[].kind="multiplier_pct"`.
- **Certification gating** (BKW naturemade-star) → `notes[]` for self-attestation.
- **Region scoping** → utility identity (separate utility entries).
- **Mid-year tariff changes** (Primeo) → additional rate windows.
- **Power-tier conditionality** → already handled by per-tier `base_model`.

---

## New schema concept — `user_inputs[]`

Some Swiss utilities offer **multiple parallel tariff variants** for the same
plant. Today the integration handles this via:

1. Multiple utility entries for one utility (current `aew_fixpreis` /
   `aew_rmp` workaround — semantically wrong, will be deleted).
2. Hardcoded preset params per utility (EKZ "segment" — doesn't scale).
3. Living without it (silent rate errors).

`user_inputs[]` declares the choice in the schema, persists in
OPT_CONFIG_HISTORY, and the resolver evaluates it via the same `applies_when` /
`when` mechanism as `hkn_cases[]` and `bonuses[]`.

### Schema shape

```jsonc
{
  "rate_window": {
    "user_inputs": [
      {
        "key": "tariff_model",
        "type": "enum",                              // "enum" | "boolean"
        "values": ["fixpreis", "rmp"],
        "default": "fixpreis",
        "label_de": "Tarifmodell",
        "description_de": "AEW bietet zwei Vergütungsmodelle..."
      }
    ],
    "power_tiers": [
      {
        "applies_when": {"tariff_model": "fixpreis"},
        "kw_min": 0, "kw_max": null,
        "base_model": "fixed_flat", "fixed_rp_kwh": 9.5,
        "hkn_rp_kwh": 2.0
      },
      {
        "applies_when": {"tariff_model": "rmp"},
        "kw_min": 0, "kw_max": null,
        "base_model": "rmp_quartal",
        "hkn_rp_kwh": 2.0
      }
    ]
  }
}
```

For HKN-only variants (Ebnat-Kappel pattern), `user_inputs` references
`hkn_cases[].when` instead of `power_tier.applies_when`.

### Verified real-world cases

| Utility | Choice | Today | With `user_inputs` |
|---------|--------|-------|--------------------|
| **AEW** | Fixpreis vs. RMP-Quartal | 2 utility entries | 1 utility, enum `tariff_model` |
| **endigo 2026 ≤150 kWp** | Fixed seasonal vs. BFE Marktpreis | None | enum `tariff_model` |
| **TBW Weinfelden** | Buys Thurgauer Naturstrom yes/no | Hardcoded HKN 2.0 (worst case) | boolean `supply_product` + `hkn_cases[]` |
| **KEW Nidwalden** | Buys EWNSonne yes/no | None | boolean `supply_product` + `hkn_cases[]` |
| **Dorfkorporation Ebnat-Kappel** | Buys Ebnat-Kappler-Solar 10/40% yes/no | None | boolean `supply_product` + `hkn_cases[]` (season axis) |
| **EKZ** | Segment / cohort | Hardcoded preset param | enum `segment` declared in schema |

### Threading through the codebase

| Layer | Role |
|-------|------|
| Schema | Declares `user_inputs[]`, legal values, default, locale labels. Validates `applies_when` / `when` references. |
| Importer | Captures the declaration once per utility. AEW: replaces 2-entry hack with 1 entry + declared `tariff_model`. |
| OptionsFlow UI | Looks up active rate window's `user_inputs[]`; renders one form field per declared input. |
| OPT_CONFIG_HISTORY | New optional field `user_inputs: dict[str, Any]` per record. User changes a choice → new record. |
| Resolver | Per-hour: filters `power_tiers[].applies_when` and `hkn_cases[].when` by record's `user_inputs`. First match wins; falls through to `hkn_rp_kwh`. |
| Notification | Render active user_inputs in config block: *"Tarifmodell: Fixpreis"*. |

---

## Comprehensive recap

(<span style="color:red">**🔴 RED + DROP**</span> = code/schema/importer removed;
~~strikethrough~~ = idea dropped without replacement; **➕ bold** = added.)

| #  | Gap                                                                       | Recommended action                                                                                                                                                                                                                                                                | Approx code changes |
|----|---------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------|
| 1  | `hkn_structure: bundled/none` not respected                               | Gate `hkn_aktiviert` toggle on `hkn_structure`. `additive_optin`→show. `bundled`→hide, force True, render note. `none`→hide, force False, render Pronovo-vermarkten note. `null`→keep current, log warning. | ~40 lines + 2 tests |
| 2  | `price_floor_rp_kwh` loaded but unused                                    | Per-hour: `effective_floor = max(federal_floor, utility_floor or 0)`. Persist `floor_source` in row. Render in config block. | ~25 lines + 2 tests |
| 3  | `bonuses[]` schema too coarse                                             | **Phase 1**: load + display in config block. **Phase 2**: schema extension — `power_tier.hkn_cases[]`, `bonuses[].kind`, `bonuses[].when`, `rate_window.user_inputs[]` (#15). Per-hour resolver evaluates cases. | Phase 1: ~100 lines. Phase 2: ~500 lines. |
| 4  | <span style="color:red">**🔴 DROP**: `hkn_switch_cadence`</span>          | <span style="color:red">**Drop entirely** from schema, importer, integration.</span> | Schema: 3 lines. Importer: 5 lines. Integration: 3 lines. |
| 5  | <span style="color:red">**🔴 DROP**: `requires_naturemade_star`</span> + **➕ add `notes[]`**  | <span style="color:red">**Drop** `requires_naturemade_star`</span>. **Add** `notes[]` per rate window. Display via HA `description_placeholders` + recompute config blocks. | ~130 lines + 4 tests |
| 6a | <span style="color:red">**🔴 DROP**: `fixed_annualized_rp_kwh`</span>     | <span style="color:red">**Drop** from schema + importer.</span> Integration never read it. | Schema: 3 lines. Importer: 5 lines. |
| 6b | `settlement_period: "stunde"` for Vernehmlassung 2025/59                 | **Phase 1 (now)**: keep `stunde` in schema; raise `NotImplementedError` if encountered. **Phase 2 (deferred)**: new `DayAheadCoordinator`, `rmp_stunde` resolver branch, extend `abrechnungs_rhythmus`, mixed-quarter notification. | Phase 1: ~10 lines. Phase 2: ~600 lines. |
| 7  | Notification doesn't mark seasonal applied                                | (a) Config-block line. (b) Per-period sub-rows on season straddle. **Combine helper with #12.** | ~80 lines + 3 tests |
| 8  | ~~`has_naturemade_star` user toggle (idea)~~                              | ~~Idea dropped, never implemented.~~ Replaced by notes-based self-attestation (#5). | 0 |
| 9  | `settlement_period` vs `abrechnungs_rhythmus` mismatch                    | <span style="color:red">**🔴 Drop** the `abrechnungs_rhythmus` user toggle.</span> Derive from utility's `settlement_period`. Auto-overwrite OPT_CONFIG_HISTORY entries on next refresh. | ~50 lines + 3 tests |
| 10 | `valid_from` naming + DateSelector inconsistency                          | Rename `effective_date` → `valid_from`. **`DateSelector` in all 3 forms.** | ~30 lines + 1 test |
| 11 | <span style="color:red">**🔴 DROP**: VESE `autot`/`energyAuto*`/`ecoAuto*`</span> | <span style="color:red">**Drop entirely** from importer.</span> Never in v1 schema; no integration code touches it. | Importer: 30 lines. |
| 12 | Notification doesn't split sub-rows for mid-period config switches       | Detect `OPT_CONFIG_HISTORY` transitions in `_aggregate_by_period`; render sub-rows. **Same helper as #7.** | bundled with #7 |
| 13 | ~~Supply-product / region-conditional bonuses unstructured~~              | ~~Bundled with #3 Phase 2.~~ Replaced by `notes[]` (#5) + `user_inputs[]` (#15). | bundled |
| 14 | Per-tier `base_model` variation (endigo)                                  | Already supported in schema; **add regression test.** | ~30 lines, 1 test |
| 15 | **➕ NEW**: `user_inputs[]` for parallel tariff variants                  | **Add** `rate_window.user_inputs[]`. New `OPT_CONFIG_HISTORY[].user_inputs: dict`. OptionsFlow renders dynamic form fields. Resolver filters by user choice. **Replaces** AEW split, EKZ hardcoded segment, supply-product cases. User will manually delete `aew_fixpreis`/`aew_rmp` and add a single `aew` entry. | ~200 lines + 5 tests |

---

## Implementation grouping

Each batch lists every item it contains with a one-line description, so this
section can be read standalone without flipping back to the recap table.

### Batch A — Quick wins / cleanup (~250 lines, target **v0.9.8**)

Small isolated fixes + bulk removals. Closes most latent issues.

- **#1** — Gate `hkn_aktiviert` UI toggle on the active utility's
  `hkn_structure` (show / hide / force based on `additive_optin` /
  `bundled` / `none` / `null`).
- **#2** — Wire utility-level `price_floor_rp_kwh` into per-hour floor:
  `effective_floor = max(federal_floor, utility_floor or 0)`. Persist
  `floor_source` in row + render in config block.
- **#4** — 🔴 **DROP** `hkn_switch_cadence` from schema, importer, integration.
- **#6a** — 🔴 **DROP** `fixed_annualized_rp_kwh` from schema + importer.
- **#6b Phase 1** — Keep `settlement_period: "stunde"` value in schema; raise
  meaningful `NotImplementedError` if encountered. Phase 2 (full hourly
  Day-Ahead) deferred to Batch E.
- **#9** — 🔴 **DROP** the user-side `abrechnungs_rhythmus` toggle. Derive from
  utility's `settlement_period`. Auto-overwrite OPT_CONFIG_HISTORY entries.
- **#10** — Rename `effective_date` → `valid_from` everywhere. Use
  `DateSelector` in all 3 forms (initial setup, apply_change, manage_history).
- **#11** — 🔴 **DROP** VESE `autot` / `energyAuto*` / `ecoAuto*` parsing
  from importer. Never made it into v1 schema.
- **#14** — Add regression test for per-tier `base_model` variation (endigo
  case: fixed_flat ≤150 kWp, RMP >150 kWp).

### Batch B — Notes + seasonal/mid-period rendering (~250 lines, target **v0.9.9**)

Notes feature spans schema/importer/integration; paired with notification
rendering improvements since both extend the recompute pipeline.

- **#5** — 🔴 **DROP** `requires_naturemade_star`. **➕ ADD** `rate_window.notes[]`
  with `{valid_from, valid_to, lang, text, severity}`. Display via HA
  `description_placeholders` in OptionsFlow forms + recompute config blocks.
- **#7 + #12 combined** — Notification: (a) config-block line *"Seasonal
  rates: Yes (summer/winter…)"* when active config has `seasonal`. (b)
  Per-period sub-rows when a period straddles a season boundary OR contains
  an `OPT_CONFIG_HISTORY` transition. Single shared rendering helper.

### Batch C — Bonuses Phase 1 / display-only (~100 lines, optional **v0.10.0**)

- **#3 Phase 1** — Load `bonuses[]` + render names + rates in the recompute
  config block. No conditional evaluation yet. Buys time before tackling
  Phase 2.

### Batch D — Bonuses Phase 2 + user_inputs + hkn_cases (~700 lines, target **v0.11.0**)

When bonus support becomes a real-world need. Major schema extension; needs
the importer in lockstep.

- **#3 Phase 2** — Schema extensions: `bonuses[].kind` enum
  (`additive_rp_kwh` / `multiplier_pct`), `bonuses[].when {}` for season /
  volume / etc. conditions. Per-hour resolver evaluates conditions; new
  `Bonus` column / sub-rows in period table.
- **#15** — **➕ NEW** `rate_window.user_inputs[]`: declarative user-side
  toggles for utilities offering parallel tariff variants. New
  `OPT_CONFIG_HISTORY[].user_inputs: dict` per record. OptionsFlow renders
  dynamic form fields. Resolver filters `power_tiers[].applies_when` and
  `hkn_cases[].when` by user choice. Adds `power_tier.hkn_cases[]` for
  conditional HKN. **Replaces** AEW split (`aew_fixpreis`/`aew_rmp` → single
  `aew` entry, manually deleted by user), EKZ hardcoded segment param, and
  supply-product cases (TBW Weinfelden, KEW Nidwalden, Ebnat-Kappel).
  Subsumes the original #13 (supply-product / region-conditional bonuses).

### Batch E — Hourly Day-Ahead (deferred, ~600 lines)

- **#6b Phase 2** — New `DayAheadCoordinator` (EPEX SPOT CH hourly feed),
  `rmp_stunde` base model branch in resolver, extend `abrechnungs_rhythmus`
  with `stunde` value (re-introduced after Batch A's drop, this time as a
  derived value), mixed-quarter notification rendering (combined with the
  Batch B helper). Triggered when (a) Vernehmlassung 2025/59 enters force
  AND (b) a utility actually adopts hourly settlement. Both currently unmet.

### Suggested order

A → B → C → D → E. Batches A and B together close most latent issues and
remove all ballast (~7 schema/importer fields gone; simpler codebase). C+D
follow, E activates when triggered.

---

## Cross-repo work plan

The work spans three repositories with very different profiles:

| Repo                                       | Profile                                                                                   |
|--------------------------------------------|-------------------------------------------------------------------------------------------|
| `bfe_rueckliefertarif` (this repo)         | The HA integration. Substantial Python; pytest-homeassistant-custom-component.            |
| `bfe-tariffs-data`                         | Pure JSON: 265-line schema + 831-line `tariffs.json`. No CI, no Python tooling. Trivially editable. |
| `bfe-tariffs-importer`                     | Python + Streamlit: 14 modules, 8 test files, VESE API client, override mechanism. Substantial. |

### Per-batch session plan

Each batch executes as one or more **separate Claude Code sessions**, started
by `cd <repo>` then `claude` in that repo's working directory. Each session
opens with the prompt **"Create a plan in plan mode for Batch X in this
repo per `<path-to>/SCHEMA_COVERAGE_ANALYSIS.md`"** so the agent proposes a
plan before making any changes.

| Batch | Sessions                                                                                          | Notes                                                                                                                 |
|-------|---------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------|
| **A** | (1) `bfe_rueckliefertarif` + schema piggyback, (2) `bfe-tariffs-importer`                         | Schema edits are 5-line drops; piggyback on integration session. Importer alone for the field_map / Streamlit cleanup. |
| **B** | (1) `bfe_rueckliefertarif` + schema piggyback, (2) `bfe-tariffs-importer`                         | Schema `notes[]` $def is ~30 lines; piggyback on integration. Importer separate for VESE→notes mapping + UI work.    |
| **C** | (1) `bfe_rueckliefertarif`                                                                        | Display-only. Integration alone.                                                                                      |
| **D** | (1) `bfe-tariffs-data` (schema + sample data + validator), (2) `bfe_rueckliefertarif`, (3) `bfe-tariffs-importer` | The one batch where schema design earns its own session. Both downstream consumers reference the locked-down schema. |
| **E** | (1) `bfe_rueckliefertarif` + schema piggyback                                                     | Trivial schema (`stunde` accepted). Importer probably untouched (no VESE source for DA prices).                       |

### Ordering rules

- **Drops** (Batch A): integration *first* (stops reading the field), then
  importer (stops emitting), then schema (removes the field). If the schema
  field disappeared while the integration still read it, validation would fail.
- **Adds** (Batch B+): schema *first* (defines the contract + sample data),
  then integration, then importer. Integration before importer so the new
  shape can be exercised against handcrafted sample data before mass emission.

### Quality bar for implementation sessions

Each implementation session works under these standing rules. Every prompt
below references this section.

- **Read the actual code, not from memory.** File paths, function names, line
  numbers, schema fields, test conventions — read the current state. Don't
  guess at signatures or shapes. If a referenced file looks different from
  what the plan expects, flag the discrepancy before proceeding.
- **Verify external claims against primary sources.** Swiss tariff regulations,
  VESE API responses, official utility PDFs, HA framework behavior, EPEX /
  Swissgrid pricing — fetch the actual source, don't rely on training data or
  earlier conversation summaries. Cite where verification came from.
- **Use parallel sub-agents liberally.** Isolation keeps the main context
  clean. There is no quota — spawn as many as the work justifies. Research-only
  agents are especially good for *"verify behavior X"* or *"find utility Y's
  tariff document"*.
- **Flag uncertainty explicitly in the plan.** A plan with *"verify X before
  implementing"* beats a plan that confidently states wrong details. Any "I
  think" or "probably" should be either resolved during plan research or
  named as an open question.
- **No code before plan approval.** Every implementation session enters plan
  mode first. The plan-mode exit gate is the approval point — don't bypass it.
- **Solo-dev / no-migrations policy applies.** Pre-v1.0; one-time manual fixes
  are acceptable; no compat shims for removed code.

---

## Summary

After cleanup, the schema/integration becomes substantially leaner:

**Removed**: `requires_naturemade_star`, `hkn_switch_cadence`,
`fixed_annualized_rp_kwh`, importer's `autot`/`energyAuto*`/`ecoAuto*` parsing,
integration's `abrechnungs_rhythmus` user toggle.

**Added**: `notes[]` (per-rate-window contextual hints, locale-aware),
`hkn_cases[]` (conditional HKN values), `user_inputs[]` (declarative user
toggles for parallel tariff variants), `bonuses[].kind`/`when` (rare genuine
bonuses).

**Improved**: HKN gating on `hkn_structure`, utility floor wiring,
DateSelector consistency, notification rendering for seasonal +
mid-period transitions, regression coverage for per-tier `base_model`.

**Deferred**: Hourly Day-Ahead (Vernehmlassung 2025/59) until a utility
actually adopts it.
