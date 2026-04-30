# Migration notes — schema v1.2.0 (importer Batch BB)

## What changed upstream

The producer (`bfe-tariffs-importer` v0.7.29) now writes
`tariffs.json` against schema **v1.2.0** with two changes affecting
this integration:

### 1. `tarif_url` → `tarif_urls` (BREAKING, but inert in our code)

- **Old**: `rate.tarif_url: str | null`.
- **New**: `rate.tarif_urls: array of { url, label_de?, label_fr?,
  label_en?, kind? ('pdf'|'html'), applies_when? }`.
- **Field is hard-renamed**, no alias.
- **Impact on our Python code**: NONE — we never read `tarif_url`.
  Confirmed by greppinging the codebase: only the bundled schema and
  bundled `data/tariffs.json` reference it.
- **Impact on bundled assets**: outdated.
  - `custom_components/bfe_rueckliefertarif/schemas/tariffs-v1.schema.json`
    needs to match upstream v1.2.0.
  - `custom_components/bfe_rueckliefertarif/data/tariffs.json` needs
    to be regenerated (or pulled fresh from `bfe-tariffs-data`).
  - `tests/test_tariffs_db.py:48` (the
    `test_bundled_json_validates` case) will fail until both above
    are refreshed.

### 2. `user_inputs[].value_labels_de/_fr/_en` (ADDITIVE)

- New optional fields on `$defs/user_input`: object mapping
  `values[i]` → display label.
- Lets curators use clean tokens (`"fixpreis"` / `"rmp"`) in `values`
  and surface pretty labels (`"AEW Fixpreis"` /
  `"Referenzmarktpreis"`) in the HA OptionsFlow / ConfigFlow.
- **Impact on our config_flow**: optional enhancement at
  `config_flow.py:510-546` (`_add_user_input_fields`):
  ```python
  values = decl.get("values", []) or []
  labels_map = (
      decl.get(f"value_labels_{lang}")
      or decl.get("value_labels_de")
      or {}
  )
  options = [
      selector.SelectOptionDict(value=str(v),
                                label=str(labels_map.get(v, v)))
      for v in values
  ]
  ```
  Reuses the existing locale picker pattern from
  `_user_input_label` (config_flow.py:499-507).

## Concrete TODO checklist for the follow-up plan

- [ ] Refresh bundled `schemas/tariffs-v1.schema.json` to v1.2.0
      (copy from `bfe-tariffs-data/schemas/tariffs-v1.schema.json`,
      or git submodule).
- [ ] Refresh bundled `data/tariffs.json` (regenerate from importer
      or copy the migrated file from `bfe-tariffs-data/`).
- [ ] Update `test_tariffs_db.py:377` (or similar) to assert
      `schema_version: "1.2.0"` if any test pins it.
- [ ] Add `_pick_localised_label()` helper near
      `config_flow.py:499-507` to encapsulate the
      `label_<lang>` → `label_de` → fallback chain (currently only
      used for `_user_input_label`).
- [ ] Wire `value_labels_*` lookup into `_add_user_input_fields`
      enum option building (config_flow.py:532).
- [ ] Optional: surface `tarif_urls` in HA — markdown card with
      multiple links, or sensor `extra_state_attributes`. Today the
      links are completely hidden from the user (only available in
      the source JSON).
- [ ] Optional: respect `tarif_urls[].applies_when` when filtering
      which links to surface — the resolver pattern at
      `tariffs_db.py:207-238` (`find_tier_for`) already does this
      for power_tiers; same logic could apply per-URL.
- [ ] Add tests covering `value_labels_*` rendering + the
      `tarif_urls` array shape on bundled data.
- [ ] Bump `manifest.json:11` version 0.11.0 → 0.12.0.

## Cross-repo coordination

Order:

1. **`bfe-tariffs-data`** — schema bump + tariffs.json migration →
   tag v1.2.0. (Importer Batch BB committed locally as `6dfd5a9`;
   awaits user to push.)
2. **`bfe-tariffs-importer`** — v0.7.29 (Batch BB).
3. **`bfe_rueckliefertarif`** — v0.12.0 (this repo, follow-up plan).

## Schema repo summary (for context)

The data repo's commit (`6dfd5a9`) does three things:

1. `schemas/tariffs-v1.schema.json`: bumped to v1.2.0. Replaces
   `rate_window.tarif_url` with `tarif_urls` (oneOf:
   array-of-`tarif_url_entry` | null). Adds `$defs/tarif_url_entry`.
   Adds optional `value_labels_de` / `_fr` / `_en` to
   `$defs/user_input`.
2. `tariffs.json`: 25 `tarif_url` strings converted to
   `tarif_urls: [{url}]` arrays. `schema_version` 1.1.1 → 1.2.0.
3. `scripts/migrate_tarif_url_to_tarif_urls.py`: one-shot migration
   tool (idempotent if rerun on already-migrated data).
4. `scripts/test_schema.py`: 3 new test cases (13/14/15) cover the
   new shapes; 15/15 pass.

---

*Generated as part of importer Batch BB (v0.7.29). When you start
the follow-up plan, point Claude at this file to load context.*
