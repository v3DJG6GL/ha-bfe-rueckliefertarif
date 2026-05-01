#!/usr/bin/env python3
"""Sync user_input slug entries + fixed integration-field labels from
bundled tariffs.json to strings.json and translations/{de,en,fr}.json.

Walks BOTH the ``config.step.*`` tree (initial setup) AND the
``options.step.*`` tree (gear-icon options flow). Both trees need
matching ``data.<key>`` entries — HA looks them up under the active
flow's tree, and missing entries cause raw slug names to leak into
the form (the v0.17.1 bug fixed in v0.18.0).

Idempotent: only INSERTS missing entries, never overwrites or deletes
existing labels. Run after every bundled tariffs.json upgrade or when
bfe-tariffs-data ships a new slug. CI auto-runs this on every push
that touches the bundled data.

Usage:
    python3 scripts/sync_user_input_slugs.py [--dry-run]

Exit codes: 0 if no changes, 1 if files were modified (CI-friendly).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

CC_ROOT = Path(__file__).resolve().parent.parent / "custom_components" / "bfe_rueckliefertarif"

# Per-tree, per-step inventory of FIELDS each step's form can render.
# Sentinel "<user_inputs>" means "all dynamic user_input slugs from
# bundled tariffs.json + EXTRA_REMOTE_SLUGS". Listed fields get
# authored under data.<field> in strings.json + 3 translations.
#
# Maintenance: when a new step is added or a field moves between steps,
# update this map and re-run the script. The CI guard test parses
# config_flow.py's `step_id="..."` literals and asserts every step here
# exists in code (catches typos).
STEPS_BY_TREE: dict[str, dict[str, tuple[str, ...]]] = {
    "config": {
        # Combined initial-flow first step (post-v0.18.0): utility +
        # date + kW. Replaces the old menu-of-utilities + tariff_pick.
        "user": ("energieversorger", "valid_from", "installierte_leistung_kwp"),
        # Initial-flow utility-specific details: EV + HKN + dynamic user_inputs.
        "tariff_details": (
            "eigenverbrauch_aktiviert",
            "hkn_aktiviert",
            "<user_inputs>",
        ),
    },
    "options": {
        # v0.18.1: "Apply config change" menu entry removed; everything
        # routes through "Manage configuration history" → Add/Edit.
        # "Add transition" / "Edit transition" page 1: utility + date + kWp.
        "add_pick_row": (
            "energieversorger",
            "valid_from",
            "installierte_leistung_kwp",
        ),
        "edit_pick_row": (
            "energieversorger",
            "valid_from",
            "installierte_leistung_kwp",
        ),
        # "Add transition" / "Edit transition" page 2: details. Edit also
        # has a "delete this record" toggle.
        "add_new_row": (
            "eigenverbrauch_aktiviert",
            "hkn_aktiviert",
            "<user_inputs>",
        ),
        "edit_row": (
            "eigenverbrauch_aktiviert",
            "hkn_aktiviert",
            "delete",
            "<user_inputs>",
        ),
    },
}

# Fixed integration-field labels per language. These are the label
# strings HA shows next to each form field. fr falls back to en when
# absent here.
FIXED_FIELD_LABELS: dict[str, dict[str, str]] = {
    "installierte_leistung_kwp": {
        "de": "Installierte Leistung (kWp)",
        "en": "Installed power (kWp)",
        "fr": "Puissance installée (kWp)",
    },
    "eigenverbrauch_aktiviert": {
        "de": "Eigenverbrauch",
        "en": "Self-consumption",
        "fr": "Autoconsommation",
    },
    "hkn_aktiviert": {
        "de": "HKN (Herkunftsnachweis)",
        "en": "GO (Guarantee of origin) (DE: HKN)",
        "fr": "GO (garantie d'origine) (DE: HKN)",
    },
    "energieversorger": {
        "de": "Energieversorger",
        "en": "Utility",
        "fr": "Fournisseur",
    },
    "valid_from": {
        "de": "Gültig ab",
        "en": "Valid from",
        "fr": "Valide dès",
    },
    "delete": {
        "de": "Diesen Eintrag löschen",
        "en": "Delete this record",
        "fr": "Supprimer cet enregistrement",
    },
}

# Generic per-language labels for shared user_input slugs whose decl
# label_<lang> varies per utility (e.g. supply_product is used by DKEK
# + EWN with different product names). The full per-utility name
# still surfaces in the help block above the form via
# _user_inputs_help_block (rendered from the decl's label_<lang> at runtime).
DEFAULT_SLUG_LABELS: dict[str, dict[str, str]] = {
    "de": {
        "supply_product": "Bezug Ökostrom-Produkt",
    },
    "en": {
        "supply_product": "Eco-electricity product subscribed",
        "fixpreis_rmp": "Compensation model (fixed price / reference market price)",
        "aew_fixpreis_rmp": "Compensation model (fixed price / reference market price)",
        "regio_top40_opted_in": "Wahltarif TOP-40",
    },
    # fr inherits en defaults via label_for_slug
    "fr": {},
}

# Slugs that ship in the *remote* tariffs-data schema ahead of the bundled
# fallback. Pre-author labels for them so users on the latest remote data
# don't see raw slugs when the bundled file is one minor version behind.
# Format: ``{slug: {"label_de": ..., "label_en": ...}}``. Merged with slugs
# discovered in bundled tariffs.json. Remove an entry once the bundled file
# catches up — the union is idempotent so duplicates are harmless.
EXTRA_REMOTE_SLUGS: dict[str, dict[str, str]] = {
    # Added in bfe-tariffs-data v1.4.0 (Regio Energie Solothurn).
    # v0.18.1: dropped "abonniert/subscribed" suffix — language-agnostic.
    "regio_top40_opted_in": {
        "label_de": "Wahltarif TOP-40",
        "label_en": "Wahltarif TOP-40",
    },
    # Added in bfe-tariffs-data v1.4.0 (renamed from aew_fixpreis_rmp).
    "fixpreis_rmp": {
        "label_de": "Vergütungsmodell Rücklieferung",
        "label_en": "Compensation model (fixed price / reference market price)",
    },
}


def collect_slugs(tariffs: dict) -> dict[str, dict]:
    """Return ``{slug: {"label_de": ..., "label_en": ...}}``. Picks the
    first utility's labels (alphabetical by utility key) for stable
    deterministic output across runs.
    """
    out: dict[str, dict] = {}
    for util_key in sorted(tariffs.get("utilities") or {}):
        util = tariffs["utilities"][util_key]
        for rate in util.get("rates") or []:
            for ui in rate.get("user_inputs") or []:
                slug = ui.get("key")
                if not slug or slug in out:
                    continue
                out[slug] = {
                    "label_de": ui.get("label_de") or slug,
                    "label_en": ui.get("label_en") or ui.get("label_de") or slug,
                }
    # Merge in slugs from EXTRA_REMOTE_SLUGS (slugs known to ship in remote
    # data ahead of bundled). Bundled entries take precedence on collision.
    for slug, info in EXTRA_REMOTE_SLUGS.items():
        if slug not in out:
            out[slug] = dict(info)
    return out


def label_for_slug(slug: str, lang: str, info: dict) -> str:
    """Resolve a label for a dynamic user_input slug."""
    if slug in DEFAULT_SLUG_LABELS.get(lang, {}):
        return DEFAULT_SLUG_LABELS[lang][slug]
    if lang == "de":
        return info["label_de"]
    if lang == "fr" and slug in DEFAULT_SLUG_LABELS.get("en", {}):
        # fr inherits en defaults rather than label_en from tariffs.json
        # (which often ships utility-specific full names).
        return DEFAULT_SLUG_LABELS["en"][slug]
    # en + fr fall back to label_en, then label_de as last resort
    return info["label_en"] or info["label_de"]


def label_for_field(field: str, lang: str) -> str | None:
    """Resolve a label for a fixed integration field (None if unknown)."""
    labels = FIXED_FIELD_LABELS.get(field)
    if not labels:
        return None
    if lang in labels:
        return labels[lang]
    # fr falls back to en when not authored
    return labels.get("en")


def fields_for_step(step_fields: tuple[str, ...], slugs: dict[str, dict]) -> list[str]:
    """Expand the <user_inputs> sentinel into concrete slug names."""
    expanded: list[str] = []
    for f in step_fields:
        if f == "<user_inputs>":
            expanded.extend(sorted(slugs.keys()))
        else:
            expanded.append(f)
    return expanded


def sync_file(path: Path, slugs: dict[str, dict], lang: str) -> bool:
    data = json.loads(path.read_text())
    changed = False
    for tree, steps in STEPS_BY_TREE.items():
        tree_block = data.setdefault(tree, {}).setdefault("step", {})
        for step, step_fields in steps.items():
            step_block = tree_block.setdefault(step, {})
            data_block = step_block.setdefault("data", {})
            for field in fields_for_step(step_fields, slugs):
                if field in data_block:
                    continue
                # Dispatch: fixed field vs user_input slug
                if field in FIXED_FIELD_LABELS:
                    label = label_for_field(field, lang)
                else:
                    info = slugs.get(field)
                    if info is None:
                        continue
                    label = label_for_slug(field, lang, info)
                if label is None:
                    continue
                data_block[field] = label
                changed = True
    if changed:
        path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n")
    return changed


def report_drift(path: Path, slugs: dict[str, dict]) -> bool:
    data = json.loads(path.read_text())
    any_missing = False
    for tree, steps in STEPS_BY_TREE.items():
        for step, step_fields in steps.items():
            step_data = (
                data.get(tree, {}).get("step", {}).get(step, {}).get("data", {})
            )
            missing = sorted(
                f for f in fields_for_step(step_fields, slugs)
                if f not in step_data
            )
            if missing:
                print(f"  WOULD ADD to {path.name} -> {tree}.{step}: {missing}")
                any_missing = True
    return any_missing


def main() -> int:
    dry = "--dry-run" in sys.argv
    tariffs_path = CC_ROOT / "data" / "tariffs.json"
    tariffs = json.loads(tariffs_path.read_text())
    slugs = collect_slugs(tariffs)
    print(
        f"Found {len(slugs)} unique user_input slug(s) in bundled tariffs.json: "
        f"{sorted(slugs)}"
    )
    targets = [
        (CC_ROOT / "strings.json", "en"),
        (CC_ROOT / "translations" / "de.json", "de"),
        (CC_ROOT / "translations" / "en.json", "en"),
        (CC_ROOT / "translations" / "fr.json", "fr"),
    ]
    any_changed = False
    for path, lang in targets:
        if dry:
            if report_drift(path, slugs):
                any_changed = True
        elif sync_file(path, slugs, lang):
            print(f"  Updated {path.relative_to(CC_ROOT.parent.parent)}")
            any_changed = True
    if not any_changed:
        print("All strings files already in sync.")
    return 1 if any_changed else 0


if __name__ == "__main__":
    sys.exit(main())
