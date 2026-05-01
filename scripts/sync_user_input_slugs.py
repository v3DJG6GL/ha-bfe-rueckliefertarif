#!/usr/bin/env python3
"""Sync user_input slug entries from bundled tariffs.json to strings.json
and translations/{de,en,fr}.json.

Idempotent: only INSERTS missing slug entries, never overwrites or deletes
existing labels (so manual translation tweaks survive). Run after every
bundled tariffs.json upgrade or when bfe-tariffs-data ships a new slug.
A CI workflow auto-runs this on every push that touches the bundled data.

Usage:
    python3 scripts/sync_user_input_slugs.py [--dry-run]

Exit codes: 0 if no changes, 1 if files were modified (CI-friendly).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

CC_ROOT = Path(__file__).resolve().parent.parent / "custom_components" / "bfe_rueckliefertarif"

# All config-flow steps that build dynamic user_input form fields. When a
# slug is shipped in tariffs.json, every one of these step blocks needs a
# matching entry under data.<slug> in strings.json + 3 translation files.
USER_INPUT_STEPS = (
    "tariff_details",
    "apply_change_details",
    "add_new_row",
    "edit_row",
    "pick_user_inputs",
)

# Generic per-language labels for slugs whose decl `label_<lang>` varies
# per utility (e.g. `supply_product` is used by DKEK + EWN with different
# product names). The full per-utility name still surfaces in the help
# block above the form via _user_inputs_help_block (rendered from the
# decl's label_<lang> at runtime).
DEFAULT_LABELS: dict[str, dict[str, str]] = {
    "de": {
        # Shared across DKEK + EWN with utility-specific full names; the
        # full names still surface via _user_inputs_help_block above the form.
        "supply_product": "Bezug Ökostrom-Produkt",
    },
    "en": {
        # Author English labels for every known slug — tariffs.json doesn't
        # ship label_en consistently; without these fallbacks the en form
        # would show German labels.
        "supply_product": "Eco-electricity product subscribed",
        "fixpreis_rmp": "Compensation model (fixed price / reference market price)",
        "aew_fixpreis_rmp": "Compensation model (fixed price / reference market price)",
        "regio_top40_opted_in": "Wahltarif TOP-40 subscribed",
    },
    # fr inherits en defaults below (see label_for).
    "fr": {},
}

# Slugs that ship in the *remote* tariffs-data schema ahead of the bundled
# fallback. Pre-author labels for them so users on the latest remote data
# don't see raw slugs when the bundled file is one minor version behind.
# Format: ``{slug: {"label_de": ..., "label_en": ...}}``. Merged with slugs
# discovered in bundled tariffs.json. Remove an entry once the bundled file
# catches up and the same slug appears in collect_slugs(...) — the union is
# idempotent so duplicates are harmless.
EXTRA_REMOTE_SLUGS: dict[str, dict[str, str]] = {
    # Added in bfe-tariffs-data v1.4.0 (Regio Energie Solothurn).
    "regio_top40_opted_in": {
        "label_de": "Wahltarif TOP-40 abonniert",
        "label_en": "Wahltarif TOP-40 subscribed",
    },
    # Added in bfe-tariffs-data v1.4.0 (renamed from aew_fixpreis_rmp).
    "fixpreis_rmp": {
        "label_de": "Vergütungsmodell Rücklieferung",
        "label_en": "Compensation model (fixed price / reference market price)",
    },
}


def collect_slugs(tariffs: dict) -> dict[str, dict]:
    """Return ``{slug: {"label_de": ..., "label_en": ...}}``. Picks the
    first utility's labels (alphabetical by utility key) for a stable
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


def label_for(slug: str, lang: str, info: dict) -> str:
    if slug in DEFAULT_LABELS.get(lang, {}):
        return DEFAULT_LABELS[lang][slug]
    if lang == "de":
        return info["label_de"]
    if lang == "fr" and slug in DEFAULT_LABELS.get("en", {}):
        # fr inherits en defaults rather than falling through to whatever
        # label_en in tariffs.json shipped (often utility-specific full names).
        return DEFAULT_LABELS["en"][slug]
    # en + fr fall back to label_en, then label_de as last resort
    return info["label_en"] or info["label_de"]


def sync_file(path: Path, slugs: dict[str, dict], lang: str) -> bool:
    data = json.loads(path.read_text())
    changed = False
    steps = data.setdefault("config", {}).setdefault("step", {})
    for step in USER_INPUT_STEPS:
        step_block = steps.setdefault(step, {})
        data_block = step_block.setdefault("data", {})
        for slug, info in slugs.items():
            if slug not in data_block:
                data_block[slug] = label_for(slug, lang, info)
                changed = True
    if changed:
        path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n")
    return changed


def report_drift(path: Path, slugs: dict[str, dict]) -> bool:
    data = json.loads(path.read_text())
    any_missing = False
    for step in USER_INPUT_STEPS:
        step_data = (
            data.get("config", {}).get("step", {}).get(step, {}).get("data", {})
        )
        missing = sorted(s for s in slugs if s not in step_data)
        if missing:
            print(f"  WOULD ADD to {path.name} -> {step}: {missing}")
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
