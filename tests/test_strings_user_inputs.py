"""Guard: every user_input slug expected by the integration has a matching
translation entry in strings.json + de/en/fr translations.

The slug set is the union of (a) slugs in bundled tariffs.json and (b)
``EXTRA_REMOTE_SLUGS`` from the sync script (slugs known to ship in remote
data ahead of bundled). When bfe-tariffs-data introduces a new slug, this
test fails until strings entries are added — typically by running
``scripts/sync_user_input_slugs.py``, which the CI workflow does
automatically on push.

Required steps (every config-flow step that builds dynamic user_input
form fields): tariff_details, apply_change_details, add_new_row,
edit_row, pick_user_inputs.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

CC_ROOT = (
    Path(__file__).resolve().parent.parent
    / "custom_components"
    / "bfe_rueckliefertarif"
)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "sync_user_input_slugs.py"
)
USER_INPUT_STEPS = (
    "tariff_details",
    "apply_change_details",
    "add_new_row",
    "edit_row",
    "pick_user_inputs",
)


def _load_sync_module():
    spec = importlib.util.spec_from_file_location("_sync_module", SCRIPT_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _expected_slugs() -> set[str]:
    """Return the union of bundled-data slugs + EXTRA_REMOTE_SLUGS — i.e.,
    the full slug set the integration expects to render labels for."""
    sync = _load_sync_module()
    tariffs = json.loads((CC_ROOT / "data" / "tariffs.json").read_text())
    return set(sync.collect_slugs(tariffs))


def _data_block(translations: dict, step: str) -> dict:
    return (
        translations.get("config", {})
        .get("step", {})
        .get(step, {})
        .get("data", {})
    )


def test_strings_json_covers_all_user_input_slugs():
    slugs = _expected_slugs()
    strings = json.loads((CC_ROOT / "strings.json").read_text())
    missing: list[str] = []
    for step in USER_INPUT_STEPS:
        block = _data_block(strings, step)
        for slug in slugs:
            if slug not in block:
                missing.append(f"{step}.data.{slug}")
    assert not missing, (
        f"Missing strings.json entries: {missing}. "
        "Run scripts/sync_user_input_slugs.py and commit."
    )


@pytest.mark.parametrize("lang", ["de", "en", "fr"])
def test_translations_cover_all_user_input_slugs(lang):
    slugs = _expected_slugs()
    translations = json.loads(
        (CC_ROOT / "translations" / f"{lang}.json").read_text()
    )
    missing: list[str] = []
    for step in USER_INPUT_STEPS:
        block = _data_block(translations, step)
        for slug in slugs:
            if slug not in block:
                missing.append(f"{lang}/{step}.data.{slug}")
    assert not missing, (
        f"Missing translation entries in {lang}.json: {missing}. "
        "Run scripts/sync_user_input_slugs.py and commit."
    )
