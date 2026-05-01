"""Guard: every user_input slug + fixed integration field declared in the
sync script's STEPS_BY_TREE has a matching ``data.<key>`` entry in
strings.json + de/en/fr translations — across BOTH ``config.step.*``
(initial setup) and ``options.step.*`` (gear-icon options flow).

When bfe-tariffs-data introduces a new slug or config_flow.py adds a new
form-bearing step, this test fails until strings entries are added —
typically by running ``scripts/sync_user_input_slugs.py``, which the CI
workflow does automatically on push.

v0.18.0 (Issue 6.3 cont. + 6.4): the v0.17.1 sync script only walked
``config.step.*``, leaving ``options.step.*`` (where Apply/Edit/Add
transition flows live) populated only with legacy entries. This test
now asserts coverage in BOTH trees.
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


def _data_block(file_data: dict, tree: str, step: str) -> dict:
    return (
        file_data.get(tree, {})
        .get("step", {})
        .get(step, {})
        .get("data", {})
    )


def _expected_fields_for_step(
    sync, tree: str, step: str, slugs: set[str]
) -> list[str]:
    """Resolve the sync script's <user_inputs> sentinel → concrete slug list."""
    raw_fields = sync.STEPS_BY_TREE.get(tree, {}).get(step, ())
    out: list[str] = []
    for f in raw_fields:
        if f == "<user_inputs>":
            out.extend(sorted(slugs))
        else:
            out.append(f)
    return out


def test_strings_json_covers_all_steps_in_both_trees():
    sync = _load_sync_module()
    slugs = _expected_slugs()
    strings = json.loads((CC_ROOT / "strings.json").read_text())
    missing: list[str] = []
    for tree, steps in sync.STEPS_BY_TREE.items():
        for step in steps:
            block = _data_block(strings, tree, step)
            for field in _expected_fields_for_step(sync, tree, step, slugs):
                if field not in block:
                    missing.append(f"{tree}.{step}.data.{field}")
    assert not missing, (
        f"Missing strings.json entries: {missing}. "
        "Run scripts/sync_user_input_slugs.py and commit."
    )


@pytest.mark.parametrize("lang", ["de", "en", "fr"])
def test_translations_cover_all_steps_in_both_trees(lang):
    sync = _load_sync_module()
    slugs = _expected_slugs()
    translations = json.loads(
        (CC_ROOT / "translations" / f"{lang}.json").read_text()
    )
    missing: list[str] = []
    for tree, steps in sync.STEPS_BY_TREE.items():
        for step in steps:
            block = _data_block(translations, tree, step)
            for field in _expected_fields_for_step(sync, tree, step, slugs):
                if field not in block:
                    missing.append(f"{lang}/{tree}.{step}.data.{field}")
    assert not missing, (
        f"Missing translation entries in {lang}.json: {missing}. "
        "Run scripts/sync_user_input_slugs.py and commit."
    )


def test_sync_script_step_ids_exist_in_config_flow():
    """Drift guard: every step ID in STEPS_BY_TREE must appear somewhere
    in config_flow.py as a string literal. Catches typos that would
    otherwise silently leave forms unauthored.

    Some step_ids are set via conditional expressions like
    ``step_id="edit_row" if is_edit else "add_new_row"``, so we scan all
    double-quoted bareword strings rather than only ``step_id="..."``.
    """
    import re
    sync = _load_sync_module()
    cf_text = (CC_ROOT / "config_flow.py").read_text()
    quoted_strings = set(re.findall(r'"([a-z][a-z0-9_]*)"', cf_text))
    missing: list[str] = []
    for tree, steps in sync.STEPS_BY_TREE.items():
        for step in steps:
            if step not in quoted_strings:
                missing.append(f"{tree}.{step}")
    assert not missing, (
        f"Step IDs in STEPS_BY_TREE not found in config_flow.py: {missing}. "
        "Either fix the typo or remove the entry from sync_user_input_slugs.py."
    )
