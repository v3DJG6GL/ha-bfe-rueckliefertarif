"""Phase 6 — runtime fetch of ``tariffs.json`` from the companion repo.

The companion repo is the canonical source of utility/floor data — it lets
community PRs land yearly tariff updates without an HACS release. The fetch
is daily, schema-validated, and falls back to the bundled file on any
failure. Cache lives in ``<config>/.storage/bfe_rueckliefertarif_tariffs.json``
as raw JSON (so ``tariffs_db.load_tariffs`` can read it directly via
``set_override_path``).

Tariffs.json is the *only* runtime-mutable data; everything else (settings,
history) is in entry.data / entry.options. A failure here is non-fatal —
worst case the user sees yesterday's data, or bundled values from the
last HACS release.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .const import (
    CONF_ENERGIEVERSORGER,
    CONF_USER_INPUTS,
    DOMAIN,
    OPT_CONFIG_HISTORY,
)
from .tariffs_db import (
    compute_user_inputs_periods,
    load_tariffs,
    set_override_path,
)

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant

_LOGGER = logging.getLogger(__name__)

# Companion repo. Replace `<owner>` once the repo is created — for v0.5
# fallback to bundled is the default behavior anyway.
REMOTE_URL = (
    "https://raw.githubusercontent.com/v3DJG6GL/bfe-tariffs-data/main/tariffs.json"
)
CACHE_FILENAME = "bfe_rueckliefertarif_tariffs.json"
META_FILENAME = "bfe_rueckliefertarif_tariffs.meta.json"
REFRESH_INTERVAL = timedelta(days=1)
FETCH_TIMEOUT_S = 30


class TariffsDataCoordinator:
    """Single-instance coordinator owning the remote tariffs.json fetch.

    On startup: if cache is fresh, use it; otherwise try fetch + fall back to
    bundled on failure. Daily refresh kicks in via the BfeCoordinator's
    update tick (it calls async_maybe_refresh).
    """

    def __init__(self, hass: HomeAssistant) -> None:
        self._hass = hass
        storage_dir = Path(hass.config.path(".storage"))
        self._cache_path = storage_dir / CACHE_FILENAME
        self._meta_path = storage_dir / META_FILENAME
        self.last_remote_update: datetime | None = None
        self.last_error: str | None = None

    async def async_load(self) -> None:
        """Initial load on integration setup. Never raises — fallback is bundled."""
        meta = await self._read_meta()
        if meta:
            try:
                self.last_remote_update = datetime.fromisoformat(meta["fetched_at"])
            except (KeyError, ValueError):
                self.last_remote_update = None

        if self._cache_path.is_file() and self._is_fresh():
            set_override_path(self._cache_path)
            _LOGGER.debug("Using cached remote tariffs.json (fetched %s)", self.last_remote_update)
        else:
            await self.async_refresh()

        # Warm tariffs_db's lru_cache so subsequent callers in the event loop
        # hit memory instead of triggering HA's blocking-I/O detector.
        await self._hass.async_add_executor_job(load_tariffs)

    async def async_maybe_refresh(self) -> None:
        """Called periodically by the BfeCoordinator tick — refresh only if stale."""
        if not self._is_fresh():
            await self.async_refresh()

    async def async_refresh(self) -> bool:
        """Fetch + validate + cache. Return True iff successful."""
        import aiohttp

        try:
            async with aiohttp.ClientSession() as session, session.get(
                REMOTE_URL, timeout=aiohttp.ClientTimeout(total=FETCH_TIMEOUT_S)
            ) as resp:
                resp.raise_for_status()
                data = await resp.json(content_type=None)
        except Exception as exc:
            _LOGGER.warning("Remote tariffs.json fetch failed: %s — using bundled", exc)
            self.last_error = str(exc)
            set_override_path(None)
            return False

        try:
            await self._hass.async_add_executor_job(self._validate, data)
        except Exception as exc:
            _LOGGER.warning(
                "Remote tariffs.json failed schema validation: %s — using bundled", exc
            )
            self.last_error = str(exc)
            set_override_path(None)
            return False

        # Persist cache + metadata.
        await self._hass.async_add_executor_job(self._write_cache, data)
        self.last_remote_update = datetime.now(UTC)
        self.last_error = None
        set_override_path(self._cache_path)
        # Warm tariffs_db's lru_cache against the new file so the next caller
        # — which may be a sync code path in the event loop — hits memory.
        await self._hass.async_add_executor_job(load_tariffs)
        _LOGGER.info(
            "Remote tariffs.json refreshed (schema_version=%s)",
            data.get("schema_version"),
        )

        # v0.13.0 (Phase 3) — scan all this domain's entries for stale
        # user_inputs against the freshly-fetched tariff schema and
        # surface repair issues. Wrapped so issue-creation failures
        # don't fail the refresh.
        try:
            self._create_drift_issues_for_all_entries()
        except Exception as exc:
            _LOGGER.warning("Drift scan after refresh failed: %s", exc)

        return True

    def _create_drift_issues_for_all_entries(self) -> None:
        """Iterate domain entries; surface drift issues for each."""
        for entry in self._hass.config_entries.async_entries(DOMAIN):
            _create_drift_issues(self._hass, entry)

    def _is_fresh(self) -> bool:
        if self.last_remote_update is None:
            return False
        age = datetime.now(UTC) - self.last_remote_update
        return age < REFRESH_INTERVAL

    async def _read_meta(self) -> dict[str, Any] | None:
        def _read() -> dict[str, Any] | None:
            if not self._meta_path.is_file():
                return None
            try:
                with open(self._meta_path, encoding="utf-8") as f:
                    return json.load(f)
            except (OSError, json.JSONDecodeError):
                return None

        return await self._hass.async_add_executor_job(_read)

    def _write_cache(self, data: dict[str, Any]) -> None:
        """Run in executor — does blocking I/O."""
        self._cache_path.parent.mkdir(parents=True, exist_ok=True)
        # Atomic write: tmp + rename.
        tmp = self._cache_path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        tmp.replace(self._cache_path)

        meta = {"fetched_at": datetime.now(UTC).isoformat()}
        meta_tmp = self._meta_path.with_suffix(".meta.json.tmp")
        with open(meta_tmp, "w", encoding="utf-8") as f:
            json.dump(meta, f)
        meta_tmp.replace(self._meta_path)

    def _validate(self, data: dict[str, Any]) -> None:
        """Schema-validate the fetched data against tariffs-v1.schema.json.

        ``jsonschema`` is a dev dep but is also pulled in by HA's voluptuous
        bridge in many setups. If it isn't available, we fall back to a
        loose structural check (top-level keys present) so v0.5 still works
        on minimal HA setups; the bundled file always validates so the user
        is never worse off.
        """
        try:
            import jsonschema
        except ImportError:
            self._loose_validate(data)
            return

        schema_path = Path(__file__).parent / "schemas" / "tariffs-v1.schema.json"
        with open(schema_path, encoding="utf-8") as f:
            schema = json.load(f)
        jsonschema.Draft202012Validator(schema).validate(data)

    @staticmethod
    def _loose_validate(data: dict[str, Any]) -> None:
        for key in ("schema_version", "federal_minimum", "utilities"):
            if key not in data:
                raise ValueError(f"missing top-level key {key!r}")
        if not isinstance(data["utilities"], dict) or not data["utilities"]:
            raise ValueError("utilities must be a non-empty dict")
        if not isinstance(data["federal_minimum"], list) or not data["federal_minimum"]:
            raise ValueError("federal_minimum must be a non-empty list")


# ----- v0.13.0 (Phase 3) — drift detection ----------------------------------


def _scan_history_for_drift(entry: ConfigEntry) -> list[dict]:
    """Walk a config entry's ``OPT_CONFIG_HISTORY`` and return a descriptor
    per stale entry × rate-window-period.

    Drift signals (per the v0.13.0 decision tracker A4.3 / A4.4):
      - **missing_key**: a current rate window declares a ``user_inputs[]``
        key that is NOT present in the stored ``user_inputs`` dict. The
        user has a new decision to make (added opt-in, renamed key).
      - **stale_value**: a stored enum value is not in the current
        declaration's ``values`` list. The user must re-pick.

    Removed-key drift (stored key no longer declared) does NOT fire — the
    stored value becomes inert noise; no user action is needed.

    Returns a list of dicts, each with keys: ``entry_idx``, ``utility``,
    ``period_from`` (date), ``period_to`` (date | None), ``missing_keys``,
    ``stale_values``.
    """
    descriptors: list[dict] = []
    history = list((entry.options or {}).get(OPT_CONFIG_HISTORY) or [])
    if not history:
        return descriptors

    sorted_history = sorted(history, key=lambda r: r.get("valid_from") or "")
    for idx, rec in enumerate(sorted_history):
        valid_from_str = rec.get("valid_from")
        if not valid_from_str or valid_from_str == "1970-01-01":
            continue
        cfg = rec.get("config") or {}
        utility = cfg.get(CONF_ENERGIEVERSORGER)
        if not utility:
            continue
        stored_inputs = cfg.get(CONF_USER_INPUTS) or {}

        try:
            span_from = date.fromisoformat(valid_from_str)
        except ValueError:
            continue
        span_to: date | None = None
        if idx + 1 < len(sorted_history):
            next_vf = sorted_history[idx + 1].get("valid_from")
            if next_vf:
                try:
                    span_to = date.fromisoformat(next_vf)
                except ValueError:
                    pass

        try:
            periods = compute_user_inputs_periods(utility, span_from, span_to)
        except (KeyError, ValueError, LookupError):
            continue
        for period_from, period_to, rep_rate in periods:
            decls = rep_rate.get("user_inputs") or []
            missing_keys: list[str] = []
            stale_values: list[str] = []
            for decl in decls:
                key = decl.get("key")
                if key is None:
                    continue
                if key not in stored_inputs:
                    missing_keys.append(key)
                    continue
                if decl.get("type") == "enum":
                    valid_values = set(decl.get("values") or [])
                    if valid_values and stored_inputs[key] not in valid_values:
                        stale_values.append(key)
            if missing_keys or stale_values:
                descriptors.append({
                    "entry_idx": idx,
                    "utility": utility,
                    "period_from": period_from,
                    "period_to": period_to,
                    "missing_keys": missing_keys,
                    "stale_values": stale_values,
                })
    return descriptors


def _create_drift_issues(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Scan one config entry; surface each drift descriptor as an HA
    repair issue. Idempotent: re-runs replace earlier issues with the
    same ``issue_id`` (HA's issue registry semantics)."""
    from homeassistant.helpers.issue_registry import (
        IssueSeverity,
        async_create_issue,
    )

    descriptors = _scan_history_for_drift(entry)
    for desc in descriptors:
        period_from_iso = desc["period_from"].isoformat()
        period_to_iso = (
            desc["period_to"].isoformat()
            if desc["period_to"] is not None else "open"
        )
        issue_id = (
            f"drift_{entry.entry_id}_{desc['entry_idx']}_{period_from_iso}"
        )
        async_create_issue(
            hass,
            DOMAIN,
            issue_id,
            is_fixable=True,
            severity=IssueSeverity.WARNING,
            translation_key="tariff_drift",
            translation_placeholders={
                "utility": desc["utility"],
                "period_from": period_from_iso,
                "period_to": period_to_iso,
                "fields": ", ".join(
                    sorted(set(desc["missing_keys"] + desc["stale_values"]))
                ),
            },
            data={
                "entry_id": entry.entry_id,
                "entry_idx": desc["entry_idx"],
                "utility": desc["utility"],
                "period_from": period_from_iso,
                "period_to": (
                    desc["period_to"].isoformat()
                    if desc["period_to"] is not None else None
                ),
                "missing_keys": desc["missing_keys"],
                "stale_values": desc["stale_values"],
            },
        )
