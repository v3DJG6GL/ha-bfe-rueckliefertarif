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
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .tariffs_db import load_tariffs, set_override_path

if TYPE_CHECKING:
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
        return True

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
