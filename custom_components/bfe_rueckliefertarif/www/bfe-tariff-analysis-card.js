/**
 * BFE Rückliefertarif — Tarif-Analyse Lovelace card
 *
 * v0.21.0 — variable-granularity charts + dual time controls.
 *
 * TOP ROW: Year + Quarter dropdowns drive the breakdown table + active
 * config (single-period detail view).
 *
 * CHART ROW: Granularity dropdown (Jahr/Quartal/Monat/Tag/Stunde) +
 * time-range chip selector + custom range inputs drive the rate
 * sparkline + stacked-breakdown bar.
 *
 * Service: bfe_rueckliefertarif.get_breakdown — called twice per refresh
 * (one detail call with year+quarter, one history call with granularity
 * + range params). Both Promise.all-parallel.
 *
 * Auto-registered by the integration via frontend.add_extra_js_url.
 *
 * Usage:
 *   type: custom:bfe-tariff-analysis-card
 *   history_quarters: 8   # optional, default 8 (used when chart range = preset)
 */

const DOMAIN = "bfe_rueckliefertarif";
const SERVICE = "get_breakdown";

const CARD_VERSION = "0.21.6";

const HISTORY_QUARTERS_DEFAULT = 8;

// Time range presets — map to service-call params.
const RANGE_PRESETS = [
  { id: "last_4q",      label: "Letzte 4Q",       params: { last_n_quarters: 4  } },
  { id: "last_8q",      label: "Letzte 8Q",       params: { last_n_quarters: 8  } },
  { id: "last_12q",     label: "Letzte 12Q",      params: { last_n_quarters: 12 } },
  { id: "last_year",    label: "Letztes Jahr",    params: "last_year" },
  { id: "current_year", label: "Aktuelles Jahr",  params: "current_year" },
  { id: "last_3y",      label: "Letzte 3J",       params: "last_3y" },
  { id: "custom",       label: "Custom…",         params: "custom" },
];

const GRANULARITY_OPTIONS = [
  { value: "jahr",    label: "Jahr"    },
  { value: "quartal", label: "Quartal" },
  { value: "monat",   label: "Monat"   },
  { value: "tag",     label: "Tag"     },
  { value: "stunde",  label: "Stunde"  },
];

class BfeTariffAnalysisCard extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });
    this._config = {};
    this._hass = null;
    this._rendered = false;
    this._loading = false;
    this._error = null;
    this._response = null;     // detail call
    this._history = null;      // history call
    this._chartRate = null;
    this._chartStack = null;

    const now = new Date();
    this._year = now.getFullYear();
    this._quarter = Math.floor(now.getMonth() / 3) + 1;

    // Chart-side state — independent of detail-view selectors.
    this._chartState = {
      granularity: "quartal",
      range_preset: "last_8q",
      range_from: { year: now.getFullYear() - 1, quarter: 1 },
      range_to:   { year: this._year,            quarter: this._quarter },
    };
  }

  setConfig(config) {
    this._config = config || {};
  }

  set hass(hass) {
    const first = !this._hass;
    this._hass = hass;
    if (!this._rendered) {
      this._renderShell();
      this._rendered = true;
      if (first) {
        this._fetch();
      }
    }
  }

  _historyParams() {
    const cs = this._chartState;
    const out = { granularity: cs.granularity };
    if (cs.range_preset === "custom") {
      out.from_year = cs.range_from.year;
      out.from_quarter = cs.range_from.quarter;
      out.to_year = cs.range_to.year;
      out.to_quarter = cs.range_to.quarter;
      return out;
    }
    const preset = RANGE_PRESETS.find((p) => p.id === cs.range_preset);
    if (!preset) {
      out.last_n_quarters = HISTORY_QUARTERS_DEFAULT;
      return out;
    }
    if (typeof preset.params === "object" && preset.params !== null) {
      Object.assign(out, preset.params);
      return out;
    }
    // Computed presets — derived from current calendar
    const now = new Date();
    const curYear = now.getFullYear();
    const curQ = Math.floor(now.getMonth() / 3) + 1;
    if (preset.params === "current_year") {
      out.from_year = curYear;
      out.from_quarter = 1;
      out.to_year = curYear;
      out.to_quarter = 4;
    } else if (preset.params === "last_year") {
      out.from_year = curYear - 1;
      out.from_quarter = 1;
      out.to_year = curYear - 1;
      out.to_quarter = 4;
    } else if (preset.params === "last_3y") {
      out.from_year = curYear - 2;
      out.from_quarter = 1;
      out.to_year = curYear;
      out.to_quarter = curQ;
    } else {
      out.last_n_quarters = HISTORY_QUARTERS_DEFAULT;
    }
    return out;
  }

  async _fetch() {
    if (!this._hass || this._loading) return;
    const t0 = _bfeT("_fetch start");
    this._loading = true;
    this._error = null;
    try {
      try { this._renderBody(); } catch (e) { console.error("BFE card pre-render failed:", e); }
      _bfeT("calling get_breakdown × 2");
      const detailPromise = this._hass.callService(
        DOMAIN, SERVICE,
        { year: this._year, quarter: this._quarter },
        undefined, false, true,
      );
      const historyPromise = this._hass.callService(
        DOMAIN, SERVICE,
        this._historyParams(),
        undefined, false, true,
      );
      const [detail, history] = await Promise.all([detailPromise, historyPromise]);
      _bfeT(`services returned (Δ ${(performance.now() - t0).toFixed(0)}ms)`);
      this._response = detail?.response ?? detail;
      this._history = history?.response ?? history;
      console.log("[BFE] detail rows:", this._response?.rows?.length, "history rows:", this._history?.rows?.length);
    } catch (err) {
      console.error("[BFE] fetch failed after", (performance.now() - t0).toFixed(0), "ms:", err);
      this._error = err?.message || String(err);
      this._response = null;
      this._history = null;
    } finally {
      this._loading = false;
      try {
        this._renderBody();
        _bfeT(`render done (Δ ${(performance.now() - t0).toFixed(0)}ms total)`);
      } catch (e) {
        console.error("BFE card post-render failed:", e);
        const body = this.shadowRoot?.querySelector(".body");
        if (body) {
          body.innerHTML = `<div class="error">Render-Fehler: ${this._escape(e?.message || String(e))}</div>`;
        }
      }
    }
  }

  _renderShell() {
    this.shadowRoot.innerHTML = `
      <style>
        :host { display: block; }
        ha-card { padding: 0; }
        .header {
          display: flex; justify-content: space-between; align-items: center;
          padding: 16px 16px 0;
        }
        .header h2 {
          margin: 0; font-size: 1.2em; font-weight: 500;
        }
        .controls, .chart-controls {
          display: flex; gap: 8px; align-items: end;
          padding: 12px 16px; flex-wrap: wrap;
        }
        .controls label, .chart-controls label {
          display: flex; flex-direction: column; gap: 4px;
          font-size: 0.85em; color: var(--secondary-text-color);
        }
        .controls select, .controls button,
        .chart-controls select, .chart-controls input, .chart-controls button {
          font: inherit;
          padding: 6px 10px;
          border: 1px solid var(--divider-color, #ccc);
          border-radius: 4px;
          background: var(--card-background-color, #fff);
          color: var(--primary-text-color);
          min-width: 70px;
        }
        .controls button, .chart-controls button.refresh {
          background: var(--primary-color);
          color: var(--text-primary-color, #fff);
          border: none;
          cursor: pointer;
          font-weight: 500;
          min-width: 110px;
        }
        .controls button:hover, .chart-controls button.refresh:hover { opacity: 0.9; }
        .controls button:disabled, .chart-controls button.refresh:disabled { opacity: 0.5; cursor: wait; }
        .chips {
          display: flex; gap: 4px; flex-wrap: wrap;
          padding: 0 16px 8px;
        }
        .chips .chip {
          padding: 4px 10px;
          border: 1px solid var(--divider-color, #ccc);
          border-radius: 14px;
          background: transparent;
          color: var(--primary-text-color);
          cursor: pointer;
          font-size: 0.85em;
          transition: all 0.15s;
        }
        .chips .chip:hover { background: var(--secondary-background-color, #f5f5f5); }
        .chips .chip.active {
          background: var(--primary-color);
          color: var(--text-primary-color, #fff);
          border-color: var(--primary-color);
        }
        .custom-range {
          display: flex; gap: 8px; align-items: end; flex-wrap: wrap;
          padding: 0 16px 12px;
        }
        .custom-range.hidden { display: none; }
        .custom-range label {
          display: flex; flex-direction: column; gap: 4px;
          font-size: 0.85em; color: var(--secondary-text-color);
        }
        .custom-range input { min-width: 80px; max-width: 100px; }
        .body { padding: 0 16px 16px; }
        .loading, .error, .empty {
          padding: 24px 16px;
          text-align: center;
          color: var(--secondary-text-color);
        }
        .error { color: var(--error-color, #c00); }
        section { margin-top: 16px; }
        section h3 {
          margin: 0 0 8px;
          font-size: 0.95em;
          font-weight: 500;
          color: var(--secondary-text-color);
          text-transform: uppercase;
          letter-spacing: 0.04em;
        }
        section h4 {
          margin: 4px 0 6px;
          font-size: 0.85em;
          font-weight: 500;
          color: var(--secondary-text-color);
          font-style: italic;
        }
        .config-grid {
          display: grid;
          grid-template-columns: max-content 1fr;
          gap: 4px 12px;
          font-size: 0.92em;
        }
        .config-grid dt {
          color: var(--secondary-text-color);
        }
        .config-grid dd {
          margin: 0;
          color: var(--primary-text-color);
        }
        .config-warning {
          font-size: 0.85em;
          color: var(--warning-color, #ff9800);
          margin-top: 4px;
          font-style: italic;
        }
        table { width: 100%; border-collapse: collapse; font-size: 0.9em; }
        th, td {
          padding: 6px 8px;
          text-align: right;
          border-bottom: 1px solid var(--divider-color, #eee);
        }
        th:first-child, td:first-child { text-align: left; }
        th { color: var(--secondary-text-color); font-weight: 500; }
        td.estimate::after { content: " *"; color: var(--secondary-text-color); }
        .footnote {
          font-size: 0.8em;
          color: var(--secondary-text-color);
          margin-top: 8px;
        }
        .truncation-hint {
          font-size: 0.85em;
          color: var(--warning-color, #ff9800);
          padding: 8px 0;
        }
        ul.bonuses { padding-left: 20px; margin: 0; font-size: 0.9em; }
        ul.bonuses li { margin: 2px 0; }
        ul.bonuses .applied { color: var(--success-color, #6c0); font-weight: 500; }
        ul.bonuses .skipped { color: var(--secondary-text-color); }
        .chart-host { height: 200px; }
        .chart-host.tall { height: 260px; }
        .chart-fallback {
          padding: 16px;
          color: var(--secondary-text-color);
          font-size: 0.9em;
          font-style: italic;
        }
      </style>
      <ha-card>
        <div class="header">
          <h2>BFE Rückliefertarif — Tarif-Analyse</h2>
        </div>
        <div class="controls">
          <label>Jahr <select class="year"></select></label>
          <label>Quartal <select class="quarter"></select></label>
          <button class="refresh">Aktualisieren</button>
        </div>
        <div class="body"></div>
      </ha-card>
    `;

    const yearSelect = this.shadowRoot.querySelector(".year");
    const quarterSelect = this.shadowRoot.querySelector(".quarter");
    const refreshBtn = this.shadowRoot.querySelector(".controls .refresh");

    const currentYear = new Date().getFullYear();
    for (let y = currentYear - 5; y <= currentYear + 1; y++) {
      const opt = document.createElement("option");
      opt.value = String(y);
      opt.textContent = String(y);
      if (y === this._year) opt.selected = true;
      yearSelect.appendChild(opt);
    }
    for (let q = 1; q <= 4; q++) {
      const opt = document.createElement("option");
      opt.value = String(q);
      opt.textContent = `Q${q}`;
      if (q === this._quarter) opt.selected = true;
      quarterSelect.appendChild(opt);
    }

    yearSelect.addEventListener("change", (e) => {
      this._year = parseInt(e.target.value, 10);
    });
    quarterSelect.addEventListener("change", (e) => {
      this._quarter = parseInt(e.target.value, 10);
    });
    refreshBtn.addEventListener("click", () => this._fetch());
  }

  _disposeCharts() {
    try { this._chartRate?.destroy(); } catch (e) { /* ignore */ }
    try { this._chartStack?.destroy(); } catch (e) { /* ignore */ }
    this._chartRate = null;
    this._chartStack = null;
  }

  _renderBody() {
    const body = this.shadowRoot.querySelector(".body");
    const refreshBtn = this.shadowRoot.querySelector(".controls .refresh");
    refreshBtn.disabled = this._loading;
    refreshBtn.textContent = this._loading ? "Lädt…" : "Aktualisieren";

    this._disposeCharts();

    if (this._loading && !this._response) {
      body.innerHTML = `<div class="loading">Lade Tarifdaten…</div>`;
      return;
    }
    if (this._error) {
      body.innerHTML = `<div class="error">Fehler: ${this._escape(this._error)}<br><small>Prüfe Developer Tools → Services → ${DOMAIN}.${SERVICE}</small></div>`;
      return;
    }
    if (!this._response) {
      body.innerHTML = `<div class="empty">Keine Daten.</div>`;
      return;
    }

    const cfgToday = this._response.config || {};
    const rows = this._response.rows || [];
    // v0.21.0 — active config follows the SELECTED period (not always today's).
    // Look for a row matching the chosen year+quarter; fall back to today's
    // config block if no row found (e.g., quarter not yet imported).
    const detailPeriod = `${this._year}Q${this._quarter}`;
    const detailRow = rows.find((r) => r.period === detailPeriod);
    const cfg = this._buildEffectiveConfig(detailRow, cfgToday);
    const periodConfigDiffersFromToday =
      detailRow !== undefined && this._configDiffersFromToday(detailRow, cfgToday);

    let html = "";

    // Active configuration block — for the selected period
    html += `<section>`;
    html += `<h3>Aktive Konfiguration${detailRow ? ` (${this._escape(detailPeriod)})` : " (heute)"}</h3>`;
    html += `<dl class="config-grid">`;
    html += this._configRow("Versorger", cfg.utility_name || cfg.utility_key || "—");
    html += this._configRow("Anlage", `${this._fmt(cfg.kwp, 1)} kWp${cfg.eigenverbrauch ? " · EV" : " · keine EV"}${cfg.hkn_optin ? " · HKN" : " · keine HKN"}`);
    html += this._configRow("Abrechnung", cfg.billing || "—");
    html += this._configRow("Tarifmodell", cfg.base_model || "—");
    html += this._configRow("Mindestvergütung", `${cfg.floor_label || "—"} (${this._fmt(cfg.floor_rp_kwh, 2)} Rp/kWh)`);
    if (cfg.fixed_rp_kwh != null) {
      html += this._configRow("Fix-Tarif", `${this._fmt(cfg.fixed_rp_kwh, 2)} Rp/kWh`);
    }
    if (cfg.seasonal && (cfg.seasonal.summer_rp_kwh != null || cfg.seasonal.winter_rp_kwh != null)) {
      html += this._configRow("Saisonal", `Sommer ${this._fmt(cfg.seasonal.summer_rp_kwh, 2)} · Winter ${this._fmt(cfg.seasonal.winter_rp_kwh, 2)} Rp/kWh`);
    }
    if (cfg.fixed_ht_rp_kwh != null) {
      html += this._configRow("HT/NT", `HT ${this._fmt(cfg.fixed_ht_rp_kwh, 2)} · NT ${this._fmt(cfg.fixed_nt_rp_kwh, 2)} Rp/kWh`);
    }
    html += `</dl>`;
    if (periodConfigDiffersFromToday) {
      html += `<div class="config-warning">⚠ Konfiguration für ${this._escape(detailPeriod)} — heute aktive Konfiguration weicht ab.</div>`;
    }
    html += `</section>`;

    // Bonuses block
    const advertised = cfg.bonuses_active || [];
    if (advertised.length > 0) {
      html += `<section><h3>Boni</h3><ul class="bonuses">`;
      for (const b of advertised) {
        const value = b.kind === "multiplier_pct"
          ? `${b.multiplier_pct >= 100 ? "+" : "−"}${Math.abs(b.multiplier_pct - 100).toFixed(2)}%`
          : `${this._fmt(b.rate_rp_kwh, 2)} Rp/kWh`;
        const annotation = b.applies_when === "always" ? " (immer)" : (b.applies_when === "opt_in" ? " (opt-in)" : "");
        html += `<li class="skipped">${this._escape(b.name || "—")}: ${value}${annotation}</li>`;
      }
      html += `</ul></section>`;
    }

    // Per-period breakdown table (single quarter detail)
    if (!detailRow) {
      html += `<section><h3>Quartal ${this._escape(detailPeriod)}</h3>`;
      html += `<div class="empty">Noch keine Daten — führe zuerst <strong>Service: ${DOMAIN}.reimport_all_history</strong> aus, damit der Importer historische Quartale schreibt.</div>`;
      html += `</section>`;
    } else {
      html += `<section><h3>Aufschlüsselung — ${this._escape(detailPeriod)}</h3>`;
      html += `<table><thead><tr>
        <th>Periode</th>
        <th>Basis</th>
        <th>HKN</th>
        <th>Boni</th>
        <th>Total</th>
        <th>kWh</th>
        <th>CHF</th>
      </tr></thead><tbody>`;
      const r = detailRow;
      const isEst = r.is_current_estimate;
      html += `<tr>
        <td class="${isEst ? "estimate" : ""}">${this._escape(r.period)}</td>
        <td>${this._fmt(r.base_rp_kwh_avg, 3)}</td>
        <td>${this._fmt(r.hkn_rp_kwh_avg, 3)}</td>
        <td>${this._fmt(r.bonus_rp_kwh_avg, 3)}</td>
        <td><strong>${this._fmt(r.rate_rp_kwh_avg, 3)}</strong></td>
        <td>${this._fmt(r.total_kwh, 2)}</td>
        <td>${this._fmt(r.total_chf, 2)}</td>
      </tr>`;
      html += `</tbody></table>`;
      if (isEst) {
        html += `<div class="footnote">* Geschätzt — laufendes Quartal, BFE hat noch nicht publiziert.</div>`;
      }
      html += `</section>`;
    }

    // CHART CONTROLS — granularity + time range chips + custom range
    html += `<section><h3>Verlauf — Steuerung</h3>`;
    html += `<div class="chart-controls">`;
    html += `<label>Granularität <select class="granularity">`;
    for (const g of GRANULARITY_OPTIONS) {
      const sel = g.value === this._chartState.granularity ? " selected" : "";
      html += `<option value="${g.value}"${sel}>${this._escape(g.label)}</option>`;
    }
    html += `</select></label>`;
    html += `<button class="refresh chart-refresh">Aktualisieren</button>`;
    html += `</div>`;
    html += `<div class="chips">`;
    for (const p of RANGE_PRESETS) {
      const active = p.id === this._chartState.range_preset ? " active" : "";
      html += `<button class="chip${active}" data-preset="${p.id}">${this._escape(p.label)}</button>`;
    }
    html += `</div>`;
    const customHidden = this._chartState.range_preset === "custom" ? "" : " hidden";
    html += `<div class="custom-range${customHidden}">`;
    html += `<label>Von Jahr <input type="number" class="from-year" value="${this._chartState.range_from.year}" min="2020" max="2099"></label>`;
    html += `<label>Von Q <input type="number" class="from-quarter" value="${this._chartState.range_from.quarter}" min="1" max="4"></label>`;
    html += `<label>Bis Jahr <input type="number" class="to-year" value="${this._chartState.range_to.year}" min="2020" max="2099"></label>`;
    html += `<label>Bis Q <input type="number" class="to-quarter" value="${this._chartState.range_to.quarter}" min="1" max="4"></label>`;
    html += `</div>`;
    html += `</section>`;

    // History charts (v0.20.0)
    const historyRows = this._history?.rows || [];
    const truncatedTo = this._history?.truncated_to_quarters;
    const originalReq = this._history?.original_quarters_requested;
    if (truncatedTo != null && originalReq != null && truncatedTo < originalReq) {
      html += `<div class="truncation-hint">⚠ Zeitraum gekürzt: ${truncatedTo} von ${originalReq} Quartalen (Granularität-Limit für ${this._escape(this._chartState.granularity)}).</div>`;
    }
    if (historyRows.length > 0) {
      html += `<section><h3>Verlauf — Effektiver Tarif</h3>`;
      html += `<div class="chart-host" id="chart-rate"></div>`;
      html += `</section>`;
      html += `<section><h3>Verlauf — Aufschlüsselung pro Periode</h3>`;
      html += `<div class="chart-host tall" id="chart-stack"></div>`;
      html += `</section>`;
    } else if (this._history) {
      html += `<section><h3>Verlauf</h3><div class="empty">Keine Daten für gewählten Zeitraum.</div></section>`;
    }

    // Data source footer
    html += `<section><h3>Datenquelle</h3><dl class="config-grid">`;
    html += this._configRow("Tariffs DB", `v${cfgToday.tariffs_version || "—"} (${cfgToday.tariffs_source || "—"})`);
    html += this._configRow("Karte", `v${CARD_VERSION}`);
    html += `</dl></section>`;

    body.innerHTML = html;

    // Wire chart-controls events
    this._wireChartControls();

    // Mount charts after innerHTML is set
    if (historyRows.length > 0) {
      this._renderCharts(historyRows);
    }
  }

  _wireChartControls() {
    const root = this.shadowRoot;
    const granularitySelect = root.querySelector(".granularity");
    const chartRefresh = root.querySelector(".chart-refresh");
    const chips = root.querySelectorAll(".chip");
    const fromYear = root.querySelector(".from-year");
    const fromQuarter = root.querySelector(".from-quarter");
    const toYear = root.querySelector(".to-year");
    const toQuarter = root.querySelector(".to-quarter");

    granularitySelect?.addEventListener("change", (e) => {
      this._chartState.granularity = e.target.value;
    });
    chartRefresh?.addEventListener("click", () => this._fetch());
    chips.forEach((chip) => {
      chip.addEventListener("click", () => {
        this._chartState.range_preset = chip.dataset.preset;
        // Re-render to update chip-active state + show/hide custom inputs
        this._renderBody();
      });
    });
    fromYear?.addEventListener("change", (e) => {
      this._chartState.range_from.year = parseInt(e.target.value, 10) || 2020;
    });
    fromQuarter?.addEventListener("change", (e) => {
      const v = parseInt(e.target.value, 10);
      this._chartState.range_from.quarter = (v >= 1 && v <= 4) ? v : 1;
    });
    toYear?.addEventListener("change", (e) => {
      this._chartState.range_to.year = parseInt(e.target.value, 10) || 2099;
    });
    toQuarter?.addEventListener("change", (e) => {
      const v = parseInt(e.target.value, 10);
      this._chartState.range_to.quarter = (v >= 1 && v <= 4) ? v : 4;
    });
  }

  _buildEffectiveConfig(row, fallback) {
    if (!row) return fallback;
    return {
      utility_key: row.utility_key_at_period ?? fallback.utility_key,
      utility_name: row.utility_name_at_period ?? fallback.utility_name,
      kwp: row.kw_at_period ?? fallback.kwp,
      eigenverbrauch: row.eigenverbrauch_at_period ?? fallback.eigenverbrauch,
      hkn_optin: row.hkn_optin_at_period ?? fallback.hkn_optin,
      hkn_rp_kwh: fallback.hkn_rp_kwh,                // not on row
      billing: row.billing_at_period ?? fallback.billing,
      floor_label: row.floor_label_at_period ?? fallback.floor_label,
      floor_rp_kwh: row.floor_rp_kwh_at_period ?? fallback.floor_rp_kwh,
      base_model: row.base_model_at_period ?? fallback.base_model,
      cap_mode: row.cap_mode_at_period ?? fallback.cap_mode,
      cap_rp_kwh: row.cap_rp_kwh_at_period ?? fallback.cap_rp_kwh,
      tariffs_version: row.tariffs_version_at_period ?? fallback.tariffs_version,
      tariffs_source: row.tariffs_source_at_period ?? fallback.tariffs_source,
      seasonal: row.seasonal_at_period ?? fallback.seasonal,
      fixed_rp_kwh: row.fixed_rp_kwh_at_period ?? fallback.fixed_rp_kwh,
      fixed_ht_rp_kwh: row.fixed_ht_rp_kwh_at_period ?? fallback.fixed_ht_rp_kwh,
      fixed_nt_rp_kwh: row.fixed_nt_rp_kwh_at_period ?? fallback.fixed_nt_rp_kwh,
      bonuses_active: row.bonuses_active_at_period ?? fallback.bonuses_active,
      user_inputs: row.user_inputs_at_period ?? fallback.user_inputs,
    };
  }

  _configDiffersFromToday(row, today) {
    return (
      row.utility_key_at_period !== today.utility_key ||
      row.kw_at_period !== today.kwp ||
      row.eigenverbrauch_at_period !== today.eigenverbrauch ||
      row.hkn_optin_at_period !== today.hkn_optin ||
      row.billing_at_period !== today.billing
    );
  }

  async _renderCharts(historyRows) {
    let Apex;
    try {
      Apex = await _loadApexScoped();
    } catch (err) {
      console.error("BFE card: ApexCharts load failed:", err);
      const fallback = `<div class="chart-fallback">ApexCharts konnte nicht geladen werden: ${this._escape(err?.message || String(err))}<br><small>Hard-Refresh mit Ctrl+Shift+R hilft oft.</small></div>`;
      const r = this.shadowRoot.querySelector("#chart-rate");
      const s = this.shadowRoot.querySelector("#chart-stack");
      if (r) r.innerHTML = fallback;
      if (s) s.innerHTML = fallback;
      return;
    }

    // Sort rows OLDEST first
    const sorted = [...historyRows].sort((a, b) => {
      return String(a.period).localeCompare(String(b.period));
    });
    const categories = sorted.map((r) => r.period);
    const ratesRpKwh = sorted.map((r) => this._numOrNull(r.rate_rp_kwh_avg));
    const baseRpKwh = sorted.map((r) => this._numOrNull(r.base_rp_kwh_avg));
    const hknRpKwh = sorted.map((r) => this._numOrNull(r.hkn_rp_kwh_avg));
    const bonusRpKwh = sorted.map((r) => this._numOrNull(r.bonus_rp_kwh_avg));

    const themeMode = this._isDarkTheme() ? "dark" : "light";
    const granularity = this._chartState.granularity;
    const xaxisLabels = this._xaxisLabelsForGranularity(granularity, sorted.length);

    // Sparkline — effective rate over time
    const rateEl = this.shadowRoot.querySelector("#chart-rate");
    if (rateEl) {
      const opts = {
        chart: {
          type: "line",
          height: 200,
          background: "transparent",
          toolbar: { show: false },
          animations: { enabled: false },
          parentHeightOffset: 0,
        },
        theme: { mode: themeMode },
        series: [{ name: "Effektiv (Rp/kWh)", data: ratesRpKwh }],
        xaxis: { categories, labels: xaxisLabels },
        yaxis: {
          title: { text: "Rp/kWh", style: { fontSize: "11px" } },
          labels: { formatter: (v) => v == null ? "—" : Number(v).toFixed(2) },
          decimalsInFloat: 2,
        },
        stroke: { curve: "straight", width: 2 },
        markers: { size: granularity === "stunde" ? 0 : 4 },
        dataLabels: { enabled: false },
        tooltip: {
          shared: false,
          intersect: false,
          y: { formatter: (v) => v == null ? "—" : `${Number(v).toFixed(3)} Rp/kWh` },
        },
        grid: { borderColor: "var(--divider-color, #eee)" },
        colors: ["#03a9f4"],
      };
      this._chartRate = new Apex(rateEl, opts);
      try { await this._chartRate.render(); } catch (e) { console.error("BFE rate chart render:", e); }
    }

    // Stacked bar — base / HKN / bonus per period.
    // Hidden at hourly granularity (illegible with 2160 bars).
    const stackEl = this.shadowRoot.querySelector("#chart-stack");
    if (stackEl) {
      if (granularity === "stunde") {
        stackEl.innerHTML = `<div class="chart-fallback">Aufschlüsselung nicht angezeigt bei stündlicher Granularität (zu viele Datenpunkte).</div>`;
      } else {
        const opts = {
          chart: {
            type: "bar",
            stacked: true,
            height: 260,
            background: "transparent",
            toolbar: { show: false },
            animations: { enabled: false },
            parentHeightOffset: 0,
          },
          theme: { mode: themeMode },
          series: [
            { name: "Basis",   data: baseRpKwh },
            { name: "HKN",     data: hknRpKwh },
            { name: "Boni",    data: bonusRpKwh },
          ],
          xaxis: { categories, labels: xaxisLabels },
          yaxis: {
            title: { text: "Rp/kWh", style: { fontSize: "11px" } },
            labels: { formatter: (v) => v == null ? "—" : Number(v).toFixed(2) },
            decimalsInFloat: 2,
          },
          plotOptions: {
            bar: { columnWidth: "55%", borderRadius: 2 },
          },
          dataLabels: { enabled: false },
          tooltip: {
            // v0.21.0 — shared:true now works because intersect:false is set
            // (was the missing piece in v0.20.2 that caused the API throw).
            shared: true,
            intersect: false,
            y: { formatter: (v) => v == null ? "—" : `${Number(v).toFixed(3)} Rp/kWh` },
          },
          legend: { position: "top", horizontalAlign: "right", fontSize: "12px" },
          grid: { borderColor: "var(--divider-color, #eee)" },
          colors: ["#03a9f4", "#4caf50", "#ff9800"],
        };
        this._chartStack = new Apex(stackEl, opts);
        try { await this._chartStack.render(); } catch (e) { console.error("BFE stack chart render:", e); }
      }
    }
  }

  _xaxisLabelsForGranularity(granularity, count) {
    // Hide most labels for high-density granularities so the axis stays readable.
    const base = { style: { fontSize: "11px" } };
    if (granularity === "stunde") {
      return { ...base, rotate: -90, hideOverlappingLabels: true,
               formatter: (v, _, opts) => (opts?.i % 24 === 12) ? String(v).slice(11, 13) + ":00" : "" };
    }
    if (granularity === "tag") {
      return { ...base, rotate: -90, hideOverlappingLabels: true,
               formatter: (v, _, opts) => (opts?.i % 7 === 0) ? String(v).slice(5) : "" };
    }
    if (granularity === "monat") {
      return { ...base, rotate: -45 };
    }
    if (granularity === "jahr") {
      return { ...base, rotate: 0 };
    }
    return { ...base, rotate: -45 };  // quartal default
  }

  _isDarkTheme() {
    try {
      const bg = getComputedStyle(this).getPropertyValue("--card-background-color") || "";
      return /^\s*#[0-3]/.test(bg) || /^\s*rgb\(\s*[0-9]{1,2}\s*,/.test(bg);
    } catch (e) {
      return false;
    }
  }

  _numOrNull(v) {
    if (v == null) return null;
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }

  _configRow(label, value) {
    return `<dt>${this._escape(label)}</dt><dd>${this._escape(value)}</dd>`;
  }

  _fmt(value, digits) {
    if (value == null || Number.isNaN(value)) return "—";
    return Number(value).toFixed(digits);
  }

  _escape(s) {
    return String(s).replace(/[&<>"']/g, (c) => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;", "'": "&#39;",
    }[c]));
  }

  disconnectedCallback() {
    this._disposeCharts();
  }

  getCardSize() {
    // v0.21.0 — card grew with the new chart-controls section.
    return 26;
  }

  static getLayoutOptions() {
    // v0.20.1 — Sections-layout defaults so new placements span full width.
    return {
      grid_columns: 12,
      grid_rows: "auto",
      grid_min_columns: 6,
      grid_min_rows: 8,
    };
  }

  static getStubConfig() {
    return { type: `custom:bfe-tariff-analysis-card` };
  }
}

// v0.21.6 — continuous registration monitor.
//
// Diagnostics from v0.21.5 confirmed (DevTools after picker error):
//   customElements.get("bfe-tariff-analysis-card")          → undefined
//   customElements.whenDefined("bfe-tariff-analysis-card")  → pending forever
// even though our v0.21.5 verified-sync log fired immediately after define.
// Conclusion: something wipes/replaces the customElements registry AFTER
// our script registers but BEFORE the picker queries it. v0.21.5's
// one-shot polling loop stopped after first success and missed the wipe.
//
// Fix: monitor every 200ms FOREVER. If our class is missing from the
// registry, re-define. Cost is ~5 calls/sec and a registry lookup —
// negligible. The first-success log fires once; subsequent wipes log a
// warning so we can quantify the problem.
function _bfeDefine() {
  if (customElements.get("bfe-tariff-analysis-card") === BfeTariffAnalysisCard) {
    return false; // already present, no-op
  }
  try {
    customElements.define("bfe-tariff-analysis-card", BfeTariffAnalysisCard);
    return customElements.get("bfe-tariff-analysis-card") === BfeTariffAnalysisCard;
  } catch (_err) {
    return false;
  }
}

let _bfeRegisterCount = 0;
let _bfeWipeCount = 0;
let _bfePreviouslyRegistered = false;

function _bfeMonitorTick() {
  const present =
    customElements.get("bfe-tariff-analysis-card") === BfeTariffAnalysisCard;
  if (present) {
    _bfePreviouslyRegistered = true;
    return;
  }
  if (_bfePreviouslyRegistered) {
    _bfeWipeCount += 1;
    console.warn(
      `[BFE] registration was wiped (wipe #${_bfeWipeCount}) — re-defining`
    );
  }
  const ok = _bfeDefine();
  if (ok) {
    _bfeRegisterCount += 1;
    if (_bfeRegisterCount === 1) {
      console.info("[BFE] customElements registered OK (verified, sync).");
    } else {
      console.info(
        `[BFE] re-registered after wipe (total registers: ${_bfeRegisterCount})`
      );
    }
    _bfePreviouslyRegistered = true;
    _bfeRecover();
  }
}

_bfeMonitorTick();
setInterval(_bfeMonitorTick, 200);

window.customCards = window.customCards || [];
if (!window.customCards.some((c) => c.type === "bfe-tariff-analysis-card")) {
  window.customCards.push({
    type: "bfe-tariff-analysis-card",
    name: "BFE Rückliefertarif — Analyse",
    description: "Interaktive Tarifanalyse für BFE-Einspeisevergütung",
    preview: false,
  });
}

console.info(
  `%c BFE-TARIFF-ANALYSIS-CARD %c v${CARD_VERSION} `,
  "color: white; background: #4caf50; font-weight: 500;",
  "color: #4caf50; background: white; font-weight: 500;",
);

// v0.21.5 — recovery walk. Once registration actually sticks, walk the
// DOM (incl. shadow roots) for:
//   1. Dashboard error placeholders whose config references our card —
//      dispatch ll-rebuild (HA's hui-card listens for this and rebuilds
//      its slot, replacing the error with our newly-registered class).
//   2. hui-card-picker instances — call _loadCards() / requestUpdate() to
//      force re-render so the spinner-stuck preview retries our card.
function _bfeRecover() {
  const tag = "bfe-tariff-analysis-card";
  const matches = (cfg) =>
    cfg && (cfg.type === tag || cfg.type === `custom:${tag}`);
  const visit = (root, depth) => {
    if (!root || !root.querySelectorAll || depth > 20) return 0;
    let n = 0;
    for (const el of root.querySelectorAll("*")) {
      if (el.shadowRoot) n += visit(el.shadowRoot, depth + 1);
      const cfg = el._config || el.config;
      if (matches(cfg)) {
        el.dispatchEvent(
          new CustomEvent("ll-rebuild", { bubbles: true, composed: true })
        );
        n += 1;
      }
      const tagName = el.tagName?.toLowerCase();
      if (tagName === "hui-card-picker") {
        try {
          if (typeof el._loadCards === "function") el._loadCards();
          if (typeof el.requestUpdate === "function") el.requestUpdate();
          n += 1;
        } catch (_) { /* picker not yet hydrated, ignore */ }
      }
    }
    return n;
  };
  setTimeout(() => {
    const n = visit(document, 0);
    if (n > 0) console.info(`[BFE] recovery: dispatched on ${n} target(s)`);
  }, 100);
}

// Helpers — declared after registration so they don't delay define().
// Function declarations are hoisted, so class methods can still reference
// them via lexical closure even though they appear textually later.
const APEX_URL = "/api/bfe_rueckliefertarif/static/apexcharts.min.js";
let _apexPromise = null;
function _loadApexScoped() {
  if (_apexPromise) return _apexPromise;
  _apexPromise = (async () => {
    const tFetchStart = performance.now();
    const code = await fetch(APEX_URL).then((r) => {
      if (!r.ok) throw new Error(`Failed to fetch ApexCharts: ${r.status}`);
      return r.text();
    });
    const tFetchEnd = performance.now();
    console.log(`[BFE] ApexCharts bundle fetched (${(tFetchEnd - tFetchStart).toFixed(0)}ms, ${(code.length / 1024).toFixed(0)}KB)`);
    const factory = new Function(
      "module", "exports",
      code + "\nreturn (typeof module !== 'undefined' && module.exports) ? module.exports : (typeof ApexCharts !== 'undefined' ? ApexCharts : null);"
    );
    const tCompiled = performance.now();
    console.log(`[BFE] ApexCharts compiled via Function() (${(tCompiled - tFetchEnd).toFixed(0)}ms)`);
    const moduleObj = { exports: {} };
    const Apex = factory(moduleObj, moduleObj.exports);
    const tFactoryDone = performance.now();
    console.log(`[BFE] ApexCharts factory ran (${(tFactoryDone - tCompiled).toFixed(0)}ms)`);
    if (!Apex) throw new Error("ApexCharts UMD bundle did not export anything");
    return Apex;
  })();
  return _apexPromise;
}

function _bfeT(label) {
  const t = performance.now();
  console.log(`[BFE] ${label} @ ${t.toFixed(1)}ms`);
  return t;
}
