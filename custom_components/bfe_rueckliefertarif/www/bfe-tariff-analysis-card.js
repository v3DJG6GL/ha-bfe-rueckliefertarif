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

const CARD_VERSION = "0.23.2";

const HISTORY_QUARTERS_DEFAULT = 8;

// Time range presets — map to service-call params.
//
// `group`: semantic bucket for the chip rendering — one row per
// group with a small label prefix (Aktuell / Letzte / Andere).
//
// `gran` (optional): natural granularity for that window. When set, picking
// the chip auto-flips the granularity dropdown so users don't end up with
// nonsensical combos like "Heute @ Jahr" (= no rows). Quarter-aligned
// presets leave `gran` undefined → user's manual choice is preserved.
//
// Fine-grained presets fetch the covering quarter range from the service
// and then `_filterHistoryToWindow` trims rows client-side to the exact
// calendar window (today / this ISO week / last calendar month / etc.).
const RANGE_PRESETS = [
  // Aktuell — windows that include "now"
  { id: "today",           label: "Heute",            group: "current", params: "today",           gran: "stunde" },
  { id: "current_week",    label: "Aktuelle Woche",   group: "current", params: "current_week",    gran: "tag"    },
  { id: "current_month",   label: "Aktueller Monat",  group: "current", params: "current_month",   gran: "tag"    },
  { id: "current_quarter", label: "Aktuelles Quartal",group: "current", params: "current_quarter", gran: "monat"  },
  { id: "current_year",    label: "Aktuelles Jahr",   group: "current", params: "current_year",    gran: "monat"  },
  // Letzte — previous full calendar window
  { id: "last_week",    label: "Letzte Woche",   group: "last", params: "last_week",    gran: "tag"    },
  { id: "last_month",   label: "Letzter Monat",  group: "last", params: "last_month",   gran: "tag"    },
  { id: "last_quarter", label: "Letztes Quartal",group: "last", params: "last_quarter", gran: "monat"  },
  { id: "last_year",    label: "Letztes Jahr",   group: "last", params: "last_year",    gran: "monat"  },
  // Andere — multi-period and custom
  { id: "last_4q",  label: "Letzte 4Q",  group: "other", params: { last_n_quarters: 4  }, gran: "quartal" },
  { id: "last_8q",  label: "Letzte 8Q",  group: "other", params: { last_n_quarters: 8  }, gran: "quartal" },
  { id: "last_12q", label: "Letzte 12Q", group: "other", params: { last_n_quarters: 12 }, gran: "quartal" },
  { id: "last_3y",  label: "Letzte 3J",  group: "other", params: "last_3y",               gran: "quartal" },
  { id: "custom",   label: "Custom…",    group: "other", params: "custom" },
];

const PRESET_GROUPS = [
  { id: "current", label: "Aktuell" },
  { id: "last",    label: "Letzte"  },
  { id: "other",   label: "Andere"  },
];

// Preset IDs that need client-side filtering after the service returns.
const FINE_WINDOWED_PRESETS = new Set([
  "today", "current_week", "last_week",
  "current_month", "last_month",
  "current_quarter", "last_quarter",
]);

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
    this._quarter = this._quarterOfDate(now);

    // Chart-side state — independent of detail-view selectors.
    // Custom preset uses range_from_date/range_to_date (HTML5 date pickers);
    // the legacy range_from/range_to year+quarter pair stays as a seed for
    // the date pickers' default values on first open, but is no longer the
    // source of truth for the service call.
    this._chartState = {
      granularity: "quartal",
      range_preset: "last_8q",
      range_from: { year: now.getFullYear() - 1, quarter: 1 },
      range_to:   { year: this._year,            quarter: this._quarter },
      range_from_date: null,   // "YYYY-MM-DD" once user picks
      range_to_date:   null,
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
    const now = new Date();
    const curYear = now.getFullYear();
    const curQ = this._quarterOfDate(now);

    if (cs.range_preset === "custom") {
      // Derive quarter range from date pickers; client filter trims rows
      // back down to the exact day window. Without dates yet, fall back to
      // current quarter (empty filter renders the empty state).
      const from = cs.range_from_date ? new Date(cs.range_from_date + "T00:00") : null;
      const to   = cs.range_to_date   ? new Date(cs.range_to_date   + "T00:00") : null;
      if (!from || !to || isNaN(from) || isNaN(to)) {
        out.from_year = curYear; out.from_quarter = curQ;
        out.to_year = curYear;   out.to_quarter = curQ;
      } else {
        out.from_year = from.getFullYear(); out.from_quarter = this._quarterOfDate(from);
        out.to_year   = to.getFullYear();   out.to_quarter   = this._quarterOfDate(to);
      }
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
    } else if (
      preset.params === "today" ||
      preset.params === "current_week" ||
      preset.params === "current_month" ||
      preset.params === "current_quarter"
    ) {
      // Fine-grained windows entirely inside the current quarter.
      out.from_year = curYear; out.from_quarter = curQ;
      out.to_year = curYear;   out.to_quarter = curQ;
    } else if (preset.params === "last_week" || preset.params === "last_month") {
      // Previous week/month may straddle a quarter boundary.
      // Request both quarters; client filter trims to the exact window.
      const prevQYear = curQ === 1 ? curYear - 1 : curYear;
      const prevQ     = curQ === 1 ? 4           : curQ - 1;
      out.from_year = prevQYear; out.from_quarter = prevQ;
      out.to_year = curYear;     out.to_quarter = curQ;
    } else if (preset.params === "last_quarter") {
      // Previous quarter only (rolls into prev year for Q1).
      const prevQYear = curQ === 1 ? curYear - 1 : curYear;
      const prevQ     = curQ === 1 ? 4           : curQ - 1;
      out.from_year = prevQYear; out.from_quarter = prevQ;
      out.to_year   = prevQYear; out.to_quarter   = prevQ;
    } else {
      out.last_n_quarters = HISTORY_QUARTERS_DEFAULT;
    }
    return out;
  }

  async _fetch() {
    if (!this._hass || this._loading) return;
    this._loading = true;
    this._error = null;
    try {
      try { this._renderBody(); } catch (e) { console.error("BFE card pre-render failed:", e); }
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
      this._response = detail?.response ?? detail;
      this._history = history?.response ?? history;
    } catch (err) {
      console.error("[BFE] fetch failed:", err);
      this._error = err?.message || String(err);
      this._response = null;
      this._history = null;
    } finally {
      this._loading = false;
      try {
        this._renderBody();
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
        .chip-groups {
          display: flex; flex-direction: column; gap: 4px;
          padding: 0 16px 8px;
        }
        .chip-group {
          display: flex; gap: 8px; align-items: center;
        }
        .chip-group-label {
          font-size: 0.7em; font-weight: 600;
          color: var(--secondary-text-color);
          min-width: 60px;
          text-transform: uppercase;
          letter-spacing: 0.05em;
        }
        .chip-group .chips {
          padding: 0;
          flex: 1;
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
        .chart-estimate-footnote {
          padding: 0 16px 12px;
          color: var(--secondary-text-color);
          font-size: 0.8em;
          font-style: italic;
          line-height: 1.4;
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
    // Active config follows the SELECTED period (not always today's).
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
    html += `<h3>Konfiguration${detailRow ? ` (${this._escape(detailPeriod)})` : " (heute)"}</h3>`;
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
    // Boni inline — only renders when the chosen period actually had Boni
    // (the at-period fallback in _buildEffectiveConfig prevents today's
    // list from leaking into past quarters that had none).
    const advertised = cfg.bonuses_active || [];
    if (advertised.length > 0) {
      const parts = advertised.map((b) => {
        const value = b.kind === "multiplier_pct"
          ? `${b.multiplier_pct >= 100 ? "+" : "−"}${Math.abs(b.multiplier_pct - 100).toFixed(2)}%`
          : `${this._fmt(b.rate_rp_kwh, 2)} Rp/kWh`;
        // Derive a human-readable annotation from both `applies_when` and
        // the `when` clause. A bonus may be season-gated, user-input-gated,
        // or unconditional — surface that so the card's "(immer)" never
        // contradicts the importer's hourly evaluate_when() decision.
        const labels = [];
        if (b.applies_when === "opt_in") labels.push("opt-in");
        if (b.when?.season === "summer") labels.push("Sommer");
        if (b.when?.season === "winter") labels.push("Winter");
        if (b.when?.user_inputs && b.applies_when !== "opt_in") labels.push("bedingt");
        if (labels.length === 0) labels.push("immer");
        const annotation = ` (${labels.join(", ")})`;
        return `${b.name || "—"}: ${value}${annotation}`;
      });
      html += this._configRow("Boni", parts.join(" · "));
    }
    html += `</dl>`;
    if (periodConfigDiffersFromToday) {
      html += `<div class="config-warning">⚠ Konfiguration für ${this._escape(detailPeriod)} — heute aktive Konfiguration weicht ab.</div>`;
    }
    html += `</section>`;

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
    html += `<div class="chip-groups">`;
    for (const group of PRESET_GROUPS) {
      const groupPresets = RANGE_PRESETS.filter((p) => p.group === group.id);
      if (groupPresets.length === 0) continue;
      html += `<div class="chip-group">`;
      html += `<span class="chip-group-label">${this._escape(group.label)}</span>`;
      html += `<div class="chips">`;
      for (const p of groupPresets) {
        const active = p.id === this._chartState.range_preset ? " active" : "";
        html += `<button class="chip${active}" data-preset="${p.id}">${this._escape(p.label)}</button>`;
      }
      html += `</div></div>`;
    }
    html += `</div>`;
    const customHidden = this._chartState.range_preset === "custom" ? "" : " hidden";
    // Custom uses HTML5 date pickers — lets the user view Month/Week/Day
    // windows. Service still receives quarter range; client filter trims
    // to exact days.
    const fromDateVal = this._chartState.range_from_date || "";
    const toDateVal   = this._chartState.range_to_date   || "";
    html += `<div class="custom-range${customHidden}">`;
    html += `<label>Von Datum <input type="date" class="from-date" value="${this._escape(fromDateVal)}"></label>`;
    html += `<label>Bis Datum <input type="date" class="to-date" value="${this._escape(toDateVal)}"></label>`;
    html += `</div>`;
    html += `</section>`;

    // History charts
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
      // Visible only when at least one displayed period was computed from
      // the federal Mindestvergütung floor (running quarter, BFE not yet
      // published). Shown beneath the charts as a single footnote rather
      // than per-bar markers — keeps the visual signal unobtrusive while
      // still being honest about the estimate.
      html += `<div class="chart-estimate-footnote" hidden></div>`;
    } else if (this._history) {
      html += `<section><h3>Verlauf</h3><div class="empty">Keine Daten für gewählten Zeitraum.</div></section>`;
    }

    // Data source footer
    html += `<section><h3>Datenquelle</h3><dl class="config-grid">`;
    html += this._configRow("Tariffs DB", `v${cfgToday.tariffs_version || "—"} (${cfgToday.tariffs_source || "—"})`);
    html += this._configRow("Integration", `v${CARD_VERSION}`);
    html += `</dl></section>`;

    body.innerHTML = html;

    // Wire chart-controls events
    this._wireChartControls();

    // Mount charts after innerHTML is set
    if (historyRows.length > 0) {
      this._renderCharts(historyRows);
      // Surface the running-quarter estimate hint when any of the displayed
      // history rows was computed from the federal floor. Same wording as
      // the detail-table footnote so users get a single shared explanation
      // across both surfaces.
      const hasEst = historyRows.some((r) => r.is_current_estimate === true);
      const footEl = this.shadowRoot.querySelector(".chart-estimate-footnote");
      if (footEl) {
        if (hasEst) {
          footEl.textContent = "* Eine angezeigte Periode ist eine Schätzung — laufendes Quartal, BFE hat noch nicht publiziert. Mindestvergütung wird als Untergrenze angesetzt.";
          footEl.hidden = false;
        } else {
          footEl.hidden = true;
        }
      }
    }
  }

  _wireChartControls() {
    const root = this.shadowRoot;
    const granularitySelect = root.querySelector(".granularity");
    const chartRefresh = root.querySelector(".chart-refresh");
    const chips = root.querySelectorAll(".chip");
    const fromDate = root.querySelector(".from-date");
    const toDate   = root.querySelector(".to-date");

    granularitySelect?.addEventListener("change", (e) => {
      this._chartState.granularity = e.target.value;
    });
    chartRefresh?.addEventListener("click", () => this._fetch());
    chips.forEach((chip) => {
      chip.addEventListener("click", () => {
        const presetId = chip.dataset.preset;
        this._chartState.range_preset = presetId;
        // Auto-set granularity to the preset's natural value (Heute → Stunde,
        // Woche/Monat → Tag, Quartal → Monat). Quarter-aligned presets have
        // no `gran` so the user's manual choice is preserved. Re-fetch
        // because both granularity and quarter range likely changed.
        const preset = RANGE_PRESETS.find((p) => p.id === presetId);
        if (preset?.gran) {
          this._chartState.granularity = preset.gran;
        }
        this._fetch();
      });
    });
    // Custom date pickers. Re-fetch on change because the chosen dates may
    // need a different quarter range from the service.
    fromDate?.addEventListener("change", (e) => {
      this._chartState.range_from_date = e.target.value || null;
      if (this._chartState.range_to_date) this._fetch();
    });
    toDate?.addEventListener("change", (e) => {
      this._chartState.range_to_date = e.target.value || null;
      if (this._chartState.range_from_date) this._fetch();
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
      cap_rp_kwh: row.cap_rp_kwh_at_period ?? fallback.cap_rp_kwh,
      tariffs_version: row.tariffs_version_at_period ?? fallback.tariffs_version,
      tariffs_source: row.tariffs_source_at_period ?? fallback.tariffs_source,
      // Mutually exclusive model-specific fields — never fall back to
      // today's config, otherwise today's seasonal/HT-NT/fixed leaks
      // into historical rows whose base_model differs (same rationale
      // as bonuses_active below).
      seasonal: row.seasonal_at_period ?? null,
      fixed_rp_kwh: row.fixed_rp_kwh_at_period ?? null,
      fixed_ht_rp_kwh: row.fixed_ht_rp_kwh_at_period ?? null,
      fixed_nt_rp_kwh: row.fixed_nt_rp_kwh_at_period ?? null,
      // When a historical row is missing this field (older imports that
      // pre-date the at_period bonus snapshot), assume "no boni at that
      // time" rather than leaking today's array into the past quarter's view.
      bonuses_active: row.bonuses_active_at_period ?? [],
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
      this._setChartFallback(`<div class="chart-fallback">ApexCharts konnte nicht geladen werden: ${this._escape(err?.message || String(err))}<br><small>Hard-Refresh mit Ctrl+Shift+R hilft oft.</small></div>`);
      return;
    }

    // Sort rows OLDEST first, then trim to the active window for fine-grained
    // presets (Heute / Letzte Woche / etc.) and Custom date ranges. Quarter-
    // aligned presets pass through unchanged (helper returns input).
    const sortedAll = [...historyRows].sort((a, b) => {
      return String(a.period).localeCompare(String(b.period));
    });
    const sorted = this._filterHistoryToWindow(sortedAll, this._chartState);
    if (sorted.length === 0) {
      // For current_* windows the most likely cause is that BFE hasn't
      // published the in-progress quarter yet (publication lags ~6 weeks
      // after quarter end). Hint at it so the user doesn't think the chart
      // is broken.
      const presetId = this._chartState.range_preset;
      const isCurrent = presetId && presetId.startsWith("current_");
      const hint = isCurrent
        ? "Keine Daten für gewählten Zeitraum — BFE-Veröffentlichung für das laufende Quartal steht noch aus."
        : "Keine Daten für gewählten Zeitraum.";
      this._setChartFallback(`<div class="chart-fallback">${this._escape(hint)}</div>`);
      return;
    }
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
            // Custom HTML so we can show a combined Total row at the bottom
            // (the effective Rückliefertarif for that period). Apex renders
            // the tooltip outside our shadow root, so styles must be inline.
            // shared:true requires intersect:false to avoid an API throw.
            shared: true,
            intersect: false,
            custom: ({ series, dataPointIndex, w }) => {
              const seriesNames = w.globals.seriesNames || [];
              const colors = w.globals.colors || [];
              const period =
                w.globals.categoryLabels?.[dataPointIndex] ??
                w.globals.labels?.[dataPointIndex] ?? "";
              let total = 0;
              let hasAny = false;
              let rowsHtml = "";
              for (let i = 0; i < seriesNames.length; i++) {
                const v = series[i]?.[dataPointIndex];
                const n = (v == null || Number.isNaN(v)) ? null : Number(v);
                if (n != null) { total += n; hasAny = true; }
                const valStr = n == null ? "—" : `${n.toFixed(3)} Rp/kWh`;
                rowsHtml +=
                  `<div style="display:flex;align-items:center;gap:8px;padding:2px 0">` +
                    `<span style="display:inline-block;width:10px;height:10px;border-radius:2px;background:${colors[i] || "#999"}"></span>` +
                    `<span style="flex:1">${seriesNames[i]}</span>` +
                    `<span style="font-variant-numeric:tabular-nums">${valStr}</span>` +
                  `</div>`;
              }
              const totalStr = hasAny ? `${total.toFixed(3)} Rp/kWh` : "—";
              return (
                `<div style="padding:8px 12px;font-size:12px;min-width:200px">` +
                  `<div style="font-weight:500;margin-bottom:6px;border-bottom:1px solid var(--divider-color,#ddd);padding-bottom:4px">${period}</div>` +
                  rowsHtml +
                  `<div style="display:flex;align-items:center;gap:8px;padding:4px 0 0;margin-top:4px;border-top:1px solid var(--divider-color,#ddd);font-weight:600">` +
                    `<span style="flex:1">Total</span>` +
                    `<span style="font-variant-numeric:tabular-nums">${totalStr}</span>` +
                  `</div>` +
                `</div>`
              );
            },
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

  _setChartFallback(html) {
    const r = this.shadowRoot.querySelector("#chart-rate");
    const s = this.shadowRoot.querySelector("#chart-stack");
    if (r) r.innerHTML = html;
    if (s) s.innerHTML = html;
  }

  _quarterOfDate(d) {
    return Math.floor(d.getMonth() / 3) + 1;
  }

  // Trim already-sorted rows to the calendar window implied by the active
  // preset (or by the Custom date pickers). Quarter-aligned presets and an
  // unset Custom (no dates picked) pass through unchanged. Day-aligned
  // windows; sub-day skew (DST, UTC vs local) is invisible at this
  // granularity.
  _filterHistoryToWindow(rows, cs) {
    const presetId = cs.range_preset;
    const isCustomDated =
      presetId === "custom" && cs.range_from_date && cs.range_to_date;
    if (!FINE_WINDOWED_PRESETS.has(presetId) && !isCustomDated) return rows;

    const now = new Date();
    const startOfDay = (d) => {
      const x = new Date(d);
      x.setHours(0, 0, 0, 0);
      return x;
    };
    const startOfIsoWeek = (d) => {
      const x = startOfDay(d);
      const dow = (x.getDay() + 6) % 7;   // Mon=0 … Sun=6
      x.setDate(x.getDate() - dow);
      return x;
    };

    let from, to;
    if (isCustomDated) {
      from = new Date(cs.range_from_date + "T00:00");
      to   = new Date(cs.range_to_date   + "T00:00");
      to.setDate(to.getDate() + 1);   // make Bis Datum inclusive
    } else if (presetId === "today") {
      from = startOfDay(now);
      to   = new Date(from); to.setDate(to.getDate() + 1);
    } else if (presetId === "current_week") {
      from = startOfIsoWeek(now);
      to   = new Date(from); to.setDate(to.getDate() + 7);
    } else if (presetId === "last_week") {
      to   = startOfIsoWeek(now);
      from = new Date(to); from.setDate(from.getDate() - 7);
    } else if (presetId === "current_month") {
      from = new Date(now.getFullYear(), now.getMonth(),     1);
      to   = new Date(now.getFullYear(), now.getMonth() + 1, 1);
    } else if (presetId === "last_month") {
      from = new Date(now.getFullYear(), now.getMonth() - 1, 1);
      to   = new Date(now.getFullYear(), now.getMonth(),     1);
    } else if (presetId === "current_quarter") {
      const qStartMonth = Math.floor(now.getMonth() / 3) * 3;
      from = new Date(now.getFullYear(), qStartMonth,     1);
      to   = new Date(now.getFullYear(), qStartMonth + 3, 1);
    } else if (presetId === "last_quarter") {
      // Previous quarter. JS Date constructor rolls negative months back
      // into the previous year automatically (curQStartMonth - 3 → -3 in
      // Q1 → resolves to October of the previous year).
      const curQStartMonth = Math.floor(now.getMonth() / 3) * 3;
      from = new Date(now.getFullYear(), curQStartMonth - 3, 1);
      to   = new Date(now.getFullYear(), curQStartMonth,     1);
    } else {
      return rows;
    }

    return rows.filter((r) => {
      const p = String(r.period);
      const ts = new Date(p.length <= 10 ? p + "T00:00" : p);
      return ts >= from && ts < to;
    });
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
    return 26;
  }

  getLayoutOptions() {
    // Must be an instance method (not static) for HA to find it; grid_max_*
    // exposes drag handles in Sections view.
    return {
      grid_columns: 12,
      grid_rows: "auto",
      grid_min_columns: 6,
      grid_max_columns: 12,
      grid_min_rows: 8,
      grid_max_rows: 40,
    };
  }

  static getStubConfig() {
    return { type: `custom:bfe-tariff-analysis-card` };
  }

  static getConfigElement() {
    return document.createElement("bfe-tariff-analysis-card-editor");
  }
}

// Minimal editor element so HA's edit-card panel renders cleanly instead
// of the red "Visual editor not supported" notification. The card has no
// editable options today; resize/position is handled by Sections.
class BfeTariffAnalysisCardEditor extends HTMLElement {
  setConfig(_config) { /* no editable options */ }
  set hass(_hass) {
    if (this._rendered) return;
    this._rendered = true;
    if (!this.shadowRoot) this.attachShadow({ mode: "open" });
    this.shadowRoot.innerHTML = `
      <style>
        :host { display: block; padding: 16px; color: var(--secondary-text-color); font-size: 0.9em; }
      </style>
      <div>
        Diese Karte hat keine konfigurierbaren Optionen.
        Größe und Position lassen sich über das Sections-Layout verändern.
      </div>`;
  }
}
if (!customElements.get("bfe-tariff-analysis-card-editor")) {
  customElements.define("bfe-tariff-analysis-card-editor", BfeTariffAnalysisCardEditor);
}

// Continuous registration monitor — workaround for HA frontend bug where
// lovelace-loader queries before our customElements.define() resolves.
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

// Recovery walk — once registration actually sticks, walk the DOM (incl.
// shadow roots) for:
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
    const code = await fetch(APEX_URL).then((r) => {
      if (!r.ok) throw new Error(`Failed to fetch ApexCharts: ${r.status}`);
      return r.text();
    });
    // Wrap the UMD bundle in a Function() factory so its top-level scope
    // doesn't leak window.ApexCharts (which would conflict with
    // RomRider/apexcharts-card's bundled copy).
    const factory = new Function(
      "module", "exports",
      code + "\nreturn (typeof module !== 'undefined' && module.exports) ? module.exports : (typeof ApexCharts !== 'undefined' ? ApexCharts : null);"
    );
    const moduleObj = { exports: {} };
    const Apex = factory(moduleObj, moduleObj.exports);
    if (!Apex) throw new Error("ApexCharts UMD bundle did not export anything");
    return Apex;
  })();
  return _apexPromise;
}
