/**
 * BFE Rückliefertarif — Tarif-Analyse Lovelace card
 *
 * Calls service `bfe_rueckliefertarif.get_breakdown` (with year/quarter
 * params), renders the active configuration block + per-period breakdown
 * table + bonuses list. Auto-registered by the integration via
 * `frontend.add_extra_js_url` — no manual Lovelace resource setup.
 *
 * Vanilla JS, no build step. Targets HA frontend Lit 3 / 2025+.
 *
 * Usage in Lovelace YAML:
 *   type: custom:bfe-tariff-analysis-card
 */

const DOMAIN = "bfe_rueckliefertarif";
const SERVICE = "get_breakdown";

const CARD_VERSION = "0.19.0";

class BfeTariffAnalysisCard extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });
    this._config = {};
    this._hass = null;
    this._rendered = false;
    this._loading = false;
    this._error = null;
    this._response = null;
    const now = new Date();
    this._year = now.getFullYear();
    this._quarter = Math.floor(now.getMonth() / 3) + 1;
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

  async _fetch() {
    if (!this._hass || this._loading) return;
    this._loading = true;
    this._error = null;
    this._renderBody();
    try {
      const result = await this._hass.callService(
        DOMAIN,
        SERVICE,
        { year: this._year, quarter: this._quarter },
        undefined,
        false,
        true, // returnResponse
      );
      this._response = result?.response ?? result;
    } catch (err) {
      this._error = err?.message || String(err);
      this._response = null;
    }
    this._loading = false;
    this._renderBody();
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
        .controls {
          display: flex; gap: 8px; align-items: end;
          padding: 12px 16px; flex-wrap: wrap;
        }
        .controls label {
          display: flex; flex-direction: column; gap: 4px;
          font-size: 0.85em; color: var(--secondary-text-color);
        }
        .controls select, .controls button {
          font: inherit;
          padding: 6px 10px;
          border: 1px solid var(--divider-color, #ccc);
          border-radius: 4px;
          background: var(--card-background-color, #fff);
          color: var(--primary-text-color);
          min-width: 80px;
        }
        .controls button {
          background: var(--primary-color);
          color: var(--text-primary-color, #fff);
          border: none;
          cursor: pointer;
          font-weight: 500;
        }
        .controls button:hover { opacity: 0.9; }
        .controls button:disabled { opacity: 0.5; cursor: wait; }
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
        ul.bonuses { padding-left: 20px; margin: 0; font-size: 0.9em; }
        ul.bonuses li { margin: 2px 0; }
        ul.bonuses .applied { color: var(--success-color, #6c0); font-weight: 500; }
        ul.bonuses .skipped { color: var(--secondary-text-color); }
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
    const refreshBtn = this.shadowRoot.querySelector(".refresh");

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

  _renderBody() {
    const body = this.shadowRoot.querySelector(".body");
    const refreshBtn = this.shadowRoot.querySelector(".refresh");
    refreshBtn.disabled = this._loading;
    refreshBtn.textContent = this._loading ? "Lädt…" : "Aktualisieren";

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

    const cfg = this._response.config || {};
    const rows = this._response.rows || [];

    let html = "";

    // Active configuration block
    html += `<section><h3>Aktive Konfiguration (heute)</h3><dl class="config-grid">`;
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
    html += `</dl></section>`;

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

    // Per-period breakdown table
    if (rows.length === 0) {
      html += `<section><h3>Quartal ${this._year}Q${this._quarter}</h3>`;
      html += `<div class="empty">Noch keine Daten — führe zuerst <strong>Service: ${DOMAIN}.reimport_all_history</strong> aus, damit der Importer historische Quartale schreibt.</div>`;
      html += `</section>`;
    } else {
      html += `<section><h3>Aufschlüsselung pro Periode</h3>`;
      html += `<table><thead><tr>
        <th>Periode</th>
        <th>Basis</th>
        <th>HKN</th>
        <th>Boni</th>
        <th>Total</th>
        <th>kWh</th>
        <th>CHF</th>
      </tr></thead><tbody>`;
      let hasEstimate = false;
      for (const r of rows) {
        const isEst = r.is_current_estimate;
        if (isEst) hasEstimate = true;
        html += `<tr>
          <td class="${isEst ? "estimate" : ""}">${this._escape(r.period)}</td>
          <td>${this._fmt(r.base_rp_kwh_avg, 3)}</td>
          <td>${this._fmt(r.hkn_rp_kwh_avg, 3)}</td>
          <td>${this._fmt(r.bonus_rp_kwh_avg, 3)}</td>
          <td><strong>${this._fmt(r.rate_rp_kwh_avg, 3)}</strong></td>
          <td>${this._fmt(r.total_kwh, 2)}</td>
          <td>${this._fmt(r.total_chf, 2)}</td>
        </tr>`;
      }
      html += `</tbody></table>`;
      if (hasEstimate) {
        html += `<div class="footnote">* Geschätzt — laufendes Quartal, BFE hat noch nicht publiziert.</div>`;
      }
      html += `</section>`;
    }

    // Data source footer
    html += `<section><h3>Datenquelle</h3><dl class="config-grid">`;
    html += this._configRow("Tariffs DB", `v${cfg.tariffs_version || "—"} (${cfg.tariffs_source || "—"})`);
    html += this._configRow("Karte", `v${CARD_VERSION}`);
    html += `</dl></section>`;

    body.innerHTML = html;
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

  getCardSize() {
    return 5;
  }

  static getStubConfig() {
    return { type: `custom:bfe-tariff-analysis-card` };
  }
}

customElements.define("bfe-tariff-analysis-card", BfeTariffAnalysisCard);

window.customCards = window.customCards || [];
window.customCards.push({
  type: "bfe-tariff-analysis-card",
  name: "BFE Rückliefertarif — Analyse",
  description: "Interaktive Tarifanalyse für BFE-Einspeisevergütung",
  preview: false,
});

console.info(
  `%c BFE-TARIFF-ANALYSIS-CARD %c v${CARD_VERSION} `,
  "color: white; background: #4caf50; font-weight: 500;",
  "color: #4caf50; background: white; font-weight: 500;",
);
