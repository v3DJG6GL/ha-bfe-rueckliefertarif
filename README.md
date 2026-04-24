# BFE Rückliefertarif für Home Assistant

[![validate](https://github.com/v3DJG6GL/ha-bfe-rueckliefertarif/actions/workflows/validate.yml/badge.svg)](https://github.com/v3DJG6GL/ha-bfe-rueckliefertarif/actions/workflows/validate.yml)

Home-Assistant-Custom-Integration für den Schweizer **PV-Rückliefertarif gemäss Art. 15 EnFV** (ab 1. Januar 2026).

Ab 2026 bildet der vom BFE **quartalsweise** publizierte Referenz-Marktpreis die gesetzliche Basis für die Abnahmevergütung aller Schweizer Energieversorger. Da dieser Preis immer erst ~2 Wochen **nach Quartalsende** publiziert wird, kann Home Assistant die Rückliefervergütung nicht nativ korrekt darstellen — die automatisch erzeugte Kompensations-Statistik (`sensor.<export>_compensation`) ist zum Zeitpunkt der Speicherung eingefroren.

Diese Integration schreibt diese Langzeitstatistik nach jeder BFE-Publikation **rückwirkend neu**, direkt über die offizielle HA-Recorder-API. Das Energy-Dashboard zeigt danach beim nächsten Laden die korrekten Kompensationsbeträge.

## Features

- Vollständige Umsetzung des ab 1.1.2026 gültigen gesetzlichen Tarifgefüges:
  - **Basisvergütung** = BFE Referenz-Marktpreis (EnG Art. 15, EnFV Art. 15)
  - **Mindestvergütung** nach Segment (EnV Art. 12 Abs. 1bis): 6.00 Rp/kWh bis 30 kW; degressive Formel `180 ÷ kW` bei 30–<150 kW mit Eigenverbrauch; 6.20 Rp/kWh ohne Eigenverbrauch; keine Mindestvergütung ab 150 kW
  - **Anrechenbarkeitsgrenze** (StromVV Art. 4a 4-Tier-Tabelle): 10.96 / 8.20 / 7.20 / 5.40 Rp/kWh je nach Grösse × Eigenverbrauch
  - **HKN-Vergütung**: nutzerkonfigurierbar
- **Zwei Basismodi**: BFE-RMP-Durchreichung (EKZ, BKW, CKW, Groupe E, Romande Energie, SAK, SGSW) und Fixpreis (ewz, IWB, SIG, AEW) — plus Custom.
- **Zwei Abrechnungsmodi**: quartalsweise (Standard für die meisten Kleinverbraucher) und monatlich (mit M3-Korrektur gemäss EKZ-Logik) — beide erfüllen garantiert `Σ(Kompensation) = Q_kWh × Q_Rate` pro Quartal.
- **DataUpdateCoordinator**: 6-stündliche Abfrage der BFE-CSVs, automatisches Re-Importieren neu publizierter Quartale.
- **Services**: `reimport_quarter`, `reimport_range`, `reimport_all_history`, `refresh` — manuell aus Developer Tools → Actions aufrufbar.
- **Transition-Spike-Fix**: Nachfolgende LTS-Einträge werden nach einer Neuberechnung automatisch verschoben, damit das Energy-Dashboard keinen Ausreisser an der Quartalsgrenze anzeigt.
- **11 EVU-Presets + Custom** mit vorausgefüllten Werten.
- **i18n**: DE / FR / IT / EN config flow.

## Installation

### Via HACS (empfohlen)

1. HACS → Integrations → oben rechts ⋮ → **Custom repositories**
2. URL: `https://github.com/v3DJG6GL/ha-bfe-rueckliefertarif`, Category: `Integration`
3. Integration `BFE Rückliefertarif (Art. 15 EnFV)` installieren
4. Home Assistant neustarten
5. Einstellungen → Geräte & Dienste → **+ Integration hinzufügen** → `BFE Rückliefertarif`

### Manuell

Repo-Verzeichnis `custom_components/bfe_rueckliefertarif/` nach `<config>/custom_components/` kopieren, HA neustarten.

## Konfiguration

Der Config-Flow führt dich durch drei Schritte:

1. **Energieversorger** — eines von 11 Presets (EKZ, BKW, CKW, Groupe E, Romande Energie, SAK, SGSW, ewz, IWB, SIG, AEW) oder `Custom`. Das Preset füllt Basismodus und HKN-Standardwert vor.
2. **Anlagedetails** — Segment (≤30 kW mit/ohne Eigenverbrauch, 30–<100, 100–<150, ≥150 kW), installierte Leistung in kWp (bei degressivem Segment erforderlich), HKN-Vergütung in Rp/kWh, optional Fixpreis, Abrechnungsmodus (quartalsweise/monatlich).
3. **Entitäten** — deine Netzrückspeisungs-Entität (kWh) und die vom Energy-Dashboard automatisch erzeugte Kompensations-Entität (CHF).

Nach dem Setup läuft die Integration autonom. Erste Re-Importierung: `Entwicklerwerkzeuge → Actions → bfe_rueckliefertarif.reimport_all_history`.

## Unterstützte Presets

| EVU | Basismodus | HKN 2026 | Notiz |
|---|---|---|---|
| EKZ | RMP | 3.0 Rp/kWh | HKN opt-in |
| BKW | RMP | 2.0 Rp/kWh | nur naturemade star, ab Q2 2026 |
| CKW | RMP | 3.0 Rp/kWh | |
| Groupe E | RMP | 4.0 Rp/kWh | |
| Romande Energie | RMP | 0.0 Rp/kWh | Q1 2026 war 1.5 |
| SAK | RMP | 3.0 Rp/kWh | |
| SGSW | RMP | 3.0 Rp/kWh | |
| ewz | Fixpreis | 3.0 Rp/kWh | ~12.91 HT-Durchschnitt 2026 |
| IWB | Fixpreis | 3.0 Rp/kWh | 14.0 Fixpreis (Basel politisch) |
| SIG | Fixpreis | inkl. | 10.96 (am Cap) |
| AEW | Fixpreis | inkl. | 8.2 HKN-inklusive |
| Custom | frei wählbar | 0 | Wert selber setzen |

HKN-Vergütungen sind markt-getrieben und ändern pro Quartal — User aktualisiert den Wert im Config-Flow, wenn der EVU ihn anpasst.

## Validierung

Nach einem Re-Import kannst du im Energy-Dashboard zum Quartal navigieren; die CHF-Gesamtsumme muss mit deiner EKZ-Abrechnung übereinstimmen.

SQL-Prüfung (sqlite3 oder phpMyAdmin):

```sql
SELECT datetime(start_ts,'unixepoch','localtime') AS t, state, sum
FROM statistics
JOIN statistics_meta ON statistics.metadata_id = statistics_meta.id
WHERE statistic_id = 'sensor.<deine>_compensation'
  AND start_ts BETWEEN strftime('%s','2026-01-01') AND strftime('%s','2026-04-01')
ORDER BY start_ts;
```

Erwartete `sum`-Spalte: monoton nicht-fallend, keine Duplikate, keine Lücken.

## Quellen

- [BFE Art. 15 EnFV Referenz-Marktpreise (OGD)](https://opendata.swiss/de/dataset/referenz-marktpreise-gemass-art-15-enfv)
- [EnG, EnFV, EnV, StromVV — Fedlex](https://www.fedlex.admin.ch)
- [EKZ Rückliefertarife 2026 (PDF)](https://www.ekz.ch/dam/ekz/privatkunden/strom/tarife-und-agb/Tarifdokumente/tarife-2026/ekz-rueckliefertarife-2026.pdf)
- [ElCom / Mantelerlass Verordnungspaket 2 (19.2.2025)](https://www.news.admin.ch/de/nsb?id=104172)

## Out of scope (v0.1)

- Lovelace-Karten — Standard Energy-Dashboard genügt.
- Automatisches Schreiben von `input_number.grid_export_price_current` — Live-Preis-Entität bleibt unberührt.
- Automatisches Aktualisieren der HKN-Vergütung aus EVU-Webseiten — HKN-Preise werden nicht maschinen-lesbar publiziert.
- Vor-2026-Historie (KEV, vermiedene Bezugskosten).
- HACS-Default-Repo-Einreichung — v0.1 nur als Custom Repository installierbar, bis Native-Speaker-Review der Übersetzungen abgeschlossen.

## Lizenz

MIT — siehe [LICENSE](LICENSE).

## Mitwirken

Native-Speaker-Reviews für FR/IT-Übersetzungen willkommen. Neue EVU-Presets via PR in `presets.py`. Bug Reports und Feature Requests via GitHub Issues.
