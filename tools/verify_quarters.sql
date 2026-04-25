-- verify_quarters.sql
--
-- Per-quarter sanity check for the BFE Rückliefertarif integration's CHF
-- output. Returns kWh exported, CHF compensated, and the implied effective
-- rate (Rp/kWh) for every quarter from 2023Q3 onward.
--
-- Compare each row's implied_rate_rp_kwh against what you expect from the
-- BFE quarterly price + your tariff config. For small_mit_ev with HKN=0
-- and verguetungs_obergrenze=on, expected = max(BFE_rp_kwh, 6.00).
--
-- Tested in pgAdmin against HA's Postgres recorder.
--
-- BEFORE RUNNING: replace these two entity IDs with your own —
--   ▸ EXPORT_SENSOR        = the kWh counter wired into the integration's
--                            "Stromnetzeinspeisung (kWh)" field
--   ▸ COMPENSATION_SENSOR  = the auto-created CHF sensor wired into
--                            "Rückliefervergütung (CHF)" (HA names it
--                            with a `_compensation` suffix)
--
-- Find them via Settings → Devices & Services → BFE Rückliefertarif →
-- Configure → "Re-wire Home Assistant entities".

WITH q_bounds(quarter_label, q_start, q_end) AS (
  VALUES
    ('2023Q3', '2023-06-30 22:00:00+00'::timestamptz, '2023-09-30 22:00:00+00'::timestamptz),
    ('2023Q4', '2023-09-30 22:00:00+00'::timestamptz, '2023-12-31 23:00:00+00'::timestamptz),
    ('2024Q1', '2023-12-31 23:00:00+00'::timestamptz, '2024-03-31 22:00:00+00'::timestamptz),
    ('2024Q2', '2024-03-31 22:00:00+00'::timestamptz, '2024-06-30 22:00:00+00'::timestamptz),
    ('2024Q3', '2024-06-30 22:00:00+00'::timestamptz, '2024-09-30 22:00:00+00'::timestamptz),
    ('2024Q4', '2024-09-30 22:00:00+00'::timestamptz, '2024-12-31 23:00:00+00'::timestamptz),
    ('2025Q1', '2024-12-31 23:00:00+00'::timestamptz, '2025-03-31 22:00:00+00'::timestamptz),
    ('2025Q2', '2025-03-31 22:00:00+00'::timestamptz, '2025-06-30 22:00:00+00'::timestamptz),
    ('2025Q3', '2025-06-30 22:00:00+00'::timestamptz, '2025-09-30 22:00:00+00'::timestamptz),
    ('2025Q4', '2025-09-30 22:00:00+00'::timestamptz, '2025-12-31 23:00:00+00'::timestamptz),
    ('2026Q1', '2025-12-31 23:00:00+00'::timestamptz, '2026-03-31 22:00:00+00'::timestamptz),
    ('2026Q2', '2026-03-31 22:00:00+00'::timestamptz, '2026-06-30 22:00:00+00'::timestamptz),
    ('2026Q3', '2026-06-30 22:00:00+00'::timestamptz, '2026-09-30 22:00:00+00'::timestamptz),
    ('2026Q4', '2026-09-30 22:00:00+00'::timestamptz, '2026-12-31 23:00:00+00'::timestamptz)
)
SELECT
  q.quarter_label,
  ROUND((MAX(e.sum) - MIN(e.sum))::numeric, 3)              AS kwh_in_quarter,
  ROUND((MAX(c.sum) - MIN(c.sum))::numeric, 4)              AS chf_in_quarter,
  ROUND(((MAX(c.sum) - MIN(c.sum)) * 100
         / NULLIF((MAX(e.sum) - MIN(e.sum)), 0))::numeric, 4) AS implied_rate_rp_kwh
FROM q_bounds q
LEFT JOIN statistics e
  ON e.metadata_id = (SELECT id FROM statistics_meta
                      WHERE statistic_id = 'sensor.YOUR_GRID_EXPORT')               -- ← edit me
 AND e.start_ts >= EXTRACT(EPOCH FROM q.q_start)
 AND e.start_ts <  EXTRACT(EPOCH FROM q.q_end)
LEFT JOIN statistics c
  ON c.metadata_id = (SELECT id FROM statistics_meta
                      WHERE statistic_id = 'sensor.YOUR_GRID_EXPORT_compensation')  -- ← edit me
 AND c.start_ts >= EXTRACT(EPOCH FROM q.q_start)
 AND c.start_ts <  EXTRACT(EPOCH FROM q.q_end)
GROUP BY q.quarter_label
ORDER BY q.quarter_label;
