-- =============================================================================
-- MetroCity Smart City DW — All 8 Analytical Views
-- Database  : smart_city_dw  (Amazon RDS MySQL 8.0)
-- Run AFTER : schema_ddl.sql has been executed
--             AND all data has been loaded (load_rds.py completed)
--             AND ML notebook has run (for View 8 only)
--
-- Execution order matters:
--   Views 1–7 : run any time after data load
--   View 8    : run after SageMaker ML notebook has populated ml_accident_risk
--
-- Verify row counts after running:
--   vw_location_summary       → 5 rows
--   vw_hourly_profile         → 24 rows
--   vw_monthly_trend          → 7 rows  (Jan–Jul 2024)
--   vw_congestion_hotspots    → 25 rows (5 locations × 5 time periods)
--   vw_weather_risk           → 20 rows (5 weather × 4 road conditions)
--   vw_severity_by_location   → 20 rows (5 locations × 4 severity levels)
--   vw_road_condition_risk    →  4 rows
--   vw_ml_location_risk       →  5 rows
-- =============================================================================

USE smart_city_dw;


-- =============================================================================
-- VIEW 1 : vw_location_summary
-- Purpose : One row per city zone. Combines accident KPIs with traffic volume
--           to produce the accident_rate_per_1000_vehicles metric.
-- Powers  : Page 1 bar charts, Page 3 overview, accident rate KPI card.
-- Rows    : 5
-- =============================================================================

CREATE OR REPLACE VIEW vw_location_summary AS
SELECT
    l.location_name,
    l.zone_type,

    -- Accident volume metrics
    COUNT(DISTINCT a.accident_pk)                                           AS total_accidents,
    SUM(a.casualties)                                                       AS total_casualties,

    -- Risk metrics
    ROUND(AVG(a.risk_score),  3)                                            AS avg_risk_score,
    ROUND(SUM(a.risk_score),  2)                                            AS total_risk_score,
    ROUND(MAX(a.risk_score),  4)                                            AS max_risk_score,
    ROUND(AVG(a.severity_score), 3)                                         AS avg_severity_score,

    -- Severity breakdown counts
    SUM(CASE WHEN a.accident_severity = 'Fatal'    THEN 1 ELSE 0 END)      AS fatal_count,
    SUM(CASE WHEN a.accident_severity = 'Severe'   THEN 1 ELSE 0 END)      AS severe_count,
    SUM(CASE WHEN a.accident_severity = 'Moderate' THEN 1 ELSE 0 END)      AS moderate_count,
    SUM(CASE WHEN a.accident_severity = 'Minor'    THEN 1 ELSE 0 END)      AS minor_count,
    SUM(a.is_fatal_or_severe)                                               AS fatal_severe_count,
    ROUND(100.0 * SUM(a.is_fatal_or_severe)
        / NULLIF(COUNT(DISTINCT a.accident_pk), 0), 1)                   AS fatal_severe_pct,

    -- Condition flags
    ROUND(100.0 * SUM(a.adverse_weather)
        / NULLIF(COUNT(*), 0), 1)                                        AS adverse_weather_pct,
    ROUND(100.0 * SUM(a.hazardous_road)
        / NULLIF(COUNT(*), 0), 1)                                        AS hazardous_road_pct,

    -- Traffic volume (from fact_traffic for the same location)
    COALESCE(SUM(t.vehicle_count), 0)                                      AS total_vehicles,
    ROUND(AVG(t.vehicle_count), 1)                                         AS avg_vehicles_per_reading,
    ROUND(AVG(t.congestion_index), 4)                                      AS avg_congestion_index,

    -- Combined KPI: accidents per 1,000 vehicles (standard road safety metric)
    ROUND(
        1000.0 * COUNT(DISTINCT a.accident_pk)
        / NULLIF(COALESCE(SUM(t.vehicle_count), 0), 0),
    4)                                                                      AS accident_rate_per_1000_vehicles

FROM fact_accident a
JOIN  dim_location l  ON a.location_id = l.location_id
LEFT JOIN fact_traffic t  ON a.location_id = t.location_id   -- LEFT JOIN: some locations may have no traffic readings
GROUP BY
    l.location_name,
    l.zone_type;


-- =============================================================================
-- VIEW 2 : vw_hourly_profile
-- Purpose : One row per hour of the day (0–23). Joins traffic sensor data with
--           accident data on the same hour to produce a 24-hour risk profile.
-- Powers  : Page 2 dual-axis congestion line chart, Page 4 hour vs risk score.
-- Rows    : 24
-- =============================================================================

CREATE OR REPLACE VIEW vw_hourly_profile AS
SELECT
    d.hour,
    d.time_of_day,
    d.is_peak_hour,

    -- Traffic sensor metrics for this hour
    COUNT(DISTINCT t.traffic_pk)                                           AS sensor_reading_count,
    ROUND(AVG(t.vehicle_count),    1)                                      AS avg_vehicles,
    MAX(t.vehicle_count)                                                   AS peak_vehicles,
    ROUND(AVG(t.average_speed),    1)                                      AS avg_speed_kmh,
    ROUND(MIN(t.average_speed),    1)                                      AS min_speed_kmh,
    ROUND(AVG(t.congestion_index), 4)                                      AS avg_congestion_index,
    ROUND(MAX(t.congestion_index), 4)                                      AS max_congestion_index,
    ROUND(100.0 * SUM(CASE WHEN t.congestion_level = 'High' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(DISTINCT t.traffic_pk), 0), 1)                   AS high_congestion_pct,

    -- Accident metrics for the same hour
    COUNT(DISTINCT a.accident_pk)                                          AS accident_count,
    SUM(a.casualties)                                                      AS total_casualties,
    ROUND(AVG(a.risk_score),  3)                                           AS avg_risk_score,
    ROUND(MAX(a.risk_score),  4)                                           AS max_risk_score,
    SUM(a.is_fatal_or_severe)                                              AS fatal_severe_count,
    ROUND(100.0 * SUM(a.is_fatal_or_severe)
        / NULLIF(COUNT(DISTINCT a.accident_pk), 0), 1)                  AS fatal_severe_pct

FROM dim_date d
LEFT JOIN fact_traffic  t ON d.date_id = t.date_id
LEFT JOIN fact_accident a ON d.date_id = a.date_id
GROUP BY
    d.hour,
    d.time_of_day,
    d.is_peak_hour
ORDER BY
    d.hour;


-- =============================================================================
-- VIEW 3 : vw_monthly_trend
-- Purpose : One row per calendar month in the dataset (Jan–Jul 2024).
--           Captures total and fatal accident trends over time.
-- Powers  : Page 4 monthly combo chart (total accidents + fatal trend line),
--           Page 4 quarterly comparison bar.
-- Rows    : 7
-- =============================================================================

CREATE OR REPLACE VIEW vw_monthly_trend AS
SELECT
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.month_sort,                                                          -- numeric sort key for BI tools

    -- Accident volume
    COUNT(DISTINCT a.accident_pk)                                          AS total_accidents,
    SUM(a.casualties)                                                      AS total_casualties,
    ROUND(AVG(a.casualties), 2)                                            AS avg_casualties_per_accident,

    -- Severity breakdown
    SUM(CASE WHEN a.accident_severity = 'Fatal'    THEN 1 ELSE 0 END)     AS fatal_accidents,
    SUM(CASE WHEN a.accident_severity = 'Severe'   THEN 1 ELSE 0 END)     AS severe_accidents,
    SUM(CASE WHEN a.accident_severity = 'Moderate' THEN 1 ELSE 0 END)     AS moderate_accidents,
    SUM(CASE WHEN a.accident_severity = 'Minor'    THEN 1 ELSE 0 END)     AS minor_accidents,
    SUM(a.is_fatal_or_severe)                                              AS fatal_severe_count,
    ROUND(100.0 * SUM(a.is_fatal_or_severe)
        / NULLIF(COUNT(*), 0), 1)                                       AS fatal_severe_pct,

    -- Risk metrics
    ROUND(AVG(a.risk_score), 3)                                            AS avg_risk_score,
    ROUND(SUM(a.risk_score), 2)                                            AS total_risk_score,

    -- Condition context
    ROUND(100.0 * SUM(a.adverse_weather)  / NULLIF(COUNT(*), 0), 1)      AS adverse_weather_pct,
    ROUND(100.0 * SUM(a.hazardous_road)   / NULLIF(COUNT(*), 0), 1)      AS hazardous_road_pct,
    ROUND(100.0 * SUM(a.is_peak_hour)     / NULLIF(COUNT(*), 0), 1)      AS peak_hour_pct

FROM fact_accident a
JOIN dim_date d ON a.date_id = d.date_id
GROUP BY
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.month_sort
ORDER BY
    d.year,
    d.month_sort;


-- =============================================================================
-- VIEW 4 : vw_congestion_hotspots
-- Purpose : One row per (location × time_of_day) combination.
--           Creates a 5×5 matrix used as a heatmap in the dashboard.
-- Powers  : Page 2 congestion hotspot heatmap matrix.
-- Rows    : 25 (5 locations × 5 time periods)
-- =============================================================================

CREATE OR REPLACE VIEW vw_congestion_hotspots AS
SELECT
    l.location_name,
    l.zone_type,
    d.time_of_day,
    d.is_peak_hour,

    -- Volume and flow metrics
    COUNT(DISTINCT t.traffic_pk)                                           AS sensor_readings,
    ROUND(AVG(t.vehicle_count),    1)                                      AS avg_vehicles,
    ROUND(MAX(t.vehicle_count),    0)                                      AS peak_vehicles,
    ROUND(AVG(t.average_speed),    1)                                      AS avg_speed_kmh,
    ROUND(MIN(t.average_speed),    1)                                      AS min_speed_kmh,

    -- Congestion index
    ROUND(AVG(t.congestion_index), 4)                                      AS avg_congestion_index,
    ROUND(MAX(t.congestion_index), 4)                                      AS max_congestion_index,

    -- Label distribution
    ROUND(100.0 * SUM(CASE WHEN t.congestion_level = 'High'     THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 1)                                       AS high_congestion_pct,
    ROUND(100.0 * SUM(CASE WHEN t.congestion_level = 'Moderate' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 1)                                       AS moderate_congestion_pct,
    ROUND(100.0 * SUM(CASE WHEN t.congestion_level = 'Low'      THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 1)                                       AS low_congestion_pct

FROM fact_traffic  t
JOIN dim_location l ON t.location_id = l.location_id
JOIN dim_date     d ON t.date_id     = d.date_id
GROUP BY
    l.location_name,
    l.zone_type,
    d.time_of_day,
    d.is_peak_hour
ORDER BY
    l.location_name,
    d.time_of_day;


-- =============================================================================
-- VIEW 5 : vw_weather_risk
-- Purpose : One row per (weather_condition × road_condition) combination.
--           Creates a 5×4 risk matrix. Key finding: Clear+Dry = highest risk.
-- Powers  : Page 3 weather × road condition heatmap.
-- Rows    : 20 (5 weather conditions × 4 road conditions)
-- =============================================================================

CREATE OR REPLACE VIEW vw_weather_risk AS
SELECT
    w.weather_condition,
    w.is_adverse                                                           AS is_adverse_weather,
    a.road_condition,
    a.hazardous_road                                                       AS is_hazardous_road,

    -- Volume
    COUNT(*)                                                               AS accidents,
    SUM(a.casualties)                                                      AS total_casualties,
    ROUND(AVG(a.casualties), 2)                                            AS avg_casualties,

    -- Severity metrics
    ROUND(AVG(a.severity_score), 3)                                        AS avg_severity_score,
    ROUND(AVG(a.risk_score),     3)                                        AS avg_risk_score,
    ROUND(MAX(a.risk_score),     4)                                        AS max_risk_score,

    -- Severity breakdown
    SUM(CASE WHEN a.accident_severity = 'Fatal'  THEN 1 ELSE 0 END)       AS fatal_count,
    SUM(CASE WHEN a.accident_severity = 'Severe' THEN 1 ELSE 0 END)       AS severe_count,
    SUM(a.is_fatal_or_severe)                                              AS fatal_severe_count,
    ROUND(100.0 * SUM(a.is_fatal_or_severe)
        / NULLIF(COUNT(*), 0), 1)                                       AS fatal_severe_pct

FROM fact_accident a
JOIN dim_weather w ON a.weather_id = w.weather_id
GROUP BY
    w.weather_condition,
    w.is_adverse,
    a.road_condition,
    a.hazardous_road
ORDER BY
    avg_risk_score DESC;


-- =============================================================================
-- VIEW 6 : vw_severity_by_location
-- Purpose : One row per (location × accident_severity) combination.
--           Feeds the 100% stacked bar chart showing severity profile per zone.
-- Powers  : Page 1 and Page 3 stacked bar charts.
-- Rows    : 20 (5 locations × 4 severity levels)
-- =============================================================================

CREATE OR REPLACE VIEW vw_severity_by_location AS
SELECT
    l.location_name,
    l.zone_type,
    a.accident_severity,

    -- Volume
    COUNT(*)                                                               AS accident_count,
    SUM(a.casualties)                                                      AS total_casualties,
    ROUND(AVG(a.casualties), 2)                                            AS avg_casualties,

    -- Risk metrics for this severity × location combination
    ROUND(AVG(a.risk_score), 3)                                            AS avg_risk_score,
    ROUND(SUM(a.risk_score), 2)                                            AS total_risk_score,

    -- Percentage of all accidents at this location that have this severity
    ROUND(
        100.0 * COUNT(*)
        / NULLIF(SUM(COUNT(*)) OVER (PARTITION BY l.location_name), 0),
    1)                                                                     AS pct_of_location_total,

    -- Condition context
    ROUND(100.0 * SUM(a.adverse_weather) / NULLIF(COUNT(*), 0), 1)       AS adverse_weather_pct,
    ROUND(100.0 * SUM(a.hazardous_road)  / NULLIF(COUNT(*), 0), 1)       AS hazardous_road_pct

FROM fact_accident a
JOIN dim_location l ON a.location_id = l.location_id
GROUP BY
    l.location_name,
    l.zone_type,
    a.accident_severity
ORDER BY
    l.location_name,
    FIELD(a.accident_severity, 'Fatal', 'Severe', 'Moderate', 'Minor');
    -- FIELD() sorts severity from most to least severe — useful for stacked bar ordering


-- =============================================================================
-- VIEW 7 : vw_road_condition_risk
-- Purpose : One row per road condition type. Simple 4-row summary.
-- Powers  : Page 3 road condition bar chart.
-- Rows    : 4
-- =============================================================================

CREATE OR REPLACE VIEW vw_road_condition_risk AS
SELECT
    a.road_condition,
    MAX(a.hazardous_road)                                                  AS is_hazardous,
                                                                        -- 1 = Icy or Under Construction
    -- Volume
    COUNT(*)                                                               AS total_accidents,
    SUM(a.casualties)                                                      AS total_casualties,
    ROUND(AVG(a.casualties), 2)                                            AS avg_casualties,

    -- Risk metrics
    ROUND(AVG(a.risk_score),     3)                                        AS avg_risk_score,
    ROUND(MAX(a.risk_score),     4)                                        AS max_risk_score,
    ROUND(AVG(a.severity_score), 3)                                        AS avg_severity_score,

    -- Severity breakdown
    SUM(CASE WHEN a.accident_severity = 'Fatal'    THEN 1 ELSE 0 END)     AS fatal_count,
    SUM(CASE WHEN a.accident_severity = 'Severe'   THEN 1 ELSE 0 END)     AS severe_count,
    SUM(a.is_fatal_or_severe)                                              AS fatal_severe_count,
    ROUND(100.0 * SUM(a.is_fatal_or_severe)
        / NULLIF(COUNT(*), 0), 1)                                       AS fatal_severe_pct,

    -- Traffic density context for this road condition
    ROUND(AVG(a.traffic_density_num), 3)                                   AS avg_traffic_density_num,
    ROUND(100.0 * SUM(a.is_peak_hour)
        / NULLIF(COUNT(*), 0), 1)                                       AS peak_hour_pct

FROM fact_accident a
GROUP BY
    a.road_condition
ORDER BY
    avg_risk_score DESC;


-- =============================================================================
-- VIEW 8 : vw_ml_location_risk
-- Purpose : One row per city zone. Aggregates ML model predictions from the
--           ml_accident_risk table (populated by the SageMaker notebook).
--           Produces a safety scorecard with A/B rating per location.
-- Powers  : Page 5 location safety scorecard table,
--           Page 5 predicted vs actual grouped bar chart.
-- Rows    : 5
-- NOTE    : Run this view AFTER the SageMaker ML notebook has been executed
--           and ml_accident_risk has been populated with 5,000 rows.
-- =============================================================================

CREATE OR REPLACE VIEW vw_ml_location_risk AS
SELECT
    r.location_name,

    -- ML predicted risk aggregates
    ROUND(AVG(r.predicted_risk_prob),  4)                                  AS avg_predicted_risk,
    ROUND(MIN(r.predicted_risk_prob),  4)                                  AS min_predicted_risk,
    ROUND(MAX(r.predicted_risk_prob),  4)                                  AS max_predicted_risk,

    -- Risk tier distribution for this location
    SUM(CASE WHEN r.risk_tier = 'Critical Risk' THEN 1 ELSE 0 END)        AS critical_risk_count,
    SUM(CASE WHEN r.risk_tier = 'High Risk'     THEN 1 ELSE 0 END)        AS high_risk_count,
    SUM(CASE WHEN r.risk_tier = 'Moderate Risk' THEN 1 ELSE 0 END)        AS moderate_risk_count,
    SUM(CASE WHEN r.risk_tier = 'Low Risk'      THEN 1 ELSE 0 END)        AS low_risk_count,

    -- Model performance: predicted vs actual
    SUM(r.predicted_class)                                                 AS predicted_fatal_severe,
    SUM(r.actual_fatal_severe)                                             AS actual_fatal_severe,
    ROUND(100.0 * SUM(r.actual_fatal_severe)
        / NULLIF(COUNT(*), 0), 2)                                       AS actual_fatal_severe_pct,

    -- Total accidents at this location (for context)
    COUNT(*)                                                               AS total_accidents,

    -- Safety rating: human-readable label for non-technical administrators
    -- Dangerous = avg predicted risk >= 0.505 (above dataset mean of ~0.50)
    -- Caution   = avg predicted risk <  0.505
    CASE
        WHEN AVG(r.predicted_risk_prob) >= 0.505 THEN 'Dangerous'
        ELSE 'Caution'
    END                                                                    AS safety_rating

FROM ml_accident_risk r
GROUP BY
    r.location_name
ORDER BY
    avg_predicted_risk DESC;


-- =============================================================================
-- VERIFICATION QUERIES
-- Run these after creating all 8 views to confirm row counts and spot-check values.
-- =============================================================================

-- Quick row count check for all 8 views
SELECT 'vw_location_summary'     AS view_name, COUNT(*) AS row_count FROM vw_location_summary     UNION ALL
SELECT 'vw_hourly_profile',                    COUNT(*) FROM vw_hourly_profile                    UNION ALL
SELECT 'vw_monthly_trend',                     COUNT(*) FROM vw_monthly_trend                     UNION ALL
SELECT 'vw_congestion_hotspots',               COUNT(*) FROM vw_congestion_hotspots               UNION ALL
SELECT 'vw_weather_risk',                      COUNT(*) FROM vw_weather_risk                      UNION ALL
SELECT 'vw_severity_by_location',              COUNT(*) FROM vw_severity_by_location              UNION ALL
SELECT 'vw_road_condition_risk',               COUNT(*) FROM vw_road_condition_risk               UNION ALL
SELECT 'vw_ml_location_risk',                  COUNT(*) FROM vw_ml_location_risk;
