-- ============================================================
-- MetroCity Smart City DW — Star Schema DDL
-- Database: smart_city_dw  (Amazon RDS MySQL 8.0)
-- Tables: 7  |  Views: 8 (in all_views.sql)
-- ============================================================

CREATE DATABASE IF NOT EXISTS smart_city_dw
    CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE smart_city_dw;

-- Dimension Tables 

CREATE TABLE dim_location (
    location_id   INT          AUTO_INCREMENT PRIMARY KEY,
    location_name VARCHAR(50)  NOT NULL UNIQUE,
    zone_type     VARCHAR(30)  NOT NULL
    -- zone_type values: Urban Core | Highway | Residential | Industrial | Suburban
);

CREATE TABLE dim_date (
    date_id         INT          AUTO_INCREMENT PRIMARY KEY,
    full_datetime   DATETIME     NOT NULL UNIQUE,
    date_only       DATE         NOT NULL,
    year            SMALLINT     NOT NULL,
    quarter         TINYINT      NOT NULL,
    month           TINYINT      NOT NULL,
    month_name      VARCHAR(15)  NOT NULL,
    month_sort      TINYINT      NOT NULL,   -- numeric sort key for Power BI / QuickSight
    week            TINYINT      NOT NULL,
    day_name        VARCHAR(12)  NOT NULL,
    day_of_week_num TINYINT      NOT NULL,   -- 1=Mon … 7=Sun, for Power BI sort
    hour            TINYINT      NOT NULL,
    time_of_day     VARCHAR(20)  NOT NULL,   -- Morning Peak | Midday | Evening Peak | Off-Peak
    is_peak_hour    TINYINT(1)   NOT NULL,
    is_weekend      TINYINT(1)   NOT NULL
);

CREATE TABLE dim_vehicle (
    vehicle_id    INT          AUTO_INCREMENT PRIMARY KEY,
    vehicle_type  VARCHAR(30)  NOT NULL UNIQUE,
    vehicle_class VARCHAR(30)  NOT NULL
    -- vehicle_class: Heavy Motor | Light Motor | Two-Wheeler | Non-Motorised
);

CREATE TABLE dim_weather (
    weather_id        INT          AUTO_INCREMENT PRIMARY KEY,
    weather_condition VARCHAR(30)  NOT NULL UNIQUE,
    is_adverse        TINYINT(1)   NOT NULL  -- 1 = Fog/Snow/Storm, 0 = Clear/Rain
);

-- Fact Tables 

CREATE TABLE fact_traffic (
    traffic_pk        INT          AUTO_INCREMENT PRIMARY KEY,
    sensor_id         VARCHAR(20)  NOT NULL,
    location_id       INT          NOT NULL,
    date_id           INT          NOT NULL,
    vehicle_count     INT          NOT NULL,
    average_speed     INT          NOT NULL,
    congestion_level  VARCHAR(20)  NOT NULL,  -- raw label from CSV
    congestion_index  FLOAT        NOT NULL,  -- engineered 0-1 KPI
    speed_category    VARCHAR(20)  NOT NULL,
    volume_band       VARCHAR(20)  NOT NULL,
    sensor_status     VARCHAR(10)  NOT NULL DEFAULT 'Active',
    FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
    FOREIGN KEY (date_id)     REFERENCES dim_date(date_id)
);

CREATE TABLE fact_accident (
    accident_pk        INT          AUTO_INCREMENT PRIMARY KEY,
    accident_id        VARCHAR(20)  NOT NULL UNIQUE,
    location_id        INT          NOT NULL,
    date_id            INT          NOT NULL,
    vehicle_id         INT          NOT NULL,
    weather_id         INT          NOT NULL,
    -- Denormalised varchar columns (for QuickSight/Power BI slicer performance)
    vehicle_type       VARCHAR(30)  NOT NULL,
    weather_condition  VARCHAR(30)  NOT NULL,
    road_condition     VARCHAR(30)  NOT NULL,
    accident_severity  VARCHAR(20)  NOT NULL,
    number_of_vehicles INT          NOT NULL,
    casualties         INT          NOT NULL,
    traffic_density    VARCHAR(20)  NOT NULL,
    -- Engineered KPI columns
    severity_score     TINYINT      NOT NULL,
    risk_score         FLOAT        NOT NULL,
    hazard_level       VARCHAR(25)  NOT NULL,
    -- Binary flag columns
    adverse_weather    TINYINT(1)   NOT NULL,
    hazardous_road     TINYINT(1)   NOT NULL,
    is_peak_hour       TINYINT(1)   NOT NULL,
    is_fatal_or_severe TINYINT(1)   NOT NULL,  -- ML binary target
    traffic_density_num TINYINT     NOT NULL,
    FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
    FOREIGN KEY (date_id)     REFERENCES dim_date(date_id),
    FOREIGN KEY (vehicle_id)  REFERENCES dim_vehicle(vehicle_id),
    FOREIGN KEY (weather_id)  REFERENCES dim_weather(weather_id)
);

-- ML Output Table

CREATE TABLE ml_accident_risk (
    risk_pk             INT          AUTO_INCREMENT PRIMARY KEY,
    accident_id         VARCHAR(20)  NOT NULL UNIQUE,
    location_name       VARCHAR(50)  NOT NULL,
    predicted_risk_prob FLOAT        NOT NULL,  -- Random Forest probability 0-1
    predicted_class     TINYINT(1)   NOT NULL,
    actual_fatal_severe TINYINT(1)   NOT NULL,
    risk_tier           VARCHAR(20)  NOT NULL
    -- risk_tier: Low Risk | Moderate Risk | High Risk | Critical Risk (>0.70)
);
