-- Canadian Data Observatory - PostgreSQL Schema Initialization
-- PostGIS + pg_trgm extensions, all schemas and tables

-- Extensions
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Schemas
CREATE SCHEMA IF NOT EXISTS geo;
CREATE SCHEMA IF NOT EXISTS census;
CREATE SCHEMA IF NOT EXISTS transit;
CREATE SCHEMA IF NOT EXISTS muni;
CREATE SCHEMA IF NOT EXISTS infra;

-- ============================================================
-- geo schema: Geographic reference tables
-- ============================================================

CREATE TABLE geo.provinces (
    province_code   CHAR(2) PRIMARY KEY,
    name_en         TEXT NOT NULL,
    name_fr         TEXT NOT NULL,
    geom            GEOMETRY(MultiPolygon, 4326),
    area_km2        DOUBLE PRECISION,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX idx_provinces_geom ON geo.provinces USING GIST (geom);

CREATE TABLE geo.cmas (
    cma_uid         VARCHAR(7) PRIMARY KEY,
    cma_name        TEXT NOT NULL,
    province_code   CHAR(2) REFERENCES geo.provinces(province_code),
    cma_type        VARCHAR(3) NOT NULL,  -- CMA or CA
    population_2021 INTEGER,
    geom            GEOMETRY(MultiPolygon, 4326),
    area_km2        DOUBLE PRECISION,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX idx_cmas_geom ON geo.cmas USING GIST (geom);
CREATE INDEX idx_cmas_province ON geo.cmas (province_code);
CREATE INDEX idx_cmas_name_trgm ON geo.cmas USING GIN (cma_name gin_trgm_ops);

CREATE TABLE geo.census_divisions (
    cd_uid          VARCHAR(4) PRIMARY KEY,
    cd_name         TEXT NOT NULL,
    cd_type         VARCHAR(10),
    province_code   CHAR(2) REFERENCES geo.provinces(province_code),
    geom            GEOMETRY(MultiPolygon, 4326),
    area_km2        DOUBLE PRECISION,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX idx_census_divisions_geom ON geo.census_divisions USING GIST (geom);
CREATE INDEX idx_census_divisions_province ON geo.census_divisions (province_code);

CREATE TABLE geo.climate_stations (
    station_id      VARCHAR(10) PRIMARY KEY,
    station_name    TEXT NOT NULL,
    province_code   CHAR(2) REFERENCES geo.provinces(province_code),
    latitude        DOUBLE PRECISION NOT NULL,
    longitude       DOUBLE PRECISION NOT NULL,
    elevation_m     DOUBLE PRECISION,
    geom            GEOMETRY(Point, 4326),
    wmo_id          VARCHAR(10),
    tc_id           VARCHAR(5),
    first_year      INTEGER,
    last_year       INTEGER,
    is_active       BOOLEAN DEFAULT true,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX idx_climate_stations_geom ON geo.climate_stations USING GIST (geom);
CREATE INDEX idx_climate_stations_province ON geo.climate_stations (province_code);
CREATE INDEX idx_climate_stations_active ON geo.climate_stations (is_active) WHERE is_active = true;

-- ============================================================
-- census schema: Statistics Canada census data
-- ============================================================

CREATE TABLE census.population (
    id              SERIAL PRIMARY KEY,
    geo_uid         VARCHAR(10) NOT NULL,
    geo_level       VARCHAR(20) NOT NULL,  -- province, cma, cd
    year            INTEGER NOT NULL,
    population      INTEGER NOT NULL,
    dwellings       INTEGER,
    land_area_km2   DOUBLE PRECISION,
    pop_density     DOUBLE PRECISION,
    pop_change_pct  DOUBLE PRECISION,
    data_source     TEXT DEFAULT 'statcan_census',
    created_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE (geo_uid, geo_level, year)
);
CREATE INDEX idx_population_geo ON census.population (geo_uid, geo_level);
CREATE INDEX idx_population_year ON census.population (year);

CREATE TABLE census.age_distribution (
    id              SERIAL PRIMARY KEY,
    geo_uid         VARCHAR(10) NOT NULL,
    geo_level       VARCHAR(20) NOT NULL,
    year            INTEGER NOT NULL,
    age_group       VARCHAR(10) NOT NULL,  -- 0-4, 5-9, ..., 85+
    sex             VARCHAR(1) NOT NULL,   -- T, M, F
    count           INTEGER NOT NULL,
    data_source     TEXT DEFAULT 'statcan_census',
    created_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE (geo_uid, geo_level, year, age_group, sex)
);
CREATE INDEX idx_age_dist_geo ON census.age_distribution (geo_uid, geo_level);
CREATE INDEX idx_age_dist_year ON census.age_distribution (year);
CREATE INDEX idx_age_dist_group ON census.age_distribution (age_group);

-- ============================================================
-- transit schema: Public transit data
-- ============================================================

CREATE TABLE transit.stops (
    id              SERIAL PRIMARY KEY,
    stop_id         VARCHAR(20) NOT NULL,
    agency          VARCHAR(50) NOT NULL,
    stop_name       TEXT NOT NULL,
    stop_lat        DOUBLE PRECISION NOT NULL,
    stop_lon        DOUBLE PRECISION NOT NULL,
    geom            GEOMETRY(Point, 4326),
    stop_code       VARCHAR(20),
    zone_id         VARCHAR(10),
    wheelchair_boarding INTEGER DEFAULT 0,
    location_type   INTEGER DEFAULT 0,
    parent_station  VARCHAR(20),
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE (stop_id, agency)
);
CREATE INDEX idx_stops_geom ON transit.stops USING GIST (geom);
CREATE INDEX idx_stops_agency ON transit.stops (agency);
CREATE INDEX idx_stops_name_trgm ON transit.stops USING GIN (stop_name gin_trgm_ops);

CREATE TABLE transit.routes (
    id              SERIAL PRIMARY KEY,
    route_id        VARCHAR(20) NOT NULL,
    agency          VARCHAR(50) NOT NULL,
    route_short_name VARCHAR(20),
    route_long_name TEXT,
    route_type      INTEGER NOT NULL,
    route_color     VARCHAR(6),
    route_text_color VARCHAR(6),
    route_url       TEXT,
    geom            GEOMETRY(MultiLineString, 4326),
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE (route_id, agency)
);
CREATE INDEX idx_routes_geom ON transit.routes USING GIST (geom);
CREATE INDEX idx_routes_agency ON transit.routes (agency);
CREATE INDEX idx_routes_type ON transit.routes (route_type);

-- ============================================================
-- muni schema: Municipal open data
-- ============================================================

CREATE TABLE muni.building_permits (
    id              SERIAL PRIMARY KEY,
    permit_number   VARCHAR(50) NOT NULL,
    city            VARCHAR(100) NOT NULL,
    issue_date      DATE,
    permit_type     VARCHAR(50),
    work_type       VARCHAR(50),
    description     TEXT,
    address         TEXT,
    project_value   NUMERIC(15, 2),
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    geom            GEOMETRY(Point, 4326),
    status          VARCHAR(30),
    data_source     TEXT,
    created_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE (permit_number, city)
);
CREATE INDEX idx_building_permits_geom ON muni.building_permits USING GIST (geom);
CREATE INDEX idx_building_permits_city ON muni.building_permits (city);
CREATE INDEX idx_building_permits_date ON muni.building_permits (issue_date);
CREATE INDEX idx_building_permits_type ON muni.building_permits (permit_type);

CREATE TABLE muni.crime_incidents (
    id              SERIAL PRIMARY KEY,
    incident_id     VARCHAR(50),
    city            VARCHAR(100) NOT NULL,
    occurred_date   DATE,
    occurred_time   TIME,
    crime_type      VARCHAR(100) NOT NULL,
    neighbourhood   VARCHAR(100),
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    geom            GEOMETRY(Point, 4326),
    premises_type   VARCHAR(50),
    data_source     TEXT,
    created_at      TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX idx_crime_geom ON muni.crime_incidents USING GIST (geom);
CREATE INDEX idx_crime_city ON muni.crime_incidents (city);
CREATE INDEX idx_crime_date ON muni.crime_incidents (occurred_date);
CREATE INDEX idx_crime_type ON muni.crime_incidents (crime_type);
CREATE INDEX idx_crime_neighbourhood ON muni.crime_incidents (neighbourhood);

CREATE TABLE muni.government_contracts (
    id              SERIAL PRIMARY KEY,
    contract_id     VARCHAR(50),
    city            VARCHAR(100) NOT NULL,
    vendor_name     TEXT,
    contract_title  TEXT,
    award_date      DATE,
    contract_value  NUMERIC(15, 2),
    department      TEXT,
    contract_type   VARCHAR(50),
    data_source     TEXT,
    created_at      TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX idx_contracts_city ON muni.government_contracts (city);
CREATE INDEX idx_contracts_date ON muni.government_contracts (award_date);
CREATE INDEX idx_contracts_vendor_trgm ON muni.government_contracts USING GIN (vendor_name gin_trgm_ops);

-- ============================================================
-- infra schema: Infrastructure data
-- ============================================================

CREATE TABLE infra.buildings (
    id              SERIAL PRIMARY KEY,
    building_id     VARCHAR(50),
    city            VARCHAR(100) NOT NULL,
    building_name   TEXT,
    building_type   VARCHAR(50),
    address         TEXT,
    year_built      INTEGER,
    floors          INTEGER,
    height_m        DOUBLE PRECISION,
    footprint_area  DOUBLE PRECISION,
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    geom            GEOMETRY(Polygon, 4326),
    data_source     TEXT,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX idx_buildings_geom ON infra.buildings USING GIST (geom);
CREATE INDEX idx_buildings_city ON infra.buildings (city);
CREATE INDEX idx_buildings_type ON infra.buildings (building_type);
CREATE INDEX idx_buildings_year ON infra.buildings (year_built);
