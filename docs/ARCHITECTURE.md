# CDO Architecture

## System Diagram

```
                    +-----------------------------------------------+
                    |                  User / Browser                |
                    +------------------------+----------------------+
                                             |
                                        :3000 (HTTP)
                                             |
                    +------------------------v----------------------+
                    |               Grafana 11.4.0                  |
                    |  - 11 provisioned dashboards                  |
                    |  - Prometheus + PostgreSQL datasources         |
                    |  - Unified alerting rules                     |
                    +--------+-------------------+------------------+
                             |                   |
                        PromQL              SQL / PostGIS
                             |                   |
               +-------------v-------+   +-------v-----------------+
               |  Prometheus v2.54   |   |  PostgreSQL 16 + PostGIS|
               |  :9090              |   |  :5432                  |
               |  365d retention     |   |  Schemas: geo, census,  |
               |  10GB max           |   |  transit, muni, infra   |
               |  Recording rules    |   +-------^-----------------+
               |  Alert rules        |           |
               +--------^------------+           |
                        |                        |
                   /metrics                  SQL INSERTs
                        |                        |
               +--------+------------+   +-------+-----------------+
               |  Exporter (FastAPI) |   |  Loader (Click CLI)     |
               |  :8000              |   |  cron-scheduled         |
               |  9 domain collectors|   |  8 domain loaders       |
               +--------+------------+   +-------+-----------------+
                        |                        |
                   +----v----+                   |
                   |  Redis  |                   |
                   |  :6379  |                   |
                   |  256MB  |                   |
                   |  LRU    |                   |
                   +----+----+                   |
                        |                        |
          +-------------+-----------+------------+------------+
          |             |           |             |            |
    +-----v----+  +----v----+  +---v------+  +---v-----+  +--v--------+
    | StatCan  |  | ECCC    |  | IRCC     |  | OpenCan |  | Municipal |
    | API/CSV  |  | Weather |  | Open Data|  | CKAN    |  | Open Data |
    +----------+  +---------+  +----------+  +---------+  +-----------+
```

## Data Flow

### Metrics Path (Prometheus)

1. **Prometheus** scrapes the **Exporter** every 60 seconds (`/metrics`) and every 15 minutes (`/metrics/weather`)
2. **Exporter** (FastAPI) runs 9 domain collectors that fetch data from upstream APIs
3. **Redis** caches API responses with domain-specific TTLs (1h for weather, 24h for economy, 7d for health)
4. Collectors parse responses and set Prometheus Gauge values
5. **Prometheus** stores time series with 365-day retention (10GB cap)
6. **Grafana** queries Prometheus via PromQL for all metric dashboards
7. **Recording rules** pre-aggregate common queries every 5 minutes
8. **Alert rules** evaluate thresholds (freshness, unemployment, crime severity, housing)

### Spatial Path (PostgreSQL)

1. **Loader** CLI runs on cron schedules inside its container
2. Loaders fetch data from municipal APIs, GTFS feeds, and StatCan boundary files
3. Data is inserted into PostGIS-enabled PostgreSQL with domain-specific schemas
4. **Grafana** queries PostgreSQL for map panels (GeoJSON) and tabular data

## Container Inventory

| Container | Image | Port | Role | Health Check |
|-----------|-------|------|------|--------------|
| cdo-grafana | grafana/grafana-oss:11.4.0 | 3000 | Dashboard UI, alerting | HTTP /api/health |
| cdo-prometheus | prom/prometheus:v2.54.0 | 9090 | Time series storage, rules | HTTP /-/healthy |
| cdo-postgres | postgis/postgis:16-3.4 | 5432 | Spatial data warehouse | pg_isready |
| cdo-redis | redis:7-alpine | 6379 | API response cache | redis-cli ping |
| cdo-exporter | Custom Python/FastAPI | 8000 | Prometheus metrics collector | HTTP /health |
| cdo-loader | Custom Python/Click | - | Scheduled data loader | - |

## Network Topology

All containers communicate over a single Docker bridge network (`cdo-net`). Only Grafana, Prometheus, PostgreSQL, and the Exporter expose host ports. Redis is internal-only.

```
cdo-net (bridge)
  |
  +-- grafana    --> :3000 (host)
  +-- prometheus --> :9090 (host)
  +-- postgres   --> :5432 (host)
  +-- redis      (internal only)
  +-- exporter   --> :8000 (host)
  +-- loader     (no ports)
```

## Volume Strategy

| Volume | Mount Point | Purpose | Persistence |
|--------|-------------|---------|-------------|
| grafana-data | /var/lib/grafana | Dashboard state, user preferences | Named volume |
| prometheus-data | /prometheus | Time series database (365d) | Named volume |
| postgres-data | /var/lib/postgresql/data | PostGIS spatial warehouse | Named volume |
| redis-data | /data | AOF persistence for cache durability | Named volume |
| ./config/grafana/provisioning | /etc/grafana/provisioning | Dashboards, datasources, alerts | Bind mount (read-only) |
| ./config/prometheus | /etc/prometheus | Prometheus config and rules | Bind mount (read-only) |
| ./config/postgres/init.sql | /docker-entrypoint-initdb.d/ | Schema DDL | Bind mount (read-only) |

## Dashboards

| # | UID | Title | Domain | Key Metrics |
|---|-----|-------|--------|-------------|
| 1 | cdo-home | Canadian Data Observatory | Overview | GDP, CPI, unemployment, population, home price, temperature, CSI, immigration |
| 2 | cdo-economy | Canada Economic Overview | Economy | GDP trend, CPI breakdown, unemployment by province, trade balance, interest rates |
| 3 | cdo-housing | Housing Market Overview | Housing | Price index, starts, vacancy, rent, price-to-income |
| 4 | cdo-climate | Climate & Weather | Climate | Temperature, humidity, wind, pressure, precipitation |
| 5 | cdo-crime | Crime Statistics | Crime | Severity index, crime rate, incidents by type |
| 6 | cdo-immigration | Immigration & Citizenship | Immigration | Permanent/temporary residents, refugees, citizenship, source countries |
| 7 | cdo-demographics | Population & Demographics | Demographics | Population total, growth rate, median age, births, deaths, migration |
| 8 | cdo-health | Health Indicators | Health | Spending per capita, life expectancy, physicians, nurses, wait times |
| 9 | cdo-transit | Public Transit | Transit | GTFS routes and stops (PostGIS maps) |
| 10 | cdo-government | Government Transparency | Government | Contracts, grants, travel expenses |
| 11 | cdo-correlations | Cross-Domain Insights | Analytics | Housing vs immigration, crime vs unemployment, health spending vs life expectancy |

## Alerting

### Prometheus Alerts (config/prometheus/rules/)

- **Data freshness alerts** for all 9 domains (stale data detection)
- **ExporterDown** alert if the exporter is unreachable for 5+ minutes

### Grafana Alerts (config/grafana/provisioning/alerting/)

- **Housing Price Spike**: average price > 20% YoY increase
- **Unemployment Threshold**: national rate > 8%
- **Crime Severity Elevated**: national CSI > 80

## External Data Sources

| Source | Protocol | Rate Limit | Auth |
|--------|----------|------------|------|
| Statistics Canada (stats_can) | HTTPS/REST | None | None |
| Statistics Canada (CSV bulk) | HTTPS/ZIP | None | None |
| Environment Canada (ECCC) | HTTPS/XML | None | None |
| IRCC Open Data | HTTPS/CSV | None | None |
| Open Canada CKAN | HTTPS/REST | None | None |
| Vancouver Open Data | HTTPS/REST | 1000/day (without key) | API key (optional) |
| Toronto Open Data | HTTPS/REST | None | None |
| Calgary Open Data | HTTPS/REST | None | None |
| TransLink GTFS | HTTPS/ZIP | None | None |
| TTC GTFS | HTTPS/ZIP | None | None |
