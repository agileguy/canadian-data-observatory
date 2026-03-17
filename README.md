# Canadian Data Observatory (CDO)

A self-hosted data platform that aggregates Canadian public datasets into a unified, queryable infrastructure with automated collection, storage, and visualization. Tracks 50+ metrics across 9 domains: economy, housing, climate, crime, immigration, health, demographics, government transparency, and transit.

## Architecture

```
                          +------------------+
                          |     Grafana      |
                          |   :3000 (UI)     |
                          +--------+---------+
                                   |
                    +--------------+--------------+
                    |                              |
           +--------+--------+          +---------+---------+
           |   Prometheus     |          |    PostgreSQL     |
           | :9090 (metrics)  |          | :5432 (PostGIS)   |
           +--------+--------+          +---------+---------+
                    |                              |
           +--------+--------+          +---------+---------+
           |    Exporter      |          |      Loader       |
           | :8000 (FastAPI)  |          |   (cron jobs)     |
           +--------+--------+          +---------+---------+
                    |                              |
                    +------+-------+-------+------+
                           |       |       |
                     +-----+  +----+  +----+-----+
                     |Redis |  |StatCan| |Open Data|
                     |:6379 |  | API   | |  APIs   |
                     +------+  +-------+ +---------+
```

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed system diagrams, container inventory, and network topology.

## Services

| Service      | Image                      | Port | Purpose                          |
|-------------|----------------------------|------|----------------------------------|
| grafana     | grafana/grafana-oss:11.4.0 | 3000 | Dashboards and visualization     |
| prometheus  | prom/prometheus:v2.54.0    | 9090 | Metrics storage (365d retention) |
| postgres    | postgis/postgis:16-3.4     | 5432 | Spatial data warehouse            |
| redis       | redis:7-alpine             | 6379 | API response cache (256MB LRU)   |
| exporter    | Custom (Python/FastAPI)    | 8000 | Prometheus metrics collector      |
| loader      | Custom (Python/Click)      | -    | Scheduled data loader (cron)     |

## Quick Start

```bash
# 1. Clone and configure
git clone https://github.com/agileguy/canadian-data-observatory.git
cd canadian-data-observatory
cp .env.example .env
# Edit .env with your API keys (optional)

# 2. Start the stack
docker compose up -d

# 3. Access dashboards
open http://localhost:3000  # Grafana (admin/admin)
```

Data collection begins immediately on startup. Economy and climate data populate within minutes. Housing, crime, and immigration data follow within the first hour. Health, demographics, government, and transit data load on their respective schedules.

## Dashboards

| # | Dashboard | Domain | Description |
|---|-----------|--------|-------------|
| 1 | **Canadian Data Observatory** | Overview | National KPI summary: GDP, CPI, unemployment, population, housing, temperature, crime, immigration |
| 2 | **Canada Economic Overview** | Economy | GDP trend, CPI breakdown by basket, unemployment by province, employment, trade balance, interest rates, retail sales |
| 3 | **Housing Market Overview** | Housing | Price index, housing starts, vacancy rates, average rent, price-to-income ratio across 10 CMAs |
| 4 | **Climate & Weather** | Climate | Real-time temperature, humidity, wind, pressure, precipitation for 10 Canadian cities |
| 5 | **Crime Statistics** | Crime | Crime Severity Index, crime rate per 100K, incidents by offence type, provincial comparison |
| 6 | **Immigration & Citizenship** | Immigration | Permanent/temporary residents, refugees, citizenship grants, source countries |
| 7 | **Population & Demographics** | Demographics | Population by province, growth rate, median age, births, deaths, net migration |
| 8 | **Health Indicators** | Health | Spending per capita, life expectancy, physicians/nurses per 100K, specialist wait times |
| 9 | **Public Transit** | Transit | GTFS route and stop maps (PostGIS), feed freshness tracking |
| 10 | **Government Transparency** | Government | Federal contracts, grants and contributions, travel expenses by department |
| 11 | **Cross-Domain Insights** | Analytics | Housing vs immigration, crime vs unemployment, health spending vs life expectancy, population growth vs housing starts |

_Screenshot placeholders: each dashboard is auto-provisioned from JSON and renders on first Grafana startup._

## Data Sources

| Domain | Source | API/Format | Frequency | Auth Required |
|--------|--------|-----------|-----------|---------------|
| Economy | Statistics Canada | stats_can vector API | Monthly | No |
| Housing | Statistics Canada | stats_can vector API | Monthly | No |
| Climate | Environment Canada (ECCC) | XML weather feeds | Hourly | No |
| Crime | Statistics Canada | CSV bulk download | Annual | No |
| Immigration | IRCC Open Data | CSV download | Monthly | No |
| Health | Statistics Canada | CSV bulk download (ZIP) | Annual | No |
| Demographics | Statistics Canada | CSV bulk download (ZIP) | Quarterly | No |
| Government | Open Canada CKAN | REST API | Monthly | No |
| Transit | TransLink / TTC | GTFS ZIP feeds | Weekly | No |
| Municipal | Vancouver / Toronto / Calgary | REST APIs | Daily | Optional (Vancouver) |
| Geographic | Statistics Canada | Boundary shapefiles | As needed | No |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| CDO_GRAFANA_PORT | 3000 | Grafana web UI port |
| CDO_GRAFANA_ADMIN_USER | admin | Grafana admin username |
| CDO_GRAFANA_ADMIN_PASSWORD | admin | Grafana admin password |
| CDO_PROMETHEUS_PORT | 9090 | Prometheus UI port |
| CDO_POSTGRES_PORT | 5432 | PostgreSQL port |
| CDO_POSTGRES_DB | cdo | Database name |
| CDO_POSTGRES_USER | cdo | Database username |
| CDO_POSTGRES_PASSWORD | cdo_secret | Database password |
| CDO_REDIS_PORT | 6379 | Redis port |
| CDO_EXPORTER_PORT | 8000 | Exporter metrics port |
| CDO_LOG_LEVEL | INFO | Log verbosity (DEBUG/INFO/WARN) |
| CDO_VANCOUVER_API_KEY | - | Vancouver Open Data API key (optional, increases rate limits) |

## Database Schemas

| Schema | Tables | Purpose |
|--------|--------|---------|
| geo | provinces, cmas, census_divisions, climate_stations | Geographic boundaries (PostGIS geometries) |
| census | population, age_distribution | Census population and demographic breakdowns |
| transit | stops, routes | GTFS public transit data |
| muni | building_permits, crime_incidents, government_contracts | Municipal open data |
| infra | buildings | Infrastructure data |

## Prometheus Metrics

The exporter exposes 50+ Prometheus Gauges organized by domain. All metric names follow the convention `cdo_{domain}_{metric}_{unit}`.

See [docs/DATA_DICTIONARY.md](docs/DATA_DICTIONARY.md) for the complete metric reference with labels, descriptions, sources, and refresh cadences.

## Alert Rules

### Prometheus Alerts (Data Freshness)

| Alert | Condition | Severity |
|-------|-----------|----------|
| ClimateDataStale | No update > 1 hour | warning |
| EconomicDataStale | No update > 30 days | warning |
| HousingDataStale | No update > 30 days | warning |
| CrimeDataStale | No update > 1 year | warning |
| ImmigrationDataStale | No update > 30 days | warning |
| DemographicsDataStale | No update > 90 days | warning |
| HealthDataStale | No update > 90 days | warning |
| TransitDataStale | No update > 90 days | warning |
| GovernmentDataStale | No update > 30 days | warning |
| ExporterDown | Unreachable > 5 min | critical |

### Grafana Alerts (Domain Thresholds)

| Alert | Condition | Severity |
|-------|-----------|----------|
| Housing Price Spike | Avg price > 20% YoY increase | warning |
| Unemployment Threshold | National rate > 8% | warning |
| Crime Severity Elevated | National CSI > 80 | warning |

## Recording Rules

Pre-aggregated metrics evaluated every 5 minutes for faster dashboard rendering:

| Rule | Expression | Purpose |
|------|-----------|---------|
| cdo:economy:national_summary | All economy metrics for CA | National economy snapshot |
| cdo:housing:avg_price_national | avg(cdo_housing_avg_price_dollars) | National average home price |
| cdo:crime:national_csi | CSI for CA, total type | National crime severity |
| cdo:immigration:annual_total | Sum of recent PR admissions | Annual immigration total |

## Project Structure

```
canadian-data-observatory/
  config/
    grafana/provisioning/
      dashboards/               # Dashboard provisioning config
        files/                  # 11 JSON dashboard definitions
      datasources/              # Prometheus + PostgreSQL datasources
      alerting/                 # Grafana unified alerting rules
    postgres/init.sql           # Schema DDL (PostGIS)
    prometheus/
      prometheus.yml            # Scrape config
      rules/
        cdo_freshness.yml       # Data freshness + exporter alerts
        cdo_recording.yml       # Pre-aggregated recording rules
  services/
    exporter/                   # FastAPI Prometheus exporter
      app/
        collectors/             # 9 domain collectors
        parsers/                # StatCan, ECCC, CKAN parsers
        cache.py                # Redis cache layer
        config.py               # Settings
        main.py                 # FastAPI application
    loader/                     # Click CLI data loader
      app/
        loaders/                # 8 domain loaders
        config.py               # Settings
        db.py                   # PostgreSQL connection helper
        main.py                 # Click CLI
      crontab                   # Scheduled job definitions
  docs/
    ARCHITECTURE.md             # System architecture and diagrams
    DATA_DICTIONARY.md          # Complete metric reference
  data/cache/                   # Local cache directory
  backups/                      # Database backup storage
  docker-compose.yml            # Full stack definition
  .env.example                  # Environment template
  CHANGELOG.md                  # Release history
```

## Troubleshooting

### Dashboards show "No data"

This is normal on first startup. Data populates progressively:
- **Climate**: ~2 minutes (hourly ECCC feeds, 15m scrape interval)
- **Economy**: ~5 minutes (StatCan vector API)
- **Housing/Crime/Immigration**: ~30 minutes (StatCan batch queries)
- **Health/Demographics**: ~1 hour (CSV bulk download and parse)
- **Government**: ~1 hour (CKAN API pagination)
- **Transit**: ~2 hours (GTFS download and PostGIS import)

Check exporter logs: `docker compose logs exporter -f`

### StatCan API errors

Statistics Canada occasionally rate-limits or returns 503 errors during maintenance. The exporter retries automatically via the Redis cache layer. Cached data remains available during outages.

### Redis memory

Redis is configured with a 256MB LRU eviction policy. If cache misses increase, consider raising `maxmemory` in `docker-compose.yml`.

### Prometheus storage

TSDB retention is set to 365 days / 10GB (whichever limit is hit first). Monitor disk usage with:
```bash
docker exec cdo-prometheus du -sh /prometheus
```

### PostgreSQL PostGIS

Geographic boundary imports require PostGIS extensions. These are automatically enabled by the `postgis/postgis:16-3.4` image and the init SQL script.

### Port conflicts

All ports are configurable via environment variables. If port 3000 is in use:
```bash
CDO_GRAFANA_PORT=3001 docker compose up -d
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-domain`)
3. Add collector in `services/exporter/app/collectors/`
4. Add loader in `services/loader/app/loaders/` (if spatial data)
5. Create dashboard JSON in `config/grafana/provisioning/dashboards/files/`
6. Add freshness alert in `config/prometheus/rules/cdo_freshness.yml`
7. Update the data dictionary in `docs/DATA_DICTIONARY.md`
8. Submit a pull request

### Adding a new data domain

Each domain requires:
- **Collector** (`services/exporter/app/collectors/`): Prometheus Gauges, fetch logic, Redis caching
- **Dashboard** (`config/grafana/provisioning/dashboards/files/`): Grafana JSON with PromQL queries
- **Freshness alert** (`config/prometheus/rules/cdo_freshness.yml`): Stale data detection
- **Data dictionary entry** (`docs/DATA_DICTIONARY.md`): Metric documentation

If the domain includes spatial data, also add:
- **Loader** (`services/loader/app/loaders/`): PostgreSQL/PostGIS insert logic
- **Schema** (`config/postgres/init.sql`): Table definitions
- **Crontab entry** (`services/loader/crontab`): Scheduled execution

## License

MIT

Data sourced from Canadian federal and municipal open data portals. Contains information licensed under the [Open Government Licence - Canada](https://open.canada.ca/en/open-government-licence-canada).
