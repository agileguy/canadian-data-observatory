# Canadian Data Observatory (CDO)

A self-hosted data platform that aggregates Canadian public datasets into a unified, queryable infrastructure with automated collection, storage, and visualization.

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
cp .env.example .env
# Edit .env with your API keys (optional)

# 2. Start the stack
docker compose up -d

# 3. Access dashboards
open http://localhost:3000  # Grafana (admin/admin)
```

## Environment Variables

| Variable                  | Default      | Description                        |
|--------------------------|--------------|------------------------------------|
| CDO_GRAFANA_PORT         | 3000         | Grafana web UI port                |
| CDO_GRAFANA_ADMIN_USER   | admin        | Grafana admin username             |
| CDO_GRAFANA_ADMIN_PASSWORD | admin      | Grafana admin password             |
| CDO_PROMETHEUS_PORT      | 9090         | Prometheus UI port                 |
| CDO_POSTGRES_PORT        | 5432         | PostgreSQL port                    |
| CDO_POSTGRES_DB          | cdo          | Database name                      |
| CDO_POSTGRES_USER        | cdo          | Database username                  |
| CDO_POSTGRES_PASSWORD    | cdo_secret   | Database password                  |
| CDO_REDIS_PORT           | 6379         | Redis port                         |
| CDO_EXPORTER_PORT        | 8000         | Exporter metrics port              |
| CDO_LOG_LEVEL            | INFO         | Log verbosity (DEBUG/INFO/WARN)    |
| CDO_VANCOUVER_API_KEY    | -            | Vancouver Open Data API key        |

## Data Sources

| Domain         | Source                          | Frequency | Tables                  |
|---------------|--------------------------------|-----------|-------------------------|
| Economy       | Statistics Canada (stats_can)   | Monthly   | Prometheus gauges       |
| Weather       | Environment Canada              | Hourly    | Prometheus gauges       |
| Census        | Statistics Canada Census        | Quarterly | census.population, census.age_distribution |
| Transit       | TransLink/TTC GTFS              | Weekly    | transit.stops, transit.routes |
| Municipal     | Vancouver/Toronto Open Data     | Daily     | muni.building_permits, muni.crime_incidents, muni.government_contracts |
| Geographic    | Statistics Canada Boundary Files | Monthly   | geo.provinces, geo.cmas, geo.census_divisions, geo.climate_stations |
| Infrastructure | Municipal Open Data            | Weekly    | infra.buildings         |

## Database Schemas

- **geo** - Geographic boundaries (provinces, CMAs, census divisions, climate stations)
- **census** - Population and demographic data
- **transit** - Public transit stops and routes
- **muni** - Municipal data (permits, crime, contracts)
- **infra** - Infrastructure (buildings)

## Prometheus Alert Rules

| Alert               | Condition          | Severity |
|--------------------|--------------------|----------|
| WeatherDataStale   | No update > 1 hour | warning  |
| EconomicDataStale  | No update > 30 days | warning  |
| ExporterDown       | Unreachable > 5 min | critical |

## Project Structure

```
canadian-data-observatory/
  config/
    grafana/provisioning/     # Grafana datasources + dashboards
    postgres/init.sql          # Schema DDL
    prometheus/
      prometheus.yml           # Scrape config
      rules/cdo_freshness.yml  # Alert rules
  services/
    exporter/                  # FastAPI Prometheus exporter
      app/
        collectors/economy.py  # StatCan economic indicators
        parsers/statcan.py     # StatCan data parsing utilities
        cache.py               # Redis cache layer
        config.py              # Settings
        main.py                # FastAPI application
    loader/                    # Click CLI data loader
      app/
        loaders/               # Domain-specific loaders
        config.py              # Settings
        db.py                  # PostgreSQL connection helper
        main.py                # Click CLI
      crontab                  # Scheduled job definitions
  data/cache/                  # Local cache directory
  backups/                     # Database backup storage
  docker-compose.yml           # Full stack definition
  .env.example                 # Environment template
```

## License

MIT
