# Changelog

All notable changes to the Canadian Data Observatory project.

## [1.0.0] - 2026-03-17

### Phase 5: Advanced Analytics & Polish
- Cross-domain correlation dashboard (Housing vs Immigration, Crime vs Unemployment, Health Spending vs Life Expectancy, Population Growth vs Housing Starts)
- Prometheus recording rules for pre-aggregated national summaries
- Grafana unified alerting rules (housing spike, unemployment threshold, crime severity)
- Complete data dictionary documenting all 50+ Prometheus metrics
- Architecture documentation with system diagram and container inventory
- Expanded README with full environment variables, data sources, and troubleshooting
- CHANGELOG covering all five development phases

### Phase 4: Health, Transit, Government Transparency
- Health indicators dashboard (spending per capita, life expectancy, physicians, nurses, wait times)
- Transit dashboard with PostGIS map panels for GTFS routes and stops
- Government transparency dashboard (contracts, grants, travel expenses)
- Health collector parsing StatCan CSV bulk downloads (Tables 10-10-0005, 13-10-0114, 13-10-0394)
- Transit metadata collector for GTFS feed freshness tracking
- Government spending collector via Open Canada CKAN API
- Calgary, Toronto, Vancouver municipal data loaders
- Transit GTFS loader for TransLink and TTC feeds
- Government proactive disclosure loader
- Data freshness alerts for health, transit, and government domains

### Phase 3: Municipal Data + Geospatial Maps
- Demographics dashboard with population breakdown by province
- Municipal open data loaders (Vancouver, Toronto, Calgary)
- Geographic boundary loaders (provinces, CMAs, census divisions, climate stations)
- PostGIS schema for spatial data (geo, census, transit, muni, infra)
- Building permit, crime incident, and government contract loaders
- Transit stop and route loaders from GTFS feeds

### Phase 2: Housing, Crime, Immigration, Demographics
- Housing market dashboard (price index, starts, vacancy, rent, price-to-income)
- Crime statistics dashboard (severity index, crime rate, incidents)
- Immigration dashboard (permanent/temporary residents, refugees, citizenship, source countries)
- Housing collector with 60+ StatCan vector mappings across 10 CMAs
- Crime collector parsing StatCan Tables 35-10-0026 and 35-10-0177
- Immigration collector from IRCC open data CSV downloads
- Demographics collector from StatCan Table 17-10-0005 (CSV bulk)
- Data freshness alerts for housing, crime, immigration, demographics

### Phase 1: Core Stack + Economic Indicators + Climate
- Docker Compose stack: Grafana, Prometheus, PostgreSQL/PostGIS, Redis, Exporter, Loader
- Home dashboard with national overview (GDP, CPI, unemployment, population, housing, temperature, crime, immigration)
- Economic indicators dashboard (GDP trend, CPI, unemployment, employment, trade, interest rates, retail)
- Climate dashboard (temperature, humidity, wind, pressure, precipitation across 10 cities)
- Economy collector using stats_can library for StatCan vector API
- Climate collector using Environment Canada XML weather feeds
- Redis caching layer with domain-specific TTLs
- Prometheus scrape configs (60s general, 15m weather)
- Data freshness and exporter-down alert rules
- PostgreSQL init schema with PostGIS extension
- Grafana provisioning (dashboards, datasources)
