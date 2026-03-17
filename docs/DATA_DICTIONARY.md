# CDO Data Dictionary

All metrics exposed by the CDO exporter follow the naming convention `cdo_{domain}_{metric}_{unit}`. Every metric is a Prometheus Gauge.

---

## Economy

| Metric | Labels | Description | Source | Refresh |
|--------|--------|-------------|--------|---------|
| `cdo_economy_gdp_millions` | province, frequency | GDP at basic prices, seasonally adjusted (millions CAD) | StatCan Table 36-10-0434-01 | Monthly |
| `cdo_economy_cpi_index` | province, basket | Consumer Price Index (2002=100) | StatCan Table 18-10-0004-01 | Monthly |
| `cdo_economy_unemployment_rate_percent` | province | Unemployment rate, seasonally adjusted (%) | StatCan Table 14-10-0287-01 | Monthly |
| `cdo_economy_employment_total` | province, industry | Employment count, seasonally adjusted (thousands) | StatCan Table 14-10-0287-01 | Monthly |
| `cdo_economy_exports_millions` | province | Total exports of goods (millions CAD) | StatCan Table 12-10-0011-01 | Monthly |
| `cdo_economy_imports_millions` | province | Total imports of goods (millions CAD) | StatCan Table 12-10-0011-01 | Monthly |
| `cdo_economy_trade_balance_millions` | province | Trade balance: exports minus imports (millions CAD) | Computed | Monthly |
| `cdo_economy_interest_rate_percent` | type | Bank of Canada policy interest rate (%) | StatCan Table 10-10-0122-01 | Monthly |
| `cdo_economy_retail_sales_millions` | province | Retail trade sales, seasonally adjusted (millions CAD) | StatCan Table 20-10-0008-01 | Monthly |
| `cdo_economy_last_update_timestamp` | - | Unix timestamp of last successful economy data update | Internal | - |

---

## Housing

| Metric | Labels | Description | Source | Refresh |
|--------|--------|-------------|--------|---------|
| `cdo_housing_starts_total` | cma, type | Housing starts by CMA and type (all/single/multi) | StatCan Table 34-10-0135-01 | Monthly |
| `cdo_housing_price_index` | cma | New Housing Price Index (2017=100) | StatCan Table 18-10-0205-01 | Monthly |
| `cdo_housing_avg_price_dollars` | cma | Average residential property price (CAD) | StatCan Table 18-10-0205-01 | Monthly |
| `cdo_housing_vacancy_rate_percent` | cma | Rental vacancy rate (%) | StatCan Table 34-10-0127-01 | Annual |
| `cdo_housing_avg_rent_dollars` | cma, bedrooms | Average monthly rent (CAD) by bedroom count | StatCan Table 34-10-0133-01 | Annual |
| `cdo_housing_price_to_income_ratio` | cma | Housing price-to-income ratio | Computed (price / median income) | Monthly |
| `cdo_housing_last_update_timestamp` | - | Unix timestamp of last successful housing data update | Internal | - |

---

## Climate

| Metric | Labels | Description | Source | Refresh |
|--------|--------|-------------|--------|---------|
| `cdo_climate_temperature_celsius` | city, province | Current temperature (Celsius) | Environment Canada | Hourly |
| `cdo_climate_humidity_percent` | city, province | Current relative humidity (%) | Environment Canada | Hourly |
| `cdo_climate_wind_speed_kmh` | city, province | Current wind speed (km/h) | Environment Canada | Hourly |
| `cdo_climate_pressure_kpa` | city, province | Atmospheric pressure (kPa) | Environment Canada | Hourly |
| `cdo_climate_precipitation_mm` | city, province | Precipitation (mm) | Environment Canada | Hourly |
| `cdo_climate_temperature_daily_high_celsius` | city, province | Daily high temperature (Celsius) | Environment Canada | Daily |
| `cdo_climate_temperature_daily_low_celsius` | city, province | Daily low temperature (Celsius) | Environment Canada | Daily |
| `cdo_climate_last_update_timestamp` | - | Unix timestamp of last successful climate data update | Internal | - |

---

## Crime

| Metric | Labels | Description | Source | Refresh |
|--------|--------|-------------|--------|---------|
| `cdo_crime_severity_index` | province, type | Crime Severity Index (2006 baseline=100) | StatCan Table 35-10-0026-01 | Annual |
| `cdo_crime_rate_per_100k` | province | Crime rate per 100,000 population | StatCan Table 35-10-0177-01 | Annual |
| `cdo_crime_incidents_total` | province, offence_type | Total crime incidents by offence type | StatCan Table 35-10-0177-01 | Annual |
| `cdo_crime_last_update_timestamp` | - | Unix timestamp of last successful crime data update | Internal | - |

---

## Immigration

| Metric | Labels | Description | Source | Refresh |
|--------|--------|-------------|--------|---------|
| `cdo_immigration_permanent_residents_total` | province, year | Total permanent residents admitted | IRCC Open Data | Monthly |
| `cdo_immigration_temporary_residents_total` | province, year | Total temporary residents | IRCC Open Data | Monthly |
| `cdo_immigration_refugees_total` | province, year | Total refugee claimants | IRCC Open Data | Monthly |
| `cdo_immigration_citizenship_grants_total` | year | Total citizenship grants | IRCC Open Data | Monthly |
| `cdo_immigration_by_source_country_total` | country, year | Permanent residents by source country | IRCC Open Data | Monthly |
| `cdo_immigration_last_update_timestamp` | - | Unix timestamp of last successful immigration data update | Internal | - |

---

## Health

| Metric | Labels | Description | Source | Refresh |
|--------|--------|-------------|--------|---------|
| `cdo_health_spending_per_capita_dollars` | province | Health expenditure per capita (CAD) | StatCan Table 10-10-0005-01 | Annual |
| `cdo_health_life_expectancy_years` | province, sex | Life expectancy at birth (years) | StatCan Table 13-10-0114-01 | Annual |
| `cdo_health_physicians_per_100k` | province | Physicians per 100,000 population | CIHI estimates | Annual |
| `cdo_health_nurses_per_100k` | province | Nurses per 100,000 population | CIHI estimates | Annual |
| `cdo_health_wait_time_days` | province, procedure | Median specialist wait time (days) | Fraser Institute estimates | Annual |
| `cdo_health_last_update_timestamp` | - | Unix timestamp of last successful health data update | Internal | - |

---

## Demographics

| Metric | Labels | Description | Source | Refresh |
|--------|--------|-------------|--------|---------|
| `cdo_demographics_population_total` | province | Total population estimate | StatCan Table 17-10-0005-01 | Quarterly |
| `cdo_demographics_population_growth_rate_percent` | province | Population growth rate (%) | StatCan Table 17-10-0005-01 | Quarterly |
| `cdo_demographics_median_age_years` | province | Median age (years) | StatCan Table 17-10-0005-01 | Quarterly |
| `cdo_demographics_births_total` | province | Total births | StatCan Table 17-10-0005-01 | Quarterly |
| `cdo_demographics_deaths_total` | province | Total deaths | StatCan Table 17-10-0005-01 | Quarterly |
| `cdo_demographics_net_migration_total` | province | Net international migration | StatCan Table 17-10-0005-01 | Quarterly |
| `cdo_demographics_last_update_timestamp` | - | Unix timestamp of last successful demographics data update | Internal | - |

---

## Government

| Metric | Labels | Description | Source | Refresh |
|--------|--------|-------------|--------|---------|
| `cdo_government_contracts_total_value_dollars` | department, fiscal_year | Total value of federal contracts over $10K (CAD) | Open Canada Proactive Disclosure | Monthly |
| `cdo_government_contracts_count` | department, fiscal_year | Number of federal contracts over $10K | Open Canada Proactive Disclosure | Monthly |
| `cdo_government_grants_total_value_dollars` | fiscal_year | Total value of federal grants and contributions (CAD) | Open Canada Proactive Disclosure | Monthly |
| `cdo_government_travel_total_dollars` | fiscal_year | Total federal travel expenses (CAD) | Open Canada Proactive Disclosure | Monthly |
| `cdo_government_last_update_timestamp` | - | Unix timestamp of last successful government data update | Internal | - |

---

## Transit

| Metric | Labels | Description | Source | Refresh |
|--------|--------|-------------|--------|---------|
| `cdo_transit_last_update_timestamp` | - | Unix timestamp of most recent transit GTFS data load | Internal | - |

---

## Recording Rules (Pre-aggregated)

| Metric | Expression | Description |
|--------|-----------|-------------|
| `cdo:economy:national_summary` | `{__name__=~"cdo_economy_.+",province="CA"}` | All national-level economy metrics |
| `cdo:housing:avg_price_national` | `avg(cdo_housing_avg_price_dollars)` | National average home price across all CMAs |
| `cdo:crime:national_csi` | `cdo_crime_severity_index{province="CA",type="total"}` | National Crime Severity Index |
| `cdo:immigration:annual_total` | `sum(cdo_immigration_permanent_residents_total{year=~"202[5-6]"})` | Total recent immigration |

---

## Label Reference

| Label | Values | Used By |
|-------|--------|---------|
| `province` | CA, ON, QC, BC, AB, MB, SK, NS, NB, NL, PE | Economy, Crime, Immigration, Health, Demographics |
| `cma` | Toronto, Vancouver, Montreal, Calgary, Edmonton, Ottawa-Gatineau, Winnipeg, Halifax, Victoria, Hamilton | Housing |
| `city` | Ottawa, Toronto, Vancouver, Montreal, Calgary, Edmonton, Winnipeg, Halifax, Victoria, St. John's | Climate |
| `type` | total, violent, non-violent, other (Crime); all, single, multi (Housing); overnight, policy (Interest) | Crime, Housing, Economy |
| `sex` | both, male, female | Health (life expectancy) |
| `basket` | All-items, Food, Shelter, Transportation, etc. | Economy (CPI) |
| `bedrooms` | total, 1br, 2br | Housing (rent) |
| `year` | 2020-2026 | Immigration |
| `procedure` | hip_replacement, knee_replacement, cataract | Health (wait times) |
| `offence_type` | Various offence categories | Crime (incidents) |
| `department` | Federal department codes | Government |
| `fiscal_year` | 2023-2024, 2024-2025, etc. | Government |
| `country` | India, Philippines, China, etc. | Immigration (source country) |
| `frequency` | monthly, quarterly | Economy (GDP) |
| `industry` | Total, Goods-producing, Services-producing | Economy (employment) |
