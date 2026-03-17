"""CDO Loader - CLI for loading Canadian open data into PostgreSQL."""

import logging
import sys

import click

from app.config import settings


def setup_logging():
    """Configure logging based on settings."""
    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )


@click.group()
def cli():
    """CDO Loader - Canadian Data Observatory data loading tool."""
    setup_logging()


@cli.command()
def economy():
    """Load economic indicators from Statistics Canada."""
    logger = logging.getLogger(__name__)
    logger.info("Loading economy data from StatCan...")
    click.echo("Economy loader: not yet implemented")


@cli.command()
def weather():
    """Load weather/climate data from Environment Canada."""
    logger = logging.getLogger(__name__)
    logger.info("Loading weather data...")
    click.echo("Weather loader: not yet implemented")


@cli.command()
def transit():
    """Load transit data from GTFS feeds."""
    from app.loaders.transit import load_transit

    logger = logging.getLogger(__name__)
    logger.info("Loading transit GTFS data...")
    results = load_transit()
    total_stops = sum(r["stops"] for r in results.values())
    total_routes = sum(r["routes"] for r in results.values())
    for city, counts in results.items():
        click.echo(f"  {city}: {counts['stops']} stops, {counts['routes']} routes")
    click.echo(f"Transit loaded: {total_stops} stops, {total_routes} routes across {len(results)} cities")


@cli.command("load-transit")
def load_transit_cmd():
    """Load transit GTFS feeds for all 6 Canadian cities."""
    from app.loaders.transit import load_transit

    logger = logging.getLogger(__name__)
    logger.info("Loading transit GTFS data...")
    results = load_transit()
    total_stops = sum(r["stops"] for r in results.values())
    total_routes = sum(r["routes"] for r in results.values())
    for city, counts in results.items():
        click.echo(f"  {city}: {counts['stops']} stops, {counts['routes']} routes")
    click.echo(f"Transit loaded: {total_stops} stops, {total_routes} routes across {len(results)} cities")


@cli.command()
def census():
    """Load census population and demographics data."""
    from app.loaders.demographics import load_demographics

    logger = logging.getLogger(__name__)
    logger.info("Loading census demographics data...")
    results = load_demographics()
    click.echo(
        f"Demographics loaded: {results['population']} population records, "
        f"{results['age_distribution']} age distribution records"
    )


@cli.command()
def municipal():
    """Load all municipal open data (permits, crime, contracts)."""
    from app.loaders.calgary import load_building_permits as calgary_permits
    from app.loaders.calgary import load_crime_incidents as calgary_crime
    from app.loaders.toronto import load_building_permits as toronto_permits
    from app.loaders.vancouver import load_crime_incidents as vancouver_crime

    logger = logging.getLogger(__name__)
    logger.info("Loading all municipal data...")

    total = 0
    total += toronto_permits()
    total += vancouver_crime()
    total += calgary_crime()
    total += calgary_permits()

    logger.info("Municipal data loading complete: %d total rows", total)
    click.echo(f"Municipal data loaded: {total} rows across all cities")


@cli.command("load-toronto")
def load_toronto():
    """Load Toronto building permits from CKAN portal."""
    from app.loaders.toronto import load_building_permits

    logger = logging.getLogger(__name__)
    logger.info("Loading Toronto building permits...")
    count = load_building_permits()
    click.echo(f"Toronto building permits: {count} rows loaded")


@cli.command("load-vancouver")
def load_vancouver():
    """Load Vancouver crime incidents from Open Data API."""
    from app.loaders.vancouver import load_crime_incidents

    logger = logging.getLogger(__name__)
    logger.info("Loading Vancouver crime incidents...")
    count = load_crime_incidents()
    click.echo(f"Vancouver crime incidents: {count} rows loaded")


@cli.command("load-calgary")
def load_calgary():
    """Load Calgary crime incidents and building permits from SODA API."""
    from app.loaders.calgary import load_building_permits, load_crime_incidents

    logger = logging.getLogger(__name__)
    logger.info("Loading Calgary data...")
    crime_count = load_crime_incidents()
    permit_count = load_building_permits()
    total = crime_count + permit_count
    click.echo(
        f"Calgary data: {crime_count} crime + {permit_count} permits "
        f"= {total} rows loaded"
    )


@cli.command("load-government")
def load_government():
    """Load federal government contracts from Open Canada CKAN."""
    from app.loaders.government import load_government_contracts

    logger = logging.getLogger(__name__)
    logger.info("Loading government contracts...")
    count = load_government_contracts()
    click.echo(f"Government contracts: {count} rows loaded")


@cli.command()
def geo():
    """Load geographic boundary files and climate station metadata."""
    from app.loaders.boundaries import load_boundaries
    from app.loaders.climate_stations import load_climate_stations

    logger = logging.getLogger(__name__)

    logger.info("Loading geographic boundaries...")
    boundary_results = load_boundaries()
    for layer, count in boundary_results.items():
        click.echo(f"  {layer}: {count} features loaded")

    logger.info("Loading climate station metadata...")
    station_count = load_climate_stations()
    click.echo(f"  climate_stations: {station_count} stations loaded")


@cli.command()
def infrastructure():
    """Load infrastructure data (buildings, footprints)."""
    logger = logging.getLogger(__name__)
    logger.info("Loading infrastructure data...")
    click.echo("Infrastructure loader: not yet implemented")


if __name__ == "__main__":
    cli()
