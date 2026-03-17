"""Demographics loader - downloads StatCan population data into census schema."""

import csv
import io
import logging
import zipfile
from pathlib import Path

import httpx

from app.db import get_connection

logger = logging.getLogger(__name__)

CACHE_DIR = Path("/app/cache")

# StatCan Table 17-10-0005-01: Population estimates, quarterly
# This is the main population-by-province table
POPULATION_CSV_URL = (
    "https://www150.statcan.gc.ca/n1/tbl/csv/17100005-eng.zip"
)

# Province GEO codes used by StatCan in this table
STATCAN_GEO_TO_PROVINCE = {
    "Canada": ("CA", "Canada"),
    "Newfoundland and Labrador": ("NL", "Newfoundland and Labrador"),
    "Prince Edward Island": ("PE", "Prince Edward Island"),
    "Nova Scotia": ("NS", "Nova Scotia"),
    "New Brunswick": ("NB", "New Brunswick"),
    "Quebec": ("QC", "Quebec"),
    "Ontario": ("ON", "Ontario"),
    "Manitoba": ("MB", "Manitoba"),
    "Saskatchewan": ("SK", "Saskatchewan"),
    "Alberta": ("AB", "Alberta"),
    "British Columbia": ("BC", "British Columbia"),
    "Yukon": ("YT", "Yukon"),
    "Northwest Territories": ("NT", "Northwest Territories"),
    "Nunavut": ("NU", "Nunavut"),
}

# Standard age groups for age_distribution
AGE_GROUPS = [
    "0-4", "5-9", "10-14", "15-19", "20-24", "25-29",
    "30-34", "35-39", "40-44", "45-49", "50-54", "55-59",
    "60-64", "65-69", "70-74", "75-79", "80-84", "85+",
]


def _download_and_extract_csv(url: str, cache_name: str) -> str:
    """Download a StatCan zip file and extract the CSV content."""
    zip_path = CACHE_DIR / f"{cache_name}.zip"

    if not zip_path.exists():
        zip_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info("Downloading %s ...", url)
        with httpx.Client(timeout=120, follow_redirects=True) as client:
            resp = client.get(url)
            resp.raise_for_status()
            zip_path.write_bytes(resp.content)
        logger.info("Downloaded %s (%.1f MB)", zip_path.name, zip_path.stat().st_size / 1e6)
    else:
        logger.info("Using cached: %s", zip_path.name)

    # Extract the main data CSV (not the metadata file)
    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_files = [n for n in zf.namelist() if n.endswith(".csv") and "MetaData" not in n]
        if not csv_files:
            raise FileNotFoundError(f"No data CSV found in {zip_path}")
        csv_name = csv_files[0]
        logger.info("Extracting %s", csv_name)
        return zf.read(csv_name).decode("utf-8-sig")


def _load_population(csv_content: str) -> int:
    """Parse StatCan 17-10-0005 and load into census.population.

    This table has quarterly population estimates by province.
    We take Q1 (January 1st) of each year as the annual figure.
    """
    reader = csv.DictReader(io.StringIO(csv_content))
    rows_by_year = {}  # (geo_uid, year) -> population

    for row in reader:
        ref_date = row.get("REF_DATE", "").strip()
        geo = row.get("GEO", "").strip()
        sex_val = row.get("Sex", row.get("SEX", "")).strip()
        age_group = row.get("Age group", row.get("AGE_GROUP", "")).strip()
        value = row.get("VALUE", "").strip()

        if not ref_date or not geo or not value:
            continue

        # Match province
        province_info = STATCAN_GEO_TO_PROVINCE.get(geo)
        if not province_info:
            continue

        geo_uid, geo_name = province_info

        # Parse year from REF_DATE (format: "2024-01" or "2024-Q1" or "2024")
        try:
            year = int(ref_date[:4])
        except ValueError:
            continue

        # We want "Both sexes" and "All ages" for the population total
        sex_lower = sex_val.lower() if sex_val else ""
        age_lower = age_group.lower() if age_group else ""

        is_both_sexes = "both" in sex_lower or sex_lower == "" or sex_lower == "total"
        is_all_ages = "all" in age_lower or age_lower == "" or age_lower == "total"

        if is_both_sexes and is_all_ages:
            try:
                pop = int(float(value))
            except ValueError:
                continue

            key = (geo_uid, year)
            # Keep the latest value for each year (Q4 > Q3 > Q2 > Q1)
            rows_by_year[key] = (geo_name, pop)

    logger.info("Parsed %d province-year population records", len(rows_by_year))

    loaded = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for (geo_uid, year), (geo_name, pop) in rows_by_year.items():
                geo_level = "country" if geo_uid == "CA" else "province"
                try:
                    cur.execute(
                        """
                        INSERT INTO census.population (geo_uid, geo_level, year, population, data_source)
                        VALUES (%s, %s, %s, %s, 'statcan_17-10-0005')
                        ON CONFLICT (geo_uid, geo_level, year) DO UPDATE SET
                            population = EXCLUDED.population,
                            data_source = EXCLUDED.data_source
                        """,
                        (geo_uid, geo_level, year, pop),
                    )
                    loaded += 1
                except Exception:
                    logger.exception("Failed to insert population for %s/%d", geo_uid, year)

    return loaded


def _load_age_distribution(csv_content: str) -> int:
    """Parse age group breakdowns from StatCan 17-10-0005 into census.age_distribution.

    Extracts population by age group and sex for each province/year.
    """
    reader = csv.DictReader(io.StringIO(csv_content))

    # Collect (geo_uid, year, age_group, sex) -> count
    records = {}

    for row in reader:
        ref_date = row.get("REF_DATE", "").strip()
        geo = row.get("GEO", "").strip()
        sex_val = row.get("Sex", row.get("SEX", "")).strip()
        age_group_raw = row.get("Age group", row.get("AGE_GROUP", "")).strip()
        value = row.get("VALUE", "").strip()

        if not ref_date or not geo or not value or not age_group_raw:
            continue

        province_info = STATCAN_GEO_TO_PROVINCE.get(geo)
        if not province_info:
            continue

        geo_uid, _ = province_info

        try:
            year = int(ref_date[:4])
        except ValueError:
            continue

        # Map sex
        sex_lower = sex_val.lower() if sex_val else ""
        if "male" in sex_lower and "female" not in sex_lower:
            sex = "M"
        elif "female" in sex_lower:
            sex = "F"
        elif "both" in sex_lower or "total" in sex_lower:
            sex = "T"
        else:
            continue

        # Normalize age group to match our schema
        age_group = _normalize_age_group(age_group_raw)
        if not age_group:
            continue

        try:
            count = int(float(value))
        except ValueError:
            continue

        key = (geo_uid, year, age_group, sex)
        records[key] = count

    logger.info("Parsed %d age distribution records", len(records))

    loaded = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for (geo_uid, year, age_group, sex), count in records.items():
                geo_level = "country" if geo_uid == "CA" else "province"
                try:
                    cur.execute(
                        """
                        INSERT INTO census.age_distribution
                            (geo_uid, geo_level, year, age_group, sex, count, data_source)
                        VALUES (%s, %s, %s, %s, %s, %s, 'statcan_17-10-0005')
                        ON CONFLICT (geo_uid, geo_level, year, age_group, sex) DO UPDATE SET
                            count = EXCLUDED.count,
                            data_source = EXCLUDED.data_source
                        """,
                        (geo_uid, geo_level, year, age_group, sex, count),
                    )
                    loaded += 1
                except Exception:
                    logger.exception(
                        "Failed to insert age dist for %s/%d/%s/%s",
                        geo_uid, year, age_group, sex,
                    )

    return loaded


def _normalize_age_group(raw: str) -> str | None:
    """Normalize StatCan age group strings to our standard format.

    StatCan uses formats like:
      '0 to 4 years', '5 to 9 years', '85 years and over', 'All ages'
    We normalize to: '0-4', '5-9', ..., '85+'
    """
    raw_lower = raw.lower().strip()

    if "all ages" in raw_lower or "total" in raw_lower:
        return None  # Skip totals for age distribution

    if "over" in raw_lower or "and older" in raw_lower:
        # "85 years and over" -> "85+"
        digits = "".join(c for c in raw_lower.split()[0] if c.isdigit())
        if digits:
            return f"{digits}+"
        return None

    # "0 to 4 years" -> "0-4"
    if " to " in raw_lower:
        parts = raw_lower.split(" to ")
        low = "".join(c for c in parts[0] if c.isdigit())
        high_part = parts[1].split()[0] if len(parts) > 1 else ""
        high = "".join(c for c in high_part if c.isdigit())
        if low and high:
            return f"{low}-{high}"

    return None


def load_demographics() -> dict:
    """Download and load demographics data from StatCan.

    Returns dict with counts of records loaded.
    """
    logger.info("=== Loading demographics data ===")

    try:
        csv_content = _download_and_extract_csv(POPULATION_CSV_URL, "17100005")
    except Exception:
        logger.exception("Failed to download population data")
        return {"population": 0, "age_distribution": 0}

    pop_count = _load_population(csv_content)
    logger.info("Loaded %d population records", pop_count)

    age_count = _load_age_distribution(csv_content)
    logger.info("Loaded %d age distribution records", age_count)

    return {"population": pop_count, "age_distribution": age_count}
