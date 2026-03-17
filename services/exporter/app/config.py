"""CDO Exporter configuration."""

import os


class Settings:
    """Application settings loaded from environment variables."""

    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    VANCOUVER_API_KEY: str = os.getenv("VANCOUVER_API_KEY", "")

    # Cache TTLs in seconds per domain
    CACHE_TTLS: dict = {
        "economy": 86400,       # 24 hours - StatCan data updates monthly
        "weather": 3600,        # 1 hour - weather updates frequently
        "transit": 604800,      # 7 days - GTFS updates weekly
        "census": 2592000,      # 30 days - census data is static
        "housing": 86400,       # 24 hours - housing data updates monthly
        "crime": 604800,        # 7 days - crime statistics update annually
        "municipal": 86400,     # 24 hours - permits/crime update daily
        "infrastructure": 604800,  # 7 days - building data rarely changes
        "immigration": 604800,     # 7 days - IRCC data updates quarterly
        "demographics": 604800,    # 7 days - population updates quarterly
        "government": 86400,       # 24 hours - contracts/grants update infrequently
    }


settings = Settings()
