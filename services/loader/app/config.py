"""CDO Loader configuration."""

import os


class Settings:
    """Application settings loaded from environment variables."""

    DATABASE_URL: str = os.getenv(
        "DATABASE_URL",
        "postgresql://cdo:cdo_secret@localhost:5432/cdo",
    )
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    VANCOUVER_API_KEY: str = os.getenv("VANCOUVER_API_KEY", "")


settings = Settings()
