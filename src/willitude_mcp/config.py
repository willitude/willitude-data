"""Configuration for Willitude Data MCP server."""

from __future__ import annotations

import os
from pathlib import Path

from pydantic import BaseModel, Field


class WillitudeConfig(BaseModel):
    """Central configuration loaded from env + sensible defaults."""

    # Cache location. Can be set via WILLITUDE_CACHE_DIR env.
    cache_dir: Path = Field(
        default_factory=lambda: Path.home() / ".willitude" / "willitude-data",
        description="Root directory for all cached market data (Parquet/CSV etc). "
        "Default: ~/.willitude/willitude-data/ (contains tardis/, databento/, manifest.jsonl).",
    )

    # AWS
    aws_profile: str | None = Field(
        default=None,
        description="AWS profile name for SSO / credentials (e.g. YongseokMacProfile). "
        "If None, uses default credential provider chain (works on AWS with IAM roles too).",
    )
    aws_region: str = Field(
        default="ap-northeast-1",
        description="AWS region for SSM (where the /willitude/* params live).",
    )

    # SSM parameter names (override only if org changes them)
    tardis_ssm_name: str = "/willitude/tardis/api-key"
    databento_ssm_name: str = "/willitude/databento/api-key"

    # Cache behavior
    cache_ttl_hours: int = Field(
        default=24 * 7,  # 1 week
        description=(
            "How long to consider cached data fresh before considering refresh "
            "(not strictly enforced for historical)."
        ),
    )
    convert_tardis_to_parquet: bool = Field(
        default=True,
        description=(
            "After downloading Tardis CSVs, also materialize a unified .parquet "
            "for easier loading with Polars/Pandas."
        ),
    )

    @classmethod
    def from_env(cls) -> WillitudeConfig:
        """Load overrides from environment variables (WILLITUDE_*)."""
        cache_dir = os.getenv("WILLITUDE_CACHE_DIR")
        aws_profile = os.getenv("AWS_PROFILE") or os.getenv("WILLITUDE_AWS_PROFILE")
        aws_region = os.getenv("AWS_REGION") or os.getenv("WILLITUDE_AWS_REGION")

        convert = os.getenv("WILLITUDE_CONVERT_TARDIS_PARQUET", "1").lower() not in {
            "0",
            "false",
            "no",
        }

        return cls(
            cache_dir=Path(cache_dir).expanduser().resolve() if cache_dir else cls.model_fields["cache_dir"].default,
            aws_profile=aws_profile,
            aws_region=aws_region or "ap-northeast-1",
            convert_tardis_to_parquet=convert,
        )


# Singleton-ish access
_config: WillitudeConfig | None = None


def get_config() -> WillitudeConfig:
    global _config
    if _config is None:
        _config = WillitudeConfig.from_env()
        _config.cache_dir.mkdir(parents=True, exist_ok=True)
    return _config


def get_tardis_cache_dir() -> Path:
    return get_config().cache_dir / "tardis"


def get_databento_cache_dir() -> Path:
    return get_config().cache_dir / "databento"
