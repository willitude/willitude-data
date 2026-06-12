"""AWS SSM Parameter Store access for API keys (Tardis + Databento).

Keys are fetched at runtime using the caller's AWS credentials (SSO profile or IAM role).
They are held only in process memory and never persisted to disk or logged.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Literal

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from .config import get_config

logger = logging.getLogger(__name__)

Provider = Literal["tardis", "databento"]


@dataclass
class CachedKey:
    value: str
    fetched_at: float


class SSMKeyProvider:
    """Thread-safe enough in-process cache for the two secret values."""

    def __init__(self, profile: str | None = None, region: str | None = None) -> None:
        cfg = get_config()
        self.profile = profile or cfg.aws_profile
        self.region = region or cfg.aws_region
        self._cache: dict[Provider, CachedKey] = {}
        self._ssm = self._make_client()

    def _make_client(self):
        try:
            if self.profile:
                session = boto3.Session(profile_name=self.profile)
                return session.client("ssm", region_name=self.region)
            return boto3.client("ssm", region_name=self.region)
        except Exception as exc:
            logger.exception("Failed to create SSM client")
            raise RuntimeError(f"Unable to initialize AWS SSM client: {exc}") from exc

    def _fetch(self, name: str) -> str:
        try:
            resp = self._ssm.get_parameter(Name=name, WithDecryption=True)
            val = resp["Parameter"]["Value"]
            if not val:
                raise ValueError(f"SSM parameter {name} is empty")
            return val
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("ParameterNotFound", "AccessDenied", "AccessDeniedException"):
                raise PermissionError(
                    f"Cannot read SSM parameter {name}. "
                    "Check that your AWS principal (SSO role or instance role) has "
                    "ssm:GetParameter permission on it, and that you are in the correct account/region."
                ) from exc
            raise
        except NoCredentialsError as exc:
            raise RuntimeError(
                "No AWS credentials. Run `aws sso login --profile YongseokMacProfile` "
                "or ensure IAM role is attached when running on AWS."
            ) from exc

    def get_key(self, provider: Provider) -> str:
        """Return the API key for the provider, using in-memory cache."""
        cfg = get_config()
        name = cfg.tardis_ssm_name if provider == "tardis" else cfg.databento_ssm_name

        cached = self._cache.get(provider)
        # Very simple: refresh after 6 hours or on first use
        if cached and (time.time() - cached.fetched_at) < 6 * 3600:
            return cached.value

        value = self._fetch(name)
        self._cache[provider] = CachedKey(value=value, fetched_at=time.time())
        # Never log the actual key
        logger.info("Fetched %s API key from SSM (length=%d)", provider, len(value))
        return value

    def clear_cache(self) -> None:
        self._cache.clear()


# Global instance (created lazily on first use)
_key_provider: SSMKeyProvider | None = None


def get_key_provider() -> SSMKeyProvider:
    global _key_provider
    if _key_provider is None:
        _key_provider = SSMKeyProvider()
    return _key_provider


def get_tardis_key() -> str:
    return get_key_provider().get_key("tardis")


def get_databento_key() -> str:
    return get_key_provider().get_key("databento")
