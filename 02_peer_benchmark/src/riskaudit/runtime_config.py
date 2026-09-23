"""Runtime configuration for the RiskAudit application boundary.

The calculation modules accept explicit paths already.  This module centralizes the
application defaults and environment overrides so the web entry point no longer
scatters machine-specific paths throughout ``app/server.py``.
"""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Mapping

from .local_market_connection import local_market_environment


def _path(value: str | Path, *, base_dir: str | Path | None = None) -> Path:
    """Expand a configured path and anchor relative values to ``base_dir``.

    Environment files can therefore use portable values such as ``data`` or
    ``data/business/history/current`` instead of a drive letter or user home.
    """

    path = Path(value).expanduser()
    if base_dir is not None and not path.is_absolute():
        path = Path(base_dir).expanduser() / path
    return path.resolve()


def _env_path(
    environment: Mapping[str, str],
    name: str,
    default: str | Path,
    *,
    base_dir: str | Path | None = None,
) -> Path:
    raw = environment.get(name)
    return (
        _path(raw, base_dir=base_dir)
        if raw and raw.strip()
        else _path(default, base_dir=base_dir)
    )


def _env_optional_path(
    environment: Mapping[str, str],
    name: str,
    *,
    base_dir: str | Path | None = None,
) -> Path | None:
    raw = environment.get(name)
    return _path(raw, base_dir=base_dir) if raw and raw.strip() else None


def _env_int(
    environment: Mapping[str, str],
    name: str,
    default: int,
    *,
    minimum: int,
    maximum: int,
) -> int:
    raw = environment.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} 必须是整数") from exc
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} 必须在 {minimum} 到 {maximum} 之间")
    return value


def _env_bool(
    environment: Mapping[str, str], name: str, default: bool
) -> bool:
    raw = environment.get(name)
    if raw is None or not raw.strip():
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} 必须是 true 或 false")


@dataclass(frozen=True)
class RuntimeSettings:
    """Resolved settings used by the current single-process application."""

    project_root: Path
    data_root: Path
    output_root: Path
    upload_root: Path
    job_root: Path
    temporary_root: Path
    log_root: Path
    backup_root: Path
    sqlite_path: Path
    broker_history_dir: Path
    broker_legacy_history_dir: Path
    broker_industry_history_dir: Path
    broker_universe_dir: Path
    broker_reference_dir: Path
    broker_calendar_path: Path
    broker_st_path: Path
    broker_lifecycle_path: Path
    market_source_mode: str
    market_p3_order_book_ids: tuple[str, ...]
    market_lake_root: Path
    p4_market_lake_root: Path
    p4_backfill_status_path: Path
    rqdata_cache_root: Path
    bind_host: str
    port: int
    max_upload_bytes: int
    environment_name: str
    allow_remote: bool
    allow_anonymous_remote: bool
    allowed_hosts: tuple[str, ...]
    minimum_free_mb: int
    log_level: str
    auth_mode: str
    cookie_secure: bool
    session_hours: int
    tls_mode: str
    tls_cert_path: Path | None
    tls_key_path: Path | None

    @classmethod
    def from_environment(
        cls,
        project_root: str | Path,
        environment: Mapping[str, str] | None = None,
    ) -> "RuntimeSettings":
        root = _path(project_root).resolve()
        env = local_market_environment(root, os.environ if environment is None else environment)

        def configured_path(name: str, default: str | Path) -> Path:
            return _env_path(env, name, default, base_dir=root)

        def optional_path(name: str) -> Path | None:
            return _env_optional_path(env, name, base_dir=root)

        data_root = configured_path("RISKAUDIT_DATA_ROOT", root / "data")
        output_root = configured_path(
            "RISKAUDIT_OUTPUT_ROOT", data_root / "outputs"
        )
        temporary_root = configured_path(
            "RISKAUDIT_TEMP_ROOT", data_root / "temporary"
        )
        log_root = configured_path("RISKAUDIT_LOG_ROOT", data_root / "logs")
        backup_root = configured_path(
            "RISKAUDIT_BACKUP_ROOT", data_root / "backups"
        )

        broker_reference_dir = configured_path(
            "RISKAUDIT_BROKER_REFERENCE_DIR",
            data_root / "business" / "reference",
        )
        market_source_mode = env.get(
            "RISKAUDIT_MARKET_SOURCE", "SYNTHETIC"
        ).strip().upper()
        if market_source_mode not in {"SYNTHETIC", "RQDATA"}:
            raise ValueError("RISKAUDIT_MARKET_SOURCE 只能是 SYNTHETIC 或 RQDATA")
        auth_mode = env.get("RISKAUDIT_AUTH_MODE", "DISABLED").strip().upper()
        if auth_mode not in {"DISABLED", "ENABLED"}:
            raise ValueError("RISKAUDIT_AUTH_MODE 只能是 DISABLED 或 ENABLED")
        environment_name = env.get("RISKAUDIT_ENV", "LOCAL").strip().upper()
        if environment_name not in {"LOCAL", "SERVER", "TEST"}:
            raise ValueError("RISKAUDIT_ENV 只能是 LOCAL、SERVER 或 TEST")
        tls_mode = env.get("RISKAUDIT_TLS_MODE", "DISABLED").strip().upper()
        if tls_mode not in {"DISABLED", "DIRECT", "REVERSE_PROXY"}:
            raise ValueError(
                "RISKAUDIT_TLS_MODE 只能是 DISABLED、DIRECT 或 REVERSE_PROXY"
            )
        tls_cert_path = optional_path("RISKAUDIT_TLS_CERT_PATH")
        tls_key_path = optional_path("RISKAUDIT_TLS_KEY_PATH")
        if tls_mode == "DIRECT" and (tls_cert_path is None or tls_key_path is None):
            raise ValueError("DIRECT TLS 必须同时配置证书和私钥路径")
        if tls_mode != "DIRECT" and (tls_cert_path is not None or tls_key_path is not None):
            raise ValueError("仅 DIRECT TLS 可以配置证书和私钥路径")
        allowed_hosts = tuple(
            item.strip().lower()
            for item in env.get(
                "RISKAUDIT_ALLOWED_HOSTS", "127.0.0.1,localhost,[::1]"
            ).split(",")
            if item.strip()
        )
        if any(item == "*" for item in allowed_hosts):
            raise ValueError("RISKAUDIT_ALLOWED_HOSTS 禁止使用通配符 *")
        log_level = env.get("RISKAUDIT_LOG_LEVEL", "INFO").strip().upper()
        if log_level not in {"DEBUG", "INFO", "WARNING", "ERROR"}:
            raise ValueError(
                "RISKAUDIT_LOG_LEVEL 只能是 DEBUG、INFO、WARNING 或 ERROR"
            )
        market_p3_order_book_ids = tuple(
            item.strip()
            for item in env.get(
                "RISKAUDIT_RQDATA_P3_IDS",
                "",
            ).split(",")
            if item.strip()
        )
        legacy_market_root = data_root / (
            "market_lake_rqdata_p3" if market_source_mode == "RQDATA" else "market_lake"
        )
        market_lake_root = configured_path(
            "RISKAUDIT_MARKET_LAKE_ROOT", legacy_market_root
        )
        p4_market_lake_root = configured_path(
            "RISKAUDIT_P4_MARKET_LAKE_ROOT",
            data_root / "market_lake_rqdata_p4",
        )

        return cls(
            project_root=root,
            data_root=data_root,
            output_root=output_root,
            upload_root=configured_path(
                "RISKAUDIT_UPLOAD_ROOT", output_root / "frontend_uploads"
            ),
            job_root=configured_path(
                "RISKAUDIT_JOB_ROOT", output_root / "frontend_jobs"
            ),
            temporary_root=temporary_root,
            log_root=log_root,
            backup_root=backup_root,
            sqlite_path=configured_path(
                "RISKAUDIT_SQLITE_PATH", data_root / "app.db"
            ),
            broker_history_dir=configured_path(
                "RISKAUDIT_BROKER_HISTORY_DIR",
                data_root / "business" / "history" / "current",
            ),
            broker_legacy_history_dir=configured_path(
                "RISKAUDIT_BROKER_LEGACY_HISTORY_DIR",
                data_root / "business" / "history" / "legacy",
            ),
            broker_industry_history_dir=configured_path(
                "RISKAUDIT_BROKER_INDUSTRY_HISTORY_DIR",
                data_root / "business" / "history" / "industry",
            ),
            broker_universe_dir=configured_path(
                "RISKAUDIT_BROKER_UNIVERSE_DIR",
                data_root / "business" / "universe",
            ),
            broker_reference_dir=broker_reference_dir,
            broker_calendar_path=configured_path(
                "RISKAUDIT_BROKER_CALENDAR_PATH",
                broker_reference_dir / "A股交易日历_20231009_20260731.csv",
            ),
            broker_st_path=configured_path(
                "RISKAUDIT_BROKER_ST_PATH",
                output_root
                / "reference_inputs"
                / "RQData完整每日ST状态_增强退市证券_v1_20231009_20260731.csv.gz",
            ),
            broker_lifecycle_path=configured_path(
                "RISKAUDIT_BROKER_LIFECYCLE_PATH",
                broker_reference_dir / "RQData全部股票合约_截至20260731.csv",
            ),
            market_source_mode=market_source_mode,
            market_p3_order_book_ids=market_p3_order_book_ids,
            market_lake_root=market_lake_root,
            p4_market_lake_root=p4_market_lake_root,
            p4_backfill_status_path=configured_path(
                "RISKAUDIT_P4_BACKFILL_STATUS",
                p4_market_lake_root / "manifests" / "backfill_status.json",
            ),
            rqdata_cache_root=configured_path(
                "RISKAUDIT_RQDATA_CACHE_ROOT", data_root / "cache" / "rqdata"
            ),
            bind_host=env.get("RISKAUDIT_BIND_HOST", "127.0.0.1").strip()
            or "127.0.0.1",
            port=_env_int(
                env,
                "RISKAUDIT_PORT",
                8010,
                minimum=1,
                maximum=65535,
            ),
            max_upload_bytes=_env_int(
                env,
                "RISKAUDIT_MAX_UPLOAD_BYTES",
                512 * 1024 * 1024,
                minimum=1,
                maximum=8 * 1024 * 1024 * 1024,
            ),
            environment_name=environment_name,
            allow_remote=_env_bool(env, "RISKAUDIT_ALLOW_REMOTE", False),
            allow_anonymous_remote=_env_bool(
                env, "RISKAUDIT_ALLOW_ANONYMOUS_REMOTE", False
            ),
            allowed_hosts=allowed_hosts,
            minimum_free_mb=_env_int(
                env,
                "RISKAUDIT_MIN_FREE_MB",
                1024,
                minimum=128,
                maximum=1024 * 1024,
            ),
            log_level=log_level,
            auth_mode=auth_mode,
            cookie_secure=_env_bool(
                env, "RISKAUDIT_COOKIE_SECURE", False
            ),
            session_hours=_env_int(
                env,
                "RISKAUDIT_SESSION_HOURS",
                12,
                minimum=1,
                maximum=24 * 30,
            ),
            tls_mode=tls_mode,
            tls_cert_path=tls_cert_path,
            tls_key_path=tls_key_path,
        )

    @property
    def auth_enabled(self) -> bool:
        return self.auth_mode == "ENABLED"

    @property
    def server_mode(self) -> bool:
        return self.environment_name == "SERVER"

    @property
    def direct_tls(self) -> bool:
        return self.tls_mode == "DIRECT"

    def managed_runtime_paths(self) -> tuple[Path, ...]:
        """Return live application paths that must remain under the data root.

        The backup root is deliberately excluded because a useful backup must live
        on a separate disk, NAS, or other independently managed location.
        """

        return (
            self.output_root,
            self.upload_root,
            self.job_root,
            self.temporary_root,
            self.log_root,
            self.sqlite_path,
        )

    def assert_managed_paths_within_data_root(self) -> None:
        """Reject a server configuration whose writable paths escape the data root."""

        approved = self.data_root.resolve()
        escaped = [
            path
            for path in self.managed_runtime_paths()
            if not path.resolve().is_relative_to(approved)
        ]
        if escaped:
            rendered = ", ".join(str(path) for path in escaped)
            raise ValueError(
                "RiskAudit 可写路径必须位于 RISKAUDIT_DATA_ROOT 内：" + rendered
            )
