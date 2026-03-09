from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    db_host: str
    db_name: str
    db_user: str
    db_password: str
    db_port: int
    db_sslmode: str

    hh_base_url: str
    hh_user_agent: str
    hh_timeout_sec: int
    hh_sleep_between_requests_sec: float


def _load_dotenv_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = value


def _load_dotenv() -> None:
    package_dir = Path(__file__).resolve().parent
    project_root = package_dir.parent
    _load_dotenv_file(project_root / ".env")
    _load_dotenv_file(package_dir / ".env")


def get_settings() -> Settings:
    _load_dotenv()
    return Settings(
        db_host=os.getenv("DB_HOST", "109.71.245.173"),
        db_name=os.getenv("DB_NAME", "default_db"),
        db_user=os.getenv("DB_USER", "gen_user"),
        db_password=os.getenv("DB_PASSWORD", ""),
        db_port=int(os.getenv("DB_PORT", "5432")),
        db_sslmode=os.getenv("DB_SSLMODE", "require"),
        hh_base_url=os.getenv("HH_BASE_URL", "https://api.hh.ru"),
        hh_user_agent=os.getenv("HH_USER_AGENT", "HHParserBot/2.0"),
        hh_timeout_sec=int(os.getenv("HH_TIMEOUT_SEC", "20")),
        hh_sleep_between_requests_sec=float(
            os.getenv("HH_SLEEP_BETWEEN_REQUESTS_SEC", "0.25")
        ),
    )
