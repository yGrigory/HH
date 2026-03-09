from __future__ import annotations

import argparse

from psycopg2 import OperationalError

from .config import get_settings
from .db import connection_scope
from .hh_client import HHClient
from .pipeline import load_vacancies
from .schema import create_schema, recreate_schema


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="HH vacancies parser to PostgreSQL")
    parser.add_argument("--query", default="data analyst", help="Search text for HH API")
    parser.add_argument("--area", type=int, default=113, help="HH area id (113 = Russia)")
    parser.add_argument("--pages", type=int, default=3, help="Pages to fetch")
    parser.add_argument("--per-page", type=int, default=20, help="Items per page")
    parser.add_argument(
        "--with-salary-only",
        action="store_true",
        help="Fetch only vacancies with salary",
    )
    parser.add_argument(
        "--recreate-schema",
        action="store_true",
        help="Drop and recreate tables before loading",
    )
    return parser.parse_args()


def run() -> None:
    args = parse_args()
    settings = get_settings()
    client = HHClient(settings)

    try:
        with connection_scope(settings) as conn:
            if args.recreate_schema:
                recreate_schema(conn)
            else:
                create_schema(conn)

            stats = load_vacancies(
                conn=conn,
                hh_client=client,
                query=args.query,
                area=args.area,
                pages=args.pages,
                per_page=args.per_page,
                only_with_salary=args.with_salary_only,
                cooldown_403_threshold=settings.hh_403_cooldown_threshold,
                cooldown_403_sec=settings.hh_403_cooldown_sec,
            )
    except OperationalError as exc:
        raise SystemExit(
            "Database connection failed. Check DB_HOST, DB_PORT, DB_NAME, "
            "DB_USER, DB_PASSWORD, DB_SSLMODE in environment or .env file.\n"
            f"Details: {exc}"
        )

    print(
        "Done. "
        f"pages_scanned={stats.pages_scanned}, "
        f"vacancies_seen={stats.vacancies_seen}, "
        f"vacancies_saved={stats.vacancies_saved}, "
        f"vacancies_failed={stats.vacancies_failed}"
    )


if __name__ == "__main__":
    run()
