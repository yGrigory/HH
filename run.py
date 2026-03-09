import os
import time
from datetime import datetime, timezone

from psycopg2 import OperationalError

from parser.config import get_settings
from parser.db import connection_scope
from parser.hh_client import HHClient
from parser.it_queries import DEFAULT_IT_QUERIES
from parser.pipeline import load_vacancies
from parser.schema import create_schema, recreate_schema


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _load_queries() -> list[str]:
    raw = os.getenv("IT_QUERIES", "").strip()
    source = DEFAULT_IT_QUERIES if not raw else [part.strip() for part in raw.split(",")]
    unique_queries: list[str] = []
    seen: set[str] = set()
    for query in source:
        query = query.strip()
        if not query:
            continue
        key = query.casefold()
        if key in seen:
            continue
        seen.add(key)
        unique_queries.append(query)
    return unique_queries


def main() -> None:
    settings = get_settings()
    client = HHClient(settings)

    area = int(os.getenv("IT_AREA", "113"))
    pages = max(1, int(os.getenv("IT_PAGES", "20")))
    per_page = max(1, min(100, int(os.getenv("IT_PER_PAGE", "100"))))
    target_per_query = max(1, int(os.getenv("IT_TARGET_PER_QUERY", "100")))
    only_with_salary = _parse_bool(os.getenv("IT_WITH_SALARY_ONLY"), False)
    interval_minutes = int(os.getenv("IT_LOOP_INTERVAL_MINUTES", "60"))
    recreate_on_start = _parse_bool(os.getenv("IT_RECREATE_ON_START"), False)
    run_once = _parse_bool(os.getenv("IT_RUN_ONCE"), False)
    queries = _load_queries()

    if not queries:
        raise SystemExit("No IT queries configured. Set IT_QUERIES or use defaults.")

    try:
        with connection_scope(settings) as conn:
            if recreate_on_start:
                recreate_schema(conn)
                print(f"[{_now_utc()}] schema recreated")
            else:
                create_schema(conn)
                print(f"[{_now_utc()}] schema checked/created")
    except OperationalError as exc:
        raise SystemExit(f"Database connection failed: {exc}")

    cycle = 0
    while True:
        cycle += 1
        print(f"[{_now_utc()}] cycle={cycle} started, queries={len(queries)}")
        total_seen = 0
        total_saved = 0
        total_failed = 0

        for query in queries:
            try:
                with connection_scope(settings) as conn:
                    stats = load_vacancies(
                        conn=conn,
                        hh_client=client,
                        query=query,
                        area=area,
                        pages=pages,
                        per_page=per_page,
                        only_with_salary=only_with_salary,
                        max_vacancies_per_query=target_per_query,
                        cooldown_403_threshold=settings.hh_403_cooldown_threshold,
                        cooldown_403_sec=settings.hh_403_cooldown_sec,
                    )
                total_seen += stats.vacancies_seen
                total_saved += stats.vacancies_saved
                total_failed += stats.vacancies_failed
                print(
                    f"[{_now_utc()}] query='{query}' "
                    f"seen={stats.vacancies_seen} "
                    f"saved={stats.vacancies_saved} "
                    f"failed={stats.vacancies_failed} "
                    f"target={target_per_query} "
                    f"run_id={stats.parse_run_id}"
                )
            except Exception as exc:
                print(f"[{_now_utc()}] query='{query}' failed: {type(exc).__name__}: {exc}")

        print(
            f"[{_now_utc()}] cycle={cycle} finished "
            f"seen={total_seen} saved={total_saved} failed={total_failed}"
        )

        if run_once:
            break

        sleep_sec = max(1, interval_minutes * 60)
        print(f"[{_now_utc()}] sleeping {interval_minutes} min")
        time.sleep(sleep_sec)


if __name__ == "__main__":
    main()
