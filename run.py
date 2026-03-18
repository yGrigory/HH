from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from psycopg2 import OperationalError

from parser.config import get_settings
from parser.db import connection_scope
from parser.hh_client import HHClient
from parser.it_queries import (
    DEFAULT_IT_ROLE_QUERIES,
    load_role_queries_from_file,
    load_role_queries_from_skills_file,
    normalize_role_queries,
)
from parser.pipeline import LoadStats, load_vacancies
from parser.repository import get_last_successful_run_finished_at
from parser.schema import create_schema, recreate_schema


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _format_hh_datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S%z")


def _parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _load_queries() -> list[str]:
    # Backward compatible: IT_SKILLS_FILE can point either to a skills list
    # (to map a subset into role queries) or to a direct role-queries file.
    skills_file = os.getenv("IT_SKILLS_FILE", "").strip()
    if skills_file:
        file_path = Path(skills_file)
        queries = load_role_queries_from_file(file_path)
        if not queries:
            queries = load_role_queries_from_skills_file(file_path)
        if queries:
            return queries

    role_raw = os.getenv("IT_ROLE_QUERIES", "").strip()
    if role_raw:
        return normalize_role_queries([part.strip() for part in role_raw.split(",")])

    # Legacy env name kept for smooth deployments.
    raw = os.getenv("IT_QUERIES", "").strip()
    if raw:
        return normalize_role_queries([part.strip() for part in raw.split(",")])

    return DEFAULT_IT_ROLE_QUERIES


def _build_backfill_windows(
    period_start: datetime,
    period_end: datetime,
    window_days: int,
    overlap_hours: int,
) -> list[tuple[datetime, datetime]]:
    windows: list[tuple[datetime, datetime]] = []
    window_size = timedelta(days=max(1, window_days))
    overlap = timedelta(hours=max(0, overlap_hours))
    cursor = period_end

    while cursor > period_start:
        window_start = max(period_start, cursor - window_size)
        windows.append((window_start, cursor))
        if window_start <= period_start:
            break
        cursor = window_start + overlap

    return windows


def _resolve_query_windows(
    conn,
    query: str,
    area: int,
    only_with_salary: bool,
    period_days: int,
    window_days: int,
    backfill_overlap_hours: int,
    incremental_overlap_minutes: int,
) -> tuple[list[tuple[datetime, datetime]], str]:
    now_utc = datetime.now(timezone.utc)
    period_start = now_utc - timedelta(days=max(1, period_days))
    last_success = get_last_successful_run_finished_at(conn, query, area, only_with_salary)

    if last_success is not None:
        if last_success.tzinfo is None:
            last_success = last_success.replace(tzinfo=timezone.utc)
        incremental_start = max(
            period_start,
            last_success - timedelta(minutes=max(0, incremental_overlap_minutes)),
        )
        return [(incremental_start, now_utc)], "incremental"

    return (
        _build_backfill_windows(
            period_start=period_start,
            period_end=now_utc,
            window_days=window_days,
            overlap_hours=backfill_overlap_hours,
        ),
        "backfill",
    )


def _merge_stats(total: LoadStats, part: LoadStats) -> None:
    total.pages_scanned += part.pages_scanned
    total.vacancies_seen += part.vacancies_seen
    total.vacancies_saved += part.vacancies_saved
    total.vacancies_failed += part.vacancies_failed
    total.vacancies_skipped_existing += part.vacancies_skipped_existing


def main() -> None:
    settings = get_settings()
    client = HHClient(settings)

    area = int(os.getenv("IT_AREA", "113"))
    pages = max(1, int(os.getenv("IT_PAGES", "20")))
    per_page = max(1, min(100, int(os.getenv("IT_PER_PAGE", "100"))))
    target_raw = int(os.getenv("IT_TARGET_PER_QUERY", "0"))
    target_per_query = target_raw if target_raw > 0 else None
    only_with_salary = _parse_bool(os.getenv("IT_WITH_SALARY_ONLY"), False)
    interval_minutes = int(os.getenv("IT_LOOP_INTERVAL_MINUTES", "5"))
    period_days = max(1, int(os.getenv("IT_PERIOD_DAYS", "60")))
    window_days = max(1, int(os.getenv("IT_WINDOW_DAYS", "7")))
    backfill_overlap_hours = max(0, int(os.getenv("IT_BACKFILL_OVERLAP_HOURS", "6")))
    incremental_overlap_minutes = max(
        0, int(os.getenv("IT_INCREMENTAL_OVERLAP_MINUTES", "30"))
    )
    skip_existing = _parse_bool(os.getenv("IT_SKIP_EXISTING"), True)
    recreate_on_start = _parse_bool(os.getenv("IT_RECREATE_ON_START"), False)
    run_once = _parse_bool(os.getenv("IT_RUN_ONCE"), False)
    queries = _load_queries()

    if not queries:
        raise SystemExit(
            "No role queries loaded. Provide IT_ROLE_QUERIES or IT_SKILLS_FILE "
            "(roles file or skills file for language-to-role mapping)."
        )

    target_label = "unlimited" if target_per_query is None else str(target_per_query)
    print(
        f"[{_now_utc()}] settings: area={area} pages={pages} per_page={per_page} "
        f"target_per_query={target_label} period_days={period_days} "
        f"window_days={window_days} backfill_overlap_hours={backfill_overlap_hours} "
        f"incremental_overlap_minutes={incremental_overlap_minutes} "
        f"with_salary_only={only_with_salary} skip_existing={skip_existing} "
        f"loop_interval_min={interval_minutes}"
    )

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
        print(
            f"[{_now_utc()}] cycle={cycle} started, queries={len(queries)}, "
            f"period_days={period_days}"
        )
        total_seen = 0
        total_saved = 0
        total_failed = 0
        total_skipped_existing = 0

        for query in queries:
            try:
                with connection_scope(settings) as conn:
                    windows, mode = _resolve_query_windows(
                        conn=conn,
                        query=query,
                        area=area,
                        only_with_salary=only_with_salary,
                        period_days=period_days,
                        window_days=window_days,
                        backfill_overlap_hours=backfill_overlap_hours,
                        incremental_overlap_minutes=incremental_overlap_minutes,
                    )
                    query_total = LoadStats()

                    for index, (date_from, date_to) in enumerate(windows, start=1):
                        remaining_target = None
                        if target_per_query is not None:
                            remaining_target = max(0, target_per_query - query_total.vacancies_saved)
                            if remaining_target == 0:
                                break

                        stats = load_vacancies(
                            conn=conn,
                            hh_client=client,
                            query=query,
                            area=area,
                            pages=pages,
                            per_page=per_page,
                            only_with_salary=only_with_salary,
                            date_from=_format_hh_datetime(date_from),
                            date_to=_format_hh_datetime(date_to),
                            order_by="publication_time",
                            max_vacancies_per_query=remaining_target,
                            cooldown_403_threshold=settings.hh_403_cooldown_threshold,
                            cooldown_403_sec=settings.hh_403_cooldown_sec,
                            skip_existing=skip_existing,
                        )
                        _merge_stats(query_total, stats)
                        print(
                            f"[{_now_utc()}] query='{query}' window={index}/{len(windows)} "
                            f"mode={mode} from={date_from.isoformat(timespec='seconds')} "
                            f"to={date_to.isoformat(timespec='seconds')} "
                            f"seen={stats.vacancies_seen} saved={stats.vacancies_saved} "
                            f"skipped_existing={stats.vacancies_skipped_existing} "
                            f"failed={stats.vacancies_failed} run_id={stats.parse_run_id}"
                        )

                total_seen += query_total.vacancies_seen
                total_saved += query_total.vacancies_saved
                total_failed += query_total.vacancies_failed
                total_skipped_existing += query_total.vacancies_skipped_existing
                print(
                    f"[{_now_utc()}] query='{query}' summary "
                    f"seen={query_total.vacancies_seen} "
                    f"saved={query_total.vacancies_saved} "
                    f"skipped_existing={query_total.vacancies_skipped_existing} "
                    f"failed={query_total.vacancies_failed} "
                    f"target={target_label}"
                )
            except Exception as exc:
                print(f"[{_now_utc()}] query='{query}' failed: {type(exc).__name__}: {exc}")

        print(
            f"[{_now_utc()}] cycle={cycle} finished "
            f"seen={total_seen} saved={total_saved} "
            f"skipped_existing={total_skipped_existing} failed={total_failed}"
        )

        if run_once:
            break

        sleep_sec = max(1, interval_minutes * 60)
        print(f"[{_now_utc()}] sleeping {interval_minutes} min")
        time.sleep(sleep_sec)


if __name__ == "__main__":
    main()
