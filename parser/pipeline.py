from __future__ import annotations

import time
from dataclasses import dataclass

from requests import HTTPError

from .hh_client import HHClient
from .repository import (
    finish_parse_run,
    get_existing_vacancy_hh_ids,
    save_vacancy_with_skills,
    start_parse_run,
)
from .transform import build_vacancy, normalize_skills


@dataclass
class LoadStats:
    pages_scanned: int = 0
    vacancies_seen: int = 0
    vacancies_saved: int = 0
    vacancies_failed: int = 0
    vacancies_skipped_existing: int = 0
    parse_run_id: int | None = None


def load_vacancies(
    conn,
    hh_client: HHClient,
    query: str,
    area: int,
    pages: int,
    per_page: int,
    only_with_salary: bool,
    date_from: str | None = None,
    date_to: str | None = None,
    order_by: str | None = "publication_time",
    max_vacancies_per_query: int | None = None,
    cooldown_403_threshold: int = 5,
    cooldown_403_sec: float = 90.0,
    skip_existing: bool = True,
) -> LoadStats:
    stats = LoadStats()
    errors_shown = 0
    max_errors_to_show = 10
    error_sample: str | None = None
    consecutive_403 = 0

    run_id = start_parse_run(conn, query, area, pages, per_page, only_with_salary)
    conn.commit()
    stats.parse_run_id = run_id

    try:
        target_saved = None
        if max_vacancies_per_query is not None and max_vacancies_per_query > 0:
            target_saved = max_vacancies_per_query

        for page in range(pages):
            if target_saved is not None and stats.vacancies_saved >= target_saved:
                break

            payload = hh_client.search_vacancies(
                query=query,
                area=area,
                page=page,
                per_page=per_page,
                only_with_salary=only_with_salary,
                date_from=date_from,
                date_to=date_to,
                order_by=order_by,
            )
            items = payload.get("items", [])
            if not items:
                break

            stats.pages_scanned += 1
            existing_ids: set[int] = set()
            if skip_existing:
                page_ids: list[int] = []
                for item in items:
                    vacancy_id = item.get("id")
                    if not vacancy_id:
                        continue
                    try:
                        page_ids.append(int(vacancy_id))
                    except (TypeError, ValueError):
                        continue
                existing_ids = get_existing_vacancy_hh_ids(conn, page_ids)

            for item in items:
                if target_saved is not None and stats.vacancies_saved >= target_saved:
                    break
                stats.vacancies_seen += 1
                vacancy_id = item.get("id")
                if not vacancy_id:
                    stats.vacancies_failed += 1
                    continue
                try:
                    vacancy_id_int = int(vacancy_id)
                except (TypeError, ValueError):
                    stats.vacancies_failed += 1
                    continue

                if vacancy_id_int in existing_ids:
                    stats.vacancies_skipped_existing += 1
                    continue

                try:
                    details = hh_client.get_vacancy(vacancy_id_int)
                    vacancy = build_vacancy(details, query=query)
                    skills = normalize_skills(details)
                    save_vacancy_with_skills(conn, vacancy, skills)
                    conn.commit()
                    stats.vacancies_saved += 1
                    consecutive_403 = 0
                except HTTPError as exc:
                    conn.rollback()
                    stats.vacancies_failed += 1
                    error_sample = error_sample or str(exc)
                    status_code = exc.response.status_code if exc.response is not None else None
                    if status_code == 403:
                        consecutive_403 += 1
                        threshold = max(1, cooldown_403_threshold)
                        if consecutive_403 >= threshold:
                            cooldown = max(1.0, cooldown_403_sec)
                            print(
                                "[COOLDOWN] "
                                f"query='{query}' got {consecutive_403} consecutive 403 responses; "
                                f"sleeping {int(cooldown)} sec"
                            )
                            time.sleep(cooldown)
                            consecutive_403 = 0
                    else:
                        consecutive_403 = 0
                    if errors_shown < max_errors_to_show:
                        print(f"[HTTP ERROR] vacancy_id={vacancy_id}: {exc}")
                        errors_shown += 1
                except Exception as exc:
                    conn.rollback()
                    stats.vacancies_failed += 1
                    consecutive_403 = 0
                    error_sample = error_sample or f"{type(exc).__name__}: {exc}"
                    if errors_shown < max_errors_to_show:
                        print(f"[SAVE ERROR] vacancy_id={vacancy_id}: {type(exc).__name__}: {exc}")
                        errors_shown += 1

        finish_parse_run(
            conn=conn,
            run_id=run_id,
            status="success",
            vacancies_seen=stats.vacancies_seen,
            vacancies_saved=stats.vacancies_saved,
            vacancies_failed=stats.vacancies_failed,
            error_sample=error_sample,
        )
        conn.commit()
        return stats
    except Exception as exc:
        conn.rollback()
        finish_parse_run(
            conn=conn,
            run_id=run_id,
            status="failed",
            vacancies_seen=stats.vacancies_seen,
            vacancies_saved=stats.vacancies_saved,
            vacancies_failed=stats.vacancies_failed,
            error_sample=f"{type(exc).__name__}: {exc}",
        )
        conn.commit()
        raise
