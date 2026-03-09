from __future__ import annotations

from dataclasses import dataclass

from requests import HTTPError

from .hh_client import HHClient
from .repository import finish_parse_run, save_vacancy_with_skills, start_parse_run
from .transform import build_vacancy, normalize_skills


@dataclass
class LoadStats:
    pages_scanned: int = 0
    vacancies_seen: int = 0
    vacancies_saved: int = 0
    vacancies_failed: int = 0
    parse_run_id: int | None = None


def load_vacancies(
    conn,
    hh_client: HHClient,
    query: str,
    area: int,
    pages: int,
    per_page: int,
    only_with_salary: bool,
) -> LoadStats:
    stats = LoadStats()
    errors_shown = 0
    max_errors_to_show = 10
    error_sample: str | None = None

    run_id = start_parse_run(conn, query, area, pages, per_page, only_with_salary)
    conn.commit()
    stats.parse_run_id = run_id

    try:
        for page in range(pages):
            payload = hh_client.search_vacancies(
                query=query,
                area=area,
                page=page,
                per_page=per_page,
                only_with_salary=only_with_salary,
            )
            items = payload.get("items", [])
            if not items:
                break

            stats.pages_scanned += 1
            for item in items:
                stats.vacancies_seen += 1
                vacancy_id = item.get("id")
                if not vacancy_id:
                    stats.vacancies_failed += 1
                    continue

                try:
                    details = hh_client.get_vacancy(vacancy_id)
                    vacancy = build_vacancy(details, query=query)
                    skills = normalize_skills(details)
                    save_vacancy_with_skills(conn, vacancy, skills)
                    conn.commit()
                    stats.vacancies_saved += 1
                except HTTPError as exc:
                    conn.rollback()
                    stats.vacancies_failed += 1
                    error_sample = error_sample or str(exc)
                    if errors_shown < max_errors_to_show:
                        print(f"[HTTP ERROR] vacancy_id={vacancy_id}: {exc}")
                        errors_shown += 1
                except Exception as exc:
                    conn.rollback()
                    stats.vacancies_failed += 1
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

