from __future__ import annotations

from psycopg2.extensions import connection as PgConnection
from psycopg2.extras import Json


def start_parse_run(
    conn: PgConnection,
    query: str,
    area: int,
    pages: int,
    per_page: int,
    only_with_salary: bool,
) -> int:
    sql = """
    INSERT INTO parse_runs (status, query, area, pages, per_page, only_with_salary)
    VALUES ('running', %s, %s, %s, %s, %s)
    RETURNING id;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (query, area, pages, per_page, only_with_salary))
        return int(cur.fetchone()[0])


def finish_parse_run(
    conn: PgConnection,
    run_id: int,
    status: str,
    vacancies_seen: int,
    vacancies_saved: int,
    vacancies_failed: int,
    error_sample: str | None = None,
) -> None:
    sql = """
    UPDATE parse_runs
    SET finished_at = NOW(),
        status = %s,
        vacancies_seen = %s,
        vacancies_saved = %s,
        vacancies_failed = %s,
        error_sample = %s
    WHERE id = %s;
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                status,
                vacancies_seen,
                vacancies_saved,
                vacancies_failed,
                error_sample,
                run_id,
            ),
        )


def upsert_vacancy(conn: PgConnection, vacancy: dict) -> int:
    payload = dict(vacancy)
    payload["raw_json"] = Json(vacancy["raw_json"])

    sql = """
    INSERT INTO vacancies (
        hh_id, title, profession_query, professional_role_id, professional_role_name,
        area, employer, employer_hh_id, employer_url,
        experience, hh_experience_id, grade,
        employment, hh_employment_id, schedule, hh_schedule_id,
        is_remote, has_test, is_archived,
        salary_min, salary_max, salary_mid, salary_gross, currency,
        address_city, lat, lng, snippet_requirement, snippet_responsibility,
        description, published_at, vacancy_url, api_url, raw_json, updated_at
    )
    VALUES (
        %(hh_id)s, %(title)s, %(profession_query)s, %(professional_role_id)s, %(professional_role_name)s,
        %(area)s, %(employer)s, %(employer_hh_id)s, %(employer_url)s,
        %(experience)s, %(hh_experience_id)s, %(grade)s,
        %(employment)s, %(hh_employment_id)s, %(schedule)s, %(hh_schedule_id)s,
        %(is_remote)s, %(has_test)s, %(is_archived)s,
        %(salary_min)s, %(salary_max)s, %(salary_mid)s, %(salary_gross)s, %(currency)s,
        %(address_city)s, %(lat)s, %(lng)s, %(snippet_requirement)s, %(snippet_responsibility)s,
        %(description)s, %(published_at)s, %(vacancy_url)s, %(api_url)s, %(raw_json)s, NOW()
    )
    ON CONFLICT (hh_id) DO UPDATE
    SET
        title = EXCLUDED.title,
        profession_query = EXCLUDED.profession_query,
        professional_role_id = EXCLUDED.professional_role_id,
        professional_role_name = EXCLUDED.professional_role_name,
        area = EXCLUDED.area,
        employer = EXCLUDED.employer,
        employer_hh_id = EXCLUDED.employer_hh_id,
        employer_url = EXCLUDED.employer_url,
        experience = EXCLUDED.experience,
        hh_experience_id = EXCLUDED.hh_experience_id,
        grade = EXCLUDED.grade,
        employment = EXCLUDED.employment,
        hh_employment_id = EXCLUDED.hh_employment_id,
        schedule = EXCLUDED.schedule,
        hh_schedule_id = EXCLUDED.hh_schedule_id,
        is_remote = EXCLUDED.is_remote,
        has_test = EXCLUDED.has_test,
        is_archived = EXCLUDED.is_archived,
        salary_min = EXCLUDED.salary_min,
        salary_max = EXCLUDED.salary_max,
        salary_mid = EXCLUDED.salary_mid,
        salary_gross = EXCLUDED.salary_gross,
        currency = EXCLUDED.currency,
        address_city = EXCLUDED.address_city,
        lat = EXCLUDED.lat,
        lng = EXCLUDED.lng,
        snippet_requirement = EXCLUDED.snippet_requirement,
        snippet_responsibility = EXCLUDED.snippet_responsibility,
        description = EXCLUDED.description,
        published_at = EXCLUDED.published_at,
        vacancy_url = EXCLUDED.vacancy_url,
        api_url = EXCLUDED.api_url,
        raw_json = EXCLUDED.raw_json,
        updated_at = NOW()
    RETURNING id;
    """
    with conn.cursor() as cur:
        cur.execute(sql, payload)
        return int(cur.fetchone()[0])


def upsert_salary_features(conn: PgConnection, vacancy_id: int, vacancy: dict) -> None:
    sql = """
    INSERT INTO vacancy_salary_features (
        vacancy_id, salary_from, salary_to, salary_mid, currency, gross, updated_at
    )
    VALUES (%s, %s, %s, %s, %s, %s, NOW())
    ON CONFLICT (vacancy_id) DO UPDATE
    SET
        salary_from = EXCLUDED.salary_from,
        salary_to = EXCLUDED.salary_to,
        salary_mid = EXCLUDED.salary_mid,
        currency = EXCLUDED.currency,
        gross = EXCLUDED.gross,
        updated_at = NOW();
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                vacancy_id,
                vacancy.get("salary_min"),
                vacancy.get("salary_max"),
                vacancy.get("salary_mid"),
                vacancy.get("currency"),
                vacancy.get("salary_gross"),
            ),
        )


def upsert_skill(conn: PgConnection, skill_name: str) -> int:
    sql = """
    INSERT INTO skills (name)
    VALUES (%s)
    ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
    RETURNING id;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (skill_name,))
        return int(cur.fetchone()[0])


def link_vacancy_skill(conn: PgConnection, vacancy_id: int, skill_id: int) -> None:
    sql = """
    INSERT INTO vacancy_skills (vacancy_id, skill_id)
    VALUES (%s, %s)
    ON CONFLICT DO NOTHING;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (vacancy_id, skill_id))


def save_vacancy_with_skills(conn: PgConnection, vacancy: dict, skills: list[str]) -> int:
    vacancy_id = upsert_vacancy(conn, vacancy)
    upsert_salary_features(conn, vacancy_id, vacancy)
    for skill in skills:
        skill_id = upsert_skill(conn, skill)
        link_vacancy_skill(conn, vacancy_id, skill_id)
    return vacancy_id

