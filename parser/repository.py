from __future__ import annotations

from datetime import date, datetime
from threading import Lock

import requests
from psycopg2.extensions import connection as PgConnection
from psycopg2.extras import Json

from .enrichment import build_enrichment
from .it_queries import is_valid_skill_query, normalize_skill_query

FX_RATES_URL = "https://www.cbr-xml-daily.ru/daily_json.js"

_fx_lock = Lock()
_fx_cache: dict | None = None


def _fetch_fx_rates() -> tuple[dict[str, float], date]:
    response = requests.get(FX_RATES_URL, timeout=10)
    response.raise_for_status()
    payload = response.json()

    rates = {"RUR": 1.0, "RUB": 1.0}
    for code, meta in (payload.get("Valute") or {}).items():
        nominal = float(meta.get("Nominal") or 0)
        value = float(meta.get("Value") or 0)
        if nominal > 0 and value > 0:
            rates[code.upper()] = value / nominal

    fx_dt_raw = payload.get("Date")
    fx_date = datetime.fromisoformat(fx_dt_raw).date() if fx_dt_raw else date.today()
    return rates, fx_date


def _get_fx_rates_cached() -> tuple[dict[str, float], date | None]:
    global _fx_cache
    today = date.today()
    with _fx_lock:
        if _fx_cache and _fx_cache["fetched_on"] == today:
            return _fx_cache["rates"], _fx_cache["fx_date"]

        try:
            rates, fx_date = _fetch_fx_rates()
            _fx_cache = {"fetched_on": today, "rates": rates, "fx_date": fx_date}
            return rates, fx_date
        except requests.RequestException:
            fallback_rates = {"RUR": 1.0, "RUB": 1.0}
            _fx_cache = {"fetched_on": today, "rates": fallback_rates, "fx_date": None}
            return fallback_rates, None


def _to_rub(amount: int | float | None, currency: str | None, rates: dict[str, float]) -> float | None:
    if amount is None:
        return None
    if not currency:
        return None
    rate = rates.get(currency.upper())
    if rate is None:
        return None
    return round(float(amount) * rate, 2)


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


def get_last_successful_run_finished_at(
    conn: PgConnection,
    query: str,
    area: int,
    only_with_salary: bool,
) -> datetime | None:
    sql = """
    SELECT finished_at
    FROM parse_runs
    WHERE status = 'success'
      AND query = %s
      AND area = %s
      AND only_with_salary = %s
      AND finished_at IS NOT NULL
    ORDER BY finished_at DESC
    LIMIT 1;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (query, area, only_with_salary))
        row = cur.fetchone()
        return row[0] if row else None


def get_existing_vacancy_hh_ids(conn: PgConnection, hh_ids: list[int]) -> set[int]:
    if not hh_ids:
        return set()

    sql = """
    SELECT hh_id
    FROM vacancies
    WHERE hh_id = ANY(%s);
    """
    with conn.cursor() as cur:
        cur.execute(sql, (hh_ids,))
        return {int(row[0]) for row in cur.fetchall()}


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
    rates, fx_date = _get_fx_rates_cached()
    currency = vacancy.get("currency")
    fx_rate = rates.get(currency.upper()) if currency else None
    salary_from = vacancy.get("salary_min")
    salary_to = vacancy.get("salary_max")
    salary_mid = vacancy.get("salary_mid")
    salary_from_rub = _to_rub(salary_from, currency, rates)
    salary_to_rub = _to_rub(salary_to, currency, rates)
    salary_mid_rub = _to_rub(salary_mid, currency, rates)

    sql = """
    INSERT INTO vacancy_salary_features (
        vacancy_id, salary_from, salary_to, salary_mid, currency, gross,
        salary_from_rub, salary_to_rub, salary_mid_rub, fx_rate, fx_date, updated_at
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
    ON CONFLICT (vacancy_id) DO UPDATE
    SET
        salary_from = EXCLUDED.salary_from,
        salary_to = EXCLUDED.salary_to,
        salary_mid = EXCLUDED.salary_mid,
        currency = EXCLUDED.currency,
        gross = EXCLUDED.gross,
        salary_from_rub = EXCLUDED.salary_from_rub,
        salary_to_rub = EXCLUDED.salary_to_rub,
        salary_mid_rub = EXCLUDED.salary_mid_rub,
        fx_rate = EXCLUDED.fx_rate,
        fx_date = EXCLUDED.fx_date,
        updated_at = NOW();
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                vacancy_id,
                salary_from,
                salary_to,
                salary_mid,
                currency,
                vacancy.get("salary_gross"),
                salary_from_rub,
                salary_to_rub,
                salary_mid_rub,
                fx_rate,
                fx_date,
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


def upsert_vacancy_enrichment(
    conn: PgConnection,
    vacancy_id: int,
    vacancy: dict,
    skills: list[str],
) -> None:
    enr = build_enrichment(vacancy, skills)
    sql = """
    INSERT INTO vacancy_enrichment (
        vacancy_id, hard_skills_norm, level_hints, english_level,
        responsibility_tags, benefit_tags, description_len, requirements_len, updated_at
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
    ON CONFLICT (vacancy_id) DO UPDATE
    SET
        hard_skills_norm = EXCLUDED.hard_skills_norm,
        level_hints = EXCLUDED.level_hints,
        english_level = EXCLUDED.english_level,
        responsibility_tags = EXCLUDED.responsibility_tags,
        benefit_tags = EXCLUDED.benefit_tags,
        description_len = EXCLUDED.description_len,
        requirements_len = EXCLUDED.requirements_len,
        updated_at = NOW();
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                vacancy_id,
                enr["hard_skills_norm"],
                enr["level_hints"],
                enr["english_level"],
                enr["responsibility_tags"],
                enr["benefit_tags"],
                enr["description_len"],
                enr["requirements_len"],
            ),
        )


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
    upsert_vacancy_enrichment(conn, vacancy_id, vacancy, skills)
    return vacancy_id


def get_skill_queries(
    conn: PgConnection,
    min_count: int = 1,
    limit: int | None = None,
) -> list[str]:
    sql = """
    SELECT s.name, COUNT(*)::int AS cnt
    FROM vacancy_skills vs
    JOIN skills s ON s.id = vs.skill_id
    GROUP BY s.name
    HAVING COUNT(*) >= %s
    ORDER BY cnt DESC, s.name ASC
    """
    params: list[object] = [max(1, min_count)]
    if limit is not None and limit > 0:
        sql += " LIMIT %s"
        params.append(limit)

    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    queries: list[str] = []
    seen: set[str] = set()
    for skill_name, _ in rows:
        if not skill_name:
            continue
        query = normalize_skill_query(str(skill_name))
        if not is_valid_skill_query(query):
            continue
        key = query.casefold()
        if key in seen:
            continue
        seen.add(key)
        queries.append(query)
    return queries
