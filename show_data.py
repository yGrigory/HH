from parser.config import get_settings
from parser.db import connection_scope
from psycopg2 import OperationalError


def main() -> None:
    settings = get_settings()
    with connection_scope(settings) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM vacancies;")
            vacancies_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM skills;")
            skills_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM vacancy_skills;")
            links_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM vacancy_salary_features;")
            salary_features_count = cur.fetchone()[0]

            cur.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE salary_min IS NOT NULL OR salary_max IS NOT NULL),
                    COUNT(*) FILTER (WHERE description IS NOT NULL AND description <> '')
                FROM vacancies;
                """
            )
            with_salary_count, with_description_count = cur.fetchone()

            cur.execute(
                """
                SELECT
                    MIN(published_at),
                    MAX(published_at)
                FROM vacancies;
                """
            )
            published_min, published_max = cur.fetchone()

            print("=== DB OVERVIEW ===")
            print(f"vacancies: {vacancies_count}")
            print(f"skills: {skills_count}")
            print(f"vacancy_skill_links: {links_count}")
            print(f"vacancy_salary_features: {salary_features_count}")
            print(f"vacancies_with_salary: {with_salary_count}")
            print(f"vacancies_with_description: {with_description_count}")
            print(f"published_at_range: {published_min} .. {published_max}")

            cur.execute(
                """
                SELECT id, started_at, finished_at, status, query, vacancies_seen, vacancies_saved, vacancies_failed
                FROM parse_runs
                ORDER BY id DESC
                LIMIT 3;
                """
            )
            print("\nLast parse runs:")
            for row in cur.fetchall():
                print(row)

            print("\nLast 10 vacancies:")
            cur.execute(
                """
                SELECT id, hh_id, title, area, employer, salary_min, salary_max, currency
                FROM vacancies
                ORDER BY id DESC
                LIMIT 10;
                """
            )
            for row in cur.fetchall():
                print(row)

            print("\nTop 20 skills:")
            cur.execute(
                """
                SELECT s.name, COUNT(*) AS cnt
                FROM vacancy_skills vs
                JOIN skills s ON s.id = vs.skill_id
                GROUP BY s.name
                ORDER BY cnt DESC, s.name ASC
                LIMIT 20;
                """
            )
            for row in cur.fetchall():
                print(row)

            print("\n=== FULL VACANCY SAMPLES (5) ===")
            cur.execute(
                """
                SELECT
                    v.id,
                    v.hh_id,
                    v.title,
                    v.area,
                    v.employer,
                    v.grade,
                    v.experience,
                    v.employment,
                    v.schedule,
                    v.salary_min,
                    v.salary_max,
                    v.currency,
                    v.published_at,
                    v.vacancy_url,
                    v.snippet_requirement,
                    v.snippet_responsibility,
                    v.description,
                    COALESCE(
                        STRING_AGG(DISTINCT s.name, ', ' ORDER BY s.name),
                        ''
                    ) AS skills,
                    ve.hard_skills_norm,
                    ve.level_hints,
                    ve.english_level,
                    ve.responsibility_tags,
                    ve.benefit_tags,
                    ve.description_len,
                    ve.requirements_len
                FROM vacancies v
                LEFT JOIN vacancy_skills vs ON vs.vacancy_id = v.id
                LEFT JOIN skills s ON s.id = vs.skill_id
                LEFT JOIN vacancy_enrichment ve ON ve.vacancy_id = v.id
                GROUP BY
                    v.id, v.hh_id, v.title, v.area, v.employer, v.grade,
                    v.experience, v.employment, v.schedule, v.salary_min,
                    v.salary_max, v.currency, v.published_at, v.vacancy_url,
                    v.snippet_requirement, v.snippet_responsibility, v.description,
                    ve.hard_skills_norm, ve.level_hints, ve.english_level,
                    ve.responsibility_tags, ve.benefit_tags, ve.description_len, ve.requirements_len
                ORDER BY v.published_at DESC NULLS LAST, v.id DESC
                LIMIT 5;
                """
            )
            rows = cur.fetchall()
            for row in rows:
                (
                    v_id,
                    hh_id,
                    title,
                    area,
                    employer,
                    grade,
                    experience,
                    employment,
                    schedule,
                    salary_min,
                    salary_max,
                    currency,
                    published_at,
                    vacancy_url,
                    snippet_requirement,
                    snippet_responsibility,
                    description,
                    skills_csv,
                    hard_skills_norm,
                    level_hints,
                    english_level,
                    responsibility_tags,
                    benefit_tags,
                    description_len,
                    requirements_len,
                ) = row
                print("\n------------------------------")
                print(f"id: {v_id}, hh_id: {hh_id}")
                print(f"title: {title}")
                print(f"area: {area}")
                print(f"employer: {employer}")
                print(f"grade: {grade}, experience: {experience}")
                print(f"employment: {employment}, schedule: {schedule}")
                print(f"salary: {salary_min} .. {salary_max} {currency}")
                print(f"published_at: {published_at}")
                print(f"url: {vacancy_url}")
                print(f"skills: {skills_csv if skills_csv else '(no skills)'}")
                req = (snippet_requirement or "").strip()
                resp = (snippet_responsibility or "").strip()
                desc = (description or "").strip()
                print(f"snippet_requirement: {req[:220] if req else '(empty)'}")
                print(f"snippet_responsibility: {resp[:220] if resp else '(empty)'}")
                print(f"description_preview: {desc[:220] if desc else '(empty)'}")
                print(
                    "enrichment: "
                    f"hard_skills_norm={hard_skills_norm or []}, "
                    f"level_hints={level_hints or []}, "
                    f"english_level={english_level}, "
                    f"responsibility_tags={responsibility_tags or []}, "
                    f"benefit_tags={benefit_tags or []}, "
                    f"description_len={description_len}, "
                    f"requirements_len={requirements_len}"
                )


if __name__ == "__main__":
    try:
        main()
    except OperationalError as exc:
        raise SystemExit(
            "Database connection failed while reading data.\n"
            "Check internet/VPN, DB host allowlist, and DB_PASSWORD in .env.\n"
            f"Details: {exc}"
        )
