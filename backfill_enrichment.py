from __future__ import annotations

from parser.config import get_settings
from parser.db import connection_scope
from parser.repository import upsert_vacancy_enrichment
from parser.schema import create_schema


def main() -> None:
    settings = get_settings()
    with connection_scope(settings) as conn:
        create_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    v.id,
                    v.title,
                    v.description,
                    v.snippet_requirement,
                    v.snippet_responsibility,
                    COALESCE(
                        ARRAY_AGG(DISTINCT s.name) FILTER (WHERE s.name IS NOT NULL),
                        ARRAY[]::TEXT[]
                    ) AS skills
                FROM vacancies v
                LEFT JOIN vacancy_skills vs ON vs.vacancy_id = v.id
                LEFT JOIN skills s ON s.id = vs.skill_id
                GROUP BY v.id, v.title, v.description, v.snippet_requirement, v.snippet_responsibility
                ORDER BY v.id;
                """
            )
            rows = cur.fetchall()

        total = len(rows)
        for idx, row in enumerate(rows, start=1):
            vacancy_id, title, description, snippet_requirement, snippet_responsibility, skills = row
            payload = {
                "title": title,
                "description": description,
                "snippet_requirement": snippet_requirement,
                "snippet_responsibility": snippet_responsibility,
            }
            upsert_vacancy_enrichment(conn, vacancy_id, payload, skills or [])
            if idx % 500 == 0 or idx == total:
                conn.commit()
                print(f"processed {idx}/{total}")

        print("enrichment backfill completed")


if __name__ == "__main__":
    main()
