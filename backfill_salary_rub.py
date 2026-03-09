from __future__ import annotations

from parser.config import get_settings
from parser.db import connection_scope
from parser.repository import upsert_salary_features


def main() -> None:
    settings = get_settings()
    with connection_scope(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, salary_min, salary_max, salary_mid, currency, salary_gross
                FROM vacancies
                ORDER BY id;
                """
            )
            rows = cur.fetchall()

        total = len(rows)
        for idx, row in enumerate(rows, start=1):
            vacancy_id, salary_min, salary_max, salary_mid, currency, salary_gross = row
            vacancy_payload = {
                "salary_min": salary_min,
                "salary_max": salary_max,
                "salary_mid": salary_mid,
                "currency": currency,
                "salary_gross": salary_gross,
            }
            upsert_salary_features(conn, vacancy_id, vacancy_payload)

            if idx % 500 == 0 or idx == total:
                conn.commit()
                print(f"processed {idx}/{total}")

        print("backfill completed")


if __name__ == "__main__":
    main()
