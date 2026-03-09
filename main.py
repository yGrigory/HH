from api import fetch_vacancies, fetch_vacancy_details, build_full_vacancy
from analytics import process_vacancy
from db import get_connection, recreate_tables, insert_vacancy, insert_skills


def run():

    recreate_tables()

    print("Загружаем вакансии")

    data = fetch_vacancies("data analyst")

    items = data["items"][:10]

    conn = get_connection()

    for item in items:

        details = fetch_vacancy_details(item["id"])

        vacancy = build_full_vacancy(item, details)

        vacancy = process_vacancy(vacancy)

        vacancy_id = insert_vacancy(conn, vacancy)

        insert_skills(conn, vacancy_id, vacancy["skills"])

        print("Добавлена:", vacancy["title"])

    conn.close()


if __name__ == "__main__":
    run()