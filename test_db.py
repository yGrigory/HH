from db import get_connection


def show_vacancies(conn):

    cur = conn.cursor()

    cur.execute("""
        SELECT
            id,
            hh_id,
            title,
            area,
            grade,
            salary_min,
            salary_max
        FROM vacancies
    """)

    rows = cur.fetchall()

    print("\nВАКАНСИИ\n")

    for row in rows:
        print(row)

    cur.close()


def show_vacancies_with_skills(conn):

    cur = conn.cursor()

    cur.execute("""
        SELECT
            v.title,
            v.grade,
            v.salary_min,
            v.salary_max,
            s.skill
        FROM vacancies v
        LEFT JOIN vacancy_skills s
        ON v.id = s.vacancy_id
    """)

    rows = cur.fetchall()

    print("\nВАКАНСИИ + SKILLS\n")

    for row in rows:
        print(row)

    cur.close()


def count_vacancies(conn):

    cur = conn.cursor()

    cur.execute("""
        SELECT COUNT(*) FROM vacancies
    """)

    count = cur.fetchone()[0]

    print("\nКОЛИЧЕСТВО ВАКАНСИЙ:", count)

    cur.close()


def top_skills(conn):

    cur = conn.cursor()

    cur.execute("""
        SELECT
            skill,
            COUNT(*) as count
        FROM vacancy_skills
        GROUP BY skill
        ORDER BY count DESC
        LIMIT 10
    """)

    rows = cur.fetchall()

    print("\nТОП НАВЫКОВ\n")

    for row in rows:
        print(row)

    cur.close()


def run_test():

    conn = get_connection()

    show_vacancies(conn)

    show_vacancies_with_skills(conn)

    count_vacancies(conn)

    top_skills(conn)

    conn.close()




if __name__ == "__main__":
    run_test()