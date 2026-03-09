import psycopg2

DB_CONFIG = {
    "host": "109.71.245.173",
    "database": "default_db",
    "user": "gen_user",
    "password": "Poi_5479",
    "port": 5432,
    "sslmode": "require"
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def recreate_tables():

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        DROP TABLE IF EXISTS vacancy_skills;
        DROP TABLE IF EXISTS vacancies;

        CREATE TABLE vacancies (
            id SERIAL PRIMARY KEY,
            hh_id BIGINT UNIQUE,
            title TEXT,
            area TEXT,
            grade TEXT,
            employment TEXT,
            schedule TEXT,
            experience TEXT,
            salary_min INTEGER,
            salary_max INTEGER,
            currency TEXT,
            published_at DATE,
            url TEXT
        );

        CREATE TABLE vacancy_skills (
            id SERIAL PRIMARY KEY,
            vacancy_id INTEGER REFERENCES vacancies(id),
            skill TEXT
        );
    """)

    conn.commit()
    cur.close()
    conn.close()

    print("Таблицы пересозданы")


def insert_vacancy(conn, vacancy):

    cur = conn.cursor()

    query = """
        INSERT INTO vacancies (
            hh_id, title, area, grade,
            employment, schedule, experience,
            salary_min, salary_max, currency,
            published_at, url
        )
        VALUES (
            %(hh_id)s, %(title)s, %(area)s, %(grade)s,
            %(employment)s, %(schedule)s, %(experience)s,
            %(salary_min)s, %(salary_max)s, %(currency)s,
            %(published_at)s, %(url)s
        )
        RETURNING id;
    """

    cur.execute(query, vacancy)
    vacancy_id = cur.fetchone()[0]

    conn.commit()
    cur.close()

    return vacancy_id


def insert_skills(conn, vacancy_id, skills):

    cur = conn.cursor()

    query = """
        INSERT INTO vacancy_skills (vacancy_id, skill)
        VALUES (%s, %s)
    """

    for skill in skills:
        cur.execute(query, (vacancy_id, skill))

    conn.commit()
    cur.close()