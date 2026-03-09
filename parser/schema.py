from __future__ import annotations

from psycopg2.extensions import connection as PgConnection


DDL = """
CREATE TABLE IF NOT EXISTS vacancies (
    id BIGSERIAL PRIMARY KEY,
    hh_id BIGINT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    profession_query TEXT,
    professional_role_id INTEGER,
    professional_role_name TEXT,
    area TEXT,
    employer TEXT,
    employer_hh_id BIGINT,
    employer_url TEXT,
    experience TEXT,
    hh_experience_id TEXT,
    grade TEXT,
    employment TEXT,
    hh_employment_id TEXT,
    schedule TEXT,
    hh_schedule_id TEXT,
    is_remote BOOLEAN,
    has_test BOOLEAN,
    is_archived BOOLEAN,
    salary_min INTEGER,
    salary_max INTEGER,
    salary_mid NUMERIC(12, 2),
    salary_gross BOOLEAN,
    currency TEXT,
    address_city TEXT,
    lat NUMERIC(10, 6),
    lng NUMERIC(10, 6),
    snippet_requirement TEXT,
    snippet_responsibility TEXT,
    description TEXT,
    published_at TIMESTAMPTZ,
    vacancy_url TEXT,
    api_url TEXT,
    raw_json JSONB,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS skills (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS vacancy_skills (
    vacancy_id BIGINT NOT NULL REFERENCES vacancies(id) ON DELETE CASCADE,
    skill_id BIGINT NOT NULL REFERENCES skills(id) ON DELETE CASCADE,
    PRIMARY KEY (vacancy_id, skill_id)
);

CREATE TABLE IF NOT EXISTS vacancy_salary_features (
    vacancy_id BIGINT PRIMARY KEY REFERENCES vacancies(id) ON DELETE CASCADE,
    salary_from INTEGER,
    salary_to INTEGER,
    salary_mid NUMERIC(12, 2),
    currency TEXT,
    gross BOOLEAN,
    salary_from_rub NUMERIC(14, 2),
    salary_to_rub NUMERIC(14, 2),
    salary_mid_rub NUMERIC(14, 2),
    fx_rate NUMERIC(14, 6),
    fx_date DATE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS vacancy_enrichment (
    vacancy_id BIGINT PRIMARY KEY REFERENCES vacancies(id) ON DELETE CASCADE,
    hard_skills_norm TEXT[],
    level_hints TEXT[],
    english_level TEXT,
    responsibility_tags TEXT[],
    benefit_tags TEXT[],
    description_len INTEGER,
    requirements_len INTEGER,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS parse_runs (
    id BIGSERIAL PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    query TEXT,
    area INTEGER,
    pages INTEGER,
    per_page INTEGER,
    only_with_salary BOOLEAN,
    vacancies_seen INTEGER NOT NULL DEFAULT 0,
    vacancies_saved INTEGER NOT NULL DEFAULT 0,
    vacancies_failed INTEGER NOT NULL DEFAULT 0,
    error_sample TEXT
);

CREATE INDEX IF NOT EXISTS idx_vacancies_title ON vacancies(title);
CREATE INDEX IF NOT EXISTS idx_vacancies_grade ON vacancies(grade);
CREATE INDEX IF NOT EXISTS idx_vacancies_published_at ON vacancies(published_at);
CREATE INDEX IF NOT EXISTS idx_vacancies_query ON vacancies(profession_query);
CREATE INDEX IF NOT EXISTS idx_vacancies_role_id ON vacancies(professional_role_id);
CREATE INDEX IF NOT EXISTS idx_vacancies_hh_exp ON vacancies(hh_experience_id);
CREATE INDEX IF NOT EXISTS idx_vacancies_area ON vacancies(area);
CREATE INDEX IF NOT EXISTS idx_parse_runs_started_at ON parse_runs(started_at);
CREATE INDEX IF NOT EXISTS idx_vacancy_enrichment_eng ON vacancy_enrichment(english_level);
CREATE INDEX IF NOT EXISTS idx_vacancy_enrichment_skills ON vacancy_enrichment USING GIN(hard_skills_norm);
CREATE INDEX IF NOT EXISTS idx_vacancy_enrichment_resp ON vacancy_enrichment USING GIN(responsibility_tags);
CREATE INDEX IF NOT EXISTS idx_vacancy_enrichment_benefits ON vacancy_enrichment USING GIN(benefit_tags);
"""


DROP_DDL = """
DROP TABLE IF EXISTS vacancy_enrichment;
DROP TABLE IF EXISTS vacancy_salary_features;
DROP TABLE IF EXISTS vacancy_skills;
DROP TABLE IF EXISTS skills;
DROP TABLE IF EXISTS parse_runs;
DROP TABLE IF EXISTS vacancies;
"""


def create_schema(conn: PgConnection) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL)


def recreate_schema(conn: PgConnection) -> None:
    with conn.cursor() as cur:
        cur.execute(DROP_DDL)
        cur.execute(DDL)
