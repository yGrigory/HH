import requests
import time

BASE_URL = "https://api.hh.ru/vacancies"
VACANCY_URL = "https://api.hh.ru/vacancies/{}"

HEADERS = {
    "User-Agent": "HHParserBot/1.0"
}


def fetch_vacancies(text, area=113, page=0, per_page=20):
    params = {
        "text": text,
        "area": area,
        "page": page,
        "per_page": per_page,
        "only_with_salary": True
    }

    response = requests.get(BASE_URL, headers=HEADERS, params=params)
    response.raise_for_status()
    return response.json()


def fetch_vacancy_details(vacancy_id):
    response = requests.get(
        VACANCY_URL.format(vacancy_id),
        headers=HEADERS
    )
    response.raise_for_status()

    time.sleep(1)
    return response.json()


def build_full_vacancy(list_item, details):

    salary = list_item["salary"]

    return {
        "hh_id": int(list_item["id"]),
        "title": list_item["name"],
        "area": list_item["area"]["name"],

        "salary_min": salary.get("from"),
        "salary_max": salary.get("to"),
        "currency": salary.get("currency"),

        "experience": details["experience"]["name"] if details.get("experience") else None,
        "schedule": details["schedule"]["name"] if details.get("schedule") else None,
        "employment": details["employment"]["name"] if details.get("employment") else None,

        "skills": [s["name"] for s in details.get("key_skills", [])],

        "published_at": list_item["published_at"][:10],
        "url": list_item["alternate_url"]
    }