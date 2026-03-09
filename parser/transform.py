from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def detect_grade(experience_name: str | None) -> str:
    if not experience_name:
        return "Unknown"
    exp = experience_name.lower()
    if "нет опыта" in exp or "no experience" in exp:
        return "Junior"
    if "1" in exp and "3" in exp:
        return "Junior/Middle"
    if "3" in exp and "6" in exp:
        return "Middle/Senior"
    if "более" in exp or "more than" in exp:
        return "Senior"
    return "Unknown"


def normalize_skills(details: dict[str, Any]) -> list[str]:
    names: set[str] = set()
    for item in details.get("key_skills", []):
        name = (item.get("name") or "").strip()
        if name:
            names.add(name)
    return sorted(names)


def _calc_salary_mid(salary_from: int | None, salary_to: int | None) -> float | None:
    if salary_from is not None and salary_to is not None:
        return round((salary_from + salary_to) / 2, 2)
    if salary_from is not None:
        return float(salary_from)
    if salary_to is not None:
        return float(salary_to)
    return None


def build_vacancy(details: dict[str, Any], query: str) -> dict[str, Any]:
    salary = details.get("salary") or {}
    experience = details.get("experience") or {}
    employment = details.get("employment") or {}
    schedule = details.get("schedule") or {}
    employer = details.get("employer") or {}
    professional_roles = details.get("professional_roles") or []
    top_role = professional_roles[0] if professional_roles else {}
    address = details.get("address") or {}
    snippet = details.get("snippet") or {}

    experience_name = experience.get("name")
    salary_min = salary.get("from")
    salary_max = salary.get("to")
    salary_mid = _calc_salary_mid(salary_min, salary_max)

    published_at = _parse_iso_datetime(details.get("published_at"))
    if published_at and not published_at.tzinfo:
        published_at = published_at.replace(tzinfo=timezone.utc)

    return {
        "hh_id": int(details["id"]),
        "title": details.get("name") or "",
        "profession_query": query,
        "professional_role_id": int(top_role["id"]) if top_role.get("id") else None,
        "professional_role_name": top_role.get("name"),
        "area": (details.get("area") or {}).get("name"),
        "employer": employer.get("name"),
        "employer_hh_id": int(employer["id"]) if employer.get("id") else None,
        "employer_url": employer.get("alternate_url"),
        "experience": experience_name,
        "hh_experience_id": experience.get("id"),
        "grade": detect_grade(experience_name),
        "employment": employment.get("name"),
        "hh_employment_id": employment.get("id"),
        "schedule": schedule.get("name"),
        "hh_schedule_id": schedule.get("id"),
        "is_remote": schedule.get("id") == "remote",
        "has_test": details.get("has_test"),
        "is_archived": details.get("archived"),
        "salary_min": salary_min,
        "salary_max": salary_max,
        "salary_mid": salary_mid,
        "salary_gross": salary.get("gross"),
        "currency": salary.get("currency"),
        "address_city": address.get("city"),
        "lat": address.get("lat"),
        "lng": address.get("lng"),
        "snippet_requirement": snippet.get("requirement"),
        "snippet_responsibility": snippet.get("responsibility"),
        "description": details.get("description"),
        "published_at": published_at,
        "vacancy_url": details.get("alternate_url"),
        "api_url": details.get("url"),
        "raw_json": details,
    }

