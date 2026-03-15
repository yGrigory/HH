from __future__ import annotations

import re


_ROLE_WORDS = {
    "developer",
    "engineer",
    "programmer",
    "backend",
    "frontend",
    "fullstack",
    "разработчик",
    "инженер",
    "программист",
    "бэкенд",
    "фронтенд",
}


def normalize_skill_query(value: str) -> str:
    normalized = re.sub(r"\s+", " ", value.strip())
    return normalized


def is_valid_skill_query(value: str) -> bool:
    if not value:
        return False
    if len(value) < 2:
        return False
    if value.isdigit():
        return False

    lowered = value.casefold()
    tokens = {token for token in re.split(r"[\s/,+\-_.]+", lowered) if token}
    if tokens & _ROLE_WORDS:
        return False
    return True

