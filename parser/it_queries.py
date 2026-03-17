from __future__ import annotations

import re
from pathlib import Path


ROLE_PATTERNS = [
    re.compile(
        r"\b(developer|engineer|programmer|backend|frontend|front-end|back-end|fullstack|full-stack|software engineer|qa engineer|tester|sdet)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(разработчик|программист|инженер|бэкенд|бекенд|фронтенд|фуллстек|тестировщик)\b",
        re.IGNORECASE,
    ),
]


def _workspace_root() -> Path:
    # C:/Users/<user>/HH/parser/it_queries.py -> C:/Users/<user>
    return Path(__file__).resolve().parents[2]


def _candidate_skills_files() -> list[Path]:
    root = _workspace_root()
    return [
        root / "skills-row-unique.txt",
        root / "skills-raw-unique.txt",
        root / "TGApp" / "skills-row-unique.txt",
        root / "TGApp" / "skills-raw-unique.txt",
        root / "HH" / "skills-row-unique.txt",
        root / "HH" / "skills-raw-unique.txt",
    ]


def _clean_query(value: str) -> str:
    return value.strip().lstrip("\ufeff").lstrip("•").strip()


def _looks_like_role_query(value: str) -> bool:
    return any(pattern.search(value) for pattern in ROLE_PATTERNS)


def normalize_technology_queries(items: list[str]) -> list[str]:
    queries: list[str] = []
    seen: set[str] = set()
    for raw in items:
        query = _clean_query(raw)
        if not query:
            continue
        if _looks_like_role_query(query):
            continue
        key = query.casefold()
        if key in seen:
            continue
        seen.add(key)
        queries.append(query)
    return queries


def load_technology_queries_from_file(path: Path) -> list[str]:
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8").splitlines()
    return normalize_technology_queries(lines)


def load_default_it_queries() -> list[str]:
    for path in _candidate_skills_files():
        queries = load_technology_queries_from_file(path)
        if queries:
            return queries
    return []


DEFAULT_IT_QUERIES = load_default_it_queries()
