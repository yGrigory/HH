from __future__ import annotations

import re
from pathlib import Path


LANGUAGE_ROLE_QUERIES = [
    "Python Developer",
    "Java Developer",
    "Go Developer",
    "Rust Developer",
    "Ruby Developer",
    "PHP Developer",
    "C++ Developer",
    "C# Developer",
    "JavaScript Developer",
    "TypeScript Developer",
    "Scala Developer",
    "Kotlin Developer",
    "Swift Developer",
]

CORE_ENGINEERING_ROLE_QUERIES = [
    "Backend Developer",
    "Frontend Developer",
    "Fullstack Developer",
    "Software Engineer",
    "Software Developer",
    "Web Developer",
    "Application Developer",
]

MOBILE_ROLE_QUERIES = [
    "Mobile Developer",
    "Android Developer",
    "iOS Developer",
    "Flutter Developer",
    "React Native Developer",
    "Kotlin Android Developer",
    "Swift iOS Developer",
]

DATA_ROLE_QUERIES = [
    "Data Engineer",
    "Data Scientist",
    "Machine Learning Engineer",
    "ML Engineer",
    "AI Engineer",
    "Analytics Engineer",
    "BI Developer",
]

INFRA_ROLE_QUERIES = [
    "DevOps Engineer",
    "SRE",
    "Platform Engineer",
    "Cloud Engineer",
    "Infrastructure Engineer",
    "Systems Engineer",
    "DBA",
]

QA_ROLE_QUERIES = [
    "QA Engineer",
    "Automation QA Engineer",
    "Test Engineer",
    "SDET",
]

SECURITY_ROLE_QUERIES = [
    "Security Engineer",
    "Application Security Engineer",
    "DevSecOps Engineer",
    "Cybersecurity Engineer",
]

GAME_ROLE_QUERIES = [
    "Game Developer",
    "Unity Developer",
    "Unreal Engine Developer",
    "C++ Game Developer",
    "C# Game Developer",
    "Gameplay Programmer",
    "Game Engine Developer",
]

EMBEDDED_ROLE_QUERIES = [
    "Embedded Developer",
    "Embedded Engineer",
    "Firmware Engineer",
    "Systems Developer",
    "Low Level Developer",
]

SPECIALIZED_ROLE_QUERIES = [
    ".NET Developer",
    "Node.js Developer",
    "React Developer",
    "Vue Developer",
    "Angular Developer",
    "Django Developer",
    "FastAPI Developer",
    "Spring Developer",
    "Laravel Developer",
    "Salesforce Developer",
    "Blockchain Developer",
    "Rust Blockchain Developer",
]

IT_ROLE_QUERY_GROUPS: dict[str, list[str]] = {
    "language_roles": LANGUAGE_ROLE_QUERIES,
    "core_engineering_roles": CORE_ENGINEERING_ROLE_QUERIES,
    "mobile_roles": MOBILE_ROLE_QUERIES,
    "data_roles": DATA_ROLE_QUERIES,
    "infra_roles": INFRA_ROLE_QUERIES,
    "qa_roles": QA_ROLE_QUERIES,
    "security_roles": SECURITY_ROLE_QUERIES,
    "game_roles": GAME_ROLE_QUERIES,
    "embedded_roles": EMBEDDED_ROLE_QUERIES,
    "specialized_roles": SPECIALIZED_ROLE_QUERIES,
}

_ROLE_SPLIT_RE = re.compile(r"[\s,;/+]+")
_ROLE_WORDS = {
    "developer",
    "engineer",
    "programmer",
    "backend",
    "frontend",
    "fullstack",
    "software",
    "application",
    "mobile",
    "android",
    "ios",
    "qa",
    "test",
    "sdet",
    "security",
    "devops",
    "sre",
    "cloud",
    "platform",
    "infrastructure",
    "systems",
    "firmware",
    "game",
    "gameplay",
    "embedded",
    "blockchain",
    "bi",
    "ml",
    "ai",
    "data",
    "analytics",
    "web",
    ".net",
    "node.js",
    "react",
    "vue",
    "angular",
    "django",
    "fastapi",
    "spring",
    "laravel",
    "salesforce",
}

SKILL_TO_ROLE_QUERIES: dict[str, list[str]] = {
    "python": ["Python Developer"],
    "java": ["Java Developer"],
    "go": ["Go Developer"],
    "golang": ["Go Developer"],
    "rust": ["Rust Developer"],
    "ruby": ["Ruby Developer"],
    "php": ["PHP Developer"],
    "c++": ["C++ Developer"],
    "c#": ["C# Developer"],
    "javascript": ["JavaScript Developer"],
    "typescript": ["TypeScript Developer"],
    "scala": ["Scala Developer"],
    "kotlin": ["Kotlin Developer", "Android Developer"],
    "swift": ["Swift Developer", "iOS Developer"],
    "node.js": ["Node.js Developer"],
    ".net": [".NET Developer"],
}


def normalize_role_query(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().lstrip("\ufeff").lstrip("•").strip())


def is_valid_role_query(value: str) -> bool:
    query = normalize_role_query(value)
    if not query:
        return False
    if len(query) < 3:
        return False
    if query.isdigit():
        return False
    tokens = [token for token in _ROLE_SPLIT_RE.split(query.casefold()) if token]
    return any(token in _ROLE_WORDS for token in tokens)


def normalize_role_queries(items: list[str]) -> list[str]:
    queries: list[str] = []
    seen: set[str] = set()
    for raw in items:
        query = normalize_role_query(raw)
        if not is_valid_role_query(query):
            continue
        key = query.casefold()
        if key in seen:
            continue
        seen.add(key)
        queries.append(query)
    return queries


def build_all_it_role_queries(groups: dict[str, list[str]]) -> list[str]:
    flattened: list[str] = []
    for group_queries in groups.values():
        flattened.extend(group_queries)
    return normalize_role_queries(flattened)


def load_role_queries_from_file(path: Path) -> list[str]:
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8").splitlines()
    return normalize_role_queries(lines)


def _normalize_skill_token(value: str) -> str:
    cleaned = value.strip().casefold().replace(" ", "")
    cleaned = cleaned.replace("golang", "go")
    return cleaned


def load_role_queries_from_skills_file(path: Path) -> list[str]:
    if not path.exists():
        return []
    role_queries: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        token = _normalize_skill_token(line)
        if not token:
            continue
        mapped = SKILL_TO_ROLE_QUERIES.get(token)
        if mapped:
            role_queries.extend(mapped)
    return normalize_role_queries(role_queries)


ALL_IT_ROLE_QUERIES = build_all_it_role_queries(IT_ROLE_QUERY_GROUPS)
DEFAULT_IT_ROLE_QUERIES = ALL_IT_ROLE_QUERIES


def normalize_technology_queries(items: list[str]) -> list[str]:
    """
    Backward-compatible alias:
    old pipeline used technology query naming, but now queries are role-based.
    """
    return normalize_role_queries(items)


def load_technology_queries_from_file(path: Path) -> list[str]:
    """
    Backward-compatible alias:
    if provided file already contains role queries, load them as-is.
    if it is a skills file, role extraction should be done via
    `load_role_queries_from_skills_file` in caller logic.
    """
    return load_role_queries_from_file(path)


# Backward-compatible aliases used by older imports.
DEFAULT_IT_QUERIES = DEFAULT_IT_ROLE_QUERIES
normalize_skill_query = normalize_role_query
is_valid_skill_query = is_valid_role_query
