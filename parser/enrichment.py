from __future__ import annotations

import re
from html import unescape


_SKILL_PATTERNS: dict[str, tuple[str, ...]] = {
    "Python": ("python",),
    "Java": (" java ", " spring ", "kotlin"),
    "JavaScript": ("javascript", " js ", "ecmascript"),
    "TypeScript": ("typescript", " ts "),
    "SQL": (" sql ", "postgresql", "mysql", "clickhouse"),
    "PostgreSQL": ("postgresql", "postgres "),
    "MySQL": ("mysql",),
    "ClickHouse": ("clickhouse",),
    "Redis": ("redis",),
    "MongoDB": ("mongodb", "mongo "),
    "Docker": ("docker",),
    "Kubernetes": ("kubernetes", "k8s"),
    "Linux": ("linux",),
    "Git": (" git ", "github", "gitlab", "bitbucket"),
    "REST API": ("rest api", "restful", "http api"),
    "GraphQL": ("graphql",),
    "FastAPI": ("fastapi",),
    "Django": ("django",),
    "Flask": ("flask",),
    "Pandas": ("pandas",),
    "NumPy": ("numpy",),
    "Airflow": ("airflow",),
    "Spark": ("spark", "pyspark"),
    "1C": ("1с", "1c"),
    "C#": ("c#", ".net", "asp.net", "dotnet"),
    "C++": ("c++",),
    "Go": (" golang", " go "),
    "PHP": (" php ", "laravel", "symfony"),
    "Node.js": ("node.js", "nodejs", "node "),
    "React": ("react", "next.js", "nextjs"),
    "Vue": ("vue", "nuxt"),
    "Angular": ("angular",),
}

_RESPONSIBILITY_PATTERNS: dict[str, tuple[str, ...]] = {
    "backend": ("backend", "бекенд", "server-side", "api"),
    "frontend": ("frontend", "фронтенд", "ui", "spa"),
    "fullstack": ("fullstack", "full stack", "full-stack"),
    "data": ("data engineer", "аналитик", "bi", "ml", "machine learning"),
    "devops": ("devops", "sre", "ci/cd", "инфраструктур"),
    "management": ("team lead", "руковод", "manage", "lead"),
    "qa": ("qa", "тестир", "автотест"),
    "mobile": ("android", "ios", "react native", "flutter"),
}

_BENEFIT_PATTERNS: dict[str, tuple[str, ...]] = {
    "remote": ("удален", "remote"),
    "hybrid": ("гибрид", "hybrid"),
    "dms": ("дмс", "медицин", "health insurance"),
    "education": ("обучен", "курсы", "сертифик", "конференц"),
    "bonus": ("преми", "бонус", "kpi"),
    "equipment": ("ноутбук", "техника", "equipment"),
    "relocation": ("релокац", "relocation"),
    "visa": ("visa", "виза"),
}


def _strip_html(text: str) -> str:
    plain = re.sub(r"<[^>]+>", " ", text)
    plain = unescape(plain)
    plain = re.sub(r"\s+", " ", plain)
    return plain.strip()


def _normalize_text_parts(vacancy: dict) -> tuple[str, str]:
    title = vacancy.get("title") or ""
    description = vacancy.get("description") or ""
    requirement = vacancy.get("snippet_requirement") or ""
    responsibility = vacancy.get("snippet_responsibility") or ""
    text = " ".join([title, description, requirement, responsibility]).lower()
    clean = f" {_strip_html(text)} "
    req_clean = _strip_html(requirement.lower())
    return clean, req_clean


def _detect_english_level(text: str) -> str | None:
    if re.search(r"\bc1\b|\bc2\b|advanced|upper-?intermediate", text):
        return "C1+"
    if re.search(r"\bb2\b|intermediate", text):
        return "B2"
    if re.search(r"\bb1\b|pre-?intermediate", text):
        return "B1"
    if re.search(r"\ba2\b|elementary", text):
        return "A2"
    if "англий" in text or "english" in text:
        return "required_unspecified"
    return None


def _collect_tags(text: str, patterns: dict[str, tuple[str, ...]]) -> list[str]:
    tags: list[str] = []
    for tag, variants in patterns.items():
        if any(variant in text for variant in variants):
            tags.append(tag)
    return sorted(tags)


def _detect_level_hints(text: str) -> list[str]:
    hints: set[str] = set()
    if any(marker in text for marker in ("intern", "стажер", "стажёр", "trainee")):
        hints.add("intern")
    if any(marker in text for marker in ("junior", "джуниор", "младш")):
        hints.add("junior")
    if any(marker in text for marker in ("middle", "мидл", "средн")):
        hints.add("middle")
    if any(marker in text for marker in ("senior", "сеньор", "старш")):
        hints.add("senior")
    if any(marker in text for marker in ("lead", "тимлид", "team lead", "руковод")):
        hints.add("lead")
    return sorted(hints)


def _normalize_skills(text: str, skills: list[str]) -> list[str]:
    skill_set: set[str] = set()
    for canonical, variants in _SKILL_PATTERNS.items():
        if any(variant in text for variant in variants):
            skill_set.add(canonical)

    for raw_skill in skills:
        name = raw_skill.strip()
        if not name:
            continue
        lowered = f" {name.lower()} "
        matched = False
        for canonical, variants in _SKILL_PATTERNS.items():
            if any(variant in lowered for variant in variants):
                skill_set.add(canonical)
                matched = True
                break
        if not matched:
            skill_set.add(name)
    return sorted(skill_set)


def build_enrichment(vacancy: dict, skills: list[str] | None = None) -> dict:
    skill_names = skills or []
    full_text, requirement_text = _normalize_text_parts(vacancy)

    return {
        "hard_skills_norm": _normalize_skills(full_text, skill_names),
        "level_hints": _detect_level_hints(full_text),
        "english_level": _detect_english_level(full_text),
        "responsibility_tags": _collect_tags(full_text, _RESPONSIBILITY_PATTERNS),
        "benefit_tags": _collect_tags(full_text, _BENEFIT_PATTERNS),
        "description_len": len(_strip_html(vacancy.get("description") or "")),
        "requirements_len": len(requirement_text),
    }
