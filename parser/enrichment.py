from __future__ import annotations

import html
import re
from collections.abc import Iterable


TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")


def _clean_text(value: str | None) -> str:
    if not value:
        return ""
    text = html.unescape(value)
    text = TAG_RE.sub(" ", text)
    return WS_RE.sub(" ", text).strip()


def _norm_text(value: str | None) -> str:
    return _clean_text(value).casefold()


def _uniq(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        normalized = item.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


SKILL_PATTERNS: dict[str, tuple[re.Pattern[str], ...]] = {
    "python": (re.compile(r"\bpython\b"),),
    "java": (re.compile(r"\bjava\b"),),
    "kotlin": (re.compile(r"\bkotlin\b"),),
    "javascript": (re.compile(r"\bjavascript\b"),),
    "typescript": (re.compile(r"\btypescript\b"),),
    "go": (re.compile(r"\bgolang\b"),),
    "php": (re.compile(r"\bphp\b"),),
    "c#": (re.compile(r"(?<!\w)c#(?!\w)"), re.compile(r"\bcsharp\b")),
    ".net": (re.compile(r"(?<!\w)\.net\b"), re.compile(r"\bdotnet\b"), re.compile(r"\basp\.net\b")),
    "c++": (re.compile(r"(?<!\w)c\+\+(?!\w)"),),
    "sql": (re.compile(r"\bsql\b"), re.compile(r"\bpostgres(?:ql)?\b"), re.compile(r"\bmysql\b")),
    "django": (re.compile(r"\bdjango\b"),),
    "flask": (re.compile(r"\bflask\b"),),
    "fastapi": (re.compile(r"\bfastapi\b"),),
    "spring": (re.compile(r"\bspring\b"), re.compile(r"\bspring boot\b")),
    "react": (re.compile(r"\breact\b"),),
    "vue": (re.compile(r"\bvue(?:\.js)?\b"),),
    "angular": (re.compile(r"\bangular\b"),),
    "node.js": (re.compile(r"\bnode(?:\.js| js)?\b"),),
    "nestjs": (re.compile(r"\bnest(?:\.js| js)?\b"), re.compile(r"\bnestjs\b")),
    "docker": (re.compile(r"\bdocker\b"),),
    "kubernetes": (re.compile(r"\bkubernetes\b"), re.compile(r"\bk8s\b")),
    "terraform": (re.compile(r"\bterraform\b"),),
    "aws": (re.compile(r"\baws\b"), re.compile(r"\bamazon web services\b")),
    "git": (re.compile(r"\bgit\b"),),
    "linux": (re.compile(r"\blinux\b"),),
    "spark": (re.compile(r"\bspark\b"), re.compile(r"\bpyspark\b")),
    "hadoop": (re.compile(r"\bhadoop\b"),),
    "airflow": (re.compile(r"\bairflow\b"),),
    "power bi": (re.compile(r"\bpower bi\b"),),
    "tableau": (re.compile(r"\btableau\b"),),
    "excel": (re.compile(r"\bexcel\b"),),
    "etl": (re.compile(r"\betl\b"),),
    "ml": (re.compile(r"\bml\b"), re.compile(r"\bmachine learning\b")),
    "data science": (re.compile(r"\bdata science\b"), re.compile(r"\bdata scientist\b")),
    "nlp": (re.compile(r"\bnlp\b"), re.compile(r"\bnatural language processing\b")),
    "computer vision": (re.compile(r"\bcomputer vision\b"),),
}

EXPLICIT_SKILL_ALIASES: dict[str, str] = {
    "python": "python",
    "java": "java",
    "kotlin": "kotlin",
    "javascript": "javascript",
    "js": "javascript",
    "typescript": "typescript",
    "ts": "typescript",
    "go": "go",
    "golang": "go",
    "php": "php",
    "c#": "c#",
    "csharp": "c#",
    ".net": ".net",
    "dotnet": ".net",
    "asp.net": ".net",
    "c++": "c++",
    "sql": "sql",
    "postgresql": "sql",
    "postgres": "sql",
    "mysql": "sql",
    "django": "django",
    "flask": "flask",
    "fastapi": "fastapi",
    "spring": "spring",
    "spring boot": "spring",
    "react": "react",
    "vue": "vue",
    "angular": "angular",
    "node.js": "node.js",
    "node js": "node.js",
    "nestjs": "nestjs",
    "docker": "docker",
    "kubernetes": "kubernetes",
    "k8s": "kubernetes",
    "terraform": "terraform",
    "aws": "aws",
    "git": "git",
    "linux": "linux",
    "spark": "spark",
    "pyspark": "spark",
    "hadoop": "hadoop",
    "airflow": "airflow",
    "power bi": "power bi",
    "tableau": "tableau",
    "excel": "excel",
    "etl": "etl",
    "ml": "ml",
    "machine learning": "ml",
    "data science": "data science",
    "data scientist": "data science",
    "nlp": "nlp",
    "computer vision": "computer vision",
}

LEVEL_HINT_PATTERNS: dict[str, tuple[re.Pattern[str], ...]] = {
    "intern": (
        re.compile(r"\bintern\b"),
        re.compile(r"\b\u0441\u0442\u0430\u0436[\u0435\u0451]\u0440\b"),
    ),
    "junior": (
        re.compile(r"\bjunior\b"),
        re.compile(r"\b\u043c\u043b\u0430\u0434\u0448"),
    ),
    "middle": (re.compile(r"\bmiddle\b"), re.compile(r"\bmid\b")),
    "senior": (
        re.compile(r"\bsenior\b"),
        re.compile(r"\b\u0441\u0442\u0430\u0440\u0448"),
    ),
    "lead": (re.compile(r"\blead\b"), re.compile(r"\bteam lead\b"), re.compile(r"\btech lead\b")),
}

ENGLISH_LEVEL_PATTERNS: tuple[tuple[str, tuple[re.Pattern[str], ...]], ...] = (
    ("c1", (re.compile(r"\bc1\b"), re.compile(r"\badvanced\b"))),
    ("b2", (re.compile(r"\bb2\b"), re.compile(r"\bupper[- ]intermediate\b"))),
    ("b1", (re.compile(r"\bb1\b"), re.compile(r"\bintermediate\b"))),
    ("a2", (re.compile(r"\ba2\b"), re.compile(r"\bpre[- ]intermediate\b"))),
)

RESPONSIBILITY_PATTERNS: dict[str, tuple[re.Pattern[str], ...]] = {
    "backend": (
        re.compile(r"\bbackend\b"),
        re.compile(r"\b\u0431\u044d?\u043a\u0435\u043d\u0434\b"),
    ),
    "frontend": (
        re.compile(r"\bfront[- ]end\b"),
        re.compile(r"\bfrontend\b"),
        re.compile(r"\b\u0444\u0440\u043e\u043d\u0442\u0435\u043d\u0434\b"),
    ),
    "mobile": (re.compile(r"\bmobile\b"), re.compile(r"\bandroid\b"), re.compile(r"\bios\b")),
    "analytics": (
        re.compile(r"\banalytics?\b"),
        re.compile(r"\b\u0430\u043d\u0430\u043b\u0438\u0442"),
        re.compile(r"\bbi\b"),
    ),
    "data_engineering": (
        re.compile(r"\bdata engineer(?:ing)?\b"),
        re.compile(r"\betl\b"),
        re.compile(r"\bdata pipeline"),
        re.compile(r"\bdata warehouse\b"),
    ),
    "ml": (re.compile(r"\bml\b"), re.compile(r"\bmachine learning\b"), re.compile(r"\bdata scientist\b")),
    "qa": (re.compile(r"\bqa\b"), re.compile(r"\btesting\b"), re.compile(r"\b\u0442\u0435\u0441\u0442\u0438\u0440")),
    "devops": (re.compile(r"\bdevops\b"), re.compile(r"\bsre\b"), re.compile(r"\bplatform engineer\b")),
    "management": (re.compile(r"\bproduct manager\b"), re.compile(r"\bproject manager\b"), re.compile(r"\bteam lead\b")),
}

BENEFIT_PATTERNS: dict[str, tuple[re.Pattern[str], ...]] = {
    "remote": (re.compile(r"\bremote\b"), re.compile(r"\b\u0443\u0434\u0430\u043b\u0435\u043d")),
    "hybrid": (re.compile(r"\bhybrid\b"), re.compile(r"\b\u0433\u0438\u0431\u0440\u0438\u0434")),
    "dms": (
        re.compile(r"\b\u0434\u043c\u0441\b"),
        re.compile(r"\bmedical insurance\b"),
        re.compile(r"\bhealth insurance\b"),
    ),
    "bonus": (re.compile(r"\bbonus"), re.compile(r"\b\u0431\u043e\u043d\u0443\u0441"), re.compile(r"\bpremium\b")),
    "equipment": (
        re.compile(r"\bequipment\b"),
        re.compile(r"\b\u043d\u043e\u0443\u0442\u0431\u0443\u043a\b"),
        re.compile(r"\blaptop\b"),
    ),
    "visa": (re.compile(r"\bvisa\b"), re.compile(r"\brelocation\b"), re.compile(r"\b\u0440\u0435\u043b\u043e\u043a\u0430\u0446")),
}


def _extract_from_patterns(
    text: str,
    patterns: dict[str, tuple[re.Pattern[str], ...]],
) -> list[str]:
    matched: list[str] = []
    for label, pattern_list in patterns.items():
        if any(pattern.search(text) for pattern in pattern_list):
            matched.append(label)
    return _uniq(sorted(matched))


def _extract_explicit_skills(skills: list[str]) -> list[str]:
    normalized: list[str] = []
    for skill in skills:
        key = _norm_text(skill)
        if not key:
            continue
        alias = EXPLICIT_SKILL_ALIASES.get(key)
        normalized.append(alias or _clean_text(skill))
    return _uniq(sorted(normalized))


def _extract_hard_skills(text: str, explicit_skills: list[str]) -> list[str]:
    found = list(explicit_skills)
    for skill, patterns in SKILL_PATTERNS.items():
        if any(pattern.search(text) for pattern in patterns):
            found.append(skill)
    return _uniq(sorted(found))


def _extract_english_level(text: str) -> str | None:
    if not re.search(r"\benglish\b|\b\u0430\u043d\u0433\u043b\u0438\u0439", text):
        return None
    for level, patterns in ENGLISH_LEVEL_PATTERNS:
        if any(pattern.search(text) for pattern in patterns):
            return level
    return "required"


def build_enrichment(vacancy: dict, skills: list[str]) -> dict[str, object]:
    title = _clean_text(vacancy.get("title"))
    requirement = _clean_text(vacancy.get("snippet_requirement"))
    responsibility = _clean_text(vacancy.get("snippet_responsibility"))
    description = _clean_text(vacancy.get("description"))

    searchable_text = _norm_text(
        " ".join(part for part in (title, requirement, responsibility, description) if part)
    )
    explicit_skills = _extract_explicit_skills(skills)

    return {
        "hard_skills_norm": _extract_hard_skills(searchable_text, explicit_skills),
        "level_hints": _extract_from_patterns(searchable_text, LEVEL_HINT_PATTERNS),
        "english_level": _extract_english_level(searchable_text),
        "responsibility_tags": _extract_from_patterns(searchable_text, RESPONSIBILITY_PATTERNS),
        "benefit_tags": _extract_from_patterns(searchable_text, BENEFIT_PATTERNS),
        "description_len": len(description),
        "requirements_len": len(requirement),
    }
