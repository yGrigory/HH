from __future__ import annotations


IT_QUERY_GROUPS: dict[str, list[str]] = {
    "backend_python": [
        "python developer",
        "python разработчик",
        "python backend developer",
        "python engineer",
        "django developer",
        "flask developer",
        "fastapi developer",
        "backend developer python",
    ],
    "backend_general": [
        "backend developer",
        "backend разработчик",
        "бекенд разработчик",
        "software engineer backend",
        "java developer",
        "java разработчик",
        "spring developer",
        "kotlin developer",
        "golang developer",
        "go developer",
        "go разработчик",
        "php developer",
        "php разработчик",
        "laravel developer",
        "node.js developer",
        "node js developer",
        "node.js разработчик",
        "nestjs developer",
        ".net developer",
        "c# developer",
        "c++ developer",
        "rust developer",
    ],
    "frontend": [
        "frontend developer",
        "frontend разработчик",
        "front-end developer",
        "javascript developer",
        "typescript developer",
        "react developer",
        "react разработчик",
        "vue developer",
        "angular developer",
        "web developer",
    ],
    "fullstack_mobile": [
        "fullstack developer",
        "fullstack разработчик",
        "mobile developer",
        "android developer",
        "android разработчик",
        "ios developer",
        "ios разработчик",
        "react native developer",
        "flutter developer",
    ],
    "data_analytics": [
        "data analyst",
        "аналитик данных",
        "product analyst",
        "product analytics",
        "business analyst it",
        "бизнес-аналитик it",
        "system analyst",
        "системный аналитик",
        "sql developer",
        "bi analyst",
        "power bi",
        "tableau",
        "data engineer",
        "инженер данных",
        "etl developer",
        "data platform engineer",
    ],
    "ml_ai": [
        "data scientist",
        "machine learning engineer",
        "ml engineer",
        "mle",
        "инженер машинного обучения",
        "deep learning engineer",
        "nlp engineer",
        "computer vision engineer",
        "ai engineer",
    ],
    "qa": [
        "qa engineer",
        "qa automation engineer",
        "manual qa",
        "test engineer",
        "sdet",
        "тестировщик",
        "инженер по тестированию",
        "автотестировщик",
    ],
    "infra_security": [
        "devops engineer",
        "devops",
        "site reliability engineer",
        "sre",
        "platform engineer",
        "cloud engineer",
        "kubernetes engineer",
        "docker engineer",
        "terraform engineer",
        "database administrator",
        "dba",
        "postgresql dba",
        "security engineer",
        "application security engineer",
        "penetration tester",
        "information security engineer",
        "специалист по информационной безопасности",
    ],
    "management_architecture": [
        "product manager it",
        "project manager it",
        "scrum master",
        "tech lead",
        "team lead",
        "solution architect",
        "software architect",
        "системный архитектор",
        "1c developer",
        "1с разработчик",
        "1с программист",
    ],
}


def build_default_it_queries() -> list[str]:
    queries: list[str] = []
    seen: set[str] = set()

    for group_queries in IT_QUERY_GROUPS.values():
        for query in group_queries:
            normalized = query.strip()
            if not normalized:
                continue
            key = normalized.casefold()
            if key in seen:
                continue
            seen.add(key)
            queries.append(normalized)

    return queries


DEFAULT_IT_QUERIES = build_default_it_queries()
