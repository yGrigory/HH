def detect_grade(experience):

    if not experience:
        return "Unknown"

    exp = experience.lower()

    if "нет опыта" in exp:
        return "Junior"

    if "1" in exp or "3" in exp:
        return "Junior/Middle"

    if "3" in exp or "6" in exp:
        return "Middle"

    if "более" in exp:
        return "Senior"

    return "Unknown"


def process_vacancy(vacancy):

    vacancy["skills_count"] = len(vacancy["skills"])
    vacancy["grade"] = detect_grade(vacancy["experience"])

    return vacancy