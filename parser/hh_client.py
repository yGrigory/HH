from __future__ import annotations

import time
from typing import Any

import requests

from .config import Settings


class HHClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": settings.hh_user_agent})

    def search_vacancies(
        self,
        query: str,
        area: int,
        page: int,
        per_page: int,
        only_with_salary: bool,
    ) -> dict[str, Any]:
        params = {
            "text": query,
            "area": area,
            "page": page,
            "per_page": per_page,
            "only_with_salary": only_with_salary,
        }
        url = f"{self._settings.hh_base_url}/vacancies"
        resp = self._session.get(url, params=params, timeout=self._settings.hh_timeout_sec)
        resp.raise_for_status()
        time.sleep(self._settings.hh_sleep_between_requests_sec)
        return resp.json()

    def get_vacancy(self, vacancy_id: str | int) -> dict[str, Any]:
        url = f"{self._settings.hh_base_url}/vacancies/{vacancy_id}"
        resp = self._session.get(url, timeout=self._settings.hh_timeout_sec)
        resp.raise_for_status()
        time.sleep(self._settings.hh_sleep_between_requests_sec)
        return resp.json()

