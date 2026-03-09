from __future__ import annotations

import time
from typing import Any

import requests
from requests import HTTPError, Response

from .config import Settings


class HHClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": settings.hh_user_agent,
                "Accept": "application/json",
            }
        )
        self._last_request_ts = 0.0

    def _throttle(self) -> None:
        min_interval = max(0.0, self._settings.hh_sleep_between_requests_sec)
        if min_interval <= 0:
            return
        elapsed = time.monotonic() - self._last_request_ts
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)

    def _request_json(self, url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        attempts = max(1, self._settings.hh_retry_attempts)
        backoff = max(0.0, self._settings.hh_retry_backoff_sec)
        last_exc: Exception | None = None

        for attempt in range(1, attempts + 1):
            self._throttle()
            resp: Response | None = None
            try:
                resp = self._session.get(url, params=params, timeout=self._settings.hh_timeout_sec)
                self._last_request_ts = time.monotonic()
                resp.raise_for_status()
                return resp.json()
            except HTTPError as exc:
                self._last_request_ts = time.monotonic()
                last_exc = exc
                status = resp.status_code if resp is not None else None
                if status not in {403, 429, 500, 502, 503, 504} or attempt == attempts:
                    raise
                retry_after = resp.headers.get("Retry-After") if resp is not None else None
                wait_time = backoff * attempt
                if retry_after and retry_after.isdigit():
                    wait_time = max(wait_time, float(retry_after))
                if wait_time > 0:
                    time.sleep(wait_time)
            except requests.RequestException as exc:
                self._last_request_ts = time.monotonic()
                last_exc = exc
                if attempt == attempts:
                    raise
                wait_time = backoff * attempt
                if wait_time > 0:
                    time.sleep(wait_time)

        if last_exc is not None:
            raise last_exc
        raise RuntimeError("HH request failed unexpectedly without exception")

    def search_vacancies(
        self,
        query: str,
        area: int,
        page: int,
        per_page: int,
        only_with_salary: bool,
        date_from: str | None = None,
    ) -> dict[str, Any]:
        params = {
            "text": query,
            "area": area,
            "page": page,
            "per_page": per_page,
            "only_with_salary": only_with_salary,
        }
        if date_from:
            params["date_from"] = date_from
        url = f"{self._settings.hh_base_url}/vacancies"
        return self._request_json(url, params=params)

    def get_vacancy(self, vacancy_id: str | int) -> dict[str, Any]:
        url = f"{self._settings.hh_base_url}/vacancies/{vacancy_id}"
        return self._request_json(url)
