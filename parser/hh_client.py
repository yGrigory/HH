from __future__ import annotations

import os
import time
import re
from typing import Any

import requests
from requests import HTTPError, Response

from .config import Settings


class HHClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._session = requests.Session()
        self._user_agent_fallback_applied = False
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
                if status == 400 and resp is not None and self._maybe_switch_user_agent(resp):
                    continue
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

    def _maybe_switch_user_agent(self, resp: Response) -> bool:
        if self._user_agent_fallback_applied:
            return False
        if not self._is_bad_user_agent_response(resp):
            return False

        fallback_user_agent = os.getenv(
            "HH_FALLBACK_USER_AGENT",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
        )
        self._session.headers["User-Agent"] = fallback_user_agent
        self._user_agent_fallback_applied = True
        print("[HH WARN] User-Agent was rejected by HH API; switched to fallback User-Agent and retrying request.")
        return True

    @staticmethod
    def _is_bad_user_agent_response(resp: Response) -> bool:
        text = (resp.text or "").lower()
        if "bad user-agent" in text or "bad_user_agent" in text or "blacklisted" in text:
            return True
        try:
            payload = resp.json()
        except ValueError:
            return False

        if isinstance(payload, dict):
            description = str(payload.get("description", "")).lower()
            if "bad user-agent" in description:
                return True
            errors = payload.get("errors")
            if isinstance(errors, list):
                for item in errors:
                    if not isinstance(item, dict):
                        continue
                    error_type = str(item.get("type", "")).lower()
                    value = str(item.get("value", "")).lower()
                    if error_type == "bad_user_agent" or value == "blacklisted":
                        return True
        return False

    @staticmethod
    def _normalize_hh_datetime(value: str) -> str:
        normalized = value.strip()
        # HH API expects timezone like +0000 (without colon and without offset seconds).
        normalized = re.sub(r"([+-]\d{2}):(\d{2}):\d{2}$", r"\1\2", normalized)
        normalized = re.sub(r"([+-]\d{2}):(\d{2})$", r"\1\2", normalized)
        return normalized

    def search_vacancies(
        self,
        query: str,
        area: int,
        page: int,
        per_page: int,
        only_with_salary: bool,
        date_from: str | None = None,
        date_to: str | None = None,
        order_by: str | None = None,
    ) -> dict[str, Any]:
        safe_per_page = max(1, min(50, int(per_page)))
        params: dict[str, Any] = {
            "text": query,
            "area": area,
            "page": page,
            "per_page": safe_per_page,
            "search_field": "name",
        }
        # HH API is picky about boolean query params. Send only explicit "true".
        if only_with_salary:
            params["only_with_salary"] = "true"
        if date_from:
            params["date_from"] = self._normalize_hh_datetime(date_from)
        if date_to:
            params["date_to"] = self._normalize_hh_datetime(date_to)
        if order_by:
            params["order_by"] = order_by
        url = f"{self._settings.hh_base_url}/vacancies"
        try:
            return self._request_json(url, params=params)
        except HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code != 400:
                raise

            fallback_params = dict(params)
            removed: list[str] = []

            if "date_from" in fallback_params or "date_to" in fallback_params:
                fallback_params.pop("date_from", None)
                fallback_params.pop("date_to", None)
                removed.extend(["date_from", "date_to"])
                try:
                    print(
                        "[HH WARN] 400 on search_vacancies; retry without date filters "
                        f"query='{query}' page={page}"
                    )
                    return self._request_json(url, params=fallback_params)
                except HTTPError as retry_exc:
                    retry_status = (
                        retry_exc.response.status_code
                        if retry_exc.response is not None
                        else None
                    )
                    if retry_status != 400:
                        raise
                    exc = retry_exc

            if "order_by" in fallback_params:
                fallback_params.pop("order_by", None)
                removed.append("order_by")
                try:
                    print(
                        "[HH WARN] 400 persists; retry without order_by "
                        f"query='{query}' page={page}"
                    )
                    return self._request_json(url, params=fallback_params)
                except HTTPError as retry_exc:
                    retry_status = (
                        retry_exc.response.status_code
                        if retry_exc.response is not None
                        else None
                    )
                    if retry_status != 400:
                        raise
                    exc = retry_exc

            if "search_field" in fallback_params:
                fallback_params.pop("search_field", None)
                removed.append("search_field")
                try:
                    print(
                        "[HH WARN] 400 persists; retry without search_field "
                        f"query='{query}' page={page}"
                    )
                    return self._request_json(url, params=fallback_params)
                except HTTPError as retry_exc:
                    retry_status = (
                        retry_exc.response.status_code
                        if retry_exc.response is not None
                        else None
                    )
                    if retry_status != 400:
                        raise
                    exc = retry_exc

            if int(fallback_params.get("per_page", 0) or 0) > 20:
                fallback_params["per_page"] = 20
                removed.append("per_page->20")
                try:
                    print(
                        "[HH WARN] 400 persists; retry with per_page=20 "
                        f"query='{query}' page={page}"
                    )
                    return self._request_json(url, params=fallback_params)
                except HTTPError as retry_exc:
                    retry_status = (
                        retry_exc.response.status_code
                        if retry_exc.response is not None
                        else None
                    )
                    if retry_status != 400:
                        raise
                    exc = retry_exc

            response_text = ""
            if exc.response is not None:
                response_text = (exc.response.text or "").strip().replace("\n", " ")
                if len(response_text) > 300:
                    response_text = response_text[:300] + "..."
            print(
                "[HH ERROR] 400 for search_vacancies with params "
                f"query='{query}' page={page} removed={removed} response='{response_text}'"
            )
            raise

    def get_vacancy(self, vacancy_id: str | int) -> dict[str, Any]:
        url = f"{self._settings.hh_base_url}/vacancies/{vacancy_id}"
        return self._request_json(url)
