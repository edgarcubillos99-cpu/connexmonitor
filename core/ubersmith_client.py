import json
import logging
import os

import requests
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)


class UbersmithAPIError(RuntimeError):
    """Error de negocio o de transporte al consultar Ubersmith."""


class UbersmithClient:
    DEFAULT_PAGE_SIZE = 50
    MIN_PAGE_SIZE = 5
    REQUEST_TIMEOUT = 60

    def __init__(self):
        self.base_url = os.getenv("UBERSMITH_API_URL")
        self.user = os.getenv("UBERSMITH_API_USER")
        self.token = os.getenv("UBERSMITH_API_TOKEN")
        self.page_size = int(os.getenv("CONNEX_PAGE_SIZE", self.DEFAULT_PAGE_SIZE))

        if not all([self.base_url, self.user, self.token]):
            raise ValueError("Faltan credenciales de Ubersmith en las variables de entorno.")

    def get(self, method, params=None):
        """Una sola petición GET. No muta el dict de params del caller."""
        return self._request(method, params)

    def get_paginated(self, method, params=None, page_size=None):
        """
        Recorre un listado GET en trozos (limit/offset).

        Ubersmith se cae con respuestas grandes: si una página falla por
        tamaño o JSON inválido, se reintenta ese offset con un limit menor.
        """
        params = dict(params or {})
        page_size = int(page_size or self.page_size)
        merged = {}
        offset = 0
        limit = max(self.MIN_PAGE_SIZE, page_size)
        seen_empty = 0

        while True:
            try:
                payload = self._request_page(method, params, limit=limit, offset=offset)
            except UbersmithAPIError as exc:
                if limit <= self.MIN_PAGE_SIZE:
                    raise
                next_limit = max(self.MIN_PAGE_SIZE, limit // 2)
                logger.warning(
                    "Respuesta grande o inválida en %s offset=%s limit=%s (%s). Reintento con limit=%s",
                    method,
                    offset,
                    limit,
                    exc,
                    next_limit,
                )
                limit = next_limit
                continue

            data = payload.get("data")
            page_items = self._normalize_data(data)
            if not page_items:
                break

            new_keys = 0
            for key, item in page_items.items():
                if key not in merged:
                    new_keys += 1
                merged[key] = item

            if new_keys == 0:
                seen_empty += 1
                if seen_empty >= 2:
                    logger.warning("Paginación de %s no avanza en offset=%s; se detiene.", method, offset)
                    break
            else:
                seen_empty = 0

            if len(page_items) < limit:
                break

            offset += len(page_items)

        return {
            "status": True,
            "error_code": None,
            "error_message": "",
            "data": merged,
        }

    def _request_page(self, method, params, limit, offset):
        page_params = dict(params)
        page_params["limit"] = limit
        page_params["offset"] = offset
        return self._request(method, page_params)

    def _request(self, method, params=None):
        query = dict(params or {})
        query["method"] = method

        try:
            response = requests.get(
                self.base_url,
                auth=HTTPBasicAuth(self.user, self.token),
                params=query,
                timeout=self.REQUEST_TIMEOUT,
            )
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            raise UbersmithAPIError(f"Fallo de red en {method}: {exc}") from exc
        except requests.exceptions.ChunkedEncodingError as exc:
            raise UbersmithAPIError(f"Respuesta incompleta en {method}: {exc}") from exc

        if response.status_code >= 500:
            raise UbersmithAPIError(
                f"HTTP {response.status_code} en {method} (posible respuesta demasiado grande)"
            )

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as exc:
            raise UbersmithAPIError(f"HTTP {response.status_code} en {method}: {exc}") from exc

        if not response.content:
            raise UbersmithAPIError(f"Respuesta vacía en {method}")

        try:
            payload = response.json()
        except (json.JSONDecodeError, requests.exceptions.JSONDecodeError, ValueError) as exc:
            raise UbersmithAPIError(
                f"JSON inválido en {method} (la API suele fallar así con payloads grandes)"
            ) from exc

        if not payload.get("status"):
            raise UbersmithAPIError(
                payload.get("error_message") or f"Ubersmith devolvió status=false en {method}"
            )

        return payload

    @staticmethod
    def _normalize_data(data):
        if not data:
            return {}
        if isinstance(data, dict):
            return {str(key): value for key, value in data.items()}
        if isinstance(data, list):
            normalized = {}
            for index, item in enumerate(data):
                if isinstance(item, dict):
                    key = item.get("clientid") or item.get("invid") or item.get("comment_id") or index
                else:
                    key = index
                normalized[str(key)] = item
            return normalized
        return {}
