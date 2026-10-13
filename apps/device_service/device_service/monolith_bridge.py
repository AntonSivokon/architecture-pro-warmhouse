from datetime import datetime
from hashlib import md5
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid3

import httpx
from pydantic import BaseModel, ConfigDict, ValidationError


class MonolithError(Exception):
    pass


class LegacySensor(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: int
    name: str
    type: str
    location: str
    value: float
    unit: str
    status: str
    last_updated: datetime
    created_at: datetime


class MonolithClient:
    def __init__(self, base_url: str) -> None:
        self._client = httpx.Client(base_url=base_url.rstrip("/"), timeout=10.0)

    def close(self) -> None:
        self._client.close()

    def get_sensors(self) -> list[LegacySensor]:
        result = self._request("GET", "/api/v1/sensors")
        if result is None:
            return []
        if not isinstance(result, list):
            raise MonolithError("Монолит вернул некорректный список датчиков.")
        try:
            return [LegacySensor.model_validate(sensor) for sensor in result]
        except ValidationError as exception:
            raise MonolithError("Монолит вернул данные датчиков в неожиданном формате.") from exception

    def create_sensor(self, name: str, location: str) -> LegacySensor:
        result = self._request(
            "POST",
            "/api/v1/sensors",
            json={
                "name": name,
                "type": "temperature",
                "location": location,
                "unit": "°C",
            },
        )
        try:
            return LegacySensor.model_validate(result)
        except ValidationError as exception:
            raise MonolithError("Монолит вернул данные датчика в неожиданном формате.") from exception

    def _request(self, method: str, path: str, **kwargs: Any) -> object:
        try:
            response = self._client.request(method, path, **kwargs)
        except httpx.RequestError as exception:
            raise MonolithError("Монолит недоступен.") from exception

        if response.is_error:
            raise MonolithError(
                f"Монолит вернул ошибку HTTP {response.status_code}."
            )
        try:
            return response.json()
        except ValueError as exception:
            raise MonolithError("Монолит вернул ответ в некорректном формате.") from exception


def device_id_for_sensor(sensor_id: int) -> UUID:
    digest = md5(f"legacy-sensor:{sensor_id}".encode("utf-8")).digest()
    return UUID(bytes=digest, version=3)


def uuid_for_legacy_value(value: str) -> UUID:
    try:
        return UUID(value)
    except ValueError:
        return uuid3(NAMESPACE_URL, f"legacy-sensor-value:{value}")
