import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from fastapi import FastAPI, HTTPException, Query, Request, Response, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from device_service.models import (
    CreateDeviceRequest,
    Device,
    DeviceCommandAccepted,
    DeviceCommandRequest,
    DevicePage,
    DeviceState,
    ErrorResponse,
)
from device_service.monolith_bridge import (
    LegacySensor,
    MonolithClient,
    MonolithError,
    device_id_for_sensor,
    uuid_for_legacy_value,
)
from device_service.publisher import CommandPublisher, KafkaCommandPublisher


logger = logging.getLogger("device_service")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def device_from_sensor(sensor: LegacySensor) -> Device:
    device_id = device_id_for_sensor(sensor.id)
    state_name = {
        "active": "ACTIVE",
        "online": "ACTIVE",
        "inactive": "INACTIVE",
        "error": "ERROR",
    }.get(sensor.status.lower(), "OFFLINE")
    state = DeviceState(
        deviceId=device_id,
        state=state_name,
        payload={
            "value": sensor.value,
            "unit": sensor.unit,
            "location": sensor.location,
            "legacySensorId": sensor.id,
        },
        updatedAt=sensor.last_updated,
    )
    return Device(
        id=device_id,
        typeId=uuid_for_legacy_value(sensor.type),
        locationId=uuid_for_legacy_value(sensor.location),
        serialNumber=str(sensor.id),
        name=sensor.name,
        status=state_name,
        modules=[],
        state=state,
        createdAt=sensor.created_at,
        updatedAt=sensor.last_updated,
    )


def create_app(publisher: CommandPublisher | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        app.state.monolith.close()

    app = FastAPI(title="Сервис устройств", version="1.0.0", lifespan=lifespan)
    configured_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "").strip()
    monolith_url = os.getenv("LEGACY_API_URL", "http://localhost:8082")
    app.state.monolith = MonolithClient(monolith_url)
    app.state.publisher = publisher

    if publisher is None and configured_servers:
        try:
            app.state.publisher = KafkaCommandPublisher(configured_servers)
        except Exception:
            logger.exception("Не удалось инициализировать отправителя команд в Kafka")
            app.state.publisher_error = True
    else:
        app.state.publisher_error = False

    @app.exception_handler(HTTPException)
    async def http_error_handler(request: Request, exc: HTTPException) -> JSONResponse:
        del request
        detail = exc.detail if isinstance(exc.detail, dict) else {}
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "code": detail.get("code", "http_error"),
                "message": detail.get("message", "Не удалось обработать запрос."),
            },
            headers=exc.headers,
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        request: Request,
        exc: RequestValidationError,
    ) -> JSONResponse:
        del request, exc
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={
                "code": "invalid_request",
                "message": "Запрос содержит недопустимые параметры или данные.",
            },
        )

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get(
        "/api/v1/users/{userId}/devices",
        response_model=DevicePage,
        response_model_exclude_none=True,
        responses={503: {"model": ErrorResponse}},
    )
    def list_user_devices(
        userId: UUID,
        offset: int = Query(default=0, ge=0),
        size: int = Query(default=20, ge=1, le=100),
    ) -> DevicePage:
        del userId
        try:
            devices = [
                device_from_sensor(sensor)
                for sensor in app.state.monolith.get_sensors()
            ]
        except MonolithError as exception:
            raise monolith_http_error(exception) from exception
        return DevicePage(
            items=devices[offset : offset + size],
            count=len(devices),
            offset=offset,
            size=size,
        )

    @app.post(
        "/api/v1/devices",
        response_model=Device,
        response_model_exclude_none=True,
        status_code=status.HTTP_201_CREATED,
        responses={503: {"model": ErrorResponse}},
    )
    def register_device(
        request: CreateDeviceRequest,
        response: Response,
    ) -> Device:
        try:
            sensor = app.state.monolith.create_sensor(
                name=request.name,
                location=str(request.locationId),
            )
        except MonolithError as exception:
            raise monolith_http_error(exception) from exception
        device = device_from_sensor(sensor)
        device_id = device.id
        response.headers["Location"] = f"/api/v1/devices/{device_id}"
        return device

    @app.get(
        "/api/v1/devices/{deviceId}",
        response_model=Device,
        response_model_exclude_none=True,
        responses={404: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
    )
    def get_device(deviceId: UUID) -> Device:
        try:
            sensors = app.state.monolith.get_sensors()
        except MonolithError as exception:
            raise monolith_http_error(exception) from exception
        sensor = next(
            (
                candidate
                for candidate in sensors
                if device_id_for_sensor(candidate.id) == deviceId
            ),
            None,
        )
        if sensor is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "code": "device_not_found",
                    "message": "Устройство с указанным идентификатором не найдено.",
                },
            )
        return device_from_sensor(sensor)

    @app.post(
        "/api/v1/devices/{deviceId}/commands",
        response_model=DeviceCommandAccepted,
        status_code=status.HTTP_202_ACCEPTED,
        responses={404: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
    )
    def send_device_command(
        deviceId: UUID,
        request: DeviceCommandRequest,
    ) -> DeviceCommandAccepted:
        try:
            sensors = app.state.monolith.get_sensors()
        except MonolithError as exception:
            raise monolith_http_error(exception) from exception
        if not any(device_id_for_sensor(sensor.id) == deviceId for sensor in sensors):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "code": "device_not_found",
                    "message": "Устройство с указанным идентификатором не найдено.",
                },
            )

        message: dict[str, Any] = {
            "deviceId": str(deviceId),
            "command": request.command,
            "payload": request.payload,
            "requestedAt": utc_now().isoformat(),
        }
        command_publisher = app.state.publisher
        if command_publisher is None:
            if app.state.publisher_error:
                logger.error(
                    "Команда устройства %s не отправлена: не удалось подключиться к Kafka",
                    deviceId,
                )
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail={
                        "code": "command_publish_failed",
                        "message": "Не удалось отправить команду.",
                    },
                )
            logger.info(
                "Kafka не настроена: команда устройства %s принята в демонстрационном режиме, "
                "но не отправлена",
                deviceId,
            )
        else:
            try:
                command_publisher.publish(message)
            except Exception:
                logger.exception("Не удалось отправить команду устройства %s", deviceId)
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail={
                        "code": "command_publish_failed",
                        "message": "Не удалось отправить команду.",
                    },
                ) from None

        return DeviceCommandAccepted(
            deviceId=deviceId,
            command=request.command,
            requestedAt=datetime.fromisoformat(message["requestedAt"]),
        )

    return app


def monolith_http_error(exception: MonolithError) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail={
            "code": "monolith_unavailable",
            "message": str(exception),
        },
    )