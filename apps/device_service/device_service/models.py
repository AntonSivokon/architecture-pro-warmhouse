from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


class Module(BaseModel):
    id: UUID
    code: str = Field(max_length=64)
    name: str = Field(max_length=150)
    description: str | None = None


class DeviceState(BaseModel):
    deviceId: UUID
    state: Literal["ACTIVE", "INACTIVE", "OFFLINE", "ERROR"]
    payload: dict[str, Any]
    updatedAt: datetime


class Device(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: UUID
    typeId: UUID
    locationId: UUID
    serialNumber: str
    name: str
    status: Literal["ACTIVE", "INACTIVE", "OFFLINE", "ERROR"]
    modules: list[Module]
    state: DeviceState | None = None
    createdAt: datetime | None = None
    updatedAt: datetime | None = None


class CreateDeviceRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    typeId: UUID
    locationId: UUID
    serialNumber: str = Field(max_length=100)
    name: str = Field(max_length=150)
    moduleIds: list[UUID] = Field(default_factory=list)

    @field_validator("moduleIds")
    @classmethod
    def module_ids_must_be_unique(cls, value: list[UUID]) -> list[UUID]:
        if len(value) != len(set(value)):
            raise ValueError("Идентификаторы модулей не должны повторяться")
        return value


class DevicePage(BaseModel):
    items: list[Device]
    count: int = Field(ge=0)
    offset: int = Field(ge=0)
    size: int = Field(ge=1)


class DeviceCommandRequest(BaseModel):
    command: str = Field(min_length=1, max_length=64)
    payload: dict[str, Any] = Field(default_factory=dict)


class DeviceCommandAccepted(BaseModel):
    deviceId: UUID
    command: str
    status: Literal["accepted"] = "accepted"
    requestedAt: datetime


class ErrorResponse(BaseModel):
    code: str
    message: str
    details: dict[str, Any] | None = None
