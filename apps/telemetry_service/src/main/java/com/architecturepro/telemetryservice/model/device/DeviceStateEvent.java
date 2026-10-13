package com.architecturepro.telemetryservice.model.device;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record DeviceStateEvent(
        UUID deviceId,
        String state,
        Map<String, Object> payload,
        Instant occurredAt
) {
}