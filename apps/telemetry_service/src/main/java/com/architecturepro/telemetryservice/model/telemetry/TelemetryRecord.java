package com.architecturepro.telemetryservice.model.telemetry;

import java.time.Instant;
import java.util.UUID;

public record TelemetryRecord(
        UUID deviceId,
        String metric,
        String unit,
        double value,
        Instant recordedAt
) {
}