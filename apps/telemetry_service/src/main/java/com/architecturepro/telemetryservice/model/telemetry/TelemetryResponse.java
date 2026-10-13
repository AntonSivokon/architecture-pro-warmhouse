package com.architecturepro.telemetryservice.model.telemetry;

import java.util.List;

public record TelemetryResponse(List<TelemetryRecord> items) {
}
