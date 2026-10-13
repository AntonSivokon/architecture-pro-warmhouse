package com.architecturepro.telemetryservice.model.legacy;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public record LegacySensor(
        int id,
        String name,
        String type,
        String location,
        double value,
        String unit,
        String status,
        @JsonProperty("last_updated") Instant lastUpdated,
        @JsonProperty("created_at") Instant createdAt
) {
}