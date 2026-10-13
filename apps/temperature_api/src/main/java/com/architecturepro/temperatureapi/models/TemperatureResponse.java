package com.architecturepro.temperatureapi.models;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

public record TemperatureResponse(
        double value,
        String unit,
        @JsonFormat(shape = JsonFormat.Shape.STRING) Instant timestamp,
        String location,
        String status,
        @JsonProperty("sensor_id") String sensorId,
        @JsonProperty("sensor_type") String sensorType,
        String description
) {}