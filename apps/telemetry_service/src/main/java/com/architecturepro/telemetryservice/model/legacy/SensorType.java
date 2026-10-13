package com.architecturepro.telemetryservice.model.legacy;

import java.util.Locale;

public enum SensorType {

    TEMPERATURE("temperature");

    private final String value;

    SensorType(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    public boolean matches(String candidate) {
        return candidate != null && value.equals(candidate.toLowerCase(Locale.ROOT));
    }
}
