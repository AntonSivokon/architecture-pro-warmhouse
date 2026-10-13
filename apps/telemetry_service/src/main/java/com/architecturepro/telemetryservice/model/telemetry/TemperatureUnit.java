package com.architecturepro.telemetryservice.model.telemetry;

public enum TemperatureUnit {

    CELSIUS("°C");

    private final String value;

    TemperatureUnit(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }
}