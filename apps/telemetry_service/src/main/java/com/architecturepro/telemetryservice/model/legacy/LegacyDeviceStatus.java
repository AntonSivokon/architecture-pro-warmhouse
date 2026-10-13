package com.architecturepro.telemetryservice.model.legacy;

import com.architecturepro.telemetryservice.model.device.DeviceState;

import java.util.Locale;

public enum LegacyDeviceStatus {

    ACTIVE("active", DeviceState.ACTIVE),
    ONLINE("online", DeviceState.ACTIVE),
    INACTIVE("inactive", DeviceState.INACTIVE),
    ERROR("error", DeviceState.ERROR);

    private final String value;
    private final DeviceState state;

    LegacyDeviceStatus(String value, DeviceState state) {
        this.value = value;
        this.state = state;
    }

    public DeviceState state() {
        return state;
    }

    public static DeviceState toDeviceState(String value) {
        String normalized = value.toLowerCase(Locale.ROOT);
        for (LegacyDeviceStatus status : values()) {
            if (status.value.equals(normalized)) {
                return status.state;
            }
        }
        return DeviceState.OFFLINE;
    }
}
