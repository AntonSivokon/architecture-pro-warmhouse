package com.architecturepro.telemetryservice.model.device;

import com.architecturepro.telemetryservice.model.legacy.LegacyDeviceStatus;

public enum DeviceState {

    ACTIVE,
    INACTIVE,
    ERROR,
    OFFLINE;

    public String value() {
        return name();
    }

    public static DeviceState fromLegacyStatus(String status) {
        if (status == null) {
            return OFFLINE;
        }
        return LegacyDeviceStatus.toDeviceState(status);
    }
}
