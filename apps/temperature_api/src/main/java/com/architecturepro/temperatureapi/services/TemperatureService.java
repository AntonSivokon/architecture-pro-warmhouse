package com.architecturepro.temperatureapi.services;

import com.architecturepro.temperatureapi.models.TemperatureResponse;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.stereotype.Service;

@Service
public class TemperatureService {

    private static final int MIN_TENTHS = 150;
    private static final int MAX_TENTHS = 300;
    private static final int READING_COUNT = MAX_TENTHS - MIN_TENTHS + 1;

    private final AtomicInteger lastReading = new AtomicInteger();

    public TemperatureResponse getTemperature(String location, String sensorId) {
        String resolvedLocation = normalize(location);
        String resolvedSensorId = normalize(sensorId);

        if (resolvedLocation.isEmpty()) {
            resolvedLocation = locationForSensor(resolvedSensorId);
        }
        if (resolvedSensorId.isEmpty()) {
            resolvedSensorId = sensorForLocation(resolvedLocation);
        }

        int reading = lastReading.updateAndGet(TemperatureService::nextReading);
        return new TemperatureResponse(
                reading / 10.0,
                "°C",
                Instant.now(),
                resolvedLocation,
                "active",
                resolvedSensorId,
                "temperature",
                "Random temperature reading for " + resolvedLocation);
    }

    private static int nextReading(int previous) {
        int reading = ThreadLocalRandom.current().nextInt(MIN_TENTHS, MAX_TENTHS + 1);
        if (reading == previous) {
            return MIN_TENTHS + (reading - MIN_TENTHS + 1) % READING_COUNT;
        }
        return reading;
    }

    private static String normalize(String value) {
        return value == null ? "" : value.trim();
    }

    private static String locationForSensor(String sensorId) {
        return switch (sensorId) {
            case "1" -> "Living Room";
            case "2" -> "Bedroom";
            case "3" -> "Kitchen";
            default -> "Unknown";
        };
    }

    private static String sensorForLocation(String location) {
        return switch (location) {
            case "Living Room" -> "1";
            case "Bedroom" -> "2";
            case "Kitchen" -> "3";
            default -> "0";
        };
    }
}