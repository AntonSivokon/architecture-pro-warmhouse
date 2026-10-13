package com.architecturepro.telemetryservice.stream;

import com.architecturepro.telemetryservice.model.device.DeviceState;
import com.architecturepro.telemetryservice.model.device.DeviceStateEvent;
import com.architecturepro.telemetryservice.model.legacy.LegacySensor;
import com.architecturepro.telemetryservice.model.legacy.SensorType;
import com.architecturepro.telemetryservice.model.telemetry.TemperatureUnit;
import com.architecturepro.telemetryservice.model.telemetry.TelemetryRecord;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Configuration
public class TelemetryTopology {

    public static final String INPUT_TOPIC = "legacy.telemetry";
    public static final String TELEMETRY_TOPIC = "iot.telemetry";
    public static final String DEVICE_STATE_TOPIC = "iot.device-state";
    public static final String HISTORY_STORE = "telemetry-history-store";
    private static final int MAX_HISTORY_PER_DEVICE = 100;

    @Bean
    KStream<String, String> telemetryStream(StreamsBuilder builder, ObjectMapper mapper) {
        KStream<String, String> snapshots = builder.stream(
                INPUT_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        KStream<String, String> telemetry = snapshots
                .mapValues(json -> toTelemetryRecord(json, mapper))
                .filter((key, value) -> value != null)
                .selectKey((key, value) -> value.deviceId().toString())
                .mapValues(value -> writeJson(mapper, value));

        // Отправляем телеметрию в сервис устройств и автоматизации
        telemetry.to(TELEMETRY_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
        // Сохраняем телеметрию в БД (в MVP сохраняем просто в Kafka Store)
        telemetry.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .aggregate(
                        () -> "[]",
                        (deviceId, eventJson, historyJson) -> appendHistory(mapper, historyJson, eventJson),
                        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(HISTORY_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.String())
                );
        // Агрегируем и отправляем состояние устройства в сервис устройств
        snapshots
                .mapValues(json -> toDeviceStateEvent(json, mapper))
                .filter((key, value) -> value != null)
                .selectKey((key, value) -> value.deviceId().toString())
                .mapValues(value -> writeJson(mapper, value))
                .to(DEVICE_STATE_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        return telemetry;
    }

    private static TelemetryRecord toTelemetryRecord(String json, ObjectMapper mapper) {
        try {
            LegacySensor sensor = mapper.readValue(json, LegacySensor.class);
            if (!SensorType.TEMPERATURE.matches(sensor.type())) {
                return null;
            }
            UUID deviceId = legacyDeviceId(sensor.id());
            String unit = temperatureUnit(sensor.unit());
            Instant recordedAt = sensor.lastUpdated() == null ? Instant.now() : sensor.lastUpdated();
            return new TelemetryRecord(deviceId, SensorType.TEMPERATURE.value(), unit, sensor.value(), recordedAt);
        } catch (IOException | IllegalArgumentException exception) {
            throw new IllegalArgumentException("Ошибка получения данных телеметрии от датчика", exception);
        }
    }

    private static DeviceStateEvent toDeviceStateEvent(String json, ObjectMapper mapper) {
        try {
            LegacySensor sensor = mapper.readValue(json, LegacySensor.class);
            if (!SensorType.TEMPERATURE.matches(sensor.type())) {
                return null;
            }
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("value", sensor.value());
            payload.put("unit", temperatureUnit(sensor.unit()));
            payload.put("location", sensor.location());
            payload.put("legacySensorId", sensor.id());
            return new DeviceStateEvent(
                    legacyDeviceId(sensor.id()),
                    DeviceState.fromLegacyStatus(sensor.status()).value(),
                    payload,
                    sensor.lastUpdated() == null ? Instant.now() : sensor.lastUpdated()
            );
        } catch (IOException | IllegalArgumentException exception) {
            throw new IllegalArgumentException("Ошибка получения данных телеметрии от датчика", exception);
        }
    }

    private static UUID legacyDeviceId(int sensorId) {
        return UUID.nameUUIDFromBytes(("legacy-sensor:" + sensorId).getBytes(StandardCharsets.UTF_8));
    }

    private static String temperatureUnit(String unit) {
        return unit == null || unit.isBlank() ? TemperatureUnit.CELSIUS.value() : unit;
    }

    private static String appendHistory(ObjectMapper mapper, String historyJson, String eventJson) {
        try {
            JsonNode existing = mapper.readTree(historyJson);
            List<JsonNode> events = new ArrayList<>();
            events.add(mapper.readTree(eventJson));
            if (existing.isArray()) {
                existing.forEach(events::add);
            }
            if (events.size() > MAX_HISTORY_PER_DEVICE) {
                events = events.subList(0, MAX_HISTORY_PER_DEVICE);
            }
            return mapper.writeValueAsString(events);
        } catch (IOException exception) {
            throw new IllegalStateException("Не удалось сохранить данные телеметрии датчика", exception);
        }
    }

    private static String writeJson(ObjectMapper mapper, Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Не удалось сериализовать телеметрию в JSON", exception);
        }
    }
}
