package com.architecturepro.telemetryservice.api;

import com.architecturepro.telemetryservice.model.telemetry.TelemetryResponse;
import com.architecturepro.telemetryservice.stream.TelemetryTopology;
import com.architecturepro.telemetryservice.model.telemetry.TelemetryRecord;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/devices")
public class TelemetryController {

    private final StreamsBuilderFactoryBean streamsFactory;
    private final ObjectMapper mapper;

    public TelemetryController(StreamsBuilderFactoryBean streamsFactory, ObjectMapper mapper) {
        this.streamsFactory = streamsFactory;
        this.mapper = mapper;
    }

    /**
     * Получает телеметрию в указанном интервале времени для устройстова.
     * <p><b>В рамках MVP получение и хранение данных происходит с помощью Kafka Store, а не InfluxDB</b></p>
     *
     * @param deviceId идентификатор устройства
     * @param from временной диапазон "от" для поиска записей телеметрии
     * @param to временной диапазон "до" для поиска записей телеметрии
     * @param limit количество записей для получения
     * @return список телеметрии по устройству
     */
    @GetMapping("/{deviceId}/telemetry")
    public TelemetryResponse getTelemetry(
            @PathVariable UUID deviceId,
            @RequestParam(required = false) String from,
            @RequestParam(required = false) String to,
            @RequestParam(defaultValue = "100") int limit
    ) {
        if (limit < 1 || limit > 1000) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Превышен лимит получения данных телеметрии по устройству");
        }
        Instant fromTime = parseTime(from);
        Instant toTime = parseTime(to);

        try {
            KafkaStreams streams = streamsFactory.getKafkaStreams();
            if (streams == null) {
                throw new InvalidStateStoreException("Kafka Streams не запущен");
            }
            ReadOnlyKeyValueStore<String, String> store = streams.store(
                    StoreQueryParameters.fromNameAndType(
                            TelemetryTopology.HISTORY_STORE,
                            QueryableStoreTypes.keyValueStore()
                    )
            );
            String historyJson = store.get(deviceId.toString());
            List<TelemetryRecord> items = filterHistory(historyJson, fromTime, toTime, limit);
            return new TelemetryResponse(items);
        } catch (InvalidStateStoreException exception) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Не удалось получить телеметрию из базы данных", exception);
        } catch (JsonProcessingException exception) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Не удалось обработать данные телеметрии после получения", exception);
        }
    }

    private List<TelemetryRecord> filterHistory(String historyJson, Instant from, Instant to, int limit) throws JsonProcessingException {
        if (historyJson == null) {
            return List.of();
        }
        JsonNode history = mapper.readTree(historyJson);
        List<TelemetryRecord> items = new ArrayList<>();
        for (JsonNode item : history) {
            TelemetryRecord record = mapper.treeToValue(item, TelemetryRecord.class);
            Instant recordedAt = record.recordedAt();
            if (from != null && recordedAt.isBefore(from)) {
                continue;
            }
            if (to != null && !recordedAt.isBefore(to)) {
                continue;
            }
            items.add(record);
            if (items.size() == limit) {
                break;
            }
        }
        return items;
    }

    private Instant parseTime(String value) {
        if (value == null) {
            return null;
        }
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException exception) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Параметры даты и времени должны соответствовать формату ISO-8601", exception);
        }
    }
}