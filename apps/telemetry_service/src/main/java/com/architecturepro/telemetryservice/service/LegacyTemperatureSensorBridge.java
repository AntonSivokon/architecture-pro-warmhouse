package com.architecturepro.telemetryservice.service;

import com.architecturepro.telemetryservice.model.legacy.LegacySensor;
import com.architecturepro.telemetryservice.config.LegacyProperties;
import com.architecturepro.telemetryservice.stream.TelemetryTopology;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class LegacyTemperatureSensorBridge {

    private static final Logger log = LoggerFactory.getLogger(LegacyTemperatureSensorBridge.class);

    private final RestClient restClient;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper mapper;

    public LegacyTemperatureSensorBridge(
            RestClient.Builder restClientBuilder,
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper mapper,
            LegacyProperties legacyProperties
    ) {
        this.restClient = restClientBuilder.baseUrl(legacyProperties.apiUrl()).build();
        this.kafkaTemplate = kafkaTemplate;
        this.mapper = mapper;
    }

    @Scheduled(fixedDelayString = "${legacy.poll-interval-ms}")
    public void publishLegacySensorSnapshots() {
        List<LegacySensor> sensors;
        try {
            sensors = restClient.get()
                    .uri("/api/v1/sensors")
                    .retrieve()
                    .body(new ParameterizedTypeReference<>() {});
        } catch (RestClientException exception) {
            log.error("Ошибка получения информации по датчикам из legacy приложения", exception);
            return;
        }

        if (sensors == null || sensors.isEmpty()) {
            return;
        }
        for (LegacySensor sensor : sensors) {
            try {
                String json = mapper.writeValueAsString(sensor);
                kafkaTemplate.send(TelemetryTopology.INPUT_TOPIC, Integer.toString(sensor.id()), json)
                        .get(10, TimeUnit.SECONDS);
            } catch (JsonProcessingException | KafkaException | ExecutionException | TimeoutException exception) {
                log.error("Ошибка отправки телеметрии датчика {} в Kafka", sensor.id(), exception);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                log.error("Прервана публикация телеметрии датчика {} при отправке в Kafka", sensor.id(), exception);
                return;
            }
        }
    }
}