package com.architecturepro.telemetryservice.api;

import org.apache.kafka.streams.KafkaStreams;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class HealthController {

    private final StreamsBuilderFactoryBean streamsFactory;

    public HealthController(StreamsBuilderFactoryBean streamsFactory) {
        this.streamsFactory = streamsFactory;
    }

    @GetMapping("/health")
    public Map<String, String> health() {
        KafkaStreams streams = streamsFactory.getKafkaStreams();
        String state = streams == null ? "NOT_RUNNING" : streams.state().name();
        String status = streams != null && streams.state() == KafkaStreams.State.RUNNING ? "UP" : "DOWN";
        return Map.of("status", status, "kafkaStreams", state);
    }
}