package com.architecturepro.telemetryservice.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "legacy")
public record LegacyProperties(String apiUrl, long pollIntervalMs) {
}