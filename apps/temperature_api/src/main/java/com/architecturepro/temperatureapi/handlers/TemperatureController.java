package com.architecturepro.temperatureapi.handlers;

import com.architecturepro.temperatureapi.models.TemperatureResponse;
import com.architecturepro.temperatureapi.services.TemperatureService;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TemperatureController {

    private final TemperatureService temperatureService;

    public TemperatureController(TemperatureService temperatureService) {
        this.temperatureService = temperatureService;
    }

    @GetMapping("/health")
    public Map<String, String> health() {
        return Map.of("status", "OK");
    }

    @GetMapping("/temperature")
    public TemperatureResponse getTemperature(
            @RequestParam(defaultValue = "") String location,
            @RequestParam(defaultValue = "") String sensorId
    ) {
        return temperatureService.getTemperature(location, sensorId);
    }

    @GetMapping("/temperature/{sensorId}")
    public TemperatureResponse getTemperatureBySensorId(@PathVariable String sensorId) {
        return temperatureService.getTemperature("", sensorId);
    }
}