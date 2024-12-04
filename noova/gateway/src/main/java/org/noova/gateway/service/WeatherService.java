package org.noova.gateway.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class WeatherService {
    private static final String WEATHER_API_URL = "https://api.open-meteo.com/v1/forecast";
    private static WeatherService instance;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private WeatherService() {}

    public static WeatherService getInstance() {
        if (instance == null) {
            instance = new WeatherService();
        }
        return instance;
    }

    public Map<String, Object> getWeatherInfo() throws IOException {
        // New York coordinates
        String latitude = "40.7128";
        String longitude = "-74.0060";

        String urlStr = WEATHER_API_URL +
                "?latitude=" + latitude +
                "&longitude=" + longitude +
                "&daily=temperature_2m_max,temperature_2m_min,precipitation_probability_mean,weather_code" +  // 获取每日数据
                "&temperature_unit=fahrenheit" +
                "&timezone=America/New_York" +
                "&forecast_days=7";

        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");

        try {
            int responseCode = conn.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(conn.getInputStream()));
                String inputLine;
                StringBuilder response = new StringBuilder();

                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();

                Map<String, Object> weatherData = OBJECT_MAPPER.readValue(
                        response.toString(),
                        new TypeReference<Map<String, Object>>() {}
                );

                Map<String, Object> daily = (Map<String, Object>) weatherData.get("daily");
                List<String> dates = (List<String>) daily.get("time");
                List<Double> maxTemps = (List<Double>) daily.get("temperature_2m_max");
                List<Double> minTemps = (List<Double>) daily.get("temperature_2m_min");
                List<Integer> weatherCodes = (List<Integer>) daily.get("weather_code");
                List<Double> precipProb = (List<Double>) daily.get("precipitation_probability_mean");

                List<Map<String, Object>> forecast = new ArrayList<>();
                for (int i = 0; i < dates.size(); i++) {
                    Map<String, Object> dayForecast = new HashMap<>();
                    dayForecast.put("date", dates.get(i));
                    dayForecast.put("maxTemp", maxTemps.get(i));
                    dayForecast.put("minTemp", minTemps.get(i));
                    dayForecast.put("conditions", getWeatherDescription(weatherCodes.get(i)));
                    dayForecast.put("precipitationChance", precipProb.get(i));
                    forecast.add(dayForecast);
                }

                Map<String, Object> result = new HashMap<>();
                result.put("location", "New York");
                result.put("temperatureUnit", "°F");
                result.put("forecast", forecast);

                return result;
            } else {
                throw new IOException("HTTP error code: " + responseCode);
            }
        } finally {
            conn.disconnect();
        }
    }

    private String getWeatherDescription(int code) {
        if (code == 0) return "Clear sky";
        if (code == 1 || code == 2 || code == 3) return "Partly cloudy";
        if (code >= 51 && code <= 57) return "Drizzle";
        if (code >= 61 && code <= 65) return "Rain";
        if (code >= 71 && code <= 77) return "Snow";
        if (code >= 80 && code <= 82) return "Rain showers";
        if (code >= 95 && code <= 99) return "Thunderstorm";
        return "Unknown";
    }
}