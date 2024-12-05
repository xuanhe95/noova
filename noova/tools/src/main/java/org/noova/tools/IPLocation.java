package org.noova.tools;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CityResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;


public class IPLocation {

    private static final Logger log = Logger.getLogger(IPLocation.class);

//    static {
//        try {
//            InputStream is = IPLocation.class.getClassLoader().getResourceAsStream("models/GeoLite2-City.mmdb");
//
//            if (is == null) {
//                log.error("Database file not found in resources");
//            }
//
//            reader = new DatabaseReader.Builder(is).build();
//            log.info("Successfully loaded GeoIP database");
//        } catch (IOException e) {
//            log.error("Error loading GeoIP database", e);
//        }
//    }

    public static String getZipFromIP(String ip) {
        try {
            String url = "https://ipapi.co/" + ip + "/json/";
            URL apiUrl = new URL(url);
            HttpURLConnection conn = (HttpURLConnection) apiUrl.openConnection();

            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(conn.getInputStream())
            );
            StringBuilder response = new StringBuilder();
            String line;

            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();

            JsonObject jsonObject = new Gson().fromJson(response.toString(), JsonObject.class);
            return jsonObject.get("postal").getAsString();
        } catch (Exception e) {
            log.error("Error getting zip code for IP: " + ip, e);
            return null;
        }
    }

    public static void main(String[] args) {

        String ip = "208.80.154.224";
        String zipCode = getZipFromIP(ip);

        if (zipCode != null) {
            System.out.println("ZIP Code for IP " + ip + ": " + zipCode);
        } else {
            System.out.println("Failed to get ZIP Code for IP: " + ip);
        }
    }
}
