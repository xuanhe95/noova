package org.noova.gateway.service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.SocketTimeoutException;
import java.util.regex.Pattern;
import org.json.JSONObject;


/**
 * @author Yuan Ding
 */
public class LocationService implements IService {

    private static final int TIMEOUT = 5000;
    private static final Pattern IPV4_PATTERN = Pattern.compile("^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$");
    private static final Pattern IPV6_PATTERN = Pattern.compile("^[0-9a-fA-F]{1,4}(:[0-9a-fA-F]{1,4}){7}$");

    public static String getZipcodeFromIP(String ipAddress) {
        if (!isValidIP(ipAddress)) {
            System.out.println("Invalid IP address format.");
            return null;
        }

        String apiUrl = "http://ip-api.com/json/" + ipAddress;
        try {
            URL url = new URL(apiUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(TIMEOUT);
            connection.setReadTimeout(TIMEOUT);

            // Check for a valid response code
            int responseCode = connection.getResponseCode();
            if (responseCode != 200) {
                System.out.println("Error: Failed to connect. Response code: " + responseCode);
                return null;
            }

            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = in.readLine()) != null) {
                response.append(line);
            }
            in.close();

            JSONObject jsonResponse = new JSONObject(response.toString());
            if ("success".equals(jsonResponse.getString("status"))) {
                return jsonResponse.getString("zip") + "," + jsonResponse.getString("city");
            }

        } catch (SocketTimeoutException e) {
            System.out.println("Connection timed out.");
        } catch (Exception e) {
            System.out.println("An error occurred: " + e.getMessage());
        }
        return null;
    }

    // Validate IP address (both IPv4 and IPv6)
    private static boolean isValidIP(String ip) {
        return IPV4_PATTERN.matcher(ip).matches() || IPV6_PATTERN.matcher(ip).matches();
    }

}
