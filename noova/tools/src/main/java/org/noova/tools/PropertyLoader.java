package org.noova.tools;

import java.io.FileInputStream;
import java.util.Properties;

public class PropertyLoader {

    private static final Logger log = Logger.getLogger(PropertyLoader.class);
    private static final String PROPERTIES_FILE = "config.properties";

    public static String getProperty(String key) {
        try (FileInputStream fis = new FileInputStream(PROPERTIES_FILE)) {
            Properties properties = new Properties();
            properties.load(fis);
            log.info("[property loader] Getting property: " + key + " = " + properties.getProperty(key));
            return properties.getProperty(key);
        } catch (Exception e) {
            return null;
        }
    }


    public static void setProperty(String key, String value) {
        try {
            Properties properties = new Properties();
            properties.load(PropertyLoader.class.getClassLoader().getResourceAsStream(PROPERTIES_FILE));
            properties.setProperty(key, value);
            properties.store(new java.io.FileOutputStream(PROPERTIES_FILE), null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    public static void prop(){
//        try {
//            BufferedReader r = new BufferedReader(new FileReader("log.properties"));
//            while (true) {
//                String line = null;
//                try { line = r.readLine(); } catch (IOException ioe) {}
//                if (line == null)
//                    break;
//                line = line.trim();
//                if (line.startsWith("#") || line.equals(""))
//                    continue;
//
//                String[] pcs = line.split("=");
//                pcs[0] = pcs[0].trim();
//                pcs[1] = pcs[1].trim();
//                if (pcs[0].equals("log")) {
//                    String logfileName = pcs[1].replaceAll("\\$MAINCLASS", getMainClassName()).replaceAll("\\$PID", ""+ProcessHandle.current().pid());
//                    try {
//                        logfile = new PrintWriter(new FileWriter(logfileName, true), true);
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//                }
//
//
//    }
}
