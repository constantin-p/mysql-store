package store.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Config {

    private static final Logger LOGGER = Logger.getLogger(Config.class.getName());

    private Map<String, Map.Entry<String, Properties>> configMap = new HashMap<>();
    private static Config instance;


    private Config() { }

    public static Config getInstance() {
        if(instance == null){
            instance = new Config();
        }
        return instance;
    }

    public void loadConfig(String key, String resourceName) {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        Properties properties = new Properties();
        InputStream input = null;

        try {
            input = loader.getResourceAsStream(resourceName);

            if (input == null) {
                LOGGER.log(Level.SEVERE, "Unable to find the config file: " +
                        resourceName + ".\n" +
                        "See template" + resourceName + " for details.\n");
                return;
            }

            properties.load(input);
            configMap.put(key, new AbstractMap.SimpleEntry<String, Properties>(resourceName, properties));
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, ex.toString(), ex);
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException ex) {
                    LOGGER.log(Level.SEVERE, ex.toString(), ex);
                }
            }
        }
    }

    public Properties getConfig(String key) {
        Map.Entry<String, Properties> configSet = configMap.get(key);
        if (configSet == null) {
            throw new IllegalArgumentException("No configuration loaded under the given KEY: [" +
                    key + "]");

        }
        return configSet.getValue();
    }
}

