package store.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class Database {

    private static Database instance;
    private static Properties configuration;
    private static Connection connection = null;
    private static String url;

    private Database(Properties configuration) {
        this.configuration = configuration;
        url = "jdbc:mysql://" +
                getProperty("DB_HOST") + ":" +
                getProperty("DB_PORT") + "/" +
                getProperty("DB_NAME");
    }

    public static void configInstance(Properties configuration) {
        instance = new Database(configuration);
    }

    public static TableHandler getTable(String tableName) throws Exception {
        if(instance == null){
            throw new NullPointerException("Database instance is not configured.\n" +
                    "Call Database.configInstance() first");
        } else {
            try {
                connection = DriverManager.getConnection(url,
                        configuration.getProperty("DB_USER"),
                        configuration.getProperty("DB_PASS"));

                return new TableHandler(tableName, connection);
            } catch (SQLException throwable) {
                throw throwable;
            }
        }
    }


    /*
     *  Helpers
     */
    private String getProperty(String key) {
        String value = configuration.getProperty(key);
        if (value == null) {
            throw new IllegalArgumentException("The given KEY was not present: [" + key + "]");
        }
        return value;
    }
}
