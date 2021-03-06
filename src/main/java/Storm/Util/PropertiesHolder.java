package Storm.Util;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by charlie on 02/02/17.
 */
public class PropertiesHolder {

    private static final Properties properties = new Properties();

    public static String databaseHost;
    public static int databasePort;
    public static String databaseSchema;
    public static String databaseUser;
    public static String databasePass;

    public static String rabbitHost;
    public static int rabbitPort;
    public static String rabbitVHost;
    public static String rabbitUser;
    public static String rabbitPass;

    public static String itemQueue;
    public static String itemStateQueue;
    public static String listQueue;
    public static String dropQueue;
    public static String dropStateQueue;

    public static String production;

    static {
        try {
            properties.load(PropertiesHolder.class.getClassLoader().getResourceAsStream("storm_local.properties"));

            rabbitHost = getValue("rabbit.host");
            rabbitPort = Integer.parseInt(getValue("rabbit.port"));
            rabbitVHost = getValue("rabbit.vhost");
            rabbitUser = getValue("rabbit.user");
            rabbitPass = getValue("rabbit.pass");

            itemQueue = getValue("rabbit.queue.item");
            itemStateQueue = getValue("rabbit.queue.item_state");
            dropQueue = getValue("rabbit.queue.drop");
            dropStateQueue = getValue("rabbit.queue.drop_state");
//            itemStateQueue = getValue("rabbit.queue.list");

            databaseHost = getValue("database.host");
            databasePort = Integer.parseInt(getValue("database.port"));
            databaseSchema = getValue("database.schema");
            databaseUser = getValue("database.user");
            databasePass = getValue("database.pass");

            production = getValue("production");


        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static String getValue(String name) {
        return properties.getProperty(name);
    }
}
