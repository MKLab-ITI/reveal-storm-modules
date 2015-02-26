package gr.iti.mklab.conf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * Created by kandreadou on 2/3/15.
 */
public class Configuration {

    public static String LEARNING_FOLDER;
    public static String INDEX_SERVICE_HOST;
    public static String MONGO_HOST;

    public static void load(String file) throws ConfigurationException {
        PropertiesConfiguration conf = new PropertiesConfiguration(file);
        LEARNING_FOLDER = conf.getString("learningFolder");
        INDEX_SERVICE_HOST = conf.getString("indexServiceHost");
        MONGO_HOST = conf.getString("mongoHost");
    }

    public static void load(InputStream stream) throws ConfigurationException, IOException {
        Properties conf = new Properties();
        conf.load(stream);
        LEARNING_FOLDER = conf.getProperty("learningFolder");
        INDEX_SERVICE_HOST = conf.getProperty("indexServiceHost");
        MONGO_HOST = conf.getProperty("mongoHost");
    }
}