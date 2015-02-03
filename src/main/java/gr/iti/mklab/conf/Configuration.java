package gr.iti.mklab.conf;

/**
 * Created by kandreadou on 2/3/15.
 */
public class Configuration {

    public static enum CONF {
        LOCAL, ITI310, DOCKER
    }

    public static String LEARNING_FOLDER;
    public static String INDEX_FOLDER;
    public static String SCRIPTS_FOLDER;
    public static String CRAWLS_FOLDER;

    public static int INDEX_SIZE = 100000; //100 thousand

    public static void main(String[] args) throws Exception {
        Configuration c = new Configuration();
        c.loadConfiguration(CONF.ITI310);
    }

    public static void loadConfiguration(CONF conf) {
        switch (conf) {
            case LOCAL:
                LEARNING_FOLDER = "/home/kandreadou/webservice/learning_files/";
                INDEX_FOLDER = "/home/kandreadou/webservice/reveal_indices/";
                break;
            case ITI310:
                LEARNING_FOLDER = "/home/iti-310/VisualIndex/learning_files/";
                INDEX_FOLDER = "/home/iti-310/VisualIndex/data/";
                SCRIPTS_FOLDER = "/home/iti-310/vdata/";
                CRAWLS_FOLDER = "/home/iti-310/VisualIndex/data/";
                break;
            case DOCKER:
                LEARNING_FOLDER = "/usr/learning_files/";
                INDEX_FOLDER = "/usr/visual/";
                SCRIPTS_FOLDER = "/usr/bubing/";
                CRAWLS_FOLDER = "/usr/crawls/";
                break;
        }

    }
}
