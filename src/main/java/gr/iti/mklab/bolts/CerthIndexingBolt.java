package gr.iti.mklab.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import gr.iti.mklab.visual.VisualIndexer;
import itinno.common.StormLoggingHelper;
import org.json.simple.JSONObject;

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by kandreadou on 3/31/15.
 */
public class CerthIndexingBolt extends BaseRichBolt {
    private OutputCollector collector;
    private String strExampleEmitFieldsId;

    // Initialise Logger object
    private Logger logger = null;
    private String strLogBaseDir;
    private String strLogPattern;
    private Level logLevel;

    static{
        try {
            VisualIndexer.init();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * Main Java Logger Bolt constructor
     *
     * @param strExampleEmitFieldsId id of the fields that will be emited by this bolt
     * @param strLogBaseDir          log base directory
     * @param logLevel               ch.qos.logback.classic.Level log level value
     * @throws Exception throws exception if log file name and/or emit field id are empty, as well as if the log level is not an instance of ch.qos.logback.classic.Level
     */
    public CerthIndexingBolt(String strExampleEmitFieldsId, String strLogBaseDir, String strLogPattern, Level logLevel) throws Exception {
        super();

        // Store emit fields name, ExampleSocialMediaJavaLoggerBolt id and path to the main configuration file
        if (strExampleEmitFieldsId.isEmpty()) {
            throw new Exception("Emit fields id can not be nil or emmty.");
        }

        // Check if the log file name length is more than 0
        if (strLogBaseDir.isEmpty()) {
            throw new Exception("Log bolt file name can not be empty,");
        }

        // Check if logging pattern is more than 0
        if (strLogPattern.isEmpty()) {
            throw new Exception("Logging pattern can not be empty,");
        }

        // Check if the log Level is instance of (be explicit here about the level in order to make sure that correct instance being checked)
        if (!(logLevel instanceof Level)) {
            throw new Exception("Log level object must be instance of the ch.qos.logback.classic.Level, but was ." + logLevel.getClass());
        }

        // After all the above checks complete, store the emit field id, path (or name) of the log file and log level
        this.strExampleEmitFieldsId = strExampleEmitFieldsId;
        this.strLogBaseDir = strLogBaseDir;
        this.strLogPattern = strLogPattern;
        this.logLevel = logLevel;
    }


    /**
     * Prepare method is similar the "Open" method for Spouts and is called when a worker is about to be put to work.
     * This method also initialise the main example Storm Java bolt logger.
     *
     * @param stormConf map of the storm configuration (passed within Storm topology itself, not be a user)
     * @param context   context (e.g. similar to description) of the topology (passed within Storm topology itself, not be a user)
     * @param collector output collector of the Storm (which is responsible to emiting new tuples, passed within Storm topology itself, not be a user)
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        String strPID = null;
        this.collector = collector;

        // Setup the logger
        try {
            // Create log file name - combination of class name and current PID e.g. ExampleJavaSocialMediaStormTopologyRunner_pid123.log
            try {
                // Try to get the pid using java.lang.Management class and split it on @ symbol (e.g. returned value will be in the format of {p_id}@{host_name})
                strPID = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];

                // Handle any possible exception here, such as if the process_name@host will not be returned (possible, depends on different JVMs)
            } catch (Exception e) {
                // Print the message, stacktrace and allow to continue (pid value will not be contained in the log file)
                System.err.println("Failed to get process process id. Will continue but the log files names will not contain pid value. Details: " + e.getMessage());
                e.printStackTrace();

                // Pid will be simply an empty value
                strPID = "";
            }

            // Create log file name - combination of class name and current process id, e.g. ExampleSocialMediaJavaLoggerBolt_pid123.log
            String strLogName = "IndexingBolt_pid" + strPID + ".log";

            // Specify the path to the log file (the file that will be created)
            String fileSep = System.getProperty("file.separator");
            String strLogFilePath = this.strLogBaseDir + fileSep + strLogName;

            StormLoggingHelper stormLoggingHelper = new StormLoggingHelper();
            this.logger = stormLoggingHelper.createLogger(CerthIndexingBolt.class.getName(), strLogFilePath,
                    this.strLogPattern, this.logLevel);

            // Issue test message
            this.logger.info("Logger was initialised.");

        } catch (Exception e) {
            // Print error message, stacktrace and throw an exception since the log functionality is the main target of this bolt
            System.err.printf("Error occurred during Storm Java Logger Bolt logger setup. Details: " + e.getMessage());
            e.printStackTrace();
            try {
                throw new Exception("Java Storm logger bolt log initialisation failed. Details: " + e.getMessage());
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }
    }


    /**
     * Execute received Tuple. In this case the bolt will parse received tuple and log the message to the log file.
     *
     * @param input standard Storm tuple input object (passed within Storm topology itself, not be a user)
     */
    @Override
    public void execute(Tuple input) {
        try {
            // Retrieve hash map tuple object from Tuple input at index 0, index 1 will be message delivery tag (not used here)
            Map<Object, Object> inputMap = (HashMap<Object, Object>) input.getValue(0);

            // Get JSON object from the HashMap from the Collections.singletonList
            JSONObject jsonObject = (JSONObject) Collections.singletonList(inputMap.get("message")).get(0);

            // Since all the input will be utf-8 encoded using AMQP Storm Schema, simple get the json string message value
            String strMessage = jsonObject.toString();

            // Finally log UTF-8 JSON message to disk to verify its all OK
            logger.info("JSON received = " + jsonObject.toString());

            // Emit a received message
            this.collector.emit(new Values(strMessage));

            // Acknowledge the collector that we actually received the input
            this.collector.ack(input);

        } catch (Exception e) {
            e.printStackTrace();
            try {
                throw new Exception("Failed to parse tuple input. Details: " + e.getMessage());
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }
    }


    /**
     * Declare output field name (in this case simple a string value that will be defined in the main example storm configuration file)
     *
     * @param declarer standard Storm output fields declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // We will not be declaring and sending any output from this Bolt
        declarer.declare(new Fields(this.strExampleEmitFieldsId));
    }
}