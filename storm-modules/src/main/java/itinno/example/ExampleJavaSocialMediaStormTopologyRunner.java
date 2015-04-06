/////////////////////////////////////////////////////////////////////////
//
// \xa9 University of Southampton IT Innovation, 2014
//
// Copyright in this software belongs to IT Innovation Centre of
// Gamma House, Enterprise Road, Southampton SO16 7NS, UK.
//
// This software may not be used, sold, licensed, transferred, copied
// or reproduced in whole or in part in any manner or form or in or
// on any media by any person other than in accordance with the terms
// of the Licence Agreement supplied with the software, or otherwise
// without the prior written consent of the copyright owners.
//
// This software is distributed WITHOUT ANY WARRANTY, without even the
// implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
// PURPOSE, except where stated in the Licence Agreement supplied with
// the software.
//
//	Created By :	Vadim Krivcov
//	Created Date :	2014/03/27
//	Created for Project:	REVEAL
//
/////////////////////////////////////////////////////////////////////////
//
// Dependencies: None
//
/////////////////////////////////////////////////////////////////////////

package itinno.example;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Properties;


// Import slf4j logger and logback logger level
import gr.iti.mklab.bolts.CerthIndexingBolt;
import gr.iti.mklab.bolts.CerthSimilarityBolt;
import gr.iti.mklab.bolts.CerthVisualBolt;
import gr.iti.mklab.bolts.IndexingBolt;
import gr.iti.mklab.conf.Configuration;
import gr.iti.mklab.visual.VisualIndexer;
import org.slf4j.Logger;

import ch.qos.logback.classic.Level;

// Import core Storm classes
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

// Import example Bolt classes
import itinno.example.ExampleSocialMediaJavaLoggerBolt;
import itinno.example.ExampleSocialMediaJavaPrinterBolt;
import itinno.example.ExampleSocialMediaAMQPSpout;

// Import example logger setup class
import itinno.common.StormLoggingHelper;

// Import AMQP Connection Configuration classes
import io.latent.storm.rabbitmq.config.ConnectionConfig;
import io.latent.storm.rabbitmq.config.ConsumerConfig;
import io.latent.storm.rabbitmq.config.ConsumerConfigBuilder;
import io.latent.storm.rabbitmq.Declarator;


// Import main RabbitMQ broker communication classes
import com.rabbitmq.client.ConnectionFactory;

// Import third party SimpleJSONScheme class
import com.rapportive.storm.scheme.SimpleJSONScheme;


/**
 * storm jar reveal-storm-modules.jar itinno.example.ExampleJavaSocialMediaStormTopologyRunner -mode distributed -assessmentid omgtest1 nimbus.host=localhost
 * <p/>
 * storm jar reveal-storm-modules.jar itinno.example.ExampleJavaSocialMediaStormTopologyRunner stormTopology -mode distributed -assessmentid TEST567 -c nimbus.host=172.17.0.12 -c nimbus.thirf.port=6627
 * <p/>
 * <p/>
 * Main Java STORM Runner class
 * <p/>
 * NOTE: There is API documentation (if available) provided in order to help understanding the Storm and its configurations/processes, but the API documentation
 * is out-dated (provides API for Storm v.0.8.1, but Storm is 0.9.1), however it is still consistent in most of the cases.
 * <p/>
 * Main STORM API (v.0.8.1): http://nathanmarz.github.io/storm/doc-0.8.1/index.html (unfortunately there is not direct link to specific APIs)
 */
public class ExampleJavaSocialMediaStormTopologyRunner {
    // General (brief) help instructions on how to start Java/Python storm and corresponding unit tests
    private static String strStormRunInstructions = "\n\n\nGeneral example Storm help instructions:"
            + "\n--------------------\n"
            + "\n\nDeploy example Storm topology"
            + "\n--------------------"
            + "\nDeploy example Java Storm topology (example Storm topology with Java bolts only):"
            + "\n - Windows OS command: ant -f build.xml example-storm-java -propertyfile=\"storm.properties\""
            + "\n - Unix OS command:    ant -f build.xml example-storm-java -propertyfile storm.properties"
            + "\n\nDeploy example Python Storm topology (example Storm topology with Java and Python bolts):"
            + "\n - Windows OS command: ant -f build.xml example-storm-python -propertyfile=\"storm.properties\""
            + "\n - Unix OS command:    ant -f build.xml example-storm-python -propertyfile storm.properties"
            + "\n\n\nUnit tests (send example message containing json object to the rabbitmq)"
            + "\n--------------------"
            + "\nUnit test using java client:"
            + "\n - Windows OS command: ant -f build.xml example-client-java -propertyfile=\"storm.properties\""
            + "\n - Unix OS command:    ant -f build.xml example-client-java -propertyfile storm.properties"
            + "\n\nUnit test using python client:"
            + "\n - Windows and Unix OS commands: ant -f build.xml example-client-python"
            + "\n\n";

    public static void main(String[] args) throws Exception {
        //Configuration.load("local.properties");
        //VisualIndexer.init();
        // Local topology cluster
        LocalCluster clusterLocalTopology;

        // Topology builder
        TopologyBuilder builder; // OK

        // Storm Spouts
        IRichSpout stormExampleSocialMediaAMQPSpout; // OK
        SpoutDeclarer spoutDeclarer; // OK

        // Storm bolts
        BoltDeclarer boltDeclarer;
        CerthVisualBolt visualBolt;
        ExampleSocialMediaJavaPrinterBolt exampleSocialMediaJavaPrinterBolt;

        // Customer configuration
        ConsumerConfig stormSocialMediaSpoutConfig = null; // OK

        // Customer configuration builder
        ConsumerConfigBuilder stormSocialMediaSpoutConfigBuilder = null; // OK

        // RabbitMQ Connection Configuration
        ConnectionConfig stormSocialMediaSpoutRabbitMQconnectionConfig = null;  // OK

        // Simple Storm tuple JSON Scheme
        SimpleJSONScheme socialMediaScheme = null; // OK

        // Storm RabbitMQ queue declarator
        Declarator declarator;

        // Java Storm runner logger
        Logger logger = null;

        // Logging configuration
        String strLogBaseDir = null;
        String strLogPatternJava = null;
        String strLogLevel = null;
        Level logLevel = null;

        // Management parameters (mainly process id)
        String strPID = null;

        // RabbitMQ configuration file variables
        String strRMQHost = null;
        int nRMQPort = 0;
        String strRMQUsername = null;
        String strRMQPassword = null;
        int nRMQHeartBeaat = 0;
        String strRMQQueueName = null;
        String strRMQExchange = null;
        String strRMQExchangeType = null;
        String strRMQRouting = null;

        // Storm Topology, Spout and Bolts IDs variables
        String strExampleSocialMediaAMQPSpoutId = null;
        String strExampleSocialMediaPrinterBoltId = null;
        String strIndexingBoltId = null;
        String strExampleSocialMediaClientFrameworkStreamId = null;
        String strExampleEmitFieldsId = null;

        // Storm Topology configuration parameters
        boolean bTopologyDebug = false;

        // Storm Spout configuration parameters
        boolean bSpoutDebug = false;
        int nRabbitMQPrefetch = 0;
        int nMaxSpoutPending = 0;

        // Visual indexing parameters
        String visualLearningFiles = null;
        String visualServiceHost = null;

        // Main Storm Social Media Properties file
        File fileConfigFile = null;

        String assessmentId = null;
        String boltType = null;
        double threshold = 0;

        // Create Properties builder object (e.g. storm properties file should be passed as a command line argument)
        Properties properties = new Properties();

        String strStormClusterMode = null;

        // Parse all configuration command line arguments
        try {
            // First of all get the length of command line arguments
            int nArgsLength = args.length;

			/* First of all need to check the number of command line arguments (minimum number of arguments should be 4) e.g.
             * -config configuration_file.ini and -mode local/distributed (total count of the arguments is 4)
			 */
            if (nArgsLength < 6) {
                throw new IllegalArgumentException("Some of the configuration command line arguments were invalid or were not specified. Please refer to the Storm help menu.");
            }

			/* If Storm mode argument was specified, then check if local or distributed mode was requested
             * 	- First of all need to check if the command line argument contained "=" character (e.g. mode=local),
			 * 	- Secondly need to check if a valid mode was specified. Valid modes are "local" or "distributed" 
			 */
            int argsLenght = args.length;
            String[] arguments = new String[argsLenght];

            boolean bModeArgument = false;
            boolean assessmentArgument = false;
            boolean typeArgument = false;

            for (int i = 0; i < arguments.length; i++) {

                // If the mode (e.g. -mode) command line arguments was specified
                if (args[i].equals("-mode")) {
                    // Set boolean flag indicating that the "-mode" command line argument was specified
                    bModeArgument = true;

					/* Check the length of the mode (minimum length is 5, e.g. local), as well as check if the mode description string 
                     * equals to either "local" or "distributed"
					 */
                    String strTempMode = args[++i];

                    // Check if the length of the mode is minimum 5 (e.g. local)
                    if (strTempMode.length() >= 5) {
                        // Finally check if the mode is equal to "local" or "distributed"
                        if (strTempMode.toLowerCase().equals("local") || strTempMode.toLowerCase().equals("distributed")) {
                            strStormClusterMode = strTempMode;

                            // If mode does not match "local" or "distributed" then raise an Exception
                        } else {
                            throw new IllegalArgumentException("Storm cluster mode is invalid. Valid Storm modes are local or distributed (e.g. -mode local).");
                        }

                        // Raise an exception if the length of mode is less that 5 and is the mode string contains any special characters
                    } else {
                        throw new IllegalArgumentException("Storm cluster mode is invalid or was not specified. Please refer to general Storm help instructions.");
                    }
                }
                if (args[i].equals("-assessmentid")) {

                    assessmentArgument = true;
                    assessmentId = args[++i];

                    if (assessmentId.length() <= 5) {
                        throw new IllegalArgumentException("The assessment id has to be longer than 5 chars");
                    }
                }
                if (args[i].equals("-type")) {
                    typeArgument = true;
                    boltType = args[++i];
                }
                if (args[i].equals("-threshold")) {
                    threshold = Double.parseDouble(args[++i]);
                }
            }

            if (bModeArgument == false || assessmentArgument == false || typeArgument == false) {
                throw new IllegalArgumentException("Main Storm mode was not specified. Please refer to general Storm help instructions.");
            }

            // Create Java properties file from the passed configuration file
            properties.load(ExampleJavaSocialMediaStormTopologyRunner.class.getClassLoader()
                    .getResourceAsStream("storm_config.ini"));


        } catch (IOException e) {
            // Print error message, stacktrace and exit
            System.err.printf(e.getMessage());
            e.printStackTrace();
            System.out.println(strStormRunInstructions);
            System.exit(1);

        } catch (Exception e) {
            // Print error message, stacktrace and exit
            System.err.printf("Exception occurred during configuration file loading. "
                    + "\n\nDetails: %s.", e.getMessage()
                    + strStormRunInstructions);
            e.printStackTrace();
            System.exit(1);
        }

        // Get all the needed RabbitMQ connection properties from the configuration file
        try {
            strRMQHost = properties.getProperty("rmqhost", "localhost");
            nRMQPort = Integer.parseInt(properties.getProperty("rmqport", "5672"));
            strRMQUsername = properties.getProperty("rmqusername", "guest");
            strRMQPassword = properties.getProperty("rmqpassword");
            nRMQHeartBeaat = Integer.parseInt(properties.getProperty("rmqheartbeat", "10"));
            strRMQQueueName = properties.getProperty("rmqqueuename", "test");
            strRMQExchange = properties.getProperty("rmqexchange", "test-exchange");
            strRMQExchangeType = properties.getProperty("rmqexchangetype", "topic");
            strRMQRouting = properties.getProperty("rmqrouting", "test-routing");

            // Get all the needed Storm Topology, Spout and Bolts IDs from the configuration file
            strExampleSocialMediaAMQPSpoutId = properties.getProperty("example_spout_amqp_spout_id", "exampleSocialMediaAMQPSpout");
            strExampleSocialMediaPrinterBoltId = properties.getProperty("example_bolt_java_printer_bolt_id", "exampleJavaPrinterBolt");
            strIndexingBoltId = properties.getProperty("indexing_bolt_id", "indexingBolt");
            strExampleSocialMediaClientFrameworkStreamId = properties.getProperty("certh_topology_id", "certhTopology");
            strExampleEmitFieldsId = properties.getProperty("example_emit_fields_id", "word");

            // Get logging configuration
            strLogBaseDir = properties.getProperty("logging_dir");
            strLogPatternJava = properties.getProperty("logging_pattern_java", "%5p %d{yyyy-MM-dd HH:mm:ss,sss} %file %t %L: %m%n");
            strLogLevel = properties.getProperty("logging_level", "debug");

            // Get Storm Topology configuration parameters
            bTopologyDebug = Boolean.valueOf(properties.getProperty("topology_debug", "false"));

            // Get Storm Spout configuration parameters
            bSpoutDebug = Boolean.valueOf(properties.getProperty("spout_debug", "false"));
            nRabbitMQPrefetch = Integer.parseInt(properties.getProperty("spout_rmqprefetch", "200"));
            nMaxSpoutPending = Integer.parseInt(properties.getProperty("spout_max_spout_pending", "200"));

            visualLearningFiles = properties.getProperty("visual_learning_files");
            visualServiceHost = properties.getProperty("visual_service_host", "127.0.0.1");

        } catch (Exception e) {
            // Print error message, stacktrace and exit
            System.err.printf("Error occurred during main STORM configuration file parsing. Please refer to the instructions in provided in the configuration file.\nDetails: %s.", e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }

        // Try to setup main Java Storm Topology runner logger
        try {
            StormLoggingHelper stormLoggingHelper = new StormLoggingHelper();

			/* Check log level that was specified in the main example Storm configuration file, and based on that specify logging level to StormLoggingHelper
             * Available log levels are: all, trace, debug, info, warn, error and off (not used here)
			 */
            if (strLogLevel.toLowerCase().equals("all")) {
                logLevel = Level.ALL;
            } else if (strLogLevel.toLowerCase().equals("trace")) {
                logLevel = Level.TRACE;
            } else if (strLogLevel.toLowerCase().equals("debug")) {
                logLevel = Level.DEBUG;
            } else if (strLogLevel.toLowerCase().equals("error")) {
                logLevel = Level.ERROR;
            } else if (strLogLevel.toLowerCase().equals("warn")) {
                logLevel = Level.WARN;
            } else if (strLogLevel.toLowerCase().equals("info")) {
                logLevel = Level.INFO;
            } else {
                logLevel = Level.OFF;
            }

            // Create log file name - combination of class name and current thread id, e.g. ExampleJavaSocialMediaStormTopologyRunner_pid123.log
            // First of all need to fetch process id using java.lang.ManagementFactory class, returned value will be in the format of {p_id}@{host_name}
            try {
                // Try to get the pid using java.lang.Management class and split it on @ symbol (e.g. returned value will be in the format of {p_id}@{host_name})
                strPID = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];

                // Handle any possible exception here, such as if the process_name@host will not be returned (possible, depends on different JVMs)
            } catch (Exception e) {
                // Print the message, stacktrace and allow to continue (pid value will not be contained in the log file)
                System.err.println("Failed to get or process process id. Storm will continue, but the log files names will not contain pid value. Details: " + e.getMessage());
                e.printStackTrace();

                // Pid will be simply an empty value
                strPID = "";
            }

            // Create log file name - combination of class name and current thread id, e.g. ExampleJavaSocialMediaStormTopologyRunner_pid123.log
            String strLogName = "CerthTopologyRunner_pid" + strPID + ".log";

            // Specify the path to the log file (the file that will be created)
            String fileSep = System.getProperty("file.separator");
            String strLogFilePath = strLogBaseDir + fileSep + strLogName;

            // Create logger
            logger = stormLoggingHelper.createLogger(ExampleJavaSocialMediaStormTopologyRunner.class.getName(),
                    strLogFilePath, strLogPatternJava, logLevel);

            // Try to issue initial log entry
            logger.info("Java example Storm Topology logger is initialised.");

        } catch (Exception e) {
            // Print error message, stacktrace and exit
            System.err.printf("Exception occurred during Java Storm runner logger setup. Details: %s.\n", e.getMessage());
            e.printStackTrace();
            System.err.println("Allowing to continue without main Java Storm runner logger setup!");
        }

        // Create SocialMediaSpout Configuration Builder, create Topology builder, set spouts/bolts and start execute the topology
        try {
            // Create Storm object Scheme (default encoding is utf-8, but others can be passed to the constructor).
            /* API: http://code.rapportive.com/storm-json/doc/com/rapportive/storm/scheme/SimpleJSONScheme.html
             *
			 */
            socialMediaScheme = new SimpleJSONScheme();

			/* Create RabbitMQ connection configuration
             * Documentation (no API, just an example of usage): https://github.com/ppat/storm-rabbitmq/blob/master/README.md (search for "RabbitMQ Spout")
			 */
            stormSocialMediaSpoutRabbitMQconnectionConfig = new ConnectionConfig(strRMQHost, nRMQPort, strRMQUsername, strRMQPassword,
                    ConnectionFactory.DEFAULT_VHOST, nRMQHeartBeaat);
            logger.info("Initialised RabbitMQ connection configuration object.");
			
			/* Create Storm Spout configuration builder
			 * Documentation (no API, just an example of usage): https://github.com/ppat/storm-rabbitmq/blob/master/README.md (search for "RabbitMQ Spout")
			 */
            stormSocialMediaSpoutConfigBuilder = new ConsumerConfigBuilder();
            stormSocialMediaSpoutConfigBuilder.connection(stormSocialMediaSpoutRabbitMQconnectionConfig);
            stormSocialMediaSpoutConfigBuilder.queue(strRMQQueueName);
            stormSocialMediaSpoutConfigBuilder.prefetch(nRabbitMQPrefetch);
            stormSocialMediaSpoutConfigBuilder.requeueOnFail();
            logger.info("Initialised Spout configuration builder.");
			
			/* Build Storm spout configuration
			 * Documentation (no API, just an example of usage): https://github.com/ppat/storm-rabbitmq/blob/master/README.md (search for "RabbitMQ Spout")
			 */
            stormSocialMediaSpoutConfig = stormSocialMediaSpoutConfigBuilder.build();
            logger.info("Initialised Spout configuration builder.");
			
			/* Create a AMQP Declarator (will declare queue if it does not exist on the time of the Storm launch)
			 * Documentation (no API, just an example of usage): https://github.com/ppat/storm-rabbitmq/blob/master/README.md (search for "Declarator")
			 */
            declarator = new ExampleSocialMediaStormDeclarator(strRMQExchange, strRMQExchangeType, strRMQRouting, strRMQQueueName);
			
			/* Initialise Social Media Spout
			 * API: http://nathanmarz.github.io/storm/doc-0.8.1/index.html (search for "IRichSpout")
			 */
            stormExampleSocialMediaAMQPSpout = new ExampleSocialMediaAMQPSpout(socialMediaScheme, declarator);
            logger.info("Initialised AMQP Spout object.");
			
			/* Create a simple STORM topology configuration file
			 * Documentation (no API, just an example of usage): https://github.com/ppat/storm-rabbitmq/blob/master/README.md (search for "Config")
			 */

            Config conf = new Config();
            conf.put(Config.TOPOLOGY_DEBUG, bTopologyDebug);
            conf.setDebug(bTopologyDebug);
            logger.info("Initialised main example Storm confuration.");
			
			/* Initialise Storm Topology
			 * API: http://nathanmarz.github.io/storm/doc-0.8.1/index.html (search for "TopologyBuilder")
			 */
            builder = new TopologyBuilder();
			
			/* Define a new Spout in the topology
			 * API: http://nathanmarz.github.io/storm/doc-0.8.1/index.html (search for "SpoutDeclarer")
			 */
            spoutDeclarer = builder.setSpout(strExampleSocialMediaAMQPSpoutId, stormExampleSocialMediaAMQPSpout);
            logger.info("Declared AMQP Spout to the example Storm topology.");

            // Add configuration to the StoputDeclarer
            spoutDeclarer.addConfigurations(stormSocialMediaSpoutConfig.asMap());
			
			/* Explanation taken from: https://github.com/ppat/storm-rabbitmq
			 * Set MaxSpoutPending value to the same value as RabbitMQ pre-fetch count (set initially in in the ConsumerConfig above). It is possible
			 * to tune them later separately, but MaxSpoutPending should always be <= Prefetch 
			 */
            spoutDeclarer.setMaxSpoutPending(nMaxSpoutPending);
            spoutDeclarer.setDebug(bSpoutDebug);

            if ("sim".equals(boltType)) {
                if (threshold <= 0)
                    threshold = 0.9;
                visualBolt = new CerthSimilarityBolt(strExampleEmitFieldsId, assessmentId, visualLearningFiles, visualServiceHost, strLogBaseDir, strLogPatternJava, logLevel, threshold);
            } else
                // Set Java Logger. At the moment the Bolt has one worker only
                visualBolt = new CerthIndexingBolt(strExampleEmitFieldsId, assessmentId, visualLearningFiles, visualServiceHost, strLogBaseDir, strLogPatternJava, logLevel);

			/* Define bolt declarer
			 * API: http://nathanmarz.github.io/storm/doc-0.8.1/index.html (search for "BoltDeclarer")
			 */
            boltDeclarer = builder.setBolt(strIndexingBoltId, visualBolt);
            boltDeclarer.shuffleGrouping(strExampleSocialMediaAMQPSpoutId);
            logger.info("Declared Indexing Bolt to the example Storm topology.");

            // Set Java Printer bolt. At the moment the Bolt has one worker only
            exampleSocialMediaJavaPrinterBolt = new ExampleSocialMediaJavaPrinterBolt();

            // Declare fields grouping - simply saying that input should be received from the Bolt with ID=strExampleSocialMediaLoggerBoltId (e.g. Python Logger Bolt in this case)
            boltDeclarer = builder.setBolt(strExampleSocialMediaPrinterBoltId, exampleSocialMediaJavaPrinterBolt);
            boltDeclarer.fieldsGrouping(strIndexingBoltId, new Fields(strExampleEmitFieldsId));
            logger.info("Declared Printer Bolt to the example Storm topology.");

            // Check configuration boolean value "bLocalTopology" and decide whether to start Local Topology cluster or submit the Topology to the distributed cluster
            if (strStormClusterMode.equals("local")) {
                // Deploy the topology on the Local Cluster (e.g. local mode)
                clusterLocalTopology = new LocalCluster();
                clusterLocalTopology.submitTopology(strExampleSocialMediaClientFrameworkStreamId, conf, builder.createTopology());

            } else if (strStormClusterMode.equals("distributed")) {
                // Submit the topology to the distribution cluster that will be defined in Storm client configuration file or via cmd as a parameter ( e.g. nimbus.host=localhost )
                strExampleSocialMediaClientFrameworkStreamId = assessmentId;
                StormSubmitter.submitTopology(strExampleSocialMediaClientFrameworkStreamId, conf, builder.createTopology());
                logger.info("Submitted the example Storm topology.");

            } else {
                throw new RuntimeException("Unknown Storm mode was specified. Valid modes are local or distributed, which should be specified as cmd argument. "
                        + "Please refer to general Storm help instructions.");
            }

        } catch (Exception e) {
            // Print error message, stacktrace and exit
            System.err.printf("Exception occurred during Storm topology start. Details: %s.\n", e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

}
