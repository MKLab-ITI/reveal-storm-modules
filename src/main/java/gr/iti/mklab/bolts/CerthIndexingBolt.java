package gr.iti.mklab.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import gr.iti.mklab.visual.VisualIndexer;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by kandreadou on 3/31/15.
 */
public class CerthIndexingBolt extends BaseRichBolt {

    private OutputCollector collector;
    private String strExampleEmitFieldsId;

    private VisualIndexer indexer;
    private String ASSESSMENT_ID;
    private String LEARNING_FOLDER;
    private String SERVICE_HOST;

    /**
     * Main Java Logger Bolt constructor
     *
     * @param strExampleEmitFieldsId id of the fields that will be emited by this bolt
     * @throws Exception throws exception if log file name and/or emit field id are empty, as well as if the log level is not an instance of ch.qos.logback.classic.Level
     */
    public CerthIndexingBolt(String strExampleEmitFieldsId, String assessmentId, String learningFolder, String serviceHost) throws Exception {
        super();

        System.out.println("### CREATING CERTH INDEXING BOLT for asssesment id "+ASSESSMENT_ID+ " and configuration "+learningFolder+" "+serviceHost);
        // Store emit fields name, ExampleSocialMediaJavaLoggerBolt id and path to the main configuration file
        if (strExampleEmitFieldsId.isEmpty()) {
            throw new Exception("Emit fields id can not be nil or empty.");
        }

        // After all the above checks complete, store the emit field id, path (or name) of the log file and log level
        this.strExampleEmitFieldsId = strExampleEmitFieldsId;
        this.ASSESSMENT_ID = assessmentId;
        this.LEARNING_FOLDER = learningFolder;
        this.SERVICE_HOST = serviceHost;
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

        this.collector = collector;

        try {
            VisualIndexer.init(LEARNING_FOLDER, SERVICE_HOST);
            indexer = new VisualIndexer(ASSESSMENT_ID);
        } catch (Exception e) {
            e.printStackTrace();
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

            // Finally log UTF-8 JSON message to disk to verify its all OK
            System.out.println(CerthIndexingBolt.class.getName() + " JSON received = " + jsonObject.toString());

            long id = (long) jsonObject.get("id");

            JSONArray media = (JSONArray) ((JSONObject) jsonObject.get("entities")).get("media");

            boolean indexed = false;

            for (int i = 0; i < media.size(); i++) {
                JSONObject object = (JSONObject) media.get(i);
                String url = (String) object.get("media_url");
                System.out.println(CerthIndexingBolt.class.getName() + " Index item " + id + " with url " + url);
                indexed = indexer.index(url, String.valueOf(id));
                System.out.println(CerthIndexingBolt.class.getName() + " Item " + id + " indexed " + indexed);
            }
            jsonObject.put("certh:vIndexed", String.valueOf(indexed));

            // Emit a received message
            this.collector.emit(new Values(jsonObject.toString()));

            // Acknowledge the collector that we actually received the input
            this.collector.ack(input);

        } catch (Exception e) {
            System.out.println(CerthIndexingBolt.class.getName() + " Failed to index. Details: " + e.getMessage());
            e.printStackTrace();
            try {
                throw new Exception("Failed to index. Details: " + e.getMessage());
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