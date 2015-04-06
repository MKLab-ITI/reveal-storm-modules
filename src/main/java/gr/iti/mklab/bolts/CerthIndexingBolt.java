package gr.iti.mklab.bolts;

import ch.qos.logback.classic.Level;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;


/**
 * Certh Indexing Bolt
 * Annotates the input json object with the field "certh:vIndexed", which
 * is true if the corresponding image has been successfuly indexed
 *
 * @author kandreadou
 */
public class CerthIndexingBolt extends CerthVisualBolt {


    private static final long serialVersionUID = -4738590257653128293L;

    /**
     * Certh Indexing Bolt constructor
     *
     * @param strExampleEmitFieldsId id of the fields that will be emited by this bolt
     * @param assessmentId
     * @param learningFolder
     * @param serviceHost
     * @param strLogBaseDir
     * @param strLogPattern
     * @param logLevel
     * @throws java.lang.Exception throws exception if log file name and/or emit field id are empty, as well as if the log level is not an instance of ch.qos.logback.classic.Level
     */
    public CerthIndexingBolt(String strExampleEmitFieldsId, String assessmentId, String learningFolder, String serviceHost, String strLogBaseDir, String strLogPattern, Level logLevel) throws Exception {
        super(strExampleEmitFieldsId, assessmentId, learningFolder, serviceHost, strLogBaseDir, strLogPattern, logLevel);
    }

    @Override
    public void processJSONObject(JSONObject jsonObject) {
        JSONArray media = (JSONArray) ((JSONObject) jsonObject.get("entities")).get("media");

        for (int i = 0; i < media.size(); i++) {
            JSONObject object = (JSONObject) media.get(i);
            String url = (String) object.get("media_url");
            long id = (long) jsonObject.get("id");
            this.logger.info(" Index item " + id + " with url " + url);
            boolean indexed = indexer.index(url, String.valueOf(id));
            this.logger.info(" Item " + id + " indexed " + indexed);
            object.put("certh:vIndexed", String.valueOf(indexed));
        }
    }

    @Override
    public String getLoggerName() {
        return "IndexingBolt";
    }
}