package gr.iti.mklab.bolts;

import ch.qos.logback.classic.Level;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import gr.iti.mklab.framework.client.search.visual.JsonResultSet;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.List;

/**
 * Certh Similarity Bolt
 * Annotates the input json object with the field "certh:nearDuplicates"
 * which corresponds to an array of visual neighbours
 *
 * @author kandreadou
 */
public class CerthSimilarityBolt extends CerthVisualBolt {

    private final double THRESHOLD;

    /**
     * Certh Similarity Bolt constructor
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
    public CerthSimilarityBolt(String strExampleEmitFieldsId, String assessmentId, String learningFolder, String serviceHost, String strLogBaseDir, String strLogPattern, Level logLevel, double threshold) throws Exception {
        super(strExampleEmitFieldsId, assessmentId, learningFolder, serviceHost, strLogBaseDir, strLogPattern, logLevel);
        THRESHOLD = threshold;
    }

    @Override
    public void processJSONObject(JSONObject jsonObject) {
        JSONArray media = (JSONArray) ((JSONObject) jsonObject.get("entities")).get("media");

        for (int i = 0; i < media.size(); i++) {
            JSONObject object = (JSONObject) media.get(i);
            String url = (String) object.get("media_url");
            long id = (long) jsonObject.get("id");
            this.logger.info(" Find similar to item " + id + " with url " + url);
            List<JsonResultSet.JsonResult> results = indexer.findSimilar(url, THRESHOLD);
            this.logger.info("Image " + id + "has " + results.size() + " neighbors");
            JsonArray array = new JsonArray();
            results.stream().forEach(r -> {
                JsonObject o = new JsonObject();
                o.addProperty("id", r.getId());
                o.addProperty("rank", r.getRank());
                o.addProperty("score", r.getScore());
                array.add(o);
            });
            object.put("certh:nearDuplicates", array);
        }
    }

    @Override
    public String getLoggerName() {
        return "SimilarityBolt";
    }
}
