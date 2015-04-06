package gr.iti.mklab.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import gr.iti.mklab.conf.FieldNames;
import gr.iti.mklab.visual.VisualIndexer;
import org.json.simple.JSONObject;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static backtype.storm.utils.Utils.tuple;

/**
 * An abstract bolt with visual indexing and search capabilities
 *
 * @author kandreadou
 * @deprecated
 */
public abstract class AbstractVisualBolt extends BaseRichBolt {

    private OutputCollector outputCollector;
    private String name;
    protected VisualIndexer indexer;

    public AbstractVisualBolt(String name) {
        this.name = name;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.outputCollector = outputCollector;
        try {
            //indexer = new VisualIndexer(name);
        } catch (Exception e) {
            getLogger().error("Problem creating indexing bolt " + e);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        JsonObject jObject = new JsonParser().parse((String) tuple.getValueByField(FieldNames.PREPARED_ITINNO_JSON)).getAsJsonObject();
        JsonObject mediaItem = jObject.get("entities").getAsJsonObject().get("media").getAsJsonArray().get(0).getAsJsonObject();
        process(jObject, mediaItem.get("id").getAsString(), mediaItem.get("media_url").getAsString());
        outputCollector.emit(tuple(jObject.toString()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(FieldNames.PREPARED_CERTH_JSON));
    }

    protected abstract void process(JsonObject object, String id, String url);

    protected abstract Logger getLogger();

}
