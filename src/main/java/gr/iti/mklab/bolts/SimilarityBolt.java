package gr.iti.mklab.bolts;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import gr.iti.mklab.framework.client.search.visual.JsonResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * The similarity bolt
 *
 * @author kandreadou
 */
public class SimilarityBolt extends AbstractVisualBolt {

    private static Logger _logger = LoggerFactory.getLogger(SimilarityBolt.class);
    private static final long serialVersionUID = 1855244583199329297L;
    private double similarityThreshold;

    public SimilarityBolt(String name, double threshold) {
        super(name);
        this.similarityThreshold = threshold;
    }

    @Override
    protected void process(JsonObject object, String id, String url) {
        _logger.info("Searching for image with id " + id + " and url " + url);
        List<JsonResultSet.JsonResult> results = indexer.findSimilar(url, similarityThreshold);
        _logger.info("Image " + id + "has "+results.size()+" neighbors");
        _logger.info(results.get(0).getId()+" score "+results.get(0).getScore());
        JsonArray array = new JsonArray();
        results.stream().forEach(r->{
            JsonObject o = new JsonObject();
            o.addProperty("id",r.getId());
            o.addProperty("rank",r.getRank());
            o.addProperty("score",r.getScore());
            array.add(o);
        });
        object.add("certh:nearDuplicates", array);
    }

    @Override
    protected Logger getLogger() {
        return _logger;
    }
}