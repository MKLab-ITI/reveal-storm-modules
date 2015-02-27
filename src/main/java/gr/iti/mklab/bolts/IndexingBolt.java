package gr.iti.mklab.bolts;

import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The indexing bolt
 *
 * @author kandreadou
 */
public class IndexingBolt extends AbstractVisualBolt {

    private static Logger _logger = LoggerFactory.getLogger(IndexingBolt.class);
    private static final long serialVersionUID = 1855244583199329297L;

    public IndexingBolt(String name) {
        super(name);
    }

    @Override
    protected void process(JsonObject object, String id, String url) {
        _logger.info("Indexing image with id " + id + " and url " + url);
        boolean indexed = indexer.index(url, id);
        _logger.info("Image " + id + "has been indexed " + indexed);
        object.addProperty("certh:vIndexed", true);
    }

    @Override
    protected Logger getLogger() {
        return _logger;
    }
}
