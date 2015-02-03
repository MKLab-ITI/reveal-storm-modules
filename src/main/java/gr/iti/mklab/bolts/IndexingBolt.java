package gr.iti.mklab.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import gr.iti.mklab.visual.IndexingController;

import java.util.Map;

/**
 * The indexing bolt
 *
 * @author kandreadou
 */
public class IndexingBolt extends BaseRichBolt {

    private OutputCollector outputCollector;

    public IndexingBolt() {
        IndexingController.initialize();
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String url = "";
        String collection = "";
        boolean indexed = IndexingController.indexImage(url, collection);
        //outputCollector.emit((Object)indexed);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("MediaItem", "ImageVector", "nearestMediaItem"));
    }
}
