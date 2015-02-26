package gr.iti.mklab.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import gr.iti.mklab.conf.FieldNames;
import gr.iti.mklab.visual.VisualIndexer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static backtype.storm.utils.Utils.tuple;

import java.util.Map;

/**
 * The indexing bolt
 *
 * @author kandreadou
 */
public class IndexingBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1855244583199329297L;
    private static Logger _logger = LoggerFactory.getLogger(IndexingBolt.class);
    private OutputCollector outputCollector;
    private VisualIndexer indexer;
    private String name;

    public IndexingBolt(String name) {
        this.name = name;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.outputCollector = outputCollector;
        try {
            indexer = new VisualIndexer(name);
        } catch (Exception e) {
            _logger.error("Problem creating indexing bolt "+e);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        String imgUrl = (String) tuple.getValueByField(FieldNames.IMAGE);
        boolean indexed = indexer.index(imgUrl, imgUrl);
        System.out.println("Image " + imgUrl + "has been indexed " + indexed);
        outputCollector.emit(tuple(indexed));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(FieldNames.INDEXED));
    }
}
