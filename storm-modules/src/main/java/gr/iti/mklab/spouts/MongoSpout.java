package gr.iti.mklab.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.CompletableSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import gr.iti.mklab.conf.FieldNames;
import gr.iti.mklab.simmo.items.Image;
import gr.iti.mklab.simmo.morphia.MediaDAO;
import gr.iti.mklab.simmo.morphia.MorphiaManager;

import java.util.Iterator;
import java.util.Map;

import static backtype.storm.utils.Utils.tuple;

/**
 * A simple mongo spout, primarily for testing
 *
 * @author kandreadou
 */
public class MongoSpout extends BaseRichSpout implements CompletableSpout {

    private final String DB_NAME;
    private final int NUM_ITEMS;
    private Iterator<Image> it;
    private SpoutOutputCollector collector;

    public MongoSpout(String host, String dbname, int numItems) {
        DB_NAME = dbname;
        NUM_ITEMS = numItems;
        MorphiaManager.setup(host);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(FieldNames.IMAGE));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        MediaDAO<Image> dao = new MediaDAO<>(Image.class, DB_NAME);
        it = dao.getItems(NUM_ITEMS, 0).iterator();
    }

    @Override
    public void nextTuple() {
        if (it.hasNext())
            collector.emit(tuple(it.next().getUrl()));
        else
            this.close();
    }

    @Override
    public void close() {
        MorphiaManager.tearDown();
        super.close();
    }

    @Override
    public Object startup() {
        return null;
    }

    @Override
    public Object cleanup() {
        return null;
    }

    @Override
    public Object exhausted_QMARK_() {
        return null;
    }
}
