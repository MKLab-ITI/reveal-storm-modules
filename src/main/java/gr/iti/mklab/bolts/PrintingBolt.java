package gr.iti.mklab.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import gr.iti.mklab.conf.FieldNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.Map;

/**
 * A simple printing bolt
 *
 * @author kandreadou
 * @deprecated
 */
public class PrintingBolt extends BaseRichBolt {

    private static final long serialVersionUID = 6662373229449004393L;
    private static Logger _logger = LoggerFactory.getLogger(PrintingBolt.class);
    private final String dirName;

    public PrintingBolt(String dirName) {
        this.dirName = dirName;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple tuple) {
        String json = (String) tuple.getValueByField(FieldNames.PREPARED_CERTH_JSON);
        try (PrintWriter writer = new PrintWriter(dirName + System.currentTimeMillis() + ".txt", "UTF-8")) {
            writer.println(json);
            writer.close();
        } catch (Exception e) {
            _logger.error("Error writing to file ", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
