package gr.iti.mklab;

import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.Testing;
import backtype.storm.generated.StormTopology;
import backtype.storm.testing.*;
import backtype.storm.topology.TopologyBuilder;
import gr.iti.mklab.bolts.IndexingBolt;
import gr.iti.mklab.bolts.PrintingBolt;
import gr.iti.mklab.bolts.SimilarityBolt;
import gr.iti.mklab.conf.Configuration;
import gr.iti.mklab.spouts.JsonSpout;
import gr.iti.mklab.spouts.MongoSpout;
import gr.iti.mklab.visual.VisualIndexer;
import junit.framework.TestCase;

import java.io.InputStream;
import java.util.Map;

/**
 * A visual test case
 */
public class VisualTest extends TestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Configuration.load("local.properties");
        //VisualIndexer.init();
    }

    //The test will fail because of a timeout. It takes longer than the hard-coded timeout of 5000ms
    public void testIndexingTopology() throws Exception {

        MkClusterParam mkClusterParam = new MkClusterParam();
        mkClusterParam.setSupervisors(4);
        Config daemonConf = new Config();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
        mkClusterParam.setDaemonConf(daemonConf);

        Testing.withLocalCluster(mkClusterParam, (ILocalCluster cluster) -> {

            StormTopology topology = getIndexingBuilder().createTopology();

            Config conf = new Config();
            conf.setNumWorkers(2);
            CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
            completeTopologyParam.setStormConf(conf);
            completeTopologyParam.setMockedSources(new MockedSources());

            Map result = Testing.completeTopology(cluster, topology,
                    completeTopologyParam);

        });
    }

    private TopologyBuilder getIndexingBuilder(){
        TopologyBuilder builder = new TopologyBuilder();
        //builder.setSpout("MongoSpout", new MongoSpout(Configuration.MONGO_HOST, "wtf5wtf", 100));
        builder.setSpout("JsonSpout", new JsonSpout());
        builder.setBolt("IndexingBolt", new IndexingBolt("newcol2")).shuffleGrouping("JsonSpout");
        builder.setBolt("PrintingBolt", new PrintingBolt("/home/kandreadou/Pictures/")).shuffleGrouping("IndexingBolt");
        return builder;
    }

    private TopologyBuilder getSimilarityBuilder(){
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("JsonSpout", new JsonSpout());
        builder.setBolt("SimilarityBolt", new SimilarityBolt("newcol", 0.95)).shuffleGrouping("JsonSpout");
        builder.setBolt("PrintingBolt", new PrintingBolt("/home/kandreadou/Pictures/")).shuffleGrouping("SimilarityBolt");
        return builder;
    }
}
