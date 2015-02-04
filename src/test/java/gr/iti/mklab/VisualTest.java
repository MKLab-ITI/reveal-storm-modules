package gr.iti.mklab;

import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.Testing;
import backtype.storm.generated.StormTopology;
import backtype.storm.testing.*;
import backtype.storm.topology.TopologyBuilder;
import gr.iti.mklab.bolts.IndexingBolt;
import gr.iti.mklab.spouts.MongoSpout;
import junit.framework.TestCase;

import java.util.Map;

/**
 * A visual test case
 */
public class VisualTest extends TestCase {

    public void testIndexingTopology() {
        MkClusterParam mkClusterParam = new MkClusterParam();
        mkClusterParam.setSupervisors(4);
        Config daemonConf = new Config();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
        mkClusterParam.setDaemonConf(daemonConf);

        Testing.withSimulatedTimeLocalCluster(mkClusterParam, (ILocalCluster cluster) -> {

            // build the test topology
            TopologyBuilder builder = new TopologyBuilder();
            builder.setSpout("MongoSpout", new MongoSpout("127.0.0.1", "test", 1000));
            builder.setBolt("IndexingBolt", new IndexingBolt());

            StormTopology topology = builder.createTopology();


            Config conf = new Config();
            conf.setNumWorkers(2);
            CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
            completeTopologyParam.setStormConf(conf);
            completeTopologyParam.setMockedSources(new MockedSources());

            Map result = Testing.completeTopology(cluster, topology,
                    completeTopologyParam);

        });
    }
}
