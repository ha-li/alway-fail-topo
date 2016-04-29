package com.gecko.topo;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import com.gecko.topo.bolts.FailureBolt;
import com.gecko.topo.spouts.FailureSpout;

/**
 * Created by hlieu on 4/21/16.
 */
public class FailTopology {
    public static void main(String[] args) throws InterruptedException {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("count-spout", new FailureSpout());//.setMaxTaskParallelism(4);
        builder.setBolt("fail-bolt", new FailureBolt()).shuffleGrouping("count-spout");

        Config config = new Config();
        config.setDebug(true);
        config.setMaxSpoutPending(1);
        int num_transactions = Integer.valueOf(args[0]);
        config.put("num-transactions", num_transactions);


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Always-Fail-Topology", config, builder.createTopology());
        Thread.sleep(1000);
        cluster.shutdown();
    }
}
