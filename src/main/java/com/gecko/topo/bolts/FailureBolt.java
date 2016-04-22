package com.gecko.topo.bolts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * Created by hlieu on 4/21/16.
 */
public class FailureBolt implements BaseBasicBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("failure"));
    }
}
