package com.gecko.topo.spouts;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by hlieu on 4/21/16.
 */
public class FailureSpout extends BaseRichSpout {

    private SpoutOutputCollector spoutCollector;
    private Long transactionCount;

    @Override
    public void ack(Object msgId) {
        System.out.println("Tuple ack: " + msgId);
    }

    public void fail(Object msgId) {
        System.out.println("Tuple failed: " + msgId);
    }

    @Override
    public void nextTuple(){
        for(int i = 0; i < transactionCount; i++) {
            spoutCollector.emit(new Values(i), i);
        }
    }

    @Override
    public void open(Map config, TopologyContext context, SpoutOutputCollector outputCollector) {
        this.transactionCount = (Long) config.get("num-transactions");
        this.spoutCollector = outputCollector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("count"));
    }

}
