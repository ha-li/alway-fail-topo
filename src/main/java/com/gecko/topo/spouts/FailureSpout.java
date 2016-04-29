package com.gecko.topo.spouts;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by hlieu on 4/21/16.
 */
public class FailureSpout extends BaseRichSpout {

    private SpoutOutputCollector spoutCollector;
    private Long transactionCount;
    private Map<Long, Long> errorCount;
    private List<Long> messages;
    private static int MAX_FAILURE = 3;


    @Override
    public void ack(Object msgId) {
        System.out.println("Tuple ack: " + msgId);
    }

    public void fail(Object msgId) {
        System.out.println("Tuple failed: " + msgId);
        Long id = (Long) msgId;
        if(errorCount.containsKey(id)) {
            Long newCount = errorCount.get(msgId) + 1;
            System.out.println("Tuple has failed " + newCount + " times.");
            if(newCount >= MAX_FAILURE) {
                throw new RuntimeException("Too many failures");
            }
        } else {
            System.out.println("First time tuple has failed. ");
            errorCount.put(id, Long.valueOf(1));
        }

        messages.add(id);
    }

    @Override
    public void nextTuple(){

        if( ! messages.isEmpty() ) {
            for (Long message : messages) {
                spoutCollector.emit(new Values(message), message);
                //messages.remove((Object)message);
            }
        }
        messages.clear();
    }

    @Override
    public void open(Map config, TopologyContext context, SpoutOutputCollector outputCollector) {
        this.transactionCount = (Long) config.get("num-transactions");
        this.spoutCollector = outputCollector;
        errorCount = new HashMap<Long, Long>();
        messages = new ArrayList<Long>();
        for(int i = 0; i < transactionCount; i++) {
            messages.add(Long.valueOf(i));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("count"));
    }

}
