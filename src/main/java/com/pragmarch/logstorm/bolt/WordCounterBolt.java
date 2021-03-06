package com.pragmarch.logstorm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class WordCounterBolt implements IRichBolt {

    Integer id;
    String name;
    Map<String, Integer> counters;
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.counters = new HashMap<>();
        this.collector = outputCollector;
        this.name = topologyContext.getThisComponentId();
        this.id = topologyContext.getThisTaskId();
    }

    /**
     * On each word We will count
     */
    @Override
    public void execute(Tuple input) {
        String str = input.getString(0);

        /**
         * If the word dosn't exist in the map we will create this, if not We will add 1
         */
        if (!counters.containsKey(str)) {
            counters.put(str, 1);
        } else {
            Integer c = counters.get(str) + 1;
            counters.put(str, c);
        }

        //Set the tuple as Acknowledge
        collector.ack(input);
    }

    /**
     * At the end of the spout (when the cluster is shutdown We will show the word counters
     */
    @Override
    public void cleanup() {
        System.out.println("-- Word Counter [" + name + "-" + id + "] --");
        for (Map.Entry<String, Integer> entry : counters.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

}
