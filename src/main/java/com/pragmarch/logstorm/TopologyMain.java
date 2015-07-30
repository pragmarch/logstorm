package com.pragmarch.logstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.pragmarch.logstorm.bolt.WordCounterBolt;
import com.pragmarch.logstorm.bolt.WordNormalizerBolt;
import com.pragmarch.logstorm.spout.WordReaderSpout;

public class TopologyMain {

    public static void main(String[] args) throws InterruptedException {

        //Topology definition
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordReaderSpout());
        builder.setBolt("word-normalizer", new WordNormalizerBolt()).shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCounterBolt(), 2).fieldsGrouping("word-normalizer", new Fields("word"));

        //Configuration
        Config conf = new Config();
        conf.put("wordsFile", args[0]);
        conf.setDebug(true);
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

        //Topology run
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());

        Thread.sleep(1000);
        cluster.shutdown();
    }

}
