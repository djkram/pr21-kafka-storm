package org.bdigital.main;

import org.apache.log4j.PropertyConfigurator;
import org.bdigital.storm.EchoConsoleBolt;
import org.bdigital.storm.KafkaTweetSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class MainStormConsumer {

    public static void main(String[] args) {
	PropertyConfigurator.configure(MainStormConsumer.class.getClassLoader().getResourceAsStream(
		"log4j.properties"));
	
	Config conf = new Config();

	TopologyBuilder builder = new TopologyBuilder();
	builder.setSpout("tweetSpout", // ID
		new KafkaTweetSpout(), // Tipus
		1 // # d'instancies - cardinalitat
	);
	builder.setBolt("echoBolt", // ID
		new EchoConsoleBolt(), // Tipus
		1) // Cardinalitat
		.allGrouping("tweetSpout"); // origen de les tuples

	LocalCluster cluster = new LocalCluster(); // Cluster de development
	cluster.submitTopology("tweetteater", conf, builder.createTopology());

    }

}
