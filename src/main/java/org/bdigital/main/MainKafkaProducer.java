package org.bdigital.main;

import org.apache.log4j.PropertyConfigurator;
import org.bdigital.kafka.TwitterKafkaProducer;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class MainKafkaProducer {

    public static void main(String[] args) {
	PropertyConfigurator.configure(MainKafkaProducer.class.getClassLoader().getResourceAsStream(
		"log4j.properties"));
	
	

	// Run Producer
	TwitterKafkaProducer twitterKafkaProducer = new TwitterKafkaProducer();
	twitterKafkaProducer.run();

    }

}
