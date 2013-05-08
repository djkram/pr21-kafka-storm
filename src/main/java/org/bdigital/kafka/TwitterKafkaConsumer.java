package org.bdigital.kafka;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaMessageStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;

public class TwitterKafkaConsumer {

    private KafkaMessageStream<Message> stream;
    
    public TwitterKafkaConsumer() {
	
	Properties props = new Properties();

	// zookeper
	props.put("zk.connect", "localhost:2181");
	props.put("zk.connectiontimeout.ms", "100000");
	props.put("groupid", "tweetsGroup"); // balanceja entre tots els
					     // consumers d'aquest id

	// Consumer
	String topicName = "realtimetweets";

	ConsumerConfig consumerConfig = new ConsumerConfig(props);
	ConsumerConnector consumerConnector = kafka.consumer.Consumer
		.createJavaConsumerConnector(consumerConfig);

	Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
	topicCountMap.put(topicName, new Integer(1));
	Map<String, List<KafkaMessageStream<Message>>> consumerMap = consumerConnector
		.createMessageStreams(topicCountMap);
	stream = consumerMap.get(topicName).get(0);
	
    }
    
    public KafkaMessageStream<Message> getStream() {

	return stream;
    }

}
