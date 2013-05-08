package org.bdigital.kafka;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.message.Message;
import kafka.producer.ProducerConfig;

import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Constants.FilterLevel;
import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.Location.Coordinate;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterKafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(TwitterKafkaProducer.class);
    
    private ClientBuilder builder;
    private BlockingQueue<String> processorQueue;
    private BlockingQueue<Event> eventQueue;
    private Producer<String, Message> producer;
    
    private final static String topicName = "realtimetweets";
    
    public TwitterKafkaProducer() {
	
	BasicConfigurator.configure();
	
	// Init properties
	Properties props =  new Properties();
	props.put("zk.connect", "127.0.0.1:2181");
	props.put("serializer.class", "kafka.serializer.DefaultEncoder");
	props.put("producer.type", "async");
	props.put("compression.codec", "1");
	props.put("batch.size", "10");
	props.put("queue.time", "1000");
	
	ProducerConfig config = new ProducerConfig(props);
	producer = new Producer<String, Message>(config);

	processorQueue = new LinkedBlockingQueue<String>(10000);
	eventQueue = new LinkedBlockingQueue<Event>(10000);

	Authentication auth = new OAuth1("BbsNbZvwGY3iQCn8LuTdQ",
		"YbYwBG3zkGnAb1Hg9eSBnT2fPwcThoFxycCGgNDY78",
		"833529332-tnyFBzf69fXcjNwZzaOzWr20mI0FUVB3IJ1AAUWV",
		"1Fk7cdselp1VyEqy7CPy9IWMp7wRvNwGJALnHDkL60");

	StatusesFilterEndpoint endPoint = new StatusesFilterEndpoint().trackTerms(
		Arrays.asList(new String[] { "Barcelona", "Catalunya" })).locations(
		Arrays.asList(new Location[] { new Location(new Coordinate(0.13, 40.49),
			new Coordinate(3.49, 43.0)) 
		// SWLon, SWLat, NELon, NELat
		}));
	
	endPoint.filterLevel(FilterLevel.None);
	endPoint.stallWarnings(true);
	
	builder = new ClientBuilder().hosts(Constants.STREAM_HOST)
		.authentication(auth)
		// .endpoint(new StatusesSampleEndpoint())
		.endpoint(endPoint).processor(new StringDelimitedProcessor(processorQueue))
		.eventMessageQueue(eventQueue);
    }

    /**
     * Run Producer Kafka
     */
    public void run() {

	Client hosebirdClient = builder.build();
	hosebirdClient.connect();

	while (hosebirdClient.isDone() == false) {

	    try {

		String jsonTweet = processorQueue.take();
		
		// adding kafka
		byte[] bytes = jsonTweet.getBytes(Charset.forName("UTF-8"));
		Message message = new Message(bytes);
		ProducerData<String, Message> data = new ProducerData<String, Message>(topicName, message);
		producer.send(data);
		
		log.debug(jsonTweet);
		log.info(hosebirdClient.getStatsTracker().toString());

	    } catch (Exception e) {
		e.printStackTrace();
	    }

	}
	hosebirdClient.stop();

    }

}
