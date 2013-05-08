package org.bdigital.storm;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.MessageFormat;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaMessageStream;
import kafka.message.Message;

import org.bdigital.kafka.TwitterKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * Spout - agafa dades de la font i ho tonra en tuples pels Bolts
 * 
 * @author mplanaguma
 * 
 */
public class KafkaTweetSpout extends BaseRichSpout implements Serializable {

    private static final long serialVersionUID = 1L;

    private Logger log = LoggerFactory.getLogger(KafkaTweetSpout.class);

    private static final Random rnd = new Random();
    private static final String[] MSSG = { "A", "B", "C", "D", "E" };
    
    private SpoutOutputCollector collector;
    private TwitterKafkaConsumer twitterKafkaConsumer;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	// el primer que s'executa: info de topologia
	log.info("Opening...");
	this.collector = collector;
	
	twitterKafkaConsumer = new TwitterKafkaConsumer();
	
    }

    public void nextTuple() {
	// Bucle controlat per storm per anar generant tuples
	// agafarem les dades de kafka
	try {
	    Thread.sleep(rnd.nextInt(500) + 500); // alentim per veure sense ser
						  // Jedi
	} catch (InterruptedException e) {
	    log.error(e.getMessage());
	}

	// obtenir tweet de kafka
	KafkaMessageStream<Message> stream = twitterKafkaConsumer.getStream();
	
	ConsumerIterator<Message> it = stream.iterator();
	while (it.hasNext() == true) {
	    Message message = it.next();
	    ByteBuffer buffer = message.payload();
	    byte[] data = new byte[buffer.remaining()];
	    buffer.get(data);
	    String messageString = new String(data, Charset.forName("UTF-8"));
	    
	    log.info(MessageFormat.format("E {0}", messageString));
	    collector.emit(new Values(UUID.randomUUID(), System.currentTimeMillis(), messageString));
	}

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	// definim tuples
	declarer.declare(new Fields("id", "timestamp", "tweet"));

    }

}
