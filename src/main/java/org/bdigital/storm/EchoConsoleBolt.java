package org.bdigital.storm;

import java.text.MessageFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * Bolt - consumeix tuples i les pinta per pantalla
 * 
 * @author mplanaguma
 *
 */
public class EchoConsoleBolt extends BaseBasicBolt {
    
    private Logger log = LoggerFactory.getLogger(EchoConsoleBolt.class);

    @Override
    public void prepare(java.util.Map stormConf, backtype.storm.task.TopologyContext context) {
	log.info("Init Bolt");
    };
    
    public void execute(Tuple input, BasicOutputCollector collector) {
	log.info(MessageFormat.format("P {0}.", input.getString(2)));

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	// declara subtuples a enviar a altres bolts

    }

}
