package com.tree.oldversion;


/**
 * ues it before version 0.9
 *
 * https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+Producer+Example
 */


import kafka.producer.KeyedMessage;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import java.util.Properties;

public class ProducerDemo {

	public static void main(String[] args) {

		Properties properties = new Properties();
		properties.put("metadata.broker.list", "kafka01:9092,kafka02:9092");
		properties.put("serializer.class", "kafka.serializer.StringEncoder");
		properties.put("request.requird.acks", "1");
		ProducerConfig config = new ProducerConfig(properties);
		Producer<String, String> producer = new Producer<String, String>(config);
		KeyedMessage<String,String> msg = new KeyedMessage<String,String>("topic","key","hello");
		producer.send(msg);
	}

}
