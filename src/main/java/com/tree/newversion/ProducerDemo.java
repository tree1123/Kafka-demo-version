package com.tree.newversion;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
/**
 * ues it after version 0.9
 *
 * https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+Producer+Example
 */
public class ProducerDemo {

	public static void main(String[] args) {

		Properties properties = new Properties();
		properties.put("bootstrap.servers", "kafka01:9092,kafka02:9092");
		properties.put("acks", "all");
		properties.put("retries", 0);
		properties.put("batch.size", 16384);
		properties.put("linger.ms", 1);
		properties.put("buffer.memory", 33554432);
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
		kafkaProducer.send(new ProducerRecord<String, String>("topic", "value"));
		kafkaProducer.close();

	}
	
}
