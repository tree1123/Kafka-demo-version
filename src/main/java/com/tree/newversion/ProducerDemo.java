package com.tree.newversion;


/**
 * ues it after version 0.9
 *
 * http://kafka.apache.org/23/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html
 */
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

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
		kafkaProducer.send(new ProducerRecord<>("topic", "value"));
		kafkaProducer.close();

	}
	
}
