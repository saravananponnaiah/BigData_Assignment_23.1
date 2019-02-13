package com.training.kafka;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.io.Console;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import scala.Int;

public class KafkaProducerWithAck {
	public static void main(String args[]) throws Exception {
		String fileName = args[0].trim();
		String delimiter = args[1].trim();
		try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
			String currentLine;
			Producer<String, String> producer = getProducerInstance();
			while((currentLine = br.readLine()) != null) {
				String[] arrayData = currentLine.split(delimiter);
				RecordMetadata ack = producer.send(new ProducerRecord<String, String>(arrayData[0], arrayData[1], arrayData[2])).get();
				if (ack != null) {
					System.out.printf("Message sent. Offset: %s | Partition: %s | Topic: %s\tKey: %s\tValue: %s\n", Long.toString(ack.offset()), Integer.toString(ack.partition()), arrayData[0], arrayData[1], arrayData[2]);
				}				
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	// Get producer object to push payload to topic
	static Producer<String, String> getProducerInstance() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 3);		// It performs 3 attempts before failing to throw exception
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");         
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		
		return producer;
	}
}
