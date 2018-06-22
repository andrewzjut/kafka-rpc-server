package com.trcloud.thrift.util;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by hzzt on 2016/9/29.
 */
public class ProducerPool {

	private static final ConcurrentLinkedQueue<KafkaProducer> producers = new ConcurrentLinkedQueue<>();
	private Properties properties;

	public ProducerPool(String propertiesFile, int producerNum) {
		properties = Connection.loadProperties(propertiesFile);
		for (int i = 0; i < producerNum; i++) {
			producers.add(new KafkaProducer<String, String>(properties));
		}
	}

	public static KafkaProducer<String,String> getProducer() {
		return producers.poll();
	}
	public static void addProducer(KafkaProducer producer){
		producers.add(producer);
	}
	public static int getNum(){
		return producers.size();
	}
}
