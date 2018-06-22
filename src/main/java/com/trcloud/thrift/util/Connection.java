package com.trcloud.thrift.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Connection {
	private static final Logger logger = LoggerFactory.getLogger(Connection.class);
	private final ConcurrentLinkedQueue<Producer> producers = new ConcurrentLinkedQueue<>();
	private static KafkaProducer<String, byte[]> producer;
	private static Properties properties;
	
	public static Properties loadProperties(String fileName) {
		properties = new Properties();
		if (fileName.isEmpty())
			throw new NullPointerException("Config name is null...");
		InputStream inputStream = null;
		try {
			inputStream = Thread.currentThread().getContextClassLoader()
					.getResourceAsStream(fileName);
			properties.load(inputStream);
			
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage(), e);
		} finally {
			try {
				if (inputStream != null)
					inputStream.close();
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
		}
		if (properties == null)
			throw new RuntimeException("Properties file loading failed: "
					+ fileName);
		return properties;
	}
	
	public static KafkaProducer<String, byte[]> getProducer() {
		producer = new KafkaProducer<String, byte []>(properties);
		logger.info("connect kafka");
		return producer;
	}

}
