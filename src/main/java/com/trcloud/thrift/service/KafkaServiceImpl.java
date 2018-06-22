package com.trcloud.thrift.service;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.trcloud.thrift.util.Connection;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




public class KafkaServiceImpl implements KafkaService.Iface {

	private static final Logger logger = LoggerFactory
			.getLogger(KafkaServiceImpl_bak.class);
	private static KafkaProducer<String, byte[]> producer;
	public  static int count = 0;
	static {
		Connection.loadProperties("kafka.properties");
		producer = Connection.getProducer();

	}

	public KafkaServiceImpl(){}

	public Result sendMessage(String topic, String value)
			throws TException {
		final Result result = new Result();
		System.out.println(topic + " "  + value);
		Future<RecordMetadata> future = producer.send(new ProducerRecord(topic, value));
		try {
			if (future.get() != null) {
				result.setMessage(value);
				result.setStatus(Status.SUCCESS);
			} else {
				result.setStatus(Status.ERROR);
				result.setMessage(value);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
			result.setStatus(Status.EXCEPTION);
		} catch (ExecutionException e) {
			e.printStackTrace();
			result.setMessage(value);
			result.setStatus(Status.EXCEPTION);
		} finally {
			return result;
		}
	}

}