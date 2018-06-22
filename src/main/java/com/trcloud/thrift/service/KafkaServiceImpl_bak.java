package com.trcloud.thrift.service;


import com.trcloud.thrift.util.ProducerPool;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


public class KafkaServiceImpl_bak implements KafkaService.Iface {

	private static final Logger logger = LoggerFactory
			.getLogger(KafkaServiceImpl_bak.class);
	private static KafkaProducer<String, String> producer;
	private static ProducerPool producerPool = new ProducerPool("kafka.properties", 5);

	public KafkaServiceImpl_bak() {
	}

	@Override
	public Result sendMessage(String topic, String value) throws TException {
		producer = ProducerPool.getProducer();

		final Result result = new Result();
		System.out.println("receive: " + topic + " "  + value);
		Future<RecordMetadata> future = producer.send(new ProducerRecord(topic, value));
		try {
			if (future.get() != null) {
				result.setStatus(Status.SUCCESS);
			} else {
				result.setStatus(Status.ERROR);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
			result.setStatus(Status.EXCEPTION);
		} catch (ExecutionException e) {
			e.printStackTrace();
			result.setStatus(Status.EXCEPTION);
		} finally {
			System.out.println(ProducerPool.getNum());
			ProducerPool.addProducer(producer);
			return result;
		}
		//return result.setStatus(Status.EXCEPTION);
	}
}
