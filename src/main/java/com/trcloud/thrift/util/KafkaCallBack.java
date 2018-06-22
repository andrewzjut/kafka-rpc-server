package com.trcloud.thrift.util;


import com.trcloud.thrift.service.Result;
import com.trcloud.thrift.service.Status;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Created by hzzt on 2016/9/29.
 */
public class KafkaCallBack implements Callback {

	private Result result;

	public KafkaCallBack(Result result) {

		this.result = result;
	}

	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		if (metadata != null) {
			result.setStatus(Status.SUCCESS);
		} else {
			result.setStatus(Status.ERROR);
			exception.printStackTrace();
		}
	}
}
