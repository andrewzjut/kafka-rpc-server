package com.trcloud.thrift.service;

import com.trcloud.thrift.util.Connection;
import com.trcloud.thrift.util.StreamingUtil;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by hzzt on 2016/10/9.
 */
public class KafkaServiceAsynImpl implements KafkaService.AsyncIface {

    private static final Logger logger = LoggerFactory
            .getLogger(KafkaServiceAsynImpl.class);
    private static KafkaProducer<String, byte[]> producer;
    private static long size = 10485760L;

    static {
        Connection.loadProperties("kafka.properties");
        producer = Connection.getProducer();

    }

    public KafkaServiceAsynImpl() {
    }

    @Override
    public void sendMessage(String topic, final String value, final AsyncMethodCallback resultHandler)
            throws TException {
        final Result result = new Result();
        byte[] values = StreamingUtil.String2Byte(value);
        if (values.length > size) {
            result.setStatus(Status.SUCCESS).setMessage(value).setThriftException(null);
            logger.error("message too large ,send failed " + values.length);
            resultHandler.onComplete(result);
        } else {
            producer.send(new ProducerRecord(topic, values), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (null != metadata) {
                        result.setStatus(Status.SUCCESS).setMessage(value).setThriftException(null);
                        resultHandler.onComplete(result);
                    } else if (null != exception) {
                        result.setStatus(Status.EXCEPTION).setMessage(value).setThriftException(new ThriftException().setMessage(exception.getMessage()));
                        resultHandler.onComplete(result);
                        logger.error("message send error " + exception.getMessage());
                    }
                }
            });
        }
    }
}
