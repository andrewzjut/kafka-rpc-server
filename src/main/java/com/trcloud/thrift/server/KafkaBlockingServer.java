package com.trcloud.thrift.server;


import com.trcloud.thrift.service.KafkaService;
import com.trcloud.thrift.service.KafkaService.Iface;
import com.trcloud.thrift.service.KafkaService.Processor;
import com.trcloud.thrift.service.KafkaServiceImpl;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaBlockingServer {
	public static final int SERVER_PORT = 8888;
	private static final Logger logger = LoggerFactory
			.getLogger(KafkaBlockingServer.class);

	/**
	 * @param port
	 */
	public void startServer(int port) {
		try {
			TProcessor tprocessor = new Processor<Iface>(new KafkaServiceImpl());
			TServerSocket serverTransport = new TServerSocket(port);
			Args tArgs = new Args(serverTransport);
			tArgs.processor(tprocessor);
			tArgs.protocolFactory(new TBinaryProtocol.Factory());
			TServer server = new TSimpleServer(tArgs);
			server.serve();
			logger.info("KafkaBlockingServer start at port:{}" + port);
		} catch (Exception e) {
			logger.warn("Server start error!!! + {}" + e);
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		KafkaBlockingServer server = new KafkaBlockingServer();
		server.startServer(SERVER_PORT);
	}
}
