package com.trcloud.thrift.server;



import com.trcloud.thrift.service.KafkaService;
import com.trcloud.thrift.service.KafkaService.AsyncProcessor;
import com.trcloud.thrift.service.KafkaServiceAsynImpl;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 使用非阻塞式IO，服务端和客户端需要指定 TFramedTransport 数据传输的方式    TNonblockingServer 服务模型
 *
 * @author hzzt
 */
public class KafkaNonblockingServer {

	private static final Logger logger = LoggerFactory
			.getLogger(KafkaNonblockingServer.class);
	public static final int SERVER_PORT = 8888;
	private static TServer m_server = null;

	private static void createNonblockingServer(int port) throws TTransportException {
		TNonblockingServerSocket socket = new TNonblockingServerSocket(port);
		AsyncProcessor tProcessor = new KafkaService.AsyncProcessor<KafkaService.AsyncIface>(new KafkaServiceAsynImpl());
		THsHaServer.Args tnbArgs = new THsHaServer.Args(socket);
		tnbArgs.processor(tProcessor);
		tnbArgs.protocolFactory(new TCompactProtocol.Factory());
		tnbArgs.transportFactory(new TFramedTransport.Factory());
		tnbArgs.processorFactory(new TProcessorFactory(tProcessor));
		// 使用非阻塞式IO，服务端和客户端需要指定TFramedTransport数据传输的方式
		m_server = new THsHaServer(tnbArgs);
	}

	/**
	 * @param port
	 * @return
	 */
	public static boolean start(int port) {
		try {
			createNonblockingServer(port);
		} catch (TTransportException e) {
			logger.warn("start thrift server error!" + e);
			return false;
		}
	logger.info("start thrift server at port:{} " + port);
		m_server.serve();
		return true;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (!start(SERVER_PORT)) {
			System.exit(0);
		}
	}
}  