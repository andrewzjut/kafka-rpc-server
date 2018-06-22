package com.trcloud.thrift.util;

import java.util.Properties;



public class Config {
	private static Properties p ;
	public static int distributeNum;
	public static int  batch ;
	public static int  interval ;
	public static String messageDir;
	public static int retentionDays;
	public static String suffix;
	public static long waitMessageTime;
	public static long reconnect_time;
	public static String server_ip;
	public static int server_port;
	public static String charset;
	public static String dateFormat;
	static{
		p= Connection.loadProperties("rpcclient.properties");
		distributeNum = Integer.parseInt(String.valueOf(p.get("file.distributeNum")));
		interval = Integer.parseInt(String.valueOf(p.get("monitor.interval")));
		retentionDays = Integer.parseInt(String.valueOf(p.get("message.retention.days")));
		messageDir = String.valueOf(p.get("message.dir"));
		suffix = String.valueOf(p.get("message.suffix"));
		server_ip = String.valueOf(p.get("server.ip"));
		charset = String.valueOf(p.get("charset"));
		dateFormat = String.valueOf(p.get("dateFormat"));
		server_port = Integer.parseInt(String.valueOf(p.get("server.port")));
		batch = Integer.parseInt(String.valueOf(p.get("batch.message.size")));
		waitMessageTime = Long.parseLong(String.valueOf(p.get("waitMessage.interval")));
		reconnect_time = Long.parseLong(String.valueOf(p.get("reconnect.time")));
	}
	
}
